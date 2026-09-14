"""Budget ledger: atomic multi-scope admission, CNY/deadline, settlement rules."""

from __future__ import annotations

import threading
from datetime import timedelta

import pytest
from pydantic import ValidationError

from kvflow.core.budget import (
    BudgetLedger,
    Charge,
    ReservationRequest,
    UsageReport,
    UsageStatus,
    new_effect_id,
    reservation_id_for,
)
from kvflow.core.contracts import BudgetScope
from kvflow.core.errors import (
    AuthorizationError,
    BudgetDenied,
    CapacityUnavailable,
    ConcurrencyError,
    ContractError,
    ReservationConflict,
)
from kvflow.core.store import Store

from .conftest import AUTH, assert_code, deadline_in


def _request(scope_ids, **overrides) -> ReservationRequest:
    payload = {
        "scope_id": scope_ids["job"],
        "project_id": scope_ids["project_id"],
        "job_id": scope_ids["job_id"],
        "node_id": "n1",
        "run_id": "run-1",
        "attempt": 1,
        "fence": 1,
        "effect_id": new_effect_id("call"),
        "purpose": "worker model call",
        "charge": Charge(calls=1, input_tokens=1000, output_tokens=2000, wall_seconds=30,
                         micro_cny=30000),
    }
    payload.update(overrides)
    if "run_id" not in overrides:
        payload["run_id"] = f"{payload['job_id']}-run"
    return ReservationRequest(**payload)


def _known(charge: Charge) -> UsageReport:
    return UsageReport.known(charge)


# ------------------------------------------------------------------ admission


def test_frozen_scope_cannot_be_mutated(ledger, scope_ids):
    scope = ledger.scope(scope_ids["job"])
    widened = BudgetScope.model_validate({**scope.model_dump(mode="json"), "calls": 99})
    with pytest.raises(AuthorizationError) as exc:
        ledger.register_scope(widened)
    assert_code(exc, "authorization")
    assert ledger.scope(scope_ids["job"]).calls == scope.calls


def test_reservation_consumes_the_whole_scope_chain(ledger, scope_ids):
    admission = ledger.reserve(_request(scope_ids))
    assert admission.accepted and admission.effects_authorized
    # the named scope carries the reservation; parents aggregate it
    assert ledger.usage(scope_ids["job"])["used"]["calls"] == 1
    assert ledger.usage(scope_ids["project"])["used"]["calls"] == 1
    assert ledger.usage(scope_ids["global"])["used"]["calls"] == 1
    for sid in (scope_ids["job"], scope_ids["project"], scope_ids["global"]):
        usage = ledger.usage(sid)
        assert usage["used"]["calls"] == 1
        assert usage["pending_reservations"] == 1


def test_call_cap_is_enforced_at_every_level(ledger, scope_ids):
    cap = ledger.scope(scope_ids["job"]).calls
    slots = ledger.scope(scope_ids["job"]).concurrency
    admitted = 0
    index = 0
    while admitted < cap:
        # settle as we go so the concurrency slots never become the binding limit
        batch = min(slots, cap - admitted)
        admissions = []
        for offset in range(batch):
            request = _request(scope_ids, effect_id=f"cap-effect-{index + offset}")
            admissions.append((ledger.reserve(request), request))
        for admission, request in admissions:
            ledger.settle(admission.reservation_id, _known(request.charge))
        admitted += batch
        index += batch
    with pytest.raises(BudgetDenied) as exc:
        ledger.reserve(_request(scope_ids, effect_id="cap-overflow"))
    assert_code(exc, "budget")
    assert ledger.usage(scope_ids["job"])["used"]["calls"] == cap
    # the parent aggregate sees the same usage, so the parent cap is real too
    assert ledger.usage(scope_ids["project"])["used"]["calls"] == cap


def test_effect_without_a_physical_call_is_refused(ledger, scope_ids):
    with pytest.raises(ContractError):
        ledger.reserve(_request(scope_ids, charge=Charge(storage_bytes=10)))


def test_deadline_is_a_real_wall_clock_bound(ledger, scope_ids, store):
    store.clock.advance(10801)
    with pytest.raises(BudgetDenied) as exc:
        ledger.reserve(_request(scope_ids))
    assert_code(exc, "budget")
    assert ledger.usage(scope_ids["job"])["deadline_elapsed"] is True


def test_zero_seconds_still_cannot_outlive_a_frozen_deadline(ledger, scope_ids, store):
    scope = ledger.scope(scope_ids["job"])
    store.clock.advance(10800)
    with pytest.raises(BudgetDenied):
        ledger.reserve(_request(scope_ids, charge=Charge(calls=1, micro_cny=1)))
    assert scope.deadline


# ------------------------------------------------------------------- replay


def test_replaying_a_reserved_effect_never_authorizes_a_second_dispatch(ledger, scope_ids):
    request = _request(scope_ids)
    first = ledger.reserve(request)
    replay = ledger.reserve(request)
    assert replay.replayed is True
    assert replay.effects_authorized is False
    assert replay.reservation_id == first.reservation_id
    assert ledger.usage(scope_ids["job"])["used"]["calls"] == 1


def test_settled_reservation_replay_does_not_authorize(ledger, scope_ids):
    request = _request(scope_ids)
    first = ledger.reserve(request)
    ledger.settle(first.reservation_id, _known(request.charge))
    replay = ledger.reserve(request)
    assert replay.already_resolved is True
    assert replay.effects_authorized is False
    assert replay.status == "SETTLED"


def test_cross_scope_reuse_of_a_reservation_identity_is_refused(ledger, scope_ids):
    """A settled reservation must never be reusable as fresh permission.

    The reservation identity deliberately does NOT include ``scope_id`` or the
    effective caps, so the same effect cannot be re-admitted with a wider scope.
    """
    request = _request(scope_ids)
    admission = ledger.reserve(request)
    ledger.settle(admission.reservation_id, _known(request.charge))
    cross_scope = ReservationRequest(**{**request.__dict__, "scope_id": scope_ids["project"]})
    result = ledger.reserve(cross_scope)
    assert result.accepted is False
    assert result.already_resolved is True
    assert result.effects_authorized is False
    assert result.reservation_id == admission.reservation_id


def test_same_effect_with_a_different_charge_is_not_readmitted(ledger, scope_ids):
    request = _request(scope_ids)
    first = ledger.reserve(request)
    altered = ReservationRequest(
        **{**request.__dict__, "charge": Charge(calls=1, input_tokens=999, output_tokens=1,
                                                wall_seconds=1, micro_cny=1)}
    )
    result = ledger.reserve(altered)
    assert result.accepted is False
    assert result.effects_authorized is False
    assert result.reservation_id == first.reservation_id
    assert ledger.usage(scope_ids["job"])["used"]["calls"] == 1


def test_reservation_ids_are_derived_from_the_effect_identity(ledger, scope_ids):
    """A collision between two different effects cannot be minted by a caller."""
    first = ReservationRequest(**{**_request(scope_ids).__dict__, "run_id": "run-a"})
    second = ReservationRequest(**{**_request(scope_ids).__dict__, "run_id": "run-b"})
    assert first.identity_digest != second.identity_digest
    assert reservation_id_for(first.identity_digest) != reservation_id_for(second.identity_digest)


def test_unknown_reservation_replay_does_not_authorize(ledger, scope_ids):
    request = _request(scope_ids)
    first = ledger.reserve(request)
    ledger.settle(first.reservation_id, UsageReport.unknown(), reconciled_by="executor")
    replay = ledger.reserve(request)
    assert replay.already_resolved is True
    assert replay.status == "UNKNOWN"
    assert replay.effects_authorized is False


# ---------------------------------------------------------------- settlement


def test_settlement_requires_an_explicit_status_for_every_dimension(ledger, scope_ids):
    ledger.reserve(_request(scope_ids))
    with pytest.raises(ContractError):
        UsageReport(status={"calls": "KNOWN"}, actual=Charge(calls=1))


def test_unknown_settlement_requires_a_reconciliation_actor(ledger, scope_ids):
    admission = ledger.reserve(_request(scope_ids))
    with pytest.raises(AuthorizationError):
        ledger.settle(admission.reservation_id, UsageReport.unknown())


def test_unknown_settlement_keeps_the_charge(ledger, scope_ids):
    request = _request(scope_ids)
    admission = ledger.reserve(request)
    state = ledger.settle(
        admission.reservation_id, UsageReport.unknown(), reconciled_by="executor"
    )
    assert state == "UNKNOWN"
    usage = ledger.usage(scope_ids["job"])
    assert usage["used"]["calls"] == 1
    assert usage["unknown_reservations"] == 1
    assert usage["pending_reservations"] == 0


def test_known_settlement_cannot_refund_a_physical_call(ledger, scope_ids):
    request = _request(scope_ids)
    admission = ledger.reserve(request)
    report = UsageReport(
        status={name: UsageStatus.KNOWN.value for name in request.charge.to_dict()},
        actual=Charge(calls=1, input_tokens=800, output_tokens=1000, wall_seconds=25,
                      micro_cny=25000),
    )
    assert ledger.settle(admission.reservation_id, report) == "SETTLED"
    usage = ledger.usage(scope_ids["job"])
    assert usage["used"]["calls"] == 1
    assert usage["used"]["input_tokens"] == 800
    assert usage["used"]["micro_cny"] == 25000


def test_never_started_releases_capacity_only_with_evidence(ledger, scope_ids):
    admission = ledger.reserve(_request(scope_ids))
    with pytest.raises(AuthorizationError):
        ledger.release_never_started(admission.reservation_id, evidence="")
    ledger.release_never_started(
        admission.reservation_id, evidence="tool dispatcher recorded no subprocess start"
    )
    usage = ledger.usage(scope_ids["job"])
    assert usage["used"]["calls"] == 0
    assert usage["pending_reservations"] == 0


def test_unknown_cannot_be_reconciled_as_never_started(ledger, scope_ids):
    request = _request(scope_ids)
    admission = ledger.reserve(request)
    ledger.settle(admission.reservation_id, UsageReport.unknown(), reconciled_by="executor")
    with pytest.raises(ContractError):
        ledger.reconcile_unknown(
            admission.reservation_id,
            UsageReport.never_started(),
            evidence="guessed",
        )


def test_reconciliation_preserves_financial_uncertainty(ledger, scope_ids):
    request = _request(scope_ids)
    admission = ledger.reserve(request)
    ledger.settle(admission.reservation_id, UsageReport.unknown(), reconciled_by="executor")
    state = ledger.reconcile_unknown(
        admission.reservation_id,
        _known(Charge(calls=1, input_tokens=900, output_tokens=1500, wall_seconds=20,
                      micro_cny=27000)),
        evidence="provider usage log",
    )
    assert state == "SETTLED"
    assert ledger.usage(scope_ids["job"])["used"]["calls"] == 1
    # the whole 30,000 micro-CNY reservation stays consumed: the effect facts
    # were never observed, so no part of the hold may be refunded
    assert ledger.usage(scope_ids["job"])["used"]["micro_cny"] == 30000
    assert ledger.usage(scope_ids["job"])["used"]["input_tokens"] == 1000
    assert ledger.usage(scope_ids["job"])["unknown_reservations"] == 0


def test_reported_overage_beyond_the_reservation_is_refused(ledger, scope_ids):
    request = _request(scope_ids, charge=Charge(calls=1, input_tokens=100, output_tokens=100,
                                                wall_seconds=5, micro_cny=1000))
    admission = ledger.reserve(request)
    with pytest.raises(BudgetDenied) as exc:
        ledger.settle(
            admission.reservation_id,
            _known(Charge(calls=1, input_tokens=5000, output_tokens=100, wall_seconds=5,
                          micro_cny=1000)),
        )
    assert_code(exc, "budget")


def test_settling_twice_is_refused(ledger, scope_ids):
    request = _request(scope_ids)
    admission = ledger.reserve(request)
    ledger.settle(admission.reservation_id, _known(request.charge))
    with pytest.raises(ConcurrencyError):
        ledger.settle(admission.reservation_id, _known(request.charge))


# ---------------------------------------------------------------- capacity


def test_slot_contention_waits_and_recovers_without_cap_changes(ledger, scope_ids):
    """A temporary slot shortage must return WAITING_CAPACITY, not pause the job."""
    job_scope = ledger.scope(scope_ids["job"])
    requests = [
        _request(scope_ids, effect_id=f"effect-{i}", run_id=f"run-{i}")
        for i in range(job_scope.concurrency)
    ]
    admitted = [ledger.reserve(request) for request in requests]
    with pytest.raises(CapacityUnavailable) as exc:
        ledger.reserve(_request(scope_ids, effect_id="overflow", run_id="run-overflow"))
    assert_code(exc, "capacity")
    assert ledger.usage(scope_ids["job"])["caps"]["calls"] == job_scope.calls
    assert ledger.has_capacity(scope_ids["job"]) is False
    ledger.settle(admitted[0].reservation_id, _known(requests[0].charge))
    assert ledger.has_capacity(scope_ids["job"]) is True
    late = ledger.reserve(
        _request(scope_ids, effect_id="late", run_id=f"run-late", charge=Charge(
            calls=1, input_tokens=1000, output_tokens=2000, wall_seconds=30, micro_cny=30000))
    )
    assert late.accepted
    # every reservation is accounted for: nothing was silently dropped
    assert ledger.usage(scope_ids["job"])["used"]["calls"] == job_scope.concurrency + 1


def test_concurrent_reservations_cannot_exceed_the_cap(tmp_path):
    """Three processes reserving the last capacity must not both win."""
    store = Store(tmp_path / "race.sqlite3")
    store.initialize()
    ledger = BudgetLedger(store)
    scope = BudgetScope(
        scope_id="scope-race",
        scope_kind="GLOBAL",
        calls=1,
        input_tokens=1000,
        output_tokens=1000,
        tool_calls=1,
        storage_bytes=1000,
        wall_seconds=60,
        micro_cny=1000,
        concurrency=3,
        deadline=deadline_in(600),
        authorization_digest=AUTH,
    )
    ledger.register_scope(scope)
    results: list[str] = []
    lock = threading.Lock()

    def worker(index: int) -> None:
        local = BudgetLedger(Store(tmp_path / "race.sqlite3"))
        request = ReservationRequest(
            scope_id="scope-race",
            project_id="p",
            job_id="j",
            node_id="n",
            run_id=f"run-{index}",
            attempt=1,
            fence=1,
            effect_id=f"effect-{index}",
            purpose="race",
            charge=Charge(calls=1, micro_cny=100),
        )
        try:
            local.reserve(request)
            outcome = "ADMITTED"
        except BudgetDenied:
            outcome = "DENIED"
        with lock:
            results.append(outcome)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert results.count("ADMITTED") == 1
    assert results.count("DENIED") == 3
    assert ledger.usage("scope-race")["used"]["calls"] == 1


def test_estimate_is_conservative_and_integer(ledger):
    micro = ledger.estimate_micro_cny(input_tokens=1_000_000, output_tokens=1_000_000)
    assert micro == 15 * 1_000_000
    assert isinstance(micro, int)


def test_scope_shape_is_validated(ledger):
    with pytest.raises(ValidationError):
        BudgetScope.model_validate(
            {
                "scope_id": "bad",
                "scope_kind": "GLOBAL",
                "project_id": "p",
                "calls": 1,
                "input_tokens": 1,
                "output_tokens": 1,
                "tool_calls": 1,
                "storage_bytes": 1,
                "wall_seconds": 1,
                "micro_cny": 1,
                "concurrency": 1,
                "deadline": deadline_in(60),
                "authorization_digest": AUTH,
            }
        )
