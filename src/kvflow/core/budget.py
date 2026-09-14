"""Atomic multi-scope budget ledger with integer money and real wall-clock deadlines.

Invariants implemented here, each one answering an F1 Manager finding:

* Money is an integer count of micro-CNY (1 CNY = 1,000,000). No float money.
* A reservation is admitted only after *every* scope in its chain
  (global -> project -> job) has been checked and updated inside one SQLite
  transaction, so two processes cannot both spend the last call.
* The scope chain, caps, parent link and absolute deadline are frozen at scope
  registration; there is no API that raises a cap or erases usage.
* Re-admitting a reservation identity that already exists never authorizes a
  second physical effect. Reuse across a different project/job/node/run/fence
  or after settlement/UNKNOWN is rejected explicitly instead of silently
  accepted.
* Settlement never refunds known execution. Each usage dimension must be
  reported as KNOWN, UNKNOWN or NEVER_STARTED; UNKNOWN keeps the reservation
  charged (financial uncertainty is preserved), and only NEVER_STARTED releases
  capacity. There is no "omit this number and get money back" path.
* Slot contention returns ``WAITING_CAPACITY`` and stays admissible later; it
  never durably pauses a job and never raises a cap.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from enum import Enum
from typing import Any, Iterable, Sequence

from .contracts import BudgetScope, canonical_json, content_digest
from .errors import (
    AuthorizationError,
    BudgetDenied,
    CapacityUnavailable,
    ConcurrencyError,
    ContractError,
    NotFoundError,
    ReservationConflict,
)
from .store import Store, utcnow

MICRO_CNY_PER_CNY = 1_000_000

#: hard ceiling on the chain length so a malformed registration cannot recurse
MAX_SCOPE_DEPTH = 8


class Dimension(str, Enum):
    CALLS = "calls"
    INPUT_TOKENS = "input_tokens"
    OUTPUT_TOKENS = "output_tokens"
    TOOL_CALLS = "tool_calls"
    STORAGE_BYTES = "storage_bytes"
    WALL_SECONDS = "wall_seconds"
    MICRO_CNY = "micro_cny"


DIMENSIONS: tuple[str, ...] = tuple(d.value for d in Dimension)
#: dimensions that describe a physical effect and therefore never vanish
EFFECT_DIMENSIONS = (Dimension.CALLS.value, Dimension.TOOL_CALLS.value, Dimension.MICRO_CNY.value)


class UsageStatus(str, Enum):
    KNOWN = "KNOWN"
    UNKNOWN = "UNKNOWN"
    NEVER_STARTED = "NEVER_STARTED"


def _whole(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ContractError(f"{name} must be an integer", field=name, value=repr(value))
    if value < 0:
        raise ContractError(f"{name} must not be negative", field=name, value=value)
    return value


@dataclass(frozen=True)
class Charge:
    """A numeric vector over the ledger dimensions."""

    calls: int = 0
    input_tokens: int = 0
    output_tokens: int = 0
    tool_calls: int = 0
    storage_bytes: int = 0
    wall_seconds: int = 0
    micro_cny: int = 0

    def __post_init__(self) -> None:
        for name in DIMENSIONS:
            _whole(getattr(self, name), name)

    def to_dict(self) -> dict[str, int]:
        return {name: int(getattr(self, name)) for name in DIMENSIONS}

    @classmethod
    def from_mapping(cls, data: dict[str, Any]) -> "Charge":
        unknown = set(data) - set(DIMENSIONS)
        if unknown:
            raise ContractError("unknown charge dimension", unknown=sorted(unknown))
        return cls(**{k: v for k, v in data.items()})

    def __add__(self, other: "Charge") -> "Charge":
        return Charge(**{n: getattr(self, n) + getattr(other, n) for n in DIMENSIONS})

    def __le__(self, other: "Charge") -> bool:  # type: ignore[override]
        return all(getattr(self, n) <= getattr(other, n) for n in DIMENSIONS)

    def __sub__(self, other: "Charge") -> "Charge":
        merged = {}
        for name in DIMENSIONS:
            value = getattr(self, name) - getattr(other, name)
            merged[name] = max(0, value)
        return Charge(**merged)

    def is_zero(self) -> bool:
        return all(getattr(self, n) == 0 for n in DIMENSIONS)


@dataclass(frozen=True)
class UsageReport:
    """Per-dimension settlement status. Never a default-zero refund."""

    status: dict[str, str]
    actual: Charge

    def __post_init__(self) -> None:
        if set(self.status) != set(DIMENSIONS):
            missing = sorted(set(DIMENSIONS) - set(self.status))
            extra = sorted(set(self.status) - set(DIMENSIONS))
            raise ContractError(
                "every usage dimension needs an explicit status",
                missing=missing,
                extra=extra,
            )
        for name, value in self.status.items():
            try:
                UsageStatus(value)
            except ValueError as exc:
                raise ContractError(
                    f"invalid usage status {value!r} for {name}", field=name
                ) from exc
            if value == UsageStatus.NEVER_STARTED.value and getattr(self.actual, name) != 0:
                raise ContractError(
                    "a dimension that never started cannot report actual usage", field=name
                )

    @classmethod
    def known(cls, actual: Charge) -> "UsageReport":
        return cls({name: UsageStatus.KNOWN.value for name in DIMENSIONS}, actual)

    @classmethod
    def unknown(cls, statuses: dict[str, str] | None = None) -> "UsageReport":
        base = {name: UsageStatus.UNKNOWN.value for name in DIMENSIONS}
        if statuses:
            base.update(statuses)
        return cls(base, Charge())

    @classmethod
    def never_started(cls) -> "UsageReport":
        return cls({name: UsageStatus.NEVER_STARTED.value for name in DIMENSIONS}, Charge())

    def unknown_dimensions(self) -> list[str]:
        return [n for n in DIMENSIONS if self.status[n] == UsageStatus.UNKNOWN.value]


@dataclass(frozen=True)
class ReservationRequest:
    """Immutable identity of one physical effect. This is the idempotency key."""

    scope_id: str
    project_id: str
    job_id: str
    node_id: str
    run_id: str
    attempt: int
    fence: int
    effect_id: str
    purpose: str
    charge: Charge

    @property
    def identity(self) -> dict[str, Any]:
        return {
            "scope_id": self.scope_id,
            "project_id": self.project_id,
            "job_id": self.job_id,
            "node_id": self.node_id,
            "run_id": self.run_id,
            "attempt": self.attempt,
            "fence": self.fence,
            "effect_id": self.effect_id,
            "purpose": self.purpose,
        }

    @property
    def identity_digest(self) -> str:
        """Stable identity of one physical effect.

        ``scope_id`` and the effective caps are deliberately excluded: an effect
        reserved under the job scope and then claimed under a wider project
        scope must resolve to the *same* reservation, so it is reported as
        already resolved instead of being admitted a second time.
        """
        identity = {k: v for k, v in self.identity.items() if k != "scope_id"}
        return content_digest(identity)

    @property
    def request_digest(self) -> str:
        return content_digest({**self.identity, "charge": self.charge.to_dict()})


@dataclass(frozen=True)
class Admission:
    accepted: bool
    reservation_id: str
    scope_id: str
    replayed: bool
    already_resolved: bool
    status: str
    effects_authorized: bool
    message: str


def reservation_id_for(identity_digest: str) -> str:
    return "resv_" + identity_digest[:24]


class BudgetLedger:
    """Durable budget ledger sharing the v1 store and its injected clock."""

    def __init__(self, store: Store) -> None:
        self.store = store

    # ------------------------------------------------------------ scopes
    def register_scope(self, scope: BudgetScope) -> str:
        chain = self._chain(scope)
        with self.store.tx() as conn:
            existing = conn.execute(
                "SELECT document, digest FROM budget_scopes WHERE scope_id = ?",
                (scope.scope_id,),
            ).fetchone()
            digest = content_digest(scope)
            if existing is not None:
                if existing["digest"] != digest:
                    raise AuthorizationError(
                        "budget scope is frozen; caps and deadline cannot be mutated",
                        scope_id=scope.scope_id,
                    )
                return scope.scope_id
            conn.execute(
                "INSERT INTO budget_scopes(scope_id, scope_kind, parent_scope_id, project_id,"
                " job_id, document, digest, authorization_digest, deadline, created_at)"
                " VALUES (?,?,?,?,?,?,?,?,?,?)",
                (
                    scope.scope_id,
                    scope.scope_kind,
                    scope.parent_scope_id,
                    scope.project_id,
                    scope.job_id,
                    canonical_json(scope),
                    digest,
                    scope.authorization_digest,
                    scope.deadline.timestamp(),
                    utcnow().isoformat(),
                ),
            )
        return scope.scope_id

    def scope(self, scope_id: str) -> BudgetScope:
        with self.store.read() as conn:
            row = conn.execute(
                "SELECT document FROM budget_scopes WHERE scope_id = ?", (scope_id,)
            ).fetchone()
        if row is None:
            raise NotFoundError("unknown budget scope", scope_id=scope_id)
        return BudgetScope.model_validate(json.loads(row["document"]))

    def _chain(self, scope: BudgetScope) -> list[BudgetScope]:
        chain = [scope]
        seen = {scope.scope_id}
        current = scope
        while current.parent_scope_id:
            if len(chain) > MAX_SCOPE_DEPTH:
                raise ContractError("budget scope chain is too deep", scope_id=scope.scope_id)
            parent = self.scope(current.parent_scope_id)
            if parent.scope_id in seen:
                raise ContractError("budget scope chain contains a cycle",
                                    scope_id=scope.scope_id)
            seen.add(parent.scope_id)
            chain.append(parent)
            current = parent
        return chain

    def _chain_ids(self, scope_id: str) -> list[str]:
        scope = self.scope(scope_id)
        return [s.scope_id for s in self._chain(scope)]

    # -------------------------------------------------------- reservation
    def _deadline_check(self, scope_ids: Iterable[str]) -> None:
        now = self.store.clock.now()
        for scope_id in scope_ids:
            scope = self.scope(scope_id)
            if now >= scope.deadline.timestamp():
                raise BudgetDenied(
                    f"budget deadline elapsed for scope {scope_id}",
                    scope_id=scope_id,
                    deadline=scope.deadline.isoformat(),
                )

    def _child_scope_ids(self, conn, scope_id: str) -> list[str]:
        """All descendant scopes, breadth first, so a parent cap sees everything."""
        found: list[str] = []
        frontier = [scope_id]
        while frontier:
            placeholders = ",".join("?" for _ in frontier)
            rows = conn.execute(
                f"SELECT scope_id FROM budget_scopes WHERE parent_scope_id IN ({placeholders})",
                tuple(frontier),
            ).fetchall()
            children = [row["scope_id"] for row in rows if row["scope_id"] not in found]
            if not children:
                break
            found.extend(children)
            frontier = children
        return found

    def _usage(self, conn, scope_id: str) -> Charge:
        """Aggregate usage for a scope: its own reservations plus its descendants.

        A reservation is booked on the scope the caller names, so a parent only
        sees it through aggregation. That keeps one authoritative number per
        scope while still enforcing the parent cap.
        """
        return self._usage_for(conn, [scope_id])

    def _usage_for(self, conn, scope_ids: Sequence[str]) -> Charge:
        if not scope_ids:
            return Charge()
        placeholders = ",".join("?" for _ in scope_ids)
        rows = conn.execute(
            f"SELECT state, reserved, actual FROM reservations WHERE scope_id IN ({placeholders})",
            tuple(scope_ids),
        ).fetchall()
        total = Charge()
        for row in rows:
            if row["state"] == "RELEASED":
                continue
            reserved = Charge.from_mapping(json.loads(row["reserved"]))
            if row["state"] == "RESERVED":
                total = total + reserved
            elif row["state"] == "UNKNOWN":
                actual = Charge.from_mapping(json.loads(row["actual"])) if row["actual"] else Charge()
                total = total + _dimension_max(reserved, actual)
            else:  # SETTLED: actual replaces the held reservation
                actual = Charge.from_mapping(json.loads(row["actual"])) if row["actual"] else Charge()
                total = total + actual
        return total

    def _aggregate_usage(self, conn, scope_id: str) -> Charge:
        return self._usage_for(conn, [scope_id, *self._child_scope_ids(conn, scope_id)])

    def _active_slots(self, conn, scope_id: str) -> int:
        row = conn.execute(
            "SELECT COUNT(*) AS c FROM reservations WHERE scope_id = ? AND state = 'RESERVED'",
            (scope_id,),
        ).fetchone()
        return int(row["c"])

    def reserve(self, request: ReservationRequest) -> Admission:
        """Atomically admit one physical effect across the whole scope chain."""
        if request.charge.calls <= 0 and request.charge.tool_calls <= 0:
            raise ContractError(
                "a reservation must cover at least one physical call or tool invocation"
            )
        identity_digest = request.identity_digest
        reservation_id = reservation_id_for(identity_digest)
        with self.store.tx() as conn:
            existing = conn.execute(
                "SELECT * FROM reservations WHERE reservation_id = ?", (reservation_id,)
            ).fetchone()
            if existing is None:
                existing = conn.execute(
                    "SELECT * FROM reservations WHERE identity_digest = ?"
                    " ORDER BY created_at DESC LIMIT 1",
                    (identity_digest,),
                ).fetchone()
            if existing is not None:
                return self._handle_existing(existing, request)
            chain = self._chain(self.scope(request.scope_id))
            scope_ids = [s.scope_id for s in chain]
            if request.scope_id not in scope_ids:  # pragma: no cover - defensive
                raise NotFoundError("unknown budget scope", scope_id=request.scope_id)
            self._deadline_check(scope_ids)
            for scope in chain:
                usage = self._aggregate_usage(conn, scope.scope_id)
                caps = _caps(scope)
                if not (usage + request.charge) <= caps:
                    raise BudgetDenied(
                        f"budget scope {scope.scope_id} has insufficient remaining capacity",
                        scope_id=scope.scope_id,
                        requested=request.charge.to_dict(),
                        used=usage.to_dict(),
                        caps=caps.to_dict(),
                    )
                if self._active_slots(conn, scope.scope_id) >= scope.concurrency:
                    raise CapacityUnavailable(
                        f"scope {scope.scope_id} has no free execution slot",
                        scope_id=scope.scope_id,
                        concurrency=scope.concurrency,
                    )
            at = utcnow().isoformat()
            conn.execute(
                "INSERT INTO reservations(reservation_id, scope_id, request_digest,"
                " identity_digest, project_id, job_id, node_id, run_id, attempt, fence, effect_id,"
                " state, reserved, actual, created_at, updated_at)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?,'RESERVED',?,NULL,?,?)",
                (
                    reservation_id,
                    request.scope_id,
                    request.request_digest,
                    identity_digest,
                    request.project_id,
                    request.job_id,
                    request.node_id,
                    request.run_id,
                    request.attempt,
                    request.fence,
                    request.effect_id,
                    json.dumps(request.charge.to_dict(), sort_keys=True),
                    at,
                    at,
                ),
            )
        return Admission(
            accepted=True,
            reservation_id=reservation_id,
            scope_id=request.scope_id,
            replayed=False,
            already_resolved=False,
            status="RESERVED",
            effects_authorized=True,
            message="reserved",
        )

    def _handle_existing(self, existing, request: ReservationRequest) -> Admission:
        """An existing effect identity is never admitted a second time.

        The charge vector and the scope are *authorization* facts of the first
        admission, not part of the effect identity, so a replay with a different
        vector is answered with the authoritative stored state instead of
        granting a second physical effect. A collision between two genuinely
        different effects (same ``effect_id``, different run/node/attempt) is
        still a hard error.
        """
        if existing["identity_digest"] != request.identity_digest:
            raise ReservationConflict(
                "reservation id collision with a different effect identity",
                reservation_id=existing["reservation_id"],
            )
        state = existing["state"]
        scope_matches = existing["scope_id"] == request.scope_id
        charge_matches = existing["request_digest"] == request.request_digest
        if state == "RESERVED" and scope_matches and charge_matches:
            return Admission(
                accepted=True,
                reservation_id=existing["reservation_id"],
                scope_id=existing["scope_id"],
                replayed=True,
                already_resolved=False,
                status=state,
                effects_authorized=False,
                message="this effect is already reserved and must not be dispatched twice",
            )
        return Admission(
            accepted=False,
            reservation_id=existing["reservation_id"],
            scope_id=existing["scope_id"],
            replayed=True,
            already_resolved=True,
            status=state,
            effects_authorized=False,
            message=(
                f"reservation is already {state}"
                + ("" if scope_matches else " under a different scope")
                + ("" if charge_matches else " with a different charge vector")
                + "; no new physical effect is authorized"
            ),
        )

    # ------------------------------------------------------------ settle
    def settle(self, reservation_id: str, report: UsageReport, *, reconciled_by: str = "") -> str:
        """Settle a reservation. Unknown usage keeps its charge and cap slot."""
        with self.store.tx() as conn:
            row = conn.execute(
                "SELECT * FROM reservations WHERE reservation_id = ?", (reservation_id,)
            ).fetchone()
            if row is None:
                raise NotFoundError("unknown reservation", reservation_id=reservation_id)
            if row["state"] != "RESERVED":
                raise ConcurrencyError(
                    f"reservation is already {row['state']}",
                    reservation_id=reservation_id,
                )
            reserved = Charge.from_mapping(json.loads(row["reserved"]))
            charged_values: dict[str, int] = {}
            statuses = {}
            for name in DIMENSIONS:
                status = report.status[name]
                statuses[name] = status
                if status == UsageStatus.KNOWN.value:
                    value = getattr(report.actual, name)
                    if value > getattr(reserved, name):
                        overage = value - getattr(reserved, name)
                        raise BudgetDenied(
                            f"provider reported {overage} more {name} than was authorized",
                            reservation_id=reservation_id,
                            dimension=name,
                            authorized=getattr(reserved, name),
                            reported=value,
                        )
                    # a known dimension charges the real amount, never a refund
                    charged_values[name] = value
                elif status == UsageStatus.UNKNOWN.value:
                    charged_values[name] = max(
                        getattr(reserved, name), getattr(report.actual, name)
                    )
                else:  # NEVER_STARTED releases capacity
                    charged_values[name] = 0
            charged = Charge(**charged_values)
            unknown = [n for n in DIMENSIONS if statuses[n] == UsageStatus.UNKNOWN.value]
            state = "UNKNOWN" if unknown else "SETTLED"
            if state == "UNKNOWN" and not reconciled_by:
                # an unknown settlement must name who/what produced the evidence
                raise AuthorizationError(
                    "UNKNOWN settlement requires the reconciliation actor",
                    reservation_id=reservation_id,
                )
            conn.execute(
                "UPDATE reservations SET state = ?, actual = ?, updated_at = ?"
                " WHERE reservation_id = ?",
                (state, json.dumps(charged.to_dict(), sort_keys=True),
                 utcnow().isoformat(), reservation_id),
            )
        return state

    def release_never_started(self, reservation_id: str, *, evidence: str) -> None:
        """Release capacity only for an effect with evidence it never started."""
        if not evidence:
            raise AuthorizationError("releasing a reservation requires evidence of non-execution")
        with self.store.tx() as conn:
            row = conn.execute(
                "SELECT state, actual FROM reservations WHERE reservation_id = ?",
                (reservation_id,),
            ).fetchone()
            if row is None:
                raise NotFoundError("unknown reservation", reservation_id=reservation_id)
            if row["state"] not in {"RESERVED", "UNKNOWN"}:
                raise ConcurrencyError(
                    "only an unresolved reservation can be released",
                    reservation_id=reservation_id,
                )
            actual = Charge.from_mapping(json.loads(row["actual"])) if row["actual"] else Charge()
            if not actual.is_zero():
                raise BudgetDenied(
                    "a reservation with recorded usage cannot be released",
                    reservation_id=reservation_id,
                )
            conn.execute(
                "UPDATE reservations SET state = 'SETTLED', actual = ?, updated_at = ?"
                " WHERE reservation_id = ?",
                (
                    json.dumps(Charge().to_dict(), sort_keys=True),
                    utcnow().isoformat(),
                    reservation_id,
                ),
            )
        return None

    def reconcile_unknown(self, reservation_id: str, report: UsageReport, *, evidence: str) -> str:
        """Resolve an UNKNOWN reservation with evidence; usage is never erased."""
        if not evidence:
            raise AuthorizationError("reconciling an UNKNOWN reservation requires evidence")
        if any(v == UsageStatus.NEVER_STARTED.value for v in report.status.values()):
            raise ContractError(
                "an UNKNOWN reservation cannot be reconciled as never started; release it instead"
            )
        with self.store.tx() as conn:
            row = conn.execute(
                "SELECT state, reserved, actual FROM reservations WHERE reservation_id = ?",
                (reservation_id,),
            ).fetchone()
            if row is None:
                raise NotFoundError("unknown reservation", reservation_id=reservation_id)
            if row["state"] != "UNKNOWN":
                raise ConcurrencyError(
                    "only an UNKNOWN reservation can be reconciled",
                    reservation_id=reservation_id,
                )
            reserved = Charge.from_mapping(json.loads(row["reserved"]))
            held = Charge.from_mapping(json.loads(row["actual"]))
            reconciled_values: dict[str, int] = {}
            for name in DIMENSIONS:
                status = report.status[name]
                if status == UsageStatus.KNOWN.value:
                    value = max(getattr(held, name), getattr(report.actual, name))
                else:
                    value = max(getattr(held, name), getattr(reserved, name))
                reconciled_values[name] = value
            charged = Charge(**reconciled_values)
            unknown = [n for n in DIMENSIONS if report.status[n] == UsageStatus.UNKNOWN.value]
            state = "UNKNOWN" if unknown else "SETTLED"
            conn.execute(
                "UPDATE reservations SET state = ?, actual = ?, updated_at = ?"
                " WHERE reservation_id = ?",
                (state, json.dumps(charged.to_dict(), sort_keys=True),
                 utcnow().isoformat(), reservation_id),
            )
        return state

    # ------------------------------------------------------------ report
    def reservation(self, reservation_id: str) -> dict[str, Any]:
        with self.store.read() as conn:
            row = conn.execute(
                "SELECT * FROM reservations WHERE reservation_id = ?", (reservation_id,)
            ).fetchone()
        if row is None:
            raise NotFoundError("unknown reservation", reservation_id=reservation_id)
        data = dict(row)
        data["reserved_charge"] = json.loads(data.pop("reserved"))
        data["actual_charge"] = json.loads(data["actual"]) if data["actual"] else None
        return data

    def usage(self, scope_id: str) -> dict[str, Any]:
        scope = self.scope(scope_id)
        with self.store.read() as conn:
            descendants = self._child_scope_ids(conn, scope_id)
            family = [scope_id, *descendants]
            usage = self._usage_for(conn, family)
            placeholders = ",".join("?" for _ in family)
            pending = conn.execute(
                f"SELECT COUNT(*) AS c FROM reservations WHERE state = 'RESERVED'"
                f" AND scope_id IN ({placeholders})",
                tuple(family),
            ).fetchone()
            unknown = conn.execute(
                f"SELECT COUNT(*) AS c FROM reservations WHERE state = 'UNKNOWN'"
                f" AND scope_id IN ({placeholders})",
                tuple(family),
            ).fetchone()
        return {
            "scope_id": scope_id,
            "scope_kind": scope.scope_kind,
            "caps": _caps(scope).to_dict(),
            "used": usage.to_dict(),
            "remaining": (Charge.from_mapping(_caps(scope).to_dict()) - usage).to_dict(),
            "pending_reservations": int(pending["c"]),
            "unknown_reservations": int(unknown["c"]),
            "deadline": scope.deadline.isoformat(),
            "deadline_elapsed": self.store.clock.now() >= scope.deadline.timestamp(),
            "concurrency": scope.concurrency,
            "project_id": scope.project_id,
            "job_id": scope.job_id,
        }

    def has_capacity(self, scope_id: str) -> bool:
        try:
            self._deadline_check(self._chain_ids(scope_id))
        except BudgetDenied:
            return False
        with self.store.read() as conn:
            for sid in self._chain_ids(scope_id):
                scope = self.scope(sid)
                if self._active_slots(conn, sid) >= scope.concurrency:
                    return False
        return True

    def estimate_micro_cny(self, *, input_tokens: int, output_tokens: int,
                           input_micro_cny_per_million: int = 3 * MICRO_CNY_PER_CNY,
                           output_micro_cny_per_million: int = 12 * MICRO_CNY_PER_CNY) -> int:
        """Conservative pre-dispatch estimate at peak rates, in micro-CNY."""
        _whole(input_tokens, "input_tokens")
        _whole(output_tokens, "output_tokens")
        numerator = (
            input_tokens * input_micro_cny_per_million
            + output_tokens * output_micro_cny_per_million
        )
        return numerator // 1_000_000


def _caps(scope: BudgetScope) -> Charge:
    return Charge(
        calls=scope.calls,
        input_tokens=scope.input_tokens,
        output_tokens=scope.output_tokens,
        tool_calls=scope.tool_calls,
        storage_bytes=scope.storage_bytes,
        wall_seconds=scope.wall_seconds,
        micro_cny=scope.micro_cny,
    )


def _dimension_max(left: Charge, right: Charge) -> Charge:
    merged = {n: max(getattr(left, n), getattr(right, n)) for n in DIMENSIONS}
    return Charge(**merged)


def scope_chain_summary(ledger: BudgetLedger, scope_id: str) -> Sequence[dict[str, Any]]:
    return [ledger.usage(sid) for sid in ledger._chain_ids(scope_id)]


def new_effect_id(prefix: str = "effect") -> str:
    return f"{prefix}_{uuid.uuid4().hex[:20]}"
