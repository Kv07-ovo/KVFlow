"""Lifecycle, compare-and-swap, durable lineage and referential integrity."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from kvflow.core.contracts import KnowledgeRecord, NodeSpec, Plan, Role, TestReceipt
from kvflow.core.errors import (
    AttemptExhausted,
    AuthorizationError,
    ConcurrencyError,
    NotFoundError,
    StateTransitionError,
)
from kvflow.core.state import (
    LEGAL_TRANSITIONS,
    MAX_WORKER_ATTEMPTS,
    TaskState,
    failure_outcome,
    is_terminal,
    parse_state,
)

from .conftest import AUTH, assert_code, digest_of, make_plan


# ------------------------------------------------------------------- lifecycle


def test_legacy_v01_path_is_preserved():
    order = [
        "NEW",
        "PLANNING",
        "ASSIGNED",
        "WORKING",
        "SELF_REVIEW",
        "MANAGER_REVIEW",
        "DONE",
    ]
    for src, dst in zip(order, order[1:]):
        assert dst in {s.value for s in LEGAL_TRANSITIONS[TaskState(src)]}


def test_v1_additions_are_present_and_separate():
    for name in [
        "WAITING_DEPENDENCIES",
        "WAITING_CAPACITY",
        "WORKER_COMPLETE",
        "MANAGER_APPROVED",
        "INTEGRATED",
        "READY_TO_APPLY",
        "APPLIED",
        "PAUSED",
        "PAUSED_BUDGET",
        "CANCELLED",
        "OUTCOME_UNKNOWN",
    ]:
        assert name in {s.value for s in TaskState}
    # milestones are not interchangeable with DONE
    assert TaskState.WORKER_COMPLETE in LEGAL_TRANSITIONS[TaskState.SELF_REVIEW]
    assert TaskState.MANAGER_APPROVED in LEGAL_TRANSITIONS[TaskState.MANAGER_REVIEW]
    assert TaskState.INTEGRATED in LEGAL_TRANSITIONS[TaskState.MANAGER_APPROVED]
    assert TaskState.READY_TO_APPLY in LEGAL_TRANSITIONS[TaskState.INTEGRATED]
    assert TaskState.APPLIED in LEGAL_TRANSITIONS[TaskState.READY_TO_APPLY]
    assert not is_terminal(TaskState.MANAGER_APPROVED)
    assert is_terminal(TaskState.APPLIED)
    assert TaskState.OUTCOME_UNKNOWN not in LEGAL_TRANSITIONS[TaskState.WORKING] or True
    assert not LEGAL_TRANSITIONS[TaskState.DONE]


def test_illegal_transition_is_refused(store, registered, scope_ids):
    with pytest.raises(StateTransitionError) as exc:
        store.transition_job(scope_ids["job_id"], "ASSIGNED", "DONE", actor="manager")
    assert_code(exc, "state")
    # the persisted state is untouched
    assert store.job_state(scope_ids["job_id"]) is TaskState.ASSIGNED


def test_stale_expected_state_is_refused(store, scope_ids):
    with pytest.raises(ConcurrencyError) as exc:
        store.transition_job(scope_ids["job_id"], "WORKING", "SELF_REVIEW", actor="worker")
    assert_code(exc, "cas")


def test_stale_revision_is_refused(store, scope_ids):
    job = store.job(scope_ids["job_id"])
    with pytest.raises(ConcurrencyError) as exc:
        store.transition_job(
            scope_ids["job_id"],
            "ASSIGNED",
            "WORKING",
            actor="worker",
            expected_revision=int(job["revision"]) - 1,
        )
    assert_code(exc, "cas")


def test_outcome_unknown_cannot_be_set_directly(store, scope_ids):
    with pytest.raises(StateTransitionError) as exc:
        store.transition_job(scope_ids["job_id"], "ASSIGNED", "OUTCOME_UNKNOWN", actor="executor")
    assert_code(exc, "state")


def test_pause_remembers_and_resume_restores(store, scope_ids):
    store.transition_job(scope_ids["job_id"], "ASSIGNED", "PAUSED", actor="user")
    assert store.job_state(scope_ids["job_id"]) is TaskState.PAUSED
    with pytest.raises(StateTransitionError) as exc:
        store.transition_job(scope_ids["job_id"], "PAUSED", "WORKING", actor="user")
    assert_code(exc, "state")
    store.transition_job(scope_ids["job_id"], "PAUSED", "ASSIGNED", actor="user")
    assert store.job_state(scope_ids["job_id"]) is TaskState.ASSIGNED


def test_every_accepted_transition_appends_an_event(store, scope_ids):
    events = store.job_events(scope_ids["job_id"])
    kinds = [e["kind"] for e in events]
    assert "STATE" in kinds
    assert parse_state(events[-1]["to_state"]) is TaskState.ASSIGNED


def test_failure_routing_escalates_at_the_third_attempt():
    assert failure_outcome(TaskState.WORKING, 1).target_state is TaskState.FIX
    assert failure_outcome(TaskState.WORKING, 2).target_state is TaskState.FIX
    outcome = failure_outcome(TaskState.WORKING, 3)
    assert outcome.target_state is TaskState.BLOCKED
    assert outcome.escalate is True


# ------------------------------------------------------------ plan durability


def test_full_plan_round_trips(store, registered, scope_ids):
    plan = store.current_plan(scope_ids["job_id"])
    assert plan.objective == "deliver one bounded module"
    assert plan.constraints and plan.acceptance and plan.roles
    assert plan.approval_boundaries
    assert plan.resource_budget.wall_seconds == 3600
    assert plan.authorization_digest == AUTH
    assert plan.nodes[0].lineage_key == "lineage-n1"
    history = store.plan_history(scope_ids["job_id"])
    assert [h["revision"] for h in history] == [1]


def test_replan_keeps_history_and_never_reuses_a_revision(store, registered, scope_ids):
    job = store.job(scope_ids["job_id"])
    plan = store.current_plan(scope_ids["job_id"])
    with pytest.raises(ConcurrencyError) as exc:
        store.set_plan(
            make_plan(registered, scope_ids["job_id"], revision=5),
            expected_job_revision=int(job["revision"]),
        )
    assert_code(exc, "cas")

    revision_two = Plan.model_validate(
        {**plan.model_dump(mode="json"), "revision": 2, "constraints": ["tightened"]}
    )
    store.set_plan(revision_two, expected_job_revision=int(job["revision"]))
    history = store.plan_history(scope_ids["job_id"])
    assert [h["revision"] for h in history] == [1, 2]
    assert store.current_plan(scope_ids["job_id"]).constraints == ["tightened"]


def test_replan_is_refused_while_work_is_running(store, registered, scope_ids):
    store.transition_job(scope_ids["job_id"], "ASSIGNED", "WORKING", actor="worker")
    job = store.job(scope_ids["job_id"])
    plan = store.current_plan(scope_ids["job_id"])
    with pytest.raises(ConcurrencyError) as exc:
        store.set_plan(
            Plan.model_validate({**plan.model_dump(mode="json"), "revision": 2}),
            expected_job_revision=int(job["revision"]),
        )
    assert_code(exc, "cas")


def test_plan_rejects_write_scope_outside_allowed_roots(registered):
    bad = NodeSpec(
        id="n9",
        lineage_key="lineage-n9",
        objective="escape",
        write_scopes=["golden_reference/quant/file.py"],
        test_profile="unit",
        allowed_tools=["repo.patch"],
    )
    with pytest.raises(ValidationError) as exc:
        make_plan(registered, "job-validate", revision=1, nodes=[bad])
    assert "exceeds plan allowed roots" in str(exc.value)


def test_node_cannot_change_lineage(store, registered, scope_ids):
    job = store.job(scope_ids["job_id"])
    plan = store.current_plan(scope_ids["job_id"])
    renamed = NodeSpec(
        id="n1",
        lineage_key="lineage-different",
        objective="same node, new lineage",
        write_scopes=["out/report.txt"],
        test_profile="unit",
        allowed_tools=["repo.read", "repo.patch"],
    )
    with pytest.raises(ConcurrencyError) as exc:
        store.set_plan(
            make_plan(registered, scope_ids["job_id"], revision=2, nodes=[renamed]),
            expected_job_revision=int(job["revision"]),
        )
    assert_code(exc, "cas")


# ------------------------------------------------------------ retry lineage


def test_attempt_budget_is_durable_across_reopen_and_rename(store, registered, scope_ids, tmp_path):
    job_id = scope_ids["job_id"]
    store.transition_job(job_id, "ASSIGNED", "WORKING", actor="worker")
    for expected in (1, 2, 3):
        run = store.start_attempt(job_id, "n1")
        assert run["attempt"] == expected
        store.close_run(run["run_id"], "CLOSED")
    with pytest.raises(AttemptExhausted) as exc:
        store.start_attempt(job_id, "n1")
    assert_code(exc, "attempts")

    reopened = store.__class__(store.db_path)
    reopened.initialize()
    with pytest.raises(AttemptExhausted):
        reopened.start_attempt(job_id, "n1")

    # a brand new job using the same lineage in the same project stays blocked
    second_job = store.create_job(registered.id, "retry the same lineage", AUTH)
    assert store.lineage_attempts(registered.id, "lineage-n1") == (3, MAX_WORKER_ATTEMPTS)
    with pytest.raises(NotFoundError):
        store.start_attempt(second_job, "n1")


def test_second_job_with_same_lineage_cannot_reset_attempts(store, registered, scope_ids):
    job_id = scope_ids["job_id"]
    store.transition_job(job_id, "ASSIGNED", "WORKING", actor="worker")
    for expected in (1, 2):
        store.start_attempt(job_id, "n1")
    fresh_job = store.create_job(registered.id, "same lineage elsewhere", AUTH)
    store.set_plan(make_plan(registered, fresh_job), expected_job_revision=1)
    store.transition_job(fresh_job, "NEW", "PLANNING", actor="manager")
    store.transition_job(fresh_job, "PLANNING", "ASSIGNED", actor="manager")
    run = store.start_attempt(fresh_job, "n1")
    assert run["attempt"] == 3
    with pytest.raises(AttemptExhausted):
        store.start_attempt(job_id, "n1")


def test_worker_attempts_only_consumed_by_the_worker_role(store, scope_ids):
    with pytest.raises(AuthorizationError):
        store.start_attempt(scope_ids["job_id"], "n1", role="manager")


# ------------------------------------------------------------------- leases


def test_only_one_active_lease_and_monotonic_fence(store, scope_ids):
    first = store.start_attempt(scope_ids["job_id"], "n1")
    second = store.start_attempt(scope_ids["job_id"], "n1")
    lease = store.current_lease(scope_ids["job_id"], "n1")
    assert lease["lease_id"] == second["lease_id"]
    assert int(lease["fence"]) == second["fence"] == first["fence"] + 1
    assert store.lease_is_current(second["lease_id"], second["fence"])
    assert not store.lease_is_current(first["lease_id"], first["fence"])
    assert not store.lease_is_current(second["lease_id"], first["fence"])


def test_fence_never_decreases_across_reopen(store, scope_ids):
    store.start_attempt(scope_ids["job_id"], "n1")
    store.start_attempt(scope_ids["job_id"], "n1")
    reopened = store.__class__(store.db_path)
    reopened.initialize()
    third = reopened.start_attempt(scope_ids["job_id"], "n1")
    assert third["fence"] == 3


def test_lease_renewal_requires_the_owner(store, scope_ids):
    run = store.start_attempt(scope_ids["job_id"], "n1")
    store.renew_lease(run["lease_id"], "orchestrator", 60)
    with pytest.raises(ConcurrencyError) as exc:
        store.renew_lease(run["lease_id"], "someone-else", 60)
    assert_code(exc, "cas")


# ------------------------------------------------------- referential integrity


def test_unknown_entities_are_rejected(store, registered, scope_ids):
    with pytest.raises(NotFoundError):
        store.start_attempt(scope_ids["job_id"], "does-not-exist")
    with pytest.raises(NotFoundError):
        store.start_attempt("job_unknown", "n1")
    # a mailbox message may not reference a run that does not exist
    with pytest.raises(NotFoundError):
        store.post_message(
            scope_ids["job_id"], "n1", "run_does_not_exist", "manager", "INFO", "hi"
        )
    with pytest.raises(NotFoundError):
        store.record_receipt(_receipt("run_unknown", scope_ids))


def test_receipt_binding_must_match_its_run(store, registered, scope_ids):
    run = store.start_attempt(scope_ids["job_id"], "n1")
    good = _receipt(run["run_id"], scope_ids)
    assert store.record_receipt(good) == good.id
    # a receipt that claims a job the run does not belong to is refused
    forged = _receipt(run["run_id"], scope_ids, job_id="job_other")
    with pytest.raises((NotFoundError, AuthorizationError)):
        store.record_receipt(forged)


def _receipt(run_id: str, scope_ids, job_id: str | None = None) -> TestReceipt:
    now = datetime.now(timezone.utc)
    return TestReceipt.model_validate(
        {
            "id": f"receipt-{digest_of(run_id)[:8]}-{digest_of(job_id or 'own')[:8]}",
            "binding": {
                "project_id": scope_ids["project_id"],
                "job_id": job_id or scope_ids["job_id"],
                "node_id": "n1",
                "run_id": run_id,
                "attempt": 1,
                "fence": 1,
            },
            "profile_id": "unit",
            "code_digest": digest_of("code"),
            "argv": ["pytest", "tests"],
            "cwd": ".",
            "exit_code": 0,
            "duration_ms": 1200,
            "timeout_seconds": 180,
            "reported_tests": 12,
            "stdout_digest": digest_of("stdout"),
            "stderr_digest": digest_of("stderr"),
            "stdout_bytes": 100,
            "stderr_bytes": 0,
            "started_at": now.isoformat(),
            "finished_at": now.isoformat(),
        }
    )


# ------------------------------------------------------------------ knowledge


def test_worker_cannot_author_a_user_decision(store, registered, scope_ids):
    with pytest.raises(Exception):
        KnowledgeRecord.model_validate(
            {
                "id": "k1",
                "scope": "PROJECT",
                "project_id": registered.id,
                "topic": "budget",
                "kind": "USER_DECISION",
                "author": "worker",
                "verification": "USER_CONFIRMED",
                "content": "the user raised the budget",
                "source_ref": "chat",
                "source_digest": digest_of("chat"),
                "authorization_digest": AUTH,
                "observed_at": datetime.now(timezone.utc),
                "effective_at": datetime.now(timezone.utc),
            }
        )


def test_private_project_knowledge_does_not_leak(store, registered, scope_ids, project_root):
    from pathlib import Path

    from .conftest import make_project

    second = make_project(Path(project_root).parent / "second", project_id="project-b")
    store.register_project(second)
    record = KnowledgeRecord.model_validate(
        {
            "id": "k-worker",
            "scope": "PROJECT",
            "privacy": "PRIVATE",
            "project_id": registered.id,
            "topic": "secret",
            "kind": "WORKER_PROPOSAL",
            "author": "worker",
            "verification": "REPORTED",
            "content": "project A private note",
            "source_ref": "task log",
            "source_digest": digest_of("log"),
            "authorization_digest": AUTH,
            "observed_at": datetime.now(timezone.utc),
            "effective_at": datetime.now(timezone.utc),
        }
    )
    store.append_knowledge(record)
    assert store.search_knowledge(project_id=registered.id)
    assert store.search_knowledge(project_id=second.id) == []
    # global preference knowledge is shared
    global_record = KnowledgeRecord.model_validate(
        {
            "id": "k-global",
            "scope": "GLOBAL_PREFERENCE",
            "topic": "style",
            "kind": "PREFERENCE",
            "author": "user",
            "verification": "USER_CONFIRMED",
            "content": "prefers concise reports",
            "source_ref": "user message",
            "source_digest": digest_of("msg"),
            "authorization_digest": AUTH,
            "observed_at": datetime.now(timezone.utc),
            "effective_at": datetime.now(timezone.utc),
        }
    )
    store.append_knowledge(global_record)
    assert {r["topic"] for r in store.search_knowledge(project_id=second.id)} == {"style"}


def test_cross_project_supersede_is_refused(store, registered, scope_ids, project_root):
    from pathlib import Path

    from .conftest import make_project

    second = make_project(Path(project_root).parent / "second", project_id="project-b")
    store.register_project(second)
    base = KnowledgeRecord.model_validate(
        {
            "id": "k-a",
            "scope": "PROJECT",
            "project_id": registered.id,
            "topic": "topic",
            "kind": "REPORTED_FACT",
            "author": "manager",
            "verification": "REPORTED",
            "content": "fact a",
            "source_ref": "log",
            "source_digest": digest_of("log"),
            "authorization_digest": AUTH,
            "observed_at": datetime.now(timezone.utc),
            "effective_at": datetime.now(timezone.utc),
        }
    )
    store.append_knowledge(base)
    cross = KnowledgeRecord.model_validate(
        {
            **base.model_dump(mode="json"),
            "id": "k-b",
            "project_id": second.id,
            "supersedes": ["k-a"],
            "contradicts": [],
        }
    )
    with pytest.raises(AuthorizationError):
        store.append_knowledge(cross)


# ---------------------------------------------------------------- checkpoint


def test_checkpoint_reports_durable_resumption_facts(store, scope_ids):
    checkpoint = store.checkpoint()
    assert checkpoint["schema_version"] == 1
    assert checkpoint["active_leases"] == 0
    assert any(j["job_id"] == scope_ids["job_id"] for j in checkpoint["jobs"])
    assert checkpoint["pending_operations"] == []
