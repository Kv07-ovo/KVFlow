"""Durable DAG scheduler tests: dependencies, real overlap, fences, recovery.

The overlap test proves *actual* concurrent execution by having every node
record its own start and end monotonic time and asserting that at least two
intervals genuinely intersect, and that no more than three were ever in flight.
Nothing here is satisfied by printing role names in sequence.
"""

from __future__ import annotations

import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import pytest

from kvflow.core.contracts import NodeSpec, Plan, Role
from kvflow.core.errors import (
    AttemptExhausted,
    ConcurrencyError,
    ContractError,
    StateTransitionError,
)
from kvflow.core.scheduler import Scheduler, SUCCESS_STATES
from kvflow.core.state import TaskState
from kvflow.core.store import Store

from .conftest import AUTH, make_project

WINDOW = 0.12


def make_dag_plan(project, job_id: str, *, revision: int = 1) -> Plan:
    nodes = [
        NodeSpec(
            id="a1",
            lineage_key="lin-a1",
            objective="independent branch one",
            write_scopes=["out/a1.txt"],
            test_profile="unit",
            allowed_tools=["repo.read", "repo.patch"],
        ),
        NodeSpec(
            id="a2",
            lineage_key="lin-a2",
            objective="independent branch two",
            write_scopes=["out/a2.txt"],
            test_profile="unit",
            allowed_tools=["repo.read", "repo.patch"],
        ),
        NodeSpec(
            id="a3",
            lineage_key="lin-a3",
            objective="independent branch three",
            write_scopes=["out/a3.txt"],
            test_profile="unit",
            allowed_tools=["repo.read", "repo.patch"],
        ),
        NodeSpec(
            id="merge",
            lineage_key="lin-merge",
            objective="dependent integration node",
            dependencies=["a1", "a2", "a3"],
            write_scopes=["out/merged.txt"],
            test_profile="unit",
            allowed_tools=["repo.read", "repo.patch"],
        ),
    ]
    return Plan.model_validate(
        {
            "id": f"plan-{job_id}",
            "job_id": job_id,
            "version": 1,
            "revision": revision,
            "objective": "three independent branches then one dependent node",
            "deliverables": ["three outputs", "one merged output"],
            "constraints": ["each node writes only its own file"],
            "acceptance": ["all four nodes reach WORKER_COMPLETE"],
            "nodes": nodes,
            "roles": [Role.WORKER, Role.MANAGER],
            "allowed_roots": ["out"],
            "tool_profiles": ["unit"],
            "resource_budget": {
                "calls": 40,
                "input_tokens": 200000,
                "output_tokens": 200000,
                "tool_calls": 200,
                "storage_bytes": 10_000_000,
                "wall_seconds": 3600,
                "concurrency": 3,
                "deadline": datetime.now(timezone.utc).replace(year=2030),
            },
            "approval_boundaries": ["no new budget"],
            "authorization_digest": AUTH,
            "created_at": datetime.now(timezone.utc),
        }
    )


@pytest.fixture()
def dag(store: Store, project_root: Path):
    project = make_project(project_root, project_id="dag-project")
    store.register_project(project)
    job_id = store.create_job(project.id, "run the DAG", AUTH)
    store.set_plan(make_dag_plan(project, job_id), expected_job_revision=1)
    store.transition_job(job_id, "NEW", "PLANNING", actor="manager")
    store.transition_job(job_id, "PLANNING", "ASSIGNED", actor="manager")
    return project, job_id


class OverlapRecorder:
    """Executor that records real intervals and executes nodes concurrently."""

    def __init__(self, *, sleep: float = WINDOW, fail: set[str] | None = None) -> None:
        self.sleep = sleep
        self.fail = fail or set()
        self.lock = threading.Lock()
        self.intervals: dict[str, tuple[float, float]] = {}
        self.starts: list[str] = []
        self.in_flight = 0
        self.peak = 0

    def __call__(self, claim):
        with self.lock:
            self.starts.append(claim.node_id)
            self.in_flight += 1
            self.peak = max(self.peak, self.in_flight)
            started = time.monotonic()
        try:
            time.sleep(self.sleep)
        finally:
            finished = time.monotonic()
            with self.lock:
                self.intervals[claim.node_id] = (started, finished)
                self.in_flight -= 1
        if claim.node_id in self.fail:
            return {"status": "FIX_REQUIRED", "reason": "injected failure"}
        return {"status": "WORKER_COMPLETE", "summary": f"{claim.node_id} done"}


def run_parallel(scheduler: Scheduler, job_id: str, executor, *, threads: int = 3,
                 rounds: int = 40):
    """Drive ``tick`` from several threads, which is how slots really fill."""
    reports = []
    lock = threading.Lock()

    def worker(index: int) -> None:
        for _ in range(rounds):
            report = scheduler.tick(job_id, executor, max_dispatch=1)
            with lock:
                reports.append(report)
            if not report.claimed and report.notes and "no node could progress" in report.notes[-1]:
                return

    pool = [threading.Thread(target=worker, args=(i,)) for i in range(threads)]
    for thread in pool:
        thread.start()
    for thread in pool:
        thread.join()
    return reports


# ---------------------------------------------------------------- structure


def test_evaluate_respects_dependencies(dag, store):
    project, job_id = dag
    scheduler = Scheduler(store)
    evaluation = scheduler.evaluate(job_id)
    assert sorted(evaluation["ready"]) == ["a1", "a2", "a3"]
    assert evaluation["waiting"] == ["merge"]


def test_dependent_node_waits_for_every_dependency(dag, store):
    project, job_id = dag
    scheduler = Scheduler(store)
    executor = OverlapRecorder(sleep=0.02)
    # one pass with one slot: only a1 can start, so merge is still waiting
    scheduler.tick(job_id, executor, max_dispatch=1)
    evaluation = scheduler.evaluate(job_id)
    assert "merge" not in evaluation["ready"]
    assert "merge" in evaluation["waiting"]
    # the remaining independent branches are still claimable
    assert set(evaluation["ready"]) == {"a2", "a3"}


def test_write_scope_conflict_is_serialized(dag, store):
    project, job_id = dag
    scheduler = Scheduler(store)
    conflicting = NodeSpec(
        id="b1",
        lineage_key="lin-b1",
        objective="same file as a1",
        write_scopes=["out/a1.txt"],
        test_profile="unit",
        allowed_tools=["repo.read", "repo.patch"],
    )
    plan = store.current_plan(job_id)
    store.set_plan(
        Plan.model_validate(
            {
                **plan.model_dump(mode="json"),
                "revision": 2,
                "nodes": [*plan.model_dump(mode="json")["nodes"], conflicting.model_dump(mode="json")],
            }
        ),
        expected_job_revision=int(store.job(job_id)["revision"]),
    )
    active = [
        scheduler.claim(job_id, "a1"),
    ]
    allow, held = scheduler.claimable(job_id, active)
    assert "b1" not in allow
    assert "b1" in held
    assert "a2" in allow


# ---------------------------------------------------------------- overlap


def test_three_workers_really_overlap_and_never_exceed_the_pool(dag, store):
    project, job_id = dag
    scheduler = Scheduler(store, worker_slots=3)
    executor = OverlapRecorder(sleep=WINDOW)
    run_parallel(scheduler, job_id, executor, threads=3)

    assert executor.peak <= 3, f"more workers ran at once than allowed: {executor.peak}"
    starts = {node: interval[0] for node, interval in executor.intervals.items()}
    finishes = {node: interval[1] for node, interval in executor.intervals.items()}
    overlapping = 0
    nodes = list(starts)
    for i in range(len(nodes)):
        for j in range(i + 1, len(nodes)):
            left, right = nodes[i], nodes[j]
            if starts[left] < finishes[right] and starts[right] < finishes[left]:
                overlapping += 1
    assert overlapping >= 1, (
        "no two node executions actually overlapped;"
        f" intervals were {sorted(executor.intervals.items())}"
    )
    assert len({round(v[0], 3) for v in executor.intervals.values() if v[0]}) >= 2


def test_dependent_node_runs_only_after_all_three(dag, store):
    project, job_id = dag
    scheduler = Scheduler(store, worker_slots=3)
    executor = OverlapRecorder(sleep=0.02)
    # finish the three independent branches sequentially so the only thing left
    # to prove is the dependency edge itself
    for node in ("a1", "a2", "a3"):
        claim = scheduler.claim(job_id, node)
        executor(claim)
        scheduler.complete(claim)
    states = scheduler.node_states(job_id)
    assert all(states[n] in SUCCESS_STATES for n in ("a1", "a2", "a3"))
    assert "merge" in scheduler.evaluate(job_id)["ready"]
    # the dependent node starts strictly after every dependency has finished
    scheduler.tick(job_id, executor, max_dispatch=1)
    assert scheduler.node_states(job_id)["merge"] in SUCCESS_STATES
    merge_start = executor.intervals["merge"][0]
    for node in ("a1", "a2", "a3"):
        assert executor.intervals[node][1] <= merge_start + 1e-6


def test_slot_pool_is_respected_even_with_many_threads(dag, store):
    project, job_id = dag
    scheduler = Scheduler(store, worker_slots=2)
    executor = OverlapRecorder(sleep=WINDOW)
    run_parallel(scheduler, job_id, executor, threads=6)
    assert executor.peak <= 2


# ---------------------------------------------------------------- failures


def test_retry_budget_is_durable_and_ends_blocked(dag, store):
    project, job_id = dag
    scheduler = Scheduler(store, worker_slots=1)
    failing = OverlapRecorder(sleep=0.01, fail={"a1"})
    for _ in range(6):
        report = scheduler.tick(job_id, failing, max_dispatch=1)
        if scheduler.node_states(job_id)["a1"] is TaskState.BLOCKED:
            break
    assert scheduler.node_states(job_id)["a1"] is TaskState.BLOCKED
    attempts, maximum = store.lineage_attempts(project.id, "lin-a1")
    assert attempts == maximum == 3


def test_exhausted_lineage_blocks_without_a_new_attempt(dag, store):
    project, job_id = dag
    scheduler = Scheduler(store, worker_slots=1)
    failing = OverlapRecorder(sleep=0.01, fail={"a2"})
    for _ in range(6):
        scheduler.tick(job_id, failing, max_dispatch=1)
    assert store.lineage_attempts(project.id, "lin-a2")[0] == 3
    with pytest.raises(AttemptExhausted):
        scheduler.claim(job_id, "a2")


def test_failed_dependency_blocks_dependents(dag, store):
    project, job_id = dag
    scheduler = Scheduler(store, worker_slots=3)
    failing = OverlapRecorder(sleep=0.01, fail={"a1"})
    for _ in range(8):
        scheduler.tick(job_id, failing, max_dispatch=3)
        if scheduler.node_states(job_id)["a1"] is TaskState.BLOCKED:
            break
    evaluation = scheduler.evaluate(job_id)
    assert "merge" in evaluation["failed"]


# ------------------------------------------------------ pause/cancel/recover


def test_pause_resume_round_trip(dag, store):
    project, job_id = dag
    scheduler = Scheduler(store)
    scheduler.pause(job_id)
    report = scheduler.tick(job_id, OverlapRecorder(sleep=0.01))
    assert report.claimed == []
    assert "paused" in report.notes[0]
    scheduler.resume(job_id)
    assert store.job_state(job_id) is TaskState.ASSIGNED
    assert scheduler.tick(job_id, OverlapRecorder(sleep=0.01), max_dispatch=1).claimed


def test_cancel_closes_runs_and_is_terminal(dag, store):
    project, job_id = dag
    scheduler = Scheduler(store)
    claim = scheduler.claim(job_id, "a1")
    result = scheduler.cancel(job_id, reason="user cancelled")
    assert result["state"] == "CANCELLED"
    assert claim.run_id in result["closed_runs"]
    assert store.job_state(job_id) is TaskState.CANCELLED
    assert scheduler.tick(job_id, OverlapRecorder(sleep=0.01)).claimed == []
    again = scheduler.cancel(job_id)
    assert again["already_terminal"] is True


def test_a_cancelled_job_accepts_no_new_claim(dag, store):
    """A claim after cancel would open a run nothing can ever settle."""
    project, job_id = dag
    scheduler = Scheduler(store)
    scheduler.cancel(job_id, reason="user cancelled")
    with pytest.raises(StateTransitionError):
        scheduler.claim(job_id, "a1")
    assert store.pending_operations() == []
    assert store.checkpoint()["open_runs"] == 0


def test_a_paused_job_accepts_no_new_claim(dag, store):
    project, job_id = dag
    scheduler = Scheduler(store)
    scheduler.pause(job_id)
    with pytest.raises(StateTransitionError):
        scheduler.claim(job_id, "a1")


def test_a_refused_settlement_never_leaves_an_open_run(dag, store):
    """The node state moving underneath a claim must not strand the run."""
    project, job_id = dag
    scheduler = Scheduler(store)
    claim = scheduler.claim(job_id, "a1")
    # somebody else moved the node on; WORKER_COMPLETE is no longer reachable
    store.set_node_state(job_id, "a1", TaskState.WORKING, TaskState.FIX, actor="test")
    with pytest.raises(StateTransitionError):
        scheduler.complete(claim, summary="too late")
    with store.read() as conn:
        status = conn.execute(
            "SELECT status FROM runs WHERE run_id = ?", (claim.run_id,)
        ).fetchone()["status"]
    assert status != "OPEN"
    assert store.checkpoint()["open_runs"] == 0
    assert store.lease_is_current(claim.lease_id, claim.fence) is False


def test_a_refused_settlement_is_reported_as_a_failed_node(dag, store):
    project, job_id = dag
    scheduler = Scheduler(store, worker_slots=1)

    class Late:
        def __call__(self, claim):
            store.set_node_state(
                claim.job_id, claim.node_id, TaskState.WORKING, TaskState.FIX, actor="test"
            )
            return {"status": "WORKER_COMPLETE", "summary": "done too late"}

    report = scheduler.tick(job_id, Late(), max_dispatch=1)
    assert report.completed == []
    assert report.failed == ["a1"]
    assert any("settlement refused" in note for note in report.notes)
    assert store.checkpoint()["open_runs"] == 0


def test_recovery_releases_a_lapsed_lease_and_does_not_replay(dag, store):
    project, job_id = dag
    scheduler = Scheduler(store)
    claim = scheduler.claim(job_id, "a1", lease_seconds=1)
    store.clock.advance(2)
    report = scheduler.recover(job_id)
    assert claim.lease_id in report["expired_leases"]
    assert claim.run_id in report["recovered_runs"]
    assert report["unknown_runs"] == []
    # nothing was silently re-dispatched: the node is left claimable for a real
    # retry decision, and the dead lease is no longer current
    assert scheduler.node_states(job_id)["a1"] is TaskState.FIX
    assert store.lease_is_current(claim.lease_id, claim.fence) is False


def test_recovery_marks_unknown_when_an_operation_is_pending(dag, store):
    project, job_id = dag
    scheduler = Scheduler(store)
    claim = scheduler.claim(job_id, "a1", lease_seconds=1)
    store.open_operation(job_id, "a1", claim.run_id, project.id, "test.run_profile",
                         "a" * 64)
    store.clock.advance(2)
    report = scheduler.recover(job_id)
    assert claim.run_id in report["unknown_runs"]
    assert claim.run_id not in report["recovered_runs"]


def test_recovery_parks_an_interrupted_effect_and_never_replays_it(dag, store):
    """A crash with an effect in flight must not leave a stuck, claimable node."""
    project, job_id = dag
    scheduler = Scheduler(store)
    claim = scheduler.claim(job_id, "a1", lease_seconds=1)
    operation_id = store.open_operation(
        job_id, "a1", claim.run_id, project.id, "test.run_profile", "a" * 64
    )
    store.clock.advance(2)
    report = scheduler.recover(job_id)
    assert report["reconciled_nodes"]["a1"] == TaskState.OUTCOME_UNKNOWN.value
    assert scheduler.node_states(job_id)["a1"] is TaskState.OUTCOME_UNKNOWN
    # the interrupted effect is reported as unknown, not as a success or a
    # failure, and it stops counting as a pending operation
    assert store.operation(operation_id)["status"] == "OUTCOME_UNKNOWN"
    assert store.pending_operations() == []
    # the dead owner keeps no ownership, and the node is not silently retried
    assert store.lease_is_current(claim.lease_id, claim.fence) is False
    assert "a1" not in scheduler.evaluate(job_id)["ready"]
    with pytest.raises(StateTransitionError):
        scheduler.claim(job_id, "a1")
    # recovery is idempotent: nothing new is reported on a second pass
    assert scheduler.recover(job_id)["unknown_runs"] == []


def test_a_completed_node_cannot_be_reclaimed(dag, store):
    """A milestone is not a free retry: re-claiming it would duplicate work."""
    project, job_id = dag
    scheduler = Scheduler(store)
    claim = scheduler.claim(job_id, "a1")
    scheduler.complete(claim, summary="done")
    assert scheduler.node_states(job_id)["a1"] is TaskState.WORKER_COMPLETE
    with pytest.raises(StateTransitionError):
        scheduler.claim(job_id, "a1")
    assert scheduler.node_states(job_id)["a1"] is TaskState.WORKER_COMPLETE


def test_stale_fence_cannot_submit(dag, store):
    project, job_id = dag
    scheduler = Scheduler(store)
    first = scheduler.claim(job_id, "a1")
    store.close_run(first.run_id, "CLOSED")
    second = scheduler.claim(job_id, "a1")
    assert second.fence == first.fence + 1
    assert store.lease_is_current(first.lease_id, first.fence) is False
    assert store.lease_is_current(second.lease_id, second.fence) is True


def test_double_claim_of_a_running_node_is_refused(dag, store):
    project, job_id = dag
    scheduler = Scheduler(store)
    scheduler.claim(job_id, "a1")
    with pytest.raises(ConcurrencyError):
        scheduler.claim(job_id, "a1")


def test_worker_slot_configuration_is_validated(store):
    with pytest.raises(ContractError):
        Scheduler(store, worker_slots=4)
    with pytest.raises(ContractError):
        Scheduler(store, worker_slots=0)


def test_completion_requires_the_full_milestone_sequence(dag, store):
    project, job_id = dag
    scheduler = Scheduler(store)
    claim = scheduler.claim(job_id, "a1")
    scheduler.complete(claim, summary="done")
    assert scheduler.node_states(job_id)["a1"] is TaskState.WORKER_COMPLETE
    with pytest.raises(StateTransitionError):
        store.set_node_state(
            job_id, "a1", TaskState.WORKER_COMPLETE, TaskState.NEW, actor="manager"
        )
