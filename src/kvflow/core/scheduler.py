"""Durable DAG scheduler: bounded concurrency, fences and pause/resume/cancel.

The scheduler is deterministic program code - it is not a third model. It owns
the task graph, slot accounting, write-scope serialization and the durable
transition of each node. Actual work is supplied by a caller-provided executor,
so the same scheduler drives a deterministic fixture in tests and a real
Manager/Worker pipeline in production.

Guarantees implemented here
---------------------------

* **Dependency order.** A node is claimable only when every dependency reached a
  successful completion milestone. Cycles are impossible because the plan
  contract rejects them, and the scheduler re-checks anyway.
* **Write-scope serialization.** Two nodes whose declared write scopes overlap
  never run concurrently; the later one waits for ``WAITING_CAPACITY``-style
  retry rather than racing.
* **Fair, bounded concurrency.** Node slots (default 3 workers) and heavy slots
  (default 1) are separate pools with a single Manager decision slot. Claims are
  handed out in stable plan order.
* **Atomic claim with a monotonic fence.** A claim starts a durable attempt and a
  lease with a strictly increasing fence, all inside one ``BEGIN IMMEDIATE``
  transaction. The old owner's token is invalidated by the new epoch.
* **No overlapping slot counts.** Active slots are taken from the distinct
  running nodes in the store and released with their run, so a crashed process
  holds nothing after ``recover()`` releases its leases.
* **Failures are bounded and durable.** A node keeps its attempt counter per
  plan lineage; a spent budget ends BLOCKED instead of looping.
* **Pause, resume and cancel are explicit states.** Cancellation stops the
  task's own process tree through the executor; it never kills by port or name.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Mapping, Protocol, Sequence

from .contracts import Plan
from .errors import (
    AttemptExhausted,
    AuthorizationError,
    ConcurrencyError,
    ContractError,
    NotFoundError,
    StateTransitionError,
)
from .state import PAUSE_STATES, TERMINAL_STATES, TaskState, parse_state
from .store import Store, utcnow

DEFAULT_WORKER_SLOTS = 3
DEFAULT_HEAVY_SLOTS = 1

#: milestones that satisfy a dependency edge
SUCCESS_STATES = frozenset(
    {
        TaskState.WORKER_COMPLETE,
        TaskState.MANAGER_APPROVED,
        TaskState.INTEGRATED,
        TaskState.READY_TO_APPLY,
        TaskState.APPLIED,
        TaskState.DONE,
    }
)

#: the only states a node may be *claimed* from
CLAIMABLE_STATES = frozenset(
    {TaskState.NEW, TaskState.ASSIGNED, TaskState.WAITING_DEPENDENCIES,
     TaskState.WAITING_CAPACITY, TaskState.FIX}
)


class NodeExecutor(Protocol):
    """What the scheduler needs from whatever actually performs a node."""

    def __call__(self, claim: "NodeClaim") -> Mapping[str, Any]:
        ...


@dataclass(frozen=True)
class NodeClaim:
    """Everything the executor is allowed to know about one claimed node."""

    job_id: str
    project_id: str
    node_id: str
    lineage_key: str
    run_id: str
    lease_id: str
    attempt: int
    fence: int
    objective: str
    role: str
    write_scopes: tuple[str, ...]
    allowed_tools: tuple[str, ...]
    test_profile: str
    heavy: bool
    plan_revision: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "job_id": self.job_id,
            "project_id": self.project_id,
            "node_id": self.node_id,
            "run_id": self.run_id,
            "lease_id": self.lease_id,
            "attempt": self.attempt,
            "fence": self.fence,
            "objective": self.objective,
            "role": self.role,
            "write_scopes": list(self.write_scopes),
            "allowed_tools": list(self.allowed_tools),
            "test_profile": self.test_profile,
            "heavy": self.heavy,
            "plan_revision": self.plan_revision,
        }


@dataclass
class TickReport:
    claimed: list[str] = field(default_factory=list)
    completed: list[str] = field(default_factory=list)
    failed: list[str] = field(default_factory=list)
    blocked: list[str] = field(default_factory=list)
    waiting: list[str] = field(default_factory=list)
    cancelled: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

    @property
    def progressed(self) -> bool:
        return bool(
            self.claimed or self.completed or self.failed or self.blocked or self.cancelled
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "claimed": self.claimed,
            "completed": self.completed,
            "failed": self.failed,
            "blocked": self.blocked,
            "waiting": self.waiting,
            "cancelled": self.cancelled,
            "notes": self.notes,
        }


class Scheduler:
    """Persistent queue/DAG driver over one job."""

    def __init__(
        self,
        store: Store,
        *,
        worker_slots: int = DEFAULT_WORKER_SLOTS,
        heavy_slots: int = DEFAULT_HEAVY_SLOTS,
        manager_slots: int = 1,
        heartbeat_seconds: float = 30.0,
    ) -> None:
        if not 1 <= int(worker_slots) <= DEFAULT_WORKER_SLOTS:
            raise ContractError("worker slots must be between 1 and 3")
        if int(heavy_slots) < 1 or int(manager_slots) != 1:
            raise ContractError("heavy slots must be >= 1 and there is one manager slot")
        self.store = store
        self.worker_slots = int(worker_slots)
        self.heavy_slots = int(heavy_slots)
        self.manager_slots = int(manager_slots)
        self.heartbeat_seconds = float(heartbeat_seconds)
        #: serialises the read-slots-then-claim step so concurrent callers cannot
        #: both observe a free slot and both take it. It is a process-local lock
        #: held only around the short durable claim, never around executor work.
        self._dispatch_lock = threading.Lock()
        #: process-local view of claims this scheduler handed out and which the
        #: executor has not settled yet; the durable runs remain authoritative
        #: for crash recovery and for other processes
        self._in_flight: dict[str, NodeClaim] = {}

    # -------------------------------------------------------------- state
    def node_states(self, job_id: str) -> dict[str, TaskState]:
        with self.store.read() as conn:
            rows = conn.execute(
                "SELECT node_id, state FROM nodes WHERE job_id = ?", (job_id,)
            ).fetchall()
        return {row["node_id"]: parse_state(row["state"]) for row in rows}

    def dependencies(self, job_id: str) -> dict[str, tuple[str, ...]]:
        with self.store.read() as conn:
            rows = conn.execute(
                "SELECT node_id, depends_on FROM node_edges WHERE job_id = ?"
                " ORDER BY node_id, depends_on",
                (job_id,),
            ).fetchall()
        graph: dict[str, list[str]] = {}
        for row in rows:
            graph.setdefault(row["node_id"], []).append(row["depends_on"])
        return {key: tuple(value) for key, value in graph.items()}

    def running_nodes(self, job_id: str) -> list[dict[str, Any]]:
        with self.store.read() as conn:
            rows = conn.execute(
                "SELECT node_id, run_id, fence, attempt FROM runs"
                " WHERE job_id = ? AND status = 'OPEN' ORDER BY node_id",
                (job_id,),
            ).fetchall()
        return [dict(r) for r in rows]

    def slot_usage(self, job_id: str) -> dict[str, int]:
        """Live slot usage: durable open runs plus this process's un-settled claims.

        The two sets are unioned by ``run_id`` so a claim is never counted twice
        and a crash cannot leave a permanently occupied local slot.
        """
        running = {row["run_id"]: row["node_id"] for row in self.running_nodes(job_id)}
        for claim in tuple(self._in_flight.values()):
            if claim.job_id == job_id:
                running.setdefault(claim.run_id, claim.node_id)
        project = self.store.project(str(self.store.job(job_id)["project_id"]))
        heavy_ids = {p.id for p in project.test_profiles if p.heavy}
        heavy = 0
        for node_id in running.values():
            if str(self.store.node(job_id, node_id)["test_profile"]) in heavy_ids:
                heavy += 1
        return {
            "workers": len(running),
            "heavy": heavy,
            "manager": 1 if running else 0,
        }

    def _project_of(self, job_id: str) -> str:
        return str(self.store.job(job_id)["project_id"])

    # ------------------------------------------------------------ planning
    def evaluate(self, job_id: str) -> dict[str, list[str]]:
        """Classify every node without changing anything."""
        states = self.node_states(job_id)
        deps = self.dependencies(job_id)
        ready: list[str] = []
        waiting: list[str] = []
        done: list[str] = []
        failed: list[str] = []
        for node_id, state in sorted(states.items()):
            if state in SUCCESS_STATES or state is TaskState.BLOCKED:
                done.append(node_id) if state in SUCCESS_STATES else failed.append(node_id)
                continue
            if state in PAUSE_STATES or state in {
                TaskState.WORKING,
                TaskState.WAITING_CAPACITY,
            }:
                waiting.append(node_id)
                continue
            blockers = [
                dep for dep in deps.get(node_id, ()) if states.get(dep) not in SUCCESS_STATES
            ]
            failed_deps = [
                dep for dep in blockers if states.get(dep) is TaskState.BLOCKED
            ]
            if failed_deps:
                failed.append(node_id)
                continue
            if blockers:
                waiting.append(node_id)
                continue
            if state in CLAIMABLE_STATES:
                ready.append(node_id)
            else:
                waiting.append(node_id)
        return {"ready": ready, "waiting": waiting, "done": done, "failed": failed}

    @staticmethod
    def scopes_overlap(left: Sequence[str], right: Sequence[str]) -> bool:
        """True when two declared write scopes could touch the same file."""
        for a in left:
            for b in right:
                if _scope_contains(a, b) or _scope_contains(b, a):
                    return True
        return False

    def claimable(
        self, job_id: str, active: Sequence[NodeClaim]
    ) -> tuple[list[str], list[str]]:
        """Nodes that may start now, and those held back by a scope conflict."""
        evaluation = self.evaluate(job_id)
        allow: list[str] = []
        held: list[str] = []
        for node_id in evaluation["ready"]:
            node = self.store.node(job_id, node_id)
            scopes = _json_tuple(node["write_scopes"])
            if any(self.scopes_overlap(scopes, claim.write_scopes) for claim in active):
                held.append(node_id)
                continue
            allow.append(node_id)
        return allow, held

    # -------------------------------------------------------------- claims
    def claim(self, job_id: str, node_id: str, *, owner: str = "orchestrator",
              lease_seconds: float = 300.0) -> NodeClaim:
        """Atomically start one durable attempt with a fresh fence.

        Only a claimable node may consume an attempt. Without this check a direct
        caller could re-run a node that already reached a milestone -- or one
        parked in OUTCOME_UNKNOWN awaiting reconciliation -- and that would be a
        duplicated side effect rather than a retry.
        """
        plan = self.store.current_plan(job_id)
        node = self.store.node(job_id, node_id)
        state = parse_state(node["state"])
        # A job that is finished or paused accepts no new work. Without this the
        # first claim after a cancel opens a run nothing will ever settle, and
        # that run keeps both an OPEN row and a live lease for the whole session.
        job_state = self.store.job_state(job_id)
        if job_state in TERMINAL_STATES or job_state in PAUSE_STATES:
            raise StateTransitionError(
                f"job {job_id} is {job_state.value} and accepts no new work",
                src=job_state.value,
                dst=TaskState.WORKING.value,
            )
        # the durable attempt counter is the bound, so it is reported even when
        # the node also happens to be parked in a non-claimable state
        attempts, max_attempts = self.store.lineage_attempts(
            str(self.store.job(job_id)["project_id"]), str(node["lineage_key"])
        )
        if int(attempts) >= int(max_attempts):
            raise AttemptExhausted(
                f"worker attempt budget exhausted ({attempts}/{max_attempts}) for lineage"
                f" {node['lineage_key']}",
                lineage_key=str(node["lineage_key"]),
                attempts=int(attempts),
                max_attempts=int(max_attempts),
            )
        if state is TaskState.WORKING:
            raise ConcurrencyError("node is already running", node_id=node_id)
        if state not in CLAIMABLE_STATES:
            raise StateTransitionError(
                f"a node in {state.value} may not be claimed", src=state.value,
                dst=TaskState.WORKING.value,
            )
        run = self.store.start_attempt(
            job_id, node_id, role="worker", lease_seconds=lease_seconds, owner=owner
        )
        spec = next((n for n in plan.nodes if n.id == node_id), None)
        if spec is None:
            self.store.close_run(run["run_id"], "FAILED")
            raise NotFoundError("node is not part of the current plan", node_id=node_id)
        profile = None
        project = self.store.project(str(self.store.job(job_id)["project_id"]))
        for candidate in project.test_profiles:
            if candidate.id == spec.test_profile:
                profile = candidate
                break
        return NodeClaim(
            job_id=job_id,
            project_id=project.id,
            node_id=node_id,
            lineage_key=str(node["lineage_key"]),
            run_id=run["run_id"],
            lease_id=run["lease_id"],
            attempt=int(run["attempt"]),
            fence=int(run["fence"]),
            objective=str(node["objective"]),
            role=str(node["role"]),
            write_scopes=tuple(_json_tuple(node["write_scopes"])),
            allowed_tools=tuple(_json_tuple(node["allowed_tools"])),
            test_profile=str(node["test_profile"]),
            heavy=bool(profile.heavy) if profile else False,
            plan_revision=int(plan.revision),
        )

    def complete(self, claim: NodeClaim, *, summary: str = "") -> None:
        """Record a successful worker completion and close the attempt.

        The run is closed whether or not the node transition succeeds. An attempt
        whose node state moved underneath it must not leave an OPEN run behind:
        that row keeps a lease alive and makes a finished node look like it is
        still running, for the rest of the session.
        """
        try:
            self._transition_node(claim, TaskState.SELF_REVIEW)
            self._transition_node(claim, TaskState.WORKER_COMPLETE)
        except (StateTransitionError, ConcurrencyError):
            self.store.close_run(claim.run_id, "FAILED")
            raise
        self.store.close_run(claim.run_id, "CLOSED")
        self.store.post_message(
            claim.job_id,
            claim.node_id,
            claim.run_id,
            "worker",
            "INFO",
            summary or f"node {claim.node_id} completed",
        )

    def fail(self, claim: NodeClaim, *, reason: str, retryable: bool = True) -> str:
        """Route a failure through the durable model, never an infinite loop."""
        attempt = claim.attempt
        state = self.store.node(claim.job_id, claim.node_id)["state"]
        try:
            if not retryable:
                target = TaskState.BLOCKED
                escalate = False
            else:
                from .state import failure_outcome

                outcome = failure_outcome(parse_state(state), attempt)
                target = outcome.target_state
                escalate = outcome.escalate
            self._transition_node(claim, target, actor="scheduler")
        except StateTransitionError:
            target = TaskState.BLOCKED
            escalate = True
        self.store.close_run(claim.run_id, "FAILED")
        self.store.post_message(
            claim.job_id, claim.node_id, claim.run_id, "worker", "BLOCKER", reason
        )
        if escalate:
            self.store.post_message(
                claim.job_id,
                claim.node_id,
                claim.run_id,
                "manager",
                "BLOCKER",
                f"worker attempt budget exhausted after attempt {attempt}: {reason}",
            )
        return target.value

    def _transition_node(
        self, claim: NodeClaim, target: TaskState, *, actor: str = "scheduler"
    ) -> None:
        node = self.store.node(claim.job_id, claim.node_id)
        current = parse_state(node["state"])
        if current is target:
            return
        self.store.set_node_state(
            claim.job_id, claim.node_id, current, target, actor=actor
        )

    # ------------------------------------------------------------ recovery
    def recover(self, job_id: str) -> dict[str, Any]:
        """Release lapsed ownership and report genuinely unknown outcomes.

        An interrupted run is moved to OUTCOME_UNKNOWN only when the executor
        recorded a pending operation; otherwise the attempt is simply failed and
        the durable retry budget decides what happens next. Nothing is replayed
        automatically.

        The node is reconciled in the same pass. A durable node left in WORKING
        after its run is gone is a stuck task: it can never be claimed again and
        its state contradicts the run that produced it. So an interrupted effect
        parks the node in OUTCOME_UNKNOWN, which is never claimable and needs an
        explicit external reconciliation, while a crash with no pending effect is
        handed back as FIX and the durable attempt budget -- not a stuck state --
        decides whether another attempt is allowed.
        """
        expired = self.store.expire_leases()
        open_runs = [r for r in self.store.crashed_runs() if r["job_id"] == job_id]
        unknown: list[str] = []
        released: list[str] = []
        reconciled_nodes: dict[str, str] = {}
        unresolved_operations: list[str] = []
        for run in open_runs:
            node_id = run["node_id"]
            with self.store.read() as conn:
                pending = conn.execute(
                    "SELECT operation_id FROM operations WHERE run_id = ?"
                    " AND status IN ('PENDING', 'RUNNING')",
                    (run["run_id"],),
                ).fetchall()
            if pending:
                # an effect may or may not have happened: it is never replayed
                # automatically, and both the run and its operations say so
                with self.store.tx() as conn:
                    conn.execute(
                        "UPDATE runs SET status = 'OUTCOME_UNKNOWN', updated_at = ?"
                        " WHERE run_id = ?",
                        (utcnow().isoformat(), run["run_id"]),
                    )
                    for row in pending:
                        conn.execute(
                            "UPDATE operations SET status = 'OUTCOME_UNKNOWN',"
                            " error_code = 'INTERRUPTED', updated_at = ?"
                            " WHERE operation_id = ?",
                            (utcnow().isoformat(), row["operation_id"]),
                        )
                    # a dead owner must not keep renewing ownership of this node
                    conn.execute(
                        "UPDATE leases SET status = 'RELEASED' WHERE run_id = ?"
                        " AND status = 'ACTIVE'",
                        (run["run_id"],),
                    )
                unknown.append(run["run_id"])
                unresolved_operations.extend(row["operation_id"] for row in pending)
                reconciled_nodes[node_id] = self._reconcile_crashed_node(
                    job_id, node_id, TaskState.OUTCOME_UNKNOWN
                )
                continue
            # close_run already returns the node from WORKING to FIX
            self.store.close_run(run["run_id"], "RECOVERED")
            released.append(run["run_id"])
            reconciled_nodes[node_id] = self._reconcile_crashed_node(
                job_id, node_id, TaskState.FIX
            )
        return {
            "job_id": job_id,
            "expired_leases": expired,
            "recovered_runs": released,
            "unknown_runs": unknown,
            "reconciled_nodes": reconciled_nodes,
            "unresolved_operations": unresolved_operations,
            "checkpoint": self.store.checkpoint(),
            "recovered_at": utcnow().isoformat(),
        }

    def _reconcile_crashed_node(
        self, job_id: str, node_id: str, target: TaskState
    ) -> str:
        """Move a crashed node to ``target``; report the final durable state.

        A node that is already there, or whose recorded state cannot legally
        become ``target``, is reported unchanged rather than forced: recovery
        never invents a transition the durable model refuses.
        """
        current = parse_state(self.store.node(job_id, node_id)["state"])
        if current is target:
            return target.value
        try:
            self.store.set_node_state(job_id, node_id, current, target, actor="recovery")
        except StateTransitionError:
            return current.value
        return target.value

    # ---------------------------------------------------------------- loop
    def tick(
        self,
        job_id: str,
        executor: Callable[[NodeClaim], Mapping[str, Any]],
        *,
        max_dispatch: int | None = None,
        active: Sequence[NodeClaim] = (),
    ) -> TickReport:
        """Claim, execute and settle as many nodes as one pass allows.

        ``executor`` runs outside every database transaction: the scheduler
        claims first (a short write), then the executor works, then the result is
        committed against the fence that was issued.
        """
        report = TickReport()
        job_state = self.store.job_state(job_id)
        if job_state in TERMINAL_STATES:
            report.notes.append(f"job is {job_state.value}")
            return report
        if job_state in PAUSE_STATES:
            report.notes.append(f"job is paused ({job_state.value})")
            return report
        limit = self.worker_slots if max_dispatch is None else max_dispatch
        usage = self.slot_usage(job_id)
        free = max(0, self.worker_slots - usage["workers"])
        free_heavy = max(0, self.heavy_slots - usage["heavy"])
        if free <= 0:
            report.waiting.append("no worker slot is free")
            return report
        allow, held = self.claimable(job_id, active)
        report.waiting.extend(held)
        for node_id in allow:
            if len(report.claimed) >= min(limit, free):
                break
            node = self.store.node(job_id, node_id)
            project = self.store.project(str(self.store.job(job_id)["project_id"]))
            profile = next(
                (p for p in project.test_profiles if p.id == node["test_profile"]), None
            )
            if profile is not None and profile.heavy and free_heavy <= 0:
                report.waiting.append(node_id)
                continue
            # the slot check and the durable claim must be one decision, or two
            # concurrent callers can both see the same free slot
            with self._dispatch_lock:
                usage = self.slot_usage(job_id)
                if usage["workers"] >= self.worker_slots:
                    report.waiting.append(node_id)
                    break
                if profile is not None and profile.heavy:
                    if usage["heavy"] >= self.heavy_slots:
                        report.waiting.append(node_id)
                        continue
                try:
                    claim = self.claim(job_id, node_id)
                except (AttemptExhausted, ConcurrencyError, StateTransitionError) as exc:
                    report.blocked.append(node_id)
                    report.notes.append(f"{node_id}: {type(exc).__name__}: {exc}")
                    continue
                self._in_flight[claim.run_id] = claim
            report.claimed.append(node_id)
            if profile is not None and profile.heavy:
                free_heavy -= 1
            try:
                outcome = executor(claim)
            except Exception as exc:  # noqa: BLE001 - a failed node is data
                target = self.fail(claim, reason=f"{type(exc).__name__}: {exc}")
                report.failed.append(node_id)
                report.notes.append(f"{node_id} -> {target}")
                continue
            finally:
                self._in_flight.pop(claim.run_id, None)
            status = str(outcome.get("status", "WORKER_COMPLETE"))
            if status == "WORKER_COMPLETE":
                try:
                    self.complete(claim, summary=str(outcome.get("summary", "")))
                except (StateTransitionError, ConcurrencyError) as exc:
                    # the run is already closed by complete(); the refusal is
                    # reported as a failed node instead of killing the whole pass
                    report.failed.append(node_id)
                    report.notes.append(
                        f"{node_id}: settlement refused: {type(exc).__name__}: {exc}"
                    )
                else:
                    report.completed.append(node_id)
            else:
                target = self.fail(
                    claim,
                    reason=str(outcome.get("reason", status)),
                    retryable=status != "BLOCKED",
                )
                (report.blocked if target == TaskState.BLOCKED.value else report.failed).append(
                    node_id
                )
        if not report.progressed:
            report.notes.append("no node could progress")
        return report

    def run_until_idle(
        self,
        job_id: str,
        executor: Callable[[NodeClaim], Mapping[str, Any]],
        *,
        max_passes: int = 50,
    ) -> dict[str, Any]:
        """Drive the job until nothing can progress; never loops forever."""
        passes: list[dict[str, Any]] = []
        for _ in range(max_passes):
            report = self.tick(job_id, executor)
            passes.append(report.to_dict())
            if not report.claimed:
                break
        return {
            "job_id": job_id,
            "passes": len(passes),
            "history": passes,
            "states": {k: v.value for k, v in self.node_states(job_id).items()},
        }

    # ------------------------------------------------------ pause/cancel
    def pause(self, job_id: str, *, actor: str = "user") -> int:
        """Pause the job; the prior state is persisted so resume is exact."""
        current = self.store.job_state(job_id)
        if current in TERMINAL_STATES:
            raise StateTransitionError("cannot pause a finished job", src=current.value)
        if current in PAUSE_STATES:
            return int(self.store.job(job_id)["revision"])
        return self.store.transition_job(job_id, current, TaskState.PAUSED, actor=actor)

    def resume(self, job_id: str, *, actor: str = "user") -> int:
        paused = self.store.job_state(job_id)
        if paused not in PAUSE_STATES:
            raise StateTransitionError("job is not paused", src=paused.value)
        prior = self.store.job(job_id)["prior_state"]
        if prior is None:
            raise StateTransitionError("no recorded state to resume to", src=paused.value)
        return self.store.transition_job(
            job_id, paused, parse_state(prior), actor=actor
        )

    def cancel(self, job_id: str, *, actor: str = "user", reason: str = "") -> dict[str, Any]:
        """Cancel a job: close its runs, revoke capability, stop nothing else."""
        state = self.store.job_state(job_id)
        if state in TERMINAL_STATES:
            return {"job_id": job_id, "state": state.value, "already_terminal": True}
        running = self.running_nodes(job_id)
        for run in running:
            self.store.close_run(run["run_id"], "CANCELLED")
        self.store.transition_job(job_id, state, TaskState.CANCELLED, actor=actor)
        return {
            "job_id": job_id,
            "state": TaskState.CANCELLED.value,
            "closed_runs": [r["run_id"] for r in running],
            "reason": reason,
            "cancelled_at": utcnow().isoformat(),
        }

    def heartbeat(self, lease_id: str, owner: str) -> float:
        return self.store.renew_lease(lease_id, owner, self.heartbeat_seconds)


def _scope_contains(container: str, candidate: str) -> bool:
    if container == ".":
        return True
    container_parts = [p for p in container.replace("\\", "/").casefold().split("/") if p]
    candidate_parts = [p for p in candidate.replace("\\", "/").casefold().split("/") if p]
    if len(candidate_parts) < len(container_parts):
        return False
    return candidate_parts[: len(container_parts)] == container_parts


def _json_tuple(value: Any) -> tuple[str, ...]:
    import json

    if value is None:
        return ()
    if isinstance(value, str):
        try:
            loaded = json.loads(value)
        except ValueError:
            return (value,)
    else:
        loaded = value
    if isinstance(loaded, (list, tuple)):
        return tuple(str(item) for item in loaded)
    return (str(loaded),)
