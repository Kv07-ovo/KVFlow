"""Lifecycle, durable retry lineage and failure routing for v1.

The accepted v0.1 lifecycle is preserved verbatim as a *subsequence* of this
table (``NEW -> PLANNING -> ASSIGNED -> WORKING -> SELF_REVIEW ->
MANAGER_REVIEW -> DONE/FIX/BLOCKED``) and the v1 additions are additive:
dependency/capacity waiting, user pause, cancellation, genuinely unknown
external outcomes, and the separated completion milestones
``WORKER_COMPLETE`` / ``MANAGER_APPROVED`` / ``INTEGRATED`` /
``READY_TO_APPLY`` / ``APPLIED``. Those milestones are never interchangeable
with ``DONE``.

Two rules that the v1 store must enforce and this module only describes:

* a state change is only legal if the persisted current state equals the
  caller's expected state (compare-and-swap), and
* ``PAUSED``/``PAUSED_BUDGET`` are entered with a persisted *prior* state and
  may only ever resume to that recorded state.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from .errors import StateTransitionError

#: initial worker attempt plus at most two repairs, per durable lineage
MAX_WORKER_ATTEMPTS = 3

#: independent, separately accounted retry dimensions
RETRY_DIMENSIONS = ("model_network", "tool_infrastructure", "worker_repair", "experiment")


class TaskState(str, Enum):
    NEW = "NEW"
    PLANNING = "PLANNING"
    ASSIGNED = "ASSIGNED"
    WAITING_DEPENDENCIES = "WAITING_DEPENDENCIES"
    WAITING_CAPACITY = "WAITING_CAPACITY"
    WORKING = "WORKING"
    SELF_REVIEW = "SELF_REVIEW"
    WORKER_COMPLETE = "WORKER_COMPLETE"
    MANAGER_REVIEW = "MANAGER_REVIEW"
    MANAGER_APPROVED = "MANAGER_APPROVED"
    INTEGRATED = "INTEGRATED"
    READY_TO_APPLY = "READY_TO_APPLY"
    APPLIED = "APPLIED"
    DONE = "DONE"
    FIX = "FIX"
    BLOCKED = "BLOCKED"
    PAUSED = "PAUSED"
    PAUSED_BUDGET = "PAUSED_BUDGET"
    CANCELLED = "CANCELLED"
    OUTCOME_UNKNOWN = "OUTCOME_UNKNOWN"


#: v0.1 name kept so legacy callers keep working without editing frozen source
State = TaskState

LEGAL_TRANSITIONS: dict[TaskState, frozenset[TaskState]] = {
    TaskState.NEW: frozenset({TaskState.PLANNING, TaskState.BLOCKED, TaskState.CANCELLED}),
    # ASSIGNED -> PLANNING lets a task that lost its plan re-run real planning
    # rather than inventing one; there is no implicit planner in any code path.
    TaskState.PLANNING: frozenset(
        {TaskState.ASSIGNED, TaskState.FIX, TaskState.BLOCKED, TaskState.CANCELLED}
    ),
    TaskState.ASSIGNED: frozenset(
        {
            TaskState.PLANNING,
            TaskState.WORKING,
            TaskState.WAITING_DEPENDENCIES,
            TaskState.WAITING_CAPACITY,
            TaskState.FIX,
            TaskState.BLOCKED,
            TaskState.CANCELLED,
        }
    ),
    TaskState.WAITING_DEPENDENCIES: frozenset(
        {TaskState.ASSIGNED, TaskState.BLOCKED, TaskState.CANCELLED}
    ),
    TaskState.WAITING_CAPACITY: frozenset(
        {TaskState.ASSIGNED, TaskState.WORKING, TaskState.BLOCKED, TaskState.CANCELLED}
    ),
    TaskState.WORKING: frozenset(
        {
            TaskState.SELF_REVIEW,
            TaskState.WORKER_COMPLETE,
            TaskState.FIX,
            TaskState.BLOCKED,
            TaskState.OUTCOME_UNKNOWN,
            TaskState.CANCELLED,
        }
    ),
    TaskState.SELF_REVIEW: frozenset(
        {
            TaskState.WORKER_COMPLETE,
            TaskState.MANAGER_REVIEW,
            TaskState.FIX,
            TaskState.BLOCKED,
            TaskState.CANCELLED,
        }
    ),
    TaskState.WORKER_COMPLETE: frozenset(
        {TaskState.MANAGER_REVIEW, TaskState.CANCELLED}
    ),
    TaskState.MANAGER_REVIEW: frozenset(
        {
            TaskState.MANAGER_APPROVED,
            TaskState.DONE,
            TaskState.FIX,
            TaskState.BLOCKED,
            TaskState.CANCELLED,
        }
    ),
    TaskState.MANAGER_APPROVED: frozenset(
        {
            TaskState.INTEGRATED,
            TaskState.READY_TO_APPLY,
            TaskState.DONE,
            TaskState.FIX,
            TaskState.CANCELLED,
        }
    ),
    TaskState.INTEGRATED: frozenset({TaskState.READY_TO_APPLY, TaskState.FIX, TaskState.CANCELLED}),
    TaskState.READY_TO_APPLY: frozenset({TaskState.APPLIED, TaskState.FIX, TaskState.CANCELLED}),
    TaskState.APPLIED: frozenset({TaskState.DONE}),
    TaskState.FIX: frozenset(
        {TaskState.ASSIGNED, TaskState.WORKING, TaskState.BLOCKED, TaskState.CANCELLED}
    ),
    TaskState.DONE: frozenset(),
    TaskState.BLOCKED: frozenset(),
    # pause/resume: RESUME is handled by the store against the persisted prior
    # state, so these sets list the states a paused task may legally resume to.
    TaskState.PAUSED: frozenset(
        {
            TaskState.PLANNING,
            TaskState.ASSIGNED,
            TaskState.WAITING_DEPENDENCIES,
            TaskState.WAITING_CAPACITY,
            TaskState.WORKING,
            TaskState.SELF_REVIEW,
            TaskState.WORKER_COMPLETE,
            TaskState.MANAGER_REVIEW,
            TaskState.FIX,
        }
    ),
    TaskState.PAUSED_BUDGET: frozenset(
        {
            TaskState.PLANNING,
            TaskState.ASSIGNED,
            TaskState.WAITING_DEPENDENCIES,
            TaskState.WAITING_CAPACITY,
            TaskState.WORKING,
            TaskState.SELF_REVIEW,
            TaskState.WORKER_COMPLETE,
            TaskState.MANAGER_REVIEW,
            TaskState.FIX,
        }
    ),
    TaskState.CANCELLED: frozenset(),
    TaskState.OUTCOME_UNKNOWN: frozenset(),
}

TERMINAL_STATES = frozenset(
    {TaskState.DONE, TaskState.BLOCKED, TaskState.CANCELLED, TaskState.APPLIED}
)

#: a task in one of these states holds a durable, user-visible pause
PAUSE_STATES = frozenset({TaskState.PAUSED, TaskState.PAUSED_BUDGET})

#: requires an explicit external-effect reconciliation, never an implicit retry
RECONCILIATION_STATES = frozenset({TaskState.OUTCOME_UNKNOWN})

#: states from which a worker attempt consumes one unit of the retry budget
ATTEMPT_STATES = frozenset({TaskState.WORKING})

#: states a crash recovery may find a task in
ACTIVE_STATES = frozenset(
    {
        TaskState.PLANNING,
        TaskState.ASSIGNED,
        TaskState.WAITING_DEPENDENCIES,
        TaskState.WAITING_CAPACITY,
        TaskState.WORKING,
        TaskState.SELF_REVIEW,
        TaskState.WORKER_COMPLETE,
        TaskState.MANAGER_REVIEW,
        TaskState.MANAGER_APPROVED,
        TaskState.INTEGRATED,
        TaskState.READY_TO_APPLY,
        TaskState.FIX,
    }
)

#: only the worker role may produce file changes
CODING_ROLES = frozenset({"worker"})

#: terminal-but-approved milestones that are *not* interchangeable with DONE
MILESTONE_STATES = frozenset(
    {
        TaskState.WORKER_COMPLETE,
        TaskState.MANAGER_APPROVED,
        TaskState.INTEGRATED,
        TaskState.READY_TO_APPLY,
        TaskState.APPLIED,
    }
)


def parse_state(value: str | TaskState) -> TaskState:
    if isinstance(value, TaskState):
        return value
    try:
        return TaskState(value)
    except ValueError as exc:
        raise StateTransitionError(f"unknown state: {value!r}", src=str(value)) from exc


def is_terminal(state: str | TaskState) -> bool:
    return parse_state(state) in TERMINAL_STATES


def is_paused(state: str | TaskState) -> bool:
    return parse_state(state) in PAUSE_STATES


def can_transition(src: str | TaskState, dst: str | TaskState) -> bool:
    return parse_state(dst) in LEGAL_TRANSITIONS[parse_state(src)]


def assert_transition(
    src: str | TaskState, dst: str | TaskState, actor: str = "scheduler"
) -> None:
    src_s, dst_s = parse_state(src), parse_state(dst)
    if dst_s not in LEGAL_TRANSITIONS[src_s]:
        raise StateTransitionError(
            f"illegal transition {src_s.value} -> {dst_s.value}",
            src=src_s.value,
            dst=dst_s.value,
            actor=actor,
        )


def assert_role_can_code(role: str) -> None:
    """Manager never codes; there is no routine coding route to Manager."""
    if role not in CODING_ROLES:
        raise StateTransitionError(
            f"role {role!r} may not produce file changes",
            role=role,
            allowed=sorted(CODING_ROLES),
        )


@dataclass(frozen=True)
class FailureOutcome:
    target_state: TaskState
    escalate: bool
    reason: str


def failure_outcome(
    state: str | TaskState,
    attempt: int,
    max_attempts: int = MAX_WORKER_ATTEMPTS,
) -> FailureOutcome:
    """Route a detected failure to FIX, or escalate when the budget is spent."""
    src = parse_state(state)
    if src in TERMINAL_STATES:
        raise StateTransitionError(f"cannot route failure from terminal state {src.value}")
    if attempt >= max_attempts:
        return FailureOutcome(
            TaskState.BLOCKED,
            True,
            f"worker attempt budget exhausted ({attempt}/{max_attempts})",
        )
    if src is TaskState.FIX:
        return FailureOutcome(TaskState.ASSIGNED, False, "retry permitted")
    if src in (TaskState.NEW, TaskState.PLANNING):
        return FailureOutcome(TaskState.BLOCKED, False, f"unrecoverable error in {src.value}")
    if src is TaskState.ASSIGNED:
        return FailureOutcome(TaskState.WORKING, False, "worker not started yet; retry permitted")
    return FailureOutcome(TaskState.FIX, False, "retry permitted")


def recovery_target(state: str | TaskState) -> TaskState:
    """Where a crashed task goes so a fresh attempt can be assigned."""
    src = parse_state(state)
    if src in PAUSE_STATES:
        return src
    if src in ACTIVE_STATES:
        return TaskState.FIX
    return src


def attempts_remaining(attempt: int, max_attempts: int = MAX_WORKER_ATTEMPTS) -> int:
    return max(0, max_attempts - int(attempt))
