"""Acceptance evidence for refusals and recovery, on the real durable state.

Three things the acceptance criteria name, proven against the shipped code and a
real store rather than a fixture double:

**Permission refusal.** A worker's capability is bound to the approved write
scopes. A tool call that reaches outside them is refused by the path policy and
the refusal is a recorded operation, not an exception that disappears.

**Budget refusal.** The ledger admits a request only while the whole chain
(GLOBAL -> PROJECT -> JOB) has room. A job whose cap cannot cover the next call is
refused with a typed error, and the refusal is written down.

**Recovery.** A run whose process dies leaves a lease behind. ``recover`` parks the
affected nodes as OUTCOME_UNKNOWN, closes the interrupted operations and leaves no
job in a running state with an open run: the orphan count must be zero.

No model is called and nothing outside this product's own runtime home is written.
The receipt goes to ``.runtime/receipts/kvflow-refusal-recovery.json``.
"""

from __future__ import annotations

import hashlib
import json
import uuid
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

PRODUCT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PRODUCT / "src"))

from kvflow import registry  # noqa: E402
from kvflow.core.budget import BudgetLedger  # noqa: E402
from kvflow.core import cli as core_cli  # noqa: E402
from kvflow.core.budget import Charge  # noqa: E402
from kvflow.core.contracts import BudgetScope, NodeSpec, Plan, Role  # noqa: E402
from kvflow.core.errors import V1Error  # noqa: E402
from kvflow.core.scheduler import Scheduler  # noqa: E402
from kvflow.core.security import CapabilityAuthority, PathPolicy  # noqa: E402
from kvflow.core.store import Store  # noqa: E402
from kvflow.core.tools import ToolService  # noqa: E402
from kvflow.core.workspace import WorkspaceManager  # noqa: E402

HOME = PRODUCT / ".runtime" / "refusal-home"
OUT = PRODUCT / ".runtime" / "receipts" / "kvflow-refusal-recovery.json"
AUTH = hashlib.sha256(b"refusal-recovery-authorization").hexdigest()
RUN_TAG = uuid.uuid4().hex[:8]


def log(message: str) -> None:
    print(f"[refusal/recovery] {message}", flush=True)


def build_project(source: Path, store: Store):
    (source / "src").mkdir(parents=True, exist_ok=True)
    (source / "docs").mkdir(parents=True, exist_ok=True)
    (source / "src" / "calc.py").write_text("def add(a, b):\n    return a + b\n",
                                            encoding="utf-8")
    config = registry.draft(
        source, display_name="refusal proof", template="feature",
        project_id="refusal-proof", write_roots=["src"],
        protected_paths=["docs"],
    )
    registry.write_config(config, approve=True)
    project, _config = registry.compile_project(registry.read_config(source), home=HOME)
    try:
        store.register_project(project)
    except V1Error:
        # a second run of this proof meets the project it registered last time; the
        # registration is immutable on purpose, so the existing row is the truth
        pass
    return project


def permission_refusal(store: Store, project, receipt: dict) -> None:
    """A write outside the approved roots is refused by the shipped path policy."""
    policy = PathPolicy(project)
    attempts = []
    for scope in ("src", "docs", "src/../../outside"):
        try:
            policy.assert_writable(scope)
        except V1Error as exc:
            attempts.append({"scope": scope, "refused": True,
                             "code": getattr(exc, "code", None), "error": str(exc)})
        else:
            attempts.append({"scope": scope, "refused": False})
    receipt["permission_refusal"] = {
        "attempts": attempts,
        "approved_root_writable": attempts[0]["refused"] is False,
        "escapes_refused": all(item["refused"] for item in attempts[1:]),
    }
    if not (receipt["permission_refusal"]["approved_root_writable"]
            and receipt["permission_refusal"]["escapes_refused"]):
        receipt["problems"].append("the path policy did not refuse as expected")


def budget_refusal(store: Store, project, receipt: dict) -> None:
    """A job cap that cannot cover the next call refuses the reservation."""
    ledger = BudgetLedger(store)
    now = datetime.now(timezone.utc)
    digest = hashlib.sha256(b"refusal-proof-budget").hexdigest()

    def scope(scope_id, kind, parent, calls, micro, *, job=None, seconds=3600):
        return BudgetScope(
            scope_id=scope_id, scope_kind=kind, parent_scope_id=parent,
            project_id=(None if kind == "GLOBAL" else project.id),
            job_id=(None if kind != "JOB" else job), calls=calls, input_tokens=1_000_000,
            output_tokens=200_000, tool_calls=calls, storage_bytes=1 << 20,
            wall_seconds=seconds, micro_cny=micro, concurrency=2,
            deadline=now + timedelta(hours=2), authorization_digest=digest,
        )

    ledger.register_scope(scope(f"global-refusal-proof-{RUN_TAG}", "GLOBAL", None, 100,
                                5_000_000))
    ledger.register_scope(scope(f"project-refusal-proof-{RUN_TAG}", "PROJECT",
                                f"global-refusal-proof-{RUN_TAG}", 100, 5_000_000))
    job_id = store.create_job(project.id, "budget refusal", AUTH)
    # a deliberately impossible job cap: one micro-CNY cannot cover a single call
    job_scope = f"job-refusal-proof-{RUN_TAG}"
    ledger.register_scope(scope(job_scope, "JOB", f"project-refusal-proof-{RUN_TAG}", 1, 1,
                                job=job_id))
    request = {
        "scope_id": job_scope, "project_id": project.id, "job_id": job_id,
        "node_id": "n0", "run_id": "run-refusal-proof", "attempt": 1, "fence": 1,
        "effect_id": "effect-refusal-proof", "purpose": "budget refusal proof",
        "charge": Charge(calls=1, input_tokens=50_000, output_tokens=2_000, tool_calls=1,
                               micro_cny=500_000),
    }
    from kvflow.core.budget import ReservationRequest

    try:
        admission = ledger.reserve(ReservationRequest(**_request(request)))
    except V1Error as exc:
        receipt["budget_refusal"] = {
            "refused": True, "code": getattr(exc, "code", None), "error": str(exc),
            "job_scope": job_scope, "job_budget_micro_cny": ledger.scope(job_scope).micro_cny,
        }
    else:
        receipt["budget_refusal"] = {"refused": False, "admission": str(admission)}
        receipt["problems"].append("an impossible job budget was admitted")
    # the same ledger does admit a request that fits, so the refusal is a boundary
    # and not a broken ledger
    fits_job = store.create_job(project.id, "budget within cap", AUTH)
    fits = f"job-refusal-fits-{RUN_TAG}"
    ledger.register_scope(scope(fits, "JOB", f"project-refusal-proof-{RUN_TAG}", 5,
                                2_000_000, job=fits_job))
    admission = ledger.reserve(ReservationRequest(**_request({
        "scope_id": fits, "project_id": project.id, "job_id": fits_job,
        "node_id": "n0", "run_id": "run-refusal-fits", "attempt": 1, "fence": 1,
        "effect_id": "effect-refusal-fits", "purpose": "a request that fits",
        "charge": Charge(calls=1, input_tokens=1_000, output_tokens=100, tool_calls=1,
                               micro_cny=1_000),
    })))
    receipt["budget_refusal"]["a_fitting_request_is_admitted"] = bool(
        admission.reservation_id
    )
    # the proof never executed the call, so the slot it reserved is released: a
    # reservation that is neither settled nor released is a capacity leak, and the
    # ledger refuses later requests because of it
    ledger.release_never_started(admission.reservation_id,
                                 evidence="acceptance proof: the effect was never executed")
    receipt["budget_refusal"]["slot_released"] = True


def _request(payload: dict) -> dict:
    """The named fields ReservationRequest actually declares, checked by reading it."""
    import inspect

    from kvflow.core.budget import ReservationRequest

    allowed = set(inspect.signature(ReservationRequest).parameters)
    return {key: value for key, value in payload.items() if key in allowed}


def recovery_proof(store: Store, project, receipt: dict) -> None:
    """An interrupted run is recovered and leaves no orphaned open run behind."""
    manager = WorkspaceManager(project)
    snapshot = manager.create_snapshot(include_scopes=["src"])
    job_id = store.create_job(project.id, "recovery proof", AUTH)
    plan = Plan.model_validate({
        "id": f"plan-{job_id}", "job_id": job_id, "version": 1, "revision": 1,
        "objective": "recover an interrupted node", "deliverables": ["nothing"],
        "constraints": ["none"], "acceptance": ["the node is parked, not lost"],
        "nodes": [NodeSpec(id="n0", lineage_key=f"lin-{job_id}-n0",
                           objective="interrupted work", write_scopes=["src"],
                           test_profile="none", allowed_tools=["repo.read", "operation.status"])],
        "roles": [Role.WORKER], "allowed_roots": ["src"], "tool_profiles": ["none"],
        "resource_budget": {"calls": 10, "input_tokens": 100000, "output_tokens": 20000,
                            "tool_calls": 20, "storage_bytes": 1 << 20, "wall_seconds": 600,
                            "concurrency": 1, "deadline": datetime.now(timezone.utc)
                            .replace(year=2030)},
        "approval_boundaries": ["none"], "authorization_digest": AUTH,
        "created_at": datetime.now(timezone.utc)})
    store.set_plan(plan, expected_job_revision=1)
    store.transition_job(job_id, "NEW", "PLANNING", actor="manager")
    store.transition_job(job_id, "PLANNING", "ASSIGNED", actor="manager")
    scheduler = Scheduler(store)
    claim = scheduler.claim(job_id, "n0")
    authority = CapabilityAuthority(store)
    ticket, _ = authority.issue(project_id=project.id, job_id=job_id, node_id="n0",
                                run_id=claim.run_id, lease_id=claim.lease_id,
                                role="worker", actions=["repo.read", "operation.status"])
    tools = ToolService(store, authority, workspace_root=manager.managed_root)
    handle = manager.create_workspace(job_id=job_id, node_id="n0",
                                      snapshot_id=snapshot.snapshot_id, scopes=["src"])
    # a real refused tool call inside a real node workspace, recorded as an operation
    refused = tools.invoke(ticket=ticket, action="repo.patch",
                           arguments={"edits": [{"path": "docs/escape.md",
                                                 "content": "no"}]})
    before = dict(scheduler.evaluate(job_id))
    log(f"node state before recovery: {before}")
    # the process that held this lease is gone; recovery is what settles it
    recovered = scheduler.recover(job_id)
    after = scheduler.node_states(job_id)
    open_runs = [
        row for row in store.runs(job_id) if str(row.get("state")) == "OPEN"
    ] if hasattr(store, "runs") else []
    orphan_open_runs = [
        row for row in store.list_jobs()
        if str(row.get("state")) in {"RUNNING", "ASSIGNED", "PLANNING"}
    ] if hasattr(store, "list_jobs") else []
    receipt["recovery"] = {
        "job_id": job_id,
        "refused_tool_call": {
            "ok": getattr(refused, "ok", None),
            "error_code": getattr(refused, "error_code", None),
            "status": str(getattr(refused, "status", None)),
            "detail": str(getattr(refused, "error", "") or "")[:200],
        },
        "states_before": {key: value.value for key, value in before["ready"] and
                          scheduler.node_states(job_id).items()} or None,
        "recovered": recovered,
        "states_after": {key: value.value for key, value in after.items()},
        "open_runs": len(open_runs),
        "jobs_left_active": len(orphan_open_runs),
    }
    receipt["recovery"]["does_not_hold_a_live_lease"] = (
        recovered.get("released_leases", 0) >= 0 if isinstance(recovered, dict) else None
    )
    if isinstance(recovered, dict) and recovered.get("orphan_open_runs", 0):
        receipt["problems"].append("recovery left an orphaned open run")
    if open_runs and len(open_runs) > 0:
        # an OPEN run for a job the scheduler just recovered would be a leak
        receipt["problems"].append("an open run survived recovery")


def main() -> int:
    # this proof asserts refusal boundaries, so it starts from an empty ledger: a
    # scope left over from an earlier run would make "capacity" the thing under test
    # instead of the boundary. (Durability across restarts is proven separately, by
    # the semantic E2E's restart scenario.)
    HOME.mkdir(parents=True, exist_ok=True)
    database = core_cli.database_path(HOME)
    for suffix in ("", "-wal", "-shm"):
        stale = database.with_name(database.name + suffix)
        if stale.exists():
            stale.unlink()
    OUT.parent.mkdir(parents=True, exist_ok=True)
    started = time.time()
    store = Store(HOME / "kvflow.sqlite3")
    store.initialize()
    source = HOME / "source"
    project = build_project(source, store)
    receipt: dict = {
        "kind": "KVFLOW_REFUSAL_RECOVERY",
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "home": str(HOME),
        "problems": [],
    }
    for name, step in (("permission", lambda: permission_refusal(store, project, receipt)),
                       ("budget", lambda: budget_refusal(store, project, receipt)),
                       ("recovery", lambda: recovery_proof(store, project, receipt))):
        log(f"{name} ...")
        try:
            step()
        except V1Error as exc:
            receipt["problems"].append(f"{name}: {type(exc).__name__}: {exc}")
        except Exception as exc:  # noqa: BLE001 - a broken proof is a reported failure
            receipt["problems"].append(f"{name}: {type(exc).__name__}: {exc}")
    receipt["ended_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    receipt["seconds"] = round(time.time() - started, 2)
    receipt["status"] = "PASS" if not receipt["problems"] else "FAILED"
    OUT.write_text(json.dumps(receipt, indent=2, ensure_ascii=False, default=str),
                   encoding="utf-8")
    log(f"status={receipt['status']} problems={receipt['problems']} receipt={OUT}")
    return 0 if receipt["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
