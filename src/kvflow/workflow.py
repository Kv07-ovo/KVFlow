"""The workflow runner: one requirement in, durable evidence out.

This is the single place where a workflow happens. The CLI, the MCP server, the
background runner process and the DSH plugin all call into it, so they cannot
drift into four different orchestrators: they read and write the same durable
store.

The work is split so a long run can outlive the caller:

``prepare_run``   resolve the approved project, template, model and budget
                  profile; create the durable job; compile and validate the plan;
                  register the budget chain; write the run manifest. It returns
                  the job identity without running anything.
``execute_run``   drive the persisted job: dependency waves with real concurrency,
                  one owned workspace and one capability ticket per node, the
                  executor receipts the workers wrote, the manager review gate,
                  integration of approved bytes, and project knowledge. It can be
                  called by any process, at any time, for a job that exists.
``run_requirement`` is the convenience path: prepare, then execute in this process.

Nothing is replayed: a node that failed is reported as failed, its attempt is
consumed durably, and an unknown outcome stays unknown.
"""

from __future__ import annotations

import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from pathlib import Path
from typing import Any, Mapping, Sequence

from . import agents, model_profiles, planner, registry, templates
from .core.budget import BudgetLedger, BudgetScope
from .core.contracts import Project, content_digest
from .core.errors import AuthorizationError, ContractError, NotFoundError, V1Error
from .core.knowledge import KnowledgeQuery, KnowledgeService
from .core.mcp_server import TOOL_SPECS
from .core.scheduler import Scheduler
from .core.security import CapabilityAuthority
from .core.state import TaskState
from .core.store import Store, utcnow
from .core.tools import ToolService
from .core.worker import WorkerLoop
from .core.workspace import WorkspaceManager, source_digest

GLOBAL_SCOPE = "kvflow-global"
#: every run is bounded twice: by the profile's caps and by this wall-clock ceiling
MAX_RUN_SECONDS = 3_600.0
#: one initial implementation pass plus at most this many manager-directed reworks,
#: matching the durable worker attempt budget (first attempt + two reworks)
MAX_FIX_ROUNDS = 2
RUNS_DIR = "runs"


# --------------------------------------------------------------- run manifest


def runs_dir(home: str | Path) -> Path:
    path = Path(home).expanduser() / RUNS_DIR
    path.mkdir(parents=True, exist_ok=True)
    return path


def run_manifest(home: str | Path, job_id: str) -> dict:
    path = runs_dir(home) / f"{job_id}.json"
    if not path.is_file():
        raise NotFoundError("unknown workflow run", job_id=job_id)
    return json.loads(path.read_text(encoding="utf-8"))


def _write_manifest(home: str | Path, job_id: str, payload: dict) -> None:
    """Write a run manifest atomically, even when two writers race for it.

    Two entrances legitimately write the same manifest: the host plugin records the
    launched run while the runner records its outcome. A single fixed temp name made
    the loser of that race fail with ``[WinError 5] access denied`` on Windows, so
    each writer now uses its own temp name and the replace is retried a bounded
    number of times for a transient sharing violation. A real failure after the last
    attempt is raised, never swallowed.
    """
    import os
    import threading
    import time as _time

    path = runs_dir(home) / f"{job_id}.json"
    body = json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True)
    temporary = path.with_suffix(
        f".json.{os.getpid()}.{threading.get_ident() % 100000}.tmp"
    )
    temporary.write_text(body, encoding="utf-8")
    last: OSError | None = None
    for attempt in range(1, 6):
        try:
            temporary.replace(path)
            return
        except OSError as exc:  # a sharing violation is transient on Windows
            last = exc
            if getattr(exc, "winerror", None) not in (5, 32, 145) and attempt >= 3:
                break
            _time.sleep(min(0.8, 0.05 * (2 ** attempt)))
    try:
        temporary.unlink(missing_ok=True)
    except OSError:  # pragma: no cover - best effort
        pass
    if last is not None:
        raise last


def update_manifest(home: str | Path, job_id: str, **fields: Any) -> dict:
    """Merge fields into one run manifest; this is how a pid or receipt is recorded."""
    manifest = run_manifest(home, job_id)
    manifest.update(fields)
    _write_manifest(home, job_id, manifest)
    return manifest


# ------------------------------------------------------------------- scopes


def config_digest(config: registry.ProjectConfig) -> str:
    return registry.authorization_digest(config)


def _scopes(ledger: BudgetLedger, config: registry.ProjectConfig,
            job_id: str, *, home: str | Path) -> dict[str, str]:
    """Register (idempotently) the global -> project -> job budget chain."""
    budgets = registry.load_budgets(home)
    profile = registry.budget_profile(home, config.budget_profile)
    global_ceiling = registry.global_ceiling(budgets, config.budget_profile)
    deadline = utcnow() + timedelta(days=1)

    def missing(scope_id: str) -> bool:
        try:
            ledger.scope(scope_id)
        except NotFoundError:
            return True
        return False

    if missing(GLOBAL_SCOPE):
        ledger.register_scope(
            BudgetScope(
                scope_id=GLOBAL_SCOPE, scope_kind="GLOBAL",
                calls=global_ceiling["calls"], input_tokens=global_ceiling["input_tokens"],
                output_tokens=global_ceiling["output_tokens"],
                tool_calls=global_ceiling["tool_calls"],
                storage_bytes=global_ceiling["storage_bytes"],
                wall_seconds=global_ceiling["wall_seconds"],
                micro_cny=global_ceiling["micro_cny"],
                concurrency=global_ceiling["concurrency"],
                deadline=deadline, authorization_digest=config_digest(config),
            )
        )
    project_scope = f"project-{config.project_id}"
    if missing(project_scope):
        ledger.register_scope(
            BudgetScope(
                scope_id=project_scope, scope_kind="PROJECT", parent_scope_id=GLOBAL_SCOPE,
                project_id=config.project_id,
                calls=global_ceiling["calls"], input_tokens=global_ceiling["input_tokens"],
                output_tokens=global_ceiling["output_tokens"],
                tool_calls=global_ceiling["tool_calls"],
                storage_bytes=global_ceiling["storage_bytes"],
                wall_seconds=global_ceiling["wall_seconds"],
                micro_cny=global_ceiling["micro_cny"],
                concurrency=global_ceiling["concurrency"],
                deadline=deadline, authorization_digest=config_digest(config),
            )
        )
    job_scope = f"job-{job_id}"
    if missing(job_scope):
        # idempotent on purpose: a resumed run, a second runner process or any
        # re-entry must reuse the scope it already registered, because a scope's
        # caps and deadline are frozen once they exist
        ledger.register_scope(
            BudgetScope(
                scope_id=job_scope, scope_kind="JOB", parent_scope_id=project_scope,
                project_id=config.project_id, job_id=job_id,
                calls=max(2, int(profile["calls"])), input_tokens=int(profile["input_tokens"]),
                output_tokens=int(profile["output_tokens"]),
                tool_calls=int(profile["tool_calls"]),
                storage_bytes=int(profile["storage_bytes"]),
                wall_seconds=min(int(profile["wall_seconds"]), int(MAX_RUN_SECONDS)),
                micro_cny=int(profile["micro_cny"]),
                concurrency=min(3, int(profile["concurrency"])),
                deadline=registry.deadline_from(profile),
                authorization_digest=config_digest(config),
            )
        )
    return {"global": GLOBAL_SCOPE, "project": project_scope, "job": job_scope}


# ------------------------------------------------------------------ resolve


def _resolve(home: Path, project_id: str) -> tuple[registry.ProjectConfig, Project]:
    index = registry.Registry(home)
    entry = index.get(project_id)
    config = registry.read_config(entry["canonical_root"])
    if not config.approved_by_user:
        raise AuthorizationError(
            "the project configuration has not been approved by the user",
            project_id=project_id,
            next=f"kvflow project onboard --path {config.canonical_root} --write",
        )
    project, _profiles = registry.compile_project(config, home=home)
    return config, project


def _store_for(home: Path) -> Store:
    """The one database every entrance opens (never a second, private state)."""
    from .core import cli as core_cli

    store = Store(core_cli.database_path(home))
    store.initialize()
    return store


def _plan_source_report(
    *, template: templates.WorkflowTemplate, config: registry.ProjectConfig,
    compiled: dict, job_id: str, home: Path, profile: model_profiles.ModelProfile,
    manager: Any | None, manager_error: str | None,
) -> tuple[Any, str, dict | None, str | None]:
    if template.plan != "manager":
        return planner.deterministic_from(compiled["plan"]), "deterministic_template", None, None
    if manager is None:
        return (planner.deterministic_from(compiled["plan"]), "deterministic_template", None,
                manager_error or "no manager is available for this profile")
    try:
        manager_plan = manager.build_plan(
            job_id=job_id, project=_PROJECT_HOLDER["project"],
            objective=_PLANNING["requirement"], allowed_roots=list(config.allowed_write_roots),
            resource_budget=compiled["plan"]["resource_budget"],
            authorization_digest=_PROJECT_HOLDER["project"].authorization_digest,
        )
        validated = planner.validate_manager_plan(
            json.loads(manager_plan.model_dump_json()), template=template, config=config
        )
        return validated, "live_manager", manager_plan.model_dump(mode="json"), None
    except V1Error as exc:
        return (planner.deterministic_from(compiled["plan"]), "deterministic_template", None,
                f"{type(exc).__name__}: {exc}")


# small explicit holders keep the planner call signature readable without a
# twelve-argument function
_PROJECT_HOLDER: dict[str, Any] = {}
_PLANNING: dict[str, Any] = {}


# ------------------------------------------------------------------ prepare


def prepare_run(
    *,
    requirement: str,
    project_id: str,
    home: str | Path,
    template_id: str | None = None,
    max_nodes: int | None = None,
    manager_factory: Any | None = None,
    manager_profile_override: str | None = None,
) -> dict[str, Any]:
    """Create the durable job, plan and budget chain for one requirement."""
    from .core import cli as core_cli

    text = (requirement or "").strip()
    if not text:
        raise ContractError("a requirement is required")
    paths = core_cli.runtime_paths(str(home))
    home_path = paths["home"]
    config, project = _resolve(home_path, project_id)
    template = templates.get(template_id or config.template, home=home_path)
    profile_id = manager_profile_override or config.model_profile
    profile = model_profiles.load(profile_id, home=home_path)
    store = _store_for(home_path)

    drift: str | None = None
    try:
        stored = store.project(project.id)
        if stored.authorization_digest != project.authorization_digest:
            drift = (
                "the project configuration differs from the registered authorization;"
                " this run uses the registered one"
            )
        project = stored
    except NotFoundError:
        store.register_project(project)

    job_id = store.create_job(project.id, f"[{template.id}] {text}",
                              project.authorization_digest)
    compiled = planner.compile_plan(requirement=text, template=template, config=config,
                                    job_id=job_id, home=home_path)
    _PROJECT_HOLDER["project"] = project
    _PLANNING["requirement"] = text

    manager = None
    manager_error: str | None = None
    if manager_factory is not None:
        manager = manager_factory()
    elif template.plan == "manager":
        try:
            manager, _identity = agents.build_manager(
                profile, home=home_path, cwd=config.canonical_root
            )
        except V1Error as exc:
            manager_error = f"{type(exc).__name__}: {exc}"
    plan, plan_source, manager_plan_document, refusal = _plan_source_report(
        template=template, config=config, compiled=compiled, job_id=job_id, home=home_path,
        profile=profile, manager=manager, manager_error=manager_error,
    )
    if max_nodes is not None and len(plan.nodes) > int(max_nodes):
        plan = planner.deterministic_from(compiled["plan"])
        plan_source = f"{plan_source}+trimmed"
    store.set_plan(plan, expected_job_revision=1)
    store.transition_job(job_id, "NEW", "PLANNING", actor="manager")
    store.transition_job(job_id, "PLANNING", "ASSIGNED", actor="manager")
    ledger = BudgetLedger(store)
    scope_ids = _scopes(ledger, config, job_id, home=home_path)

    manifest = {
        "job_id": job_id,
        "project_id": project.id,
        "display_name": config.display_name,
        "requirement": text,
        "template": template.id,
        "model_profile": profile.id,
        "budget_profile": config.budget_profile,
        "authorization_digest": project.authorization_digest,
        "plan_id": plan.id,
        "plan_digest": content_digest(plan),
        "plan_source": plan_source,
        "acceptance": list(plan.acceptance),
        "nodes": [node.id for node in plan.nodes],
        "waves": planner.parallel_groups(plan),
        "max_parallel_workers": template.max_parallel_workers,
        "scopes": scope_ids,
        "manager_plan": manager_plan_document,
        # planning is reported as its own fact: which planner produced the DAG, and
        # why a manager plan was not used. It is visible in every entrance without
        # being confused with the run's own outcome.
        "planning": {
            "source": plan_source,
            "manager_plan_used": manager_plan_document is not None,
            "fallback_reason": refusal,
        },
        "problems": [item for item in (drift,) if item],
        "created_at": utcnow().isoformat(),
    }
    _write_manifest(home_path, job_id, manifest)
    return manifest


# ------------------------------------------------------------------ execute


def _tool_specs_for(actions: Sequence[str]) -> list[dict[str, Any]]:
    """Only the tools this node may actually use are shown to the model."""
    allowed = set(actions)
    return [
        {"name": spec["name"], "description": spec["description"],
         "inputSchema": spec["inputSchema"]}
        for spec in TOOL_SPECS
        if spec["action"] in allowed
    ]


def _run_wave(
    *, node_ids: Sequence[str], plan: Any, project: Project,
    store: Store, scheduler: Scheduler, authority: CapabilityAuthority, tools: ToolService,
    ledger: BudgetLedger, scope_id: str, manager_ws: WorkspaceManager, snapshot: Any,
    handles: dict[str, Any], dependencies: Mapping[str, Sequence[str]], provider: Any,
    max_steps: int, max_completion_tokens: int, max_parallel: int, lock: threading.Lock,
    results: dict[str, dict[str, Any]],
    feedback: Mapping[str, str] | None = None,
) -> None:
    """Run one dependency wave: every node runs, at most ``max_parallel`` at once.

    ``feedback`` carries the manager's findings back into the objective of a node
    that is being reworked, so a retry is a repair attempt and not a repeat of the
    same prompt.
    """
    node_by_id = {node.id: node for node in plan.nodes}
    job_id = plan.job_id

    def work(node_id: str) -> None:
        node = node_by_id[node_id]
        objective = node.objective
        if feedback and feedback.get(node_id):
            objective = f"{objective}\n\n{feedback[node_id]}"
        started = time.monotonic()
        report: dict[str, Any] = {
            "node_id": node_id, "status": "EXECUTOR_ERROR",
            "started_at": utcnow().isoformat(), "started_monotonic": started,
        }
        try:
            base = None
            for dependency in dependencies.get(node_id, ()):
                if dependency in handles:
                    base = handles[dependency]
            if node_id in handles:
                # A rework re-dispatch keeps the tree the manager reviewed, so the
                # repair builds on the reviewed bytes instead of redoing the work.
                # A second workspace for the same node is refused by the store, and
                # a fresh tree would also throw away the diff under review.
                handle = handles[node_id]
            elif base is not None:
                handle = manager_ws.create_workspace_from(
                    job_id=job_id, node_id=node_id, base_handle=base,
                    scopes=list(node.write_scopes) or ["."],
                )
            else:
                handle = manager_ws.create_workspace(
                    job_id=job_id, node_id=node_id, snapshot_id=snapshot.snapshot_id,
                    scopes=list(node.write_scopes) or ["."],
                )
            with lock:
                handles[node_id] = handle
            claim = scheduler.claim(job_id, node_id)
            actions = [action.value for action in node.allowed_tools]
            ticket, _ = authority.issue(
                project_id=project.id, job_id=job_id, node_id=node_id, run_id=claim.run_id,
                lease_id=claim.lease_id, role="worker", actions=actions,
            )
            loop = WorkerLoop(
                provider=provider, tools=tools, ledger=ledger, ticket=ticket,
                binding={
                    "project_id": project.id, "job_id": job_id, "node_id": node_id,
                    "run_id": claim.run_id, "attempt": claim.attempt, "fence": claim.fence,
                },
                scope_id=scope_id,
                discovered_tools=_tool_specs_for(actions),
                max_steps=max_steps, max_completion_tokens=max_completion_tokens,
                max_seconds=900.0,
            )
            outcome = loop.run(objective)
            if outcome.status == "WORKER_COMPLETE":
                scheduler.complete(claim, summary=outcome.summary[:300])
            else:
                scheduler.fail(claim, reason=outcome.summary[:400],
                               retryable=outcome.status != "BLOCKED")
            report.update(
                {
                    "status": outcome.status, "summary": outcome.summary[:600],
                    "steps": outcome.steps, "tool_calls": outcome.tool_calls,
                    "live_calls": outcome.live_calls,
                    "receipt_ids": list(outcome.receipt_ids), "usage": dict(outcome.usage),
                    "notes": list(outcome.notes)[:12],
                }
            )
        except V1Error as exc:
            report["error"] = f"{type(exc).__name__}: {exc}"
            report["error_code"] = getattr(exc, "code", "ERROR")
        except Exception as exc:  # noqa: BLE001 - a failed node is data
            report["error"] = f"{type(exc).__name__}: {exc}"
        finally:
            report["finished_at"] = utcnow().isoformat()
            report["finished_monotonic"] = time.monotonic()
            report["seconds"] = round(report["finished_monotonic"] - started, 2)
            with lock:
                results[node_id] = report

    with ThreadPoolExecutor(max_workers=max(1, min(len(node_ids), int(max_parallel)))) as pool:
        list(pool.map(work, list(node_ids)))


def _source_intact(config: registry.ProjectConfig, snapshot: Any) -> bool:
    """The registered source tree still hashes to what the snapshot captured."""
    for entry in getattr(snapshot, "entries", ()):
        source = Path(config.canonical_root) / entry.relative_path
        if not source.is_file():
            return False
        digest, _size = source_digest(source)
        if digest != entry.sha256:
            return False
    return True


def execute_run(
    *,
    job_id: str,
    home: str | Path,
    max_steps: int = 10,
    max_completion_tokens: int = 1024,
    provider_factory: Any | None = None,
    manager_factory: Any | None = None,
) -> dict[str, Any]:
    """Drive one persisted run to its review and integration."""
    from .core import cli as core_cli

    paths = core_cli.runtime_paths(str(home))
    home_path = paths["home"]
    manifest = run_manifest(home_path, job_id)
    project_id = manifest["project_id"]
    config, project = _resolve(home_path, project_id)
    template = templates.get(manifest["template"], home=home_path)
    profile = model_profiles.load(manifest["model_profile"], home=home_path)
    store = _store_for(home_path)
    try:
        project = store.project(project.id)
    except NotFoundError:
        store.register_project(project)
    plan = store.current_plan(job_id)
    run: dict[str, Any] = {
        "kind": "KVFLOW_WORKFLOW",
        "product": "KVFlow",
        "job_id": job_id,
        "project_id": project_id,
        "display_name": manifest["display_name"],
        "requirement": manifest["requirement"],
        "template": template.id,
        "model_profile": profile.id,
        "plan_id": plan.id,
        "plan_source": manifest["plan_source"],
        "plan_digest": manifest["plan_digest"],
        "acceptance": list(plan.acceptance),
        "nodes": [node.id for node in plan.nodes],
        "waves": planner.parallel_groups(plan),
        "authorization_digest": project.authorization_digest,
        "manager_plan": manifest.get("manager_plan"),
        "planning": manifest.get("planning") or {"source": manifest["plan_source"]},
        "started_at": utcnow().isoformat(),
        "problems": list(manifest.get("problems", [])),
    }

    manager = None
    manager_error: str | None = None
    if manager_factory is not None:
        manager = manager_factory()
    elif template.review == "manager":
        try:
            manager, _identity = agents.build_manager(
                profile, home=home_path, cwd=config.canonical_root
            )
        except V1Error as exc:
            manager_error = f"{type(exc).__name__}: {exc}"

    manager_ws = WorkspaceManager(project)
    if "." in config.allowed_read_roots:
        snapshot_scopes = ["."]
    else:
        snapshot_scopes = list(config.allowed_read_roots)
        for scope in config.allowed_write_roots:
            if scope not in snapshot_scopes:
                snapshot_scopes.append(scope)
    snapshot = manager_ws.create_snapshot(include_scopes=snapshot_scopes or ["."],
                                          notes=f"kvflow {template.id}")
    run["snapshot_id"] = snapshot.snapshot_id

    ledger = BudgetLedger(store)
    scope_ids = manifest.get("scopes") or _scopes(ledger, config, job_id, home=home_path)
    scheduler = Scheduler(
        store,
        worker_slots=max(1, min(3, template.max_parallel_workers,
                                int(registry.budget_profile(
                                    home_path, config.budget_profile)["concurrency"]))),
    )
    authority = CapabilityAuthority(store)
    tools = ToolService(store, authority, workspace_root=manager_ws.managed_root)
    provider = (provider_factory() if provider_factory is not None
                else agents.build_worker_provider(profile))

    handles: dict[str, Any] = {}
    results: dict[str, dict[str, Any]] = {}
    lock = threading.Lock()
    dependencies = {node.id: tuple(node.dependencies) for node in plan.nodes}
    feedback: dict[str, str] = {}
    rounds: list[dict[str, Any]] = []
    verdict = None
    diff: dict[str, Any] = {"changed": [], "content_digest": ""}
    receipts: list[dict[str, Any]] = []

    # One initial pass plus at most MAX_FIX_ROUNDS reworks, which is the same bound
    # the durable attempt counter enforces. A node the manager sent back is
    # re-dispatched with the findings in its objective; a node that exhausted its
    # attempts is refused by the scheduler and the refusal is recorded.
    for round_index in range(0, MAX_FIX_ROUNDS + 1):
        if round_index == 0:
            waves = planner.parallel_groups(plan)
        else:
            ready = scheduler.evaluate(job_id)["ready"]
            if not ready:
                break
            waves = [ready]
        for wave in waves:
            _run_wave(
                node_ids=wave, plan=plan, project=project, store=store,
                scheduler=scheduler, authority=authority, tools=tools, ledger=ledger,
                scope_id=scope_ids["job"], manager_ws=manager_ws, snapshot=snapshot,
                handles=handles, dependencies=dependencies, provider=provider,
                max_steps=max_steps, max_completion_tokens=max_completion_tokens,
                max_parallel=template.max_parallel_workers, lock=lock, results=results,
                feedback=feedback,
            )
        rounds.append({
            "round": round_index,
            "nodes": list(wave) if waves else [],
            "states": {key: value.value for key, value in scheduler.node_states(job_id).items()},
        })

        latest_handle = handles.get(plan.nodes[-1].id) or next(iter(handles.values()), None)
        diff = manager_ws.workspace_diff(latest_handle) if latest_handle else {
            "changed": [], "content_digest": ""
        }
        for change in diff.get("changed", []):
            change.pop("base_sha256", None)
            path = (latest_handle.root / change["relative_path"]) if latest_handle else None
            if path is not None and path.is_file() and path.stat().st_size <= 200_000:
                body = path.read_text(encoding="utf-8", errors="replace")
                change["content_preview"] = body[:6000]
                change["content_bytes"] = len(body.encode("utf-8"))
        receipts = store.receipts(job_id)

        verdict = None
        if template.review == "manager" and manager is not None:
            try:
                verdict = manager.review(
                    objective=plan.objective, acceptance=list(plan.acceptance), diff=diff,
                    receipts=receipts, content_digest=diff.get("content_digest") or "",
                    # the workers' own result text travels with the receipts: for a
                    # read-only task it is the deliverable the manager has to judge
                    worker_reports=[results[key] for key in sorted(results)],
                )
                run["review"] = verdict.to_dict()
            except V1Error as exc:
                run["review"] = {"verdict": "REFUSED",
                                 "error": f"{type(exc).__name__}: {exc}"}
        elif template.review == "manager":
            run["review"] = {"verdict": "NOT_RUN",
                             "reason": manager_error or "no manager is available"}
        else:
            run["review"] = {"verdict": "NOT_REQUIRED", "reason": f"template {template.id}"}

        if verdict is not None and verdict.verdict == "APPROVE":
            # An approval decides the review, not the node lifecycle. A node whose
            # worker call ended OUTCOME_UNKNOWN is still claimable, and a run that
            # leaves a claimable node behind is not settled -- the run would be
            # integrated from a state the scheduler does not consider finished. So
            # the same bounded rework rounds are used to settle it first, and the
            # settled result is reviewed again before anything is integrated.
            unsettled = scheduler.evaluate(job_id)["ready"]
            if not unsettled or round_index >= MAX_FIX_ROUNDS:
                break
            feedback = {
                node_id: (
                    "The manager reviewed this node's earlier output and approved the"
                    " direction, but the node has no settled outcome: its last attempt"
                    " ended without completing (for example because a provider call"
                    " timed out). Finish this node and prove the result with the"
                    " registered profile; do not change work the manager already"
                    " accepted unless it is wrong."
                )
                for node_id in unsettled
            }
            continue
        if round_index >= MAX_FIX_ROUNDS:
            break
        if template.review != "manager":
            break
        states = scheduler.node_states(job_id)
        reviewed = [
            node.id for node in plan.nodes
            if states.get(node.id) in {TaskState.WORKER_COMPLETE, TaskState.FIX}
        ]
        _reopen_for_rework(store, job_id, reviewed)
        ready = scheduler.evaluate(job_id)["ready"]
        if not ready:
            break
        findings = list((run.get("review") or {}).get("findings") or [])[:10]
        feedback = {
            node_id: (
                "A previous attempt of this node was reviewed and sent back for rework."
                " Address exactly these findings, then prove the result with the"
                " registered profile:\n- " + "\n- ".join(findings)
            )
            for node_id in ready
        } if findings else {
            node_id: (
                "A previous attempt of this node was reviewed and sent back for rework."
                " Finish the outstanding work and prove it with the registered profile."
            )
            for node_id in ready
        }

    run["node_reports"] = {key: results[key] for key in sorted(results)}
    run["node_states_final"] = {key: value.value
                                for key, value in scheduler.node_states(job_id).items()}
    run["rounds"] = rounds
    run["worker_totals"] = {
        "live_calls": sum(report.get("live_calls", 0) for report in results.values()),
        "tool_calls": sum(report.get("tool_calls", 0) for report in results.values()),
        "usage": {
            key: sum(report.get("usage", {}).get(key, 0) for report in results.values())
            for key in ("prompt_tokens", "completion_tokens", "total_tokens",
                        "prompt_cache_hit_tokens", "prompt_cache_miss_tokens")
        },
    }
    run["diff"] = {"changed": [change["relative_path"] for change in diff.get("changed", [])],
                   "content_digest": diff.get("content_digest")}
    run["test_receipts"] = [
        {"receipt_id": row["receipt_id"], "profile_id": row["profile_id"],
         "exit_code": row["exit_code"]}
        for row in receipts
    ]
    if not receipts:
        run["problems"].append("no executor test receipt was produced")

    run["integration"] = {"applied": [], "skipped": True,
                          "reason": "the manager did not approve the change"}
    if verdict is not None and verdict.verdict == "APPROVE":
        try:
            integration_root = manager_ws.create_integration_tree(
                job_id=job_id, snapshot_id=snapshot.snapshot_id
            )
            applied_paths: list[str] = []
            final_digest = ""
            # Each leaf node's workspace already chains the bytes of its
            # dependencies, so integrating the leaves carries the whole result.
            # Applying an intermediate node as well would ask the integration tree
            # to accept bytes it already holds from the leaf, which is a conflict by
            # construction rather than a real disagreement between branches.
            dependents = {dependency for node in plan.nodes for dependency in node.dependencies}
            leaves = [node.id for node in plan.nodes if node.id not in dependents]
            for node_id in leaves:
                node_handle = handles.get(node_id)
                if node_handle is None:
                    continue
                outcome_for_node = manager_ws.apply_to_integration(
                    job_id=job_id, handle=node_handle
                )
                applied_paths.extend(outcome_for_node.get("applied", []))
                final_digest = outcome_for_node.get("content_digest", final_digest)
            run["integration"] = {
                "applied": applied_paths, "content_digest": final_digest,
                "integration_root": str(integration_root), "skipped": False,
                "source_project_untouched": True,
                "note": ("approved bytes were copied into this job's integration tree;"
                         " the registered source project was not written"),
            }
        except V1Error as exc:
            run["integration"] = {"applied": [], "skipped": True,
                                  "reason": f"{type(exc).__name__}: {exc}"}
            run["problems"].append(f"integration refused: {type(exc).__name__}")
    # ---- the program gate: a manager verdict cannot bypass it -----------------
    # Only a coordinated run (one that registered contracts, requirements,
    # invariants or submissions) is judged by the semantic layers; an ordinary run
    # reports NOT_APPLICABLE rather than a fake PASS.
    gate: dict[str, Any] | None = None
    try:
        from . import coordination

        coordinator = coordination.SemanticCoordinator(store, project_id=project_id,
                                                       job_id=job_id)
        layers = coordination.integration_layers(
            coordinator,
            applied=list(run.get("integration", {}).get("applied") or []),
            changed=list(diff.get("changed") or []),
            receipts_exit_zero=any(int(row["exit_code"]) == 0 for row in receipts),
            node_states=run["node_states_final"],
        )
        run["integration_layers"] = layers
        gate = coordinator.final_gate(
            node_states=run["node_states_final"],
            receipts_exit_zero=any(int(row["exit_code"]) == 0 for row in receipts),
            required_artifacts=manifest.get("required_artifacts") or None,
            integration=layers,
        )
        run["correctness_gate"] = gate
        for name in gate["failed"]:
            run["problems"].append(f"correctness gate failed: {name}")
    except V1Error as exc:  # a gate that cannot be evaluated is reported, never skipped
        run["correctness_gate"] = {"gate": "FINAL_MANAGER_GATE", "passed": None,
                                   "error": f"{type(exc).__name__}: {exc}",
                                   "failed": ["GATE_ERROR"]}
        run["problems"].append("correctness gate could not be evaluated")
    _record_knowledge(store, project_id=project_id, job_id=job_id, run=run, receipts=receipts)
    run["budget"] = {"scopes": scope_ids, "job_usage": ledger.usage(scope_ids["job"])}
    run["source_not_modified"] = _source_intact(config, snapshot)
    if not run["source_not_modified"]:
        run["problems"].append("a registered source file changed during the run")
    run["resolved_roles"] = agents.diagnose(profile, home=home_path)
    completed = [key for key, value in run["node_states_final"].items()
                 if value in {"WORKER_COMPLETE", "MANAGER_APPROVED", "INTEGRATED",
                              "APPLIED", "DONE"}]
    # PASS means the whole loop finished: every node complete, the manager's review
    # accepted the evidence (when the template has one), and approved bytes really
    # reached the integration tree. A FIX verdict is a real, reported outcome -- not
    # a pass with a note.
    review_ok = str((run.get("review") or {}).get("verdict")) in {"APPROVE", "NOT_REQUIRED"}
    integration_ok = bool(run.get("integration", {}).get("skipped") is False)
    if not review_ok:
        run["problems"].append(
            f"the manager verdict was {(run.get('review') or {}).get('verdict')}"
        )
    if not integration_ok:
        run["problems"].append("nothing was integrated")
    gate_ok = True if not run.get("correctness_gate") else bool(
        run["correctness_gate"].get("passed"))
    if not gate_ok:
        run["problems"].append("the final correctness gate did not pass")
    run["status"] = (
        "PASS"
        if not run["problems"] and len(completed) == len(plan.nodes)
        and review_ok and integration_ok and gate_ok
        else "PARTIAL"
    )
    run["ended_at"] = utcnow().isoformat()
    _write_manifest(home_path, job_id, {**manifest, "last_receipt": {
        key: run[key] for key in
        ("status", "problems", "review", "test_receipts", "integration", "ended_at",
         "node_states_final", "node_reports", "worker_totals", "budget", "diff",
         "source_not_modified", "resolved_roles", "plan_source", "planning", "template",
         "requirement", "acceptance", "nodes", "job_id", "project_id")
        if key in run
    }})
    return run


def run_requirement(
    *,
    requirement: str,
    project_id: str,
    home: str | Path,
    template_id: str | None = None,
    max_steps: int = 10,
    max_completion_tokens: int = 1024,
    max_nodes: int | None = None,
    dry_run: bool = False,
    provider_factory: Any | None = None,
    manager_factory: Any | None = None,
) -> dict[str, Any]:
    """Prepare and execute one workflow in this process."""
    from .core import cli as core_cli

    if dry_run:
        paths = core_cli.runtime_paths(str(home))
        home_path = paths["home"]
        config, _project = _resolve(home_path, project_id)
        template = templates.get(template_id or config.template, home=home_path)
        profile = model_profiles.load(config.model_profile, home=home_path)
        compiled = planner.compile_plan(requirement=requirement, template=template,
                                        config=config, job_id="dry-run", home=home_path)
        return {
            "kind": "KVFLOW_WORKFLOW", "product": "KVFlow", "dry_run": True,
            "executed": False, "project_id": project_id, "requirement": requirement,
            "template": template.id, "model_profile": profile.id,
            "plan": compiled["plan"], "plan_source": "dry_run_preview",
            "resolved_roles": agents.diagnose(profile, home=home_path),
            "budget_profile": registry.budget_profile(home_path, config.budget_profile),
            "note": "no job was created and no model was called",
        }
    manifest = prepare_run(requirement=requirement, project_id=project_id, home=home,
                           template_id=template_id, max_nodes=max_nodes,
                           manager_factory=manager_factory)
    return execute_run(job_id=manifest["job_id"], home=home, max_steps=max_steps,
                       max_completion_tokens=max_completion_tokens,
                       provider_factory=provider_factory, manager_factory=manager_factory)


# ------------------------------------------------------- knowledge/status


def _reopen_for_rework(store: Store, job_id: str, node_ids: Sequence[str]) -> list[str]:
    """Move reviewed nodes back to FIX so a rejected result can be reworked.

    A node that reached a milestone is not claimable, and the durable lifecycle
    says how it may legally go back to work: WORKER_COMPLETE -> MANAGER_REVIEW ->
    FIX. Nothing here resets the attempt counter, so the rework is still bounded by
    the retry budget the lineage already carries.
    """
    from .core.state import parse_state
    from .core.state import TaskState as CoreState

    reopened: list[str] = []
    for node_id in node_ids:
        state = parse_state(store.node(job_id, node_id)["state"])
        if state is CoreState.WORKER_COMPLETE:
            store.set_node_state(job_id, node_id, state, CoreState.MANAGER_REVIEW,
                                 actor="scheduler")
            state = CoreState.MANAGER_REVIEW
        if state is CoreState.MANAGER_REVIEW:
            store.set_node_state(job_id, node_id, state, CoreState.FIX, actor="scheduler")
            reopened.append(node_id)
    return reopened


def _record_knowledge(store: Store, *, project_id: str, job_id: str, run: dict,
                      receipts: Sequence[Mapping[str, Any]]) -> None:
    """Project knowledge with provenance: a report, plus executor-verified facts."""
    service = KnowledgeService(store)
    digest = run.get("authorization_digest") or content_digest(
        {"job": job_id, "plan": run.get("plan_digest")}
    )
    summary = (
        f"KVFlow {run['template']} run for {run['requirement'][:200]} finished with"
        f" status {run.get('status')}; nodes {run.get('nodes')}; review"
        f" {run.get('review', {}).get('verdict')}."
    )
    try:
        service.propose(
            project_id=project_id, topic=f"workflow-{job_id}", content=summary,
            author="manager", kind="REPORTED_FACT", source_ref=f"workflow:{job_id}",
            source_digest=content_digest(run), authorization_digest=digest, job_id=job_id,
        )
    except V1Error:
        return
    for row in receipts:
        try:
            record = store.receipt(row["receipt_id"])
            service.record_executor_fact(
                project_id=project_id, topic=f"executor-receipt-{row['receipt_id']}",
                content=f"profile {row['profile_id']} exited {row['exit_code']}",
                receipt_id=row["receipt_id"], receipt_digest=content_digest(record),
                authorization_digest=digest, job_id=job_id,
            )
        except V1Error:
            continue


def status(home: str | Path, job_id: str) -> dict[str, Any]:
    """Durable status of one workflow, readable from any entrance."""
    paths = _paths(home)
    store = _store_for(paths["home"])
    job = store.job(job_id)
    scheduler = Scheduler(store)
    manifest: dict[str, Any] = {}
    try:
        manifest = run_manifest(paths["home"], job_id)
    except NotFoundError:
        pass
    return {
        "job_id": job_id,
        "project_id": job["project_id"],
        "state": job["state"],
        "objective": job["objective"],
        "revision": job["revision"],
        "template": manifest.get("template"),
        "model_profile": manifest.get("model_profile"),
        "plan_source": manifest.get("plan_source"),
        "nodes": {key: value.value for key, value in scheduler.node_states(job_id).items()},
        "receipts": [
            {"receipt_id": row["receipt_id"], "profile_id": row["profile_id"],
             "exit_code": row["exit_code"]}
            for row in store.receipts(job_id)
        ],
        "knowledge": [
            {"id": record["id"], "topic": record["topic"], "kind": record["kind"],
             "content": record["content"][:400]}
            for record in KnowledgeService(store).search(
                KnowledgeQuery(project_id=job["project_id"], limit=25)
            )
        ],
        "last_receipt": manifest.get("last_receipt"),
        "checkpoint": store.checkpoint(),
    }


def list_runs(home: str | Path, *, project_id: str | None = None, limit: int = 25) -> dict:
    paths = _paths(home)
    store = _store_for(paths["home"])
    with store.read() as conn:
        if project_id:
            rows = conn.execute(
                "SELECT job_id, project_id, state, objective, revision, updated_at"
                " FROM jobs WHERE project_id = ? ORDER BY updated_at DESC LIMIT ?",
                (project_id, int(limit)),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT job_id, project_id, state, objective, revision, updated_at"
                " FROM jobs ORDER BY updated_at DESC LIMIT ?", (int(limit),),
            ).fetchall()
    runs = []
    for row in rows:
        item = dict(row)
        try:
            manifest = run_manifest(paths["home"], item["job_id"])
            item["template"] = manifest.get("template")
            item["plan_source"] = manifest.get("plan_source")
        except NotFoundError:
            item["template"] = None
        runs.append(item)
    return {"runs": runs, "count": len(runs)}


def control(home: str | Path, job_id: str, action: str, *, reason: str = "") -> dict:
    """pause / resume / cancel, shared by the CLI, MCP and the plugin."""
    paths = _paths(home)
    store = _store_for(paths["home"])
    scheduler = Scheduler(store)
    if action == "pause":
        revision = scheduler.pause(job_id, actor="user")
    elif action == "resume":
        revision = scheduler.resume(job_id, actor="user")
    elif action == "cancel":
        return scheduler.cancel(job_id, actor="user", reason=reason)
    else:
        raise ContractError("unknown control action", action=action)
    return {"job_id": job_id, "state": store.job_state(job_id).value, "revision": revision}


def _paths(home: str | Path) -> dict[str, Path]:
    from .core import cli as core_cli

    return core_cli.runtime_paths(str(home))


# ------------------------------------------------------------- run lifetime


def runner_log_path(home: str | Path, job_id: str) -> Path:
    return runs_dir(home) / f"{job_id}.log"


def launch_run(
    home: str | Path,
    job_id: str,
    *,
    mode: str = "auto",
    max_steps: int = 10,
    max_completion_tokens: int = 1024,
) -> dict:
    """Start a prepared run so it outlives the caller.

    ``process`` launches a detached child: the strongest form, because the run
    survives the host that started it. Some sandboxes put every descendant in a
    job object that kills the tree when the session ends; that is detected within
    seconds and reported, and ``auto`` then falls back to ``inline``, which runs
    the workflow in a background thread of the calling process. An inline run
    still never blocks the call, but it depends on the host staying alive -- and
    the record says which one was used instead of implying the stronger promise.
    """
    if mode == "inline":
        return _launch_inline(home, job_id, max_steps, max_completion_tokens)
    if mode not in {"auto", "process"}:
        raise ContractError("unknown runner mode", mode=mode)

    import subprocess
    import sys
    import time as _time

    log = runner_log_path(home, job_id)
    argv = [
        sys.executable, "-m", "kvflow.runner", "--home", str(home), "--job", job_id,
        "--max-steps", str(int(max_steps)),
        "--max-completion-tokens", str(int(max_completion_tokens)),
    ]
    env = dict(os.environ)
    package_root = str(Path(__file__).resolve().parents[1])
    env["PYTHONPATH"] = os.pathsep.join(
        [package_root] + [part for part in env.get("PYTHONPATH", "").split(os.pathsep) if part]
    )
    env["PYTHONIOENCODING"] = "utf-8"
    creationflags = 0
    if os.name == "nt":  # pragma: no cover - platform specific
        creationflags = 0x00000008 | 0x00000200  # DETACHED_PROCESS | NEW_PROCESS_GROUP
    with open(log, "ab") as handle:
        process = subprocess.Popen(  # noqa: S603 - fixed argv, never a shell
            argv, stdin=subprocess.DEVNULL, stdout=handle, stderr=handle, env=env,
            cwd=str(home), creationflags=creationflags, close_fds=True,
        )
    update_manifest(home, job_id, runner_mode="process", runner_pid=process.pid,
                    runner_started_at=utcnow().isoformat(), runner_log=str(log))
    _time.sleep(3.0)
    if process.poll() is not None:
        detail = {
            "mode_requested": mode,
            "process_exit_code": process.poll(),
            "log": str(log),
            "note": (
                "the detached runner did not survive this sandbox's process"
                " containment; the run continues inline in the calling process"
            ),
        }
        return {**detail, **_launch_inline(home, job_id, max_steps, max_completion_tokens)}
    return {"mode": "process", "pid": process.pid, "log": str(log)}


def _launch_inline(home: str | Path, job_id: str, max_steps: int,
                   max_completion_tokens: int) -> dict:
    import threading
    import traceback

    log = runner_log_path(home, job_id)

    def work() -> None:
        try:
            execute_run(job_id=job_id, home=home, max_steps=max_steps,
                        max_completion_tokens=max_completion_tokens)
        except BaseException as exc:  # noqa: BLE001 - the failure belongs in the record
            detail = f"{type(exc).__name__}: {exc}\n{traceback.format_exc()}"
            try:
                with open(log, "a", encoding="utf-8") as handle:
                    handle.write(detail)
                update_manifest(home, job_id, runner_error=detail[:2000],
                                runner_failed_at=utcnow().isoformat())
            except Exception:  # noqa: BLE001 - never mask the original failure
                pass

    thread = threading.Thread(target=work, name=f"kvflow-{job_id}", daemon=True)
    thread.start()
    update_manifest(home, job_id, runner_mode="inline",
                    runner_started_at=utcnow().isoformat(), runner_log=str(log))
    return {
        "mode": "inline",
        "thread": thread.name,
        "log": str(log),
        "note": (
            "this run depends on the caller staying alive; cancel stops it once the"
            " cancellation reaches the durable state"
        ),
    }


def stop_run(home: str | Path, job_id: str) -> dict:
    """Stop exactly the process this run started, and say what happened."""
    try:
        manifest = run_manifest(home, job_id)
    except V1Error:
        return {"stopped": False, "reason": "no run manifest"}
    if (manifest.get("runner_mode") or "") != "process":
        return {"stopped": False, "mode": manifest.get("runner_mode"),
                "reason": "this run has no separate process to stop"}
    pid = manifest.get("runner_pid")
    if not pid:
        return {"stopped": False, "reason": "this run recorded no process id"}
    try:
        os.kill(int(pid), 15)
    except OSError as exc:
        return {"stopped": False, "pid": pid, "reason": str(exc)}
    return {"stopped": True, "pid": pid}
