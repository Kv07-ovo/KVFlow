"""The workflow runner: one requirement in, durable evidence out.

This is the single place where a workflow actually happens. The CLI, the MCP
server and the DSH plugin all call :func:`run_requirement`, so they cannot drift
into three different orchestrators: they read and write the same durable store.

Sequence for one run:

1. resolve the approved project configuration, template, model and budget profile;
2. create the durable job and compile the plan (manager plan when the template
   asks for it and the adapter is reachable, otherwise the template's own DAG);
3. register the global -> project -> job budget chain with a hard micro-CNY cap;
4. run the dependency waves, at most ``max_parallel_workers`` nodes at a time,
   each in its own owned workspace with its own capability ticket;
5. collect the executor receipts the workers produced, ask the manager to review
   that evidence, and integrate only what the review approved;
6. write project-scoped knowledge with provenance, and return a receipt.

Nothing is replayed: a node that failed is reported as failed, its attempt is
consumed durably, and an unknown outcome stays unknown.
"""

from __future__ import annotations

import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
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
from .core.store import Store, new_id, utcnow
from .core.tools import ToolService
from .core.worker import WorkerLoop
from .core.workspace import WorkspaceManager, source_digest

GLOBAL_SCOPE = "kvflow-global"
#: every run is bounded twice: by the profile's money/time caps and by this
MAX_RUN_SECONDS = 3_600.0


def _scopes(ledger: BudgetLedger, config: registry.ProjectConfig, store: Store,
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


def config_digest(config: registry.ProjectConfig) -> str:
    return registry.authorization_digest(config)


def _tool_specs_for(actions: Sequence[str]) -> list[dict[str, Any]]:
    """Only the tools this node may actually use are shown to the model."""
    allowed = set(actions)
    return [
        {"name": spec["name"], "description": spec["description"],
         "inputSchema": spec["inputSchema"]}
        for spec in TOOL_SPECS
        if spec["action"] in allowed
    ]


def _planning(
    *, requirement: str, template: templates.WorkflowTemplate, config: registry.ProjectConfig,
    project: Project, compiled: dict, job_id: str, home: Path, profile: model_profiles.ModelProfile,
    manager: Any | None, manager_error: str | None,
) -> tuple[Any, str, dict[str, Any] | None, str | None]:
    """Decide the plan: the manager's, or the template's own DAG."""
    if template.plan != "manager":
        return planner.deterministic_from(compiled["plan"]), "deterministic_template", None, None
    if manager is None:
        return (
            planner.deterministic_from(compiled["plan"]),
            "deterministic_template",
            None,
            manager_error or "no manager is available for this profile",
        )
    try:
        manager_plan = manager.build_plan(
            job_id=job_id,
            project=project,
            objective=requirement,
            allowed_roots=list(config.allowed_write_roots),
            resource_budget=compiled["plan"]["resource_budget"],
            authorization_digest=project.authorization_digest,
        )
        validated = planner.validate_manager_plan(
            json.loads(manager_plan.model_dump_json()), template=template, config=config
        )
        return validated, "live_manager", manager_plan.model_dump(mode="json"), None
    except V1Error as exc:
        return (
            planner.deterministic_from(compiled["plan"]),
            "deterministic_template",
            None,
            f"{type(exc).__name__}: {exc}",
        )


def _run_wave(
    *,
    node_ids: Sequence[str],
    plan: Any,
    project: Project,
    config: registry.ProjectConfig,
    store: Store,
    scheduler: Scheduler,
    authority: CapabilityAuthority,
    tools: ToolService,
    ledger: BudgetLedger,
    scope_id: str,
    manager_ws: WorkspaceManager,
    snapshot: Any,
    handles: dict[str, Any],
    dependencies: Mapping[str, Sequence[str]],
    provider: Any,
    profile: model_profiles.ModelProfile,
    max_steps: int,
    max_completion_tokens: int,
    max_parallel: int,
    lock: threading.Lock,
    results: dict[str, dict[str, Any]],
) -> None:
    """Run one dependency wave: every node runs, at most ``max_parallel`` at once."""
    node_by_id = {node.id: node for node in plan.nodes}
    job_id = plan.job_id

    def work(node_id: str) -> None:
        node = node_by_id[node_id]
        started_monotonic = time.monotonic()
        report: dict[str, Any] = {
            "node_id": node_id,
            "status": "EXECUTOR_ERROR",
            "started_at": utcnow().isoformat(),
            "started_monotonic": started_monotonic,
        }
        try:
            base = None
            for dependency in dependencies.get(node_id, ()):
                if dependency in handles:
                    base = handles[dependency]
            if base is not None:
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
                project_id=project.id, job_id=job_id, node_id=node_id,
                run_id=claim.run_id, lease_id=claim.lease_id, role="worker", actions=actions,
            )
            loop = WorkerLoop(
                provider=provider, tools=tools, ledger=ledger, ticket=ticket,
                binding={
                    "project_id": project.id, "job_id": job_id, "node_id": node_id,
                    "run_id": claim.run_id, "attempt": claim.attempt, "fence": claim.fence,
                },
                scope_id=scope_id,
                discovered_tools=_tool_specs_for(actions),
                max_steps=max_steps,
                max_completion_tokens=max_completion_tokens,
                max_seconds=900.0,
            )
            outcome = loop.run(node.objective)
            if outcome.status == "WORKER_COMPLETE":
                scheduler.complete(claim, summary=outcome.summary[:300])
            else:
                scheduler.fail(claim, reason=outcome.summary[:400],
                               retryable=outcome.status != "BLOCKED")
            report.update(
                {
                    "status": outcome.status,
                    "summary": outcome.summary[:600],
                    "steps": outcome.steps,
                    "tool_calls": outcome.tool_calls,
                    "live_calls": outcome.live_calls,
                    "receipt_ids": list(outcome.receipt_ids),
                    "usage": dict(outcome.usage),
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
            report["seconds"] = round(report["finished_monotonic"] - started_monotonic, 2)
            with lock:
                results[node_id] = report

    with ThreadPoolExecutor(max_workers=max(1, min(len(node_ids), int(max_parallel)))) as pool:
        list(pool.map(work, list(node_ids)))


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
    integrate: bool = False,
) -> dict[str, Any]:
    """Start and drive one workflow. ``provider_factory``/``manager_factory`` are
    test seams: the product path builds the real adapters from the model profile."""
    from .core import cli as core_cli

    text = (requirement or "").strip()
    if not text:
        raise ContractError("a requirement is required")
    paths = core_cli.runtime_paths(str(home))
    home_path = paths["home"]
    index = registry.Registry(home_path)
    entry = index.get(project_id)
    config = registry.read_config(entry["canonical_root"])
    if not config.approved_by_user:
        raise AuthorizationError(
            "the project configuration has not been approved by the user",
            project_id=project_id,
            next=f"kvflow project onboard --path {config.canonical_root} --write",
        )
    template = templates.get(template_id or config.template, home=home_path)
    profile = model_profiles.load(config.model_profile, home=home_path)
    project, _profiles = registry.compile_project(config, home=home_path)
    store = Store(paths["database"])
    store.initialize()
    drift: str | None = None
    try:
        stored = store.project(project.id)
    except NotFoundError:
        store.register_project(project)
        stored = project
    if stored.authorization_digest != project.authorization_digest:
        # the durable registration is the authorization of record: a configuration
        # change never silently widens or narrows what this run may do
        drift = (
            "the project configuration differs from the registered authorization;"
            " this run uses the registered one"
        )
    project = stored
    manager_ws = WorkspaceManager(project)
    # one snapshot scope list, deduplicated: a read root of "." already covers the
    # write roots, and overlapping scopes would copy the same file twice
    if "." in config.allowed_read_roots:
        snapshot_scopes = ["."]
    else:
        snapshot_scopes = list(config.allowed_read_roots)
        for scope in config.allowed_write_roots:
            if scope not in snapshot_scopes:
                snapshot_scopes.append(scope)
    snapshot = manager_ws.create_snapshot(include_scopes=snapshot_scopes or ["."],
                                          notes=f"kvflow {template.id}")

    run: dict[str, Any] = {
        "kind": "KVFLOW_WORKFLOW",
        "product": "KVFlow",
        "project_id": project_id,
        "display_name": config.display_name,
        "requirement": text,
        "template": template.id,
        "model_profile": profile.id,
        "started_at": utcnow().isoformat(),
        "snapshot_id": snapshot.snapshot_id,
        "authorization_digest": project.authorization_digest,
        "problems": [],
    }
    if drift:
        run["problems"].append(drift)

    dry_run_report = {
        **run,
        "dry_run": True,
        "executed": False,
        "resolved_roles": agents.diagnose(profile, home=home_path),
        "budget_profile": registry.budget_profile(home_path, config.budget_profile),
        "note": "no job was created and no model was called",
    }
    if dry_run:
        compiled = planner.compile_plan(
            requirement=text, template=template, config=config, job_id="dry-run",
            home=home_path,
        )
        return {**dry_run_report, "plan": compiled["plan"], "plan_source": "dry_run_preview"}

    job_id = store.create_job(project.id, f"[{template.id}] {text}",
                              project.authorization_digest)
    compiled = planner.compile_plan(
        requirement=text, template=template, config=config, job_id=job_id, home=home_path,
    )
    manager = None
    manager_error: str | None = None
    if manager_factory is not None:
        manager = manager_factory()
    elif not dry_run:
        try:
            manager, _identity = agents.build_manager(
                profile, home=home_path, cwd=config.canonical_root
            )
        except V1Error as exc:
            manager_error = f"{type(exc).__name__}: {exc}"
    plan, plan_source, manager_plan_document, refusal = _planning(
        requirement=text, template=template, config=config, project=project,
        compiled=compiled, job_id=job_id, home=home_path, profile=profile,
        manager=manager if template.plan == "manager" else None,
        manager_error=manager_error,
    )
    if refusal:
        run["problems"].append(f"manager planning fell back: {refusal}")
    if max_nodes is not None and len(plan.nodes) > int(max_nodes):
        plan = planner.deterministic_from(
            planner.compile_plan(
                requirement=text, template=template, config=config, job_id=job_id,
                home=home_path,
            )["plan"]
        )
        plan_source = f"{plan_source}+trimmed"

    store.set_plan(plan, expected_job_revision=1)
    store.transition_job(job_id, "NEW", "PLANNING", actor="manager")
    store.transition_job(job_id, "PLANNING", "ASSIGNED", actor="manager")
    run.update(
        {
            "job_id": job_id,
            "plan_id": plan.id,
            "plan_source": plan_source,
            "plan_digest": content_digest(plan),
            "acceptance": list(plan.acceptance),
            "nodes": [node.id for node in plan.nodes],
            "waves": planner.parallel_groups(plan),
            "manager_plan": manager_plan_document,
        }
    )

    ledger = BudgetLedger(store)
    scope_ids = _scopes(ledger, config, store, job_id, home=home_path)
    scheduler = Scheduler(store, worker_slots=min(3, template.max_parallel_workers,
                                                  int(registry.budget_profile(
                                                      home_path, config.budget_profile
                                                  )["concurrency"])))
    authority = CapabilityAuthority(store)
    tools = ToolService(store, authority, workspace_root=manager_ws.managed_root)
    provider = provider_factory() if provider_factory is not None else agents.build_worker_provider(profile)

    handles: dict[str, Any] = {}
    results: dict[str, dict[str, Any]] = {}
    lock = threading.Lock()
    dependencies = {node.id: tuple(node.dependencies) for node in plan.nodes}
    for wave in planner.parallel_groups(plan):
        if len(wave) > template.max_parallel_workers:
            wave = sorted(wave)[: template.max_parallel_workers]
        _run_wave(
            node_ids=wave, plan=plan, project=project, config=config, store=store,
            scheduler=scheduler, authority=authority, tools=tools, ledger=ledger,
            scope_id=scope_ids["job"], manager_ws=manager_ws, snapshot=snapshot,
            handles=handles, dependencies=dependencies, provider=provider, profile=profile,
            max_steps=max_steps, max_completion_tokens=max_completion_tokens,
            max_parallel=template.max_parallel_workers,
            lock=lock, results=results,
        )

    run["node_reports"] = {key: results[key] for key in sorted(results)}
    run["node_states"] = {key: value.value for key, value in scheduler.node_states(job_id).items()}
    run["worker_totals"] = {
        "live_calls": sum(report.get("live_calls", 0) for report in results.values()),
        "tool_calls": sum(report.get("tool_calls", 0) for report in results.values()),
        "usage": {
            key: sum(report.get("usage", {}).get(key, 0) for report in results.values())
            for key in ("prompt_tokens", "completion_tokens", "total_tokens",
                        "prompt_cache_hit_tokens", "prompt_cache_miss_tokens")
        },
    }
    latest_handle = handles.get(plan.nodes[-1].id) or next(iter(handles.values()), None)
    diff = manager_ws.workspace_diff(latest_handle) if latest_handle else {"changed": [],
                                                                          "content_digest": ""}
    for change in diff.get("changed", []):
        change.pop("base_sha256", None)
        path = (latest_handle.root / change["relative_path"]) if latest_handle else None
        if path is not None and path.is_file() and path.stat().st_size <= 200_000:
            text_body = path.read_text(encoding="utf-8", errors="replace")
            change["content_preview"] = text_body[:6000]
            change["content_bytes"] = len(text_body.encode("utf-8"))
    receipts = store.receipts(job_id)
    run["diff"] = {
        "changed": [change["relative_path"] for change in diff.get("changed", [])],
        "content_digest": diff.get("content_digest"),
    }
    run["test_receipts"] = [
        {"receipt_id": row["receipt_id"], "profile_id": row["profile_id"],
         "exit_code": row["exit_code"]}
        for row in receipts
    ]
    if not receipts:
        run["problems"].append("no executor test receipt was produced")

    verdict = None
    if template.review == "manager" and manager is not None:
        try:
            verdict = manager.review(
                objective=plan.objective,
                acceptance=list(plan.acceptance),
                diff=diff,
                receipts=receipts,
                content_digest=diff.get("content_digest") or "",
            )
            run["review"] = verdict.to_dict()
        except V1Error as exc:
            run["review"] = {"verdict": "REFUSED", "error": f"{type(exc).__name__}: {exc}"}
            run["problems"].append(f"manager review refused: {type(exc).__name__}")
    elif template.review == "manager":
        run["review"] = {"verdict": "NOT_RUN",
                         "reason": manager_error or "no manager is available"}
        run["problems"].append("the manager review did not run")
    else:
        run["review"] = {"verdict": "NOT_REQUIRED", "reason": f"template {template.id}"}

    run["integration"] = {"applied": [], "skipped": True,
                          "reason": "the manager did not approve the change"}
    approved = bool(verdict is not None and verdict.verdict == "APPROVE")
    if approved:
        try:
            integration_root = manager_ws.create_integration_tree(
                job_id=job_id, snapshot_id=snapshot.snapshot_id
            )
            applied_paths: list[str] = []
            final_digest = ""
            for node in plan.nodes:
                node_handle = handles.get(node.id)
                if node_handle is None:
                    continue
                outcome_for_node = manager_ws.apply_to_integration(
                    job_id=job_id, handle=node_handle
                )
                applied_paths.extend(outcome_for_node.get("applied", []))
                final_digest = outcome_for_node.get("content_digest", final_digest)
            run["integration"] = {
                "applied": applied_paths,
                "content_digest": final_digest,
                "integration_root": str(integration_root),
                "skipped": False,
                "source_project_untouched": True,
                "note": (
                    "approved bytes were copied into this job's integration tree; the"
                    " registered source project was not written"
                ),
            }
        except V1Error as exc:
            run["integration"] = {"applied": [], "skipped": True,
                                  "reason": f"{type(exc).__name__}: {exc}"}
            run["problems"].append(f"integration refused: {type(exc).__name__}")

    _record_knowledge(store, project_id=project_id, job_id=job_id, run=run, receipts=receipts)

    run["budget"] = {
        "scopes": scope_ids,
        "job_usage": ledger.usage(scope_ids["job"]),
    }
    run["source_not_modified"] = _source_intact(config, snapshot)
    if not run["source_not_modified"]:
        run["problems"].append("a registered source file changed during the run")
    run["resolved_roles"] = agents.diagnose(profile, home=home_path)
    run["node_states_final"] = {
        key: value.value for key, value in scheduler.node_states(job_id).items()
    }
    completed = [
        key for key, value in run["node_states_final"].items()
        if value in {"WORKER_COMPLETE", "MANAGER_APPROVED", "INTEGRATED", "APPLIED", "DONE"}
    ]
    run["status"] = (
        "PASS" if not run["problems"] and len(completed) == len(plan.nodes) else "PARTIAL"
    )
    run["ended_at"] = utcnow().isoformat()
    return run


def _source_intact(config: registry.ProjectConfig, snapshot: Any) -> bool:
    """The registered source tree still hashes to what the snapshot captured.

    The comparison uses the core's own canonical source digest, so it agrees with
    the snapshot by construction instead of re-inventing newline handling here.
    """
    for entry in getattr(snapshot, "entries", ()):
        source = Path(config.canonical_root) / entry.relative_path
        if not source.is_file():
            return False
        digest, _size = source_digest(source)
        if digest != entry.sha256:
            return False
    return True


def _record_knowledge(store: Store, *, project_id: str, job_id: str, run: dict,
                      receipts: Sequence[Mapping[str, Any]]) -> None:
    """Project knowledge with provenance: a report, plus executor-verified facts."""
    service = KnowledgeService(store)
    digest = config_digest_from_run(run)
    summary = (
        f"KVFlow {run['template']} run for {run['requirement'][:200]} finished with"
        f" status {run.get('status')}; nodes {run.get('nodes')}; review"
        f" {run.get('review', {}).get('verdict')}."
    )
    try:
        service.propose(
            project_id=project_id, topic=f"workflow-{job_id}", content=summary,
            author="manager", kind="REPORTED_FACT",
            source_ref=f"workflow:{job_id}", source_digest=content_digest(run),
            authorization_digest=digest, job_id=job_id,
        )
    except V1Error:
        return
    for row in receipts:
        try:
            record = store.receipt(row["receipt_id"])
            service.record_executor_fact(
                project_id=project_id, topic=f"executor-receipt-{row['receipt_id']}",
                content=(f"profile {row['profile_id']} exited {row['exit_code']}"),
                receipt_id=row["receipt_id"], receipt_digest=content_digest(record),
                authorization_digest=digest, job_id=job_id,
            )
        except V1Error:
            continue


def config_digest_from_run(run: Mapping[str, Any]) -> str:
    digest = run.get("authorization_digest")
    if isinstance(digest, str) and len(digest) == 64:
        return digest
    return content_digest({"job": run.get("job_id"), "plan": run.get("plan_digest")})


def status(home: str | Path, job_id: str) -> dict[str, Any]:
    """Durable status of one workflow, readable from any entrance."""
    from .core import cli as core_cli

    paths = core_cli.runtime_paths(str(home))
    store = Store(paths["database"])
    store.initialize()
    job = store.job(job_id)
    scheduler = Scheduler(store)
    return {
        "job_id": job_id,
        "project_id": job["project_id"],
        "state": job["state"],
        "objective": job["objective"],
        "revision": job["revision"],
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
        "checkpoint": store.checkpoint(),
    }


def list_runs(home: str | Path, *, project_id: str | None = None, limit: int = 25) -> dict:
    from .core import cli as core_cli

    paths = core_cli.runtime_paths(str(home))
    store = Store(paths["database"])
    store.initialize()
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
                " FROM jobs ORDER BY updated_at DESC LIMIT ?",
                (int(limit),),
            ).fetchall()
    return {"runs": [dict(row) for row in rows], "count": len(rows)}


def control(home: str | Path, job_id: str, action: str, *, reason: str = "") -> dict:
    """pause / resume / cancel, shared by the CLI, MCP and the plugin."""
    from .core import cli as core_cli

    paths = core_cli.runtime_paths(str(home))
    store = Store(paths["database"])
    store.initialize()
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
