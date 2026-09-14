"""Planner: turn a template and a requirement into a validated DAG.

Two paths exist and both end in the same core ``Plan`` contract:

* ``plan="deterministic"`` (or a fallback when no manager is configured) compiles
  the template's stages into nodes itself, so a workflow never depends on a model
  being reachable;
* ``plan="manager"`` asks the configured manager for a DAG and then *validates*
  it against the template's approved envelope: the tools may not exceed what the
  template and the project authorized, the plan still has to be able to produce
  the executor receipt its own acceptance criteria demand, and it may not grow
  past the template's scale.

A manager plan that fails validation is refused with the reason, never silently
repaired; the caller can then re-plan or fall back to the deterministic shape.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Sequence

from .core.contracts import Action, NodeSpec, Plan, Role, content_digest
from .core.errors import ContractError
from .registry import ProjectConfig, authorization_digest, budget_profile, deadline_from
from .templates import Stage, WorkflowTemplate

#: a plan may never grow past this many nodes, whatever a manager proposes
MAX_PLAN_NODES = 8


def _stage_nodes(template: WorkflowTemplate, config: ProjectConfig,
                 requirement: str, job_id: str) -> list[NodeSpec]:
    """One node per stage, with a lineage key scoped to *this task*.

    ``lineage_key`` is what makes a retry budget impossible to reset inside a
    task: attempts are counted durably per (project, lineage). Scoping it to the
    job means "another attempt of this node in this task" is bounded at the core's
    limit, while a genuinely new user request is a new task with its own budget --
    a new request is not a retry of the previous one.
    """
    known_profiles = {profile.id for profile in config.profiles}
    reader_profiles = [
        profile.id for profile in config.profiles
        if profile.proof in {"check", "test", "build"}
    ]
    default_profile = reader_profiles[0] if reader_profiles else None
    nodes: list[NodeSpec] = []
    for stage in template.stages:
        profile = stage.test_profile
        if profile is not None and profile not in known_profiles:
            profile = None
        if profile is None:
            profile = default_profile
        if profile is None:
            raise ContractError(
                "the project registered no profile this workflow can run",
                stage=stage.id, project_id=config.project_id,
            )
        nodes.append(
            NodeSpec(
                id=stage.id,
                lineage_key=f"lin-{job_id}-{stage.id}",
                objective=stage.objective.format(requirement=requirement.strip()),
                write_scopes=list(config.allowed_write_roots) if stage.write else [],
                test_profile=profile,
                allowed_tools=[action.value for action in stage.tools],
                dependencies=list(stage.depends_on),
                role=Role.WORKER,
            )
        )
    return nodes


def _budget(config: ProjectConfig, home: Any, *, nodes: int) -> dict:
    profile = budget_profile(home, config.budget_profile)
    return {
        "calls": int(profile["calls"]),
        "input_tokens": int(profile["input_tokens"]),
        "output_tokens": int(profile["output_tokens"]),
        "tool_calls": int(profile["tool_calls"]),
        "storage_bytes": int(profile["storage_bytes"]),
        "wall_seconds": int(profile["wall_seconds"]),
        "concurrency": min(3, int(profile["concurrency"]), max(1, nodes)),
        "deadline": deadline_from(profile).isoformat(),
    }


def compile_plan(
    *,
    requirement: str,
    template: WorkflowTemplate,
    config: ProjectConfig,
    job_id: str,
    home: Any = None,
    plan_id: str | None = None,
    revision: int = 1,
) -> dict:
    """Compile the template's stages into a core plan document (deterministic path)."""
    text = (requirement or "").strip()
    if not text:
        raise ContractError("a requirement is required")
    nodes = _stage_nodes(template, config, text, job_id)
    document = {
        "id": plan_id or f"plan-{job_id}",
        "job_id": job_id,
        "version": 1,
        "revision": revision,
        "objective": text,
        "deliverables": [stage.produces for stage in template.stages],
        "constraints": [
            "writes stay inside the project's declared write scopes",
            "the manager never codes and the worker never reviews itself",
            f"template: {template.id} ({template.title})",
        ],
        "acceptance": list(template.acceptance),
        "nodes": [node.model_dump(mode="json") for node in nodes],
        "roles": [Role.WORKER, Role.MANAGER],
        "allowed_roots": list(config.allowed_write_roots),
        "tool_profiles": sorted({node.test_profile for node in nodes}),
        "resource_budget": _budget(config, home, nodes=len(nodes)),
        "approval_boundaries": [
            "no push",
            "no dependency installation without an explicit approval",
            "the source project is only written when the integration is approved",
        ],
        "authorization_digest": authorization_digest(config),
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    plan = Plan.model_validate(document)
    return {
        "plan": jsonable(plan),
        "plan_digest": content_digest(plan),
        "plan_source": "deterministic_template",
        "template": template.id,
        "nodes": [node.id for node in plan.nodes],
        "write_nodes": [node.id for node in plan.nodes if node.write_scopes],
        "max_parallel_workers": min(template.max_parallel_workers, len(plan.nodes)),
        "manager_plan_preferred": template.plan == "manager",
    }


def template_envelope(template: WorkflowTemplate, config: ProjectConfig) -> dict:
    """The exact envelope a manager plan must stay inside."""
    tools: set[str] = set()
    for stage in template.stages:
        tools.update(action.value for action in stage.tools)
    project_tools = (
        {action.value for action in config.allowed_tools}
        if config.allowed_tools is not None else None
    )
    return {
        "template": template.id,
        "tools": sorted(tools if project_tools is None else tools & project_tools),
        "profiles": sorted(profile.id for profile in config.profiles),
        "write_roots": sorted(config.allowed_write_roots),
        "acceptance": list(template.acceptance),
        "max_nodes": MAX_PLAN_NODES,
        "scale": template.scale,
    }


def validate_manager_plan(
    document: dict, *, template: WorkflowTemplate, config: ProjectConfig
) -> Plan:
    """Accept a manager plan only inside the template's approved envelope."""
    envelope = template_envelope(template, config)
    try:
        plan = Plan.model_validate(document)
    except Exception as exc:  # noqa: BLE001 - a bad plan is a typed refusal
        raise ContractError("the manager plan failed contract validation",
                            problems=str(exc)[:600]) from exc
    problems: list[str] = []
    if plan.authorization_digest != authorization_digest(config):
        problems.append("the plan carries a different authorization digest")
    if len(plan.nodes) > int(envelope["max_nodes"]):
        problems.append(
            f"the plan has {len(plan.nodes)} nodes, more than the {envelope['max_nodes']} allowed"
        )
    allowed_tools = set(envelope["tools"])
    for node in plan.nodes:
        requested = {action.value for action in node.allowed_tools}
        extra = requested - allowed_tools
        if extra:
            problems.append(f"node {node.id} asks for tools outside the template: {sorted(extra)}")
        if node.test_profile not in envelope["profiles"]:
            problems.append(f"node {node.id} names an unregistered profile {node.test_profile!r}")
        for scope in node.write_scopes:
            if not any(
                scope == root or scope.startswith(root.rstrip("/") + "/")
                for root in envelope["write_roots"]
            ):
                problems.append(f"node {node.id} writes outside the approved roots: {scope!r}")
    writers = [node.id for node in plan.nodes if node.write_scopes]
    runners = [
        node.id for node in plan.nodes
        if "test.run_profile" in {action.value for action in node.allowed_tools}
    ]
    if writers and not runners:
        problems.append(
            "no node could produce the executor receipt the acceptance criteria require"
        )
    if template.scale == "small" and len(plan.nodes) > 3:
        problems.append("a small template may not expand into more than three nodes")
    if problems:
        raise ContractError("the manager plan is outside the approved template envelope",
                            template=template.id, problems=problems[:8])
    return plan


def deterministic_from(document: dict) -> Plan:
    return Plan.model_validate(document)


def stage_by_id(template: WorkflowTemplate, stage_id: str) -> Stage | None:
    for stage in template.stages:
        if stage.id == stage_id:
            return stage
    return None


def jsonable(value: Any) -> Any:
    """One canonical JSON shape, so the CLI, MCP and plugin agree byte for byte."""
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json")
    if isinstance(value, dict):
        return {str(key): jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [jsonable(item) for item in value]
    return value


def summarize(plan: Plan) -> dict:
    return {
        "plan_id": plan.id,
        "job_id": plan.job_id,
        "revision": plan.revision,
        "objective": plan.objective,
        "acceptance": list(plan.acceptance),
        "nodes": [
            {
                "id": node.id,
                "dependencies": list(node.dependencies),
                "write_scopes": list(node.write_scopes),
                "test_profile": node.test_profile,
                "tools": sorted(action.value for action in node.allowed_tools),
            }
            for node in plan.nodes
        ],
    }


def parallel_groups(plan: Plan) -> list[list[str]]:
    """Dependency waves, so a caller can see the real parallelism of a plan."""
    remaining = {node.id: set(node.dependencies) for node in plan.nodes}
    waves: list[list[str]] = []
    while remaining:
        ready = sorted(key for key, deps in remaining.items() if not deps)
        if not ready:
            raise ContractError("the plan has cyclic dependencies")
        waves.append(ready)
        remaining = {
            key: deps - set(ready) for key, deps in remaining.items() if key not in ready
        }
    return waves


def tools_used_by(plan: Plan, stage_tools: Sequence[str]) -> list[str]:
    used = {action.value for node in plan.nodes for action in node.allowed_tools}
    return sorted(used & set(stage_tools))
