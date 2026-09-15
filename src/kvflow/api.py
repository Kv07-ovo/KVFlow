"""KVFlow's tool handlers: one implementation, several entrances.

The DSH plugin (through the CLI bridge), the MCP server (over stdio) and any
future host call exactly these functions, so no entrance can drift into its own
weaker version of "start a workflow" or "read the result".

Authority is unchanged by the entrance:

* a caller supplies a requirement, a registered ``project_id`` and a template id;
* scope, write roots, protected paths, profiles, tools, model profile and budget
  come from the configuration the user approved at onboarding;
* an unregistered or unapproved project is refused with the next step for the
  user instead of being registered silently.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Callable

from . import model_profiles, registry, templates, workflow
from .core.errors import NotFoundError, V1Error

#: environment variable naming the runtime directory this process serves
HOME_ENV = "KVFLOW_HOME"


def home_from(explicit: str | None = None) -> Path:
    value = explicit or os.environ.get(HOME_ENV)
    if not value:
        # the product default, so a plugin does not have to be configured to work
        value = str(Path.home() / ".kvflow")
    return Path(value).expanduser().resolve()


def project_status(home: Path, arguments: dict) -> dict:
    project_id = str(arguments.get("project_id", ""))
    index = registry.Registry(home)
    entry = index.get(project_id)
    config = registry.read_config(entry["canonical_root"])
    profile = model_profiles.load(config.model_profile, home=home)
    return {
        "entry": entry,
        "approval": registry.approval_summary(config),
        "model_profile": model_profiles.describe(profile),
        "budget_profile": registry.budget_profile(home, config.budget_profile),
        "recent_runs": workflow.list_runs(home, project_id=project_id, limit=5)["runs"],
    }


def projects(home: Path, arguments: dict) -> dict:
    index = registry.Registry(home)
    entries = index.list()
    stale = {item["project_id"]: item["problem"] for item in registry.stale_entries(index)}
    return {
        "runtime": str(home),
        "projects": [
            {**entry, "problem": stale.get(entry["project_id"]),
             "healthy": entry["project_id"] not in stale}
            for entry in entries
        ],
        "count": len(entries),
        "note": (
            "choosing a project for work means passing its project_id; its scope,"
            " profiles and budget were approved by the user at onboarding"
        ),
    }


def project_for_path(home: Path, arguments: dict) -> dict:
    """Resolve a directory to a registered project — how a host binds "current project"."""
    raw = str(arguments.get("path", "")).strip()
    if not raw:
        raise V1Error("a path is required", argument="path")
    target = Path(raw).expanduser().resolve()
    index = registry.Registry(home)
    for entry in index.list():
        root = Path(entry["canonical_root"])
        if root == target:
            return {"bound": True, "match": "exact", "project": entry}
    for entry in index.list():
        root = Path(entry["canonical_root"])
        if root in target.parents or target in root.parents:
            return {
                "bound": False,
                "match": "overlap",
                "project": entry,
                "note": (
                    "the directory overlaps a registered project but is not its root;"
                    " run onboarding for this exact directory if it is a separate project"
                ),
            }
    return {
        "bound": False,
        "match": "none",
        "path": str(target),
        "next": (
            f"kvflow project onboard --path {target} "
            "(review the scope, then add --write to approve and register it)"
        ),
    }


def list_templates(home: Path, arguments: dict) -> dict:
    return {"templates": templates.catalogue(home=home)}


def start(home: Path, arguments: dict) -> dict:
    requirement = str(arguments.get("requirement", "")).strip()
    project_id = str(arguments.get("project_id", "")).strip()
    template_id = arguments.get("template")
    if not requirement:
        raise V1Error("a requirement is required", argument="requirement")
    if not project_id:
        raise V1Error("a registered project_id is required", argument="project_id")
    manifest = workflow.prepare_run(
        requirement=requirement, project_id=project_id, home=home,
        template_id=str(template_id) if template_id else None,
    )
    launched = workflow.launch_run(
        home, manifest["job_id"], mode=str(arguments.get("mode") or "auto"),
        max_steps=int(arguments.get("max_steps", 10)),
        max_completion_tokens=int(arguments.get("max_completion_tokens", 1024)),
    )
    return {
        "job_id": manifest["job_id"],
        "project_id": project_id,
        "template": manifest["template"],
        "model_profile": manifest["model_profile"],
        "plan_source": manifest["plan_source"],
        "nodes": manifest["nodes"],
        "acceptance": manifest["acceptance"],
        "runner": launched,
        "note": (
            "the run continues outside this call; poll kvflow_status and read"
            " kvflow_result when it finishes"
        ),
    }


def runs(home: Path, arguments: dict) -> dict:
    project_id = arguments.get("project_id")
    return workflow.list_runs(home, project_id=str(project_id) if project_id else None,
                              limit=int(arguments.get("limit", 25)))


def status(home: Path, arguments: dict) -> dict:
    job_id = str(arguments.get("job_id", ""))
    if not job_id:
        raise V1Error("a job_id is required", argument="job_id")
    return workflow.status(home, job_id)


def result(home: Path, arguments: dict) -> dict:
    job_id = str(arguments.get("job_id", ""))
    if not job_id:
        raise V1Error("a job_id is required", argument="job_id")
    manifest = workflow.run_manifest(home, job_id)
    receipt = manifest.get("last_receipt")
    if receipt is None:
        state = workflow.status(home, job_id)
        return {
            "job_id": job_id,
            "finished": False,
            "state": state["state"],
            "nodes": state["nodes"],
            "runner_error": manifest.get("runner_error"),
            "note": "the run has not finished; poll kvflow_status",
        }
    return {"job_id": job_id, "finished": True, **receipt}


def control(home: Path, arguments: dict) -> dict:
    job_id = str(arguments.get("job_id", ""))
    action = str(arguments.get("action", "")).lower()
    if not job_id or action not in {"pause", "resume", "cancel"}:
        raise V1Error("job_id and action (pause|resume|cancel) are required")
    outcome = workflow.control(home, job_id, action, reason=str(arguments.get("reason", "")))
    if action == "cancel":
        outcome["runner_stopped"] = workflow.stop_run(home, job_id)
    return outcome


def knowledge(home: Path, arguments: dict) -> dict:
    from .core.knowledge import KnowledgeQuery, KnowledgeService
    from .core.store import Store

    project_id = str(arguments.get("project_id", ""))
    if not project_id:
        raise V1Error("a project_id is required", argument="project_id")
    store = Store(home / "agent_os.sqlite3")
    store.initialize()
    records = KnowledgeService(store).search(
        KnowledgeQuery(project_id=project_id, topic=arguments.get("topic"),
                       limit=int(arguments.get("limit", 25)))
    )
    return {
        "project_id": project_id,
        "topic": arguments.get("topic"),
        "records": [
            {key: record[key] for key in
             ("id", "topic", "kind", "author", "verification", "content", "source_ref",
              "conflict_state", "recorded_at")}
            for record in records
        ],
        "scope_note": (
            "only this project's records and expressly shared global preferences are"
            " reachable; another project's private records are not"
        ),
    }


#: the tool table every entrance shares: name -> (handler, description, schema)
TOOLS: tuple[dict[str, Any], ...] = (
    {
        "name": "kvflow_projects",
        "description": "List the registered KVFlow projects, with registry health.",
        "handler": projects,
        "schema": {"type": "object", "properties": {}},
    },
    {
        "name": "kvflow_current_project",
        "description": (
            "Resolve a directory (normally the current workspace) to a registered KVFlow"
            " project, or report exactly how to onboard it."
        ),
        "handler": project_for_path,
        "schema": {
            "type": "object",
            "properties": {"path": {"type": "string", "minLength": 1}},
            "required": ["path"],
        },
    },
    {
        "name": "kvflow_project_status",
        "description": (
            "Report one registered project's approved scope, profiles, budget and"
            " recent runs."
        ),
        "handler": project_status,
        "schema": {
            "type": "object",
            "properties": {"project_id": {"type": "string", "minLength": 1}},
            "required": ["project_id"],
        },
    },
    {
        "name": "kvflow_templates",
        "description": "List the workflow templates a registered project may use.",
        "handler": list_templates,
        "schema": {"type": "object", "properties": {}},
    },
    {
        "name": "kvflow_start",
        "description": (
            "Start one development workflow for a registered project. Only the"
            " requirement, the project_id and an optional template may be given; every"
            " permission, profile and budget comes from the approved project."
        ),
        "handler": start,
        "schema": {
            "type": "object",
            "properties": {
                "requirement": {"type": "string", "minLength": 3},
                "project_id": {"type": "string", "minLength": 1},
                "template": {"type": "string"},
                "mode": {"type": "string", "enum": ["auto", "inline", "process"]},
                "max_steps": {"type": "integer", "minimum": 1, "maximum": 40},
                "max_completion_tokens": {"type": "integer", "minimum": 128, "maximum": 8192},
            },
            "required": ["requirement", "project_id"],
        },
    },
    {
        "name": "kvflow_runs",
        "description": "List recent workflow runs, newest first.",
        "handler": runs,
        "schema": {
            "type": "object",
            "properties": {
                "project_id": {"type": "string"},
                "limit": {"type": "integer", "minimum": 1, "maximum": 100},
            },
        },
    },
    {
        "name": "kvflow_status",
        "description": "Durable status of one run: state, nodes, receipts, knowledge.",
        "handler": status,
        "schema": {
            "type": "object",
            "properties": {"job_id": {"type": "string", "minLength": 1}},
            "required": ["job_id"],
        },
    },
    {
        "name": "kvflow_result",
        "description": "The finished run's evidence: diff, receipts, review, integration.",
        "handler": result,
        "schema": {
            "type": "object",
            "properties": {"job_id": {"type": "string", "minLength": 1}},
            "required": ["job_id"],
        },
    },
    {
        "name": "kvflow_control",
        "description": "Pause, resume or cancel one run. Cancel stops its own process only.",
        "handler": control,
        "schema": {
            "type": "object",
            "properties": {
                "job_id": {"type": "string", "minLength": 1},
                "action": {"type": "string", "enum": ["pause", "resume", "cancel"]},
                "reason": {"type": "string"},
            },
            "required": ["job_id", "action"],
        },
    },
    {
        "name": "kvflow_knowledge",
        "description": "Search the knowledge visible to one project, with provenance.",
        "handler": knowledge,
        "schema": {
            "type": "object",
            "properties": {
                "project_id": {"type": "string", "minLength": 1},
                "topic": {"type": "string"},
                "limit": {"type": "integer", "minimum": 1, "maximum": 100},
            },
            "required": ["project_id"],
        },
    },
)

HANDLERS: dict[str, Callable[[Path, dict], dict]] = {
    spec["name"]: spec["handler"] for spec in TOOLS
}


def invoke(tool: str, arguments: dict, *, home: str | None = None) -> dict:
    """Call one tool by name. The single entry point used by the CLI bridge."""
    handler = HANDLERS.get(tool)
    if handler is None:
        raise V1Error("unknown KVFlow tool", tool=tool, known=sorted(HANDLERS))
    return handler(home_from(home), dict(arguments or {}))
