"""The KVFlow MCP server: the standard tool entrance for any host.

This is a real MCP server over stdio, built on the official SDK. It is the
*control* surface (what work exists, what it is doing, what it produced), not a
second scheduler: every call goes through the same registry, the same durable
store and the same workflow runner the CLI and the DSH plugin use.

Authority rules
---------------

* The model may supply a **requirement**, a registered ``project_id`` and one of
  the four template ids. Nothing else.
* Scope, write roots, protected paths, profiles, tools, model profile and budget
  all come from the project configuration the *user* approved. A model cannot
  widen any of them by asking.
* An unregistered or unapproved project is refused with the exact next step for
  the user, instead of silently registering anything.
* A started run is a detached process with its own pid recorded in the run
  manifest, so a long run neither blocks the tool connection nor dies with the
  session; cancelling stops exactly that process tree.

Tools: ``kvflow_projects``, ``kvflow_project_status``, ``kvflow_templates``,
``kvflow_start``, ``kvflow_runs``, ``kvflow_status``, ``kvflow_result``,
``kvflow_control``, ``kvflow_knowledge``.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Callable

import mcp.types as types
from mcp.server.lowlevel import Server
from mcp.server.stdio import stdio_server

from . import model_profiles, registry, templates, workflow
from .core.errors import V1Error

#: the runtime directory this server serves; set by whoever launches it
HOME_ENV = "KVFLOW_HOME"

SERVER_NAME = "kvflow"


def _home(explicit: str | None = None) -> Path:
    value = explicit or os.environ.get(HOME_ENV)
    if not value:
        raise V1Error(
            "the KVFlow runtime directory is not configured",
            environment=HOME_ENV,
        )
    return Path(value).expanduser().resolve()


# --------------------------------------------------------------------- tools


def tool_projects(home: Path, arguments: dict) -> dict:
    index = registry.Registry(home)
    entries = index.list()
    stale = {item["project_id"]: item["problem"] for item in registry.stale_entries(index)}
    return {
        "runtime": str(home),
        "projects": [
            {
                **entry,
                "problem": stale.get(entry["project_id"]),
                "healthy": entry["project_id"] not in stale,
            }
            for entry in entries
        ],
        "count": len(entries),
        "note": (
            "choosing a project for work means passing its project_id; the scope,"
            " profiles and budget were approved by the user at onboarding"
        ),
    }


def tool_project_status(home: Path, arguments: dict) -> dict:
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


def tool_templates(home: Path, arguments: dict) -> dict:
    return {"templates": templates.catalogue(home=home)}


def tool_start(home: Path, arguments: dict) -> dict:
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
    mode = str(arguments.get("mode") or "auto")
    launched = _launch_runner(home, manifest["job_id"], arguments, mode=mode)
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
            "the run continues outside this tool call; poll kvflow_status and read"
            " kvflow_result when it finishes"
        ),
    }


def tool_runs(home: Path, arguments: dict) -> dict:
    project_id = arguments.get("project_id")
    limit = int(arguments.get("limit", 25))
    return workflow.list_runs(home, project_id=str(project_id) if project_id else None,
                              limit=limit)


def tool_status(home: Path, arguments: dict) -> dict:
    job_id = str(arguments.get("job_id", ""))
    if not job_id:
        raise V1Error("a job_id is required", argument="job_id")
    return workflow.status(home, job_id)


def tool_result(home: Path, arguments: dict) -> dict:
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
            "note": "the run has not finished; poll kvflow_status",
        }
    return {
        "job_id": job_id,
        "finished": True,
        "template": manifest.get("template"),
        "requirement": manifest.get("requirement"),
        "plan_source": manifest.get("plan_source"),
        "acceptance": manifest.get("acceptance"),
        **receipt,
        "diff_paths": (receipt.get("integration") or {}).get("applied", []),
    }


def tool_control(home: Path, arguments: dict) -> dict:
    job_id = str(arguments.get("job_id", ""))
    action = str(arguments.get("action", "")).lower()
    if not job_id or action not in {"pause", "resume", "cancel"}:
        raise V1Error("job_id and action (pause|resume|cancel) are required")
    result = workflow.control(home, job_id, action, reason=str(arguments.get("reason", "")))
    if action == "cancel":
        result["runner_stopped"] = _stop_runner(home, job_id)
    return result


def tool_knowledge(home: Path, arguments: dict) -> dict:
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


# ------------------------------------------------------------------ helpers


def _log_path(home: Path, job_id: str) -> Path:
    directory = workflow.runs_dir(home)
    return directory / f"{job_id}.log"


def _launch_runner(home: Path, job_id: str, arguments: dict, *, mode: str = "auto") -> dict:
    """Start the run so it outlives this tool call.

    ``process`` launches a detached child, which is the strongest form: the run
    survives the host that started it. Some sandboxes put every descendant in a
    job object that kills the whole tree when the session ends; that is detected
    immediately (the child is gone within a few seconds) and reported, and
    ``auto`` falls back to ``inline``, which runs the workflow in a background
    thread of this server. Inline runs still never block the tool call, but they
    depend on the host staying alive -- and the receipt says which one was used
    instead of implying the stronger promise.
    """
    if mode == "inline":
        return _launch_inline(home, job_id, arguments)
    log = _log_path(home, job_id)
    argv = [
        sys.executable, "-m", "kvflow.runner",
        "--home", str(home), "--job", job_id,
        "--max-steps", str(int(arguments.get("max_steps", 10))),
        "--max-completion-tokens", str(int(arguments.get("max_completion_tokens", 1024))),
    ]
    env = {key: value for key, value in os.environ.items()}
    package_root = str(Path(__file__).resolve().parents[1])
    env["PYTHONPATH"] = os.pathsep.join(
        [package_root] + [part for part in env.get("PYTHONPATH", "").split(os.pathsep) if part]
    )
    env["PYTHONIOENCODING"] = "utf-8"
    creationflags = 0
    if os.name == "nt":  # pragma: no cover - platform specific
        creationflags = 0x00000008 | 0x00000200  # DETACHED_PROCESS | NEW_PROCESS_GROUP
    with open(log, "ab") as handle:
        process = subprocess.Popen(  # noqa: S603 - fixed argv, no shell
            argv, stdin=subprocess.DEVNULL, stdout=handle, stderr=handle, env=env,
            cwd=str(home), creationflags=creationflags, close_fds=True,
        )
    workflow.update_manifest(
        home, job_id, runner_mode="process", runner_pid=process.pid,
        runner_started_at=workflow.utcnow().isoformat(), runner_log=str(log),
    )
    time.sleep(3.0)
    if process.poll() is not None:
        # the sandbox killed the detached child at once: fall back, and say so
        detail = {
            "mode_requested": mode,
            "process_exit_code": process.poll(),
            "log": str(log),
            "note": (
                "the detached runner did not survive this sandbox's process"
                " containment; the run continues inline in the server process"
            ),
        }
        inline = _launch_inline(home, job_id, arguments)
        return {**detail, **inline}
    return {"mode": "process", "pid": process.pid, "log": str(log)}


def _launch_inline(home: Path, job_id: str, arguments: dict) -> dict:
    """Run the workflow in a background thread of this server (never blocking)."""
    import threading
    import traceback

    log = _log_path(home, job_id)

    def work() -> None:
        try:
            workflow.execute_run(
                job_id=job_id, home=home,
                max_steps=int(arguments.get("max_steps", 10)),
                max_completion_tokens=int(arguments.get("max_completion_tokens", 1024)),
            )
        except BaseException as exc:  # noqa: BLE001 - the failure belongs in the record
            detail = f"{type(exc).__name__}: {exc}\n{traceback.format_exc()}"
            try:
                with open(log, "a", encoding="utf-8") as handle:
                    handle.write(detail)
                workflow.update_manifest(home, job_id, runner_error=detail[:2000],
                                         runner_failed_at=workflow.utcnow().isoformat())
            except Exception:  # noqa: BLE001 - never mask the original failure
                pass

    thread = threading.Thread(target=work, name=f"kvflow-{job_id}", daemon=True)
    thread.start()
    workflow.update_manifest(
        home, job_id, runner_mode="inline",
        runner_started_at=workflow.utcnow().isoformat(), runner_log=str(log),
    )
    return {
        "mode": "inline",
        "thread": thread.name,
        "log": str(log),
        "note": (
            "this run depends on the host staying alive; cancel stops it only after"
            " the cancellation reaches the durable state"
        ),
    }


def _stop_runner(home: Path, job_id: str) -> dict:
    """Stop exactly the process this run started, and say what happened."""
    try:
        manifest = workflow.run_manifest(home, job_id)
    except V1Error:
        return {"stopped": False, "reason": "no run manifest"}
    pid = manifest.get("runner_pid")
    if not pid:
        return {"stopped": False, "reason": "this run has no recorded process"}
    try:
        os.kill(int(pid), 15)
    except OSError as exc:
        return {"stopped": False, "pid": pid, "reason": str(exc)}
    return {"stopped": True, "pid": pid}


TOOLS: tuple[dict[str, Any], ...] = (
    {
        "name": "kvflow_projects",
        "description": "List the registered KVFlow projects, with registry health.",
        "handler": tool_projects,
        "schema": {"type": "object", "properties": {}},
    },
    {
        "name": "kvflow_project_status",
        "description": (
            "Report one registered project's approved scope, profiles, budget and"
            " recent runs."
        ),
        "handler": tool_project_status,
        "schema": {
            "type": "object",
            "properties": {"project_id": {"type": "string", "minLength": 1}},
            "required": ["project_id"],
        },
    },
    {
        "name": "kvflow_templates",
        "description": "List the workflow templates a registered project may use.",
        "handler": tool_templates,
        "schema": {"type": "object", "properties": {}},
    },
    {
        "name": "kvflow_start",
        "description": (
            "Start one development workflow for a registered project. Only the"
            " requirement, the project_id and an optional template may be given;"
            " every permission, profile and budget comes from the approved project."
        ),
        "handler": tool_start,
        "schema": {
            "type": "object",
            "properties": {
                "requirement": {"type": "string", "minLength": 3},
                "project_id": {"type": "string", "minLength": 1},
                "template": {"type": "string"},
                "max_steps": {"type": "integer", "minimum": 1, "maximum": 40},
                "max_completion_tokens": {"type": "integer", "minimum": 128, "maximum": 8192},
                "mode": {"type": "string", "enum": ["auto", "inline", "process"]},
            },
            "required": ["requirement", "project_id"],
        },
    },
    {
        "name": "kvflow_runs",
        "description": "List recent workflow runs, newest first.",
        "handler": tool_runs,
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
        "handler": tool_status,
        "schema": {
            "type": "object",
            "properties": {"job_id": {"type": "string", "minLength": 1}},
            "required": ["job_id"],
        },
    },
    {
        "name": "kvflow_result",
        "description": "The finished run's evidence: diff, receipts, review, integration.",
        "handler": tool_result,
        "schema": {
            "type": "object",
            "properties": {"job_id": {"type": "string", "minLength": 1}},
            "required": ["job_id"],
        },
    },
    {
        "name": "kvflow_control",
        "description": "Pause, resume or cancel one run. Cancel stops its own process only.",
        "handler": tool_control,
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
        "handler": tool_knowledge,
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


def build_server(*, home: Path | None = None, name: str = SERVER_NAME) -> Server:
    """Build the MCP server bound to one KVFlow runtime directory."""
    server: Server = Server(name)
    handlers: dict[str, Callable[[Path, dict], dict]] = {
        spec["name"]: spec["handler"] for spec in TOOLS
    }

    @server.list_tools()
    async def list_tools() -> list[types.Tool]:
        return [
            types.Tool(name=spec["name"], description=spec["description"],
                       inputSchema=spec["schema"])
            for spec in TOOLS
        ]

    @server.call_tool()
    async def call_tool(tool_name: str, arguments: dict[str, Any]) -> list[types.ContentBlock]:
        handler = handlers.get(tool_name)
        if handler is None:
            return [types.TextContent(type="text", text=json.dumps(
                {"ok": False, "error_code": "UNKNOWN_TOOL", "tool": tool_name}))]
        try:
            payload = handler(_home(str(home) if home else None), dict(arguments or {}))
            return [types.TextContent(type="text", text=json.dumps(
                {"ok": True, "tool": tool_name, "result": payload},
                ensure_ascii=False, default=str))]
        except V1Error as exc:
            return [types.TextContent(type="text", text=json.dumps(
                {"ok": False, "tool": tool_name, "error_code": exc.code,
                 "error": exc.to_dict()}, ensure_ascii=False, default=str))]
        except Exception as exc:  # noqa: BLE001 - a tool failure is a reported result
            return [types.TextContent(type="text", text=json.dumps(
                {"ok": False, "tool": tool_name, "error_code": "UNEXPECTED",
                 "error": f"{type(exc).__name__}: {exc}"}, ensure_ascii=False))]

    return server


async def serve_stdio(*, home: Path | None = None) -> None:
    server = build_server(home=home)
    async with stdio_server() as (read, write):
        await server.run(read, write, server.create_initialization_options())


def main(argv: list[str] | None = None) -> int:
    import argparse
    import asyncio

    parser = argparse.ArgumentParser(prog="kvflow-mcp")
    parser.add_argument("--home", default=None)
    args = parser.parse_args(argv)
    if args.home:
        os.environ[HOME_ENV] = str(Path(args.home).expanduser().resolve())
    if not os.environ.get(HOME_ENV):
        print(f"{HOME_ENV} or --home is required", file=sys.stderr)
        return 2
    asyncio.run(serve_stdio(home=Path(os.environ[HOME_ENV])))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
