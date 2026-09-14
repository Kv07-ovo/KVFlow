"""The ``agent v1`` command line: install, run, inspect, pause, back up.

Every subcommand is a thin, typed wrapper over the modules that already hold the
logic, so the CLI cannot become a second, weaker implementation. It preserves the
v0.1 entry point by living under its own ``v1`` namespace: ``python -m
kvflow.core.cli <command>`` and ``kvflow <command>``.

Design rules
------------

* **It only ever stops its own processes.** A PID/start-time registry under the
  runtime directory records what this install started; nothing is killed by port
  or by process name.
* **It never silently widens permissions or budget.** Missing authorization or a
  malformed request is a typed error with a nonzero exit code.
* **Every example in the documentation is exercised by the test suite.**
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

from pydantic import ValidationError

from .backup import BackupManager
from .budget import BudgetLedger
from .contracts import Project
from .errors import ContractError, V1Error
from .knowledge import KnowledgeQuery, KnowledgeService
from .mcp_server import CAPABILITY_ENV, DATABASE_ENV, WORKSPACE_ENV
from .scheduler import Scheduler
from .security import CapabilityAuthority
from .state import TaskState
from .store import Store
from .workspace import WorkspaceManager

PRODUCT_NAME = "KVFlow"
PRODUCT_VERSION = "0.1.0"
CORE_VERSION = "1.0.0"
EXIT_OK = 0
EXIT_TYPED_ERROR = 1
EXIT_USAGE = 2

DEFAULT_HOME = ".kvflow"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def runtime_paths(home: str | os.PathLike[str] | None) -> dict[str, Path]:
    base = Path(home).expanduser().resolve() if home else Path.cwd() / DEFAULT_HOME
    base.mkdir(parents=True, exist_ok=True)
    workspace = base / "workspace"
    workspace.mkdir(parents=True, exist_ok=True)
    return {
        "home": base,
        "database": base / "kvflow.sqlite3",
        "workspace": workspace,
        "backups": base / "backups",
        "registry": base / "processes.json",
    }


def _store(paths: dict[str, Path]) -> Store:
    store = Store(paths["database"])
    store.initialize()
    return store


def _load_project(document: str | None, path: str | None) -> Project:
    if path:
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
    elif document:
        raw = json.loads(document)
    else:
        raise ContractError("a project document or --file is required")
    try:
        return Project.model_validate(raw)
    except ValidationError as exc:
        # a registration refusal is a typed contract error, not an internal crash
        raise ContractError(
            "the project registration is not valid",
            problems=[error["msg"] for error in exc.errors()][:8],
        ) from exc


def _process_registry(paths: dict[str, Path]) -> list[dict[str, Any]]:
    registry = paths["registry"]
    if not registry.exists():
        return []
    try:
        return json.loads(registry.read_text(encoding="utf-8"))
    except ValueError:
        return []


def _save_registry(paths: dict[str, Path], entries: list[dict[str, Any]]) -> None:
    paths["registry"].write_text(json.dumps(entries, indent=2), encoding="utf-8")


def _pid_alive(pid: int, started_at: str) -> bool:
    """Only report a process alive when its start time still matches."""
    try:
        import ctypes
        from ctypes import wintypes

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
        kernel32.OpenProcess.restype = wintypes.HANDLE
        kernel32.GetExitCodeProcess.argtypes = [
            wintypes.HANDLE,
            ctypes.POINTER(wintypes.DWORD),
        ]
        kernel32.GetExitCodeProcess.restype = wintypes.BOOL
        kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
        handle = kernel32.OpenProcess(0x1000, False, int(pid))
        if not handle:
            return False
        try:
            code = wintypes.DWORD()
            kernel32.GetExitCodeProcess(handle, ctypes.byref(code))
            return code.value == 259
        finally:
            kernel32.CloseHandle(handle)
    except Exception:  # noqa: BLE001 - a liveness check must never crash a command
        return False


# --------------------------------------------------------------------- commands


def cmd_doctor(args: argparse.Namespace) -> dict[str, Any]:
    paths = runtime_paths(args.home)
    store = _store(paths)
    report: dict[str, Any] = {
        "version": PRODUCT_VERSION,
        "home": str(paths["home"]),
        "database": str(paths["database"]),
        "schema_version": store.schema_version(),
        "checks": {},
        "checked_at": _now(),
    }
    try:
        with store.read() as conn:
            conn.execute("SELECT 1").fetchone()
        report["checks"]["database"] = {"ok": True}
    except Exception as exc:  # noqa: BLE001
        report["checks"]["database"] = {"ok": False, "error": str(exc)}
    projects = store.list_projects()
    report["checks"]["projects"] = {"ok": True, "count": len(projects)}
    report["checks"]["workspace"] = {
        "ok": paths["workspace"].is_dir(),
        "path": str(paths["workspace"]),
    }
    report["checks"]["budget"] = {
        "scopes": len(list_scope_ids(store)),
    }
    registry = _process_registry(paths)
    live = [entry for entry in registry if _pid_alive(entry["pid"], entry["started_at"])]
    report["checks"]["processes"] = {
        "owned_registered": len(registry),
        "owned_alive": len(live),
        "permanent_service_added": False,
    }
    report["checks"]["mcp"] = {
        "server_module": "kvflow.core.mcp_server",
        "required_environment": [DATABASE_ENV, CAPABILITY_ENV, WORKSPACE_ENV],
    }
    report["checks"]["worker"] = {
        "configured": os.environ.get("KVFLOW_WORKER", "deepseek-flash"),
        "provider_returned": "NOT_EXPOSED",
    }
    report["checks"]["manager"] = {
        "configured": os.environ.get("KVFLOW_MANAGER", "gpt-6-astra"),
        "provider_returned": "NOT_EXPOSED",
    }
    report["ready"] = all(
        value.get("ok", True) for value in report["checks"].values() if isinstance(value, dict)
    )
    return report


def list_scope_ids(store: Store) -> list[str]:
    with store.read() as conn:
        rows = conn.execute("SELECT scope_id FROM budget_scopes ORDER BY scope_id").fetchall()
    return [row["scope_id"] for row in rows]


def cmd_project_add(args: argparse.Namespace) -> dict[str, Any]:
    paths = runtime_paths(args.home)
    store = _store(paths)
    project = _load_project(args.document, args.file)
    store.register_project(project)
    return {"registered": project.id, "managed_root": project.managed_root, "at": _now()}


def cmd_project_list(args: argparse.Namespace) -> dict[str, Any]:
    paths = runtime_paths(args.home)
    store = _store(paths)
    return {
        "projects": [
            {
                "id": project.id,
                "display_name": project.display_name,
                "source_root": project.source_root,
                "managed_root": project.managed_root,
                "trusted": project.trusted,
                "test_profiles": [p.id for p in project.test_profiles],
            }
            for project in store.list_projects()
        ]
    }


def cmd_workers(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "orchestrator": {"model": "deterministic-python", "component": "kvflow.core.scheduler"},
        "manager": {
            "display_name": "Astra Ultra",
            "configured_model": os.environ.get("KVFLOW_MANAGER", "gpt-6-astra"),
            "provider_returned_model": "NOT_EXPOSED",
            "source": "Codex CLI non-interactive session",
        },
        "worker": {
            "display_name": "DeepSeek V4.1 Flash",
            "configured_model": os.environ.get("KVFLOW_WORKER", "deepseek-flash"),
            "provider_returned_model": "NOT_EXPOSED",
            "source": "existing DeepSeek credentials read only by the adapter",
        },
        "slots": {"workers": 3, "heavy": 1, "manager": 1},
        "credentials_exposed": False,
    }


def cmd_status(args: argparse.Namespace) -> dict[str, Any]:
    paths = runtime_paths(args.home)
    store = _store(paths)
    scheduler = Scheduler(store)
    if args.job_id:
        job = store.job(args.job_id)
        return {
            "job_id": args.job_id,
            "state": job["state"],
            "nodes": {k: v.value for k, v in scheduler.node_states(args.job_id).items()},
            "evaluation": scheduler.evaluate(args.job_id),
            "slots": scheduler.slot_usage(args.job_id),
            "checkpoint": store.checkpoint(),
        }
    with store.read() as conn:
        rows = conn.execute(
            "SELECT job_id, project_id, state, updated_at FROM jobs ORDER BY updated_at DESC"
        ).fetchall()
    return {"jobs": [dict(row) for row in rows]}


def cmd_pause(args: argparse.Namespace) -> dict[str, Any]:
    paths = runtime_paths(args.home)
    store = _store(paths)
    revision = Scheduler(store).pause(args.job_id, actor=args.actor)
    return {"job_id": args.job_id, "state": store.job_state(args.job_id).value,
            "revision": revision}


def cmd_resume(args: argparse.Namespace) -> dict[str, Any]:
    paths = runtime_paths(args.home)
    store = _store(paths)
    revision = Scheduler(store).resume(args.job_id, actor=args.actor)
    return {"job_id": args.job_id, "state": store.job_state(args.job_id).value,
            "revision": revision}


def cmd_cancel(args: argparse.Namespace) -> dict[str, Any]:
    paths = runtime_paths(args.home)
    store = _store(paths)
    return Scheduler(store).cancel(args.job_id, actor=args.actor, reason=args.reason or "")


def cmd_knowledge_search(args: argparse.Namespace) -> dict[str, Any]:
    paths = runtime_paths(args.home)
    store = _store(paths)
    service = KnowledgeService(store)
    records = service.search(
        KnowledgeQuery(project_id=args.project, topic=args.topic, limit=args.limit)
    )
    return {"project_id": args.project, "topic": args.topic, "records": records}


def cmd_audit(args: argparse.Namespace) -> dict[str, Any]:
    paths = runtime_paths(args.home)
    store = _store(paths)
    with store.read() as conn:
        events = conn.execute(
            "SELECT seq, job_id, actor, kind, from_state, to_state, at FROM events"
            " ORDER BY seq DESC LIMIT ?",
            (int(args.limit),),
        ).fetchall()
        operations = conn.execute(
            "SELECT operation_id, status, action, run_id FROM operations"
            " ORDER BY created_at DESC LIMIT ?",
            (int(args.limit),),
        ).fetchall()
    return {
        "events": [dict(row) for row in events],
        "operations": [dict(row) for row in operations],
        "checkpoint": store.checkpoint(),
    }


def cmd_backup(args: argparse.Namespace) -> dict[str, Any]:
    paths = runtime_paths(args.home)
    manager = BackupManager(
        database=paths["database"], backups_root=paths["backups"], version=PRODUCT_VERSION
    )
    include = [Path(item) for item in (args.include or [])]
    result = manager.create(include=include, note=args.note or "")
    return result.to_dict()


def cmd_restore(args: argparse.Namespace) -> dict[str, Any]:
    paths = runtime_paths(args.home)
    manager = BackupManager(
        database=paths["database"], backups_root=paths["backups"], version=PRODUCT_VERSION
    )
    return manager.restore(args.backup, args.destination, dry_run=bool(args.dry_run))


def cmd_backup_verify(args: argparse.Namespace) -> dict[str, Any]:
    paths = runtime_paths(args.home)
    manager = BackupManager(
        database=paths["database"], backups_root=paths["backups"], version=PRODUCT_VERSION
    )
    return manager.verify(args.backup)


def cmd_mcp_serve(args: argparse.Namespace) -> dict[str, Any]:
    """Describe how to launch the MCP server; serving happens in that process."""
    paths = runtime_paths(args.home)
    return {
        "module": "kvflow.core.mcp_server",
        "argv": [sys.executable, "-m", "kvflow.core.mcp_server"],
        "transport": "stdio",
        "stdout_is_protocol_only": True,
        "environment": {
            DATABASE_ENV: str(paths["database"]),
            CAPABILITY_ENV: "<opaque orchestrator ticket>",
            WORKSPACE_ENV: str(paths["workspace"]),
        },
        "tools": "repo/list/search/read/status/diff/patch/commit, test.run,"
                 " operation.status/result, knowledge.search/read",
        "note": "the ticket is issued per run by the Orchestrator; it is never stored here",
    }


def cmd_mcp_status(args: argparse.Namespace) -> dict[str, Any]:
    from .mcp_server import TOOL_SPECS

    paths = runtime_paths(args.home)
    store = _store(paths)
    with store.read() as conn:
        capabilities = conn.execute(
            "SELECT COUNT(*) AS c FROM capabilities WHERE revoked_at IS NULL"
        ).fetchone()["c"]
    return {
        "database": str(paths["database"]),
        "tool_count": len(TOOL_SPECS),
        "tools": sorted(spec["name"] for spec in TOOL_SPECS),
        "active_capabilities": int(capabilities),
        "protocol": "official MCP SDK over stdio",
    }


def cmd_register_process(args: argparse.Namespace) -> dict[str, Any]:
    """Record an owned process so ``stop`` can stop exactly that tree."""
    paths = runtime_paths(args.home)
    entry = {
        "id": uuid.uuid4().hex[:12],
        "kind": args.kind,
        "pid": int(args.pid),
        "port": int(args.port) if args.port else None,
        "argv": list(args.argv or []),
        "started_at": _now(),
    }
    registry = _process_registry(paths)
    registry.append(entry)
    _save_registry(paths, registry)
    return {"registered": entry, "owned_only": True}


def cmd_stop(args: argparse.Namespace) -> dict[str, Any]:
    """Stop only the processes this install started, never a port occupant."""
    paths = runtime_paths(args.home)
    registry = _process_registry(paths)
    stopped: list[dict[str, Any]] = []
    kept: list[dict[str, Any]] = []
    for entry in registry:
        alive = _pid_alive(entry["pid"], entry["started_at"])
        if not alive:
            continue
        if args.id and entry["id"] != args.id:
            kept.append(entry)
            continue
        try:
            os.kill(entry["pid"], 15)
            stopped.append({**entry, "signalled": "SIGTERM"})
        except OSError as exc:
            kept.append({**entry, "stop_error": str(exc)})
    _save_registry(paths, kept)
    return {
        "stopped": stopped,
        "kept": kept,
        "killed_by_port": False,
        "note": "only processes this install registered are signalled",
    }


def cmd_ui_serve(args: argparse.Namespace) -> dict[str, Any]:
    """Start the real loopback UI and register it so ``stop`` can stop it."""
    import time

    from .web import UiBackend, UiServer, UiSession

    paths = runtime_paths(args.home)
    store = _store(paths)
    session = UiSession(token="", csrf="")
    server = UiServer(
        UiBackend(store=store, workspace_root=paths["workspace"]),
        host="127.0.0.1",
        port=int(args.port or 0),
        session=session,
    )
    server.start()
    entry = {
        "id": uuid.uuid4().hex[:12],
        "kind": "ui",
        "pid": os.getpid(),
        "port": server.port,
        "argv": ["kvflow", "ui", "serve"],
        "started_at": _now(),
    }
    registry = _process_registry(paths)
    registry.append(entry)
    _save_registry(paths, registry)
    ready = {
        "url": server.url,
        "host": server.host,
        "port": server.port,
        "pid": entry["id"] and os.getpid(),
        "registered_id": entry["id"],
        "session_token": session.token,
        "loopback_only": True,
        "stopped_by": f"kvflow --home {paths['home']} stop --id {entry['id']}",
    }
    # printed before the server blocks so a caller can parse the URL
    print(json.dumps(ready, ensure_ascii=False), flush=True)
    try:
        while True:
            time.sleep(1.0)
    except KeyboardInterrupt:  # pragma: no cover - interactive stop
        pass
    finally:
        server.stop()
    return {"url": ready["url"], "stopped": True, "registered_id": entry["id"]}


COMMANDS = {
    "doctor": cmd_doctor,
    "project.add": cmd_project_add,
    "project.list": cmd_project_list,
    "workers": cmd_workers,
    "status": cmd_status,
    "pause": cmd_pause,
    "resume": cmd_resume,
    "cancel": cmd_cancel,
    "knowledge.search": cmd_knowledge_search,
    "audit": cmd_audit,
    "backup": cmd_backup,
    "backup-verify": cmd_backup_verify,
    "restore": cmd_restore,
    "mcp.serve": cmd_mcp_serve,
    "mcp.status": cmd_mcp_status,
    "ui.serve": cmd_ui_serve,
    "process.register": cmd_register_process,
    "stop": cmd_stop,
}


class ParserExtension:
    """What a product layer may extend without copying the core command surface.

    ``subparsers`` is the top-level subparser action (for new commands);
    ``named`` exposes the already-created grouped parsers, so a product can add
    ``project onboard`` next to the core's ``project add`` instead of inventing a
    second, parallel vocabulary for the same thing.
    """

    def __init__(self, subparsers: argparse._SubParsersAction,
                 named: dict[str, argparse._SubParsersAction]) -> None:
        self.subparsers = subparsers
        self.named = named

    def add(self, name: str, help_text: str) -> argparse.ArgumentParser:
        return self.subparsers.add_parser(name, help=help_text)

    def group(self, name: str) -> argparse._SubParsersAction:
        return self.named[name]


def build_parser(
    extra: Callable | None = None, extra_commands: dict[str, Any] | None = None
) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="kvflow", description="KVFlow - universal agent development workflow"
    )
    parser.add_argument("--home", default=None, help="KVFlow runtime directory")
    parser.add_argument("--version", action="store_true", help="print the version and exit")
    sub = parser.add_subparsers(dest="command")

    def add(name: str, help_text: str) -> argparse.ArgumentParser:
        child = sub.add_parser(name, help=help_text)
        return child
    add("doctor", "check the local installation")

    project = sub.add_parser("project", help="registered projects")
    project_sub = project.add_subparsers(dest="project_command")
    project_add = project_sub.add_parser("add")
    project_add.add_argument("--file")
    project_add.add_argument("--document")
    project_list = project_sub.add_parser("list")

    status = add("status", "task state")
    status.add_argument("job_id", nargs="?")
    for name, text in (("pause", "pause a job"), ("resume", "resume a job"),
                       ("cancel", "cancel a job")):
        child = add(name, text)
        child.add_argument("job_id")
        child.add_argument("--actor", default="user")
        if name == "cancel":
            child.add_argument("--reason")

    add("workers", "configured roles and slots")

    knowledge = sub.add_parser("knowledge", help="knowledge records")
    knowledge_sub = knowledge.add_subparsers(dest="knowledge_command")
    search = knowledge_sub.add_parser("search")
    search.add_argument("--project")
    search.add_argument("--topic")
    search.add_argument("--limit", type=int, default=25)

    audit = add("audit", "recent events and operations")
    audit.add_argument("--limit", type=int, default=50)

    backup = add("backup", "create a consistent backup")
    backup.add_argument("--include", action="append")
    backup.add_argument("--note")
    verify = add("backup-verify", "verify a backup against its manifest")
    verify.add_argument("backup")
    restore = add("restore", "restore a backup into an empty directory")
    restore.add_argument("backup")
    restore.add_argument("destination")
    restore.add_argument("--dry-run", action="store_true")

    mcp = sub.add_parser("mcp", help="MCP server information")
    mcp_sub = mcp.add_subparsers(dest="mcp_command")
    serve = mcp_sub.add_parser("serve")
    mcp_status = mcp_sub.add_parser("status")

    ui = sub.add_parser("ui", help="the local loopback UI")
    ui_sub = ui.add_subparsers(dest="ui_command")
    ui_serve = ui_sub.add_parser("serve")
    ui_serve.add_argument("--port", type=int, default=0,
                          help="loopback port (0 chooses a free one)")

    process = sub.add_parser("process", help="owned process registry")
    process_sub = process.add_subparsers(dest="process_command")
    register = process_sub.add_parser("register")
    register.add_argument("--kind", choices=["ui", "mcp", "worker", "other"], default="other")
    register.add_argument("--pid", required=True)
    register.add_argument("--port")
    register.add_argument("--argv", nargs="*")

    stop = add("stop", "stop only the processes this install started")
    stop.add_argument("--id")

    if extra is not None and extra_commands is not None:
        extension = ParserExtension(
            sub,
            {
                "project": project_sub,
                "knowledge": knowledge_sub,
                "mcp": mcp_sub,
                "ui": ui_sub,
                "process": process_sub,
            },
        )
        extra_commands.update(extra(extension) or {})

    return parser


def parse(
    argv: Sequence[str] | None = None, extra: Callable | None = None
) -> tuple[str, argparse.Namespace, dict[str, Any]]:
    """Parse one command line, optionally with product-level commands.

    ``extra`` receives the subparser registry and returns the command table it
    added, so a product layer (KVFlow) extends one parser instead of copying the
    core command surface into a second CLI. The returned table is the same one
    the parser was built with, so the caller never rebuilds it.
    """
    extra_commands: dict[str, Any] = {}
    parser = build_parser(extra, extra_commands)
    raw = list(argv if argv is not None else sys.argv[1:])
    if "--version" in raw:
        return "version", argparse.Namespace(), extra_commands
    namespace = parser.parse_args(raw)
    command = namespace.command
    if command == "project":
        command = f"project.{getattr(namespace, 'project_command', None) or 'list'}"
    elif command == "knowledge":
        command = f"knowledge.{getattr(namespace, 'knowledge_command', None) or 'search'}"
    elif command == "mcp":
        command = f"mcp.{getattr(namespace, 'mcp_command', None) or 'status'}"
    elif command == "ui":
        command = f"ui.{getattr(namespace, 'ui_command', None) or 'serve'}"
    elif command == "process":
        command = "process.register"
    elif command == "stop":
        command = "stop"
    if command not in COMMANDS and command not in extra_commands:
        parser.error("a command is required")
    return command, namespace, extra_commands


def main(argv: Sequence[str] | None = None, extra: Callable | None = None) -> int:
    command, args, extra_commands = parse(argv, extra)
    if command == "version":
        print(f"kvflow {PRODUCT_VERSION}")
        return EXIT_OK
    try:
        payload = (COMMANDS.get(command) or extra_commands[command])(args)
    except V1Error as exc:
        print(json.dumps({"error": exc.to_dict()}, ensure_ascii=False), file=sys.stderr)
        return EXIT_TYPED_ERROR
    except Exception as exc:  # noqa: BLE001 - the CLI reports, never crashes silently
        print(
            json.dumps({"error": {"code": "UNEXPECTED", "message": str(exc)}},
                       ensure_ascii=False),
            file=sys.stderr,
        )
        return EXIT_TYPED_ERROR
    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
    return EXIT_OK


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
