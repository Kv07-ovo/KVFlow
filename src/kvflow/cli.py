"""The ``kvflow`` command line: one entry point over one authoritative state.

The core command surface (doctor, status, pause/resume/cancel, knowledge, MCP
information, backup/restore, the local UI) lives in :mod:`kvflow.core.cli`. This
module adds the product surface on top of the *same* parser and the *same*
durable store:

``project onboard``   choose a directory, see what was detected, approve a scope
``project list/show/doctor/rebind/remove``
``template list/show``  the four declarative workflow templates
``profile list/show``   model profiles, and ``budget list/show``
``plan``               compile a template into a DAG without running anything
``run``                start the workflow for one requirement

Every product command reads and writes the registry and the store under the same
``--home``, so the CLI, the MCP server and the DSH plugin can never disagree about
which task is running.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Callable

from . import model_profiles, registry, templates
from .core import cli as core_cli
from .core.errors import ConfigError, NotFoundError, V1Error
from .core.store import Store


def _paths(home: str | None) -> dict[str, Path]:
    return core_cli.runtime_paths(home)


def _store(paths: dict[str, Path]) -> Store:
    store = Store(paths["database"])
    store.initialize()
    return store


def _home(args: argparse.Namespace) -> Path:
    return _paths(args.home)["home"]


# ------------------------------------------------------------------ project


def cmd_project_onboard(args: argparse.Namespace) -> dict[str, Any]:
    """Draft a project configuration from real files, then approve it explicitly."""
    root = Path(args.path or os.getcwd()).resolve()
    home = _home(args)
    registry_index = registry.Registry(home)
    conflicts = registry.project_conflicts(registry_index, root)
    if conflicts and not args.force:
        raise ConfigError(
            "that directory is already covered by a registered project",
            conflicts=[entry["project_id"] for entry in conflicts],
        )
    existing = registry.config_path(root)
    if existing.is_file() and not args.overwrite:
        config = registry.read_config(root)
        return {
            "action": "existing",
            "config_path": str(existing),
            "approval": registry.approval_summary(config),
            "note": "pass --overwrite to draft a replacement (a backup is kept)",
        }

    template = args.template or templates.DEFAULT_TEMPLATE
    templates.get(template, home=home)  # a bad template id fails here, not later
    budget_profile = args.budget_profile or "standard"
    registry.budget_profile(home, budget_profile)
    model_profile = args.model_profile or (
        os.environ.get(model_profiles.PROFILE_ENV) or model_profiles.DEFAULT_PROFILE
    )
    model_profiles.load(model_profile, home=home)

    config = registry.draft(
        root,
        display_name=args.display_name,
        template=template,
        model_profile=model_profile,
        budget_profile=budget_profile,
        project_id=args.id,
        adapter=args.adapter,
        write_roots=args.write_root or None,
        protected_paths=args.protect,
    )
    if config.adapter:
        # a project may only name an adapter this runtime home has enabled: the
        # refusal names the command, so "why is my adapter ignored" has one answer
        from .adapters import registry as adapters

        adapters.require_enabled(home, config.adapter)
    summary = registry.approval_summary(config)
    if not args.write:
        return {
            "action": "draft",
            "config_path": str(registry.config_path(root)),
            "approval": summary,
            "next": "review the scope above, then re-run with --write to approve it",
        }

    written = registry.write_config(config, approve=True)
    approved = registry.read_config(root)
    registered = registry_index.register(approved, store=_store(_paths(args.home)))
    return {
        "action": "registered",
        "config_path": written["config_path"],
        "backup_path": written["backup_path"],
        "approval": registry.approval_summary(approved),
        "registry_entry": registered["entry"],
        "authorization_digest": registered["entry"]["authorization_digest"],
    }


def cmd_project_list(args: argparse.Namespace) -> dict[str, Any]:
    index = registry.Registry(_home(args))
    return {"projects": index.list(), "count": len(index.list())}


def cmd_project_show(args: argparse.Namespace) -> dict[str, Any]:
    index = registry.Registry(_home(args))
    entry = index.get(args.project_id)
    root = Path(entry["canonical_root"])
    payload: dict[str, Any] = {"entry": entry, "directory_exists": root.is_dir()}
    if root.is_dir() and registry.config_path(root).is_file():
        config = registry.read_config(root)
        payload["approval"] = registry.approval_summary(config)
        payload["profiles"] = [profile.id for profile in config.profiles]
    return payload


def cmd_project_doctor(args: argparse.Namespace) -> dict[str, Any]:
    """Report registry, config and scope problems without changing anything."""
    home = _home(args)
    index = registry.Registry(home)
    entries = index.list() if not args.project_id else [index.get(args.project_id)]
    budget_table = registry.load_budgets(home)
    report: list[dict[str, Any]] = []
    for entry in entries:
        problems: list[str] = []
        root = Path(entry["canonical_root"])
        if not root.is_dir():
            problems.append("the project directory is missing")
        elif not registry.config_path(root).is_file():
            problems.append("the project-local configuration is missing")
        else:
            try:
                config = registry.read_config(root)
                if not config.approved_by_user:
                    problems.append("the configuration is not approved")
                if config.template not in templates.load_all(home=home):
                    problems.append(f"unknown workflow template {config.template!r}")
                if config.budget_profile not in budget_table:
                    problems.append(f"unknown budget profile {config.budget_profile!r}")
                for profile in config.profiles:
                    if profile.proof == "build" and not profile.argv:
                        problems.append(f"build profile {profile.id!r} has no argv")
                if entry.get("authorization_digest") is None:
                    problems.append("the entry has no usable authorization digest")
            except V1Error as exc:
                problems.append(f"configuration error: {exc}")
        conflicts = [
            other["project_id"] for other in registry.project_conflicts(index, root)
            if other["project_id"] != entry["project_id"]
        ]
        if conflicts:
            problems.append(f"overlapping registered projects: {conflicts}")
        report.append({**entry, "problems": problems, "healthy": not problems})
    return {
        "projects": report,
        "healthy": all(item["healthy"] for item in report),
        "checked": len(report),
    }


def cmd_project_rebind(args: argparse.Namespace) -> dict[str, Any]:
    home = _home(args)
    index = registry.Registry(home)
    result = index.rebind(args.project_id, args.path, store=_store(_paths(args.home)))
    return {
        **result,
        "note": (
            "the new directory needs its own approval: run"
            f" `kvflow project onboard --path {result['to']} --write` after reviewing"
            " its scope"
        ),
    }


def cmd_project_remove(args: argparse.Namespace) -> dict[str, Any]:
    index = registry.Registry(_home(args))
    result = index.remove(args.project_id, keep_data=not args.purge)
    return {
        **result,
        "note": (
            "task history, receipts and knowledge are kept unless --purge is given"
            if not args.purge else
            "the registry entry was removed; stored task data is untouched by design"
        ),
    }


# --------------------------------------------------------------- templates


def cmd_template_list(args: argparse.Namespace) -> dict[str, Any]:
    return {"templates": templates.catalogue(home=_home(args))}


def cmd_template_show(args: argparse.Namespace) -> dict[str, Any]:
    return templates.describe(templates.get(args.template_id, home=_home(args)))


# ---------------------------------------------------------------- profiles


def cmd_profile_list(args: argparse.Namespace) -> dict[str, Any]:
    home = _home(args)
    return {
        "model_profiles": model_profiles.catalogue(home=home),
        "budget_profiles": registry.load_budgets(home),
        "selected": os.environ.get(model_profiles.PROFILE_ENV) or model_profiles.DEFAULT_PROFILE,
    }


def cmd_profile_show(args: argparse.Namespace) -> dict[str, Any]:
    home = _home(args)
    profile = model_profiles.load(args.profile_id, home=home)
    return {
        "model_profile": model_profiles.describe(profile),
        "budget_profile": registry.budget_profile(home, args.budget or "standard"),
    }


# ------------------------------------------------------------------ adapters


def cmd_adapter_list(args: argparse.Namespace) -> dict[str, Any]:
    """Every known optional adapter with its opt-in state. Reads nothing else."""
    from .adapters import registry as adapters

    home = _home(args)
    rows = adapters.describe(home)
    return {
        "adapters": rows,
        "enabled": [row["adapter"] for row in rows if row["enabled"]],
        "home": str(home),
        "policy": "optional adapters are deny-by-default and read-only",
    }


def cmd_adapter_enable(args: argparse.Namespace) -> dict[str, Any]:
    """Enable one adapter for this runtime home, recording the approved roots."""
    from .adapters import registry as adapters

    home = _home(args)
    options: dict[str, Any] = {}
    if args.root:
        options["root"] = str(Path(args.root).resolve())
    if args.source:
        options["sources"] = [json.loads(item) for item in args.source]
    entry = adapters.enable(home, args.adapter_id, options=options,
                            reason=args.reason or "")
    written = adapters.describe(home)
    return {
        "action": "enabled",
        "entry": entry,
        "adapters": written,
        "note": (
            "enabling grants read-only access through this runtime home; the adapter"
            " cannot write to the adapted project"
        ),
    }


def cmd_adapter_disable(args: argparse.Namespace) -> dict[str, Any]:
    from .adapters import registry as adapters

    return {**adapters.disable(_home(args), args.adapter_id), "action": "disabled"}


def cmd_adapter_show(args: argparse.Namespace) -> dict[str, Any]:
    """What the enabled adapter can actually reach, or the refusal that says why not."""
    from .adapters import kvstock_adapter as adapter

    home = _home(args)
    entry = adapter.enabled_entry(home, args.adapter_id)
    return adapter.describe(entry)


def cmd_adapter_status(args: argparse.Namespace) -> dict[str, Any]:
    from .adapters import kvstock_adapter as adapter

    entry = adapter.enabled_entry(_home(args), args.adapter_id)
    return adapter.status(entry)


def cmd_adapter_read(args: argparse.Namespace) -> dict[str, Any]:
    from .adapters import kvstock_adapter as adapter

    entry = adapter.enabled_entry(_home(args), args.adapter_id)
    return adapter.read(
        entry, args.source_key, selector=args.selector, offset=args.offset,
        limit=args.limit, prefix=args.prefix or "", cursor=args.cursor,
        page_limit=args.page_limit,
    )


# --------------------------------------------------- host install lifecycle


def cmd_install(args: argparse.Namespace) -> dict[str, Any]:
    """Bind the plugin into one DSH profile, with a backup and a verification."""
    from . import install as host_install

    # The host reads this path from its profile configuration, possibly from a
    # completely different working directory, so an install never records a
    # cwd-relative runtime home. An explicit --home still wins.
    home = (Path(args.home).expanduser().resolve() if args.home
            else Path.home() / ".kvflow")
    python = Path(args.python).resolve() if args.python else Path(sys.executable)
    python_path = Path(args.python_path).resolve() if args.python_path else Path(
        __file__).resolve().parents[1]
    workspace = Path(args.workspace).resolve() if args.workspace else None
    credentials = Path(args.credentials).resolve() if args.credentials else None
    return host_install.install(
        profile=args.profile, home=home, python=python, python_path=python_path,
        workspace=workspace, credentials=credentials, version=_version(),
        upgrade=bool(getattr(args, "upgrade", False)) or args.command == "upgrade",
        dry_run=bool(args.dry_run),
    )


def cmd_uninstall(args: argparse.Namespace) -> dict[str, Any]:
    from . import install as host_install

    purge = Path(args.purge_home).resolve() if args.purge_home else None
    return host_install.uninstall(
        profile=args.profile, version=_version(), purge_home=purge,
        dry_run=bool(args.dry_run),
    )


def cmd_host_status(args: argparse.Namespace) -> dict[str, Any]:
    from . import install as host_install

    return host_install.status(args.profile)


def _version() -> str:
    from . import __version__

    return __version__


# -------------------------------------------------------------------- plan


def cmd_plan(args: argparse.Namespace) -> dict[str, Any]:
    """Compile a template into a DAG. Nothing is executed and no model is called."""
    from . import planner

    home = _home(args)
    index = registry.Registry(home)
    entry = index.get(args.project_id)
    config = registry.read_config(entry["canonical_root"])
    template = templates.get(args.template or config.template, home=home)
    compiled = planner.compile_plan(
        requirement=args.requirement,
        template=template,
        config=config,
        job_id="plan-preview",
    )
    return {**compiled, "executed": False, "note": "preview only: no job was created"}


def cmd_run(args: argparse.Namespace) -> dict[str, Any]:
    """Start one workflow for one requirement and report its durable identity."""
    from . import workflow

    home = _home(args)
    return workflow.run_requirement(
        requirement=args.requirement,
        project_id=args.project_id,
        home=home,
        template_id=args.template,
        max_steps=args.max_steps,
        max_completion_tokens=args.max_completion_tokens,
        dry_run=bool(args.dry_run),
        max_nodes=args.max_nodes,
    )


# ------------------------------------------------------------------ wiring


def cmd_bridge(args: argparse.Namespace) -> dict[str, Any]:
    """Call one KVFlow tool by name with JSON arguments.

    This is the entrance a host plugin uses: the plugin stays a thin adapter and
    the tool implementation lives in exactly one place (:mod:`kvflow.api`), which
    the MCP server also serves.
    """
    from . import api

    try:
        arguments = json.loads(args.args) if args.args else {}
    except ValueError as exc:
        raise ConfigError("--args must be a JSON object", problem=str(exc)[:200]) from exc
    if not isinstance(arguments, dict):
        raise ConfigError("--args must be a JSON object")
    return api.invoke(args.tool, arguments, home=args.home)


def cmd_runs(args: argparse.Namespace) -> dict[str, Any]:
    from . import api

    return api.invoke("kvflow_runs", {"project_id": args.project, "limit": args.limit},
                      home=args.home)


def cmd_result(args: argparse.Namespace) -> dict[str, Any]:
    from . import api

    return api.invoke("kvflow_result", {"job_id": args.job_id}, home=args.home)


def cmd_coordinate_show(args: argparse.Namespace) -> dict[str, Any]:
    """What semantic state exists, and what is blocking it."""
    from . import api

    return api.invoke("kvflow_coordination",
                      {"project_id": args.project_id, "job_id": args.job_id},
                      home=args.home)


def cmd_coordinate_gate(args: argparse.Namespace) -> dict[str, Any]:
    """The final gate for one job's current node states, as the program sees it."""
    from . import api, workflow
    from .core.store import Store

    home = _home(args)
    payload = api.invoke("kvflow_coordination",
                         {"project_id": args.project_id, "job_id": args.job_id},
                         home=args.home)
    node_states: dict[str, str] = {}
    if args.job_id:
        store = Store(core_cli.database_path(home))
        store.initialize()
        from .core.scheduler import Scheduler

        node_states = {key: value.value
                       for key, value in Scheduler(store).node_states(args.job_id).items()}
    return {"project_id": args.project_id, "job_id": args.job_id,
            "node_states": node_states, "coordination": payload,
            "note": ("a failing check here cannot be overridden by a manager verdict;"
                     " only a re-run, a rebase or a compatibility confirmation with"
                     " evidence can clear it")}


def cmd_tools(args: argparse.Namespace) -> dict[str, Any]:
    from . import api

    return {
        "tools": [
            {"name": spec["name"], "description": spec["description"],
             "schema": spec["schema"]}
            for spec in api.TOOLS
        ],
        "count": len(api.TOOLS),
        "runtime": str(api.home_from(args.home)),
    }


def add_product_commands(extension) -> dict[str, Callable]:
    """Register the product command surface on the core parser.

    ``project onboard`` and friends sit next to the core's ``project add/list``,
    so a user has one vocabulary for one concept.
    """
    sub = extension.subparsers
    project_sub = extension.group("project")

    onboard = project_sub.add_parser("onboard")
    onboard.add_argument("--path", default=None)
    onboard.add_argument("--display-name")
    onboard.add_argument("--template")
    onboard.add_argument("--model-profile")
    onboard.add_argument("--budget-profile")
    onboard.add_argument("--adapter")
    onboard.add_argument("--write-root", action="append")
    onboard.add_argument("--protect", action="append")
    onboard.add_argument("--id")
    onboard.add_argument("--write", action="store_true",
                         help="approve the drafted scope and register the project")
    onboard.add_argument("--overwrite", action="store_true")
    onboard.add_argument("--force", action="store_true")
    show = project_sub.add_parser("show")
    show.add_argument("project_id")
    doctor = project_sub.add_parser("doctor")
    doctor.add_argument("project_id", nargs="?")
    rebind = project_sub.add_parser("rebind")
    rebind.add_argument("project_id")
    rebind.add_argument("--path", required=True)
    remove = project_sub.add_parser("remove")
    remove.add_argument("project_id")
    remove.add_argument("--purge", action="store_true")

    def add(name: str, help_text: str) -> argparse.ArgumentParser:
        return sub.add_parser(name, help=help_text)

    template = add("template", "workflow templates")
    template_sub = template.add_subparsers(dest="template_command")
    template_sub.add_parser("list")
    template_show = template_sub.add_parser("show")
    template_show.add_argument("template_id")

    profile = add("profile", "model and budget profiles")
    profile_sub = profile.add_subparsers(dest="profile_command")
    profile_sub.add_parser("list")
    profile_show = profile_sub.add_parser("show")
    profile_show.add_argument("profile_id", nargs="?", default=None)
    profile_show.add_argument("--budget", default=None)

    adapter = add("adapter", "optional, explicitly enabled project adapters")
    adapter_sub = adapter.add_subparsers(dest="adapter_command")
    adapter_sub.add_parser("list")
    adapter_enable = adapter_sub.add_parser("enable")
    adapter_enable.add_argument("adapter_id", choices=["kvstock"])
    adapter_enable.add_argument("--root", default=None,
                                help="the only readable tree for this adapter")
    adapter_enable.add_argument("--source", action="append", default=[],
                                help="one JSON SourceSpec, repeatable")
    adapter_enable.add_argument("--reason", default="")
    adapter_disable = adapter_sub.add_parser("disable")
    adapter_disable.add_argument("adapter_id", choices=["kvstock"])
    adapter_show = adapter_sub.add_parser("show")
    adapter_show.add_argument("adapter_id", nargs="?", default="kvstock")
    adapter_status = adapter_sub.add_parser("status")
    adapter_status.add_argument("adapter_id", nargs="?", default="kvstock")
    adapter_read = adapter_sub.add_parser("read")
    adapter_read.add_argument("source_key")
    adapter_read.add_argument("--adapter", dest="adapter_id", default="kvstock")
    adapter_read.add_argument("--selector", default=None)
    adapter_read.add_argument("--prefix", default=None)
    adapter_read.add_argument("--cursor", default=None)
    adapter_read.add_argument("--offset", type=int, default=0)
    adapter_read.add_argument("--limit", type=int, default=4096)
    adapter_read.add_argument("--page-limit", type=int, default=50)

    install_cmd = add("install", "bind the plugin into one DSH profile")
    install_cmd.add_argument("--profile", default=None)
    install_cmd.add_argument("--python", default=None,
                             help="the interpreter the host plugin should run")
    install_cmd.add_argument("--python-path", default=None,
                             help="the import root for that interpreter")
    install_cmd.add_argument("--workspace", default=None,
                             help="default workspace directory for the host tools")
    install_cmd.add_argument("--credentials", default=None,
                             help="read-only path to the host credential store"
                                  " (recorded as a pointer, never a secret)")
    install_cmd.add_argument("--dry-run", action="store_true")
    upgrade_cmd = add("upgrade", "re-install the plugin after the checkout changed")
    upgrade_cmd.add_argument("--profile", default=None)
    upgrade_cmd.add_argument("--python", default=None)
    upgrade_cmd.add_argument("--python-path", default=None)
    upgrade_cmd.add_argument("--workspace", default=None)
    upgrade_cmd.add_argument("--credentials", default=None)
    upgrade_cmd.add_argument("--dry-run", action="store_true")
    uninstall_cmd = add("uninstall", "detach the plugin, restoring the profile files")
    uninstall_cmd.add_argument("--profile", default=None)
    uninstall_cmd.add_argument("--purge-home", default=None,
                               help="report (never delete) this runtime home")
    uninstall_cmd.add_argument("--dry-run", action="store_true")
    host = add("host", "the DSH host integration status")
    host_sub = host.add_subparsers(dest="host_command")
    host_status = host_sub.add_parser("status")
    host_status.add_argument("--profile", default=None)

    plan = add("plan", "compile a template into a DAG without running it")
    plan.add_argument("requirement")
    plan.add_argument("--project", dest="project_id", required=True)
    plan.add_argument("--template")

    run = add("run", "start one workflow for one requirement")
    run.add_argument("requirement")
    run.add_argument("--project", dest="project_id", required=True)
    run.add_argument("--template")
    run.add_argument("--max-steps", type=int, default=10)
    run.add_argument("--max-completion-tokens", type=int, default=1024)
    run.add_argument("--max-nodes", type=int, default=None)
    run.add_argument("--dry-run", action="store_true")

    runs = add("runs", "recent workflow runs")
    runs.add_argument("--project", dest="project", default=None)
    runs.add_argument("--limit", type=int, default=25)

    result = add("result", "the evidence of one finished run")
    result.add_argument("job_id")

    coordinate = add("coordinate", "semantic coordination state and the final gate")
    coordinate_sub = coordinate.add_subparsers(dest="coordinate_command")
    coordinate_show = coordinate_sub.add_parser("show")
    coordinate_show.add_argument("--project", dest="project_id", required=True)
    coordinate_show.add_argument("--job", dest="job_id", default=None)
    coordinate_gate = coordinate_sub.add_parser("gate")
    coordinate_gate.add_argument("--project", dest="project_id", required=True)
    coordinate_gate.add_argument("--job", dest="job_id", default=None)

    tools = add("tools", "the tool surface this runtime exposes to hosts")
    bridge = add("bridge", "call one KVFlow tool with JSON arguments (host adapter)")
    bridge.add_argument("--tool", required=True)
    bridge.add_argument("--args", default="{}")

    return {
        "project.onboard": cmd_project_onboard,
        "project.show": cmd_project_show,
        "project.doctor": cmd_project_doctor,
        "project.rebind": cmd_project_rebind,
        "project.remove": cmd_project_remove,
        "registry": cmd_project_list,
        "template": lambda args: {
            "list": cmd_template_list,
            "show": cmd_template_show,
        }[getattr(args, "template_command", None) or "list"](args),
        "profile": lambda args: {
            "list": cmd_profile_list,
            "show": cmd_profile_show,
        }[getattr(args, "profile_command", None) or "list"](args),
        "adapter": lambda args: {
            "list": cmd_adapter_list,
            "enable": cmd_adapter_enable,
            "disable": cmd_adapter_disable,
            "show": cmd_adapter_show,
            "status": cmd_adapter_status,
            "read": cmd_adapter_read,
        }[getattr(args, "adapter_command", None) or "list"](args),
        "install": cmd_install,
        "upgrade": cmd_install,
        "uninstall": cmd_uninstall,
        "host": lambda args: {
            "status": cmd_host_status,
        }[getattr(args, "host_command", None) or "status"](args),
        "plan": cmd_plan,
        "run": cmd_run,
        "runs": cmd_runs,
        "result": cmd_result,
        "coordinate": lambda args: {
            "show": cmd_coordinate_show,
            "gate": cmd_coordinate_gate,
        }[getattr(args, "coordinate_command", None) or "show"](args),
        "tools": cmd_tools,
        "bridge": cmd_bridge,
    }


def main(argv: list[str] | None = None) -> int:
    return core_cli.main(argv, extra=add_product_commands)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
