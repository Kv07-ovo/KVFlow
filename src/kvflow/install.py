"""Install, upgrade and uninstall the KVFlow host integration.

KVFlow reaches a DSH host as a *profile bundle*: the profile's ``package.json``
gets the plugin as a dependency and lists it in ``dsh.profile.bundles``, and the
profile's ``cordis.patch.yml`` gets one ``- id: kvflow`` config row. This module
performs that edit for one named profile, on the real profile directory, with a
backup taken first and a verification pass afterwards.

What it deliberately does not do
--------------------------------

* It never starts, restarts or kills a DSH process. A host picks the bundle up
  when it next loads the profile; the reported ``host_restart_required`` says so
  instead of pretending the change is live.
* It never edits a profile it was not asked about, and it never deletes another
  plugin's rows: the KVFlow patch row is the only row it writes or removes.
* It refuses to guess. A missing profile, a missing plugin directory, a Python
  that cannot import KVFlow, or a missing ``dsh`` CLI are reported as refusals
  with the exact command to run, and any partial change is rolled back from the
  backup taken in the same call.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from .core.errors import ConfigError, ContractError, NotFoundError

#: the dependency name the plugin is published under inside a profile
PLUGIN_PACKAGE = "kvflow-dsh"

#: the Cordis plugin id this product owns in ``cordis.patch.yml``
PLUGIN_ID = "kvflow"

PATCH_HEADER = (
    "# KVFlow host integration (written by `kvflow install`).\n"
    "# This is the only row KVFlow adds or removes; every other plugin, model and\n"
    "# proxy setting in this file is left untouched. Deleting the row (or setting\n"
    "# disabled: true) detaches the plugin without uninstalling anything.\n"
)

INSTALL_TIMEOUT_SECONDS = 900


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")


def _tail(text: str | None, limit: int = 400) -> str:
    """The printable end of a tool's output, for the receipt.

    Package managers print progress bars, ANSI escapes and box-drawing glyphs; the
    receipt keeps the informative text and drops the control characters and
    anything the local console cannot encode.
    """
    if not text:
        return ""
    cleaned = re.sub(r"\x1b\[[0-9;?]*[A-Za-z]", "", text)
    cleaned = "".join(
        character if (character.isprintable() or character in "\r\n\t") else " "
        for character in cleaned
    )
    cleaned = re.sub(r"[^\x20-\x7e\r\n\t]", "?", cleaned)
    return cleaned[-limit:]


def dsh_root() -> Path:
    override = os.environ.get("DSH_HOME")
    return Path(override) if override else Path.home() / ".dsh"


def profiles_root() -> Path:
    return dsh_root() / "profiles"


def list_profiles() -> list[str]:
    root = profiles_root()
    if not root.is_dir():
        return []
    return sorted(
        entry.name for entry in root.iterdir()
        if entry.is_dir() and (entry / "package.json").is_file()
        and not entry.name.startswith(".")
    )


def resolve_profile(name: str | None) -> tuple[str, Path]:
    """Resolve the profile to act on, refusing rather than guessing."""
    available = list_profiles()
    candidate = name or os.environ.get("KVFLOW_DSH_PROFILE") or None
    if candidate is None:
        if len(available) == 1:
            candidate = available[0]
        else:
            raise ConfigError(
                "no DSH profile was named and it is not obvious which one to use",
                profiles=available,
                hint="pass --profile NAME (or set KVFLOW_DSH_PROFILE)",
            )
    directory = profiles_root() / candidate
    if not (directory / "package.json").is_file():
        raise NotFoundError(
            f"the DSH profile {candidate!r} has no package.json",
            profile=candidate, profiles=available, root=str(directory),
        )
    return candidate, directory


def plugin_source() -> Path:
    """The plugin directory shipped next to this package."""
    root = Path(__file__).resolve().parents[2]
    candidate = root / "dsh-plugin"
    if not (candidate / "package.json").is_file():
        raise NotFoundError(
            "this installation has no dsh-plugin directory next to the package",
            expected=str(candidate),
            hint="install KVFlow from its source checkout, or run it from the checkout",
        )
    return candidate


def dsh_cli() -> str:
    found = shutil.which("dsh")
    if not found:
        raise NotFoundError(
            "the dsh CLI is not on PATH, so the profile bundle cannot be installed",
            hint="run KVFlow's install from a shell where `dsh` works",
        )
    return found


def compose_config(cli: str, profile: str, *, patch: Path | None = None,
                   timeout: int = INSTALL_TIMEOUT_SECONDS) -> dict[str, Any]:
    """Ask the host to compose one profile and report what it produced.

    This is the only check that proves the written patch is valid: the host parses
    its own YAML, loads every bundle and prints the composed tree, so a malformed
    row, a missing bundle or a bad config value shows up here and nowhere earlier.
    """
    argv = [cli, "--profile", profile]
    if patch is not None:
        argv += ["--patch", str(patch)]
    argv.append("--dump-config")
    try:
        completed = subprocess.run(
            argv, capture_output=True, text=True, timeout=timeout, check=False,
            encoding="utf-8", errors="replace",
        )
    except (OSError, subprocess.SubprocessError) as exc:
        return {"returncode": -1, "stdout": "", "stderr": f"{type(exc).__name__}: {exc}",
                "kvflow_row": False, "argv": argv}
    text = f"{completed.stdout or ''}\n{completed.stderr or ''}"
    return {
        "returncode": completed.returncode,
        "stdout": completed.stdout or "",
        "stderr": completed.stderr or "",
        "kvflow_row": f"- id: {PLUGIN_ID}" in text and PLUGIN_PACKAGE in text,
        "argv": argv,
    }


# --------------------------------------------------------------- profile files


def read_profile(profile_dir: Path) -> dict[str, Any]:
    path = profile_dir / "package.json"
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ConfigError(
            f"the profile package.json is unreadable: {type(exc).__name__}"
        ) from exc
    if not isinstance(document, dict):
        raise ConfigError("the profile package.json is not an object")
    return document


def profile_bundles(document: Mapping[str, Any]) -> list[str]:
    dsh = document.get("dsh")
    profile = dsh.get("profile") if isinstance(dsh, Mapping) else None
    bundles = profile.get("bundles") if isinstance(profile, Mapping) else None
    return [str(item) for item in bundles] if isinstance(bundles, list) else []


def read_patch(profile_dir: Path) -> str:
    path = profile_dir / "cordis.patch.yml"
    if not path.is_file():
        return ""
    return path.read_text(encoding="utf-8")


def plugin_config(home: Path, python: Path, python_path: Path | None,
                  workspace: Path | None, credentials: Path | None = None) -> dict[str, Any]:
    """The config row the plugin reads, as YAML-safe plain scalars.

    ``credentials`` is the read-only pointer to the host's credential store. The
    desktop host does not export DEEPSEEK_API_KEY to the children it spawns, so
    without this pointer the plugin's KVFlow child has no provider credential and
    every workflow is refused with AuthorityDenied - the value is a path, never a
    secret, and KVFlow reads it exactly the way it reads KVFLOW_CREDENTIALS.
    """
    return {
        "home": str(home).replace("\\", "/"),
        "python": str(python).replace("\\", "/"),
        "pythonPath": (str(python_path).replace("\\", "/") if python_path else None),
        "workspace": (str(workspace).replace("\\", "/") if workspace else None),
        "credentials": (str(credentials).replace("\\", "/") if credentials else None),
    }


def _render_row(config: Mapping[str, Any]) -> str:
    lines = [f"- id: {PLUGIN_ID}", "  config:"]
    for key in ("home", "python", "pythonPath", "workspace", "credentials"):
        value = config.get(key)
        if value is None:
            lines.append(f"    {key}: null")
        else:
            lines.append(f'    {key}: "{value}"')
    return "\n".join(lines) + "\n"


def patch_row_blocks(text: str) -> list[str]:
    """Split a patch file into top-level ``- `` blocks, preserving everything else."""
    blocks: list[str] = []
    current: list[str] = []
    for line in text.splitlines(keepends=True):
        if line.startswith("- ") and current:
            blocks.append("".join(current))
            current = [line]
        else:
            current.append(line)
    if current:
        blocks.append("".join(current))
    return blocks


def has_kvflow_row(text: str) -> bool:
    return any(
        re.match(rf"^- id:\s*{re.escape(PLUGIN_ID)}\s*$", block.splitlines()[0], re.M)
        for block in patch_row_blocks(text) if block.startswith("- ")
    )


def _strip_kvflow_header(text: str) -> str:
    """Drop this module's own comment banner so repeated installs do not stack it."""
    if "written by `kvflow install`" not in text:
        return text
    lines = PATCH_HEADER.splitlines()
    return "".join(
        line for line in text.splitlines(keepends=True)
        if line.rstrip("\r\n") not in lines
    )


def write_kvflow_row(text: str, config: Mapping[str, Any]) -> str:
    """Insert or replace the single KVFlow block, leaving every other block as is.

    A bare ``[]`` is a complete YAML document, so a row appended after it would be
    a second document and the host would refuse the whole file. Since this writer
    always guarantees the KVFlow row exists, that empty-document marker is removed
    rather than kept, and the module's own banner is rewritten instead of stacked.
    """
    blocks = patch_row_blocks(_strip_kvflow_header(text))
    kept: list[str] = []
    replaced = False
    for block in blocks:
        if block.startswith("- "):
            first = block.splitlines()[0]
            if re.match(rf"^- id:\s*{re.escape(PLUGIN_ID)}\s*$", first):
                if not replaced:
                    kept.append(_render_row(config))
                    replaced = True
                continue
            kept.append(block)
            continue
        kept.append(re.sub(r"(?m)^[ \t]*\[\][ \t]*$\n?", "", block))
    body = "".join(kept)
    row_text = _render_row(config)
    if not replaced:
        if body and not body.endswith("\n"):
            body += "\n"
        if body and not body.endswith("\n\n"):
            body += "\n"
        body += row_text
    # exactly one banner, immediately above the one row this module owns
    body = body.replace(row_text, PATCH_HEADER + row_text, 1)
    return body


def remove_kvflow_row(text: str) -> str:
    kept: list[str] = []
    for block in patch_row_blocks(text):
        if block.startswith("- "):
            first = block.splitlines()[0]
            if re.match(rf"^- id:\s*{re.escape(PLUGIN_ID)}\s*$", first):
                continue
        kept.append(block)
    return "".join(kept)


# ------------------------------------------------------------------ inspection


def host_processes() -> list[dict[str, Any]]:
    """Running DSH hosts, so the caller learns a restart is needed rather than being
    surprised. Reading a process list never stops anything."""
    if os.name != "nt":  # pragma: no cover - the shipped host is Windows
        return []
    try:
        completed = subprocess.run(
            ["tasklist", "/FO", "CSV", "/NH"],
            capture_output=True, text=True, timeout=60, check=False,
            encoding="utf-8", errors="replace",
        )
    except (OSError, subprocess.SubprocessError):  # pragma: no cover - best effort
        return []
    rows: list[dict[str, Any]] = []
    for line in completed.stdout.splitlines():
        parts = [part.strip('"') for part in line.split('","')]
        if len(parts) < 2:
            continue
        if "dsh" in parts[0].casefold() or "deepseek" in parts[0].casefold():
            rows.append({"image": parts[0], "pid": parts[1]})
    return rows


def status(profile: str | None = None) -> dict[str, Any]:
    """What the host integration looks like right now, without changing it."""
    name, directory = resolve_profile(profile)
    document = read_profile(directory)
    patch = read_patch(directory)
    bundles = profile_bundles(document)
    dependencies = document.get("dependencies") if isinstance(document.get("dependencies"), dict) else {}
    installed_dependency = PLUGIN_PACKAGE in dependencies
    installed_bundle = PLUGIN_PACKAGE in bundles
    installed_row = has_kvflow_row(patch)
    backups = sorted(
        entry.name for entry in directory.iterdir()
        if entry.is_dir() and entry.name.startswith(".kvflow-install-backup-")
    )
    return {
        "profile": name,
        "profile_dir": str(directory),
        "dependency": dependencies.get(PLUGIN_PACKAGE),
        "bundles_contains_plugin": installed_bundle,
        "patch_row": installed_row,
        "installed": bool(installed_dependency and installed_bundle and installed_row),
        "backups": backups,
        "hosts_running": host_processes(),
        "host_restart_required": bool(installed_dependency or installed_bundle or installed_row),
        "plugin_source": _plugin_source_or_none(),
        "dsh_home": str(dsh_root()),
    }


def _plugin_source_or_none() -> str | None:
    try:
        return str(plugin_source())
    except NotFoundError:
        return None


def verify(python: Path, *, home: Path, timeout: int = 120) -> dict[str, Any]:
    """Prove the configured Python can import and drive KVFlow before writing.

    A wrong interpreter is the single most common broken installation, so it is
    checked by running it, not by trusting the path.
    """
    if not python.is_file():
        raise NotFoundError("the configured Python interpreter does not exist",
                            python=str(python))
    script = (
        "import json,sys;"
        "import kvflow;"
        "from kvflow import api;"
        "print(json.dumps({'version': kvflow.__version__,"
        " 'module': kvflow.__file__, 'tools': len(api.TOOLS),"
        " 'python': sys.version.split()[0]}))"
    )
    environment = dict(os.environ)
    package_root = str(Path(__file__).resolve().parents[1])
    if package_root not in environment.get("PYTHONPATH", "").split(os.pathsep):
        environment["PYTHONPATH"] = os.pathsep.join(
            [part for part in (package_root, environment.get("PYTHONPATH", "")) if part]
        )
    environment.setdefault("KVFLOW_HOME", str(home))
    try:
        completed = subprocess.run(
            [str(python), "-c", script],
            capture_output=True, text=True, timeout=timeout,
            env=environment, check=False, encoding="utf-8", errors="replace",
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise ConfigError(f"the configured Python could not be run: {type(exc).__name__}") from exc
    if completed.returncode != 0:
        raise ConfigError(
            "the configured Python cannot import kvflow",
            python=str(python), returncode=completed.returncode,
            stderr=_tail(completed.stderr, 600),
            hint="pass --python-path pointing at the checkout's src directory",
        )
    try:
        report = json.loads(completed.stdout.strip().splitlines()[-1])
    except (ValueError, IndexError) as exc:
        raise ConfigError("the Python check produced no usable report") from exc
    return report


# --------------------------------------------------------------------- backup


def take_backup(profile_dir: Path, *, version: str, note: str) -> Path:
    # a backup name is never reused: two operations inside the same second get
    # distinct directories rather than one overwriting the other
    stamp = utc_stamp()
    target = profile_dir / f".kvflow-install-backup-{stamp}"
    suffix = 1
    while target.exists():
        suffix += 1
        target = profile_dir / f".kvflow-install-backup-{stamp}-{suffix}"
    target.mkdir(parents=True)
    copied: list[str] = []
    for name in ("package.json", "cordis.patch.yml", "cordis.yml"):
        source = profile_dir / name
        if source.is_file():
            shutil.copy2(source, target / name)
            copied.append(name)
    (target / "manifest.json").write_text(
        json.dumps(
            {
                "kind": "KVFLOW_DSH_PROFILE_BACKUP",
                "created_at": datetime.now(timezone.utc).isoformat(),
                "profile_dir": str(profile_dir),
                "plugin_package": PLUGIN_PACKAGE,
                "kvflow_version": version,
                "copied": copied,
                "note": note,
            },
            ensure_ascii=False, indent=2,
        ),
        encoding="utf-8",
    )
    return target


def restore_backup(backup: Path, profile_dir: Path) -> dict[str, Any]:
    if not backup.is_dir():
        raise NotFoundError("the backup directory does not exist", path=str(backup))
    restored: list[str] = []
    for name in ("package.json", "cordis.patch.yml", "cordis.yml"):
        source = backup / name
        if source.is_file():
            shutil.copy2(source, profile_dir / name)
            restored.append(name)
    return {"restored": restored, "from": str(backup)}


# -------------------------------------------------------------------- actions


def plan(profile: str | None, *, home: Path, python: Path, python_path: Path | None,
         workspace: Path | None, credentials: Path | None = None) -> dict[str, Any]:
    """Exactly what an install would write, without writing it."""
    name, directory = resolve_profile(profile)
    document = read_profile(directory)
    patch = read_patch(directory)
    config = plugin_config(home, python, python_path, workspace, credentials)
    return {
        "profile": name,
        "profile_dir": str(directory),
        "plugin_source": str(plugin_source()),
        "dependency": {PLUGIN_PACKAGE: f"link:{plugin_source()}"},
        "bundle_listed": PLUGIN_PACKAGE in profile_bundles(document),
        "patch_row_present": has_kvflow_row(patch),
        "config": config,
        "hosts_running": host_processes(),
        "note": "no file was written by this plan",
    }


def install(
    *,
    profile: str | None,
    home: Path,
    python: Path,
    python_path: Path | None,
    workspace: Path | None,
    version: str,
    credentials: Path | None = None,
    upgrade: bool = False,
    dry_run: bool = False,
    timeout: int = INSTALL_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    """Patch one profile so the host loads the plugin, with a backup and a verify."""
    name, directory = resolve_profile(profile)
    source = plugin_source()
    manifest = json.loads((source / "package.json").read_text(encoding="utf-8"))
    if not isinstance(manifest.get("dsh"), Mapping) or "bundle" not in manifest["dsh"]:
        raise ContractError("the plugin package does not declare a dsh.bundle patch")
    python_report = verify(python, home=home)
    config = plugin_config(home, python, python_path, workspace, credentials)
    before = read_profile(directory)
    already = (
        PLUGIN_PACKAGE in (before.get("dependencies") or {})
        and PLUGIN_PACKAGE in profile_bundles(before)
        and has_kvflow_row(read_patch(directory))
    )
    if upgrade and not already:
        raise ConfigError(
            "there is no installed KVFlow integration to upgrade",
            profile=name, hint="run `kvflow install` first",
        )
    if dry_run:
        return {
            "action": "dry_run",
            "upgrade": upgrade,
            "already_installed": already,
            "plan": plan(name, home=home, python=python, python_path=python_path,
                         workspace=workspace, credentials=credentials),
            "python_check": python_report,
        }

    backup = take_backup(directory, version=version,
                         note="before `kvflow install`" if not upgrade else "before `kvflow upgrade`")
    steps: list[dict[str, Any]] = []
    try:
        cli = dsh_cli()
        completed = subprocess.run(
            [cli, "plugin", "--profile", name, "add", str(source)],
            capture_output=True, text=True, timeout=timeout, check=False,
            cwd=str(directory), encoding="utf-8", errors="replace",
        )
        steps.append({
            "step": "dsh plugin add",
            "argv": [cli, "plugin", "--profile", name, "add", str(source)],
            "returncode": completed.returncode,
            "stdout_tail": _tail(completed.stdout),
            "stderr_tail": _tail(completed.stderr),
        })
        if completed.returncode != 0:
            raise ConfigError(
                "`dsh plugin add` failed", returncode=completed.returncode,
                stderr=_tail(completed.stderr, 600),
            )
        patch_path = directory / "cordis.patch.yml"
        patch_path.write_text(
            write_kvflow_row(read_patch(directory), config), encoding="utf-8"
        )
        steps.append({"step": "cordis.patch.yml", "written": str(patch_path)})
        # The host itself is the only authority on whether the composed profile is
        # valid, so the freshly written patch is composed by `dsh --dump-config`
        # before this install claims success.
        composed = compose_config(cli, name, timeout=timeout)
        steps.append({
            "step": "dsh --dump-config",
            "returncode": composed["returncode"],
            "kvflow_row_composed": composed["kvflow_row"],
            "stderr_tail": _tail(composed["stderr"]),
        })
        if composed["returncode"] != 0 or not composed["kvflow_row"]:
            raise ConfigError(
                "the host could not compose the profile after the patch was written",
                returncode=composed["returncode"],
                stderr=_tail(composed["stderr"], 600),
            )
    except Exception as exc:  # noqa: BLE001 - a failed install is rolled back, not left half done
        restored = restore_backup(backup, directory)
        return {
            "action": "install_failed",
            "profile": name,
            "error": f"{type(exc).__name__}: {exc}",
            "steps": steps,
            "backup": str(backup),
            "rolled_back": restored,
            "verified": False,
        }

    after = inspect_integration(name, home=home, python=python, python_path=python_path)
    return {
        "action": "upgraded" if upgrade else "installed",
        "profile": name,
        "profile_dir": str(directory),
        "plugin_source": str(source),
        "config": config,
        "binding": after,
        "steps": steps,
        "backup": str(backup),
        "python_check": python_report,
        "verified": bool(after["verified"]),
    }


def inspect_integration(name: str, *, home: Path | None = None,
                        python: Path | None = None,
                        python_path: Path | None = None) -> dict[str, Any]:
    """Re-read the profile and report whether the plugin is really bound.

    The binding is what the *profile* says, so a detach reports no home or
    interpreter rather than echoing the values the caller happened to pass in.
    """
    _, directory = resolve_profile(name)
    document = read_profile(directory)
    patch = read_patch(directory)
    dependencies = document.get("dependencies") if isinstance(document.get("dependencies"), dict) else {}
    dependency = dependencies.get(PLUGIN_PACKAGE)
    bundle = PLUGIN_PACKAGE in profile_bundles(document)
    row = has_kvflow_row(patch)
    config: dict[str, Any] = {}
    for block in patch_row_blocks(patch):
        if block.startswith("- ") and re.match(rf"^- id:\s*{re.escape(PLUGIN_ID)}\s*$",
                                               block.splitlines()[0]):
            for line in block.splitlines()[1:]:
                match = re.match(r"^\s{4}([A-Za-z]+):\s*(.*)$", line)
                if match:
                    value = match.group(2).strip().strip('"')
                    config[match.group(1)] = None if value == "null" else value
    # the configured home and interpreter come from the row when it exists, so the
    # report describes the profile rather than the caller's arguments
    return {
        "profile": name,
        "dependency": dependency,
        "bundle_listed": bundle,
        "patch_row": row,
        "config": config or None,
        "python": config.get("python") or (str(python) if python else None),
        "python_path": config.get("pythonPath") or (str(python_path) if python_path else None),
        "home": config.get("home") or (str(home) if home else None),
        "installed": bool(dependency and bundle and row),
        "verified": bool(dependency and bundle and row and config.get("python")),
    }


def uninstall(*, profile: str | None, version: str, purge_home: Path | None = None,
              dry_run: bool = False, timeout: int = INSTALL_TIMEOUT_SECONDS) -> dict[str, Any]:
    """Detach the plugin from one profile, restoring the profile files afterwards."""
    name, directory = resolve_profile(profile)
    if dry_run:
        return {"action": "dry_run", "profile": name, "plan": {
            "remove_dependency": PLUGIN_PACKAGE,
            "remove_bundle": PLUGIN_PACKAGE,
            "remove_patch_row": PLUGIN_ID,
            "purge_home": str(purge_home) if purge_home else None,
        }}
    backup = take_backup(directory, version=version, note="before `kvflow uninstall`")
    steps: list[dict[str, Any]] = []
    try:
        cli = dsh_cli()
        completed = subprocess.run(
            [cli, "plugin", "--profile", name, "remove", PLUGIN_PACKAGE],
            capture_output=True, text=True, timeout=timeout, check=False,
            cwd=str(directory), encoding="utf-8", errors="replace",
        )
        steps.append({
            "step": "dsh plugin remove",
            "returncode": completed.returncode,
            "stderr_tail": _tail(completed.stderr),
        })
        if completed.returncode != 0:
            raise ConfigError("`dsh plugin remove` failed", returncode=completed.returncode,
                              stderr=(completed.stderr or "")[-600:])
        document = read_profile(directory)
        bundles = profile_bundles(document)
        if PLUGIN_PACKAGE in bundles:
            dsh = document.setdefault("dsh", {}).setdefault("profile", {})
            dsh["bundles"] = [item for item in bundles if item != PLUGIN_PACKAGE]
            (directory / "package.json").write_text(
                json.dumps(document, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
            )
            steps.append({"step": "package.json bundles", "removed": PLUGIN_PACKAGE})
        (directory / "cordis.patch.yml").write_text(
            remove_kvflow_row(read_patch(directory)), encoding="utf-8"
        )
        steps.append({"step": "cordis.patch.yml", "removed_row": PLUGIN_ID})
    except Exception as exc:  # noqa: BLE001 - restore the profile, never leave it half detached
        restored = restore_backup(backup, directory)
        return {
            "action": "uninstall_failed",
            "profile": name,
            "error": f"{type(exc).__name__}: {exc}",
            "backup": str(backup),
            "rolled_back": restored,
        }

    after = inspect_integration(name)
    purged: dict[str, Any] | None = None
    if purge_home is not None:
        # the runtime home is only ever removed when the caller names it, and the
        # durable database is reported rather than silently deleted
        database = purge_home / "kvflow.sqlite3"
        purged = {
            "home": str(purge_home),
            "existed": purge_home.exists(),
            "database_present": database.is_file(),
            "removed": False,
            "note": "the runtime home holds the only task state; delete it yourself to purge",
        }
    return {
        "action": "uninstalled",
        "profile": name,
        "profile_dir": str(directory),
        "binding": after,
        "steps": steps,
        "backup": str(backup),
        "detached": not after["installed"],
        "purge": purged,
        "host_restart_required": bool(host_processes()),
    }
