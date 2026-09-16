"""Project registry: choose a directory, confirm its scope, then use it anywhere.

A project enters KVFlow as *configuration*, never as core code:

* ``.kvflow/project.json`` inside the project is the small declarative input a
  user reads and approves (identity, scopes, profiles, template, policy);
* the product's own directory holds the registry index, the managed workspaces,
  the durable store and the budgets -- a project never receives a copy of KVFlow;
* ``compile_project`` turns the approved configuration into the core ``Project``
  document, intersecting every requested tool with the authorized set instead of
  granting whatever a template or a plan asks for.

Identity is stable: ``project_id`` is generated once and the path is a separate,
explicitly rebindable fact. Moving a directory never inherits the old project's
permissions by folder name -- ``rebind`` is a deliberate, recorded action.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Literal

from .core.contracts import (
    Action,
    Contract,
    Id,
    Project,
    StrictBool,
    StrictInt,
    StrictStr,
    TestProfile,
    Text,
    content_digest,
)
from .core.errors import AuthorizationError, ConfigError, ConflictError, NotFoundError, V1Error
from .core.store import Store

CONFIG_DIR = ".kvflow"
CONFIG_NAME = "project.json"
REGISTRY_FILE = "registry.json"
BUDGET_FILE = "budgets.json"

#: tools a project may be authorized for; the config can only narrow this list
DEFAULT_TOOLS: tuple[str, ...] = (
    "repo.read", "repo.list", "repo.search", "repo.status", "repo.diff",
    "repo.patch", "repo.commit", "test.run_profile",
    "operation.status", "operation.result",
    "knowledge.search", "knowledge.read",
    "research.run_authorized",
)

#: files that are *evidence* about a project, never an authorization
DETECTED_MARKERS = (
    "pyproject.toml", "setup.py", "setup.cfg", "requirements.txt",
    "package.json", "pnpm-lock.yaml", "package-lock.json", "yarn.lock", "tsconfig.json",
    "next.config.js", "next.config.ts", "vite.config.ts", "vite.config.js",
    "pytest.ini", "tox.ini", "noxfile.py", "Makefile", "justfile",
    "AGENTS.md", "CLAUDE.md", ".editorconfig", "Cargo.toml", "go.mod",
)

#: default budget profiles; money is integer micro-CNY and always has a ceiling
DEFAULT_BUDGETS: dict[str, dict] = {
    "small": {
        "calls": 24, "input_tokens": 300_000, "output_tokens": 120_000, "tool_calls": 160,
        "storage_bytes": 32 << 20, "wall_seconds": 3_600, "micro_cny": 2_000_000,
        "concurrency": 2, "deadline_days": 7,
    },
    "standard": {
        "calls": 60, "input_tokens": 1_200_000, "output_tokens": 400_000, "tool_calls": 512,
        "storage_bytes": 64 << 20, "wall_seconds": 14_400, "micro_cny": 8_000_000,
        "concurrency": 3, "deadline_days": 14,
    },
    "large": {
        "calls": 160, "input_tokens": 4_000_000, "output_tokens": 1_200_000, "tool_calls": 1_500,
        "storage_bytes": 256 << 20, "wall_seconds": 43_200, "micro_cny": 30_000_000,
        "concurrency": 3, "deadline_days": 30,
    },
}

#: interpreters and package managers a build profile may invoke, by file name
ALLOWED_EXECUTABLES = frozenset(
    {"python", "python3", "python.exe", "node", "node.exe", "npm", "npm.cmd", "npx",
     "npx.cmd", "pnpm", "pnpm.cmd", "yarn", "yarn.cmd", "tsc", "deno", "bun", "git",
     "git.exe", "pytest"}
)

_METACHARACTERS = re.compile(r"[&|;<>`$\r\n]")


class ProfileSpec(Contract):
    """One pre-registered execution profile a project approves.

    ``runner="argv"`` exists because a web project must run ``npm test`` and a
    documentation project must run a checker. The argv is data the user approved
    with the project, it is executed as a list (never through a shell), and the
    executable must be one of the allowlisted names, so a model can never supply
    a command string of its own.
    """

    id: Id
    description: Text
    runner: Literal["pytest", "unittest", "readonly_inventory", "argv"]
    targets: list[StrictStr] = []
    pythonpath: list[StrictStr] = []
    cwd: StrictStr = "."
    timeout_seconds: StrictInt = 300
    heavy: StrictBool = False
    argv: list[StrictStr] = []
    proof: Literal["test", "build", "check"] = "test"
    research_authorized: StrictBool = False

    def model_post_init(self, __context: object) -> None:  # noqa: D105 - pydantic hook
        if self.runner == "argv":
            if not self.argv:
                raise ValueError("an argv profile needs a non-empty argv")
            if len(self.argv) > 32:
                raise ValueError("an argv profile is limited to 32 elements")
            for index, part in enumerate(self.argv):
                if not part or len(part) > 512:
                    raise ValueError("argv elements must be 1..512 characters")
                if _METACHARACTERS.search(part):
                    raise ValueError(
                        f"argv element {index} contains a shell metacharacter;"
                        " KVFlow always executes a list, never a shell string"
                    )
            executable = Path(self.argv[0]).name.casefold()
            if executable not in ALLOWED_EXECUTABLES:
                raise ValueError(
                    f"argv executable {executable!r} is not in the allowlist"
                )
            if self.targets:
                raise ValueError("an argv profile declares argv, not targets")
        else:
            if not self.targets:
                raise ValueError("a runner profile needs at least one target")
            if self.argv:
                raise ValueError("argv belongs to the argv runner only")
        if not 1 <= int(self.timeout_seconds) <= 3600:
            raise ValueError("timeout_seconds must be between 1 and 3600")


class ProjectConfig(Contract):
    """The declarative, user-readable project input."""

    project_id: Id
    display_name: Text
    canonical_root: Text
    template: Id
    allowed_read_roots: list[StrictStr]
    allowed_write_roots: list[StrictStr]
    protected_paths: list[StrictStr] = []
    profiles: list[ProfileSpec]
    model_profile: Id
    budget_profile: Id
    knowledge_scope: Literal["GLOBAL", "PROJECT", "PROJECT_AND_GLOBAL"] = "PROJECT_AND_GLOBAL"
    integration_policy: Literal["MANAGED_BRANCH", "REVIEW_ONLY", "PATCH_ONLY"] = "MANAGED_BRANCH"
    adapter: Id | None = None
    allowed_tools: list[Action] | None = None
    detection: dict = {}
    created_at: Text
    approved_by_user: StrictBool = False
    notes: Text | None = None

    def model_post_init(self, __context: object) -> None:  # noqa: D105 - pydantic hook
        if not Path(self.canonical_root).is_absolute():
            raise ValueError("canonical_root must be absolute")
        profile_ids = [profile.id for profile in self.profiles]
        if len(set(profile_ids)) != len(profile_ids):
            raise ValueError("duplicate profile id")
        if not self.allowed_read_roots:
            raise ValueError("a project needs at least one readable root")
        for protected in self.protected_paths:
            for writable in self.allowed_write_roots:
                folded_protected = protected.replace("\\", "/").casefold().strip("/")
                folded_write = writable.replace("\\", "/").casefold().strip("/")
                if folded_write == folded_protected or folded_write.startswith(
                    folded_protected + "/"
                ):
                    raise ValueError(f"write root {writable!r} overlaps protected {protected!r}")
        if self.allowed_tools is not None:
            unknown = {action.value for action in self.allowed_tools} - set(DEFAULT_TOOLS)
            if unknown:
                raise ValueError(f"unknown tool(s) in allowed_tools: {sorted(unknown)}")

    def profile(self, profile_id: str) -> ProfileSpec:
        for candidate in self.profiles:
            if candidate.id == profile_id:
                return candidate
        raise NotFoundError("unknown profile in this project", profile_id=profile_id)


def detect(root: Path) -> dict:
    """Read the project's real files as *evidence* for a configuration draft."""
    root = Path(root).resolve()
    if not root.is_dir():
        raise NotFoundError("project directory does not exist", path=str(root))
    markers = sorted(
        name for name in DETECTED_MARKERS if (root / name).is_file()
    )
    directories = sorted(
        name for name in ("tests", "test", "src", "app", "pages", "components", "docs",
                          "data", "scripts")
        if (root / name).is_dir()
    )
    git = (root / ".git").exists()
    existing = (root / CONFIG_DIR / CONFIG_NAME).is_file()
    return {
        "root": str(root),
        "markers": markers,
        "directories": directories,
        "git_repository": git,
        "existing_kvflow_config": existing,
        "file_count_sampled": len(
            [p for p in root.iterdir() if p.is_file()]
        ),
    }


def _default_profiles(root: Path, evidence: dict) -> list[ProfileSpec]:
    """Profiles that the detection actually justifies, with honest defaults."""
    markers = set(evidence["markers"])
    directories = set(evidence["directories"])
    profiles: list[ProfileSpec] = []

    python_project = bool({"pyproject.toml", "setup.py", "setup.cfg"} & markers)
    node_project = "package.json" in markers
    test_dir = "tests" if "tests" in directories else ("test" if "test" in directories else None)

    if python_project and test_dir:
        profiles.append(
            ProfileSpec(
                id="test",
                description=f"pytest over {test_dir}/ using the project's own configuration",
                runner="pytest",
                targets=[test_dir],
                pythonpath=["src"] if "src" in directories else [],
                timeout_seconds=600,
                proof="test",
            )
        )
    if node_project:
        profiles.append(
            ProfileSpec(
                id="test",
                description="the project's own test script, executed as a fixed argv",
                runner="argv",
                argv=["npm", "test", "--silent"],
                timeout_seconds=900,
                proof="test",
            )
        )
        if "tsconfig.json" in markers:
            profiles.append(
                ProfileSpec(
                    id="typecheck",
                    description="the project's TypeScript check as a fixed argv",
                    runner="argv",
                    argv=["npm", "run", "typecheck", "--silent"],
                    timeout_seconds=600,
                    proof="build",
                )
            )
        profiles.append(
            ProfileSpec(
                id="build",
                description="the project's own build script as a fixed argv",
                runner="argv",
                argv=["npm", "run", "build", "--silent"],
                timeout_seconds=900,
                proof="build",
            )
        )
    if not profiles:
        # a documentation or data project: the artifact checker is the evidence
        profiles.append(
            ProfileSpec(
                id="artifact",
                description=(
                    "KVFlow's artifact checker: verifies the declared outputs exist, are"
                    " non-empty and can be recorded by digest"
                ),
                runner="argv",
                argv=["python", "-m", "kvflow.checks.artifact", "--require", "README.md:1"],
                timeout_seconds=120,
                proof="check",
            )
        )
    return profiles


def draft(
    root: str | os.PathLike[str],
    *,
    display_name: str | None = None,
    template: str = "feature",
    model_profile: str = "deepseek_only",
    budget_profile: str = "standard",
    project_id: str | None = None,
    adapter: str | None = None,
    write_roots: list[str] | None = None,
    protected_paths: list[str] | None = None,
) -> ProjectConfig:
    """Build a configuration draft from what is really in the directory."""
    resolved = Path(root).resolve()
    evidence = detect(resolved)
    profiles = _default_profiles(resolved, evidence)
    identifier = project_id or f"{_slug(resolved.name)}-{uuid.uuid4().hex[:8]}"
    writable = write_roots or (
        ["src", "tests"] if (resolved / "src").is_dir() and (resolved / "tests").is_dir()
        else ["."]
    )
    protected = protected_paths if protected_paths is not None else []
    return ProjectConfig(
        project_id=identifier,
        display_name=display_name or resolved.name,
        canonical_root=str(resolved),
        template=template,
        allowed_read_roots=["."],
        allowed_write_roots=sorted(writable),
        protected_paths=sorted(protected),
        profiles=profiles,
        model_profile=model_profile,
        budget_profile=budget_profile,
        adapter=adapter,
        detection=evidence,
        created_at=datetime.now(timezone.utc).isoformat(),
        approved_by_user=False,
    )


def _slug(value: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9]+", "-", value).strip("-").casefold()
    return slug[:40] or "project"


def config_path(root: str | os.PathLike[str]) -> Path:
    return Path(root).resolve() / CONFIG_DIR / CONFIG_NAME


def read_config(root: str | os.PathLike[str]) -> ProjectConfig:
    path = config_path(root)
    if not path.is_file():
        raise NotFoundError("the project has no KVFlow configuration", path=str(path))
    try:
        return ProjectConfig.model_validate(json.loads(path.read_text(encoding="utf-8")))
    except (ValueError, OSError) as exc:
        raise ConfigError("the project configuration is not valid", path=str(path),
                          problem=str(exc)[:400]) from exc


def write_config(config: ProjectConfig, *, approve: bool = False) -> dict:
    """Write the project-local configuration, keeping a backup of any previous one.

    Only this product's own ``.kvflow/project.json`` is touched: a user's
    README, AGENTS.md or editor configuration is never rewritten.
    """
    root = Path(config.canonical_root)
    if not root.is_dir():
        raise NotFoundError("project directory does not exist", path=str(root))
    directory = root / CONFIG_DIR
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / CONFIG_NAME
    backup = None
    if path.exists():
        previous = path.read_bytes()
        backup = directory / f"{CONFIG_NAME}.bak-{uuid.uuid4().hex[:8]}"
        backup.write_bytes(previous)
    approved = config.model_copy(update={"approved_by_user": bool(approve)})
    payload = approved.model_dump(mode="json")
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True),
                    encoding="utf-8")
    return {
        "config_path": str(path),
        "backup_path": str(backup) if backup else None,
        "approved_by_user": bool(approve),
        "config_digest": content_digest(payload),
    }


def authorization_digest(config: ProjectConfig) -> str:
    """A stable digest of exactly what the user approved for this project."""
    payload = config.model_dump(mode="json")
    payload.pop("detection", None)
    payload.pop("created_at", None)
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, ensure_ascii=False).encode("utf-8")
    ).hexdigest()


def compile_project(
    config: ProjectConfig, *, home: str | os.PathLike[str]
) -> tuple[Project, list[TestProfile]]:
    """Turn an approved configuration into the core project and profile contracts."""
    if not config.approved_by_user:
        raise AuthorizationError(
            "the project configuration has not been approved by the user",
            project_id=config.project_id,
        )
    root = Path(config.canonical_root)
    managed = Path(home).expanduser().resolve() / "projects" / config.project_id / "managed"
    managed.mkdir(parents=True, exist_ok=True)
    digest = authorization_digest(config)

    profiles: list[TestProfile] = []
    for spec in config.profiles:
        code_digest = hashlib.sha256(
            json.dumps(
                {"id": spec.id, "runner": spec.runner, "targets": spec.targets,
                 "argv": spec.argv, "cwd": spec.cwd},
                sort_keys=True,
            ).encode("utf-8")
        ).hexdigest()
        profiles.append(
            TestProfile(
                id=spec.id,
                project_id=config.project_id,
                description=spec.description,
                runner=spec.runner,
                # an argv profile declares its whole command line; only a runner
                # profile has targets, and a runner profile needs at least one
                targets=spec.targets if spec.targets else (
                    [] if spec.runner == "argv" else ["."]
                ),
                cwd=spec.cwd,
                pythonpath=spec.pythonpath,
                timeout_seconds=spec.timeout_seconds,
                code_digest=code_digest,
                authorization_digest=digest,
                heavy=spec.heavy,
                argv=spec.argv,
                research_authorized=spec.research_authorized,
            )
        )

    requested = (
        {action.value for action in config.allowed_tools}
        if config.allowed_tools is not None else set(DEFAULT_TOOLS)
    )
    allowed = sorted(requested & set(DEFAULT_TOOLS))
    if any(spec.proof == "build" for spec in config.profiles) and "test.run_profile" not in allowed:
        allowed.append("test.run_profile")

    project = Project.model_validate(
        {
            "id": config.project_id,
            "display_name": config.display_name,
            "source_root": str(root),
            "managed_root": str(managed),
            "allowed_read_roots": list(config.allowed_read_roots),
            "allowed_write_roots": list(config.allowed_write_roots),
            "protected_roots": list(config.protected_paths),
            "test_profiles": [profile.model_dump(mode="json") for profile in profiles],
            "allowed_tools": sorted(set(allowed)),
            "trusted": True,
            "authorization_digest": digest,
            "integration_policy": config.integration_policy,
            "registered_at": datetime.now(timezone.utc).isoformat(),
        }
    )
    return project, profiles


# ------------------------------------------------------------------ registry


class Registry:
    """The product-owned index of registered projects."""

    def __init__(self, home: str | os.PathLike[str]) -> None:
        self.home = Path(home).expanduser().resolve()
        self.home.mkdir(parents=True, exist_ok=True)
        self.path = self.home / REGISTRY_FILE

    # ------------------------------------------------------------- storage
    def _read(self) -> dict:
        if not self.path.is_file():
            return {"version": 1, "projects": {}}
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
        except ValueError as exc:
            raise ConfigError("the registry file is not valid JSON",
                              path=str(self.path)) from exc
        if not isinstance(raw.get("projects"), dict):
            raise ConfigError("the registry file has no projects object", path=str(self.path))
        return raw

    def _write(self, payload: dict) -> None:
        temporary = self.path.with_suffix(".json.tmp")
        temporary.write_text(
            json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True), encoding="utf-8"
        )
        temporary.replace(self.path)

    # ------------------------------------------------------------ commands
    def register(self, config: ProjectConfig, *, store: Store | None = None) -> dict:
        """Register an approved configuration and index it by stable identity."""
        project, profiles = compile_project(config, home=self.home)
        payload = self._read()
        previous = payload["projects"].get(config.project_id)
        entry = {
            "project_id": config.project_id,
            "display_name": config.display_name,
            "canonical_root": config.canonical_root,
            "template": config.template,
            "model_profile": config.model_profile,
            "budget_profile": config.budget_profile,
            "adapter": config.adapter,
            "authorization_digest": project.authorization_digest,
            "config_path": str(config_path(config.canonical_root)),
            "profiles": [profile.id for profile in profiles],
            "registered_at": datetime.now(timezone.utc).isoformat(),
            "rebound_from": previous["canonical_root"] if previous else None,
        }
        payload["projects"][config.project_id] = entry
        self._write(payload)
        amended: dict | None = None
        if store is not None:
            # A re-approved configuration must reach the durable row too. The store
            # refuses an implicit re-registration (a run may never widen its own
            # scope), so an existing project is amended explicitly and the previous
            # authorization digest is recorded in the index.
            try:
                store.register_project(project)
            except V1Error:
                amended = store.amend_project(project)
                entry["amended_from"] = amended.get("previous_authorization_digest")
                entry["amended_at"] = datetime.now(timezone.utc).isoformat()
                payload["projects"][config.project_id] = entry
                self._write(payload)
        return {"entry": entry, "project": json.loads(project.model_dump_json()),
                "amended": amended}

    def list(self) -> list[dict]:
        payload = self._read()
        return [payload["projects"][key] for key in sorted(payload["projects"])]

    def get(self, project_id: str) -> dict:
        payload = self._read()
        try:
            return payload["projects"][project_id]
        except KeyError as exc:
            raise NotFoundError("unknown project", project_id=project_id,
                                known=sorted(payload["projects"])) from exc

    def config_for(self, project_id: str) -> ProjectConfig:
        entry = self.get(project_id)
        return read_config(entry["canonical_root"])

    def rebind(self, project_id: str, new_root: str | os.PathLike[str],
               *, store: Store | None = None) -> dict:
        """Point a project at a new directory after an explicit, recorded request."""
        resolved = Path(new_root).resolve()
        if not resolved.is_dir():
            raise NotFoundError("the new directory does not exist", path=str(resolved))
        entry = self.get(project_id)
        payload = self._read()
        config = read_config(entry["canonical_root"])
        moved = config.model_copy(
            update={
                "canonical_root": str(resolved),
                "detection": detect(resolved),
                "approved_by_user": False,
            }
        )
        written = write_config(moved, approve=False)
        payload["projects"][project_id] = {
            **entry,
            "canonical_root": str(resolved),
            "config_path": written["config_path"],
            "rebound_from": entry["canonical_root"],
            "rebound_at": datetime.now(timezone.utc).isoformat(),
            "authorization_digest": None,
            "needs_approval": True,
        }
        self._write(payload)
        return {
            "project_id": project_id,
            "from": entry["canonical_root"],
            "to": str(resolved),
            "config_path": written["config_path"],
            "needs_reapproval": True,
        }

    def remove(self, project_id: str, *, keep_data: bool = True) -> dict:
        """Unregister a project. Task history and knowledge are kept by default."""
        payload = self._read()
        entry = payload["projects"].pop(project_id, None)
        if entry is None:
            raise NotFoundError("unknown project", project_id=project_id)
        self._write(payload)
        return {"project_id": project_id, "removed": entry, "data_kept": bool(keep_data)}


def load_budgets(home: str | os.PathLike[str]) -> dict[str, dict]:
    """Budget profiles: the built-ins plus a validated user file."""
    table = dict(DEFAULT_BUDGETS)
    path = Path(home).expanduser() / BUDGET_FILE
    if path.is_file():
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except ValueError as exc:
            raise ConfigError("the budget profile file is not valid JSON",
                              path=str(path)) from exc
        entries = raw.get("budgets") if isinstance(raw, dict) else None
        if not isinstance(entries, dict):
            raise ConfigError("the budget profile file needs a budgets object",
                              path=str(path))
        for key, value in entries.items():
            if not isinstance(value, dict):
                raise ConfigError("a budget profile must be an object", profile=str(key))
            unknown = set(value) - set(DEFAULT_BUDGETS["standard"])
            if unknown:
                raise ConfigError("unknown budget dimension", profile=str(key),
                                  unknown=sorted(unknown))
            table[str(key)] = {**DEFAULT_BUDGETS["standard"], **value}
    return table


def budget_profile(home: str | os.PathLike[str], profile_id: str) -> dict:
    table = load_budgets(home)
    try:
        return table[profile_id]
    except KeyError as exc:
        raise NotFoundError("unknown budget profile", budget_profile=profile_id,
                            known=sorted(table)) from exc


def deadline_from(profile: dict, *, now: datetime | None = None) -> datetime:
    base = now or datetime.now(timezone.utc)
    return base + timedelta(days=int(profile.get("deadline_days", 14)))


def global_ceiling(budgets: dict[str, dict], profile_id: str) -> dict:
    """The process-wide ceiling is the widest configured profile, never unlimited."""
    widest = max(
        (budgets[key] for key in budgets if key != profile_id),
        key=lambda item: int(item["micro_cny"]),
        default=budgets[profile_id],
    )
    return {
        **budgets[profile_id],
        "calls": widest["calls"],
        "input_tokens": widest["input_tokens"],
        "output_tokens": widest["output_tokens"],
        "tool_calls": widest["tool_calls"],
        "micro_cny": widest["micro_cny"],
        "concurrency": max(int(widest["concurrency"]), int(budgets[profile_id]["concurrency"])),
        "deadline_days": widest["deadline_days"],
    }


def project_conflicts(registry: Registry, root: str | os.PathLike[str]) -> list[dict]:
    """Registered projects whose canonical root overlaps the given directory."""
    resolved = Path(root).resolve()
    conflicts = []
    for entry in registry.list():
        other = Path(entry["canonical_root"])
        if other == resolved or other in resolved.parents or resolved in other.parents:
            conflicts.append(entry)
    return conflicts


def stale_entries(registry: Registry) -> list[dict]:
    """Registered projects whose directory is gone or whose config moved."""
    stale = []
    for entry in registry.list():
        root = Path(entry["canonical_root"])
        if not root.is_dir():
            stale.append({**entry, "problem": "directory_missing"})
        elif not config_path(root).is_file():
            stale.append({**entry, "problem": "config_missing"})
    return stale


def approval_summary(config: ProjectConfig) -> dict:
    """What the user is being asked to approve, in plain terms."""
    return {
        "project_id": config.project_id,
        "display_name": config.display_name,
        "canonical_root": config.canonical_root,
        "template": config.template,
        "model_profile": config.model_profile,
        "budget_profile": config.budget_profile,
        "adapter": config.adapter,
        "read_roots": list(config.allowed_read_roots),
        "write_roots": list(config.allowed_write_roots),
        "protected_paths": list(config.protected_paths),
        "profiles": [
            {
                "id": profile.id,
                "runner": profile.runner,
                "argv": list(profile.argv),
                "targets": list(profile.targets),
                "proof": profile.proof,
                "timeout_seconds": profile.timeout_seconds,
            }
            for profile in config.profiles
        ],
        "integration_policy": config.integration_policy,
        "knowledge_scope": config.knowledge_scope,
        "detected_files": config.detection.get("markers", []),
        "note": (
            "Detected files are evidence about the project, not permission. Approving"
            " this draft authorizes exactly the read roots, write roots and profiles above."
        ),
    }
