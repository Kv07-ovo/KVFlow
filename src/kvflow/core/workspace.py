"""Owned workspaces: snapshot, worktree and integration isolation.

The product never writes into a registered source project. Every task gets an
owned workspace under the project's ``managed_root``:

``<managed_root>/<project_id>/snapshots/<snapshot_id>/``
    A byte-exact copy of the selected source files plus a hash manifest. A
    snapshot records *what* code was read, so a later review can prove the
    delivered change was made against the tested bytes.

``<managed_root>/<project_id>/worktrees/<job_id>/<node_id>/``
    The writable tree for one node. Created from a snapshot (or from a managed
    Git base revision) and confined to the node's declared scopes.

``<managed_root>/<project_id>/integration/<job_id>/``
    The independent integration tree where approved node outputs are combined.
    Conflicts fail loudly; nothing is silently ``ours``/``theirs``.

This module is deliberately Git-conservative: it only ever runs Git inside the
*owned* managed tree, never in the registered source root, and never pushes,
merges into a user branch or rewrites history.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Iterable, Sequence

from .contracts import ArtifactRef, Project, relative_scope
from .errors import (
    AuthorizationError,
    ConcurrencyError,
    ConfigError,
    ConflictError,
    ContractError,
    NotFoundError,
    PathDenied,
)
from .security import PathPolicy, ReparseScanner

#: byte ceiling for one snapshot, so a bad selection cannot copy a whole disk
MAX_SNAPSHOT_BYTES = 512 * 1024 * 1024
MAX_SNAPSHOT_FILES = 20000
#: directories never copied into a snapshot even if they sit under a scope
DEFAULT_EXCLUDES = (
    ".git",
    ".venv",
    "venv",
    "__pycache__",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    "node_modules",
    ".kvflow_runtime",
)

#: files or directories this product creates inside a workspace. They are harness
#: bookkeeping, not part of the delivered change, so a diff and an integration
#: step must never report them as if the Worker had written them.
PRODUCT_ARTIFACTS = (
    "WORKSPACE.json",
    ".kvflow_pytest.ini",
    ".kvflow_pytest_cache",
    ".kvflow_logs",
)


def is_product_artifact(relative_path: str) -> bool:
    head = relative_path.replace("\\", "/").split("/", 1)[0]
    return head in PRODUCT_ARTIFACTS


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def source_digest(path: Path) -> tuple[str, int]:
    """The canonical identity of one source file: newline-normalised bytes.

    The same function decides whether a source file is unchanged later, so a
    snapshot digest and a "was this file touched?" check can never disagree about
    what the file's content is.
    """
    raw = path.read_bytes()
    normalized = raw.replace(b"\r\n", b"\n").replace(b"\r", b"\n")
    return hashlib.sha256(normalized).hexdigest(), len(normalized)


def copy_source_file(source: Path, destination: Path) -> tuple[str, int]:
    """Copy one source file into an owned tree with a stable, canonical digest.

    The bytes are read and written back with a fixed newline convention so the
    snapshot identity is reproducible across platforms and Git checkouts instead
    of depending on the host's autocrlf setting.
    """
    digest, size = source_digest(source)
    destination.parent.mkdir(parents=True, exist_ok=True)
    raw = source.read_bytes()
    normalized = raw.replace(b"\r\n", b"\n").replace(b"\r", b"\n")
    with open(destination, "wb") as handle:
        handle.write(normalized)
    return digest, size


def canonical_manifest(entries: Sequence[dict[str, Any]]) -> str:
    return json.dumps(
        list(entries), ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )


def manifest_digest(entries: Sequence[dict[str, Any]]) -> str:
    return _sha256_bytes(canonical_manifest(entries).encode("utf-8"))


@dataclass(frozen=True)
class SnapshotEntry:
    relative_path: str
    sha256: str
    size_bytes: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "relative_path": self.relative_path,
            "sha256": self.sha256,
            "size_bytes": self.size_bytes,
        }


@dataclass(frozen=True)
class Snapshot:
    snapshot_id: str
    project_id: str
    root: Path
    entries: tuple[SnapshotEntry, ...]
    manifest_sha256: str
    created_at: str
    source_root: str
    notes: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "snapshot_id": self.snapshot_id,
            "project_id": self.project_id,
            "root": str(self.root),
            "entries": [e.to_dict() for e in self.entries],
            "manifest_sha256": self.manifest_sha256,
            "created_at": self.created_at,
            "source_root": self.source_root,
            "notes": self.notes,
            "file_count": len(self.entries),
            "total_bytes": sum(e.size_bytes for e in self.entries),
        }


@dataclass(frozen=True)
class WorkspaceHandle:
    job_id: str
    node_id: str
    project_id: str
    root: Path
    snapshot_id: str
    scopes: tuple[str, ...]
    created_at: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "job_id": self.job_id,
            "node_id": self.node_id,
            "project_id": self.project_id,
            "root": str(self.root),
            "snapshot_id": self.snapshot_id,
            "scopes": list(self.scopes),
            "created_at": self.created_at,
        }


@dataclass(frozen=True)
class GitResult:
    argv: tuple[str, ...]
    exit_code: int
    stdout: str
    stderr: str


class WorkspaceManager:
    """Creates and owns snapshots, node worktrees and integration trees."""

    def __init__(self, project: Project, *, scanner: ReparseScanner | None = None) -> None:
        self.project = project
        self.policy = PathPolicy(project, scanner=scanner)
        self.managed_root = Path(project.managed_root)
        self.source_root = Path(project.source_root)
        self.base = self.managed_root / project.id
        self.snapshots_dir = self.base / "snapshots"
        self.worktrees_dir = self.base / "worktrees"
        self.integration_dir = self.base / "integration"
        for directory in (self.snapshots_dir, self.worktrees_dir, self.integration_dir):
            directory.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------ snapshots
    def create_snapshot(
        self,
        *,
        include_scopes: Sequence[str] | None = None,
        exclude: Sequence[str] = DEFAULT_EXCLUDES,
        notes: str = "",
    ) -> Snapshot:
        """Copy selected *source* bytes into an owned, hash-manifested snapshot."""
        scopes = [relative_scope(s) for s in (include_scopes or ["."])]
        snapshot_id = "snap_" + uuid.uuid4().hex[:16]
        target = self.snapshots_dir / snapshot_id
        target.mkdir(parents=True, exist_ok=False)
        excluded = {name.casefold() for name in exclude}
        entries: list[SnapshotEntry] = []
        total = 0
        try:
            for scope in scopes:
                origin = self._source_path(scope)
                if not origin.exists():
                    continue
                candidates: Iterable[Path]
                if origin.is_dir():
                    candidates = sorted(origin.rglob("*"))
                else:
                    candidates = [origin]
                for path in candidates:
                    if path.is_dir():
                        continue
                    relative_parts = path.relative_to(self.source_root).parts
                    if any(part.casefold() in excluded for part in relative_parts):
                        continue
                    if path.is_symlink():
                        raise PathDenied(
                            "refusing to snapshot a link", component=str(path)
                        )
                    data_size = path.stat().st_size
                    if len(entries) >= MAX_SNAPSHOT_FILES:
                        raise ContractError("snapshot has too many files")
                    if total + data_size > MAX_SNAPSHOT_BYTES:
                        raise ContractError("snapshot exceeds the byte ceiling")
                    relative = PurePosixPath(*relative_parts).as_posix()
                    destination = target / PurePosixPath(relative)
                    digest, stored_size = copy_source_file(path, destination)
                    entries.append(
                        SnapshotEntry(
                            relative_path=relative,
                            sha256=digest,
                            size_bytes=stored_size,
                        )
                    )
                    total += stored_size
            entries.sort(key=lambda e: e.relative_path)
            manifest = {
                "snapshot_id": snapshot_id,
                "project_id": self.project.id,
                "source_root": str(self.source_root),
                "created_at": _now(),
                "notes": notes,
                "entries": [e.to_dict() for e in entries],
                "manifest_sha256": manifest_digest([e.to_dict() for e in entries]),
            }
            (target / "SNAPSHOT_MANIFEST.json").write_text(
                json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
            )
        except BaseException:
            shutil.rmtree(target, ignore_errors=True)
            raise
        return Snapshot(
            snapshot_id=snapshot_id,
            project_id=self.project.id,
            root=target,
            entries=tuple(entries),
            manifest_sha256=manifest["manifest_sha256"],
            created_at=manifest["created_at"],
            source_root=str(self.source_root),
            notes=notes,
        )

    def _source_path(self, scope: str) -> Path:
        """Resolve a scope inside the *registered source root* for snapshotting."""
        if scope == ".":
            return self.source_root
        parts = scope.split("/")
        candidate = self.source_root.joinpath(*parts)
        resolved = Path(os.path.abspath(str(candidate)))
        root = Path(os.path.abspath(str(self.source_root)))
        if os.path.normcase(str(resolved)) != os.path.normcase(str(root)) and not (
            os.path.normcase(str(resolved)).startswith(os.path.normcase(str(root)) + os.sep)
        ):
            raise PathDenied("snapshot scope escapes the source root", scope=scope)
        if resolved.exists() and (resolved.is_symlink() or self._is_link(resolved)):
            raise PathDenied("snapshot scope is a link", scope=scope)
        return resolved

    @staticmethod
    def _is_link(path: Path) -> bool:
        try:
            return bool(getattr(path.lstat(), "st_file_attributes", 0) & 0x400)
        except OSError:
            return False

    def snapshot(self, snapshot_id: str) -> Snapshot:
        root = self.snapshots_dir / snapshot_id
        manifest_path = root / "SNAPSHOT_MANIFEST.json"
        if not manifest_path.exists():
            raise NotFoundError("unknown snapshot", snapshot_id=snapshot_id)
        data = json.loads(manifest_path.read_text(encoding="utf-8"))
        return Snapshot(
            snapshot_id=data["snapshot_id"],
            project_id=data["project_id"],
            root=root,
            entries=tuple(SnapshotEntry(**e) for e in data["entries"]),
            manifest_sha256=data["manifest_sha256"],
            created_at=data["created_at"],
            source_root=data["source_root"],
            notes=data.get("notes", ""),
        )

    def verify_snapshot(self, snapshot_id: str) -> dict[str, Any]:
        """Re-hash a snapshot and report every drift, including deletions."""
        snapshot = self.snapshot(snapshot_id)
        changed: list[str] = []
        missing: list[str] = []
        for entry in snapshot.entries:
            path = snapshot.root / PurePosixPath(entry.relative_path)
            if not path.exists():
                missing.append(entry.relative_path)
                continue
            if _sha256_file(path) != entry.sha256:
                changed.append(entry.relative_path)
        known = {e.relative_path for e in snapshot.entries}
        extra: list[str] = []
        for path in snapshot.root.rglob("*"):
            if path.is_dir() or path.name == "SNAPSHOT_MANIFEST.json":
                continue
            relative = PurePosixPath(*path.relative_to(snapshot.root).parts).as_posix()
            if relative not in known:
                extra.append(relative)
        return {
            "snapshot_id": snapshot_id,
            "verified": not (changed or missing or extra),
            "changed": sorted(changed),
            "missing": sorted(missing),
            "extra": sorted(extra),
            "manifest_sha256": snapshot.manifest_sha256,
            "checked_at": _now(),
        }

    # ------------------------------------------------------------ worktrees
    def create_workspace(
        self,
        *,
        job_id: str,
        node_id: str,
        snapshot_id: str,
        scopes: Sequence[str],
    ) -> WorkspaceHandle:
        """Materialise one node's writable tree from an owned snapshot."""
        for value, label in ((job_id, "job_id"), (node_id, "node_id")):
            if not isinstance(value, str) or not value or "/" in value or "\\" in value:
                raise ContractError(f"invalid {label}")
        snapshot = self.snapshot(snapshot_id)
        normalised = tuple(relative_scope(s) for s in scopes)
        root = self.worktrees_dir / job_id / node_id
        if root.exists():
            raise ConcurrencyError("this node already has a workspace", root=str(root))
        root.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(snapshot.root, root)
        manifest_path = root / "SNAPSHOT_MANIFEST.json"
        if manifest_path.exists():
            manifest_path.unlink()
        handle = WorkspaceHandle(
            job_id=job_id,
            node_id=node_id,
            project_id=self.project.id,
            root=root,
            snapshot_id=snapshot_id,
            scopes=normalised,
            created_at=_now(),
        )
        (root / "WORKSPACE.json").write_text(
            json.dumps(handle.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8"
        )
        return handle

    def create_workspace_from(
        self,
        *,
        job_id: str,
        node_id: str,
        base_handle: WorkspaceHandle,
        scopes: Sequence[str],
    ) -> WorkspaceHandle:
        """Create a node workspace that continues from an earlier node's tree.

        A dependent node must see the bytes its dependency produced, so its base
        is the earlier node's workspace rather than the original snapshot. The
        snapshot id is still recorded, so provenance is not lost.
        """
        for value, label in ((job_id, "job_id"), (node_id, "node_id")):
            if not isinstance(value, str) or not value or "/" in value or "\\" in value:
                raise ContractError(f"invalid {label}")
        root = self.worktrees_dir / job_id / node_id
        if root.exists():
            raise ConcurrencyError("this node already has a workspace", root=str(root))
        root.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(base_handle.root, root)
        for name in ("WORKSPACE.json", ".kvflow_pytest.ini"):
            stale = root / name
            if stale.exists():
                stale.unlink()
        shutil.rmtree(root / ".kvflow_logs", ignore_errors=True)
        shutil.rmtree(root / ".kvflow_pytest_cache", ignore_errors=True)
        shutil.rmtree(root / ".git", ignore_errors=True)
        handle = WorkspaceHandle(
            job_id=job_id,
            node_id=node_id,
            project_id=self.project.id,
            root=root,
            snapshot_id=base_handle.snapshot_id,
            scopes=tuple(relative_scope(s) for s in scopes),
            created_at=_now(),
        )
        (root / "WORKSPACE.json").write_text(
            json.dumps(handle.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8"
        )
        return handle

    def workspace_path(self, handle: WorkspaceHandle, scope: str) -> Path:
        """Resolve a write target inside the node's declared scopes only."""
        clean = relative_scope(scope)
        if not any(PathPolicy._contains(allowed, clean) for allowed in handle.scopes):
            raise AuthorizationError(
                "the node did not declare this write scope", scope=clean
            )
        target = handle.root.joinpath(*clean.split("/"))
        resolved = Path(os.path.abspath(str(target)))
        base = Path(os.path.abspath(str(handle.root)))
        if not os.path.normcase(str(resolved)).startswith(
            os.path.normcase(str(base)) + os.sep
        ):
            raise PathDenied("write scope escaped the workspace", scope=clean)
        return resolved

    def workspace_diff(self, handle: WorkspaceHandle) -> dict[str, Any]:
        """Changed files with content digests, computed against the snapshot."""
        snapshot = self.snapshot(handle.snapshot_id)
        original = {e.relative_path: e for e in snapshot.entries}
        changed: list[dict[str, Any]] = []
        for path in sorted(handle.root.rglob("*")):
            if path.is_dir():
                continue
            relative = PurePosixPath(*path.relative_to(handle.root).parts).as_posix()
            if is_product_artifact(relative):
                continue
            digest = _sha256_file(path)
            previous = original.get(relative)
            if previous is None:
                changed.append(
                    {"relative_path": relative, "change": "ADDED", "sha256": digest,
                     "size_bytes": path.stat().st_size}
                )
            elif previous.sha256 != digest:
                changed.append(
                    {"relative_path": relative, "change": "MODIFIED", "sha256": digest,
                     "base_sha256": previous.sha256, "size_bytes": path.stat().st_size}
                )
        present = {
            PurePosixPath(*p.relative_to(handle.root).parts).as_posix()
            for p in handle.root.rglob("*")
            if p.is_file()
            and not is_product_artifact(
                PurePosixPath(*p.relative_to(handle.root).parts).as_posix()
            )
        }
        for relative in sorted(set(original) - present):
            changed.append(
                {
                    "relative_path": relative,
                    "change": "DELETED",
                    "sha256": None,
                    "base_sha256": original[relative].sha256,
                }
            )
        entries = [c for c in changed if c["sha256"]]
        return {
            "job_id": handle.job_id,
            "node_id": handle.node_id,
            "base_snapshot": handle.snapshot_id,
            "base_identity": snapshot.manifest_sha256,
            "changed": changed,
            "change_count": len(changed),
            "content_digest": manifest_digest(
                [
                    {"relative_path": c["relative_path"], "sha256": c["sha256"]}
                    for c in sorted(entries, key=lambda c: c["relative_path"])
                ]
            ),
        }

    # ----------------------------------------------------------- integration
    def create_integration_tree(self, *, job_id: str, snapshot_id: str) -> Path:
        snapshot = self.snapshot(snapshot_id)
        root = self.integration_dir / job_id
        if root.exists():
            raise ConcurrencyError("this job already has an integration tree", root=str(root))
        root.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(snapshot.root, root)
        manifest = root / "SNAPSHOT_MANIFEST.json"
        if manifest.exists():
            manifest.unlink()
        return root

    def apply_to_integration(
        self,
        *,
        job_id: str,
        handle: WorkspaceHandle,
        approved_paths: Sequence[str] | None = None,
    ) -> dict[str, Any]:
        """Apply one node's approved files into the integration tree.

        A conflict - the target already holds different bytes that a *previous*
        approved node introduced - fails loudly instead of overwriting.
        """
        root = self.integration_dir / job_id
        if not root.exists():
            raise NotFoundError("no integration tree for this job", job_id=job_id)
        diff = self.workspace_diff(handle)
        allowed = (
            None
            if approved_paths is None
            else {relative_scope(p) for p in approved_paths}
        )
        applied: list[str] = []
        conflicts: list[dict[str, Any]] = []
        for change in diff["changed"]:
            relative = change["relative_path"]
            if allowed is not None and relative not in allowed:
                continue
            source = handle.root / PurePosixPath(relative)
            target = root / PurePosixPath(relative)
            if change["change"] == "DELETED":
                if target.exists():
                    target.unlink()
                    applied.append(relative)
                continue
            if target.exists() and change.get("base_sha256") not in (None, _sha256_file(target)):
                conflicts.append(
                    {
                        "relative_path": relative,
                        "integration_sha256": _sha256_file(target),
                        "node_base_sha256": change.get("base_sha256"),
                    }
                )
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, target)
            applied.append(relative)
        if conflicts:
            raise ConflictError(
                "integration conflict: the target holds different bytes",
                job_id=job_id,
                node_id=handle.node_id,
                conflicts=conflicts,
            )
        return {
            "job_id": job_id,
            "node_id": handle.node_id,
            "applied": sorted(applied),
            "applied_count": len(applied),
            "integration_root": str(root),
            "content_digest": diff["content_digest"],
            "applied_at": _now(),
        }

    def integration_digest(self, *, job_id: str) -> str:
        root = self.integration_dir / job_id
        if not root.exists():
            raise NotFoundError("no integration tree for this job", job_id=job_id)
        entries = []
        for path in sorted(root.rglob("*")):
            if path.is_dir():
                continue
            relative = PurePosixPath(*path.relative_to(root).parts).as_posix()
            entries.append({"relative_path": relative, "sha256": _sha256_file(path)})
        return manifest_digest(entries)

    # ----------------------------------------------------------------- git
    def git(self, argv: Sequence[str], *, cwd: Path | None = None) -> GitResult:
        """Run Git only inside the owned managed tree."""
        target = Path(cwd) if cwd is not None else self.managed_root
        resolved = Path(os.path.abspath(str(target)))
        managed = Path(os.path.abspath(str(self.managed_root)))
        if not os.path.normcase(str(resolved)).startswith(os.path.normcase(str(managed))):
            raise AuthorizationError(
                "git may only run inside the owned managed root", cwd=str(resolved)
            )
        if os.path.normcase(str(resolved)).startswith(
            os.path.normcase(str(self.source_root))
        ):
            raise AuthorizationError("git must never run in the registered source root")
        completed = subprocess.run(
            ["git", *argv],
            cwd=str(resolved),
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
        )
        return GitResult(
            argv=tuple(completed.args if isinstance(completed.args, list) else argv),
            exit_code=completed.returncode,
            stdout=completed.stdout,
            stderr=completed.stderr,
        )

    def init_managed_repo(self, *, default_branch: str = "main") -> GitResult:
        if (self.managed_root / ".git").exists():
            return GitResult(("git", "rev-parse"), 0, "already a repository", "")
        return self.git(["init", "-b", default_branch, "."], cwd=self.managed_root)

    def commit_workspace(
        self, handle: WorkspaceHandle, *, message: str, author: str
    ) -> dict[str, Any]:
        """Commit inside one node workspace; never in the source project."""
        if not (handle.root / ".git").exists():
            self.git(["init", "-b", "work"], cwd=handle.root)
            self.git(["add", "-A"], cwd=handle.root)
            self.git(
                [
                    "-c",
                    f"user.name={author}",
                    "-c",
                    "user.email=worker@kvflow.local",
                    "commit",
                    "-q",
                    "-m",
                    "snapshot base",
                ],
                cwd=handle.root,
            )
        self.git(["add", "-A"], cwd=handle.root)
        result = self.git(
            [
                "-c",
                f"user.name={author}",
                "-c",
                "user.email=worker@kvflow.local",
                "commit",
                "-q",
                "--allow-empty",
                "-m",
                message,
            ],
            cwd=handle.root,
        )
        head = self.git(["rev-parse", "HEAD"], cwd=handle.root)
        return {
            "exit_code": result.exit_code,
            "commit": head.stdout.strip(),
            "message": message,
            "author": author,
            "stderr": result.stderr.strip()[-500:],
        }

    def artifacts_for(self, handle: WorkspaceHandle) -> list[ArtifactRef]:
        diff = self.workspace_diff(handle)
        refs: list[ArtifactRef] = []
        for index, change in enumerate(sorted(diff["changed"], key=lambda c: c["relative_path"])):
            if not change["sha256"]:
                continue
            refs.append(
                ArtifactRef(
                    id=f"artifact-{handle.node_id}-{index:04d}",
                    digest=change["sha256"],
                    relative_path=change["relative_path"],
                    size_bytes=int(change.get("size_bytes") or 0),
                )
            )
        return refs
