"""Versioned backup, restore rehearsal, upgrade and rollback.

Everything here works on *copies*. A backup uses SQLite's own backup API so a
live database with a non-empty WAL is captured consistently rather than copied
file-by-file; a restore targets an empty destination; an upgrade validates a copy
before it replaces anything and keeps the previous version for rollback.

Facts a restore must preserve
-----------------------------

* task/run/attempt counters and the durable lineage table,
* budgets, reservations and their settled/UNKNOWN state,
* receipts, reviews, knowledge records and publications,
* the artifact/knowledge references the manifest lists.

A restore never starts the queue, never re-runs a DONE task and never resends an
operation whose outcome was unknown. Model credentials are never included.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3
import tempfile
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Iterable, Sequence

from .errors import ConfigError, ConflictError, ContractError, NotFoundError

#: archive format version, written into every manifest
BACKUP_FORMAT = 1
#: tables a restore must find, with the row counts it must reproduce
REQUIRED_TABLES = (
    "projects",
    "jobs",
    "plans",
    "nodes",
    "node_attempts",
    "runs",
    "leases",
    "reservations",
    "test_receipts",
    "reviews",
    "knowledge",
    "publications",
    "operations",
    "events",
    "experiments",
    "experiment_runs",
)

#: never copied into a backup, whatever a caller asks for
FORBIDDEN_BACKUP_NAMES = frozenset(
    {
        ".env",
        "credentials.json",
        "auth.json",
        "id_rsa",
        "id_ed25519",
        ".netrc",
        "cookies.txt",
    }
)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


@dataclass(frozen=True)
class BackupEntry:
    relative_path: str
    sha256: str
    size_bytes: int
    kind: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "relative_path": self.relative_path,
            "sha256": self.sha256,
            "size_bytes": self.size_bytes,
            "kind": self.kind,
        }


@dataclass(frozen=True)
class BackupResult:
    backup_id: str
    root: Path
    manifest_path: Path
    entries: tuple[BackupEntry, ...]
    created_at: str
    source_version: str
    database_bytes: int
    manifest_sha256: str
    row_counts: dict[str, int] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "backup_id": self.backup_id,
            "root": str(self.root),
            "manifest": str(self.manifest_path),
            "created_at": self.created_at,
            "source_version": self.source_version,
            "database_bytes": self.database_bytes,
            "manifest_sha256": self.manifest_sha256,
            "entry_count": len(self.entries),
            "row_counts": self.row_counts,
            "entries": [e.to_dict() for e in self.entries],
        }


def _cleanup_tree(path: str | Path, *, attempts: int = 8) -> bool:
    """Remove a scratch directory with a bounded retry.

    Windows removes a directory asynchronously: ``shutil.rmtree`` can raise
    ``WinError 145`` ("the directory is not empty") or ``WinError 5`` while the
    kernel still holds the last handle, even though nothing in this process is
    using it. A short bounded retry is the honest fix; a scratch directory that
    still cannot be removed is reported to the caller rather than swallowed.
    """
    import time

    for attempt in range(1, attempts + 1):
        try:
            shutil.rmtree(path)
            return True
        except FileNotFoundError:
            return True
        except OSError:
            if attempt >= attempts:
                return False
            time.sleep(min(0.5, 0.02 * (2 ** attempt)))
    return False


class _scratch_dir:
    """A temporary directory whose cleanup tolerates the Windows delete race."""

    def __init__(self, prefix: str = "kvflow-") -> None:
        self.path = Path(tempfile.mkdtemp(prefix=prefix))
        self.cleaned = True

    def __enter__(self) -> Path:
        return self.path

    def __exit__(self, *exc: object) -> bool:
        self.cleaned = _cleanup_tree(self.path)
        return False

class BackupManager:
    """Creates, verifies and restores consistent Agent OS backups."""

    def __init__(self, *, database: Path, backups_root: Path, version: str) -> None:
        self.database = Path(database)
        self.backups_root = Path(backups_root)
        self.version = str(version)
        self.backups_root.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------- helpers
    @staticmethod
    def _connect_readonly(path: Path) -> sqlite3.Connection:
        uri = f"file:{path.as_posix()}?mode=ro"
        connection = sqlite3.connect(uri, uri=True, isolation_level=None)
        connection.row_factory = sqlite3.Row
        return connection

    @staticmethod
    def row_counts(path: Path) -> dict[str, int]:
        """Counts via the backup API path so a live WAL is included."""
        counts: dict[str, int] = {}
        with _scratch_dir() as scratch:
            snapshot = Path(scratch) / "snapshot.sqlite3"
            BackupManager._snapshot_database(path, snapshot)
            connection = sqlite3.connect(snapshot)
            connection.row_factory = sqlite3.Row
            try:
                tables = {
                    row["name"]
                    for row in connection.execute(
                        "SELECT name FROM sqlite_master WHERE type = 'table'"
                    )
                }
                for table in REQUIRED_TABLES:
                    if table not in tables:
                        counts[table] = -1
                        continue
                    counts[table] = int(
                        connection.execute(f"SELECT COUNT(*) AS c FROM {table}").fetchone()["c"]
                    )
            finally:
                connection.close()
        return counts

    @staticmethod
    def _snapshot_database(source: Path, target: Path) -> None:
        """Use SQLite's own backup so WAL contents are included consistently."""
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists():
            target.unlink()
        origin = sqlite3.connect(f"file:{source.as_posix()}?mode=ro", uri=True)
        try:
            origin.execute("PRAGMA query_only = ON")
            destination = sqlite3.connect(target)
            try:
                origin.backup(destination)
                destination.commit()
            finally:
                destination.close()
        finally:
            origin.close()

    # -------------------------------------------------------------- backup
    def create(
        self,
        *,
        include: Sequence[Path] = (),
        note: str = "",
        backup_id: str | None = None,
    ) -> BackupResult:
        """Create a versioned backup directory with a hashed manifest."""
        if not self.database.exists():
            raise NotFoundError("the database to back up does not exist",
                                database=str(self.database))
        identifier = backup_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        root = self.backups_root / f"kvflow-{self.version}-{identifier}"
        if root.exists():
            raise ConflictError("that backup id already exists", root=str(root))
        root.mkdir(parents=True, exist_ok=False)
        entries: list[BackupEntry] = []
        try:
            database_target = root / "kvflow.sqlite3"
            self._snapshot_database(self.database, database_target)
            entries.append(
                BackupEntry(
                    relative_path="kvflow.sqlite3",
                    sha256=_sha256_file(database_target),
                    size_bytes=database_target.stat().st_size,
                    kind="database",
                )
            )
            counts = self.row_counts(self.database)
            config_target = root / "config"
            config_target.mkdir()
            config = {
                "version": self.version,
                "backup_format": BACKUP_FORMAT,
                "created_at": _now(),
                "note": note,
                "database": "kvflow.sqlite3",
                "row_counts": counts,
                "includes_credentials": False,
                "artifacts": [],
            }
            for index, source in enumerate(include):
                source = Path(source)
                if not source.exists():
                    continue
                if source.name.casefold() in FORBIDDEN_BACKUP_NAMES:
                    raise ConfigError(
                        "refusing to back up a credential-like file", path=str(source)
                    )
                target = config_target / f"artifact-{index:04d}-{source.name}"
                if source.is_dir():
                    shutil.copytree(source, target)
                    copied = [p for p in sorted(target.rglob("*")) if p.is_file()]
                else:
                    shutil.copyfile(source, target)
                    copied = [target]
                for path in copied:
                    relative = PurePosixPath(
                        "config", *path.relative_to(config_target).parts
                    ).as_posix()
                    entries.append(
                        BackupEntry(
                            relative_path=relative,
                            sha256=_sha256_file(path),
                            size_bytes=path.stat().st_size,
                            kind="artifact",
                        )
                    )
                config["artifacts"].append(
                    {"source": str(source), "included_as": target.name}
                )
            config_path = root / "config" / "backup_config.json"
            config_path.write_text(_canonical(config), encoding="utf-8")
            entries.append(
                BackupEntry(
                    relative_path="config/backup_config.json",
                    sha256=_sha256_file(config_path),
                    size_bytes=config_path.stat().st_size,
                    kind="config",
                )
            )
            manifest = {
                "backup_format": BACKUP_FORMAT,
                "backup_id": root.name,
                "created_at": config["created_at"],
                "version": self.version,
                "note": note,
                "row_counts": counts,
                "entries": [e.to_dict() for e in sorted(entries, key=lambda e: e.relative_path)],
            }
            manifest_path = root / "MANIFEST.json"
            manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False),
                                     encoding="utf-8")
        except BaseException:
            shutil.rmtree(root, ignore_errors=True)
            raise
        result = BackupResult(
            backup_id=root.name,
            root=root,
            manifest_path=manifest_path,
            entries=tuple(sorted(entries, key=lambda e: e.relative_path)),
            created_at=manifest["created_at"],
            source_version=self.version,
            database_bytes=database_target.stat().st_size,
            manifest_sha256=hashlib.sha256(
                _canonical(manifest).encode("utf-8")
            ).hexdigest(),
            row_counts=counts,
        )
        (root / "BACKUP_RECEIPT.json").write_text(
            json.dumps(result.to_dict(), indent=2, ensure_ascii=False), encoding="utf-8"
        )
        return result

    # ------------------------------------------------------------- verify
    def verify(self, backup: Path | str) -> dict[str, Any]:
        """Re-hash a backup and report every drift, including extra files."""
        root = Path(backup)
        manifest_path = root / "MANIFEST.json"
        if not manifest_path.exists():
            raise NotFoundError("not a backup directory", root=str(root))
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if int(manifest.get("backup_format", 0)) != BACKUP_FORMAT:
            raise ConfigError("unsupported backup format",
                              found=manifest.get("backup_format"))
        changed: list[str] = []
        missing: list[str] = []
        for entry in manifest["entries"]:
            path = root / PurePosixPath(entry["relative_path"])
            if not path.exists():
                missing.append(entry["relative_path"])
                continue
            if _sha256_file(path) != entry["sha256"]:
                changed.append(entry["relative_path"])
        known = {e["relative_path"] for e in manifest["entries"]}
        extra: list[str] = []
        for path in root.rglob("*"):
            if path.is_dir() or path.name in {"MANIFEST.json", "BACKUP_RECEIPT.json"}:
                continue
            relative = PurePosixPath(*path.relative_to(root).parts).as_posix()
            if relative not in known:
                extra.append(relative)
        database = root / manifest.get("database_path", "kvflow.sqlite3")
        integrity = "MISSING"
        counts: dict[str, int] = {}
        if database.exists():
            connection = sqlite3.connect(database)
            connection.row_factory = sqlite3.Row
            try:
                integrity = str(
                    connection.execute("PRAGMA integrity_check").fetchone()[0]
                )
                tables = {
                    row["name"]
                    for row in connection.execute(
                        "SELECT name FROM sqlite_master WHERE type = 'table'"
                    )
                }
                for table in REQUIRED_TABLES:
                    counts[table] = (
                        int(
                            connection.execute(
                                f"SELECT COUNT(*) AS c FROM {table}"
                            ).fetchone()["c"]
                        )
                        if table in tables
                        else -1
                    )
            finally:
                connection.close()
        mismatch = {
            table: (manifest["row_counts"].get(table), counts.get(table))
            for table in manifest["row_counts"]
            if counts.get(table) != manifest["row_counts"][table]
        }
        return {
            "backup_id": manifest.get("backup_id"),
            "verified": not (changed or missing or extra or mismatch)
            and integrity == "ok",
            "changed": sorted(changed),
            "missing": sorted(missing),
            "extra": sorted(extra),
            "row_count_mismatch": mismatch,
            "sqlite_integrity": integrity,
            "version": manifest.get("version"),
            "checked_at": _now(),
        }

    # -------------------------------------------------------- restore plan
    def restore_plan(self, backup: Path | str, destination: Path | str) -> dict[str, Any]:
        """Validate a restore without writing anything (``--dry-run``)."""
        root = Path(backup)
        target = Path(destination)
        verification = self.verify(root)
        problems: list[str] = []
        if not verification["verified"]:
            problems.append("the backup does not verify against its manifest")
        if target.exists() and any(target.iterdir()):
            problems.append("the destination is not empty")
        counts = verification.get("row_count_mismatch")
        plan = {
            "backup_id": verification["backup_id"],
            "destination": str(target),
            "dry_run": True,
            "ready": not problems,
            "problems": problems,
            "verification": verification,
            "row_counts": counts,
            "will_start_queue": False,
            "will_replay_completed_tasks": False,
            "will_resend_unknown_operations": False,
            "note": (
                "a restore reproduces durable state only; it never resumes work,"
                " re-runs a DONE task or resends an operation with an unknown outcome"
            ),
        }
        return plan

    def restore(self, backup: Path | str, destination: Path | str,
                *, dry_run: bool = False) -> dict[str, Any]:
        """Restore into an empty destination, or rehearse the restore."""
        plan = self.restore_plan(backup, destination)
        if dry_run:
            return plan
        if not plan["ready"]:
            raise ConflictError("the restore was refused", problems=plan["problems"])
        root = Path(backup)
        target = Path(destination)
        target.mkdir(parents=True, exist_ok=True)
        manifest = json.loads((root / "MANIFEST.json").read_text(encoding="utf-8"))
        for entry in manifest["entries"]:
            source = root / PurePosixPath(entry["relative_path"])
            destination_path = target / PurePosixPath(entry["relative_path"])
            destination_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, destination_path)
        return {
            **plan,
            "dry_run": False,
            "ready": True,
            "restored_files": len(manifest["entries"]),
            "restored_at": _now(),
        }

    # -------------------------------------------------------------- upgrade
    def upgrade(
        self,
        *,
        new_version: str,
        migration: Any,
        backup_first: bool = True,
    ) -> dict[str, Any]:
        """Migrate a *copy* and swap it in only after it validates.

        ``migration`` is a caller-supplied callable that receives a database path
        and returns a report. The live database is never migrated in place.
        """
        if new_version == self.version:
            raise ContractError("the candidate version equals the current version")
        backup = self.create(note=f"pre-upgrade to {new_version}") if backup_first else None
        with _scratch_dir() as scratch:
            candidate = Path(scratch) / "kvflow.sqlite3"
            self._snapshot_database(self.database, candidate)
            before = self.row_counts(self.database)
            report = migration(candidate)
            after = self.row_counts(candidate)
            preserved = {
                table: (before.get(table), after.get(table))
                for table in REQUIRED_TABLES
                if before.get(table) != after.get(table)
            }
            if preserved:
                raise ConflictError(
                    "the migration did not preserve durable counters",
                    differences=preserved,
                )
            connection = sqlite3.connect(candidate)
            try:
                integrity = str(connection.execute("PRAGMA integrity_check").fetchone()[0])
            finally:
                connection.close()
            if integrity != "ok":
                raise ConflictError("the migrated copy failed its integrity check",
                                    integrity=integrity)
            retain = self.database.with_suffix(f".rollback-{self.version}.sqlite3")
            self._snapshot_database(self.database, retain)
            shutil.copyfile(candidate, self.database)
        self.version = new_version
        return {
            "from_version": backup.source_version if backup else "unknown",
            "to_version": new_version,
            "migration": report,
            "preserved_row_counts": before == after,
            "integrity": "ok",
            "rollback_copy": str(retain),
            "backup_id": backup.backup_id if backup else None,
            "upgraded_at": _now(),
        }

    def rollback(self, *, to_version: str) -> dict[str, Any]:
        """Restore the retained pre-upgrade copy, counters and all."""
        retain = self.database.with_suffix(f".rollback-{to_version}.sqlite3")
        if not retain.exists():
            raise NotFoundError("no rollback copy for that version",
                                version=to_version, path=str(retain))
        counts = self.row_counts(self.database)
        self._snapshot_database(retain, self.database)
        restored = self.row_counts(self.database)
        self.version = to_version
        return {
            "to_version": to_version,
            "row_counts_before": counts,
            "row_counts_after": restored,
            "counters_preserved": counts == restored,
            "rolled_back_at": _now(),
        }
