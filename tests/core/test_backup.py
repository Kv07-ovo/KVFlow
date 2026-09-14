"""Backup, restore rehearsal, upgrade and rollback tests with a live WAL."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

from kvflow.core.backup import BackupManager
from kvflow.core.contracts import NodeSpec, Plan, Role
from kvflow.core.errors import ConflictError, ConfigError, NotFoundError
from kvflow.core.store import Store

from .conftest import AUTH, make_project


def seed_store(database: Path, tmp_path: Path) -> Store:
    store = Store(database)
    store.initialize()
    project = make_project(tmp_path / "seed", project_id="backup-project")
    store.register_project(project)
    job_id = store.create_job(project.id, "back up and restore", AUTH)
    plan = Plan.model_validate(
        {
            "id": "plan-backup",
            "job_id": job_id,
            "version": 1,
            "revision": 1,
            "objective": "leave durable state worth preserving",
            "deliverables": ["state"],
            "constraints": ["no writes outside the workspace"],
            "acceptance": ["counters survive"],
            "nodes": [
                NodeSpec(
                    id="n1",
                    lineage_key="lin-n1",
                    objective="do the work",
                    write_scopes=["out"],
                    test_profile="unit",
                    allowed_tools=["repo.read"],
                )
            ],
            "roles": [Role.WORKER, Role.MANAGER],
            "allowed_roots": ["out"],
            "tool_profiles": ["unit"],
            "resource_budget": {
                "calls": 5,
                "input_tokens": 1000,
                "output_tokens": 1000,
                "tool_calls": 10,
                "storage_bytes": 100000,
                "wall_seconds": 600,
                "concurrency": 3,
                "deadline": datetime.now(timezone.utc).replace(year=2030),
            },
            "approval_boundaries": ["none"],
            "authorization_digest": AUTH,
            "created_at": datetime.now(timezone.utc),
        }
    )
    store.set_plan(plan, expected_job_revision=1)
    store.transition_job(job_id, "NEW", "PLANNING", actor="manager")
    store.transition_job(job_id, "PLANNING", "ASSIGNED", actor="manager")
    store.transition_job(job_id, "ASSIGNED", "WORKING", actor="worker")
    store.start_attempt(job_id, "n1")
    store.start_attempt(job_id, "n1")
    return store


@pytest.fixture()
def manager(tmp_path: Path):
    database = tmp_path / "kvflow.sqlite3"
    store = seed_store(database, tmp_path)
    backups = tmp_path / "backups"
    return BackupManager(database=database, backups_root=backups, version="1.0.0"), store, database


def test_backup_is_consistent_with_a_live_wal(tmp_path):
    database = tmp_path / "live.sqlite3"
    store = seed_store(database, tmp_path)
    writer = sqlite3.connect(database)
    writer.execute("PRAGMA journal_mode = WAL")
    writer.execute(
        "INSERT INTO events(job_id, actor, kind, payload, at)"
        " SELECT job_id, 'tester', 'WAL_ONLY', '{}', datetime('now') FROM jobs LIMIT 1"
    )
    writer.commit()
    wal = Path(str(database) + "-wal")
    assert wal.exists() and wal.stat().st_size > 0
    try:
        manager = BackupManager(
            database=database, backups_root=tmp_path / "backups", version="1.0.0"
        )
        result = manager.create(note="wal fixture")
        copy_path = result.root / "kvflow.sqlite3"
        connection = sqlite3.connect(copy_path)
        try:
            count = connection.execute(
                "SELECT COUNT(*) FROM events WHERE kind = 'WAL_ONLY'"
            ).fetchone()[0]
        finally:
            connection.close()
        assert count == 1, "the WAL-only row must be inside the backup"
        assert result.row_counts["node_attempts"] == 1
    finally:
        writer.close()


def test_backup_manifest_verifies_and_detects_tampering(manager):
    manager_obj, store, database = manager
    result = manager_obj.create(note="baseline")
    assert manager_obj.verify(result.root)["verified"] is True

    target = result.root / "kvflow.sqlite3"
    raw = bytearray(target.read_bytes())
    raw[-1] ^= 0xFF
    target.write_bytes(bytes(raw))
    report = manager_obj.verify(result.root)
    assert report["verified"] is False
    assert "kvflow.sqlite3" in report["changed"]


def test_restore_dry_run_reports_and_writes_nothing(manager):
    manager_obj, store, database = manager
    result = manager_obj.create()
    destination = manager_obj.backups_root.parent / "restore-target"
    plan = manager_obj.restore_plan(result.root, destination)
    assert plan["ready"] is True
    assert plan["dry_run"] is True
    assert plan["will_start_queue"] is False
    assert plan["will_replay_completed_tasks"] is False
    assert not destination.exists()


def test_restore_reproduces_counters_and_evidence(manager):
    manager_obj, store, database = manager
    before = store.checkpoint()
    result = manager_obj.create()
    destination = manager_obj.backups_root.parent / "restored"
    outcome = manager_obj.restore(result.root, destination)
    assert outcome["dry_run"] is False

    restored_db = destination / "kvflow.sqlite3"
    restored = Store(restored_db)
    after = restored.checkpoint()
    assert after["jobs"] == before["jobs"]
    assert after["schema_version"] == before["schema_version"]
    assert restored.row_counts_for_test() == store.row_counts_for_test()
    assert restored.lineage_attempts("backup-project", "lin-n1") == (2, 3)


def test_restore_refuses_a_non_empty_destination(manager):
    manager_obj, store, database = manager
    result = manager_obj.create()
    destination = manager_obj.backups_root.parent / "occupied"
    destination.mkdir(parents=True, exist_ok=True)
    (destination / "something.txt").write_text("in the way", encoding="utf-8")
    plan = manager_obj.restore_plan(result.root, destination)
    assert plan["ready"] is False
    assert any("not empty" in problem for problem in plan["problems"])
    with pytest.raises(ConflictError):
        manager_obj.restore(result.root, destination)


def test_restore_never_starts_work_or_replays_operations(manager):
    manager_obj, store, database = manager
    result = manager_obj.create()
    destination = manager_obj.backups_root.parent / "quiet"
    manager_obj.restore(result.root, destination)
    restored = Store(destination / "kvflow.sqlite3")
    # nothing is queued, no new lease appears, and the interrupted runs are still
    # visible as un-settled state rather than being silently replayed or dropped
    assert restored.pending_operations() == []
    assert restored.checkpoint()["active_leases"] == store.checkpoint()["active_leases"]
    assert len(restored.crashed_runs()) == len(store.crashed_runs())


def test_credentials_are_not_included_and_are_refused(manager, tmp_path):
    manager_obj, store, database = manager
    secret = tmp_path / "credentials.json"
    secret.write_text('{"api_key": "sk-not-a-real-key"}', encoding="utf-8")
    with pytest.raises(ConfigError):
        manager_obj.create(include=[secret])
    result = manager_obj.create(note="no secrets")
    blob = result.manifest_path.read_text(encoding="utf-8").lower()
    for forbidden in ("sk-", "api_key", "password", "bearer"):
        assert forbidden not in blob
    assert (result.root / "config" / "backup_config.json").exists()


def test_backup_includes_declared_artifacts(manager, tmp_path):
    manager_obj, store, database = manager
    artifact = tmp_path / "report.json"
    artifact.write_text('{"state": "DATA_BLOCKED"}', encoding="utf-8")
    result = manager_obj.create(include=[artifact], note="with an artifact")
    assert any(entry.kind == "artifact" for entry in result.entries)
    report = manager_obj.verify(result.root)
    assert report["verified"] is True


def test_upgrade_preserves_counters_and_rolls_back(manager):
    manager_obj, store, database = manager
    before = store.row_counts_for_test()

    def migration(path: Path) -> dict:
        connection = sqlite3.connect(path)
        try:
            connection.execute(
                "CREATE TABLE IF NOT EXISTS meta_extra(key TEXT PRIMARY KEY, value TEXT)"
            )
            connection.execute(
                "INSERT OR REPLACE INTO meta_extra(key, value) VALUES ('upgraded', 'yes')"
            )
            connection.commit()
        finally:
            connection.close()
        return {"added": ["meta_extra"]}

    outcome = manager_obj.upgrade(new_version="1.1.0", migration=migration)
    assert outcome["to_version"] == "1.1.0"
    assert outcome["preserved_row_counts"] is True
    assert outcome["integrity"] == "ok"
    assert manager_obj.row_counts(database) == before
    connection = sqlite3.connect(database)
    try:
        value = connection.execute(
            "SELECT value FROM meta_extra WHERE key = 'upgraded'"
        ).fetchone()[0]
    finally:
        connection.close()
    assert value == "yes"

    rolled = manager_obj.rollback(to_version="1.0.0")
    assert rolled["to_version"] == "1.0.0"
    assert rolled["counters_preserved"] is True
    connection = sqlite3.connect(database)
    try:
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
    finally:
        connection.close()
    assert "meta_extra" not in tables


def test_upgrade_refuses_a_migration_that_loses_counters(manager):
    manager_obj, store, database = manager

    def destructive(path: Path) -> dict:
        connection = sqlite3.connect(path)
        try:
            connection.execute("DELETE FROM node_attempts")
            connection.commit()
        finally:
            connection.close()
        return {"deleted": ["node_attempts"]}

    with pytest.raises(ConflictError) as excinfo:
        manager_obj.upgrade(new_version="1.1.0", migration=destructive)
    assert "preserve durable counters" in str(excinfo.value)
    # the live database is untouched
    assert store.row_counts_for_test()["node_attempts"] == 1


def test_rollback_without_a_copy_is_a_typed_error(manager):
    manager_obj, store, database = manager
    with pytest.raises(NotFoundError):
        manager_obj.rollback(to_version="9.9.9")


def test_verifying_a_non_backup_directory_is_a_typed_error(manager, tmp_path):
    manager_obj, store, database = manager
    plain = tmp_path / "not-a-backup"
    plain.mkdir()
    with pytest.raises(NotFoundError):
        manager_obj.verify(plain)
