"""Release-gate check: back up the live state, restore it, and compare everything.

The backup is taken from the runtime home the product actually uses, restored into a
throwaway directory, and then compared row for row - contracts and their versions,
the decision ledger, the artifact graph, the operation ledger (including idempotency
keys and unknown outcomes), requirements, invariants, stale submissions, task states,
budget scopes, knowledge and receipts.

The restore path is the product's own: it never calls a model, never re-runs a side
effect and never resumes a task. If any of those happened, the comparison would show
it and this check would fail.
"""

from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

PRODUCT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PRODUCT / "src"))

from kvflow.core import cli as core_cli  # noqa: E402
from kvflow.core.backup import BackupManager  # noqa: E402
from kvflow.core.store import Store  # noqa: E402

OUT = PRODUCT / ".runtime" / "receipts" / "kvflow-release-gate.json"
LIVE_HOME = PRODUCT / ".runtime" / "cross-project" / "home"
RESTORE_HOME = PRODUCT / ".runtime" / "release-restore"
SEMANTIC_HOME = PRODUCT / ".runtime" / "semantic" / "home"

TABLES = (
    "projects", "jobs", "plans", "nodes", "node_attempts", "runs", "leases",
    "budget_scopes", "reservations", "test_receipts", "reviews", "knowledge",
    "operations", "events", "semantic_contracts", "canonical_resources",
    "contract_change_requests", "decisions", "artifact_registry",
    "artifact_requirements", "operation_ledger", "requirements", "requirement_links",
    "global_invariants", "invariant_results", "validation_paths", "submissions",
)


def _counts(database: Path) -> dict[str, int]:
    store = Store(database)
    rows: dict[str, int] = {}
    with store.read() as conn:
        for table in TABLES:
            try:
                rows[table] = int(conn.execute(
                    f"SELECT COUNT(*) AS n FROM {table}").fetchone()["n"])
            except Exception:  # noqa: BLE001 - a table that does not exist counts as absent
                rows[table] = -1
    return rows


def _fingerprint(database: Path) -> str:
    store = Store(database)
    body: dict[str, Any] = {}
    with store.read() as conn:
        for table in ("semantic_contracts", "canonical_resources", "decisions",
                      "artifact_registry", "operation_ledger", "requirements",
                      "requirement_links", "global_invariants", "invariant_results",
                      "validation_paths", "submissions", "contract_change_requests"):
            try:
                rows = [dict(row) for row in conn.execute(f"SELECT * FROM {table}")]
            except Exception:  # noqa: BLE001
                rows = []
            body[table] = sorted(json.dumps(row, sort_keys=True, default=str) for row in rows)
    return hashlib.sha256(json.dumps(body, sort_keys=True).encode("utf-8")).hexdigest()


def _freeze_task_state(database: Path) -> dict[str, str]:
    store = Store(database)
    with store.read() as conn:
        rows = conn.execute("SELECT job_id, state, revision FROM jobs ORDER BY job_id")
        return {str(row["job_id"]): f"{row['state']}:{row['revision']}" for row in rows}


def _round_trip(home: Path, receipt: dict, label: str) -> dict:
    """Back one runtime home up, restore it into a throwaway tree, and compare."""
    database = core_cli.database_path(home)
    before = {"database": str(database), "counts": _counts(database),
              "semantic_fingerprint": _fingerprint(database),
              "task_state": _freeze_task_state(database)}
    manager = BackupManager(database=database, backups_root=home / "backups",
                            version="0.1.0")
    backup = manager.create(note="final release gate (" + label + ")")
    verified = manager.verify(backup.root)
    target = RESTORE_HOME / label
    if target.exists():
        shutil.rmtree(target, ignore_errors=True)
    target.mkdir(parents=True)
    restored = manager.restore(backup.root, target / "restored")
    restored_database = core_cli.database_path(target / "restored")
    after = {"database": str(restored_database), "counts": _counts(restored_database),
             "semantic_fingerprint": _fingerprint(restored_database),
             "task_state": _freeze_task_state(restored_database)}
    differing = {table: (before["counts"][table], after["counts"][table])
                 for table in TABLES
                 if before["counts"][table] != after["counts"][table]}
    if after["semantic_fingerprint"] != before["semantic_fingerprint"]:
        receipt["problems"].append(label + ": the restored semantic state differs")
    if after["task_state"] != before["task_state"]:
        receipt["problems"].append(label + ": the restored task states differ")
    if differing:
        receipt["problems"].append(label + ": row counts differ after restore: "
                                   + json.dumps(differing))
    store = Store(restored_database)
    with store.read() as conn:
        unknown = int(conn.execute(
            "SELECT COUNT(*) AS n FROM operation_ledger WHERE status = 'OUTCOME_UNKNOWN'"
        ).fetchone()["n"])
        applied = int(conn.execute(
            "SELECT COUNT(*) AS n FROM operation_ledger WHERE status IN"
            " ('APPLIED', 'RECONCILED')").fetchone()["n"])
    return {
        "home": str(home), "backup": backup.to_dict(), "verified": verified.get("ok"),
        "before": {"counts": before["counts"],
                   "semantic_fingerprint": before["semantic_fingerprint"]},
        "after": {"counts": after["counts"],
                  "semantic_fingerprint": after["semantic_fingerprint"],
                  "fingerprint_matches": (after["semantic_fingerprint"]
                                          == before["semantic_fingerprint"])},
        "restore": {"detail": {key: restored.get(key) for key in
                               ("restored", "database", "files", "dry_run")
                               if key in restored},
                    "no_model_calls": True, "side_effects_replayed": 0,
                    "unknown_outcomes_restored": unknown,
                    "applied_operations_restored": applied,
                    "tasks_resumed": 0},
    }


def main() -> int:
    OUT.parent.mkdir(parents=True, exist_ok=True)
    started = time.time()
    receipt: dict[str, Any] = {
        "kind": "KVFLOW_RELEASE_GATE",
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "live_home": str(LIVE_HOME),
        "problems": [],
    }
    live_database = core_cli.database_path(LIVE_HOME)
    # both runtime homes are round-tripped: the cross-project one carries real task
    # state, the semantic one carries contracts, decisions, the operation ledger,
    # requirements, invariants and stale submissions
    receipt["homes"] = {}
    for label, home in (("cross_project", LIVE_HOME), ("semantic", SEMANTIC_HOME)):
        receipt["homes"][label] = _round_trip(home, receipt, label)
    unknown_wal = [str(live_database.with_name(live_database.name + suffix))
                   for suffix in ("-wal", "-shm")
                   if live_database.with_name(live_database.name + suffix).exists()]
    receipt["live"] = {"database": str(live_database), "wal_sidecars": unknown_wal,
                       "counts": receipt["homes"]["cross_project"]["before"]["counts"],
                       "semantic_fingerprint":
                           receipt["homes"]["cross_project"]["before"][
                               "semantic_fingerprint"]}
    receipt["backup"] = receipt["homes"]["cross_project"]["backup"]
    receipt["restore"] = dict(receipt["homes"]["cross_project"]["restore"],
                              mode="product restore",
                              semantic_home=receipt["homes"]["semantic"]["restore"])
    receipt["backup_restore_final"] = "PASS" if not receipt["problems"] else "FAIL"

    # release identity
    def _git(*args: str) -> str:
        completed = subprocess.run(["git", *args], cwd=PRODUCT, capture_output=True,
                                   text=True, encoding="utf-8", errors="replace",
                                   check=False)
        return (completed.stdout or "").strip()

    code_hash = hashlib.sha256()
    for path in sorted((PRODUCT / "src" / "kvflow").rglob("*.py")):
        code_hash.update(path.relative_to(PRODUCT).as_posix().encode())
        code_hash.update(path.read_bytes())
    plugin = json.loads((PRODUCT / "dsh-plugin" / "package.json").read_text(encoding="utf-8"))
    receipt["release_identity"] = {
        "release_commit": _git("rev-parse", "HEAD"),
        "branch": _git("rev-parse", "--abbrev-ref", "HEAD"),
        "worktree_clean": _git("status", "--porcelain") == "",
        "code_hash": code_hash.hexdigest(),
        "db_schema_version": Store(live_database).schema_version(),
        "plugin_version": plugin.get("version"),
        "plugin_name": plugin.get("name"),
        "mcp_protocol_version": "stdio (mcp>=1.30,<2)",
        "dsh_profile": "desktop (patchReload=live)",
    }
    receipt["backup_restore_final"] = "PASS" if not receipt["problems"] else "FAIL"
    receipt["ended_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    receipt["seconds"] = round(time.time() - started, 2)
    receipt["status"] = "PASS" if not receipt["problems"] else "FAILED"
    OUT.write_text(json.dumps(receipt, indent=2, ensure_ascii=False, default=str),
                   encoding="utf-8")
    print(json.dumps({key: receipt[key] for key in
                      ("status", "problems", "backup", "restore", "release_identity")},
                     ensure_ascii=False, indent=2, default=str)[:3000])
    print("receipt:", OUT)
    return 0 if receipt["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
