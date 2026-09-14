"""Inspect one KVFlow run: node states, operations, runs, reservations, events."""

from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

PRODUCT = Path(r"C:\Users\90428\Desktop\KVStock-restored\kvflow")
sys.path.insert(0, str(PRODUCT / "src"))

from kvflow import workflow  # noqa: E402

home = Path(sys.argv[1])
job_id = sys.argv[2]
status = workflow.status(home, job_id)
print("job state   ", status["state"], "revision", status["revision"])
print("node states ", json.dumps(status["nodes"]))
print("receipts    ", json.dumps(status["receipts"]))
print("plan        ", json.dumps({k: status.get(k) for k in ("template", "model_profile", "plan_source")}))

con = sqlite3.connect(str(home / "agent_os.sqlite3"))
con.row_factory = sqlite3.Row
print("-- operations --")
for row in con.execute(
    "SELECT action, status, error_code, created_at, updated_at FROM operations WHERE job_id = ?"
    " ORDER BY created_at", (job_id,)
):
    print("  ", dict(row))
print("-- runs --")
for row in con.execute(
    "SELECT node_id, status, attempt, fence, created_at, updated_at FROM runs"
    " WHERE job_id = ? ORDER BY created_at", (job_id,)
):
    print("  ", dict(row))
print("-- leases --")
for row in con.execute(
    "SELECT node_id, status, fence, expires_at FROM leases WHERE job_id = ?", (job_id,)
):
    print("  ", dict(row))
print("-- reservations --")
for row in con.execute(
    "SELECT scope_id, state, reserved, actual FROM reservations ORDER BY created_at DESC LIMIT 4"
):
    print("  ", dict(row))
print("-- last events --")
for row in con.execute(
    "SELECT kind, from_state, to_state, actor, at FROM events WHERE job_id = ?"
    " ORDER BY seq DESC LIMIT 8", (job_id,)
):
    print("  ", dict(row))
con.close()
