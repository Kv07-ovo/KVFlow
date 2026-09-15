"""Which node produced the failing receipt, and what did the run record around it?"""

from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

PRODUCT = Path(r"C:\Users\90428\Desktop\KVStock-restored\kvflow")
sys.path.insert(0, str(PRODUCT / "src"))

HOME = PRODUCT / ".runtime" / "cross-project" / "home"
con = sqlite3.connect(str(HOME / "agent_os.sqlite3"))
con.row_factory = sqlite3.Row
print("-- receipts --")
for row in con.execute(
    "SELECT receipt_id, job_id, node_id, run_id, profile_id, exit_code, created_at"
    " FROM test_receipts ORDER BY created_at"
):
    print("  ", dict(row))
print("-- operations for the docs job --")
for row in con.execute(
    "SELECT operation_id, node_id, run_id, action, status, error_code, created_at"
    " FROM operations WHERE job_id IN (SELECT job_id FROM jobs WHERE project_id = 'docs-data-e2e')"
    " ORDER BY created_at"
):
    print("  ", dict(row))
print("-- runs for the docs job --")
for row in con.execute(
    "SELECT run_id, node_id, status, attempt, fence, created_at FROM runs"
    " WHERE job_id IN (SELECT job_id FROM jobs WHERE project_id = 'docs-data-e2e')"
    " ORDER BY created_at"
):
    print("  ", dict(row))
print("-- node events (last 12) --")
for row in con.execute(
    "SELECT node_id, kind, from_state, to_state, actor, at FROM events"
    " WHERE job_id IN (SELECT job_id FROM jobs WHERE project_id = 'docs-data-e2e')"
    " ORDER BY seq DESC LIMIT 12"
):
    print("  ", dict(row))
con.close()
