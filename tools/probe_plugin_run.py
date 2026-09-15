"""Report the KVFlow runs and whether the plugin-launched run exists and finished."""

from __future__ import annotations

import json
import sys
from pathlib import Path

PRODUCT = Path(r"C:\Users\90428\Desktop\KVStock-restored\kvflow")
sys.path.insert(0, str(PRODUCT / "src"))

from kvflow import workflow  # noqa: E402

HOME = PRODUCT / ".runtime" / "smoke" / "home"
listing = workflow.list_runs(HOME, limit=12)
print("runs:", listing["count"])
for row in listing["runs"]:
    print(f"  {row['job_id']}  {row['state']:10s} {row.get('plan_source')}  {row['objective'][:70]}")

candidates = [row for row in listing["runs"] if "cube" in row["objective"]]
print("plugin-launched candidates:", [row["job_id"] for row in candidates])
for row in candidates:
    manifest = workflow.run_manifest(HOME, row["job_id"])
    receipt = manifest.get("last_receipt") or {}
    print("job", row["job_id"], "state", row["state"],
          "runner_mode", manifest.get("runner_mode"),
          "status", receipt.get("status"),
          "problems", json.dumps(receipt.get("problems"), ensure_ascii=False)[:200])
    print("  nodes", json.dumps(receipt.get("node_states_final")))
    print("  receipts", json.dumps(receipt.get("test_receipts")))
    print("  review", (receipt.get("review") or {}).get("verdict"),
          "integration", json.dumps({k: v for k, v in (receipt.get("integration") or {}).items()
                                     if k != "integration_root"}, ensure_ascii=False)[:200])
    print("  live", (receipt.get("worker_totals") or {}).get("live_calls"),
          "usage", json.dumps((receipt.get("worker_totals") or {}).get("usage")))
    print("  diff", json.dumps(receipt.get("diff"), ensure_ascii=False)[:200])
    print("  source_not_modified", receipt.get("source_not_modified"))
