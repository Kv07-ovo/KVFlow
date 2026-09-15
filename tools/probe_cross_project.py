"""Print the cross-project legs' details: node errors, review findings, diffs."""

from __future__ import annotations

import json
from pathlib import Path

PRODUCT = Path(r"C:\Users\90428\Desktop\KVStock-restored\kvflow")
receipt = json.loads(
    (PRODUCT / ".runtime" / "receipts" / "kvflow-cross-project-e2e.json").read_text(
        encoding="utf-8")
)
for name, leg in receipt["legs"].items():
    print("=" * 70)
    print(name, leg.get("status"), "plan_source", leg.get("plan_source"))
    print("  nodes      ", json.dumps(leg.get("nodes") or leg.get("states")))
    for node, report in (leg.get("node_reports") or {}).items():
        print(f"  {node:16s}", json.dumps(report, ensure_ascii=False)[:400])
    print("  review     ", leg.get("review"))
    print("  receipts   ", json.dumps(leg.get("test_receipts"), ensure_ascii=False))
    print("  diff       ", json.dumps(leg.get("diff"), ensure_ascii=False)[:200])
    print("  integration", json.dumps(leg.get("integration"), ensure_ascii=False)[:300])
    print("  problems   ", json.dumps(leg.get("problems"), ensure_ascii=False)[:400])
    print("  usage      ", json.dumps(leg.get("usage")), "live", leg.get("live_calls"))
