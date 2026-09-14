"""Report what the last inline runner did: mode, error field, and its log tail."""

from __future__ import annotations

import json
from pathlib import Path

HOME = Path(r"C:\Users\90428\Desktop\KVStock-restored\kvflow\.runtime\smoke\home")
runs = HOME / "runs"
manifests = sorted(runs.glob("*.json"), key=lambda path: path.stat().st_mtime)
if not manifests:
    raise SystemExit("no run manifests")
latest = manifests[-1]
data = json.loads(latest.read_text(encoding="utf-8"))
print("manifest        ", latest.name)
for key in ("job_id", "plan_source", "runner_mode", "runner_pid", "runner_started_at",
            "runner_failed_at", "last_receipt"):
    if key in data:
        print(f"{key:16s}", json.dumps(data[key], ensure_ascii=False)[:400])
print("runner_error    ", (data.get("runner_error") or "none")[:1500])
log = latest.with_suffix(".log")
if log.exists():
    print("--- log tail ---")
    print(log.read_text(encoding="utf-8", errors="replace")[-1500:])
else:
    print("no log file at", log)
