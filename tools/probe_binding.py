"""Check the current-project binding and the bridge, the way the plugin calls them."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

PRODUCT = Path(r"C:\Users\90428\Desktop\KVStock-restored\kvflow")
PY = Path(
    r"C:\Users\90428\Desktop\KVStock-restored\kvstock-platform"
    r"\agent_os\releases\1.0.0-rc1\.venv\Scripts\python.exe"
)
HOME = PRODUCT / ".runtime" / "smoke" / "home"
DEMO = PRODUCT / ".runtime" / "smoke" / "demo-python-api"


def bridge(tool: str, args: dict) -> dict:
    completed = subprocess.run(
        [str(PY), "-m", "kvflow.cli", "--home", str(HOME), "bridge",
         "--tool", tool, "--args", json.dumps(args)],
        capture_output=True, text=True, encoding="utf-8", errors="replace",
        env={"PATH": "", "PYTHONPATH": str(PRODUCT / "src"), "SYSTEMROOT": "C:\\Windows"},
        cwd=str(PRODUCT),
    )
    if not completed.stdout.strip():
        return {"exit": completed.returncode, "stderr": completed.stderr[-300:]}
    return json.loads(completed.stdout)


def main() -> int:
    exact = bridge("kvflow_current_project", {"path": str(DEMO)})
    print("exact   ", json.dumps(exact, ensure_ascii=False)[:220])
    nested = bridge("kvflow_current_project", {"path": str(DEMO / "src")})
    print("nested  ", json.dumps(nested, ensure_ascii=False)[:220])
    outside = bridge("kvflow_current_project", {"path": str(PRODUCT / "src")})
    print("outside ", json.dumps(outside, ensure_ascii=False)[:260])
    templates = bridge("kvflow_templates", {})
    print("templates", [item["id"] for item in templates["templates"]])
    unknown = bridge("kvflow_status", {"job_id": "does-not-exist"})
    print("unknown job refused:", unknown.get("code") or unknown.get("error_code"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
