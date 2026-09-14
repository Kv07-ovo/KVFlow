"""Separate prepare from execute with a scripted model, to find the runner fault."""

from __future__ import annotations

import sys
from pathlib import Path

PRODUCT = Path(r"C:\Users\90428\Desktop\KVStock-restored\kvflow")
sys.path.insert(0, str(PRODUCT / "src"))
sys.path.insert(0, str(PRODUCT / "tools"))

import smoke_workflow as sw  # noqa: E402
from kvflow import workflow  # noqa: E402

project_id = sw.resolve_project_id()
manifest = workflow.prepare_run(
    requirement="add a divide(a, b) helper to src/calc.py and prove it with the profile",
    project_id=project_id, home=sw.HOME, template_id="feature",
)
print("prepared", manifest["job_id"], manifest["plan_source"], manifest["nodes"])
try:
    receipt = workflow.execute_run(
        job_id=manifest["job_id"], home=sw.HOME, max_steps=6, max_completion_tokens=512,
        provider_factory=lambda: sw.ScriptedProvider(),
    )
    print("status", receipt["status"], receipt["problems"])
    print("nodes ", {key: value.get("status") for key, value in receipt["node_reports"].items()})
    print("errors", {key: value.get("error") for key, value in receipt["node_reports"].items()
                     if value.get("error")})
except BaseException as exc:  # noqa: BLE001 - this probe must print the real failure
    import traceback

    print("EXECUTE RAISED", type(exc).__name__, exc)
    traceback.print_exc()
