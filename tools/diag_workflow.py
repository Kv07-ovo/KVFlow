"""One workflow run with the scripted model, printing the full node report."""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import smoke_workflow as sw  # noqa: E402
from kvflow import registry, workflow  # noqa: E402

pid = sw.resolve_project_id()
config = registry.read_config(sw.SMOKE / "demo-python-api")
requirement = ("add a subtract(a, b) helper to src/calc.py and prove it with the"
               " registered profile")
provider = sw.ScriptedProvider()
run = workflow.run_requirement(
    requirement=requirement, project_id=pid, home=sw.HOME, template_id="feature",
    max_steps=6, max_completion_tokens=512,
    provider_factory=lambda: provider,
    manager_factory=lambda: sw.ScriptedManager(config, requirement),
)
print("status      ", run["status"])
print("plan_source ", run["plan_source"])
print("problems    ", json.dumps(run["problems"], ensure_ascii=False))
print("nodes       ", json.dumps(run["node_reports"], ensure_ascii=False, default=str)[:1500])
print("states      ", json.dumps(run["node_states_final"]))
print("receipts    ", json.dumps(run["test_receipts"]))
print("source_ok   ", run["source_not_modified"])

# which snapshot entries no longer match the source tree, and how the manifest is shaped
from kvflow.core.workspace import WorkspaceManager  # noqa: E402
from kvflow import registry as registry_module  # noqa: E402

project, _ = registry_module.compile_project(config, home=sw.HOME)
manager = WorkspaceManager(project)
scopes = list(config.allowed_read_roots)
for scope in config.allowed_write_roots:
    if scope not in scopes:
        scopes.append(scope)
snapshot = manager.create_snapshot(include_scopes=scopes, notes="diag")
first = snapshot.entries[0]
print("entry fields", sorted(first.to_dict()))
mismatch = []
for entry in snapshot.entries:
    path = Path(config.canonical_root) / entry.relative_path
    if not path.is_file():
        mismatch.append((entry.relative_path, "missing"))
        continue
    import hashlib

    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    if digest != entry.sha256:
        mismatch.append((entry.relative_path, digest[:12], entry.sha256[:12]))
print("snapshot entries", len(snapshot.entries), "mismatches", mismatch[:6])
