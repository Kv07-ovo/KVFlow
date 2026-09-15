"""An ``argv`` profile may run one of the product's own commands.

The artifact checker is the shipped example: a documentation or data project's
approved evidence is that checker's exit code. The interpreter that runs it gets a
controlled environment, so the product's own import root has to be supplied by the
executor -- otherwise ``python -m kvflow.checks.artifact`` fails with
``No module named 'kvflow'`` and a documentation project can never produce a
passing receipt.
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone

from kvflow.core.contracts import NodeSpec, Plan, Role, TestProfile
from kvflow.core.security import CapabilityAuthority
from kvflow.core.store import Store
from kvflow.core.tools import ToolService
from kvflow.core.workspace import WorkspaceManager

from .conftest import AUTH, make_project

CHECKER_ARGV = [
    "python", "-m", "kvflow.checks.artifact", "--require", "docs/report.md:16",
]


def test_an_argv_profile_runs_the_products_own_checker(tmp_path, store: Store):
    source = tmp_path / "docs-source"
    (source / "docs").mkdir(parents=True, exist_ok=True)
    (source / "docs" / "report.md").write_text(
        "# Report\n\nA finding with a source.\n", encoding="utf-8"
    )
    profile = TestProfile(
        id="artifact",
        project_id="docs-project",
        description="the shipped artifact checker over the declared output",
        runner="argv",
        argv=CHECKER_ARGV,
        timeout_seconds=120,
        code_digest=hashlib.sha256(b"checker").hexdigest(),
        authorization_digest=AUTH,
    )
    project = make_project(
        tmp_path,
        project_id="docs-project",
        source_root=str(source),
        allowed_read_roots=["."],
        allowed_write_roots=["docs"],
        protected_roots=[],
        test_profiles=[profile],
    )
    store.register_project(project)
    job_id = store.create_job(project.id, "run the artifact checker", AUTH)
    plan = Plan.model_validate(
        {
            "id": "plan-docs",
            "job_id": job_id,
            "version": 1,
            "revision": 1,
            "objective": "prove the artifact exists",
            "deliverables": ["a checker receipt"],
            "constraints": ["writes stay inside docs"],
            "acceptance": ["the checker exits zero"],
            "nodes": [
                NodeSpec(
                    id="n1",
                    lineage_key="lin-docs",
                    objective="check the artifact",
                    write_scopes=["docs"],
                    test_profile="artifact",
                    allowed_tools=["repo.read", "repo.list", "test.run_profile"],
                )
            ],
            "roles": [Role.WORKER, Role.MANAGER],
            "allowed_roots": ["docs"],
            "tool_profiles": ["artifact"],
            "resource_budget": {
                "calls": 4,
                "input_tokens": 4000,
                "output_tokens": 4000,
                "tool_calls": 8,
                "storage_bytes": 500_000,
                "wall_seconds": 600,
                "concurrency": 1,
                "deadline": datetime.now(timezone.utc).replace(year=2030),
            },
            "approval_boundaries": ["no push"],
            "authorization_digest": AUTH,
            "created_at": datetime.now(timezone.utc),
        }
    )
    store.set_plan(plan, expected_job_revision=1)
    store.transition_job(job_id, "NEW", "PLANNING", actor="manager")
    store.transition_job(job_id, "PLANNING", "ASSIGNED", actor="manager")
    manager = WorkspaceManager(project)
    snapshot = manager.create_snapshot(include_scopes=["docs"], notes="docs fixture")
    manager.create_workspace(
        job_id=job_id, node_id="n1", snapshot_id=snapshot.snapshot_id, scopes=["docs"]
    )
    run = store.start_attempt(job_id, "n1")
    authority = CapabilityAuthority(store)
    ticket, _ = authority.issue(
        project_id=project.id,
        job_id=job_id,
        node_id="n1",
        run_id=run["run_id"],
        lease_id=run["lease_id"],
        role="worker",
        actions=["repo.read", "repo.list", "test.run_profile"],
    )
    service = ToolService(store, authority, workspace_root=manager.managed_root)
    result = service.invoke(ticket, "test.run_profile", {"profile_id": "artifact"})
    assert result.ok is True
    receipt = result.output["receipt"]
    assert receipt["exit_code"] == 0, result.output.get("stdout", "")[-400:]
    assert receipt["argv"][0].endswith("python.exe") or "python" in receipt["argv"][0]
    assert "kvflow.checks.artifact" in receipt["argv"]
    assert result.output["passed"] is True
