"""Tool-group tests: capability-gated Repo/Test/Knowledge operations.

Every call goes through a real Orchestrator ticket bound to a real run, lease and
fence, and every file operation lands inside an owned workspace snapshot. Nothing
here reaches the registered source root or any arbitrary shell/SQL/URL.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from kvflow.core.budget import BudgetLedger
from kvflow.core.contracts import ExperimentSpec, NodeSpec, Plan, Role, TestProfile
from kvflow.core.errors import AuthorizationError, ContractError, PathDenied
from kvflow.core.security import CapabilityAuthority
from kvflow.core.state import TaskState
from kvflow.core.store import Store
from kvflow.core.tools import ToolService, workspace_scope_fingerprint
from kvflow.core.workspace import WorkspaceManager, manifest_digest

from .conftest import AUTH, make_project

SOURCE_FILE = "def add(a, b):\n    return a + b\n"
TEST_FILE = "from calc import add\n\n\ndef test_add():\n    assert add(1, 2) == 3\n"
SECRET = "DO_NOT_READ_ME"


@pytest.fixture()
def tool_env(tmp_path: Path, store: Store):
    """A registered project with a snapshot, a workspace and a live ticket."""
    source_root = tmp_path / "source"
    (source_root / "src").mkdir(parents=True, exist_ok=True)
    (source_root / "tests").mkdir(parents=True, exist_ok=True)
    (source_root / "src" / "calc.py").write_text(SOURCE_FILE, encoding="utf-8")
    (source_root / "tests" / "test_calc.py").write_text(TEST_FILE, encoding="utf-8")
    (source_root / "secret.txt").write_text(SECRET, encoding="utf-8")
    (source_root / "golden_reference").mkdir(exist_ok=True)
    (source_root / "golden_reference" / "frozen.py").write_text("FROZEN", encoding="utf-8")

    profile = TestProfile(
        id="unit",
        project_id="tools-project",
        description="focused unit tests",
        runner="pytest",
        targets=["tests"],
        pythonpath=["src"],
        timeout_seconds=120,
        code_digest=hashlib.sha256(b"code").hexdigest(),
        authorization_digest=AUTH,
    )
    heavy = TestProfile(
        id="heavy",
        project_id="tools-project",
        description="heavy integration profile",
        runner="pytest",
        targets=["tests"],
        pythonpath=["src"],
        heavy=True,
        code_digest=hashlib.sha256(b"code").hexdigest(),
        authorization_digest=AUTH,
    )
    project = make_project(
        tmp_path,
        project_id="tools-project",
        source_root=str(source_root),
        allowed_read_roots=["."],
        allowed_write_roots=["src", "tests"],
        protected_roots=["golden_reference"],
        test_profiles=[profile, heavy],
    )
    store.register_project(project)
    ledger = BudgetLedger(store)
    job_id = store.create_job(project.id, "operate inside an owned workspace", AUTH)
    plan = Plan.model_validate(
        {
            "id": "plan-tools",
            "job_id": job_id,
            "version": 1,
            "revision": 1,
            "objective": "change code and prove it with a registered test profile",
            "deliverables": ["a patched source file", "a test receipt"],
            "constraints": ["writes stay inside the owned workspace"],
            "acceptance": ["the registered profile passes"],
            "nodes": [
                NodeSpec(
                    id="n1",
                    lineage_key="lin-n1",
                    objective="patch calc and run unit tests",
                    write_scopes=["src", "tests"],
                    test_profile="unit",
                    allowed_tools=[
                        "repo.read", "repo.search", "repo.list", "repo.status",
                        "repo.diff", "repo.patch", "repo.commit", "test.run_profile",
                        "operation.status", "operation.result", "operation.cancel",
                    ],
                )
            ],
            "roles": [Role.WORKER, Role.MANAGER],
            "allowed_roots": ["src", "tests"],
            "tool_profiles": ["unit", "heavy"],
            "resource_budget": {
                "calls": 10,
                "input_tokens": 10000,
                "output_tokens": 10000,
                "tool_calls": 50,
                "storage_bytes": 1_000_000,
                "wall_seconds": 3600,
                "concurrency": 3,
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
    snapshot = manager.create_snapshot(include_scopes=["src", "tests"], notes="tools fixture")
    handle = manager.create_workspace(
        job_id=job_id, node_id="n1", snapshot_id=snapshot.snapshot_id, scopes=["src", "tests"]
    )
    node_actions = [
        action.value if hasattr(action, "value") else str(action)
        for action in plan.nodes[0].allowed_tools
    ]
    run = store.start_attempt(job_id, "n1")
    authority = CapabilityAuthority(store)
    ticket, _ = authority.issue(
        project_id=project.id,
        job_id=job_id,
        node_id="n1",
        run_id=run["run_id"],
        lease_id=run["lease_id"],
        role="worker",
        actions=node_actions,
    )
    service = ToolService(store, authority, workspace_root=manager.managed_root)
    return {
        "project": project,
        "manager": manager,
        "handle": handle,
        "snapshot": snapshot,
        "store": store,
        "ledger": ledger,
        "authority": authority,
        "service": service,
        "ticket": ticket,
        "run": run,
        "job_id": job_id,
        "source_root": source_root,
    }


# ------------------------------------------------------------- authorization


def test_a_call_without_a_ticket_is_refused(tool_env):
    service = tool_env["service"]
    result = service.invoke("", "repo.list", {"path": "."})
    assert result.ok is False
    assert result.status == "NOT_DISPATCHED"
    assert result.operation_id is None


def test_a_call_outside_the_allowlist_is_refused(tool_env):
    service = tool_env["service"]
    result = service.invoke(tool_env["ticket"], "research.run_authorized", {})
    assert result.ok is False
    assert result.error_code == "CAPABILITY_DENIED"


def test_an_unknown_tool_name_is_refused(tool_env):
    authority = tool_env["authority"]
    project = tool_env["project"]
    run = tool_env["run"]
    ticket, _ = authority.issue(
        project_id=project.id,
        job_id=tool_env["job_id"],
        node_id="n1",
        run_id=run["run_id"],
        lease_id=run["lease_id"],
        role="worker",
        actions=["repo.read"],
    )
    result = tool_env["service"].invoke(ticket, "shell.exec", {})
    assert result.ok is False
    assert result.error_code == "CAPABILITY_DENIED"


# --------------------------------------------------------------------- repo


def test_repo_read_is_inside_the_workspace_only(tool_env):
    service = tool_env["service"]
    ticket = tool_env["ticket"]
    ok = service.invoke(ticket, "repo.read", {"path": "src/calc.py"})
    assert ok.ok is True
    assert ok.output["text"] == SOURCE_FILE
    assert ok.operation_id and ok.output_digest

    for bad in ("../secret.txt", "C:/Windows/win.ini", "/etc/passwd", "//server/share/x"):
        refused = service.invoke(ticket, "repo.read", {"path": bad})
        assert refused.ok is False, bad
        # a path rejected after dispatch is recorded as a failed operation, and
        # the typed code says exactly why it was refused
        assert refused.status == "FAILED"
        assert refused.error_code == "PATH_DENIED"


def test_repo_read_cannot_reach_the_registered_source_root(tool_env):
    """The workspace is a copy: reading it never touches the source project."""
    service = tool_env["service"]
    result = service.invoke(tool_env["ticket"], "repo.read", {"path": "secret.txt"})
    assert result.ok is False
    assert (tool_env["source_root"] / "secret.txt").read_text(encoding="utf-8") == SECRET


def test_repo_search_is_bounded(tool_env):
    service = tool_env["service"]
    result = service.invoke(tool_env["ticket"], "repo.search", {"pattern": "def ", "path": "."})
    assert result.ok is True
    assert result.output["match_count"] >= 2
    assert result.output["truncated"] is False


def test_repo_patch_only_writes_declared_scopes(tool_env):
    service = tool_env["service"]
    ticket = tool_env["ticket"]
    applied = service.invoke(
        ticket,
        "repo.patch",
        {"edits": [{"path": "src/calc.py", "content": "def add(a, b):\n    return a + b + 1\n"}]},
    )
    assert applied.ok is True
    assert applied.output["applied"][0]["previous_sha256"] == hashlib.sha256(
        SOURCE_FILE.encode()
    ).hexdigest()
    written = (tool_env["handle"].root / "src" / "calc.py").read_text(encoding="utf-8")
    assert written.endswith("return a + b + 1\n")
    # the source project is untouched
    assert (tool_env["source_root"] / "src" / "calc.py").read_text(encoding="utf-8") == SOURCE_FILE


def test_repo_patch_outside_the_declared_scopes_is_refused(tool_env):
    service = tool_env["service"]
    ticket = tool_env["ticket"]
    for bad in ("docs/notes.md", "../escape.py", "golden_reference/x.py"):
        result = service.invoke(
            ticket, "repo.patch", {"edits": [{"path": bad, "content": "x"}]}
        )
        assert result.ok is False, bad
        assert result.error_code in {"AUTHORIZATION_INVALID", "PATH_DENIED"}


def test_repo_status_and_diff_reporter_uses_the_snapshot_base(tool_env):
    service = tool_env["service"]
    ticket = tool_env["ticket"]
    before = service.invoke(ticket, "repo.status", {})
    assert before.output["change_count"] == 0
    service.invoke(
        ticket, "repo.patch", {"edits": [{"path": "src/new.py", "content": "value = 1\n"}]}
    )
    after = service.invoke(ticket, "repo.status", {})
    assert after.output["change_count"] == 1
    change = after.output["changed"][0]
    assert change["relative_path"] == "src/new.py"
    assert change["change"] == "ADDED"
    diff = service.invoke(ticket, "repo.diff", {})
    assert diff.output["change_count"] == 1
    assert diff.output["base_snapshot"] == tool_env["snapshot"].snapshot_id


def test_repo_commit_runs_inside_the_workspace(tool_env):
    service = tool_env["service"]
    ticket = tool_env["ticket"]
    service.invoke(
        ticket, "repo.patch", {"edits": [{"path": "src/calc.py", "content": "value = 2\n"}]}
    )
    result = service.invoke(ticket, "repo.commit", {"message": "change calc"})
    assert result.ok is True
    assert len(result.output["commit"]) == 40
    # the author is the authenticated role, not a caller-supplied string
    assert "worker" in result.output["author"]


# --------------------------------------------------------------------- test


def test_registered_profile_runs_and_returns_a_receipt(tool_env):
    service = tool_env["service"]
    ticket = tool_env["ticket"]
    result = service.invoke(ticket, "test.run_profile", {"profile_id": "unit"})
    assert result.ok is True
    receipt = result.output["receipt"]
    assert receipt["exit_code"] == 0
    assert receipt["timed_out"] is False
    assert receipt["reported_tests"] == 1
    assert receipt["stdout_sha256"] and receipt["code_digest"]
    assert receipt["profile_id"] == "unit"
    # a fixed argv, never a model-supplied command string
    assert "-m" in receipt["argv"] and "pytest" in receipt["argv"]
    assert not any("shell" in part for part in receipt["argv"])


def test_the_executor_receipt_is_durable_and_bound_to_its_run(tool_env):
    """Approval evidence must outlive the process that produced it."""
    service = tool_env["service"]
    store = tool_env["store"]
    ticket = tool_env["ticket"]
    before = store.receipts(tool_env["job_id"])
    result = service.invoke(ticket, "test.run_profile", {"profile_id": "unit"})
    assert result.ok is True
    receipt_id = result.output["receipt_id"]
    assert receipt_id and result.output["receipt"]["receipt_id"] == receipt_id
    stored = [row for row in store.receipts(tool_env["job_id"]) if row["receipt_id"] == receipt_id]
    assert len(stored) == 1
    assert stored[0]["exit_code"] == 0
    assert stored[0]["run_id"] == result.audit["run_id"]
    assert len(store.receipts(tool_env["job_id"])) == len(before) + 1
    # the durable document is the executor's own object, not a caller-supplied dict
    durable = store.receipt(receipt_id)
    assert durable.binding.run_id == result.audit["run_id"]
    assert durable.binding.fence == result.audit["fence"]
    assert durable.exit_code == 0


def test_an_unregistered_profile_is_refused(tool_env):
    result = tool_env["service"].invoke(
        tool_env["ticket"], "test.run_profile", {"profile_id": "made-up"}
    )
    assert result.ok is False
    assert result.error_code == "AUTHORIZATION_INVALID"


def test_failing_profile_reports_a_failure_not_a_pass(tool_env):
    service = tool_env["service"]
    ticket = tool_env["ticket"]
    service.invoke(
        ticket,
        "repo.patch",
        {"edits": [{"path": "src/calc.py", "content": "def add(a, b):\n    return a - b\n"}]},
    )
    result = service.invoke(ticket, "test.run_profile", {"profile_id": "unit"})
    assert result.ok is True  # the tool ran
    assert result.output["passed"] is False
    assert result.output["receipt"]["exit_code"] != 0


def test_test_child_does_not_inherit_model_credentials(tool_env, monkeypatch):
    monkeypatch.setenv("DEEPSEEK_API_KEY", "FAKE_SENTINEL_VALUE")
    service = tool_env["service"]
    ticket = tool_env["ticket"]
    service.invoke(
        ticket,
        "repo.patch",
        {
            "edits": [
                {"path": "src/calc.py", "content": SOURCE_FILE},
                {
                    "path": "tests/test_env.py",
                    "content": (
                        "import os\n\n\ndef test_no_credentials():\n"
                        "    assert 'DEEPSEEK_API_KEY' not in os.environ\n"
                        "    assert 'HTTPS_PROXY' not in os.environ\n"
                    ),
                },
            ]
        },
    )
    result = service.invoke(ticket, "test.run_profile", {"profile_id": "unit"})
    assert result.output["passed"] is True, result.output["stdout"]
    assert "DEEPSEEK_API_KEY" not in "".join(result.output["receipt"]["argv"])


# -------------------------------------------------------------- operations


def test_operation_status_is_scoped_to_its_own_run(tool_env):
    service = tool_env["service"]
    ticket = tool_env["ticket"]
    run = service.invoke(ticket, "repo.list", {"path": "."})
    assert run.operation_id
    status = service.invoke(ticket, "operation.status", {"operation_id": run.operation_id})
    assert status.output["status"] == "SUCCEEDED"

    store = tool_env["store"]
    other_run = store.start_attempt(tool_env["job_id"], "n1")
    other_ticket, _ = tool_env["authority"].issue(
        project_id=tool_env["project"].id,
        job_id=tool_env["job_id"],
        node_id="n1",
        run_id=other_run["run_id"],
        lease_id=other_run["lease_id"],
        role="worker",
        actions=["operation.status"],
    )
    stolen = service.invoke(other_ticket, "operation.status", {"operation_id": run.operation_id})
    assert stolen.ok is False
    assert stolen.error_code == "AUTHORIZATION_INVALID"


def test_result_and_cancel_report_settled_state(tool_env):
    service = tool_env["service"]
    ticket = tool_env["ticket"]
    run = service.invoke(ticket, "repo.list", {"path": "."})
    result = service.invoke(ticket, "operation.result", {"operation_id": run.operation_id})
    assert result.output["status"] == "SUCCEEDED"
    cancel = service.invoke(ticket, "operation.cancel", {"operation_id": run.operation_id})
    assert cancel.output["cancelled"] is False


def test_output_is_truncated_with_a_digest(tool_env):
    store = tool_env["store"]
    authority = tool_env["authority"]
    run = tool_env["run"]
    # a workspace that yields far more matches than any bound allows
    handle = tool_env["handle"]
    body = "\n".join(f"needle_{index} = {index}" for index in range(2000))
    (handle.root / "src" / "big.py").write_text(body, encoding="utf-8")
    limited = ToolService(
        store, authority, workspace_root=tool_env["manager"].managed_root,
        max_result_bytes=512,
    )
    ticket, _ = authority.issue(
        project_id=tool_env["project"].id,
        job_id=tool_env["job_id"],
        node_id="n1",
        run_id=run["run_id"],
        lease_id=run["lease_id"],
        role="worker",
        actions=["repo.search"],
    )
    result = limited.invoke(ticket, "repo.search", {"pattern": "needle_", "path": "src"})
    assert result.ok is True
    # whichever bound stopped the search, the caller is told and can narrow it
    assert result.truncated is True
    assert result.output_bytes <= 512
    assert result.output_digest
    stopped = result.output.get("truncated_by") or "output_budget"
    assert stopped in {"match_count", "output_bytes", "output_budget"}
    assert not result.output.get("truncated", False) or "preview_text" in result.output


def test_a_search_that_stops_early_never_silently_loses_that_fact(tool_env):
    service = tool_env["service"]
    result = service.invoke(
        tool_env["ticket"], "repo.search", {"pattern": "def ", "path": ".", "max_matches": 1}
    )
    assert result.ok is True
    assert result.output["truncated"] is True
    assert result.output["truncated_by"] == "match_count"


def test_a_compact_result_is_never_marked_truncated(tool_env):
    result = tool_env["service"].invoke(
        tool_env["ticket"], "repo.search", {"pattern": "def ", "path": "."}
    )
    assert result.ok is True
    assert result.truncated is False
    assert result.output_bytes <= tool_env["service"].max_result_bytes


def test_product_bookkeeping_never_appears_as_a_worker_change(tool_env):
    """The pytest config and log directories are harness files, not deliverables."""
    service = tool_env["service"]
    ticket = tool_env["ticket"]
    # running a profile creates the owned pytest config and a log directory
    run = service.invoke(ticket, "test.run_profile", {"profile_id": "unit"})
    assert run.ok is True
    assert (tool_env["handle"].root / ".kvflow_pytest.ini").exists()

    status = service.invoke(ticket, "repo.status", {})
    paths = {c["relative_path"] for c in status.output["changed"]}
    assert ".kvflow_pytest.ini" not in paths
    assert not any(p.startswith(".kvflow_") for p in paths)
    assert not any(p.startswith("WORKSPACE.json") for p in paths)
    # and the listing does not advertise the harness files either
    listing = service.invoke(ticket, "repo.list", {"path": "."})
    assert listing.ok is True, listing.output
    assert listing.truncated is False
    names = {entry["name"] for entry in listing.output["entries"]}
    assert ".kvflow_pytest.ini" not in names
    assert "WORKSPACE.json" not in names
    assert "src" in names, "a real directory must still be listed"


def test_a_shadow_file_named_like_a_worker_output_is_still_a_change(tool_env):
    """Filtering must be exact: a real file under a normal name is reported."""
    service = tool_env["service"]
    ticket = tool_env["ticket"]
    service.invoke(
        ticket, "repo.patch", {"edits": [{"path": "src/real_work.py", "content": "x = 1\n"}]}
    )
    status = service.invoke(ticket, "repo.status", {})
    paths = {c["relative_path"] for c in status.output["changed"]}
    assert "src/real_work.py" in paths
    service = tool_env["service"]
    ticket = tool_env["ticket"]
    failed = service.invoke(ticket, "repo.read", {"path": "src/absent.py"})
    assert failed.ok is False
    assert failed.operation_id
    status = service.invoke(ticket, "operation.status", {"operation_id": failed.operation_id})
    assert status.output["status"] == "FAILED"
    assert store_pending(tool_env["store"], failed.operation_id) is False


def store_pending(store: Store, operation_id: str) -> bool:
    return any(
        row["operation_id"] == operation_id for row in store.pending_operations()
    )


# ---------------------------------------------------------------- research
# An isolated research entry point: a frozen spec, its own registered profile and
# an output scope. It is an engineering fixture, never real Alpha/Forward work.

EXPERIMENT = (
    "import json\n"
    "from pathlib import Path\n"
    "\n"
    "\n"
    "def _emit():\n"
    "    out = Path('out')\n"
    "    out.mkdir(parents=True, exist_ok=True)\n"
    "    (out / 'metrics.json').write_text(json.dumps({'ic': 0.01}), encoding='utf-8')\n"
    "\n"
    "\n"
    "def test_experiment_emits_metrics():\n"
    "    _emit()\n"
    "    assert Path('out/metrics.json').exists()\n"
)
EXPERIMENT_TWO_CANDIDATES = EXPERIMENT + (
    "\n"
    "\n"
    "def test_experiment_emits_a_second_candidate():\n"
    "    _emit()\n"
    "    assert Path('out/metrics.json').exists()\n"
)
EXPERIMENT_ESCAPES = (
    "from pathlib import Path\n"
    "\n"
    "\n"
    "def test_experiment_writes_into_the_source_tree():\n"
    "    Path('src').mkdir(parents=True, exist_ok=True)\n"
    "    (Path('src') / 'sneaky.py').write_text('x = 1\\n', encoding='utf-8')\n"
)
RESEARCH_ACTIONS = ["repo.read", "repo.list", "repo.status", "research.run_authorized"]


def make_research_env(store: Store, root: Path, *, experiment: str = EXPERIMENT,
                      research_authorized: bool = True, output_scope: str = "out",
                      maximum_candidates: int = 4, maximum_attempts: int = 2,
                      budget_seconds: int = 60, extra_actions=()):
    source = root / "source"
    (source / "src").mkdir(parents=True, exist_ok=True)
    (source / "experiments").mkdir(parents=True, exist_ok=True)
    (source / "data").mkdir(parents=True, exist_ok=True)
    (source / "golden_reference").mkdir(parents=True, exist_ok=True)
    (source / "src" / "calc.py").write_text(SOURCE_FILE, encoding="utf-8")
    (source / "data" / "frozen.csv").write_text("a,b\n1,2\n", encoding="utf-8")
    (source / "golden_reference" / "frozen.py").write_text("FROZEN", encoding="utf-8")
    (source / "experiments" / "test_metrics.py").write_text(experiment, encoding="utf-8")
    profile = TestProfile(
        id="research",
        project_id="research-project",
        description="isolated engineering research fixture",
        runner="pytest",
        targets=["experiments"],
        pythonpath=["src"],
        timeout_seconds=120,
        code_digest=hashlib.sha256(experiment.encode()).hexdigest(),
        authorization_digest=AUTH,
        research_authorized=research_authorized,
    )
    project = make_project(
        root,
        project_id="research-project",
        source_root=str(source),
        allowed_read_roots=["."],
        allowed_write_roots=["src", "out"],
        protected_roots=["golden_reference"],
        test_profiles=[profile],
        allowed_tools=RESEARCH_ACTIONS + list(extra_actions),
    )
    store.register_project(project)
    job_id = store.create_job(project.id, "run one frozen research spec", AUTH)
    plan = Plan.model_validate(
        {
            "id": "plan-research",
            "job_id": job_id,
            "version": 1,
            "revision": 1,
            "objective": "run one frozen isolated experiment",
            "deliverables": ["a recorded experiment outcome"],
            "constraints": ["writes stay inside the declared output scope"],
            "acceptance": ["the recorded outcome names its boundary"],
            "nodes": [
                NodeSpec(
                    id="n1",
                    lineage_key="lin-research",
                    objective="run the frozen experiment",
                    write_scopes=["out"],
                    test_profile="research",
                    allowed_tools=RESEARCH_ACTIONS + list(extra_actions),
                )
            ],
            "roles": [Role.WORKER, Role.MANAGER],
            "allowed_roots": ["out"],
            "tool_profiles": ["research"],
            "resource_budget": {
                "calls": 10,
                "input_tokens": 10000,
                "output_tokens": 10000,
                "tool_calls": 50,
                "storage_bytes": 1_000_000,
                "wall_seconds": 3600,
                "concurrency": 3,
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
    snapshot = manager.create_snapshot(
        include_scopes=["src", "experiments", "data", "golden_reference", "out"],
        notes="research fixture",
    )
    handle = manager.create_workspace(
        job_id=job_id, node_id="n1", snapshot_id=snapshot.snapshot_id,
        scopes=["src", "out", "data", "experiments"],
    )
    run = store.start_attempt(job_id, "n1")
    authority = CapabilityAuthority(store)
    ticket, _ = authority.issue(
        project_id=project.id, job_id=job_id, node_id="n1", run_id=run["run_id"],
        lease_id=run["lease_id"], role="worker", actions=RESEARCH_ACTIONS,
    )
    spec = ExperimentSpec.model_validate(
        {
            "id": "exp-spec-1",
            "project_id": project.id,
            "job_id": job_id,
            "node_id": "n1",
            "hypothesis_lineage_id": "lin-research",
            "authorization_digest": AUTH,
            "hypothesis": "an engineering fixture never validates Alpha or Forward",
            "profile_id": "research",
            "code_digest": profile.code_digest,
            "data_digest": workspace_scope_fingerprint(handle, "data"),
            "data_effective_time": datetime.now(timezone.utc),
            "parameters": {"lookback": {"minimum": 20, "maximum": 60}},
            "metrics": {"ic": {"minimum": -1.0, "maximum": 1.0}},
            "maximum_candidates": maximum_candidates,
            "maximum_attempts": maximum_attempts,
            "computational_budget_seconds": budget_seconds,
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
            "input_scope": "data",
            "output_scope": output_scope,
            "exposure_status": "ENGINEERING_FIXTURE",
        }
    )
    store.record_experiment(spec)
    service = ToolService(store, authority, workspace_root=manager.managed_root)
    return {
        "store": store,
        "project": project,
        "manager": manager,
        "handle": handle,
        "service": service,
        "ticket": ticket,
        "job_id": job_id,
        "source_root": source,
        "spec": spec,
    }


@pytest.fixture()
def research_env(tmp_path: Path, store: Store):
    return make_research_env(store, tmp_path / "research")


def test_the_research_entry_point_runs_a_frozen_spec(research_env):
    env = research_env
    result = env["service"].invoke(
        env["ticket"], "research.run_authorized", {"experiment_id": "exp-spec-1"}
    )
    assert result.ok is True
    output = result.output
    assert output["outcome"] == "COMPLETED"
    assert output["conclusion_boundary"] == "NO_ALPHA_OR_FORWARD_VALIDATION"
    assert output["candidate_count"] == 1
    assert output["attempts_used"] == 1 and output["attempts_allowed"] == 2
    assert output["output_scope"] == "out"
    assert output["receipt_id"]
    # the outcome is durable, and it is an isolated result by construction
    run = env["store"].experiment_runs("exp-spec-1")[0]
    assert run["outcome"] == "COMPLETED"
    assert env["store"].experiment_attempts("exp-spec-1") == 1
    produced = env["handle"].root / "out" / "metrics.json"
    assert produced.is_file()
    # the registered source project was never touched
    assert (env["source_root"] / "src" / "calc.py").read_text(encoding="utf-8") == SOURCE_FILE
    assert not (env["source_root"] / "out").exists()


def test_a_changed_frozen_input_is_refused(tmp_path, store):
    env = make_research_env(store, tmp_path / "research")
    (env["handle"].root / "data" / "frozen.csv").write_text("a,b\n9,9\n", encoding="utf-8")
    result = env["service"].invoke(
        env["ticket"], "research.run_authorized", {"experiment_id": "exp-spec-1"}
    )
    assert result.ok is False
    assert result.error_code == "CONTRACT_INVALID"
    assert env["store"].experiment_attempts("exp-spec-1") == 0


def test_an_ordinary_test_profile_is_not_a_research_entry_point(tmp_path, store):
    env = make_research_env(store, tmp_path / "research", research_authorized=False)
    result = env["service"].invoke(
        env["ticket"], "research.run_authorized", {"experiment_id": "exp-spec-1"}
    )
    assert result.ok is False
    assert result.error_code == "AUTHORIZATION_INVALID"
    assert env["store"].experiment_attempts("exp-spec-1") == 0


def test_an_output_scope_outside_the_node_write_scopes_is_refused(tmp_path, store):
    env = make_research_env(store, tmp_path / "research", output_scope="src")
    result = env["service"].invoke(
        env["ticket"], "research.run_authorized", {"experiment_id": "exp-spec-1"}
    )
    assert result.ok is False
    assert result.error_code == "PATH_DENIED"


def test_an_output_scope_over_a_protected_root_is_refused(tmp_path, store):
    env = make_research_env(store, tmp_path / "research")
    forged = ExperimentSpec.model_validate(
        {
            **env["spec"].model_dump(mode="json"),
            "id": "exp-spec-protected",
            "output_scope": "golden_reference",
        }
    )
    env["store"].record_experiment(forged)
    result = env["service"].invoke(
        env["ticket"], "research.run_authorized", {"experiment_id": "exp-spec-protected"}
    )
    assert result.ok is False
    assert result.error_code == "PATH_DENIED"


def test_an_experiment_cannot_exceed_its_frozen_attempts(tmp_path, store):
    env = make_research_env(store, tmp_path / "research", maximum_attempts=1)
    first = env["service"].invoke(
        env["ticket"], "research.run_authorized", {"experiment_id": "exp-spec-1"}
    )
    assert first.ok is True
    second = env["service"].invoke(
        env["ticket"], "research.run_authorized", {"experiment_id": "exp-spec-1"}
    )
    assert second.ok is False
    assert second.error_code == "ATTEMPT_EXHAUSTED"
    assert env["store"].experiment_attempts("exp-spec-1") == 1


def test_a_compute_budget_above_the_profile_timeout_is_refused(tmp_path, store):
    env = make_research_env(store, tmp_path / "research", budget_seconds=600)
    result = env["service"].invoke(
        env["ticket"], "research.run_authorized", {"experiment_id": "exp-spec-1"}
    )
    assert result.ok is False
    assert result.error_code == "CONTRACT_INVALID"


def test_more_candidates_than_the_spec_allows_is_refused(tmp_path, store):
    env = make_research_env(
        store, tmp_path / "research",
        experiment=EXPERIMENT_TWO_CANDIDATES, maximum_candidates=1,
    )
    result = env["service"].invoke(
        env["ticket"], "research.run_authorized", {"experiment_id": "exp-spec-1"}
    )
    assert result.ok is False
    assert result.error_code == "CONTRACT_INVALID"
    assert env["store"].experiment_attempts("exp-spec-1") == 0


def test_an_experiment_that_writes_outside_its_output_scope_is_refused(tmp_path, store):
    env = make_research_env(store, tmp_path / "research", experiment=EXPERIMENT_ESCAPES)
    result = env["service"].invoke(
        env["ticket"], "research.run_authorized", {"experiment_id": "exp-spec-1"}
    )
    assert result.ok is False
    assert result.error_code == "PATH_DENIED"
    # the escape is visible in the owned workspace and never reached the source
    assert (env["handle"].root / "src" / "sneaky.py").exists()
    assert not (env["source_root"] / "src" / "sneaky.py").exists()


def test_a_spec_bound_to_another_node_is_refused(tmp_path, store):
    env = make_research_env(store, tmp_path / "research")
    from kvflow.core.contracts import ExperimentSpec

    other = ExperimentSpec.model_validate(
        {**env["spec"].model_dump(mode="json"), "id": "exp-spec-other-node",
         "node_id": "n-elsewhere"}
    )
    with pytest.raises(Exception):
        env["store"].record_experiment(other)
    result = env["service"].invoke(
        env["ticket"], "research.run_authorized", {"experiment_id": "exp-spec-other-node"}
    )
    assert result.ok is False
    assert result.error_code == "NOT_FOUND"


def test_the_research_action_is_not_available_without_it_in_the_ticket(tmp_path, store):
    env = make_research_env(store, tmp_path / "research")
    other_ticket, _ = CapabilityAuthority(store).issue(
        project_id=env["project"].id,
        job_id=env["job_id"],
        node_id="n1",
        run_id=env["store"].start_attempt(env["job_id"], "n1")["run_id"],
        lease_id=env["store"].current_lease(env["job_id"], "n1")["lease_id"],
        role="worker",
        actions=["repo.read"],
    )
    result = env["service"].invoke(
        other_ticket, "research.run_authorized", {"experiment_id": "exp-spec-1"}
    )
    assert result.ok is False
    assert result.error_code == "CAPABILITY_DENIED"
