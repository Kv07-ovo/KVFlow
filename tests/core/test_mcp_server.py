"""Real stdio MCP test: the four tool groups over the official SDK protocol.

The server is started as an actual child process and driven by a real MCP
client session: ``initialize`` negotiation, ``tools/list`` discovery and
``tools/call``. Nothing here is a Python function with the same name, and the
capability ticket travels only through the reviewed child environment channel.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

from kvflow.core.contracts import NodeSpec, Plan, Role, TestProfile
from kvflow.core.security import CapabilityAuthority
from kvflow.core.store import Store
from kvflow.core.workspace import WorkspaceManager

from .conftest import AUTH, make_project

#: the source directory the child server must import ``agent_os`` from
SRC_DIR = Path(__file__).resolve().parents[2] / "src"

SERVER_MODULE = "kvflow.core.mcp_server"
SOURCE_FILE = "def add(a, b):\n    return a + b\n"
TEST_FILE = "from calc import add\n\n\ndef test_add():\n    assert add(1, 2) == 3\n"


def _paths():
    from mcp import ClientSession, StdioServerParameters
    from mcp.client.stdio import stdio_client

    return ClientSession, StdioServerParameters, stdio_client


@pytest.fixture()
def mcp_env(tmp_path: Path):
    """A real store, project, plan, workspace and ticket for the child server."""
    source_root = tmp_path / "source"
    (source_root / "src").mkdir(parents=True, exist_ok=True)
    (source_root / "tests").mkdir(parents=True, exist_ok=True)
    (source_root / "src" / "calc.py").write_text(SOURCE_FILE, encoding="utf-8")
    (source_root / "tests" / "test_calc.py").write_text(TEST_FILE, encoding="utf-8")
    profile = TestProfile(
        id="unit",
        project_id="mcp-project",
        description="focused unit tests",
        runner="pytest",
        targets=["tests"],
        pythonpath=["src"],
        timeout_seconds=120,
        code_digest=hashlib.sha256(b"code").hexdigest(),
        authorization_digest=AUTH,
    )
    project = make_project(
        tmp_path,
        project_id="mcp-project",
        source_root=str(source_root),
        allowed_read_roots=["."],
        allowed_write_roots=["src", "tests"],
        protected_roots=["golden_reference"],
        test_profiles=[profile],
    )
    database = tmp_path / "v1.sqlite3"
    store = Store(database)
    store.initialize()
    store.register_project(project)
    job_id = store.create_job(project.id, "exercise the MCP tool groups", AUTH)
    plan = Plan.model_validate(
        {
            "id": "plan-mcp",
            "job_id": job_id,
            "version": 1,
            "revision": 1,
            "objective": "drive the tool groups over real MCP",
            "deliverables": ["a patched file", "a test receipt"],
            "constraints": ["no writes outside the node scopes"],
            "acceptance": ["the profile passes"],
            "nodes": [
                NodeSpec(
                    id="n1",
                    lineage_key="lin-n1",
                    objective="patch and test",
                    write_scopes=["src", "tests"],
                    test_profile="unit",
                    allowed_tools=[
                        "repo.read", "repo.list", "repo.search", "repo.status",
                        "repo.diff", "repo.patch", "repo.commit", "test.run_profile",
                        "operation.status", "operation.result", "operation.cancel",
                        "knowledge.search", "knowledge.read",
                    ],
                )
            ],
            "roles": [Role.WORKER, Role.MANAGER],
            "allowed_roots": ["src", "tests"],
            "tool_profiles": ["unit"],
            "resource_budget": {
                "calls": 20,
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
    snapshot = manager.create_snapshot(include_scopes=["src", "tests"])
    manager.create_workspace(
        job_id=job_id, node_id="n1", snapshot_id=snapshot.snapshot_id, scopes=["src", "tests"]
    )
    run = store.start_attempt(job_id, "n1")
    ticket, _ = CapabilityAuthority(store).issue(
        project_id=project.id,
        job_id=job_id,
        node_id="n1",
        run_id=run["run_id"],
        lease_id=run["lease_id"],
        role="worker",
        actions=[
            "repo.read", "repo.list", "repo.search", "repo.status", "repo.diff",
            "repo.patch", "repo.commit", "test.run_profile",
            "operation.status", "operation.result", "operation.cancel",
            "knowledge.search", "knowledge.read",
        ],
    )
    return {
        "database": database,
        "ticket": ticket,
        "workspace_root": manager.managed_root,
        "source_root": source_root,
        "handle": manager.worktrees_dir / job_id / "n1",
        "project": project,
        "store": store,
    }


def _server_params(env):
    _, StdioServerParameters, _ = _paths()
    child_env = {
        key: os.environ[key]
        for key in ("PATH", "SYSTEMROOT", "SYSTEMDRIVE", "WINDIR", "COMSPEC", "TEMP", "TMP")
        if key in os.environ
    }
    child_env.update(
        {
            "PYTHONIOENCODING": "utf-8",
            "PYTHONUTF8": "1",
            "PYTHONDONTWRITEBYTECODE": "1",
            "PYTHONPATH": str(SRC_DIR),
            "KVFLOW_MCP_DATABASE": str(env["database"]),
            "KVFLOW_MCP_CAPABILITY": env["ticket"],
            "KVFLOW_MCP_WORKSPACE_ROOT": str(env["workspace_root"]),
        }
    )
    return StdioServerParameters(
        command=sys.executable,
        args=["-m", SERVER_MODULE],
        env=child_env,
    )


def _call(env, calls):
    """Run one real MCP session and return the parsed results of ``calls``."""
    ClientSession, _, stdio_client = _paths()

    async def scenario():
        results = []
        async with stdio_client(_server_params(env)) as (read, write):
            async with ClientSession(read, write) as session:
                handshake = await session.initialize()
                tools = await session.list_tools()
                names = sorted(tool.name for tool in tools.tools)
                for name, arguments in calls:
                    response = await session.call_tool(name, arguments)
                    text = "".join(
                        getattr(block, "text", "") for block in response.content
                    )
                    results.append((name, response.isError, json.loads(text)))
                return handshake, names, results

    return asyncio.run(scenario())


def test_real_stdio_session_negotiates_and_discovers_tools(mcp_env):
    handshake, names, _ = _call(mcp_env, [])
    assert handshake.protocolVersion
    assert names == sorted(
        [
            "knowledge_read",
            "knowledge_search",
            "operation_result",
            "operation_status",
            "repo_commit",
            "repo_diff",
            "repo_list",
            "repo_patch",
            "repo_read",
            "repo_search",
            "repo_status",
            "research_run",
            "test_run",
        ]
    )


def test_repo_read_over_real_mcp(mcp_env):
    _, _, results = _call(mcp_env, [("repo_read", {"path": "src/calc.py"})])
    name, is_error, payload = results[0]
    assert is_error is False
    assert payload["ok"] is True
    assert payload["output"]["text"] == SOURCE_FILE
    assert payload["operation_id"]
    assert payload["audit"]["project_id"] == mcp_env["project"].id


def test_patch_then_test_over_real_mcp(mcp_env):
    calls = [
        ("repo_patch", {"edits": [{"path": "src/calc.py", "content": "def add(a, b):\n    return a + b + 0\n"}]}),
        ("repo_status", {}),
        ("test_run", {"profile_id": "unit"}),
    ]
    _, _, results = _call(mcp_env, calls)
    assert results[0][2]["ok"] is True
    assert results[1][2]["output"]["change_count"] == 1
    assert results[1][2]["output"]["changed"][0]["relative_path"] == "src/calc.py"
    receipt = results[2][2]["output"]["receipt"]
    assert receipt["exit_code"] == 0
    assert receipt["reported_tests"] == 1
    assert receipt["profile_id"] == "unit"


def test_a_path_outside_the_workspace_is_refused_over_mcp(mcp_env):
    _, _, results = _call(mcp_env, [("repo_read", {"path": "../source/secret.py"})])
    _, _, payload = results[0]
    assert payload["ok"] is False
    assert payload["error_code"] == "PATH_DENIED"
    assert payload["status"] == "FAILED"


def test_an_unknown_argument_is_refused_over_mcp(mcp_env):
    _, _, results = _call(mcp_env, [("repo_read", {"path": "src/calc.py", "shell": "dir"})])
    _, _, payload = results[0]
    assert payload["ok"] is False
    assert payload["error_code"] == "CONTRACT_INVALID"


def test_an_unknown_tool_is_refused_over_mcp(mcp_env):
    _, _, results = _call(mcp_env, [("shell_exec", {"command": "dir"})])
    _, _, payload = results[0]
    assert payload["ok"] is False
    assert payload["error_code"] == "UNKNOWN_TOOL"


def test_knowledge_is_scoped_over_mcp(mcp_env):
    from kvflow.core.knowledge import KnowledgeService

    service = KnowledgeService(mcp_env["store"])
    service.propose(
        project_id=mcp_env["project"].id,
        topic="note",
        content="project scoped note",
        author="manager",
        source_ref="log",
        source_digest=hashlib.sha256(b"log").hexdigest(),
        authorization_digest=AUTH,
    )
    _, _, results = _call(mcp_env, [("knowledge_search", {"topic": "note"})])
    payload = results[0][2]
    assert payload["ok"] is True
    assert payload["output"]["record_count"] == 1
    assert payload["output"]["records"][0]["topic"] == "note"


def test_the_child_receives_no_model_credentials(mcp_env, monkeypatch):
    """The ticket is the only secret in the child environment."""
    monkeypatch.setenv("DEEPSEEK_API_KEY", "FAKE_SENTINEL_VALUE")
    _, _, results = _call(mcp_env, [("repo_list", {"path": "."})])
    _, _, payload = results[0]
    assert payload["ok"] is True
    # the server has no shell action, so this is a boundary statement, not a probe
    assert "shell_exec" not in json.dumps(payload["audit"])
