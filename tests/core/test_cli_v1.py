"""CLI tests: every documented command is exercised for real, in a subprocess."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = REPO_ROOT / "src"


def cli_env() -> dict[str, str]:
    env = dict(os.environ)
    parts = [str(SRC_DIR)] + [p for p in env.get("PYTHONPATH", "").split(os.pathsep) if p]
    env["PYTHONPATH"] = os.pathsep.join(parts)
    return env


def run(*args: str, home: Path) -> subprocess.CompletedProcess[str]:
    # --home is a top-level option, so it must precede the subcommand
    return subprocess.run(
        [sys.executable, "-m", "kvflow.cli", "--home", str(home), *args],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
        env=cli_env(),
        cwd=str(REPO_ROOT),
    )


def parse(proc: subprocess.CompletedProcess[str]) -> dict:
    assert proc.returncode == 0, proc.stderr
    return json.loads(proc.stdout)


@pytest.fixture()
def home(tmp_path: Path) -> Path:
    return tmp_path / "agent-os-home"


def project_document(tmp_path: Path, project_id: str = "cli-project") -> dict:
    source = tmp_path / "source"
    (source / "src").mkdir(parents=True, exist_ok=True)
    managed = tmp_path / "managed"
    managed.mkdir(parents=True, exist_ok=True)
    return {
        "id": project_id,
        "display_name": "CLI project",
        "source_root": str(source),
        "managed_root": str(managed),
        "allowed_read_roots": ["."],
        "allowed_write_roots": ["out"],
        "protected_roots": ["golden_reference"],
        "test_profiles": [],
        "allowed_tools": ["repo.read"],
        "trusted": True,
        "authorization_digest": "a" * 64,
        "registered_at": "2026-09-14T00:00:00+00:00",
    }


def test_version_and_doctor(home: Path):
    version = subprocess.run(
        [sys.executable, "-m", "kvflow.cli", "--version"],
        capture_output=True, text=True, check=False, env=cli_env(), cwd=str(REPO_ROOT),
    )
    assert version.returncode == 0
    assert "kvflow 0.1.0" in version.stdout

    report = parse(run("doctor", home=home))
    assert report["schema_version"] == 1
    assert report["checks"]["database"]["ok"] is True
    assert report["checks"]["processes"]["permanent_service_added"] is False
    assert report["checks"]["manager"]["provider_returned"] == "NOT_EXPOSED"
    assert report["checks"]["worker"]["provider_returned"] == "NOT_EXPOSED"
    assert report["ready"] is True


def test_project_add_and_list(home: Path, tmp_path: Path):
    document = tmp_path / "project.json"
    document.write_text(json.dumps(project_document(tmp_path)), encoding="utf-8")
    added = parse(run("project", "add", "--file", str(document), home=home))
    assert added["registered"] == "cli-project"
    listed = parse(run("project", "list", home=home))
    assert [p["id"] for p in listed["projects"]] == ["cli-project"]
    assert listed["projects"][0]["trusted"] is True


def test_a_malformed_project_is_a_typed_error(home: Path, tmp_path: Path):
    document = tmp_path / "bad.json"
    document.write_text(json.dumps({"id": "x"}), encoding="utf-8")
    proc = run("project", "add", "--file", str(document), home=home)
    assert proc.returncode == 1
    error = json.loads(proc.stderr)["error"]
    assert error["code"].startswith(("CONTRACT", "V1", "AUTHORIZATION"))


def test_workers_reports_roles_without_credentials(home: Path, monkeypatch):
    monkeypatch.setenv("DEEPSEEK_API_KEY", "FAKE_SENTINEL_VALUE")
    report = parse(run("workers", home=home))
    assert report["orchestrator"]["model"] == "deterministic-python"
    assert report["worker"]["display_name"] == "DeepSeek V4.1 Flash"
    assert report["manager"]["configured_model"]
    blob = json.dumps(report).lower()
    assert "sk-" not in blob and "fake_sentinel_value" not in blob
    assert report["credentials_exposed"] is False


def test_status_audit_and_mcp_status(home: Path):
    assert parse(run("status", home=home))["jobs"] == []
    audit = parse(run("audit", "--limit", "5", home=home))
    assert audit["events"] == [] and audit["operations"] == []
    assert audit["checkpoint"]["schema_version"] == 1
    mcp = parse(run("mcp", "status", home=home))
    assert mcp["tool_count"] >= 10
    assert "repo_read" in mcp["tools"]


def test_mcp_serve_describes_the_real_launch_contract(home: Path):
    serve = parse(run("mcp", "serve", home=home))
    assert serve["transport"] == "stdio"
    assert serve["stdout_is_protocol_only"] is True
    assert serve["environment"]["KVFLOW_MCP_CAPABILITY"] == "<opaque orchestrator ticket>"
    assert "never stored here" in serve["note"]


def test_pause_resume_cancel_on_a_real_job(home: Path, tmp_path: Path):
    """The lifecycle commands act on durable state created through the API."""
    from kvflow.core.store import Store

    document = tmp_path / "project.json"
    document.write_text(json.dumps(project_document(tmp_path)), encoding="utf-8")
    parse(run("project", "add", "--file", str(document), home=home))

    database = home / "kvflow.sqlite3"
    store = Store(database)
    store.initialize()
    job_id = store.create_job("cli-project", "pause me", "a" * 64)
    store.transition_job(job_id, "NEW", "PLANNING", actor="manager")
    store.transition_job(job_id, "PLANNING", "ASSIGNED", actor="manager")

    paused = parse(run("pause", job_id, home=home))
    assert paused["state"] == "PAUSED"
    resumed = parse(run("resume", job_id, home=home))
    assert resumed["state"] == "ASSIGNED"
    cancelled = parse(run("cancel", job_id, "--reason", "user stopped it", home=home))
    assert cancelled["state"] == "CANCELLED"
    assert cancelled["closed_runs"] == []

    status = parse(run("status", job_id, home=home))
    assert status["state"] == "CANCELLED"


def test_a_job_in_a_terminal_state_cannot_be_paused(home: Path):
    from kvflow.core.contracts import Project
    from kvflow.core.store import Store

    parse(run("doctor", home=home))
    store = Store(home / "kvflow.sqlite3")
    store.register_project(Project.model_validate(project_document(home)))
    job_id = store.create_job("cli-project", "still NEW", "a" * 64)
    proc = run("pause", job_id, home=home)
    assert proc.returncode == 1
    assert json.loads(proc.stderr)["error"]["code"] == "ILLEGAL_TRANSITION"


def test_a_paused_job_cannot_be_resumed_twice(home: Path):
    from kvflow.core.contracts import Project
    from kvflow.core.store import Store

    parse(run("doctor", home=home))  # creates the schema via the CLI itself
    store = Store(home / "kvflow.sqlite3")
    store.register_project(Project.model_validate(project_document(home)))
    job_id = store.create_job("cli-project", "lifecycle", "a" * 64)
    store.transition_job(job_id, "NEW", "PLANNING", actor="manager")
    store.transition_job(job_id, "PLANNING", "ASSIGNED", actor="manager")
    parse(run("pause", job_id, home=home))
    proc = run("resume", job_id, home=home)
    assert proc.returncode == 0
    again = run("resume", job_id, home=home)
    assert again.returncode == 1
    assert json.loads(again.stderr)["error"]["code"] == "ILLEGAL_TRANSITION"


def test_backup_verify_restore_round_trip(home: Path, tmp_path: Path):
    parse(run("doctor", home=home))
    created = parse(run("backup", "--note", "cli round trip", home=home))
    assert created["row_counts"]["jobs"] == 0
    verified = parse(run("backup-verify", created["root"], home=home))
    assert verified["verified"] is True

    destination = tmp_path / "restored"
    dry = parse(run("restore", created["root"], str(destination), "--dry-run", home=home))
    assert dry["ready"] is True and dry["dry_run"] is True
    assert not destination.exists()

    real = parse(run("restore", created["root"], str(destination), home=home))
    assert real["dry_run"] is False
    assert (destination / "kvflow.sqlite3").exists()


def test_restore_into_a_non_empty_directory_is_refused(home: Path, tmp_path: Path):
    parse(run("doctor", home=home))
    created = parse(run("backup", home=home))
    destination = tmp_path / "occupied"
    destination.mkdir()
    (destination / "already.txt").write_text("x", encoding="utf-8")
    proc = run("restore", created["root"], str(destination), home=home)
    assert proc.returncode == 1
    assert json.loads(proc.stderr)["error"]["code"] == "CONFLICT"


def test_process_registry_stops_only_its_own_processes(home: Path):
    """The registry stops exactly the owned process tree it recorded.

    A throwaway child is started for the test; the test runner's own pid is never
    registered, because that would make the command stop the test session itself.
    """
    import time

    child = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(120)"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        registered = parse(
            run("process", "register", "--kind", "ui", "--pid", str(child.pid), home=home)
        )
        assert registered["owned_only"] is True
        assert registered["registered"]["pid"] == child.pid
        stopped = parse(run("stop", home=home))
        assert stopped["killed_by_port"] is False
        assert "only processes this install registered" in stopped["note"]
        assert [entry["pid"] for entry in stopped["stopped"]] == [child.pid]
        deadline = time.monotonic() + 10
        while child.poll() is None and time.monotonic() < deadline:
            time.sleep(0.1)
        assert child.poll() is not None, "the registered child was not stopped"
        # the registry no longer claims to own a live process
        assert parse(run("stop", home=home))["stopped"] == []
    finally:
        if child.poll() is None:  # pragma: no cover - defensive cleanup
            child.kill()
        child.wait(timeout=10)


def test_ui_serve_starts_the_real_loopback_ui_and_stop_ends_it(home: Path, tmp_path: Path):
    """The documented way to start and stop the local UI, tested for real."""
    import socket
    import time
    import urllib.request

    parse(run("doctor", home=home))
    child = subprocess.Popen(
        [sys.executable, "-m", "kvflow.cli", "--home", str(home), "ui", "serve",
         "--port", "0"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        env=cli_env(),
        cwd=str(REPO_ROOT),
    )
    try:
        ready = json.loads(child.stdout.readline())
        assert ready["loopback_only"] is True
        assert ready["host"] == "127.0.0.1"
        assert ready["port"] > 0
        assert ready["session_token"]
        with urllib.request.urlopen(ready["url"], timeout=10) as response:
            body = response.read().decode("utf-8")
        assert response.status == 200
        assert "KVFlow" in body
        assert ready["session_token"] in body  # the page carries the session token
        # the same install that started it can stop exactly it. The pid the
        # registry holds is the one that reported itself, which on Windows may be
        # a child of the launcher process this test started.
        stopped = parse(run("stop", "--id", ready["registered_id"], home=home))
        assert [entry["pid"] for entry in stopped["stopped"]] == [ready["pid"]]
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            try:
                socket.create_connection(("127.0.0.1", ready["port"]), timeout=1).close()
            except OSError:
                break
            time.sleep(0.2)
        with pytest.raises(OSError):
            socket.create_connection(("127.0.0.1", ready["port"]), timeout=2)
    finally:
        if child.poll() is None:  # pragma: no cover - defensive cleanup
            child.kill()
        child.wait(timeout=10)


def test_knowledge_search_is_scoped(home: Path):
    from kvflow.core.contracts import Project
    from kvflow.core.knowledge import KnowledgeService
    from kvflow.core.store import Store

    parse(run("doctor", home=home))
    store = Store(home / "kvflow.sqlite3")
    store.register_project(Project.model_validate(project_document(home)))
    KnowledgeService(store).propose(
        project_id="cli-project",
        topic="status",
        content="the journal reports DATA_BLOCKED",
        author="manager",
        source_ref="journal",
        source_digest="b" * 64,
        authorization_digest="a" * 64,
    )
    found = parse(run("knowledge", "search", "--project", "cli-project", home=home))
    assert found["records"][0]["topic"] == "status"
    empty = parse(run("knowledge", "search", "--project", "other", home=home))
    assert empty["records"] == []
