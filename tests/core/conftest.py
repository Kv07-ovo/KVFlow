"""Shared fixtures for v1 foundation boundary tests.

Every fixture builds a real on-disk project layout so the Windows path,
ancestry and reparse checks are exercised against the actual filesystem rather
than a string stub.
"""

from __future__ import annotations

import hashlib
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from kvflow.core.budget import BudgetLedger
from kvflow.core.contracts import (
    BudgetLimits,
    BudgetScope,
    NodeSpec,
    Plan,
    Project,
    Role,
    TestProfile,
)
from kvflow.core.security import CapabilityAuthority, PathPolicy
from kvflow.core.store import Store

AUTH = hashlib.sha256(b"test-authorization-digest").hexdigest()
CODE = hashlib.sha256(b"test-code-digest").hexdigest()


def digest_of(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def deadline_in(seconds: float) -> datetime:
    return datetime.now(timezone.utc) + timedelta(seconds=seconds)


def make_project(root: Path, project_id: str = "kvstock-test", **overrides) -> Project:
    source = root / "source"
    managed = root / "managed"
    (managed / "out").mkdir(parents=True, exist_ok=True)
    (managed / "golden_reference").mkdir(parents=True, exist_ok=True)
    (managed / "FORWARD_SIMULATION_V1").mkdir(parents=True, exist_ok=True)
    (managed / "other").mkdir(parents=True, exist_ok=True)
    (source / "src").mkdir(parents=True, exist_ok=True)
    profile = TestProfile(
        id="unit",
        project_id=project_id,
        description="focused unit tests",
        runner="pytest",
        targets=["tests"],
        code_digest=CODE,
        authorization_digest=AUTH,
    )
    payload = {
        "id": project_id,
        "display_name": "KVStock test project",
        "source_root": str(source),
        "managed_root": str(managed),
        "allowed_read_roots": ["."],
        "allowed_write_roots": ["out"],
        "protected_roots": ["golden_reference", "FORWARD_SIMULATION_V1"],
        "test_profiles": [profile],
        "allowed_tools": [
            "repo.read",
            "repo.search",
            "repo.list",
            "repo.status",
            "repo.diff",
            "repo.patch",
            "repo.commit",
            "test.run_profile",
            "operation.status",
            "operation.result",
            "operation.cancel",
            "knowledge.search",
            "knowledge.read",
            "knowledge.propose",
            "research.read_named",
            "research.read_qualification",
        ],
        "trusted": True,
        "authorization_digest": AUTH,
        "registered_at": datetime.now(timezone.utc),
    }
    payload.update(overrides)
    return Project.model_validate(payload)


def make_plan(
    project: Project,
    job_id: str,
    *,
    revision: int = 1,
    nodes: list[NodeSpec] | None = None,
    micro_free: bool = True,
) -> Plan:
    node_list = nodes or [
        NodeSpec(
            id="n1",
            lineage_key="lineage-n1",
            objective="implement the bounded module",
            write_scopes=["out/report.txt"],
            test_profile="unit",
            allowed_tools=["repo.read", "repo.patch", "test.run_profile"],
        )
    ]
    return Plan.model_validate(
        {
            "id": f"plan-{job_id}",
            "job_id": job_id,
            "version": 1,
            "revision": revision,
            "objective": "deliver one bounded module",
            "deliverables": ["module source", "executor test receipt"],
            "constraints": ["no writes outside the managed workspace"],
            "acceptance": ["pytest profile passes", "manager review approves the diff"],
            "nodes": node_list,
            "roles": [Role.WORKER, Role.MANAGER],
            "allowed_roots": ["out"],
            "tool_profiles": ["unit"],
            "resource_budget": {
                "calls": 10,
                "input_tokens": 100000,
                "output_tokens": 100000,
                "tool_calls": 40,
                "storage_bytes": 1048576,
                "wall_seconds": 3600,
                "concurrency": 3,
                "deadline": deadline_in(3600),
            },
            "approval_boundaries": ["no new paid budget"],
            "authorization_digest": AUTH,
            "created_at": datetime.now(timezone.utc),
        }
    )


@pytest.fixture()
def store(tmp_path: Path) -> Store:
    store = Store(tmp_path / "v1.sqlite3")
    store.initialize()
    return store


@pytest.fixture()
def project_root(tmp_path: Path) -> Path:
    root = tmp_path / "project"
    root.mkdir(parents=True, exist_ok=True)
    return root


@pytest.fixture()
def project(project_root: Path) -> Project:
    return make_project(project_root)


@pytest.fixture()
def registered(store: Store, project: Project) -> Project:
    store.register_project(project)
    return project


@pytest.fixture()
def policy(project: Project) -> PathPolicy:
    return PathPolicy(project)


@pytest.fixture()
def authority(store: Store) -> CapabilityAuthority:
    return CapabilityAuthority(store)


@pytest.fixture()
def ledger(store: Store) -> BudgetLedger:
    return BudgetLedger(store)


@pytest.fixture()
def scope_ids(store: Store, ledger: BudgetLedger, registered: Project):
    """global -> project -> job scope chain with a fenced run to bind against."""
    global_scope = BudgetScope(
        scope_id="scope-global",
        scope_kind="GLOBAL",
        calls=100,
        input_tokens=1_000_000,
        output_tokens=1_000_000,
        tool_calls=500,
        storage_bytes=10_000_000,
        wall_seconds=86400,
        micro_cny=100_000_000,
        concurrency=8,
        deadline=deadline_in(86400),
        authorization_digest=AUTH,
    )
    ledger.register_scope(global_scope)
    job_id = store.create_job(registered.id, "deliver a bounded module", AUTH)
    project_scope = BudgetScope(
        scope_id="scope-project",
        scope_kind="PROJECT",
        parent_scope_id="scope-global",
        project_id=registered.id,
        calls=50,
        input_tokens=500_000,
        output_tokens=500_000,
        tool_calls=200,
        storage_bytes=5_000_000,
        wall_seconds=43200,
        micro_cny=50_000_000,
        concurrency=6,
        deadline=deadline_in(43200),
        authorization_digest=AUTH,
    )
    job_scope = BudgetScope(
        scope_id="scope-job",
        scope_kind="JOB",
        parent_scope_id="scope-project",
        project_id=registered.id,
        job_id=job_id,
        calls=12,
        input_tokens=40_000,
        output_tokens=40_000,
        tool_calls=10,
        storage_bytes=1_000_000,
        wall_seconds=10800,
        micro_cny=2_000_000,
        concurrency=8,
        deadline=deadline_in(10800),
        authorization_digest=AUTH,
    )
    ledger.register_scope(project_scope)
    ledger.register_scope(job_scope)
    store.set_plan(make_plan(registered, job_id), expected_job_revision=1)
    store.transition_job(job_id, "NEW", "PLANNING", actor="manager")
    store.transition_job(job_id, "PLANNING", "ASSIGNED", actor="manager")
    return {
        "project_id": registered.id,
        "job_id": job_id,
        "global": "scope-global",
        "project": "scope-project",
        "job": "scope-job",
    }


def create_windows_junction(link: Path, target: Path) -> bool:
    """Create a real junction. Returns False when the platform refuses."""
    import subprocess

    link.parent.mkdir(parents=True, exist_ok=True)
    completed = subprocess.run(
        ["cmd", "/c", "mklink", "/J", str(link), str(target)],
        capture_output=True,
        text=True,
    )
    return completed.returncode == 0 and link.exists()


def is_reparse(path: Path) -> bool:
    try:
        stat = path.lstat()
    except OSError:
        return False
    return bool(getattr(stat, "st_file_attributes", 0) & 0x400)


@pytest.fixture()
def junction_target(tmp_path: Path) -> Path:
    outside = tmp_path / "outside"
    outside.mkdir(parents=True, exist_ok=True)
    (outside / "secret.txt").write_text("secret", encoding="utf-8")
    return outside


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


CODES = {
    "contract": "CONTRACT_INVALID",
    "state": "ILLEGAL_TRANSITION",
    "cas": "CAS_CONFLICT",
    "notfound": "NOT_FOUND",
    "path": "PATH_DENIED",
    "capability": "CAPABILITY_DENIED",
    "lease": "LEASE_DENIED",
    "budget": "BUDGET_DENIED",
    "capacity": "WAITING_CAPACITY",
    "attempts": "ATTEMPT_EXHAUSTED",
    "reservation": "RESERVATION_CONFLICT",
    "authorization": "AUTHORIZATION_INVALID",
}


def assert_code(exc_info, key: str) -> None:
    assert getattr(exc_info.value, "code", None) == CODES[key], (
        f"expected failure code {CODES[key]}, got {getattr(exc_info.value, 'code', None)}:"
        f" {exc_info.value}"
    )


def ignore_case_path(path: Path) -> str:
    value = str(path)
    return value[:1].swapcase() + value[1:]
