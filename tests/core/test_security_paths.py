"""Windows path containment, ancestry and capability isolation (E2E-F core)."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from kvflow.core.contracts import Project
from kvflow.core.errors import CapabilityDenied, LeaseDenied, PathDenied
from kvflow.core.security import CapabilityAuthority, PathPolicy
from kvflow.core.store import Store

from .conftest import (
    AUTH,
    assert_code,
    create_windows_junction,
    make_plan,
    make_project,
)


# ------------------------------------------------------------------ exact paths


def test_broad_write_root_still_rejects_protected_descendants(project, policy):
    """A broad ``write_roots=["."]`` must not open Golden/Forward."""
    broad = make_project(
        Path(project.managed_root).parent,
        project_id="broad",
        allowed_write_roots=["."],
    )
    broad_policy = PathPolicy(broad)
    with pytest.raises(PathDenied) as exc:
        broad_policy.resolve("golden_reference/quant/file.py", operation="write")
    assert_code(exc, "path")
    with pytest.raises(PathDenied):
        broad_policy.resolve("FORWARD_SIMULATION_V1/journal.sqlite3", operation="write")
    # a legitimately unprotected sibling is still allowed
    resolved = broad_policy.resolve("other/report.md", operation="write")
    assert resolved.absolute == Path(broad.managed_root) / "other" / "report.md"


def test_selected_subroot_keeps_its_prefix(project_root, policy):
    """The returned target must be the requested path, not a truncated sibling."""
    target = Path(policy.managed_root) / "out" / "report.txt"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("original", encoding="utf-8")
    sibling = Path(policy.managed_root) / "report.txt"
    sibling.write_text("untouched", encoding="utf-8")

    resolved = policy.resolve("out/report.txt", operation="write")
    assert resolved.absolute == target
    assert resolved.relative_to_root == "out"
    assert resolved.absolute.parent.name == "out"
    assert sibling.read_text(encoding="utf-8") == "untouched"


def test_nested_non_dot_write_root_is_not_rejected(project_root):
    nested = make_project(
        project_root,
        project_id="nested",
        allowed_write_roots=["out/reports"],
        protected_roots=["golden_reference"],
    )
    policy = PathPolicy(nested)
    (Path(nested.managed_root) / "out" / "reports").mkdir(parents=True, exist_ok=True)
    resolved = policy.resolve("out/reports/2026/summary.md", operation="write")
    assert resolved.absolute == Path(nested.managed_root) / "out" / "reports" / "2026" / "summary.md"
    with pytest.raises(PathDenied):
        policy.resolve("out/other/summary.md", operation="write")


@pytest.mark.parametrize(
    "scope",
    [
        "../outside.txt",
        "out/../../escape.txt",
        "C:/Windows/System32/drivers/etc/hosts",
        "//server/share/file.txt",
        "\\\\?\\C:\\Windows\\win.ini",
        "\\\\.\\PhysicalDrive0",
        "out/report.txt:secret",
        "out/aux.txt",
        "out/COM1.log",
        "out/trailing.",
        "out/trailing ",
        "out/bad<name>.txt",
        "out/bad|name.txt",
        "out/\u0000null",
        "/absolute/path.txt",
    ],
)
def test_illegal_windows_scopes_are_denied(policy, scope):
    with pytest.raises(PathDenied) as exc:
        policy.resolve(scope, operation="write")
    assert_code(exc, "path")


def test_case_and_separator_variants_are_handled(policy):
    assert policy.resolve("out/Report.TXT", operation="write").scope == "out/Report.TXT"
    assert policy.resolve("OUT\\report.txt", operation="write").scope == "OUT/report.txt"
    # a sibling whose name merely starts with the write root must not match
    with pytest.raises(PathDenied):
        policy.resolve("outbox/report.txt", operation="write")
    # case-insensitive protected comparison
    with pytest.raises(PathDenied):
        policy.resolve("GOLDEN_REFERENCE/quant/file.py", operation="write")


def test_windows_case_insensitive_root_is_recognised(project_root):
    """The registered root must match regardless of the case used to spell it."""
    project = make_project(project_root, project_id="cased")
    variant = str(project.managed_root)[:1].swapcase() + str(project.managed_root)[1:]
    cased = Project.model_validate(
        {**project.model_dump(mode="json"), "managed_root": variant}
    )
    policy = PathPolicy(cased)
    assert policy.resolve("out/x.txt", operation="write").scope == "out/x.txt"


# ------------------------------------------------------------------ reparse


def test_junction_inside_write_root_is_denied(project, junction_target):
    policy = PathPolicy(project)
    link = Path(project.managed_root) / "out" / "junction"
    created = create_windows_junction(link, junction_target)
    if not created:
        pytest.skip("NTFS junction creation unavailable: recorded NOT_TESTED, denial retained")
    with pytest.raises(PathDenied) as exc:
        policy.resolve("out/junction/secret.txt", operation="write")
    assert_code(exc, "path")


def test_junction_inside_write_root_cannot_be_created_at_all(project, junction_target):
    """Even a plain (non-traversing) write below a junction must be denied."""
    policy = PathPolicy(project)
    link = Path(project.managed_root) / "out" / "junction"
    if not create_windows_junction(link, junction_target):
        pytest.skip("NTFS junction creation unavailable: recorded NOT_TESTED, denial retained")
    with pytest.raises(PathDenied):
        policy.resolve("out/junction/new-file.txt", operation="write")


def test_junction_above_registered_root_is_denied(tmp_path, junction_target):
    """An ancestor link above the registered root must fail closed."""
    real = tmp_path / "real-root"
    real.mkdir(parents=True, exist_ok=True)
    hop = tmp_path / "hop"
    if not create_windows_junction(hop, junction_target):
        pytest.skip("NTFS junction creation unavailable: recorded NOT_TESTED, denial retained")
    project = make_project(real, project_id="ancestor")
    linked_managed = hop / "managed"
    (junction_target / "managed" / "out").mkdir(parents=True, exist_ok=True)
    spoofed = Project.model_validate(
        {**project.model_dump(mode="json"), "managed_root": str(linked_managed)}
    )
    with pytest.raises(PathDenied) as exc:
        PathPolicy(spoofed)
    assert_code(exc, "path")


def test_ordinary_sibling_without_links_is_allowed(project):
    policy = PathPolicy(project)
    assert policy.resolve("other/notes.md", operation="read").scope == "other/notes.md"


# --------------------------------------------------------------- capability


def test_capability_cannot_be_relabelled_to_another_project(store, registered, authority, scope_ids):
    second_root = Path(registered.managed_root).parent / "second"
    second = make_project(second_root, project_id="second-project")
    store.register_project(second)
    second_job = store.create_job(second.id, "other work", AUTH)
    store.set_plan(make_plan(second, second_job), expected_job_revision=1)
    store.transition_job(second_job, "NEW", "PLANNING", actor="manager")
    store.transition_job(second_job, "PLANNING", "ASSIGNED", actor="manager")
    run = store.start_attempt(second_job, "n1")
    with pytest.raises(CapabilityDenied) as exc:
        authority.issue(
            project_id=registered.id,
            job_id=second_job,
            node_id="n1",
            run_id=run["run_id"],
            lease_id=run["lease_id"],
            role="worker",
            actions=["repo.read"],
        )
    assert_code(exc, "capability")


def test_capability_rejects_unknown_and_mismatched_actions(store, registered, authority, scope_ids):
    run = store.start_attempt(scope_ids["job_id"], "n1")
    with pytest.raises(CapabilityDenied) as exc:
        authority.issue(
            project_id=registered.id,
            job_id=scope_ids["job_id"],
            node_id="n1",
            run_id=run["run_id"],
            lease_id=run["lease_id"],
            role="worker",
            actions=["repo.read", "research.run_authorized"],
        )
    assert_code(exc, "capability")

    token, capability = authority.issue(
        project_id=registered.id,
        job_id=scope_ids["job_id"],
        node_id="n1",
        run_id=run["run_id"],
        lease_id=run["lease_id"],
        role="worker",
        actions=["repo.read", "repo.patch"],
    )
    assert capability.may("repo.read")
    assert not capability.may("research.run_authorized")
    verified = authority.verify(token, "repo.read")
    assert verified.run_id == run["run_id"]
    with pytest.raises(CapabilityDenied) as exc:
        authority.verify(token, "research.run_authorized")
    assert_code(exc, "capability")
    with pytest.raises(CapabilityDenied):
        authority.verify(token, "repo.read", project_id="second-project")
    with pytest.raises(CapabilityDenied):
        authority.verify(token + "x", "repo.read")


def test_role_and_task_strings_are_not_authority(store, registered, authority, scope_ids):
    run = store.start_attempt(scope_ids["job_id"], "n1")
    with pytest.raises(CapabilityDenied):
        authority.issue(
            project_id=registered.id,
            job_id=scope_ids["job_id"],
            node_id="n1",
            run_id=run["run_id"],
            lease_id=run["lease_id"],
            role="orchestrator",
            actions=["repo.read"],
        )
    token, _ = authority.issue(
        project_id=registered.id,
        job_id=scope_ids["job_id"],
        node_id="n1",
        run_id=run["run_id"],
        lease_id=run["lease_id"],
        role="worker",
        actions=["repo.read"],
    )
    # a forged JSON claim of manager authority changes nothing
    verified = authority.verify(token, "repo.read")
    assert verified.role == "worker"
    assert verified.may("repo.patch") is False


def test_new_lease_epoch_invalidates_the_old_token(store, registered, authority, scope_ids):
    first = store.start_attempt(scope_ids["job_id"], "n1")
    token, _ = authority.issue(
        project_id=registered.id,
        job_id=scope_ids["job_id"],
        node_id="n1",
        run_id=first["run_id"],
        lease_id=first["lease_id"],
        role="worker",
        actions=["repo.read"],
    )
    assert authority.verify(token, "repo.read").fence == first["fence"]
    second = store.start_attempt(scope_ids["job_id"], "n1")
    assert second["fence"] == first["fence"] + 1
    with pytest.raises((LeaseDenied, CapabilityDenied)) as exc:
        authority.verify(token, "repo.read")
    assert exc.value.code in {"LEASE_DENIED", "CAPABILITY_DENIED"}


def test_expired_capability_and_lease_are_rejected(store, registered, authority, scope_ids):
    run = store.start_attempt(scope_ids["job_id"], "n1", lease_seconds=30)
    token, _ = authority.issue(
        project_id=registered.id,
        job_id=scope_ids["job_id"],
        node_id="n1",
        run_id=run["run_id"],
        lease_id=run["lease_id"],
        role="worker",
        actions=["repo.read"],
        ttl_seconds=10,
    )
    assert authority.verify(token, "repo.read")
    store.clock.advance(11)
    with pytest.raises(CapabilityDenied) as exc:
        authority.verify(token, "repo.read")
    assert_code(exc, "capability")


def test_paused_or_terminal_job_rejects_capability(store, registered, authority, scope_ids):
    run = store.start_attempt(scope_ids["job_id"], "n1")
    token, _ = authority.issue(
        project_id=registered.id,
        job_id=scope_ids["job_id"],
        node_id="n1",
        run_id=run["run_id"],
        lease_id=run["lease_id"],
        role="worker",
        actions=["repo.read"],
    )
    assert authority.verify(token, "repo.read")
    store.transition_job(scope_ids["job_id"], "ASSIGNED", "PAUSED", actor="user")
    with pytest.raises(CapabilityDenied) as exc:
        authority.verify(token, "repo.read")
    assert_code(exc, "capability")


def test_revoked_capability_is_rejected(store, registered, authority, scope_ids):
    run = store.start_attempt(scope_ids["job_id"], "n1")
    token, capability = authority.issue(
        project_id=registered.id,
        job_id=scope_ids["job_id"],
        node_id="n1",
        run_id=run["run_id"],
        lease_id=run["lease_id"],
        role="worker",
        actions=["repo.read"],
    )
    authority.revoke(capability.token_hash, reason="manager cancelled the node")
    with pytest.raises(CapabilityDenied) as exc:
        authority.verify(token, "repo.read")
    assert_code(exc, "capability")


def test_capability_requires_a_real_lease(store, registered, authority, scope_ids):
    run = store.start_attempt(scope_ids["job_id"], "n1")
    with pytest.raises(LeaseDenied) as exc:
        authority.issue(
            project_id=registered.id,
            job_id=scope_ids["job_id"],
            node_id="n1",
            run_id=run["run_id"],
            lease_id="lease_does_not_exist",
            role="worker",
            actions=["repo.read"],
        )
    assert_code(exc, "lease")


def test_expired_lease_is_not_current(store, registered, authority, scope_ids):
    run = store.start_attempt(scope_ids["job_id"], "n1", lease_seconds=5)
    token, _ = authority.issue(
        project_id=registered.id,
        job_id=scope_ids["job_id"],
        node_id="n1",
        run_id=run["run_id"],
        lease_id=run["lease_id"],
        role="worker",
        actions=["repo.read"],
        ttl_seconds=5,
    )
    store.clock.advance(6)
    assert store.expire_leases() == [run["lease_id"]]
    with pytest.raises((LeaseDenied, CapabilityDenied)):
        authority.verify(token, "repo.read")


def test_capability_bound_path_is_rechecked(store, registered, authority, scope_ids):
    run = store.start_attempt(scope_ids["job_id"], "n1")
    token, _ = authority.issue(
        project_id=registered.id,
        job_id=scope_ids["job_id"],
        node_id="n1",
        run_id=run["run_id"],
        lease_id=run["lease_id"],
        role="worker",
        actions=["repo.patch"],
    )
    assert authority.verify(token, "repo.patch", path="out/report.txt")
    with pytest.raises(PathDenied):
        authority.verify(token, "repo.patch", path="golden_reference/quant/file.py")
