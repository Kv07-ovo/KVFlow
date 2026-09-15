"""The semantic coordination layer, exercised as the product runs it.

Every test drives the durable store directly: contracts are appended and never
edited, canonical writes are owner + CAS only, an idempotency key applies an effect
once, a stale identity is refused, and the final gate outranks any manager verdict.
"""

from __future__ import annotations

import pytest

from kvflow import coordination as coord
from kvflow.core.errors import NotFoundError, V1Error
from kvflow.core.store import Store


@pytest.fixture()
def store(tmp_path):
    store = Store(tmp_path / "coordination.sqlite3")
    store.initialize()
    return store


@pytest.fixture()
def coordinator(store):
    return coord.SemanticCoordinator(store, project_id="proj-a", job_id="job-1")


def test_contract_versions_are_appended_and_never_edited(coordinator):
    first = coordinator.publish_contract(
        contract_id="USER_STATUS", scope="user", created_by="manager",
        document={"entities": {"User.status": {"enum": ["active", "disabled"]}}},
    )
    second = coordinator.publish_contract(
        contract_id="USER_STATUS", scope="user", created_by="manager",
        document={"entities": {"User.status": {"enum": ["active", "disabled", "locked"]}}},
        expected_version=1,
    )
    assert (first["version"], second["version"]) == (1, 2)
    assert coordinator.contract_versions("USER_STATUS") == [1, 2]
    assert coordinator.contract("USER_STATUS", 1)["document"]["entities"]["User.status"][
        "enum"] == ["active", "disabled"]
    assert coordinator.contract("USER_STATUS")["version"] == 2
    # the same content hashes the same way; different content never does
    assert first["content_hash"] != second["content_hash"]


def test_a_cas_contract_write_naming_a_stale_version_is_refused(coordinator):
    coordinator.publish_contract(contract_id="API", scope="api", created_by="manager",
                                 document={"v": 1})
    coordinator.publish_contract(contract_id="API", scope="api", created_by="manager",
                                 document={"v": 2}, expected_version=1)
    with pytest.raises(coord.VersionConflict) as excinfo:
        coordinator.publish_contract(contract_id="API", scope="api", created_by="manager",
                                     document={"v": 3}, expected_version=1)
    assert excinfo.value.code == "CONCURRENT_MODIFICATION"
    assert coordinator.contract("API")["version"] == 2


def test_only_the_canonical_owner_may_write_and_others_are_told_what_to_do(coordinator):
    coordinator.register_resource(resource_id="AUTH_API_SCHEMA", kind="API_CONTRACT",
                                 owner_id="api-owner", document={"fields": ["id"]})
    written = coordinator.write_canonical(
        resource_id="AUTH_API_SCHEMA", writer_id="api-owner",
        document={"fields": ["id", "email"]}, expected_revision=1,
    )
    assert written["revision"] == 2
    with pytest.raises(coord.WriteDenied) as excinfo:
        coordinator.write_canonical(resource_id="AUTH_API_SCHEMA", writer_id="other-worker",
                                    document={"fields": ["id"]})
    error = excinfo.value.to_dict()
    assert error["code"] == "WRITE_DENIED"
    assert error["next_action"] == "CONTRACT_CHANGE_REQUIRED"
    # the refused write changed nothing
    assert coordinator.resource("AUTH_API_SCHEMA")["revision"] == 2


def test_two_writers_at_the_same_revision_leave_exactly_one_winner(coordinator):
    coordinator.register_resource(resource_id="SHARED_ENUM", kind="SHARED_ENUM",
                                 owner_id="owner", document={"values": ["a"]})
    first = coordinator.write_canonical(resource_id="SHARED_ENUM", writer_id="owner",
                                        document={"values": ["a", "b"]},
                                        expected_revision=1)
    assert first["revision"] == 2
    with pytest.raises(coord.VersionConflict) as excinfo:
        coordinator.write_canonical(resource_id="SHARED_ENUM", writer_id="owner",
                                    document={"values": ["a", "c"]},
                                    expected_revision=1)
    assert excinfo.value.to_dict()["current_revision"] == 2
    assert coordinator.resource("SHARED_ENUM")["document"] == {"values": ["a", "b"]}


def test_write_denied_and_version_conflict_are_different_refusals(coordinator):
    coordinator.register_resource(resource_id="CFG", kind="CONFIG_SCHEMA", owner_id="owner",
                                 document={"k": 1})
    with pytest.raises(coordindrome := coord.WriteDenied):  # noqa: F841 - clarity
        coordinator.write_canonical(resource_id="CFG", writer_id="intruder",
                                    document={"k": 2})
    coordinator.write_canonical(resource_id="CFG", writer_id="owner", document={"k": 2},
                                expected_revision=1)
    with pytest.raises(coord.VersionConflict):
        coordinator.write_canonical(resource_id="CFG", writer_id="owner", document={"k": 3},
                                    expected_revision=1)
    with pytest.raises(coord.VersionConflict):
        coordinator.write_canonical(resource_id="CFG", writer_id="owner", document={"k": 3},
                                    expected_revision=2, expected_hash="0" * 64)


def test_a_contract_change_request_is_the_only_way_a_version_moves(coordinator):
    coordinator.publish_contract(contract_id="USER_STATUS", scope="user",
                                 created_by="manager", document={"enum": ["active"]})
    request = coordinator.request_contract_change(
        contract_id="USER_STATUS", base_version=1, task_id="frontend",
        problem="the UI needs a third state", proposed_change={"enum": ["active", "locked"]},
        reason="a locked account is not the same as a disabled one",
        affected_artifacts=["USER_STATUS_TS"], affected_tasks=["frontend", "backend"],
    )
    assert request["status"] == "PENDING"
    decided = coordinator.decide_change_request(
        request_id=request["request_id"], decision="APPROVE", manager_id="mgr",
        task_states={"frontend": "WORKING", "backend": "WORKER_COMPLETE"},
    )
    assert decided["version"] == 2
    assert decided["previous_version"] == 1
    actions = {item["task_id"]: item["action"] for item in decided["fallout"]}
    assert actions["frontend"] == "STALE_PENDING_REVIEW"
    assert actions["backend"] == "STALE_RESULT"
    assert set(decided["stale_tasks"]) == {"frontend", "backend"}
    # the old version is still readable: change is append-only
    assert coordinator.contract("USER_STATUS", 1)["document"] == {"enum": ["active"]}
    # and a decided request cannot be decided twice
    with pytest.raises(V1Error):
        coordinator.decide_change_request(request_id=request["request_id"],
                                          decision="APPROVE", manager_id="mgr")


def test_a_rejected_change_request_leaves_the_contract_alone(coordinator):
    coordinator.publish_contract(contract_id="API", scope="api", created_by="manager",
                                 document={"v": 1})
    request = coordinator.request_contract_change(
        contract_id="API", base_version=1, problem="needs a field",
        proposed_change={"v": 2}, reason="convenience",
    )
    decided = coordinator.decide_change_request(request_id=request["request_id"],
                                                decision="REJECT", manager_id="mgr")
    assert decided["status"] == "REJECTED"
    assert coordinator.contract("API")["version"] == 1
    assert coordinator.change_requests(status="REJECTED")[0]["request_id"] == \
        request["request_id"]


def test_the_decision_ledger_is_the_context_and_supersedes_cleanly(coordinator):
    first = coordinator.record_decision(scope="auth", summary="use opaque tokens",
                                        reason_summary="rotation is simpler",
                                        manager_id="mgr",
                                        affected_contracts=["AUTH_API_SCHEMA"])
    second = coordinator.record_decision(scope="auth", summary="use JWT",
                                         reason_summary="stateless verification",
                                         manager_id="mgr", supersedes=first["decision_id"])
    active = coordinator.active_decisions()
    assert len(active) == 1
    assert active[0]["summary"] == "use JWT"
    assert active[0]["version"] == 2
    assert second["supersedes"] == first["decision_id"]
    digest = coordinator.decision_set_hash()
    coordinator.record_decision(scope="auth", summary="use JWT with a key id",
                                reason_summary="rotation without downtime",
                                manager_id="mgr", supersedes=first["decision_id"])
    # a changed decision set changes the identity every worker is judged against
    assert coordinator.decision_set_hash() != digest
    coordinator.revoke_decision(decision_id=first["decision_id"])
    assert coordinator.active_decisions() == []


def test_artifacts_carry_versions_hashes_and_dependencies(coordinator):
    produced = coordinator.register_artifact(
        artifact_id="AUTH_API_SCHEMA", producer_task="api", content_hash="a" * 64,
        semantic_contract_version=1, requires=[("USER_STATUS_TS", 1)], version=1)
    assert produced["version"] == 1
    assert coordinator.missing_required_artifacts(needed={"AUTH_API_SCHEMA": 1,
                                                          "USER_STATUS_TS": 1}) == [
        {"artifact_id": "USER_STATUS_TS", "required": 1, "available": 0}]
    coordinator.register_artifact(artifact_id="USER_STATUS_TS", producer_task="api",
                                  content_hash="b" * 64, version=1)
    assert coordinator.missing_required_artifacts(needed={"USER_STATUS_TS": 1}) == []
    # a second version is additive
    coordinator.register_artifact(artifact_id="USER_STATUS_TS", producer_task="api",
                                  content_hash="c" * 64, requires=[("AUTH_API_SCHEMA", 1)])
    assert coordinator.missing_required_artifacts(needed={"USER_STATUS_TS": 2}) == []
    assert {row["artifact_id"]: row["version"] for row in coordinator.artifacts()} == {
        "AUTH_API_SCHEMA": 1, "USER_STATUS_TS": 2}


def test_an_idempotency_key_applies_an_effect_exactly_once(coordinator):
    first = coordinator.begin_operation(
        idempotency_key="patch-src-app", tool="repo_patch", effect_type="APPLY_PATCH",
        target="src/app.mjs", arguments={"edits": [{"path": "src/app.mjs"}]})
    assert first["status"] == "NEW"
    repeat = coordinator.begin_operation(
        idempotency_key="patch-src-app", tool="repo_patch", effect_type="APPLY_PATCH",
        target="src/app.mjs", arguments={"edits": [{"path": "src/app.mjs"}]})
    assert repeat["status"] == "IN_PROGRESS"       # started but not yet recorded as applied
    coordinator.complete_operation(idempotency_key="patch-src-app", status="APPLIED")
    duplicate = coordinator.begin_operation(
        idempotency_key="patch-src-app", tool="repo_patch", effect_type="APPLY_PATCH",
        target="src/app.mjs", arguments={"edits": [{"path": "src/app.mjs"}]})
    assert duplicate["status"] == "ALREADY_APPLIED"
    assert duplicate["duplicate"] is True


def test_the_same_key_with_different_content_is_a_conflict_not_a_second_effect(coordinator):
    coordinator.begin_operation(idempotency_key="publish", tool="integration",
                                effect_type="PUBLISH", target="integration-tree",
                                arguments={"digest": "one"})
    with pytest.raises(coord.IdempotencyConflict) as excinfo:
        coordinator.begin_operation(idempotency_key="publish", tool="integration",
                                    effect_type="PUBLISH", target="integration-tree",
                                    arguments={"digest": "two"})
    error = excinfo.value.to_dict()
    assert error["code"] == "IDEMPOTENCY_CONFLICT"
    assert error["recorded_effect"] != error["offered_effect"]


def test_an_unknown_outcome_is_never_retried_blindly(coordinator):
    coordinator.begin_operation(idempotency_key="external-publish", tool="fixture_service",
                                effect_type="EXTERNAL_WRITE", target="remote://object/1",
                                arguments={"body": "x"})
    coordinator.mark_outcome_unknown(idempotency_key="external-publish",
                                     detail="the response was lost after the request")
    with pytest.raises(coord.OutcomeUnknown) as excinfo:
        coordinator.begin_operation(idempotency_key="external-publish", tool="fixture_service",
                                    effect_type="EXTERNAL_WRITE", target="remote://object/1",
                                    arguments={"body": "x"})
    assert excinfo.value.to_dict()["next_action"] == "RECONCILE"
    reconciled = coordinator.reconcile_operation(idempotency_key="external-publish",
                                                applied=True, evidence="fixture: applied=1",
                                                reconciled_by="manager")
    assert reconciled["status"] == "RECONCILED"
    assert reconciled["retry_allowed"] is False
    after = coordinator.begin_operation(idempotency_key="external-publish",
                                        tool="fixture_service", effect_type="EXTERNAL_WRITE",
                                        target="remote://object/1", arguments={"body": "x"})
    assert after["status"] == "ALREADY_APPLIED"


def test_reconciling_a_not_applied_effect_allows_exactly_one_retry(coordinator):
    coordinator.begin_operation(idempotency_key="maybe", tool="fixture_service",
                                effect_type="EXTERNAL_WRITE", target="remote://2",
                                arguments={})
    coordinator.mark_outcome_unknown(idempotency_key="maybe", detail="timeout")
    reconciled = coordinator.reconcile_operation(idempotency_key="maybe", applied=False,
                                                evidence="fixture: applied=0",
                                                reconciled_by="manager")
    assert reconciled["status"] == "FAILED"
    assert reconciled["retry_allowed"] is True
    retry = coordinator.begin_operation(idempotency_key="maybe", tool="fixture_service",
                                        effect_type="EXTERNAL_WRITE", target="remote://2",
                                        arguments={})
    assert retry["status"] == "IN_PROGRESS"


def test_a_stale_identity_is_refused_at_submit_time(coordinator):
    coordinator.publish_contract(contract_id="USER_STATUS", scope="user",
                                 created_by="manager", document={"enum": ["active"]})
    coordinator.register_artifact(artifact_id="USER_STATUS_TS", producer_task="api",
                                  content_hash="a" * 64, version=1)
    frozen = coordinator.freeze_identity(node_id="frontend")
    assert coordinator.check_staleness(frozen)["verdict"] == "CURRENT"
    coordinator.publish_contract(contract_id="USER_STATUS", scope="user",
                                 created_by="manager", document={"enum": ["active", "locked"]},
                                 expected_version=1)
    check = coordinator.check_staleness(frozen)
    assert check["verdict"] == "STALE_RESULT"
    assert check["changes"][0]["kind"] == "contract_changed"
    submission = coordinator.record_submission(node_id="frontend", frozen=frozen)
    assert submission["stale"] is True
    assert [row["node_id"] for row in coordinator.stale_submissions()] == ["frontend"]


def test_a_decision_change_alone_makes_an_identity_stale(coordinator):
    coordinator.publish_contract(contract_id="API", scope="api", created_by="manager",
                                 document={"v": 1})
    frozen = coordinator.freeze_identity(node_id="worker")
    coordinator.record_decision(scope="api", summary="freeze v1", reason_summary="stable",
                                manager_id="mgr")
    check = coordinator.check_staleness(frozen)
    assert check["stale"] is True
    assert check["changes"][0]["kind"] == "decision_changed"


def test_a_lease_that_moved_makes_a_submission_stale(coordinator):
    frozen = coordinator.freeze_identity(node_id="worker",
                                         lease={"lease_id": "l1", "fence": 1})
    fresh = coordinator.record_submission(node_id="worker", frozen=frozen)
    assert fresh["verdict"] == "CURRENT"
    stale = coordinator.record_submission(node_id="worker", frozen=frozen, lease_ok=False)
    assert stale["verdict"] == "STALE_RESULT"
    assert stale["reasons"][0]["kind"] == "lease_stale"
    superseded = coordinator.record_submission(node_id="worker", frozen=frozen,
                                               plan_version=3, expected_plan_version=2)
    assert superseded["reasons"][0]["kind"] == "plan_superseded"


def test_requirement_traceability_catches_a_locally_passing_miss(coordinator):
    coordinator.record_requirements(
        original_request="add login, logout and a password reset",
        requirements=[
            {"req_id": "REQ-001", "normalized_requirement": "login works",
             "acceptance": ["a valid user can authenticate"], "criticality": "CRITICAL"},
            {"req_id": "REQ-002", "normalized_requirement": "logout works",
             "criticality": "CRITICAL"},
            {"req_id": "REQ-003", "normalized_requirement": "password reset works",
             "criticality": "CRITICAL"},
        ])
    coordinator.link_requirement(req_id="REQ-001", node_id="impl", role="IMPLEMENTS")
    coordinator.link_requirement(req_id="REQ-001", node_id="qa", role="VALIDATES")
    coordinator.link_requirement(req_id="REQ-002", node_id="impl", role="IMPLEMENTS")
    coordinator.link_requirement(req_id="REQ-002", node_id="qa", role="VALIDATES")
    coordinator.link_requirement(req_id="REQ-003", node_id="impl", role="IMPLEMENTS")
    report = coordinator.requirement_status(node_states={"impl": "WORKER_COMPLETE",
                                                         "qa": "WORKER_COMPLETE"})
    verdicts = {item["req_id"]: item["verdict"] for item in report["requirements"]}
    assert verdicts == {"REQ-001": "PASS", "REQ-002": "PASS", "REQ-003": "UNVALIDATED"}
    assert report["all_pass"] is False
    assert report["critical_failed"] == []          # nothing literally failed
    assert report["critical_unresolved"] == ["REQ-003"]   # but it still blocks DONE
    # even a clean implementation cannot pass a requirement nobody validated
    assert report["original_request_hash"]


def test_a_requirement_with_no_successful_implementer_fails(coordinator):
    coordinator.record_requirements(original_request="do the thing",
                                    requirements=[{"req_id": "REQ-001",
                                                   "normalized_requirement": "the thing",
                                                   "criticality": "CRITICAL"}])
    coordinator.link_requirement(req_id="REQ-001", node_id="impl", role="IMPLEMENTS")
    report = coordinator.requirement_status(node_states={"impl": "FIX"})
    assert report["critical_failed"] == ["REQ-001"]
    assert report["requirements"][0]["verdict"] == "FAIL"


def test_critical_invariants_must_be_testable_and_pass(coordinator):
    coordinator.record_invariants([
        {"invariant_id": "INV-001",
         "description": "frontend and backend share one User.status enum",
         "severity": "CRITICAL", "validation_method": "schema_compare",
         "required_evidence": ["both modules import the frozen enum artifact"]},
        {"invariant_id": "INV-002", "description": "password hashes never leave the API",
         "severity": "CRITICAL", "validation_method": "response_scan"},
        {"invariant_id": "INV-003", "description": "logs are tidy", "severity": "MINOR",
         "validation_method": "inspection"},
    ])
    coordinator.validate_invariant(invariant_id="INV-001", result="PASS",
                                   validated_by="qa")
    coordinator.validate_invariant(invariant_id="INV-002", result="NOT_TESTABLE",
                                   validated_by="qa")
    coordinator.validate_invariant(invariant_id="INV-003", result="NOT_TESTABLE",
                                   validated_by="qa")
    report = coordinator.invariant_status()
    assert [item["invariant_id"] for item in report["blocking"]] == ["INV-002"]
    assert report["all_pass"] is False
    coordinator.validate_invariant(invariant_id="INV-002", result="FAIL",
                                   validated_by="qa", evidence="hash appeared in /me")
    assert coordinator.invariant_status()["blocking"][0]["result"] == "FAIL"


def test_independent_validation_is_recorded_with_its_path(coordinator):
    coordinator.record_validation(subject_node="impl", validator_node="qa",
                                  path_kind="INDEPENDENT_EXECUTION_PATH",
                                  independent=True, result="MISMATCH",
                                  evidence="qa recomputed 41, impl reported 42")
    report = coordinator.validation_status()
    assert report["all_pass"] is False
    assert report["mismatches"][0]["validator_node"] == "qa"
    coordinator.record_validation(subject_node="impl", validator_node="impl",
                                  path_kind="SELF_CHECK", independent=False,
                                  result="MATCH")
    assert coordinator.validation_status()["non_independent"]


def test_the_final_gate_fails_closed_and_lists_every_check(coordinator):
    coordinator.publish_contract(contract_id="USER_STATUS", scope="user",
                                 created_by="manager", document={"enum": ["active"]})
    coordinator.record_invariants([{"invariant_id": "INV-001", "description": "one enum",
                                    "severity": "CRITICAL",
                                    "validation_method": "schema_compare"}])
    coordinator.validate_invariant(invariant_id="INV-001", result="FAIL",
                                   validated_by="qa")
    gate = coordinator.final_gate(
        node_states={"frontend": "WORKER_COMPLETE", "backend": "WORKING"},
        integration={"textual": "PASS", "semantic": "FAIL", "behavioral": "FAIL"},
        required_artifacts={"AUTH_API_SCHEMA": 1},
    )
    assert gate["passed"] is False
    failed = set(gate["failed"])
    assert {"ALL_REQUIRED_TASKS_TERMINAL", "SEMANTIC_INTEGRATION",
            "BEHAVIORAL_INTEGRATION", "GLOBAL_INVARIANTS",
            "NO_MISSING_REQUIRED_ARTIFACT"} <= failed
    assert "approval cannot turn" in gate["note"]


def test_the_final_gate_says_not_applicable_instead_of_pretending(coordinator):
    gate = coordinator.final_gate(node_states={})
    assert gate["passed"] is True
    statuses = {item["check"]: item["status"] for item in gate["checks"]}
    assert statuses["ALL_REQUIRED_TASKS_TERMINAL"] == "NOT_APPLICABLE"
    assert statuses["NO_UNRESOLVED_CONTRACT_CHANGE"] == "NOT_APPLICABLE"
    assert statuses["GLOBAL_INVARIANTS"] == "NOT_APPLICABLE"
    assert statuses["USER_INTENT_VALIDATION"] == "NOT_APPLICABLE"


def test_unknown_side_effects_block_the_gate(coordinator):
    coordinator.publish_contract(contract_id="API", scope="api", created_by="manager",
                                 document={"v": 1})
    coordinator.begin_operation(idempotency_key="external", tool="fixture",
                               effect_type="EXTERNAL_WRITE", target="remote://1",
                               arguments={})
    coordinator.mark_outcome_unknown(idempotency_key="external", detail="lost response")
    gate = coordinator.final_gate(node_states={"impl": "WORKER_COMPLETE"})
    assert "NO_UNKNOWN_SIDE_EFFECTS" in gate["failed"]


def test_contracts_do_not_leak_between_projects(store):
    first = coord.SemanticCoordinator(store, project_id="proj-a", job_id="job-1")
    second = coord.SemanticCoordinator(store, project_id="proj-b", job_id="job-2")
    first.publish_contract(contract_id="USER_STATUS", scope="user", created_by="manager",
                           document={"enum": ["active", "disabled"]})
    first.record_decision(scope="user", summary="a-only", reason_summary="a",
                          manager_id="mgr")
    first.register_artifact(artifact_id="SHARED_NAME", producer_task="a",
                            content_hash="a" * 64)
    second.publish_contract(contract_id="USER_STATUS", scope="user", created_by="manager",
                            document={"enum": ["enabled", "disabled"]})
    second.register_artifact(artifact_id="SHARED_NAME", producer_task="b",
                             content_hash="b" * 64)
    assert first.contract("USER_STATUS")["version"] == 1
    assert second.contract("USER_STATUS")["document"] == {"enum": ["enabled", "disabled"]}
    assert [row["summary"] for row in second.active_decisions()] == []
    assert second.artifacts()[0]["content_hash"] == "b" * 64
    with pytest.raises(NotFoundError):
        second.contract("NOT_THERE")


def test_operation_keys_are_scoped_per_project(store):
    first = coord.SemanticCoordinator(store, project_id="proj-a")
    second = coord.SemanticCoordinator(store, project_id="proj-b")
    first.begin_operation(idempotency_key="same-key", tool="t", effect_type="E",
                          target="x", arguments={"a": 1})
    other = second.begin_operation(idempotency_key="same-key", tool="t", effect_type="E",
                                   target="x", arguments={"a": 1})
    assert other["status"] == "NEW"


def test_state_survives_a_reopened_store(tmp_path):
    path = tmp_path / "durable.sqlite3"
    store = Store(path)
    store.initialize()
    coordinator = coord.SemanticCoordinator(store, project_id="proj-a", job_id="job-1")
    coordinator.publish_contract(contract_id="API", scope="api", created_by="manager",
                                 document={"v": 1})
    coordinator.record_decision(scope="api", summary="keep it", reason_summary="simple",
                                manager_id="mgr")
    coordinator.begin_operation(idempotency_key="op-1", tool="t", effect_type="E",
                               target="x", arguments={})
    coordinator.complete_operation(idempotency_key="op-1", status="APPLIED")
    coordinator.record_requirements(original_request="req",
                                    requirements=[{"req_id": "REQ-001",
                                                   "normalized_requirement": "r",
                                                   "criticality": "CRITICAL"}])
    reopened = coord.SemanticCoordinator(Store(path), project_id="proj-a", job_id="job-1")
    assert reopened.contract("API")["version"] == 1
    assert [row["summary"] for row in reopened.active_decisions()] == ["keep it"]
    assert reopened.operations()[0]["status"] == "APPLIED"
    duplicate = reopened.begin_operation(idempotency_key="op-1", tool="t", effect_type="E",
                                         target="x", arguments={})
    assert duplicate["status"] == "ALREADY_APPLIED"
    assert reopened.requirement_status(node_states={})["requirements"][0]["req_id"] == \
        "REQ-001"


def test_propagation_classifies_every_task_state():
    classified = coord.propagate_contract_change(
        None, node_states={"a": "NEW", "b": "WORKING", "c": "WORKER_COMPLETE",
                           "d": "INTEGRATED", "e": "BLOCKED"})
    assert classified["dependency_updates"] == ["a"]
    assert classified["stale_pending_review"] == ["b"]
    assert classified["stale_results"] == ["c"]
    assert classified["change_impact_review"] == ["d"]
    assert classified["classified"]["e"] == "REVIEW_REQUIRED"
