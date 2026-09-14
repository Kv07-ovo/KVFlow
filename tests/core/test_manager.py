"""Manager tests: plan and verdict validation with a scripted caller.

No Codex or model call happens here. The tests assert the rules that bound the
Manager: scope containment, contract validation, evidence requirements for an
approval, and honest reporting of an unknown cost.
"""

from __future__ import annotations

import json

import pytest

from kvflow.core.errors import ContractError, ProviderError
from kvflow.core.manager import Manager, extract_json

from .conftest import AUTH, make_project

RESOURCE_BUDGET = {
    "calls": 20,
    "input_tokens": 100_000,
    "output_tokens": 100_000,
    "tool_calls": 100,
    "storage_bytes": 10_000_000,
    "wall_seconds": 3600,
    "concurrency": 3,
    "deadline": "2030-01-01T00:00:00+00:00",
}

GOOD_PLAN = {
    "objective": "add a multiply helper",
    "deliverables": ["a patched module"],
    "constraints": ["stay inside src"],
    "acceptance": ["the unit profile passes"],
    "nodes": [
        {
            "id": "n1",
            "lineage_key": "lin-n1",
            "dependencies": [],
            "objective": "patch and test",
            "write_scopes": ["out/report.txt"],
            "test_profile": "unit",
            "allowed_tools": ["repo.read", "repo.patch", "test.run_profile"],
        }
    ],
}


class ScriptedCaller:
    def __init__(self, replies):
        self.replies = list(replies)
        self.prompts: list[tuple[str, str]] = []

    def __call__(self, kind, prompt):
        self.prompts.append((kind, prompt))
        if not self.replies:
            raise AssertionError("the scripted caller ran out of replies")
        reply = self.replies.pop(0)
        if isinstance(reply, Exception):
            raise reply
        return {"text": reply, "model": "gpt-6-astra", "reports_identity": True}


def make_manager(replies, **overrides):
    return Manager(caller=ScriptedCaller(replies), **overrides)


# --------------------------------------------------------------- json parsing


def test_extract_json_accepts_a_fenced_object():
    assert extract_json('```json\n{"a": 1}\n```') == {"a": 1}


def test_extract_json_accepts_prose_around_the_object():
    assert extract_json('Here is the plan: {"a": 1} — done.') == {"a": 1}


@pytest.mark.parametrize("text", ["", "no json here", "{broken", None])
def test_extract_json_refuses_anything_else(text):
    with pytest.raises((ContractError, ProviderError)):
        extract_json(text)


# ------------------------------------------------------------------- planning


def test_a_valid_plan_is_accepted(tmp_path):
    project = make_project(tmp_path, project_id="mgr-project")
    manager = make_manager([json.dumps(GOOD_PLAN)])
    plan = manager.build_plan(
        job_id="job-1",
        project=project,
        objective="add a multiply helper",
        allowed_roots=["out"],
        resource_budget=RESOURCE_BUDGET,
        authorization_digest=AUTH,
    )
    assert [node.id for node in plan.nodes] == ["n1"]
    assert plan.objective == "add a multiply helper"
    assert plan.authorization_digest == AUTH
    assert plan.nodes[0].lineage_key == "lin-n1"


def test_a_plan_with_an_unregistered_profile_is_refused(tmp_path):
    project = make_project(tmp_path, project_id="mgr-project")
    bad = json.loads(json.dumps(GOOD_PLAN))
    bad["nodes"][0]["test_profile"] = "does-not-exist"
    manager = make_manager([json.dumps(bad)])
    with pytest.raises(ContractError):
        manager.build_plan(
            job_id="job-1", project=project, objective="x", allowed_roots=["out"],
            resource_budget=RESOURCE_BUDGET, authorization_digest=AUTH,
        )


def test_a_plan_whose_scope_escapes_the_allowed_roots_is_refused(tmp_path):
    project = make_project(tmp_path, project_id="mgr-project")
    bad = json.loads(json.dumps(GOOD_PLAN))
    bad["nodes"][0]["write_scopes"] = ["golden_reference/quant/file.py"]
    manager = make_manager([json.dumps(bad)])
    with pytest.raises(ContractError):
        manager.build_plan(
            job_id="job-1", project=project, objective="x", allowed_roots=["out"],
            resource_budget=RESOURCE_BUDGET, authorization_digest=AUTH,
        )


def test_a_plan_with_a_dependency_cycle_is_refused(tmp_path):
    project = make_project(tmp_path, project_id="mgr-project")
    cyclic = {
        "objective": "loop",
        "nodes": [
            {**GOOD_PLAN["nodes"][0], "id": "a", "dependencies": ["b"]},
            {**GOOD_PLAN["nodes"][0], "id": "b", "dependencies": ["a"]},
        ],
    }
    manager = make_manager([json.dumps(cyclic)])
    with pytest.raises(ContractError):
        manager.build_plan(
            job_id="job-1", project=project, objective="x", allowed_roots=["out"],
            resource_budget=RESOURCE_BUDGET, authorization_digest=AUTH,
        )


def test_a_plan_with_no_nodes_is_refused(tmp_path):
    project = make_project(tmp_path, project_id="mgr-project")
    manager = make_manager([json.dumps({"objective": "empty", "nodes": []})])
    with pytest.raises(ContractError):
        manager.build_plan(
            job_id="job-1", project=project, objective="x", allowed_roots=["out"],
            resource_budget=RESOURCE_BUDGET, authorization_digest=AUTH,
        )


def test_a_plan_that_cannot_produce_a_receipt_is_refused(tmp_path):
    """A two-node plan whose writers may never test is internally inconsistent."""
    project = make_project(tmp_path, project_id="mgr-project")
    untestable = {
        "objective": "write without any way to prove it",
        "deliverables": ["a patched module"],
        "constraints": ["stay inside out"],
        "acceptance": ["the unit profile passes"],
        "nodes": [
            {**GOOD_PLAN["nodes"][0], "id": "patch",
             "allowed_tools": ["repo.read", "repo.patch"]},
            {**GOOD_PLAN["nodes"][0], "id": "docs", "dependencies": ["patch"],
             "write_scopes": ["out/notes.md"],
             "allowed_tools": ["repo.read", "repo.list"]},
        ],
    }
    manager = make_manager([json.dumps(untestable)])
    with pytest.raises(ContractError) as excinfo:
        manager.build_plan(
            job_id="job-1", project=project, objective="x", allowed_roots=["out"],
            resource_budget=RESOURCE_BUDGET, authorization_digest=AUTH,
        )
    assert "never produce an executor test receipt" in str(excinfo.value)


def test_a_read_only_plan_needs_no_test_node(tmp_path):
    """A plan that changes nothing may legitimately contain no test run."""
    project = make_project(tmp_path, project_id="mgr-project")
    read_only = {
        "objective": "report the current state",
        "deliverables": ["a report"],
        "constraints": ["read only"],
        "acceptance": ["the report names its sources"],
        "nodes": [
            {**GOOD_PLAN["nodes"][0], "id": "survey", "write_scopes": [],
             "allowed_tools": ["repo.read", "repo.list", "repo.search"]},
        ],
    }
    manager = make_manager([json.dumps(read_only)])
    plan = manager.build_plan(
        job_id="job-1", project=project, objective="x", allowed_roots=["out"],
        resource_budget=RESOURCE_BUDGET, authorization_digest=AUTH,
    )
    assert [node.id for node in plan.nodes] == ["survey"]


def test_the_manager_call_budget_is_enforced(tmp_path):
    project = make_project(tmp_path, project_id="mgr-project")
    manager = make_manager([json.dumps(GOOD_PLAN)], max_calls=1)
    manager.build_plan(
        job_id="job-1", project=project, objective="x", allowed_roots=["out"],
        resource_budget=RESOURCE_BUDGET, authorization_digest=AUTH,
    )
    with pytest.raises(ProviderError) as excinfo:
        manager.review(
            objective="x", acceptance=["a"],
            diff={"changed": [], "content_digest": "d" * 64}, receipts=[],
            content_digest="d" * 64,
        )
    assert excinfo.value.kind == "manager_budget"


# --------------------------------------------------------------------- review


def receipt(exit_code: int = 0):
    return {
        "receipt_id": "rcpt_1",
        "profile_id": "unit",
        "exit_code": exit_code,
        "reported_tests": 3,
        "stdout_sha256": "a" * 64,
    }


def diff():
    return {
        "changed": [
            {"relative_path": "out/report.txt", "change": "ADDED", "sha256": "b" * 64}
        ],
        "content_digest": "c" * 64,
    }


def test_an_approval_with_a_passing_receipt_is_accepted():
    manager = make_manager(
        [json.dumps({"verdict": "APPROVE", "findings": ["the profile passed"],
                    "rationale": "receipt exits zero and the diff is in scope"})]
    )
    verdict = manager.review(
        objective="add a helper", acceptance=["the profile passes"],
        diff=diff(), receipts=[receipt()], content_digest="c" * 64,
    )
    assert verdict.verdict == "APPROVE"
    assert verdict.content_digest == "c" * 64
    assert verdict.findings == ["the profile passed"]


def test_an_approval_without_any_receipt_is_refused():
    manager = make_manager([json.dumps({"verdict": "APPROVE", "findings": [],
                                        "rationale": "looks fine"})])
    with pytest.raises(ContractError) as excinfo:
        manager.review(
            objective="x", acceptance=["a"], diff=diff(), receipts=[],
            content_digest="c" * 64,
        )
    assert "without any executor test receipt" in str(excinfo.value)


def test_an_approval_over_a_failing_receipt_is_refused():
    manager = make_manager([json.dumps({"verdict": "APPROVE", "findings": [],
                                        "rationale": "ok"})])
    with pytest.raises(ContractError) as excinfo:
        manager.review(
            objective="x", acceptance=["a"], diff=diff(), receipts=[receipt(exit_code=1)],
            content_digest="c" * 64,
        )
    assert "no test receipt exited zero" in str(excinfo.value)


def test_a_fix_verdict_needs_no_receipt():
    manager = make_manager([json.dumps({"verdict": "FIX", "findings": ["the test file"
                                                                        " was not updated"],
                                        "rationale": "incomplete"})])
    verdict = manager.review(
        objective="x", acceptance=["a"], diff=diff(), receipts=[],
        content_digest="c" * 64,
    )
    assert verdict.verdict == "FIX"


def test_an_unknown_verdict_is_refused():
    manager = make_manager([json.dumps({"verdict": "SHIP_IT", "findings": [],
                                        "rationale": "yolo"})])
    with pytest.raises(ContractError):
        manager.review(
            objective="x", acceptance=["a"], diff=diff(), receipts=[receipt()],
            content_digest="c" * 64,
        )


def test_the_review_prompt_carries_the_real_evidence():
    manager = make_manager([json.dumps({"verdict": "FIX", "findings": [],
                                        "rationale": "x"})])
    manager.review(
        objective="add a multiply helper", acceptance=["the profile passes"],
        diff=diff(), receipts=[receipt()], content_digest="c" * 64,
    )
    prompt = manager.caller.prompts[-1][1]
    assert "add a multiply helper" in prompt
    assert "the profile passes" in prompt
    assert "out/report.txt" in prompt
    assert "rcpt_1" in prompt
    assert "cc" + "c" * 62 in prompt


# ------------------------------------------------------------------- reporting


def test_identity_separates_configured_and_observed():
    manager = make_manager([json.dumps(GOOD_PLAN)])
    before = manager.identity()
    assert before["provider_returned_model"] == "NOT_EXPOSED"
    assert before["verification_level"] == "CONFIGURED_ONLY"


def test_cost_is_reported_as_unknown_not_zero():
    manager = make_manager([json.dumps(GOOD_PLAN)])
    report = manager.report()
    assert report["cost"]["status"] == "UNKNOWN"
    assert "$0" not in json.dumps(report)


def test_the_identity_is_promoted_only_after_a_provider_report(tmp_path):
    project = make_project(tmp_path, project_id="mgr-project")
    manager = make_manager([json.dumps(GOOD_PLAN)])
    manager.build_plan(
        job_id="job-1", project=project, objective="x", allowed_roots=["out"],
        resource_budget=RESOURCE_BUDGET, authorization_digest=AUTH,
    )
    identity = manager.identity()
    assert identity["provider_returned_model"] == "gpt-6-astra"
    assert identity["verification_level"] == "PROVIDER_MODEL"
