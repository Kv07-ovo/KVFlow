"""The reviewer must receive the evidence the work actually produced.

The daily DSH read-only task was sent back with FIX for a precise reason: the packet
carried an empty diff and three receipts whose ``reported_tests`` and
``stdout_sha256`` were null, and it carried no worker result text at all - so a
read-only deliverable (the report) and a real test run could not be told apart from
repeated no-ops. These tests pin the packet contents.
"""

from __future__ import annotations

import json

from kvflow.core.manager import Manager


def _manager():
    captured: dict[str, str] = {}

    def caller(kind: str, prompt: str):
        captured["prompt"] = prompt
        return {"text": json.dumps({"verdict": "APPROVE", "findings": [],
                                    "rationale": "evidence complete"}),
                "model": "fixture:manager", "reports_identity": True}

    return Manager(caller=caller, max_calls=2, display_name="fixture"), captured


def _evidence(captured: dict[str, str]) -> dict:
    prompt = captured["prompt"]
    return json.loads(prompt.split("Evidence (JSON):", 1)[1])


def test_receipt_digests_and_test_counts_reach_the_reviewer():
    manager, captured = _manager()
    manager.review(
        objective="run the suite", acceptance=["the profile exits zero"],
        diff={"changed": [], "content_digest": "d" * 64},
        receipts=[{
            # a store row shape: stdout_digest, not stdout_sha256
            "receipt_id": "rcpt_1", "profile_id": "test", "exit_code": 0,
            "runner": "pytest", "reported_tests": 7, "stdout_digest": "a" * 64,
            "stderr_digest": "b" * 64, "stdout_bytes": 123, "duration_ms": 456,
        }],
        content_digest="d" * 64,
    )
    evidence = _evidence(captured)
    receipt = evidence["executor_test_receipts"][0]
    assert receipt["reported_tests"] == 7
    assert receipt["stdout_sha256"] == "a" * 64, "the stdout digest was dropped"
    assert receipt["stderr_sha256"] == "b" * 64
    assert receipt["duration_ms"] == 456


def test_the_worker_result_text_is_part_of_the_evidence():
    manager, captured = _manager()
    manager.review(
        objective="report the project state", acceptance=["a sourced report"],
        diff={"changed": [], "content_digest": "d" * 64},
        receipts=[{"receipt_id": "r", "profile_id": "test", "exit_code": 0,
                   "reported_tests": 2, "stdout_digest": "a" * 64}],
        content_digest="d" * 64,
        worker_reports=[{
            "node_id": "inspect_verify", "status": "WORKER_COMPLETE",
            "summary": "No changes. src/calc.py holds add(a, b); tests/test_calc.py "
                       "covers it; the registered profile test exits 0.",
            "steps": 6, "tool_calls": 12, "notes": ["read-only"],
        }],
    )
    evidence = _evidence(captured)
    report = evidence["worker_reports"][0]
    assert report["node_id"] == "inspect_verify"
    assert "No changes" in report["summary"]
    assert report["tool_calls"] == 12


def test_an_already_shaped_receipt_naming_stdout_sha256_is_accepted():
    manager, captured = _manager()
    manager.review(
        objective="x", acceptance=["y"], diff={"changed": [], "content_digest": "d" * 64},
        receipts=[{"receipt_id": "r", "profile_id": "test", "exit_code": 0,
                   "stdout_sha256": "c" * 64}],
        content_digest="d" * 64,
    )
    assert _evidence(captured)["executor_test_receipts"][0]["stdout_sha256"] == "c" * 64


def test_a_review_without_worker_reports_still_works():
    manager, captured = _manager()
    verdict = manager.review(
        objective="x", acceptance=["y"], diff={"changed": [], "content_digest": "d" * 64},
        receipts=[{"receipt_id": "r", "profile_id": "test", "exit_code": 0}],
        content_digest="d" * 64,
    )
    assert verdict.verdict == "APPROVE"
    assert _evidence(captured)["worker_reports"] == []
