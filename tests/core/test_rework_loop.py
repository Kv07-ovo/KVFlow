"""A manager-directed rework retries the node and carries the findings back.

The core's worker budget is "first attempt plus at most two reworks", counted
durably per lineage. The workflow runner has to honour the same contract: when the
manager answers FIX, the node is re-dispatched with the findings in its objective,
and the loop is bounded so a run can never rework forever.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

from kvflow import registry, workflow
from kvflow.core.contracts import NodeSpec, Plan, Role, TestProfile
from kvflow.core.errors import ProviderError
from kvflow.core.manager import Manager
from kvflow.core.providers import Completion, ProviderIdentity, ToolCall
from kvflow.core.store import Store

from .conftest import AUTH, make_project

PY_SOURCE = "def add(a, b):\n    return a + b\n"
PY_TEST = "from calc import add\n\n\ndef test_add():\n    assert add(1, 2) == 3\n"


class _Provider:
    """One patch + one profile run, then a summary; real tool calls, no network."""

    def __init__(self, *, fail_summary_attempts: set[int] | None = None) -> None:
        self.model = "fixture:rework"
        self.calls = 0
        self.step = 0
        self.attempt = 0
        self.objectives: list[str] = []
        # attempts whose closing summary call dies on the wire, which is how a real
        # provider timeout leaves a node OUTCOME_UNKNOWN instead of complete
        self.fail_summary_attempts = set(fail_summary_attempts or ())

    def estimate_input_tokens(self, messages) -> int:
        return len(json.dumps(list(messages), default=str).encode("utf-8"))

    def identity(self) -> ProviderIdentity:
        return ProviderIdentity(display_name="fixture", requested_model=self.model,
                                requested_effort="NOT_EXPOSED")

    def enrich_identity(self, completion) -> ProviderIdentity:
        return self.identity()

    def complete(self, messages, *, tools=None, max_tokens=512, **kwargs):
        self.calls += 1
        self.objectives.append(str(messages[-1].get("content", ""))[:4000])
        # a fresh patch -> profile -> summary cycle for every attempt, so a rework
        # behaves like a real attempt instead of depending on a global call count
        step, self.step = self.step, (self.step + 1) % 3

        def call(index: int, name: str, arguments: dict) -> ToolCall:
            return ToolCall(id=f"c{self.calls}-{index}", name=name, arguments=arguments,
                            raw_arguments=json.dumps(arguments))

        usage = {"prompt_tokens": 400, "completion_tokens": 40, "total_tokens": 440}
        if step == 0:
            self.attempt += 1
            body = PY_SOURCE + f"\n\ndef touch_{self.attempt}(a):\n    return a\n"
            # the patched test imports the helper it exercises, so a correct patch
            # makes the profile exit zero and a broken one does not
            patched_test = (
                f"from calc import add, touch_{self.attempt}\n\n"
                "def test_add():\n    assert add(1, 2) == 3\n\n"
                f"def test_touch_{self.attempt}():\n"
                f"    assert touch_{self.attempt}(1) == 1\n"
            )
            calls = [call(1, "repo_patch", {"edits": [
                {"path": "src/calc.py", "content": body},
                {"path": "tests/test_calc.py", "content": patched_test},
            ]})]
        elif step == 1:
            calls = [call(1, "test_run", {"profile_id": "unit"})]
        else:
            if self.attempt in self.fail_summary_attempts:
                raise ProviderError("the fixture summary call timed out",
                                    outcome="OUTCOME_UNKNOWN")
            return Completion(content="Done: the helper exists and the profile exited zero.",
                              tool_calls=[], finish_reason="stop", provider_model=self.model,
                              usage=usage)
        return Completion(content="", tool_calls=calls, finish_reason="tool_calls",
                          provider_model=self.model, usage=usage)


def _manager(verdicts: list[str]) -> Manager:
    replies = list(verdicts)

    def caller(kind: str, prompt: str):
        if kind != "review":
            return {"text": "{}", "model": "fixture:manager", "reports_identity": True}
        verdict = replies.pop(0) if replies else "APPROVE"
        payload = {"verdict": verdict,
                   "findings": ["the profile receipt was not reported"] if verdict == "FIX" else [],
                   "rationale": f"fixture verdict {verdict}"}
        return {"text": json.dumps(payload), "model": "fixture:manager",
                "reports_identity": True}

    return Manager(caller=caller, max_calls=8, display_name="fixture manager")


def _build_job(tmp_path):
    """The product's own store, project and prepared job for one rework run.

    The workflow runner opens the runtime database, so this builds that exact
    store rather than a private fixture one, and resolves the project the way the
    product does: through the registry and an approved project-local config.
    """
    store = Store(tmp_path / "agent_os.sqlite3")
    store.initialize()
    source = tmp_path / "rework-source"
    (source / "src").mkdir(parents=True, exist_ok=True)
    (source / "tests").mkdir(parents=True, exist_ok=True)
    (source / "src" / "calc.py").write_text(PY_SOURCE, encoding="utf-8")
    (source / "tests" / "test_calc.py").write_text(PY_TEST, encoding="utf-8")
    config = registry.draft(
        source, display_name="rework project", template="feature",
        project_id="rework-project", write_roots=["src", "tests"],
    ).model_copy(update={"profiles": [registry.ProfileSpec(
        id="unit", description="pytest over the project tests", runner="pytest",
        targets=["tests"], pythonpath=["src"], timeout_seconds=300,
    )]})
    registry.write_config(config, approve=True)
    registry.Registry(tmp_path).register(registry.read_config(source), store=store)
    project = store.project("rework-project")
    job_id = store.create_job(project.id, "rework me", AUTH)
    plan = Plan.model_validate(
        {
            "id": "plan-rework", "job_id": job_id, "version": 1, "revision": 1,
            "objective": "add a helper", "deliverables": ["a helper"],
            "constraints": ["stay inside src and tests"],
            "acceptance": ["the unit profile exits zero"],
            "nodes": [NodeSpec(id="impl", lineage_key="lin-rework-impl",
                               objective="add a helper and run the unit profile",
                               write_scopes=["src", "tests"], test_profile="unit",
                               allowed_tools=["repo.read", "repo.list", "repo.diff",
                                              "repo.patch", "test.run_profile",
                                              "operation.status"])],
            "roles": [Role.WORKER, Role.MANAGER],
            "allowed_roots": ["src", "tests"], "tool_profiles": ["unit"],
            "resource_budget": {"calls": 30, "input_tokens": 200_000, "output_tokens": 90_000,
                                "tool_calls": 120, "storage_bytes": 4 << 20,
                                "wall_seconds": 3600, "concurrency": 2,
                                "deadline": datetime.now(timezone.utc).replace(year=2030)},
            "approval_boundaries": ["no push"], "authorization_digest": AUTH,
            "created_at": datetime.now(timezone.utc),
        }
    )
    store.set_plan(plan, expected_job_revision=1)
    store.transition_job(job_id, "NEW", "PLANNING", actor="manager")
    store.transition_job(job_id, "PLANNING", "ASSIGNED", actor="manager")
    workflow._write_manifest(  # noqa: SLF001 - the runner reads its own manifest
        tmp_path, job_id,
        {"job_id": job_id, "project_id": project.id, "display_name": project.display_name,
         "requirement": "add a helper", "template": "feature", "model_profile": "deepseek_only",
         "budget_profile": "standard", "authorization_digest": AUTH, "plan_id": plan.id,
         "plan_digest": "d" * 64, "plan_source": "deterministic_template",
         "acceptance": ["the unit profile exits zero"], "nodes": ["impl"],
         "waves": [["impl"]], "max_parallel_workers": 1, "problems": []},
    )
    return job_id


def test_a_fix_verdict_re_dispatches_the_node_with_the_findings(tmp_path):
    job_id = _build_job(tmp_path)
    provider = _Provider()
    receipt = workflow.execute_run(
        job_id=job_id, home=tmp_path, max_steps=4, max_completion_tokens=256,
        provider_factory=lambda: provider,
        manager_factory=lambda: _manager(["FIX", "APPROVE"]),
    )
    assert [entry["round"] for entry in receipt["rounds"]] == [0, 1], (
        f"rounds={receipt['rounds']} review={receipt.get('review')} "
        f"problems={receipt.get('problems')} receipts={receipt.get('test_receipts')} "
        f"nodes={ {k: {f: v.get(f) for f in ('status', 'error', 'notes', 'steps')} for k, v in receipt['node_reports'].items()} }"
    )
    assert receipt["review"]["verdict"] == "APPROVE"
    assert receipt["status"] == "PASS", receipt["problems"]
    # the rework carried the manager's finding into the objective the worker saw
    assert any("sent back for rework" in objective for objective in provider.objectives)
    assert any("profile receipt was not reported" in objective
               for objective in provider.objectives)
    assert receipt["test_receipts"], "the rework produced no executor receipt"
    assert receipt["integration"]["applied"], "approved bytes were not integrated"


def test_an_approved_run_still_settles_a_node_left_without_an_outcome(tmp_path):
    """An approval is not a settled node: the node is finished before integration.

    The live ``docs_project`` leg reproduced this: the last provider call timed
    out, so the node kept the durable ``FIX`` state the scheduler calls claimable
    while the manager approved the bytes on disk. Integrating from that state
    leaves the run PARTIAL forever, so the runner gives the node its bounded
    rework rounds and reviews the settled result before it integrates anything.
    """
    job_id = _build_job(tmp_path)
    provider = _Provider(fail_summary_attempts={1})
    receipt = workflow.execute_run(
        job_id=job_id, home=tmp_path, max_steps=4, max_completion_tokens=256,
        provider_factory=lambda: provider,
        manager_factory=lambda: _manager(["APPROVE", "APPROVE"]),
    )
    assert [entry["round"] for entry in receipt["rounds"]] == [0, 1], (
        f"rounds={receipt['rounds']} review={receipt.get('review')} "
        f"problems={receipt.get('problems')} "
        f"nodes={ {k: v.get('status') for k, v in receipt['node_reports'].items()} }"
    )
    assert receipt["rounds"][0]["states"] == {"impl": "FIX"}, (
        "the first attempt was expected to leave the node claimable"
    )
    assert receipt["review"]["verdict"] == "APPROVE"
    assert receipt["node_states_final"] == {"impl": "WORKER_COMPLETE"}, (
        receipt["node_states_final"]
    )
    assert receipt["status"] == "PASS", receipt["problems"]
    assert receipt["integration"]["applied"], "approved bytes were not integrated"
    # the settling attempt told the worker what was wrong instead of repeating it
    assert any("no settled outcome" in objective for objective in provider.objectives)
