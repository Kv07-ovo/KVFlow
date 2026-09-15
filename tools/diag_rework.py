"""Diagnostic: why does the rework fixture's profile run exit non-zero?

Not a product entry point. It rebuilds the ``test_rework_loop`` fixture, then
prints every tool result the WorkerLoop fed back to the provider, so the executor
stdout/stderr behind a non-zero receipt is visible.
"""

from __future__ import annotations

import hashlib
import json
import sys
import tempfile
import traceback
from datetime import datetime, timezone
from pathlib import Path

from kvflow import registry, workflow
from kvflow.core import cli as core_cli
from kvflow.core.contracts import NodeSpec, Plan, Role
from kvflow.core.manager import Manager
from kvflow.core.providers import Completion, ProviderIdentity, ToolCall
from kvflow.core.store import Store

AUTH = hashlib.sha256(b"diag-rework-authorization").hexdigest()
PY_SOURCE = "def add(a, b):\n    return a + b\n"
PY_TEST = "from calc import add\n\n\ndef test_add():\n    assert add(1, 2) == 3\n"


class _Provider:
    def __init__(self) -> None:
        self.model = "fixture:rework"
        self.calls = 0
        self.step = 0
        self.attempt = 0
        self.transcript: list[list[dict]] = []

    def estimate_input_tokens(self, messages) -> int:
        return len(json.dumps(list(messages), default=str).encode("utf-8"))

    def identity(self) -> ProviderIdentity:
        return ProviderIdentity(display_name="fixture", requested_model=self.model,
                                requested_effort="NOT_EXPOSED")

    def enrich_identity(self, completion) -> ProviderIdentity:
        return self.identity()

    def complete(self, messages, *, tools=None, max_tokens=512, **kwargs):
        self.calls += 1
        self.transcript.append([dict(message) for message in messages])
        step, self.step = self.step, (self.step + 1) % 3

        def call(index: int, name: str, arguments: dict) -> ToolCall:
            return ToolCall(id=f"c{self.calls}-{index}", name=name, arguments=arguments,
                            raw_arguments=json.dumps(arguments))

        usage = {"prompt_tokens": 400, "completion_tokens": 40, "total_tokens": 440}
        if step == 0:
            self.attempt += 1
            body = PY_SOURCE + f"\n\ndef touch_{self.attempt}(a):\n    return a\n"
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


def build(tmp_path: Path):
    store = Store(core_cli.database_path(tmp_path))
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
    workflow._write_manifest(
        tmp_path, job_id,
        {"job_id": job_id, "project_id": project.id, "display_name": project.display_name,
         "requirement": "add a helper", "template": "feature", "model_profile": "deepseek_only",
         "budget_profile": "standard", "authorization_digest": AUTH, "plan_id": plan.id,
         "plan_digest": "d" * 64, "plan_source": "deterministic_template",
         "acceptance": ["the unit profile exits zero"], "nodes": ["impl"],
         "waves": [["impl"]], "max_parallel_workers": 1, "problems": []},
    )
    return job_id


def main() -> int:
    root = Path(tempfile.mkdtemp(prefix="kvflow-diag-rework-"))
    print(f"home={root}")
    job_id = build(root)
    provider = _Provider()
    receipt = workflow.execute_run(
        job_id=job_id, home=root, max_steps=4, max_completion_tokens=256,
        provider_factory=lambda: provider,
        manager_factory=lambda: _manager(["FIX", "APPROVE"]),
    )
    print("rounds:", json.dumps(receipt["rounds"], indent=2))
    print("review:", json.dumps(receipt.get("review"), indent=2))
    print("problems:", receipt.get("problems"))
    print("test_receipts:", json.dumps(receipt.get("test_receipts"), indent=2)[:2000])
    print("node_reports:", json.dumps(receipt["node_reports"], indent=2)[:3000])
    for index, messages in enumerate(provider.transcript):
        for message in messages:
            if message.get("role") == "tool":
                print(f"--- call {index} tool result ---")
                print(str(message.get("content"))[:4000])
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:  # noqa: BLE001 - diagnostic
        traceback.print_exc()
        sys.exit(2)
