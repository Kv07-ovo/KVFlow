"""Drive one KVFlow workflow with a scripted (fixture) model and manager.

Nothing here calls a paid endpoint: the point is to prove the *runner* — durable
job, plan, budget chain, owned workspaces, capability tickets, the worker loop's
real tool calls, executor receipts, the manager review gate and the integration
tree — before spending money on the real model. The receipt marks the model as a
fixture so it can never be mistaken for live evidence.

    python tools/smoke_workflow.py [--template feature|bugfix|refactor|docs_or_data]
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PRODUCT = Path(r"C:\Users\90428\Desktop\KVStock-restored\kvflow")
sys.path.insert(0, str(PRODUCT / "src"))

from kvflow import registry, workflow  # noqa: E402
from kvflow.core.manager import Manager  # noqa: E402
from kvflow.core.providers import Completion, ProviderIdentity, ToolCall  # noqa: E402

SMOKE = PRODUCT / ".runtime" / "smoke"
HOME = SMOKE / "home"


def resolve_project_id() -> str:
    """The onboarding smoke test generates a stable-but-unique id; read it back."""
    entries = registry.Registry(HOME).list()
    for entry in entries:
        if Path(entry["canonical_root"]) == (SMOKE / "demo-python-api"):
            return entry["project_id"]
    raise SystemExit(
        "run tools/smoke_product.py first: the demo project is not registered"
    )


class ScriptedProvider:
    """A deterministic stand-in for the worker model: real tool calls, no network."""

    def __init__(self) -> None:
        self.model = "fixture:scripted-worker"
        self.calls = 0
        self.seen: list[list[dict]] = []

    def estimate_input_tokens(self, messages) -> int:
        return len(json.dumps(list(messages), ensure_ascii=False, default=str).encode("utf-8"))

    def identity(self) -> ProviderIdentity:
        return ProviderIdentity(
            display_name="Scripted fixture worker",
            requested_model=self.model,
            requested_effort="NOT_EXPOSED",
            verification_level="CONFIGURED_ONLY",
        )

    def enrich_identity(self, completion) -> ProviderIdentity:
        """The loop asks the provider to describe the response it just produced."""
        return ProviderIdentity(
            display_name="Scripted fixture worker",
            requested_model=self.model,
            requested_effort="NOT_EXPOSED",
            provider_returned_model=getattr(completion, "provider_model", self.model),
            verification_level="CONFIGURED_ONLY",
        )

    def complete(self, messages, *, tools=None, max_tokens=1024, temperature=0.0, **kwargs):
        self.calls += 1
        self.seen.append([dict(message) for message in messages])
        step = self.calls

        def tool_call(index: int, name: str, arguments: dict) -> ToolCall:
            return ToolCall(id=f"call-{step}-{index}", name=name, arguments=arguments,
                            raw_arguments=json.dumps(arguments))

        if step == 1:
            calls = [tool_call(1, "repo_read", {"path": "src/calc.py"})]
        elif step == 2:
            calls = [
                tool_call(
                    1, "repo_patch",
                    {"edits": [{
                        "path": "src/calc.py",
                        "content": (
                            "def add(a, b):\n    return a + b\n\n\n"
                            "def subtract(a, b):\n    return a - b\n"
                        ),
                    }]},
                )
            ]
        elif step == 3:
            calls = [tool_call(1, "test_run", {"profile_id": "test"})]
        else:
            return Completion(
                content=(
                    "Done. src/calc.py now defines subtract(a, b) below add, the registered"
                    " test profile was executed and its receipt is reported."
                ),
                tool_calls=[],
                finish_reason="stop",
                provider_model=self.model,
                usage={"prompt_tokens": 1200, "completion_tokens": 90, "total_tokens": 1290},
            )
        return Completion(
            content="", tool_calls=calls, finish_reason="tool_calls",
            provider_model=self.model,
            usage={"prompt_tokens": 900, "completion_tokens": 60, "total_tokens": 960},
        )


class ScriptedManager:
    """A manager whose answers are fixed JSON, standing in for the real adapter."""

    def __init__(self, config: registry.ProjectConfig, job_objective: str) -> None:
        self.config = config
        self.objective = job_objective

    def _manager(self) -> Manager:
        def caller(kind: str, prompt: str):
            if kind == "plan":
                payload = {
                    "objective": self.objective,
                    "deliverables": ["a patched module", "a passing profile receipt"],
                    "constraints": ["stay inside the declared write scopes"],
                    "acceptance": ["the registered profile exits zero"],
                    "nodes": [
                        {
                            "id": "implement",
                            "lineage_key": "lin-implement",
                            "objective": "add subtract(a, b) to src/calc.py and run the profile",
                            "write_scopes": ["src", "tests"],
                            "test_profile": "test",
                            "allowed_tools": [
                                "repo.read", "repo.list", "repo.search", "repo.status",
                                "repo.diff", "repo.patch", "test.run_profile",
                                "operation.status",
                            ],
                        }
                    ],
                }
            else:
                payload = {
                    "verdict": "APPROVE",
                    "findings": ["the module defines subtract(a, b)", "the profile receipt exits zero"],
                    "rationale": "the change matches the request and the executor receipt passes",
                }
            return {"text": json.dumps(payload), "model": "fixture:scripted-manager",
                    "reports_identity": True}

        return Manager(caller=caller, max_calls=4, display_name="Scripted fixture manager")

    def build_plan(self, **kwargs):
        return self._manager().build_plan(**kwargs)

    def review(self, **kwargs):
        return self._manager().review(**kwargs)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--template", default="feature")
    parser.add_argument("--fresh", action="store_true",
                        help="re-run the onboarding smoke first, so attempts start clean")
    args = parser.parse_args()

    if args.fresh:
        import smoke_product

        smoke_product.main()
    if not (SMOKE / "demo-python-api").is_dir():
        print("run tools/smoke_product.py first: no demo project")
        return 2
    project_id = resolve_project_id()
    config = registry.read_config(SMOKE / "demo-python-api")
    requirement = ("add a subtract(a, b) helper to src/calc.py and prove it with the"
                   " registered profile")
    provider = ScriptedProvider()
    run = workflow.run_requirement(
        requirement=requirement,
        project_id=project_id,
        home=HOME,
        template_id=args.template,
        max_steps=6,
        max_completion_tokens=512,
        provider_factory=lambda: provider,
        manager_factory=lambda: ScriptedManager(config, requirement),
    )
    summary = {
        key: run[key] for key in
        ("status", "problems", "job_id", "plan_source", "nodes", "waves", "node_states_final",
         "test_receipts", "review", "integration", "worker_totals", "source_not_modified",
         "budget")
        if key in run
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2, default=str)[:2600])
    print("MODEL: fixture (scripted) — this run is not live-model evidence")
    return 0 if run["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
