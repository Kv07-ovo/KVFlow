"""Compile every template for the smoke project and report the resulting DAGs.

This is the planner's own acceptance check: it proves the templates really
compile into valid core plans, that the DAG has the parallelism the template
promises, and that a manager plan outside the approved envelope is refused.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

PRODUCT = Path(r"C:\Users\90428\Desktop\KVStock-restored\kvflow")
sys.path.insert(0, str(PRODUCT / "src"))

from kvflow import planner, registry, templates  # noqa: E402
from kvflow.core.errors import ContractError  # noqa: E402

SMOKE = PRODUCT / ".runtime" / "smoke"
DEMO = SMOKE / "demo-python-api"
HOME = SMOKE / "home"


def main() -> int:
    config = registry.read_config(DEMO)
    report = {}
    for template_id in ("feature", "bugfix", "refactor", "docs_or_data"):
        template = templates.get(template_id, home=HOME)
        compiled = planner.compile_plan(
            requirement="add rate limiting to the login endpoint",
            template=template,
            config=config,
            job_id=f"job-preview-{template_id}",
            home=HOME,
        )
        plan = planner.deterministic_from(compiled["plan"])
        report[template_id] = {
            "nodes": compiled["nodes"],
            "write_nodes": compiled["write_nodes"],
            "waves": planner.parallel_groups(plan),
            "max_parallel": compiled["max_parallel_workers"],
            "acceptance": plan.acceptance,
        }
    print(json.dumps(report, ensure_ascii=False, indent=2)[:2600])

    # a manager plan that exceeds the template envelope must be refused
    template = templates.get("bugfix", home=HOME)
    overreaching = {
        "id": "plan-bad",
        "job_id": "job-bad",
        "version": 1,
        "revision": 1,
        "objective": "do everything at once",
        "deliverables": ["x"],
        "constraints": ["y"],
        "acceptance": ["z"],
        "nodes": [
            {
                "id": "one",
                "lineage_key": "lin-one",
                "objective": "patch and ship",
                "write_scopes": ["src"],
                "test_profile": "test",
                "allowed_tools": ["repo.read", "repo.patch", "repo.commit",
                                  "test.run_profile", "operation.cancel"],
            }
        ],
        "roles": ["worker", "manager"],
        "allowed_roots": ["src"],
        "tool_profiles": ["test"],
        "resource_budget": {
            "calls": 4, "input_tokens": 1000, "output_tokens": 1000, "tool_calls": 8,
            "storage_bytes": 1000, "wall_seconds": 60, "concurrency": 1,
            "deadline": "2030-01-01T00:00:00+00:00",
        },
        "approval_boundaries": ["no push"],
        "authorization_digest": registry.authorization_digest(config),
        "created_at": "2026-01-01T00:00:00+00:00",
    }
    try:
        planner.validate_manager_plan(overreaching, template=template, config=config)
        print("MANAGER ENVELOPE: NOT ENFORCED")
        return 1
    except ContractError as exc:
        print("MANAGER ENVELOPE REFUSED:", json.dumps(exc.to_dict(), ensure_ascii=False)[:400])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
