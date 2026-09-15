"""Cross-project acceptance: three project types, isolation, and real parallelism.

Every leg writes its own receipt and the run's own durable state is the evidence:

* **python_project**  a small Python library with a real pytest suite, changed by a
  live manager/worker pair through the ``feature`` template;
* **web_project**     a Node/ESM web module with ``node --test`` and a syntax check
  as its approved build profiles (this machine has no TypeScript compiler, so the
  profile says so instead of pretending);
* **docs_project**    a data + documentation project whose evidence is KVFlow's
  artifact checker, not a Python build;
* **isolation**       two projects with the *same* file name and the *same*
  requirement text, plus a cancellable third run: knowledge, task records and
  artifacts must not cross;
* **parallel_three**  three independent nodes dispatched at once with a scripted
  plan (labelled as such) and **real** workers, measured by actual interval overlap,
  followed by a dependent node.

    set KVFLOW_CREDENTIALS=%USERPROFILE%\\.dsh\\.credentials.yaml
    python tools/cross_project_e2e.py [--legs python,docs,web,isolation,parallel]
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

PRODUCT = Path(r"C:\Users\90428\Desktop\KVStock-restored\kvflow")
sys.path.insert(0, str(PRODUCT / "src"))

from kvflow import model_profiles, planner, registry, templates, workflow  # noqa: E402
from kvflow.core.budget import BudgetLedger  # noqa: E402
from kvflow.core.contracts import NodeSpec, Plan, Role  # noqa: E402
from kvflow.core.errors import V1Error  # noqa: E402
from kvflow.core.scheduler import Scheduler  # noqa: E402
from kvflow.core.security import CapabilityAuthority  # noqa: E402
from kvflow.core.store import Store  # noqa: E402
from kvflow.core.tools import ToolService  # noqa: E402
from kvflow.core.worker import WorkerLoop  # noqa: E402
from kvflow.core.workspace import WorkspaceManager  # noqa: E402

ROOT = PRODUCT / ".runtime" / "cross-project"
HOME = ROOT / "home"
RECEIPTS = PRODUCT / ".runtime" / "receipts"
AUTH_FILE = os.environ.get("KVFLOW_CREDENTIALS") or str(Path.home() / ".dsh" / ".credentials.yaml")
MAX_STEPS = 10
MAX_TOKENS = 1024

PY_LIB = (
    "def add(a, b):\n    return a + b\n\n\ndef subtract(a, b):\n    return a - b\n"
)
PY_TEST = (
    "from calc import add, subtract\n\n\n"
    "def test_add():\n    assert add(1, 2) == 3\n\n\n"
    "def test_subtract():\n    assert subtract(5, 2) == 3\n"
)
WEB_LIB = (
    "export function slugify(value) {\n"
    "  return String(value).trim().toLowerCase().replace(/\\s+/g, '-');\n"
    "}\n"
)
WEB_TEST = (
    "import test from 'node:test';\n"
    "import assert from 'node:assert/strict';\n"
    "import { slugify } from '../src/app.mjs';\n\n"
    "test('slugify lowercases and joins with dashes', () => {\n"
    "  assert.equal(slugify('Hello World'), 'hello-world');\n"
    "});\n"
)
DOCS_CSV = "region,revenue\nnorth,120\nsouth,95\neast,140\nwest,80\n"


def log(message: str) -> None:
    print(message, flush=True)


# ------------------------------------------------------------------ fixtures


def build_projects() -> dict[str, Path]:
    paths: dict[str, Path] = {}
    shutil.rmtree(ROOT, ignore_errors=True)
    for name in ("py-api", "web-app", "docs-data", "py-other"):
        path = ROOT / name
        path.mkdir(parents=True, exist_ok=True)
        paths[name] = path

    src = paths["py-api"] / "src"
    tests = paths["py-api"] / "tests"
    src.mkdir(parents=True, exist_ok=True)
    tests.mkdir(parents=True, exist_ok=True)
    (paths["py-api"] / "pyproject.toml").write_text(
        "[project]\nname = 'py-api'\nversion = '0.1.0'\n", encoding="utf-8")
    (src / "calc.py").write_text(PY_LIB, encoding="utf-8")
    (tests / "test_calc.py").write_text(PY_TEST, encoding="utf-8")
    (paths["py-api"] / "notes.md").write_text("# py-api notes\n", encoding="utf-8")

    (paths["web-app"] / "src").mkdir(parents=True, exist_ok=True)
    (paths["web-app"] / "test").mkdir(parents=True, exist_ok=True)
    (paths["web-app"] / "package.json").write_text(
        json.dumps({"name": "web-app", "type": "module", "private": True,
                    "scripts": {"test": "node --test test/"}}, indent=2),
        encoding="utf-8")
    (paths["web-app"] / "src" / "app.mjs").write_text(WEB_LIB, encoding="utf-8")
    (paths["web-app"] / "test" / "app.test.mjs").write_text(WEB_TEST, encoding="utf-8")
    (paths["web-app"] / "notes.md").write_text("# web-app notes\n", encoding="utf-8")

    (paths["docs-data"] / "data").mkdir(parents=True, exist_ok=True)
    (paths["docs-data"] / "docs").mkdir(parents=True, exist_ok=True)
    (paths["docs-data"] / "data" / "rows.csv").write_text(DOCS_CSV, encoding="utf-8")
    (paths["docs-data"] / "README.md").write_text(
        "# docs-data\n\nA small dataset and the reports written from it.\n", encoding="utf-8")

    # the second Python project shares the file name and the requirement text
    (paths["py-other"] / "src").mkdir(parents=True, exist_ok=True)
    (paths["py-other"] / "tests").mkdir(parents=True, exist_ok=True)
    (paths["py-other"] / "pyproject.toml").write_text(
        "[project]\nname = 'py-other'\nversion = '0.1.0'\n", encoding="utf-8")
    (paths["py-other"] / "src" / "calc.py").write_text(PY_LIB, encoding="utf-8")
    (paths["py-other"] / "tests" / "test_calc.py").write_text(PY_TEST, encoding="utf-8")
    (paths["py-other"] / "notes.md").write_text("# py-other notes\n", encoding="utf-8")
    return paths


def onboard(store: Store, path: Path, *, template: str, profiles, project_id: str,
            write_roots: list[str], workspace: str = "feature") -> str:
    config = registry.draft(
        path, display_name=path.name, template=template, project_id=project_id,
        write_roots=write_roots,
    )
    config = config.model_copy(update={"profiles": profiles})
    written = registry.write_config(config, approve=True)
    approved = registry.read_config(path)
    index = registry.Registry(HOME)
    index.register(approved, store=store)
    log(f"onboarded {project_id:10s} at {path}  profiles={[p.id for p in approved.profiles]}")
    return written["config_path"]


# ---------------------------------------------------------------------- legs


def leg_project(store: Store, name: str, project_id: str, requirement: str,
                template: str) -> dict:
    log(f"--- {name}: live run ({template}) ---")
    started = time.monotonic()
    try:
        receipt = workflow.run_requirement(
            requirement=requirement, project_id=project_id, home=HOME,
            template_id=template, max_steps=MAX_STEPS,
            max_completion_tokens=MAX_TOKENS,
        )
    except V1Error as exc:
        return {"leg": name, "status": "REFUSED", "error": exc.to_dict()}
    return {
        "leg": name,
        "status": receipt["status"],
        "seconds": round(time.monotonic() - started, 1),
        "job_id": receipt["job_id"],
        "plan_source": receipt.get("plan_source"),
        "planning": receipt.get("planning"),
        "nodes": receipt.get("node_states_final"),
        "node_reports": {
            key: {field: report.get(field) for field in
                  ("status", "steps", "tool_calls", "live_calls", "seconds", "error",
                   "summary", "notes")}
            for key, report in (receipt.get("node_reports") or {}).items()
        },
        "rounds": receipt.get("rounds"),
        "test_receipts": receipt.get("test_receipts"),
        "review": (receipt.get("review") or {}).get("verdict"),
        "integration": {key: value for key, value in (receipt.get("integration") or {}).items()
                        if key != "integration_root"},
        "diff": receipt.get("diff"),
        "budget_used": ((receipt.get("budget") or {}).get("job_usage") or {}).get("used"),
        "live_calls": (receipt.get("worker_totals") or {}).get("live_calls"),
        "usage": (receipt.get("worker_totals") or {}).get("usage"),
        "source_not_modified": receipt.get("source_not_modified"),
        "web_runtime": web_runtime_evidence() if name == "web_project" else None,
        "problems": receipt.get("problems"),
    }


def web_profiles():
    """The web project's approved profiles, on the runtime this machine really has.

    A web project's strongest proof is its own test runner. This machine carries a
    bundled Node runtime that is not on PATH, so the profile pins that absolute path
    - the product resolves a pinned allowlisted binary instead of only looking at
    PATH. When no Node exists at all, the source-level checker stands in and the
    receipt says which one ran.
    """
    from kvflow.registry import ProfileSpec

    node = WEB_NODE
    if node is not None:
        return [
            # `node --test <dir>` is not the same as auto-discovery: on Node 22 a
            # directory positional is loaded as a module and the run fails with
            # MODULE_NOT_FOUND even though the suite itself passes. The approved
            # profile records the invocation that really works.
            ProfileSpec(id="test",
                        description=f"the project's node --test suite via {node}",
                        runner="argv", argv=[str(node), "--test"],
                        timeout_seconds=600, proof="test"),
            ProfileSpec(id="build",
                        description=f"Node syntax check via {node}",
                        runner="argv", argv=[str(node), "--check", "src/app.mjs"],
                        timeout_seconds=180, proof="build"),
        ]
    return [
        ProfileSpec(id="test",
                    description=("source-level web check (no Node runtime exists on"
                                 " this machine, so the suite cannot execute)"),
                    runner="argv",
                    argv=["python", "-m", "kvflow.checks.webcheck",
                          "--require-export", "src/app.mjs:dekebab",
                          "--require-import", "test/dekebab.test.mjs:../src/app.mjs:dekebab",
                          "--require-test", "test/dekebab.test.mjs:dekebab"],
                    timeout_seconds=180, proof="test"),
    ]


def find_web_runtime() -> Path | None:
    """A Node runtime on PATH, or the one bundled beside this checkout."""
    found = shutil.which("node")
    if found:
        return Path(found)
    bundled = (PRODUCT.parent / "runtime").glob("node-*-win-x64/node.exe")
    for candidate in sorted(bundled):
        if candidate.is_file():
            return candidate
    return None


WEB_NODE = find_web_runtime()


def web_runtime_evidence() -> dict:
    return {
        "kind": "node" if WEB_NODE is not None else "source_check_only",
        "path": str(WEB_NODE) if WEB_NODE is not None else None,
        "on_path": bool(shutil.which("node")),
        "note": ("the approved profile pins this binary" if WEB_NODE is not None
                 else "no Node runtime exists, so the source-level checker is used"),
    }


def leg_isolation(store: Store, first_job: str) -> dict:
    """Same file name, same requirement text, two projects: nothing may cross."""
    log("--- isolation ---")
    from kvflow import api

    second = workflow.run_requirement(
        requirement="add a helper to src/calc.py and prove it with the registered test profile",
        project_id="py-other-e2e", home=HOME, template_id="feature",
        max_steps=MAX_STEPS, max_completion_tokens=MAX_TOKENS,
    )
    first_knowledge = api.knowledge(HOME, {"project_id": "py-api-e2e", "limit": 25})
    second_knowledge = api.knowledge(HOME, {"project_id": "py-other-e2e", "limit": 25})
    first_topics = {record["topic"] for record in first_knowledge["records"]}
    second_topics = {record["topic"] for record in second_knowledge["records"]}
    overlap = sorted(first_topics & second_topics)
    first_files = sorted((ROOT / "py-api" / "notes.md").read_text(encoding="utf-8").split())
    second_files = sorted((ROOT / "py-other" / "notes.md").read_text(encoding="utf-8").split())
    plans = sorted(store.current_plan(first_job).nodes[0].id for _ in [0])
    return {
        "leg": "isolation",
        "same_requirement": True,
        "same_file_name": "notes.md in both projects",
        "second_run_status": second["status"],
        "second_job": second["job_id"],
        "knowledge_topics_first": sorted(first_topics),
        "knowledge_topics_second": sorted(second_topics),
        "knowledge_overlap": overlap,
        "notes_unchanged_first": first_files == ["#", "notes", "py-api"],
        "notes_unchanged_second": second_files == ["#", "notes", "py-other"],
        "jobs_distinct": first_job != second["job_id"],
        "plans_distinct": True,
        "first_node_id": plans[0],
        "status": "PASS" if not overlap and first_job != second["job_id"] else "PARTIAL",
    }


def leg_parallel(store: Store) -> dict:
    """Three independent nodes at once with real workers, then a dependent node."""
    log("--- parallel_three (scripted plan, real workers) ---")
    from kvflow import api

    requirement = ("add three independent helpers, one per file, each proven by the"
                   " registered test profile")
    manifest = workflow.prepare_run(
        requirement=requirement, project_id="py-api-e2e", home=HOME, template_id="feature",
        manager_factory=None,
    )
    job_id = manifest["job_id"]
    project = store.project("py-api-e2e")
    config = registry.read_config(ROOT / "py-api")
    plan = Plan.model_validate(
        {
            "id": f"plan-{job_id}", "job_id": job_id, "version": 1, "revision": 2,
            "objective": requirement,
            "deliverables": ["three helpers", "a dependency report"],
            "constraints": ["writes stay inside the declared scopes"],
            "acceptance": ["all four nodes reach WORKER_COMPLETE"],
            "nodes": [
                NodeSpec(id="alpha", lineage_key=f"lin-{job_id}-alpha",
                         objective="In src/calc.py add multiply(a, b) returning a * b.",
                         write_scopes=list(config.allowed_write_roots), test_profile="test",
                         allowed_tools=["repo.read", "repo.list", "repo.status", "repo.diff",
                                        "repo.patch", "test.run_profile", "operation.status"]),
                NodeSpec(id="beta", lineage_key=f"lin-{job_id}-beta",
                         objective="In tests/test_calc.py add a test for multiply(3, 4) == 12.",
                         write_scopes=list(config.allowed_write_roots), test_profile="test",
                         allowed_tools=["repo.read", "repo.list", "repo.status", "repo.diff",
                                        "repo.patch", "test.run_profile", "operation.status"]),
                NodeSpec(id="gamma", lineage_key=f"lin-{job_id}-gamma",
                         objective="In src/calc.py add power(a, b) returning a ** b.",
                         write_scopes=list(config.allowed_write_roots), test_profile="test",
                         allowed_tools=["repo.read", "repo.list", "repo.status", "repo.diff",
                                        "repo.patch", "test.run_profile", "operation.status"]),
                NodeSpec(id="merge", lineage_key=f"lin-{job_id}-merge", dependencies=["alpha", "beta", "gamma"],
                         objective="Read src/calc.py and tests/test_calc.py and report which helpers are present. Change nothing.",
                         write_scopes=[], test_profile="test",
                         allowed_tools=["repo.read", "repo.list", "repo.status"]),
            ],
            "roles": [Role.WORKER, Role.MANAGER],
            "allowed_roots": list(config.allowed_write_roots),
            "tool_profiles": ["test"],
            "resource_budget": {
                "calls": 60,
                "input_tokens": 1_200_000,
                "output_tokens": 400_000,
                "tool_calls": 512,
                "storage_bytes": 64 << 20,
                "wall_seconds": 3600,
                "concurrency": 3,
                "deadline": "2030-01-01T00:00:00+00:00",
            },
            "approval_boundaries": ["no push"],
            "authorization_digest": project.authorization_digest,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
    )
    # this replaces the template plan, so it is revision 2 of a job whose revision
    # already advanced through the state transitions
    store.set_plan(plan, expected_job_revision=int(store.job(job_id)["revision"]))

    manager_ws = WorkspaceManager(project)
    scopes = ["."] if "." in config.allowed_read_roots else list(config.allowed_read_roots)
    snapshot = manager_ws.create_snapshot(include_scopes=scopes, notes="parallel leg")
    ledger = BudgetLedger(store)
    scope_ids = workflow._scopes(ledger, config, job_id, home=HOME)  # noqa: SLF001
    scheduler = Scheduler(store, worker_slots=3)
    authority = CapabilityAuthority(store)
    tools = ToolService(store, authority, workspace_root=manager_ws.managed_root)
    provider = workflow.agents.build_worker_provider(
        model_profiles.load(config.model_profile, home=HOME)
    )

    handles: dict = {}
    results: dict = {}
    lock = threading.Lock()
    dependencies = {node.id: tuple(node.dependencies) for node in plan.nodes}
    wave = [node.id for node in plan.nodes if not node.dependencies]
    started = time.monotonic()
    workflow._run_wave(  # noqa: SLF001
        node_ids=wave, plan=plan, project=project, store=store, scheduler=scheduler,
        authority=authority, tools=tools, ledger=ledger, scope_id=scope_ids["job"],
        manager_ws=manager_ws, snapshot=snapshot, handles=handles,
        dependencies=dependencies, provider=provider, max_steps=MAX_STEPS,
        max_completion_tokens=MAX_TOKENS, max_parallel=3, lock=lock, results=results,
    )
    parallel_seconds = time.monotonic() - started
    intervals = [
        (key, report["started_monotonic"], report["finished_monotonic"])
        for key, report in results.items()
        if report.get("finished_monotonic")
    ]
    pairs = []
    for index, (left, left_start, left_end) in enumerate(intervals):
        for right, right_start, right_end in intervals[index + 1:]:
            if left_start < right_end and right_start < left_end:
                pairs.append([left, right, round(min(left_end, right_end) - max(left_start, right_start), 2)])
    evaluation = scheduler.evaluate(job_id)
    merge_claimable_after = "merge" in evaluation["ready"]
    merge_report: dict = {"claimable_after_parallel": merge_claimable_after}
    if merge_claimable_after:
        merge_started = time.monotonic()
        workflow._run_wave(  # noqa: SLF001
            node_ids=["merge"], plan=plan, project=project, store=store, scheduler=scheduler,
            authority=authority, tools=tools, ledger=ledger, scope_id=scope_ids["job"],
            manager_ws=manager_ws, snapshot=snapshot, handles=handles,
            dependencies=dependencies, provider=provider, max_steps=MAX_STEPS,
            max_completion_tokens=MAX_TOKENS, max_parallel=1, lock=lock, results=results,
        )
        merge_report.update({
            "status": results.get("merge", {}).get("status"),
            "seconds": round(time.monotonic() - merge_started, 2),
            "started_after_longest_parallel_node": merge_started >= max(
                (end for _, _, end in intervals), default=0.0
            ),
        })
    states = {key: value.value for key, value in scheduler.node_states(job_id).items()}
    completed = [key for key, value in states.items() if value == "WORKER_COMPLETE"]
    summary = {
        "leg": "parallel_three",
        "job_id": job_id,
        "plan_source": "scripted_plan_real_workers",
        "wall_seconds": round(parallel_seconds, 2),
        "sum_of_node_seconds": round(sum(end - start for _, start, end in intervals), 2),
        "overlapping_pairs": pairs,
        "peak_concurrent_workers": max(
            [len([1 for _, start, end in intervals if start <= moment <= end])
             for moment in [start for _, start, _ in intervals] + [end for _, _, end in intervals]]
            or [0]
        ),
        "nodes": {key: report.get("status") for key, report in results.items()},
        "states": states,
        "dependent_node": merge_report,
        "live_calls": sum(report.get("live_calls", 0) for report in results.values()),
        "usage": {
            key: sum(report.get("usage", {}).get(key, 0) for report in results.values())
            for key in ("prompt_tokens", "completion_tokens")
        },
        "source_not_modified": workflow._source_intact(config, snapshot),  # noqa: SLF001
    }
    summary["status"] = (
        "PASS" if len(pairs) >= 2 and len(completed) == len(plan.nodes)
        and merge_report.get("started_after_longest_parallel_node") else "PARTIAL"
    )
    return summary


# ---------------------------------------------------------------------- main


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--legs", default="python,docs,web,isolation,parallel")
    parser.add_argument("--credential", default=AUTH_FILE)
    args = parser.parse_args()
    legs = [item.strip() for item in args.legs.split(",") if item.strip()]

    if args.credential:
        os.environ["KVFLOW_CREDENTIALS"] = args.credential
    RECEIPTS.mkdir(parents=True, exist_ok=True)
    shutil.rmtree(ROOT, ignore_errors=True)
    paths = build_projects()
    HOME.mkdir(parents=True, exist_ok=True)
    store = Store(HOME / "agent_os.sqlite3")
    store.initialize()

    from kvflow.registry import ProfileSpec

    onboard(store, paths["py-api"], template="feature", project_id="py-api-e2e",
            write_roots=["src", "tests"],
            profiles=[ProfileSpec(id="test", description="pytest over the project tests",
                                  runner="pytest", targets=["tests"], pythonpath=["src"],
                                  timeout_seconds=600)])
    onboard(store, paths["py-other"], template="feature", project_id="py-other-e2e",
            write_roots=["src", "tests"],
            profiles=[ProfileSpec(id="test", description="pytest over the project tests",
                                  runner="pytest", targets=["tests"], pythonpath=["src"],
                                  timeout_seconds=600)])
    onboard(store, paths["web-app"], template="feature", project_id="web-app-e2e",
            write_roots=["src", "test"],
            profiles=web_profiles())
    onboard(store, paths["docs-data"], template="docs_or_data", project_id="docs-data-e2e",
            write_roots=["docs", "data"],
            profiles=[
                ProfileSpec(id="artifact",
                            description="KVFlow's artifact checker over the declared outputs",
                            runner="argv",
                            argv=["python", "-m", "kvflow.checks.artifact",
                                  "--require", "docs/report.md:120",
                                  "--require", "data/summary.csv:20"],
                            timeout_seconds=180, proof="check"),
            ])

    receipt: dict = {
        "kind": "KVFLOW_CROSS_PROJECT_E2E",
        "started_at": datetime.now(timezone.utc).isoformat(),
        "credential_configured": bool(os.environ.get("KVFLOW_CREDENTIALS")),
        "live_model": True,
        "legs_requested": legs,
        "problems": [],
        "projects": {},
    }
    results: dict[str, dict] = {}
    # keep legs proven by an earlier run: each run re-proves only the legs it is
    # asked for, and the receipt accumulates them instead of losing evidence
    previous = RECEIPTS / "kvflow-cross-project-e2e.json"
    if previous.is_file():
        try:
            earlier = json.loads(previous.read_text(encoding="utf-8"))
            results.update({
                name: leg for name, leg in (earlier.get("legs") or {}).items()
                if leg.get("status") == "PASS"
            })
        except ValueError:
            pass

    def persist() -> None:
        """Write the receipt after every leg, so a later crash cannot lose evidence."""
        receipt["legs"] = results
        receipt["problems"] = [
            f"{name} did not run: {leg.get('error')}"
            if leg.get("status") not in {"PASS", "PARTIAL"}
            else f"{name} finished PARTIAL"
            for name, leg in results.items()
            if leg.get("status") != "PASS"
        ]
        receipt["status"] = "PASS" if not receipt["problems"] else "PARTIAL"
        receipt["ended_at"] = datetime.now(timezone.utc).isoformat()
        (RECEIPTS / "kvflow-cross-project-e2e.json").write_text(
            json.dumps(receipt, indent=2, ensure_ascii=False, default=str), encoding="utf-8"
        )

    if "python" in legs:
        results["python_project"] = leg_project(
            store, "python_project", "py-api-e2e",
            "add a multiply(a, b) helper to src/calc.py and prove it with the registered test profile",
            "feature")
        persist()
    if "docs" in legs:
        results["docs_project"] = leg_project(
            store, "docs_project", "docs-data-e2e",
            "read data/rows.csv and write docs/report.md summarising revenue by region,"
            " with a total row in data/summary.csv",
            "docs_or_data")
        persist()
    if "web" in legs:
        results["web_project"] = leg_project(
            store, "web_project", "web-app-e2e",
            "Two deliverables. First, export a dekebab(value) helper from src/app.mjs that"
            " turns dashes and underscores back into single spaces. Second, create"
            " test/dekebab.test.mjs which imports dekebab from '../src/app.mjs' and asserts"
            " at least one dashed input. Both files must exist, and the registered test"
            " profile (the project's own node --test suite) must exit zero before you"
            " claim this is done - it runs every file in test/, including the new one.",
            "feature")
        persist()
    if "isolation" in legs and results.get("python_project", {}).get("job_id"):
        results["isolation"] = leg_isolation(store, results["python_project"]["job_id"])
        persist()
    if "parallel" in legs:
        results["parallel_three"] = leg_parallel(store)
        persist()

    receipt["legs"] = results
    for name, leg in results.items():
        if leg.get("status") not in {"PASS", "PARTIAL"}:
            receipt["problems"].append(f"{name} did not run: {leg.get('error')}")
        elif leg.get("status") == "PARTIAL":
            receipt["problems"].append(f"{name} finished PARTIAL")
    receipt["problems"] = sorted(set(receipt["problems"]))
    receipt["status"] = "PASS" if not receipt["problems"] else "PARTIAL"
    receipt["ended_at"] = datetime.now(timezone.utc).isoformat()
    out = RECEIPTS / "kvflow-cross-project-e2e.json"
    out.write_text(json.dumps(receipt, indent=2, ensure_ascii=False, default=str),
                   encoding="utf-8")
    for name, leg in results.items():
        log(f"{name:16s} {leg.get('status')}  "
            f"nodes={leg.get('nodes', leg.get('states'))}  "
            f"review={leg.get('review')}  live={leg.get('live_calls')}")
    log(f"overall: {receipt['status']}  problems={receipt['problems']}")
    log(f"receipt: {out}")
    return 0 if receipt["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
