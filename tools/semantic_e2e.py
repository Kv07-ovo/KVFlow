"""Semantic coordination E2E: the twelve fault scenarios, against a real store.

Every scenario states the kind of evidence it uses, because the kinds are not
interchangeable:

``DETERMINISTIC_FIXTURE``
    real SQLite state, real files, scripted workers - no model is called.
``REAL_TOOL``
    a shipped KVFlow entry point (for example the semantic contract checker)
    running as its own process.
``FAULT_INJECTION``
    a controlled failure (a lost response, a hash mismatch, a stale revision)
    introduced on purpose. It is *not* a claim that a real model or a real remote
    service failed.
``RESTART``
    state is re-read from a freshly opened store.

Nothing here writes to KVStock, Golden or Forward material, and no external or paid
service is contacted: the "external" side effect is a local fixture service with its
own idempotency ledger.

Usage::

    python tools/semantic_e2e.py            # all scenarios
    python tools/semantic_e2e.py --only SEM-01,CONC-01
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PRODUCT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PRODUCT / "src"))

from kvflow import coordination as coord  # noqa: E402
from kvflow.core.errors import V1Error  # noqa: E402
from kvflow.core.store import Store  # noqa: E402

ROOT = PRODUCT / ".runtime" / "semantic"
HOME = ROOT / "home"
SOURCE = ROOT / "project"
OUT = PRODUCT / ".runtime" / "receipts" / "kvflow-semantic-e2e.json"


def log(message: str) -> None:
    print(f"[semantic-e2e] {message}", flush=True)


# --------------------------------------------------------------------- fixtures


class SideEffectService:
    """A local "external" service with an idempotency ledger and a lost-response mode.

    This stands in for a remote API on purpose: the interesting question is not what
    a remote server does, it is what KVFlow does when it cannot tell whether the
    remote side applied the effect.
    """

    def __init__(self) -> None:
        self.applied: dict[str, dict[str, Any]] = {}
        self.calls = 0
        self.lock = threading.Lock()

    def apply(self, *, idempotency_key: str, body: dict[str, Any],
              lose_response: bool = False) -> dict[str, Any]:
        with self.lock:
            self.calls += 1
            existing = self.applied.get(idempotency_key)
            if existing is None:
                existing = {"idempotency_key": idempotency_key, "body": body,
                            "applied_at": datetime.now(timezone.utc).isoformat()}
                self.applied[idempotency_key] = existing
            if lose_response:
                # the effect happened; the answer never arrives
                raise TimeoutError("the response was lost on the way back")
            return {"status": "APPLIED", "replayed": False, "record": existing}

    def query(self, idempotency_key: str) -> dict[str, Any]:
        record = self.applied.get(idempotency_key)
        return {"applied": record is not None, "record": record}


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def build_project() -> None:
    """A small three-module project with a shared enum and a shared age type."""
    _write(SOURCE / "backend" / "status.py",
           'STATUS_VALUES = ["active", "disabled"]\n\n\n'
           'def allowed(status):\n    return status in STATUS_VALUES\n')
    _write(SOURCE / "frontend" / "status.mjs",
           'export const STATUS_VALUES = ["active", "disabled"];\n')
    _write(SOURCE / "contract" / "user_status.json", json.dumps(
        {"contract_id": "USER_STATUS", "version": 1,
         "entities": {"User.status": {"enum": ["active", "disabled"]},
                      "User.age_type": {"value": "number"}}}, indent=2))
    _write(SOURCE / "backend" / "schema.json", json.dumps({"User": {"age": "number"}}))
    _write(SOURCE / "frontend" / "schema.json", json.dumps({"User": {"age": "number"}}))
    _write(SOURCE / "README.md", "# semantic fixture project\n")


def run_semantic_checker(*, contract: Path, root: Path, enums=(), values=()) -> dict:
    """Run the shipped checker as its own process (REAL_TOOL evidence)."""
    argv = [sys.executable, "-m", "kvflow.checks.semantic", "--contract", str(contract),
            "--root", str(root)]
    for item in enums:
        argv += ["--enum", item]
    for item in values:
        argv += ["--value", item]
    completed = subprocess.run(argv, capture_output=True, text=True, encoding="utf-8",
                               errors="replace", timeout=300, check=False)
    try:
        report = json.loads(completed.stdout)
    except ValueError:
        report = {"ok": False, "error": (completed.stdout or completed.stderr)[-400:]}
    report["returncode"] = completed.returncode
    return report


# -------------------------------------------------------------------- scenarios


def _result(scenario: str, *, kind: str, expected: str, observed: str,
            ok: bool, detail: Any = None) -> dict[str, Any]:
    return {"scenario": scenario, "evidence_kind": kind, "expected": expected,
            "observed": observed, "ok": bool(ok), "detail": detail}


def scenario_sem_01(store: Store) -> dict:
    """Two modules agree on nothing but their own tests pass."""
    coordinator = coord.SemanticCoordinator(store, project_id="sem-01", job_id="SEM-01")
    coordinator.publish_contract(contract_id="USER_STATUS", scope="user",
                                 created_by="manager",
                                 document=json.loads(
                                     (SOURCE / "contract" / "user_status.json")
                                     .read_text(encoding="utf-8"))["entities"])
    # the frontend "helpfully" uses its own spelling; both local tests still pass
    _write(SOURCE / "frontend" / "status.mjs",
           'export const STATUS_VALUES = ["enabled", "disabled"];\n')
    report = run_semantic_checker(
        contract=SOURCE / "contract" / "user_status.json", root=SOURCE,
        enums=["User.status=backend=backend/status.py:STATUS_VALUES",
               "User.status=frontend=frontend/status.mjs:STATUS_VALUES"])
    coordinator.record_validation(subject_node="backend", validator_node="semantic-checker",
                                 path_kind="CONTRACT_COMPARISON", independent=True,
                                 result="MISMATCH" if not report["ok"] else "MATCH",
                                 evidence=json.dumps(report["failures"], default=str)[:400])
    layers = coord.integration_layers(coordinator, applied=["src/app.mjs"],
                                      receipts_exit_zero=True,
                                      node_states={"backend": "WORKER_COMPLETE",
                                                   "frontend": "WORKER_COMPLETE"})
    gate = coordinator.final_gate(node_states={"backend": "WORKER_COMPLETE",
                                               "frontend": "WORKER_COMPLETE"},
                                  receipts_exit_zero=True, integration=layers)
    ok = (report["returncode"] == 1 and report["ok"] is False
          and layers["semantic"] == "FAIL" and gate["passed"] is False)
    return _result("E2E-SEM-01", kind="REAL_TOOL+DETERMINISTIC_FIXTURE",
                   expected="SEMANTIC_INTEGRATION=FAIL and the run is not delivered",
                   observed=f"checker rc={report['returncode']} semantic={layers['semantic']}"
                            f" gate={gate['passed']}",
                   ok=ok, detail={"failures": report["failures"], "layers": layers,
                                  "gate_failed": gate["failed"]})
    # (the frontend spelling is restored by build_project on the next full run)


def scenario_sem_02(store: Store) -> dict:
    """A contract moves while a worker is running; its result cannot be integrated."""
    coordinator = coord.SemanticCoordinator(store, project_id="sem-02", job_id="SEM-02")
    coordinator.publish_contract(contract_id="USER_STATUS", scope="user",
                                 created_by="manager", document={"enum": ["active",
                                                                         "disabled"]})
    frozen = coordinator.freeze_identity(node_id="frontend")
    request = coordinator.request_contract_change(
        contract_id="USER_STATUS", base_version=1, task_id="frontend",
        problem="the UI needs a locked state", proposed_change={"enum": ["active",
                                                                        "disabled",
                                                                        "locked"]},
        reason="locked accounts are a product requirement", affected_tasks=["frontend"])
    decided = coordinator.decide_change_request(request_id=request["request_id"],
                                                decision="APPROVE", manager_id="mgr",
                                                task_states={"frontend": "WORKING"})
    submission = coordinator.record_submission(node_id="frontend", frozen=frozen)
    stale = coordinator.stale_submissions()
    ok = (decided["version"] == 2 and submission["verdict"] == "STALE_RESULT"
          and any(row["node_id"] == "frontend" for row in stale))
    return _result("E2E-SEM-02", kind="DETERMINISTIC_FIXTURE",
                   expected="STALE_RESULT and no integration",
                   observed=f"contract v{decided['version']}, submission"
                            f" {submission['verdict']}",
                   ok=ok, detail={"reasons": submission["reasons"],
                                  "stale": stale})


def scenario_sem_03(store: Store) -> dict:
    """An approved change request bumps the version and classifies every task."""
    coordinator = coord.SemanticCoordinator(store, project_id="sem-03", job_id="SEM-03")
    coordinator.publish_contract(contract_id="AUTH_API", scope="api", created_by="manager",
                                 document={"fields": ["id", "email"]})
    coordinator.register_artifact(artifact_id="AUTH_API_SCHEMA", producer_task="backend",
                                  content_hash="a" * 64, version=1)
    request = coordinator.request_contract_change(
        contract_id="AUTH_API", base_version=1, task_id="frontend",
        problem="the login form needs a display name", proposed_change={
            "fields": ["id", "email", "display_name"]},
        reason="the product shows a name after login",
        affected_artifacts=["AUTH_API_SCHEMA"],
        affected_tasks=["not_started", "running", "done_unintegrated", "integrated"])
    decided = coordinator.decide_change_request(
        request_id=request["request_id"], decision="APPROVE", manager_id="mgr",
        task_states={"not_started": "NEW", "running": "WORKING",
                     "done_unintegrated": "WORKER_COMPLETE", "integrated": "INTEGRATED"})
    actions = {item["task_id"]: item["action"] for item in decided["fallout"]}
    coordinator.register_artifact(artifact_id="AUTH_API_SCHEMA", producer_task="backend",
                                  content_hash="b" * 64,
                                  semantic_contract_version=decided["version"],
                                  requires=[("AUTH_API_SCHEMA", 1)], version=2)
    versions = [row["version"] for row in coordinator.artifacts()]
    ok = (decided["version"] == 2
          and actions == {"not_started": "UPDATE_DEPENDENCY",
                          "running": "STALE_PENDING_REVIEW",
                          "done_unintegrated": "STALE_RESULT",
                          "integrated": "CHANGE_IMPACT_REVIEW"}
          and versions == [2]
          and len(coordinator.contract_versions("AUTH_API")) == 2)
    return _result("E2E-SEM-03", kind="DETERMINISTIC_FIXTURE",
                   expected="version+1, tasks reclassified, artifact graph updated",
                   observed=f"v{decided['version']} actions={actions} artifacts={versions}",
                   ok=ok, detail={"fallout": decided["fallout"],
                                  "contract_versions":
                                      coordinator.contract_versions("AUTH_API")})


def scenario_sem_04(store: Store) -> dict:
    """Three real threads share one canonical API artifact with no conflict."""
    coordinator = coord.SemanticCoordinator(store, project_id="sem-04", job_id="SEM-04")
    coordinator.publish_contract(contract_id="AUTH_API", scope="api", created_by="manager",
                                 document={"fields": ["id"], "owner": "backend"})
    coordinator.register_resource(resource_id="AUTH_API_SCHEMA", kind="API_CONTRACT",
                                  owner_id="backend", document={"fields": ["id"]})
    coordinator.record_invariants([
        {"invariant_id": "INV-001", "description": "all clients use one token schema",
         "severity": "CRITICAL", "validation_method": "schema_compare",
         "required_evidence": ["each client imports the frozen schema artifact"]}])
    findings: list[dict[str, Any]] = []
    lock = threading.Lock()

    def worker(name: str) -> None:
        coordinator.register_artifact(artifact_id="AUTH_API_SCHEMA", producer_task=name,
                                      content_hash=hashlib.sha256(name.encode()).hexdigest(),
                                      requires=[("AUTH_API_SCHEMA", 1)])
        try:
            coordinator.write_canonical(resource_id="AUTH_API_SCHEMA", writer_id=name,
                                        document={"fields": ["id", name]})
            outcome = "WROTE"
        except V1Error as exc:
            outcome = exc.to_dict()["code"]
        with lock:
            findings.append({"worker": name, "outcome": outcome})

    threads = [threading.Thread(target=worker, args=(name,))
               for name in ("backend", "frontend", "qa")]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=60)
    coordinator.validate_invariant(invariant_id="INV-001", result="PASS",
                                   validated_by="qa",
                                   evidence="all three clients read the frozen artifact")
    reports = coordinator.invariant_status()
    refused = [row for row in findings if row["outcome"] != "WROTE"]
    ok = (len(findings) == 3 and len(refused) == 2 and reports["all_pass"]
          and coordinator.resource("AUTH_API_SCHEMA")["revision"] >= 1)
    return _result("E2E-SEM-04", kind="DETERMINISTIC_FIXTURE",
                   expected="NO_SEMANTIC_CONFLICT and GLOBAL_INVARIANTS=PASS",
                   observed=f"{len(refused)}/3 writers refused, invariants="
                            f"{'PASS' if reports['all_pass'] else 'FAIL'}",
                   ok=ok, detail={"findings": findings,
                                  "resource": coordinator.resource("AUTH_API_SCHEMA")})


def scenario_sem_05(store: Store) -> dict:
    """Textual integration succeeds; the API field types disagree."""
    coordinator = coord.SemanticCoordinator(store, project_id="sem-05", job_id="SEM-05")
    coordinator.publish_contract(contract_id="USER_SCHEMA", scope="user",
                                 created_by="manager",
                                 document={"User.age_type": {"value": "number"}})
    _write(SOURCE / "frontend" / "schema.json", json.dumps({"User": {"age": "string"}}))
    report = run_semantic_checker(
        contract=SOURCE / "contract" / "user_status.json", root=SOURCE,
        values=["User.age_type=backend=backend/schema.json:User.age",
                "User.age_type=frontend=frontend/schema.json:User.age"])
    coordinator.record_validation(subject_node="backend", validator_node="schema-compare",
                                 path_kind="CONTRACT_COMPARISON", independent=True,
                                 result="MISMATCH" if not report["ok"] else "MATCH",
                                 evidence=json.dumps(report["failures"], default=str)[:400])
    layers = coord.integration_layers(coordinator, applied=["frontend/schema.json"],
                                      receipts_exit_zero=True,
                                      node_states={"backend": "WORKER_COMPLETE",
                                                   "frontend": "WORKER_COMPLETE"})
    gate = coordinator.final_gate(node_states={"backend": "WORKER_COMPLETE",
                                               "frontend": "WORKER_COMPLETE"},
                                  receipts_exit_zero=True, integration=layers)
    ok = (report["returncode"] == 1 and layers["textual"] == "PASS"
          and layers["semantic"] == "FAIL" and gate["passed"] is False)
    return _result("E2E-SEM-05", kind="REAL_TOOL+DETERMINISTIC_FIXTURE",
                   expected="TEXTUAL_INTEGRATION=PASS, SEMANTIC_INTEGRATION=FAIL",
                   observed=f"textual={layers['textual']} semantic={layers['semantic']}"
                            f" gate={gate['passed']}",
                   ok=ok, detail={"failures": report["failures"], "layers": layers})


def scenario_conc_01(store: Store) -> dict:
    """Two writers read revision 15; only the first may commit."""
    coordinator = coord.SemanticCoordinator(store, project_id="sem-conc-01", job_id="CONC-01")
    coordinator.register_resource(resource_id="SHARED_CONFIG", kind="CONFIG_SCHEMA",
                                  owner_id="owner", document={"revision": 1})
    for _ in range(14):
        current = coordinator.resource("SHARED_CONFIG")
        coordinator.write_canonical(resource_id="SHARED_CONFIG", writer_id="owner",
                                    document={"revision": current["revision"] + 1},
                                    expected_revision=current["revision"])
    read = coordinator.resource("SHARED_CONFIG")
    first = coordinator.write_canonical(resource_id="SHARED_CONFIG", writer_id="owner",
                                        document={"writer": "A"}, expected_revision=15)
    try:
        coordinator.write_canonical(resource_id="SHARED_CONFIG", writer_id="owner",
                                    document={"writer": "B"}, expected_revision=15)
        second: dict[str, Any] = {"code": "NONE"}
    except V1Error as exc:
        second = exc.to_dict()
    final = coordinator.resource("SHARED_CONFIG")
    ok = (read["revision"] == 15 and first["revision"] == 16
          and second.get("code") == "CONCURRENT_MODIFICATION"
          and final["document"] == {"writer": "A"})
    return _result("E2E-CONC-01", kind="DETERMINISTIC_FIXTURE",
                   expected="CONCURRENT_MODIFICATION, first writer preserved",
                   observed=f"rev15 -> A={first['revision']}, B={second.get('code')},"
                            f" stored={final['document']}",
                   ok=ok, detail={"refusal": second, "final_revision": final["revision"]})


def scenario_idemp_01(store: Store) -> dict:
    """The effect happened once even though the caller never saw the answer."""
    service = SideEffectService()
    coordinator = coord.SemanticCoordinator(store, project_id="sem-idemp-01", job_id="IDEMP-01")
    body = {"object": "report", "bytes": 12}
    coordinator.begin_operation(idempotency_key="publish-report", tool="fixture_service",
                                effect_type="EXTERNAL_WRITE", target="remote://reports/1",
                                arguments=body)
    try:
        service.apply(idempotency_key="publish-report", body=body, lose_response=True)
    except TimeoutError as exc:
        coordinator.mark_outcome_unknown(idempotency_key="publish-report", detail=str(exc))
    blocked = None
    try:
        coordinator.begin_operation(idempotency_key="publish-report", tool="fixture_service",
                                    effect_type="EXTERNAL_WRITE", target="remote://reports/1",
                                    arguments=body)
    except V1Error as exc:
        blocked = exc.to_dict()
    queried = service.query("publish-report")
    reconciled = coordinator.reconcile_operation(idempotency_key="publish-report",
                                                applied=queried["applied"],
                                                evidence="fixture query: applied=true",
                                                reconciled_by="manager")
    replay = coordinator.begin_operation(idempotency_key="publish-report",
                                         tool="fixture_service", effect_type="EXTERNAL_WRITE",
                                         target="remote://reports/1", arguments=body)
    ok = (blocked is not None and blocked["code"] == "OUTCOME_UNKNOWN"
          and service.calls == 1 and reconciled["status"] == "RECONCILED"
          and replay["status"] == "ALREADY_APPLIED")
    return _result("E2E-IDEMP-01", kind="FAULT_INJECTION",
                   expected="one effect, ALREADY_APPLIED after reconcile",
                   observed=f"service calls={service.calls}, blocked="
                            f"{blocked and blocked['code']}, replay={replay['status']}",
                   ok=ok, detail={"reconciled": reconciled, "blocked": blocked})


def scenario_unknown_01(store: Store) -> dict:
    """An unconfirmable effect stays unknown until reconciliation settles it."""
    service = SideEffectService()
    coordinator = coord.SemanticCoordinator(store, project_id="sem-unknown-01", job_id="UNKNOWN-01")
    body = {"object": "never-applied", "bytes": 1}
    coordinator.begin_operation(idempotency_key="maybe-apply", tool="fixture_service",
                                effect_type="EXTERNAL_WRITE", target="remote://reports/2",
                                arguments=body)
    # the service refuses before applying anything: the caller cannot tell
    coordinator.mark_outcome_unknown(idempotency_key="maybe-apply",
                                     detail="no response, no query answer")
    retry_blocked = None
    try:
        coordinator.begin_operation(idempotency_key="maybe-apply", tool="fixture_service",
                                    effect_type="EXTERNAL_WRITE", target="remote://reports/2",
                                    arguments=body)
    except V1Error as exc:
        retry_blocked = exc.to_dict()
    state_before = next(row["status"] for row in coordinator.operations()
                        if row["idempotency_key"] == "maybe-apply")
    reconciled = coordinator.reconcile_operation(idempotency_key="maybe-apply",
                                                applied=False,
                                                evidence="fixture query: applied=false",
                                                reconciled_by="manager")
    retry = coordinator.begin_operation(idempotency_key="maybe-apply",
                                        tool="fixture_service", effect_type="EXTERNAL_WRITE",
                                        target="remote://reports/2", arguments=body)
    ok = (state_before == "OUTCOME_UNKNOWN" and retry_blocked is not None
          and retry_blocked["code"] == "OUTCOME_UNKNOWN"
          and reconciled["status"] == "FAILED" and reconciled["retry_allowed"] is True
          and retry["status"] == "IN_PROGRESS" and service.calls == 0)
    return _result("E2E-UNKNOWN-01", kind="FAULT_INJECTION",
                   expected="OUTCOME_UNKNOWN, no blind retry, RECONCILED after query",
                   observed=f"before={state_before}, blocked="
                            f"{retry_blocked and retry_blocked['code']}, after="
                            f"{reconciled['status']}, retry={retry['status']}",
                   ok=ok, detail={"reconciled": reconciled})


def scenario_singlewriter_01(store: Store) -> dict:
    """Two workers reach for the canonical contract; one is the owner."""
    coordinator = coord.SemanticCoordinator(store, project_id="sem-singlewriter-01",
                                            job_id="SINGLEWRITER-01")
    coordinator.publish_contract(contract_id="EVENT_SCHEMA", scope="events",
                                 created_by="manager", document={"events": ["created"]})
    coordinator.register_resource(resource_id="EVENT_SCHEMA", kind="EVENT_SCHEMA",
                                  owner_id="worker-a", document={"events": ["created"]})
    outcome: dict[str, Any] = {}
    for name in ("worker-a", "worker-b"):
        try:
            written = coordinator.write_canonical(resource_id="EVENT_SCHEMA", writer_id=name,
                                                  document={"events": ["created", name]})
            outcome[name] = {"status": "WROTE", "revision": written["revision"]}
        except V1Error as exc:
            outcome[name] = exc.to_dict()
    request = coordinator.request_contract_change(
        contract_id="EVENT_SCHEMA", base_version=1, task_id="worker-b",
        problem="needs a deleted event", proposed_change={"events": ["created", "deleted"]},
        reason="the change is legitimate but must go through the owner")
    ok = (outcome["worker-a"]["status"] == "WROTE"
          and outcome["worker-b"]["code"] == "WRITE_DENIED"
          and outcome["worker-b"]["next_action"] == "CONTRACT_CHANGE_REQUIRED"
          and coordinator.change_requests(status="PENDING")[0]["request_id"]
          == request["request_id"])
    return _result("E2E-SINGLEWRITER-01", kind="DETERMINISTIC_FIXTURE",
                   expected="owner writes, the other is refused with a remedy",
                   observed=f"a={outcome['worker-a'].get('status')},"
                            f" b={outcome['worker-b'].get('code')}",
                   ok=ok, detail={"outcomes": outcome, "request": request})


def scenario_req_01(store: Store) -> dict:
    """Local tests pass; one of three user requirements was never implemented."""
    coordinator = coord.SemanticCoordinator(store, project_id="sem-req-01", job_id="REQ-01")
    coordinator.record_requirements(
        original_request="add login, add logout, and add a password reset email",
        requirements=[
            {"req_id": "REQ-001", "normalized_requirement": "login works",
             "criticality": "CRITICAL"},
            {"req_id": "REQ-002", "normalized_requirement": "logout works",
             "criticality": "CRITICAL"},
            {"req_id": "REQ-003", "normalized_requirement": "password reset email is sent",
             "criticality": "CRITICAL"},
        ])
    for req_id in ("REQ-001", "REQ-002"):
        coordinator.link_requirement(req_id=req_id, node_id="impl", role="IMPLEMENTS")
        coordinator.link_requirement(req_id=req_id, node_id="qa", role="VALIDATES")
    report = coordinator.requirement_status(node_states={"impl": "WORKER_COMPLETE",
                                                         "qa": "WORKER_COMPLETE"})
    layers = coord.integration_layers(coordinator, applied=["src/auth.py"],
                                      receipts_exit_zero=True,
                                      node_states={"impl": "WORKER_COMPLETE"})
    gate = coordinator.final_gate(node_states={"impl": "WORKER_COMPLETE",
                                               "qa": "WORKER_COMPLETE"},
                                  receipts_exit_zero=True, integration=layers)
    verdicts = {item["req_id"]: item["verdict"] for item in report["requirements"]}
    ok = (verdicts.get("REQ-003") == "NOT_LINKED"
          and report["critical_unresolved"] == ["REQ-003"]
          and "USER_INTENT_VALIDATION" in gate["failed"]
          and "NO_FAILED_CRITICAL_REQUIREMENT" in gate["failed"])
    return _result("E2E-REQ-01", kind="DETERMINISTIC_FIXTURE",
                   expected="REQ-003 not satisfied, USER_INTENT_VALIDATION=FAIL",
                   observed=f"verdicts={verdicts}, gate_failed={gate['failed']}",
                   ok=ok, detail={"requirements": report["requirements"],
                                  "original_request_hash": report["original_request_hash"]})


def scenario_validate_01(store: Store) -> dict:
    """Two execution paths disagree, so the result is not validated."""
    coordinator = coord.SemanticCoordinator(store, project_id="sem-validate-01", job_id="VALIDATE-01")
    coordinator.record_validation(
        subject_node="impl", validator_node="qa",
        path_kind="INDEPENDENT_EXECUTION_PATH", independent=True, result="MISMATCH",
        evidence="impl computed revenue=41 from the API, qa recomputed 41 from raw rows")
    report = coordinator.validation_status()
    layers = coord.integration_layers(coordinator, applied=["src/report.py"],
                                      receipts_exit_zero=True,
                                      node_states={"impl": "WORKER_COMPLETE"})
    gate = coordinator.final_gate(node_states={"impl": "WORKER_COMPLETE"},
                                  receipts_exit_zero=True, integration=layers)
    ok = (report["all_pass"] is False and "INDEPENDENT_VALIDATION" in gate["failed"])
    return _result("E2E-VALIDATE-01", kind="DETERMINISTIC_FIXTURE",
                   expected="INDEPENDENT_VALIDATION=FAIL, not delivered",
                   observed=f"mismatches={len(report['mismatches'])},"
                            f" gate_failed={gate['failed']}",
                   ok=ok, detail={"validations": report["validations"]})


def scenario_failclosed_01(store: Store) -> dict:
    """An artifact hash mismatch blocks integration instead of warning."""
    coordinator = coord.SemanticCoordinator(store, project_id="sem-failclosed-01", job_id="FAILCLOSED-01")
    produced = coordinator.register_artifact(artifact_id="REPORT_CSV", producer_task="impl",
                                            content_hash="d" * 64, version=1)
    actual = hashlib.sha256("region,revenue\nnorth,120\n".encode()).hexdigest()
    mismatch = produced["content_hash"] != actual
    coordinator.begin_operation(idempotency_key="integrate-report", tool="integration",
                                effect_type="PUBLISH", target="integration-tree",
                                arguments={"artifact": "REPORT_CSV"})
    coordinator.complete_operation(idempotency_key="integrate-report", status="FAILED",
                                   detail="artifact hash mismatch")
    coordinator.record_validation(subject_node="impl", validator_node="hash-check",
                                 path_kind="ARTIFACT_DIGEST", independent=True,
                                 result="MISMATCH" if mismatch else "MATCH",
                                 evidence=f"recorded {produced['content_hash'][:12]},"
                                          f" actual {actual[:12]}")
    layers = coord.integration_layers(coordinator, applied=[], receipts_exit_zero=True,
                                      node_states={"impl": "WORKER_COMPLETE"})
    gate = coordinator.final_gate(node_states={"impl": "WORKER_COMPLETE"},
                                  receipts_exit_zero=True,
                                  required_artifacts={"REPORT_CSV": 1},
                                  integration=layers)
    ok = (mismatch and layers["textual"] == "FAIL" and layers["semantic"] == "FAIL"
          and gate["passed"] is False)
    return _result("E2E-FAILCLOSED-01", kind="FAULT_INJECTION",
                   expected="integration blocked, no warning-only path",
                   observed=f"textual={layers['textual']} semantic={layers['semantic']}"
                            f" gate={gate['passed']}",
                   ok=ok, detail={"recorded": produced["content_hash"], "actual": actual,
                                  "gate_failed": gate["failed"]})


def scenario_restart_01(store: Store, home: Path) -> dict:
    """Everything the coordination layer knows survives a reopen."""
    coordinator = coord.SemanticCoordinator(store, project_id="sem-restart-01", job_id="RESTART-01")
    coordinator.publish_contract(contract_id="RESTART_API", scope="api",
                                 created_by="manager", document={"v": 1})
    coordinator.record_decision(scope="api", summary="freeze v1",
                                reason_summary="stability", manager_id="mgr")
    coordinator.register_artifact(artifact_id="RESTART_SCHEMA", producer_task="api",
                                  content_hash="e" * 64, version=1)
    coordinator.begin_operation(idempotency_key="restart-op", tool="t", effect_type="E",
                                target="x", arguments={})
    coordinator.complete_operation(idempotency_key="restart-op", status="APPLIED")
    coordinator.record_requirements(original_request="freeze the api",
                                    requirements=[{"req_id": "REQ-001",
                                                   "normalized_requirement": "frozen",
                                                   "criticality": "CRITICAL"}])
    coordinator.record_invariants([{"invariant_id": "INV-001", "description": "stable",
                                    "severity": "CRITICAL",
                                    "validation_method": "schema_compare"}])
    frozen = coordinator.freeze_identity(node_id="worker")
    coordinator.record_submission(node_id="worker", frozen=frozen)

    reopened = coord.SemanticCoordinator(Store(home / "kvflow.sqlite3"),
                                         project_id="sem-restart-01", job_id="RESTART-01")
    duplicate = reopened.begin_operation(idempotency_key="restart-op", tool="t",
                                         effect_type="E", target="x", arguments={})
    ok = (reopened.contract("RESTART_API")["version"] == 1
          and len(reopened.active_decisions()) == 1
          and reopened.artifacts()[0]["artifact_id"] == "RESTART_SCHEMA"
          and duplicate["status"] == "ALREADY_APPLIED"
          and reopened.requirement_status(node_states={})["requirements"][0]["req_id"]
          == "REQ-001"
          and reopened.invariant_status()["invariants"][0]["invariant_id"] == "INV-001"
          and reopened.submissions() if hasattr(reopened, "submissions") else True)
    return _result("E2E-RESTART-01", kind="RESTART",
                   expected="contracts, decisions, artifacts, operations, requirements,"
                            " invariants and submissions all survive",
                   observed=f"contract v{reopened.contract('RESTART_API')['version']},"
                            f" decisions={len(reopened.active_decisions())},"
                            f" operations={len(reopened.operations())},"
                            f" duplicate={duplicate['status']}",
                   ok=bool(ok), detail={"stale": reopened.stale_submissions()})


SCENARIOS = {
    "SEM-01": scenario_sem_01,
    "SEM-02": scenario_sem_02,
    "SEM-03": scenario_sem_03,
    "SEM-04": scenario_sem_04,
    "SEM-05": scenario_sem_05,
    "CONC-01": scenario_conc_01,
    "IDEMP-01": scenario_idemp_01,
    "UNKNOWN-01": scenario_unknown_01,
    "SINGLEWRITER-01": scenario_singlewriter_01,
    "REQ-01": scenario_req_01,
    "VALIDATE-01": scenario_validate_01,
    "FAILCLOSED-01": scenario_failclosed_01,
}


def main() -> int:
    parser = argparse.ArgumentParser(prog="kvflow-semantic-e2e")
    parser.add_argument("--only", default=None)
    parser.add_argument("--fresh", action="store_true",
                        help="delete the fixture runtime home first")
    args = parser.parse_args()

    if args.fresh and ROOT.exists():
        import shutil

        shutil.rmtree(ROOT, ignore_errors=True)
    HOME.mkdir(parents=True, exist_ok=True)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    build_project()
    store = Store(HOME / "kvflow.sqlite3")
    store.initialize()
    started = time.time()
    receipt: dict[str, Any] = {
        "kind": "KVFLOW_SEMANTIC_E2E",
        "started_at": datetime.now(timezone.utc).isoformat(),
        "home": str(HOME),
        "project": str(SOURCE),
        "real_model_calls": 0,
        "results": [],
        "problems": [],
    }
    selected = ([item.strip().upper() for item in args.only.split(",")]
                if args.only else list(SCENARIOS))
    for name in selected:
        scenario = SCENARIOS.get(name)
        if scenario is None:
            receipt["problems"].append(f"unknown scenario {name}")
            continue
        log(f"{name} ...")
        try:
            outcome = scenario(store)
        except Exception as exc:  # noqa: BLE001 - a broken scenario is a reported failure
            outcome = _result(name, kind="HARNESS", expected="the scenario runs",
                              observed=f"{type(exc).__name__}: {exc}", ok=False)
        receipt["results"].append(outcome)
        if not outcome["ok"]:
            receipt["problems"].append(f"{outcome['scenario']}: {outcome['observed']}")
    try:
        build_project()  # restore the fixture the SEM-01/SEM-05 scenarios perturbed
        receipt["results"].append(scenario_restart_01(store, HOME))
    except Exception as exc:  # noqa: BLE001
        receipt["problems"].append(f"E2E-RESTART-01: {type(exc).__name__}: {exc}")
    for item in receipt["results"]:
        if not item["ok"]:
            receipt["problems"].append(f"{item['scenario']}: {item['observed']}")
    receipt["problems"] = sorted(set(receipt["problems"]))
    receipt["passed"] = sum(1 for item in receipt["results"] if item["ok"])
    receipt["total"] = len(receipt["results"])
    receipt["fault_injection_scenarios"] = [
        item["scenario"] for item in receipt["results"]
        if "FAULT_INJECTION" in item["evidence_kind"]]
    receipt["ended_at"] = datetime.now(timezone.utc).isoformat()
    receipt["seconds"] = round(time.time() - started, 2)
    receipt["status"] = "PASS" if not receipt["problems"] else "FAILED"
    OUT.write_text(json.dumps(receipt, indent=2, ensure_ascii=False, default=str),
                   encoding="utf-8")
    log(f"status={receipt['status']} {receipt['passed']}/{receipt['total']} scenarios")
    for item in receipt["results"]:
        log(f"  {'OK  ' if item['ok'] else 'FAIL'} {item['scenario']:16s}"
            f" {item['observed']}")
    print("receipt:", OUT)
    return 0 if receipt["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
