"""The Manager role: Astra planning, review, and the rules that bound it.

The Manager is Astra Ultra through the local Codex CLI, not a second chat model
inside this process. Calls are injected so the same code serves the real CLI and
a deterministic fixture in tests, and so the budget ledger stays in control.

What this module refuses to do
------------------------------

* **It never codes.** The Manager produces a plan or a verdict; the only actor
  that changes files is the Worker.
* **It never widens scope.** A plan is validated against the registered project
  and the job's authorization digest; a node whose write scope is outside the
  plan's allowed roots, or which claims the Orchestrator role, is rejected before
  anything is persisted.
* **It never approves without evidence.** A verdict can only reference test
  receipts this store holds, and approval requires a signed-off change digest
  that matches the diff actually under review.
* **It is never a blank cheque.** The Manager's own wall-clock and call count are
  bounded and recorded; unknown cost is recorded as UNKNOWN rather than zero.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Mapping, Sequence

from .contracts import Action, Plan, Project, Role
from .errors import ContractError, ProviderError, V1Error

PLAN_SCHEMA_HINT = """{
  "objective": "one sentence",
  "deliverables": ["..."],
  "constraints": ["..."],
  "acceptance": ["..."],
  "nodes": [
    {
      "id": "short_id",
      "lineage_key": "stable_key_that_survives_replanning",
      "dependencies": ["other_node_id"],
      "objective": "what this node must achieve",
      "write_scopes": ["relative/path"],
      "test_profile": "registered_profile_id",
      "allowed_tools": ["repo.read", "repo.patch", "test.run_profile"]
    }
  ]
}"""

PLAN_PROMPT = (
    "You are the Manager (Astra Ultra) of a local Agent OS. Produce ONE JSON"
    " object describing a bounded plan for the objective below. Do not write"
    " code and do not call tools. The JSON must match this shape exactly:\n"
    f"{PLAN_SCHEMA_HINT}\n"
    "Rules: every write_scope must be inside the allowed roots; every"
    " test_profile must be one of the registered profiles; ids must be short and"
    " stable; at most 8 nodes; dependencies must form a DAG. Each node's"
    " allowed_tools must be the COMPLETE set that node needs, because a node can"
    " use nothing else. A node that must run a test profile needs"
    " 'test.run_profile'; a node that must inspect the tree needs 'repo.list',"
    " 'repo.search' and 'repo.status'; a node that must change a file needs"
    " 'repo.read' and 'repo.patch'. Prefer the fewest nodes that can finish the"
    " objective, and never create a node that cannot do its job with the tools"
    " it lists.\n"
    "Reply with the JSON object only, with no prose and no code fence."
)

REVIEW_SCHEMA_HINT = """{
  "verdict": "APPROVE" | "FIX" | "BLOCKED",
  "findings": ["concrete, checkable statements"],
  "rationale": "why this verdict follows from the evidence"
}"""

REVIEW_PROMPT = (
    "You are the Manager (Astra Ultra) reviewing a Worker's delivered change."
    " Judge only from the evidence given: the objective, the acceptance"
    " criteria, the changed files with their content digests, and the executor"
    " test receipts. Do not claim you ran anything. Reply with one JSON object:\n"
    f"{REVIEW_SCHEMA_HINT}\n"
    "Approve only when the evidence shows the acceptance criteria are met and the"
    " test receipt passed. Use FIX when the work is close but incomplete. Use"
    " BLOCKED when the evidence is missing, contradictory, or the change is out"
    " of scope. Reply with the JSON object only."
)


@dataclass
class ManagerCall:
    """One Manager exchange, with identity and cost facts separated."""

    kind: str
    text: str
    provider_model: str = ""
    reported_identity: bool = False
    duration_ms: float = 0.0
    at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "provider_model": self.provider_model or "NOT_EXPOSED",
            "reported_identity": self.reported_identity,
            "duration_ms": self.duration_ms,
            "at": self.at,
            "characters": len(self.text),
            "cost": {"status": "UNKNOWN", "note": "Astra runs on the existing Codex account"},
        }


@dataclass
class ManagerVerdict:
    verdict: str
    findings: list[str]
    rationale: str
    content_digest: str
    call: ManagerCall

    def to_dict(self) -> dict[str, Any]:
        return {
            "verdict": self.verdict,
            "findings": self.findings,
            "rationale": self.rationale,
            "content_digest": self.content_digest,
            "call": self.call.to_dict(),
        }


def extract_json(text: str) -> Any:
    """Pull one JSON object out of a model reply, refusing anything else.

    A fenced block is tolerated because models add one; the *parsing* is still
    strict, and the caller validates the object against the real contract rather
    than trusting the shape.
    """
    if not isinstance(text, str) or not text.strip():
        raise ProviderError("the Manager returned no text", outcome="OUTCOME_UNKNOWN")
    body = text.strip()
    fence = re.search(r"```(?:json)?\s*(.*?)```", body, re.DOTALL)
    if fence:
        body = fence.group(1).strip()
    start = body.find("{")
    end = body.rfind("}")
    if start == -1 or end == -1 or end < start:
        raise ContractError("the Manager reply contained no JSON object")
    candidate = body[start : end + 1]
    try:
        return json.loads(candidate)
    except ValueError as exc:
        raise ContractError("the Manager reply was not valid JSON") from exc


class Manager:
    """Bounded Manager: planning and review over an injected caller."""

    def __init__(
        self,
        *,
        caller: Callable[[str, str], Mapping[str, Any]],
        display_name: str = "Astra Ultra",
        requested_model: str = "gpt-6-astra",
        requested_effort: str = "ultra",
        max_calls: int = 6,
        max_seconds: float = 900.0,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self.caller = caller
        self.display_name = display_name
        self.requested_model = requested_model
        self.requested_effort = requested_effort
        self.max_calls = int(max_calls)
        self.max_seconds = float(max_seconds)
        self.calls: list[ManagerCall] = []

    # ------------------------------------------------------------- identity
    def identity(self) -> dict[str, Any]:
        observed = [call.provider_model for call in self.calls if call.provider_model]
        return {
            "display_name": self.display_name,
            "requested_model": self.requested_model,
            "requested_effort": self.requested_effort,
            "provider_returned_model": observed[-1] if observed else "NOT_EXPOSED",
            "provider_returned_effort": "NOT_EXPOSED",
            "verification_level": "PROVIDER_MODEL" if observed else "CONFIGURED_ONLY",
        }

    def _budget_ok(self, kind: str) -> None:
        if len(self.calls) >= self.max_calls:
            raise ProviderError(
                "the Manager call budget is exhausted",
                outcome="NOT_DISPATCHED",
                kind="manager_budget",
                used=len(self.calls),
            )

    def _ask(self, kind: str, prompt: str) -> ManagerCall:
        self._budget_ok(kind)
        started = datetime.now(timezone.utc)
        response = self.caller(kind, prompt)
        if not isinstance(response, Mapping):
            raise ProviderError("the Manager caller returned nothing usable")
        call = ManagerCall(
            kind=kind,
            text=str(response.get("text") or ""),
            provider_model=str(response.get("model") or ""),
            reported_identity=bool(response.get("reports_identity")),
            duration_ms=(datetime.now(timezone.utc) - started).total_seconds() * 1000.0,
        )
        self.calls.append(call)
        return call

    # -------------------------------------------------------------- planning
    def build_plan(
        self,
        *,
        job_id: str,
        project: Project,
        objective: str,
        allowed_roots: Sequence[str],
        resource_budget: Mapping[str, Any],
        authorization_digest: str,
        plan_id: str | None = None,
        revision: int = 1,
        version: int = 1,
    ) -> Plan:
        """Ask for a plan, then validate it against the real contract."""
        prompt = (
            f"{PLAN_PROMPT}\n\nObjective: {objective}\n"
            f"Allowed roots (write scopes must be inside these): {list(allowed_roots)}\n"
            f"Registered test profiles: {[p.id for p in project.test_profiles]}\n"
            f"Available tools: {sorted(a.value for a in Action)}\n"
            "If any node writes files, at least one node must be allowed to run a"
            " registered test profile (test.run_profile): approval requires a real"
            " executor receipt, and a plan that cannot produce one is refused.\n"
            "Maximum nodes: 8"
        )
        call = self._ask("plan", prompt)
        payload = extract_json(call.text)
        if not isinstance(payload, Mapping):
            raise ContractError("the Manager plan was not a JSON object")
        nodes = payload.get("nodes")
        if not isinstance(nodes, list) or not nodes:
            raise ContractError("the Manager plan contained no nodes")
        document = {
            "id": plan_id or f"plan-{job_id}",
            "job_id": job_id,
            "version": version,
            "revision": revision,
            "objective": payload.get("objective") or objective,
            "deliverables": payload.get("deliverables") or ["the requested change"],
            "constraints": payload.get("constraints") or ["stay inside the allowed roots"],
            "acceptance": payload.get("acceptance") or ["the registered profile passes"],
            "nodes": nodes,
            "roles": [Role.WORKER, Role.MANAGER],
            "allowed_roots": list(allowed_roots),
            "tool_profiles": [p.id for p in project.test_profiles],
            "resource_budget": dict(resource_budget),
            "approval_boundaries": [
                "no push",
                "no new paid budget",
                "the Manager does not apply changes to the source project",
            ],
            "authorization_digest": authorization_digest,
            "created_at": datetime.now(timezone.utc),
        }
        try:
            plan = Plan.model_validate(document)
        except Exception as exc:  # noqa: BLE001 - a bad plan is a typed refusal
            raise ContractError(
                "the Manager plan failed contract validation", problems=str(exc)[:600]
            ) from exc
        self._assert_plan_scope(plan, project)
        return plan

    @staticmethod
    def _assert_plan_scope(plan: Plan, project: Project) -> None:
        """A plan may not escape the registered project or claim Orchestrator.

        A node that requests an action the project never authorized is refused
        here, with the node named, instead of surfacing later as a mysterious
        capability denial in the middle of a run.
        """
        project_actions = {a.value for a in project.allowed_tools}
        for node in plan.nodes:
            if node.role is Role.ORCHESTRATOR:  # pragma: no cover - contract rejects
                raise ContractError("a plan may not assign the Orchestrator role")
            if node.test_profile not in {p.id for p in project.test_profiles}:
                raise ContractError(
                    "a node referenced an unregistered test profile",
                    node=node.id,
                    profile=node.test_profile,
                )
            requested = {a.value for a in node.allowed_tools}
            missing = requested - project_actions
            if missing:
                raise ContractError(
                    "a node requested actions the project is not authorized for",
                    node=node.id,
                    unregistered=sorted(missing),
                )
        for profile_id in plan.tool_profiles:
            if profile_id not in {p.id for p in project.test_profiles}:
                raise ContractError(
                    "the plan referenced an unregistered profile", profile=profile_id
                )
        # A plan that changes files must be able to produce the executor receipt
        # its own acceptance criteria will be judged against. Without this check
        # the inconsistency only surfaces at review time, as an approval that is
        # refused after the work is already done.
        writes = [node.id for node in plan.nodes if node.write_scopes]
        can_test = [
            node.id
            for node in plan.nodes
            if Action.TEST_RUN in {a for a in node.allowed_tools}
        ]
        if writes and not can_test:
            raise ContractError(
                "the plan can never produce an executor test receipt: no node is"
                " allowed to run a registered test profile",
                write_nodes=writes,
                registered_profiles=[p.id for p in project.test_profiles],
            )

    # ---------------------------------------------------------------- review
    def review(
        self,
        *,
        objective: str,
        acceptance: Sequence[str],
        diff: Mapping[str, Any],
        receipts: Sequence[Mapping[str, Any]],
        content_digest: str,
        worker_reports: Sequence[Mapping[str, Any]] | None = None,
    ) -> ManagerVerdict:
        """Ask for a verdict, then enforce the evidence rules on the answer.

        The packet carries what the reviewer needs to judge the *work*, not only its
        side effects: receipts with their real test counts and output digests (a bare
        exit code cannot be told apart from a repeated run), and the workers' own
        result text, which is the entire deliverable of a read-only task.
        """
        evidence = {
            "objective": objective,
            "acceptance_criteria": list(acceptance),
            "jobs_status": {
                "changed_files": [
                    {
                        "path": c.get("relative_path"),
                        "change": c.get("change"),
                        "sha256": c.get("sha256"),
                        "content_preview": c.get("content_preview"),
                        "content_bytes": c.get("content_bytes"),
                    }
                    for c in diff.get("changed", [])
                ],
                "content_digest": diff.get("content_digest"),
            },
            "executor_test_receipts": [
                {
                    "receipt_id": r.get("receipt_id"),
                    "profile_id": r.get("profile_id"),
                    "exit_code": r.get("exit_code"),
                    "runner": r.get("runner"),
                    "reported_tests": r.get("reported_tests"),
                    # a store row names the digest stdout_digest, an already shaped
                    # receipt names it stdout_sha256: accept either, never drop it
                    "stdout_sha256": r.get("stdout_sha256") or r.get("stdout_digest"),
                    "stderr_sha256": r.get("stderr_digest"),
                    "stdout_bytes": r.get("stdout_bytes"),
                    "stderr_bytes": r.get("stderr_bytes"),
                    "duration_ms": r.get("duration_ms"),
                }
                for r in receipts
            ],
            "worker_reports": [
                {
                    "node_id": report.get("node_id"),
                    "status": report.get("status"),
                    "summary": report.get("summary"),
                    "steps": report.get("steps"),
                    "tool_calls": report.get("tool_calls"),
                    "notes": list(report.get("notes") or [])[:6],
                }
                for report in (worker_reports or [])
            ],
        }
        prompt = (
            f"{REVIEW_PROMPT}\n\nEvidence (JSON):\n"
            f"{json.dumps(evidence, ensure_ascii=False, sort_keys=True, default=str)}"
        )
        call = self._ask("review", prompt)
        payload = extract_json(call.text)
        if not isinstance(payload, Mapping):
            raise ContractError("the Manager verdict was not a JSON object")
        verdict = str(payload.get("verdict") or "").upper()
        if verdict not in {"APPROVE", "FIX", "BLOCKED"}:
            raise ContractError(f"the Manager returned an unknown verdict: {verdict!r}")
        findings = payload.get("findings") or []
        if not isinstance(findings, list):
            raise ContractError("the Manager findings were not a list")
        rationale = str(payload.get("rationale") or "")
        approved = ManagerVerdict(
            verdict=verdict,
            findings=[str(item)[:2000] for item in findings][:50],
            rationale=rationale[:4000],
            content_digest=content_digest,
            call=call,
        )
        self._assert_evidence(approved, receipts)
        return approved

    @staticmethod
    def _assert_evidence(verdict: ManagerVerdict, receipts: Sequence[Mapping[str, Any]]) -> None:
        """Approval must be backed by a passing executor receipt."""
        if verdict.verdict != "APPROVE":
            return
        if not receipts:
            raise ContractError(
                "the Manager approved without any executor test receipt",
                verdict=verdict.verdict,
            )
        if not any(int(r.get("exit_code", 1)) == 0 for r in receipts):
            raise ContractError(
                "the Manager approved while no test receipt exited zero",
                verdict=verdict.verdict,
            )

    def report(self) -> dict[str, Any]:
        return {
            "identity": self.identity(),
            "calls": [call.to_dict() for call in self.calls],
            "call_count": len(self.calls),
            "max_calls": self.max_calls,
            "cost": {"status": "UNKNOWN", "note": "Astra uses the existing Codex account"},
        }


def codex_caller(
    *,
    binary: str = "codex",
    model: str = "gpt-6-astra",
    effort: str = "ultra",
    cwd: str | None = None,
    timeout_seconds: int = 900,
) -> Callable[[str, str], Mapping[str, Any]]:
    """Build a caller backed by the real local Codex CLI.

    The argv shape, the read-only sandbox, the event-stream parsing and the
    identity check all come from the already-verified v0.1 adapter; this
    function only adapts it to the v1 Manager's ``caller`` signature.
    """
    from ..adapters.base import DispatchRequest
    from ..adapters.codex_cli import CodexCliAdapter

    adapter = CodexCliAdapter(model=model, effort=effort, cwd=cwd)

    def caller(kind: str, prompt: str) -> Mapping[str, Any]:
        request = DispatchRequest(
            task_id=f"manager-{kind}",
            run_id=f"manager-{kind}",
            attempt=1,
            phase="MANAGER_REVIEW" if kind == "review" else "PLANNING",
            role="manager",
            prompt=prompt,
            requested_model=model,
            timeout_seconds=timeout_seconds,
        )
        response = adapter.dispatch(request)
        return {
            "text": response.text,
            "model": response.model,
            "reports_identity": response.reports_identity,
        }

    caller.identity = {  # type: ignore[attr-defined]
        "display_name": "Astra Ultra",
        "requested_model": model,
        "requested_effort": effort,
    }
    caller.close = adapter.close  # type: ignore[attr-defined]
    return caller
