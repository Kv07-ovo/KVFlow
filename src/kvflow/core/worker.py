"""The Worker loop: a real model using the real MCP tools.

This is where the acceptance story actually happens. A DeepSeek V4.1 Flash
response is parsed into native ``tool_calls``; each call is authorized against
the capability ticket, executed through the product MCP tools, and returned as a
``role="tool"`` message before the next request. An empty ``content`` with tool
calls is treated as a normal step, not as a missing answer.

Discipline that the tests check:

* every physical request passes the budget ledger first and settles afterwards,
* the loop is bounded by both a step count and a wall-clock deadline,
* a transport timeout marks the effect ``OUTCOME_UNKNOWN`` and is never retried
  blindly, because the model may already have been billed,
* provider usage is recorded verbatim, with ``UNKNOWN`` preserved,
* the model never sees a capability secret: only the ticket id it is given.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Mapping, Sequence

from .budget import (
    BudgetLedger,
    Charge,
    ReservationRequest,
    UsageReport,
    UsageStatus,
    new_effect_id,
)
from .contracts import Action
from .errors import BudgetDenied, ContractError, ProviderError, V1Error
from .providers import DeepSeekProvider
from .tools import ToolResult, ToolService

DEFAULT_MAX_STEPS = 12
DEFAULT_MAX_SECONDS = 900.0
#: the ledger dimensions, so a settle can name every one explicitly
_DIMENSIONS = (
    "calls",
    "input_tokens",
    "output_tokens",
    "tool_calls",
    "storage_bytes",
    "wall_seconds",
    "micro_cny",
)
#: the Worker may only ever see these tool actions through the loop
WORKER_ACTIONS = (
    Action.REPO_READ,
    Action.REPO_LIST,
    Action.REPO_SEARCH,
    Action.REPO_STATUS,
    Action.REPO_DIFF,
    Action.REPO_PATCH,
    Action.TEST_RUN,
    Action.OPERATION_STATUS,
)

SYSTEM_PROMPT = (
    "You are the Worker role of a local Agent OS. Work only inside the owned"
    " workspace you are given, using the provided tools. Do not invent file"
    " contents: read a file before you change it, and run the registered test"
    " profile before you claim the work is done. When the objective is complete,"
    " reply with a short plain-text summary and no tool call. If you cannot"
    " complete the objective, reply with 'BLOCKED:' followed by the concrete"
    " reason. Never ask for a shell, a URL or arbitrary SQL; those tools do not"
    " exist."
)


@dataclass
class ToolTrace:
    step: int
    name: str
    arguments: dict[str, Any]
    ok: bool
    status: str
    error_code: str | None
    output_digest: str
    duration_ms: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "step": self.step,
            "name": self.name,
            "arguments": self.arguments,
            "ok": self.ok,
            "status": self.status,
            "error_code": self.error_code,
            "output_digest": self.output_digest,
            "duration_ms": self.duration_ms,
        }


@dataclass
class WorkerOutcome:
    status: str
    summary: str
    steps: int
    tool_calls: int
    live_calls: int = 0
    traces: list[ToolTrace] = field(default_factory=list)
    usage: dict[str, int] = field(default_factory=dict)
    identity: dict[str, Any] = field(default_factory=dict)
    reservation_ids: list[str] = field(default_factory=list)
    receipt_ids: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    started_at: str = ""
    finished_at: str = ""
    duration_ms: float = 0.0
    estimated_micro_cny: int = 0
    unknown_outcomes: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "summary": self.summary,
            "steps": self.steps,
            "tool_calls": self.tool_calls,
            "live_calls": self.live_calls,
            "traces": [t.to_dict() for t in self.traces],
            "usage": self.usage,
            "identity": self.identity,
            "reservation_ids": self.reservation_ids,
            "receipt_ids": self.receipt_ids,
            "notes": self.notes,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "duration_ms": self.duration_ms,
            "estimated_micro_cny": self.estimated_micro_cny,
            "unknown_outcomes": self.unknown_outcomes,
        }


def mcp_tools_to_function_schemas(tools: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Map MCP-discovered tools onto the provider's ``tools`` array.

    The name is passed through unchanged so the tool that runs is the tool that
    was discovered; an unmapped name is a hard error rather than a silent skip.
    """
    schemas: list[dict[str, Any]] = []
    for tool in tools:
        name = tool.get("name") or (tool.get("function") or {}).get("name")
        if not isinstance(name, str) or not name:
            raise ContractError("an MCP tool had no usable name")
        if "inputSchema" in tool:
            parameters = tool["inputSchema"]
        elif "input_schema" in tool:
            parameters = tool["input_schema"]
        else:
            parameters = (tool.get("function") or {}).get("parameters")
        if not isinstance(parameters, Mapping):
            raise ContractError("an MCP tool had no usable input schema", tool=name)
        description = tool.get("description") or (tool.get("function") or {}).get(
            "description", ""
        )
        schemas.append(
            {
                "type": "function",
                "function": {
                    "name": name,
                    "description": str(description)[:1024],
                    "parameters": dict(parameters),
                },
            }
        )
    return schemas


class WorkerLoop:
    """Drives one Worker attempt against a real provider and real tools."""

    def __init__(
        self,
        *,
        provider: DeepSeekProvider,
        tools: ToolService,
        ledger: BudgetLedger,
        ticket: str,
        binding: Mapping[str, Any],
        scope_id: str,
        discovered_tools: Sequence[Mapping[str, Any]],
        max_steps: int = DEFAULT_MAX_STEPS,
        max_seconds: float = DEFAULT_MAX_SECONDS,
        max_completion_tokens: int = 8_192,
        clock: Callable[[], float] = time.monotonic,
        tool_executor: Callable[[str, dict[str, Any]], ToolResult] | None = None,
        receipt_recorder: Callable[[dict[str, Any]], str] | None = None,
    ) -> None:
        if max_steps < 1:
            raise ContractError("max_steps must be positive")
        self.provider = provider
        self.tools = tools
        self.ledger = ledger
        self.ticket = ticket
        self.binding = dict(binding)
        self.scope_id = scope_id
        self.max_steps = int(max_steps)
        self.max_seconds = float(max_seconds)
        self.max_completion_tokens = int(max_completion_tokens)
        self.clock = clock
        self._execute_tool = tool_executor or self._mcp_tool
        self._receipt_recorder = receipt_recorder
        self._discovered = self._select_tools(discovered_tools)

    def _select_tools(
        self, discovered: Sequence[Mapping[str, Any]]
    ) -> list[Mapping[str, Any]]:
        """Keep only the Worker-visible tools, in discovery order."""
        allowed = {action.value for action in WORKER_ACTIONS}
        selected: list[Mapping[str, Any]] = []
        for tool in discovered:
            name = str(tool.get("name", ""))
            try:
                action = self._action_for(name)
            except V1Error:
                continue
            if action in allowed:
                selected.append(tool)
        if not selected:
            raise ContractError("no Worker tool survived the mapping")
        return selected

    # ------------------------------------------------------------------ tools
    def _mcp_tool(self, name: str, arguments: dict[str, Any]) -> ToolResult:
        return self.tools.invoke(self.ticket, self._action_for(name), arguments)

    def _action_for(self, name: str) -> str:
        mapping = {
            "repo_read": Action.REPO_READ.value,
            "repo_list": Action.REPO_LIST.value,
            "repo_search": Action.REPO_SEARCH.value,
            "repo_status": Action.REPO_STATUS.value,
            "repo_diff": Action.REPO_DIFF.value,
            "repo_patch": Action.REPO_PATCH.value,
            "test_run": Action.TEST_RUN.value,
            "operation_status": Action.OPERATION_STATUS.value,
        }
        try:
            return mapping[name]
        except KeyError as exc:
            raise V1Error(f"the Worker has no such tool: {name!r}") from exc

    # ---------------------------------------------------------------- budget
    def _reserve(self, *, estimated_input: int) -> str:
        request = ReservationRequest(
            scope_id=self.scope_id,
            project_id=self.binding["project_id"],
            job_id=self.binding["job_id"],
            node_id=self.binding["node_id"],
            run_id=self.binding["run_id"],
            attempt=int(self.binding["attempt"]),
            fence=int(self.binding["fence"]),
            effect_id=new_effect_id("model-call"),
            purpose="worker model call",
            charge=Charge(
                calls=1,
                input_tokens=estimated_input,
                output_tokens=self.max_completion_tokens,
                wall_seconds=int(min(self.max_seconds, 900)),
                micro_cny=self.ledger.estimate_micro_cny(
                    input_tokens=estimated_input, output_tokens=self.max_completion_tokens
                ),
            ),
        )
        admission = self.ledger.reserve(request)
        if not admission.effects_authorized:
            raise ProviderError(
                "the budget ledger refused a second dispatch for this effect",
                outcome="NOT_DISPATCHED",
                kind="budget_replay",
            )
        return admission.reservation_id

    def _unknown_report(self) -> UsageReport:
        return UsageReport(
            status={name: UsageStatus.UNKNOWN.value for name in _DIMENSIONS},
            actual=Charge(),
        )

    def _reserve_tool_call(self, name: str) -> str:
        """Charge one tool invocation to the scope before it is dispatched.

        ``calls`` stays zero: a tool call is not a model call. The wall-clock
        dimension is left to the loop, which enforces its own deadline, so one
        span of time is never charged twice.
        """
        request = ReservationRequest(
            scope_id=self.scope_id,
            project_id=self.binding["project_id"],
            job_id=self.binding["job_id"],
            node_id=self.binding["node_id"],
            run_id=self.binding["run_id"],
            attempt=int(self.binding["attempt"]),
            fence=int(self.binding["fence"]),
            effect_id=new_effect_id("tool-call"),
            purpose=f"worker tool call {name}",
            charge=Charge(tool_calls=1),
        )
        admission = self.ledger.reserve(request)
        if not admission.effects_authorized:
            raise ProviderError(
                "the budget ledger refused a second dispatch for this tool call",
                outcome="NOT_DISPATCHED",
                kind="budget_replay",
            )
        return admission.reservation_id

    def _settle_tool_call(self, reservation_id: str) -> None:
        """Settle a dispatched tool invocation as one known tool call."""
        self.ledger.settle(
            reservation_id,
            UsageReport(
                status={
                    name: (
                        UsageStatus.KNOWN.value
                        if name == "tool_calls"
                        else UsageStatus.NEVER_STARTED.value
                    )
                    for name in _DIMENSIONS
                },
                actual=Charge(tool_calls=1),
            ),
            reconciled_by="worker-loop",
        )

    def _settle(self, reservation_id: str, usage: Mapping[str, int], *, calls: int) -> None:
        """Settle a physical call: known usage is charged, absent usage stays UNKNOWN."""
        if "prompt_tokens" not in usage or "completion_tokens" not in usage:
            # the provider did not report usage: keep the whole hold and say so
            self.ledger.settle(
                reservation_id,
                UsageReport(
                    status={name: UsageStatus.UNKNOWN.value for name in _DIMENSIONS},
                    actual=Charge(),
                ),
                reconciled_by="worker-loop",
            )
            return
        actual = Charge(
            calls=calls,
            input_tokens=int(usage.get("prompt_tokens", 0)),
            output_tokens=int(usage.get("completion_tokens", 0)),
            # money is derived from the provider's own token counts at the
            # documented conservative rate: a KNOWN value, not a guess
            micro_cny=self.ledger.estimate_micro_cny(
                input_tokens=int(usage.get("prompt_tokens", 0)),
                output_tokens=int(usage.get("completion_tokens", 0)),
            ),
        )
        status = {
            "calls": UsageStatus.KNOWN.value,
            "input_tokens": UsageStatus.KNOWN.value,
            "output_tokens": UsageStatus.KNOWN.value,
            # the model itself cannot know about tool/storage/wall accounting
            "tool_calls": UsageStatus.NEVER_STARTED.value,
            "storage_bytes": UsageStatus.NEVER_STARTED.value,
            "wall_seconds": UsageStatus.NEVER_STARTED.value,
            "micro_cny": UsageStatus.KNOWN.value,
        }
        self.ledger.settle(
            reservation_id,
            UsageReport(status=status, actual=actual),
            reconciled_by="worker-loop",
        )

    # ------------------------------------------------------------------ loop
    def run(self, objective: str, *, context: str = "") -> WorkerOutcome:
        started_at = datetime.now(timezone.utc).isoformat()
        started_clock = self.clock()
        tools = mcp_tools_to_function_schemas(self._discovered)
        messages: list[dict[str, Any]] = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {
                "role": "user",
                "content": (
                    f"Objective: {objective}\n"
                    + (f"Context you were given:\n{context}\n" if context else "")
                    + "Use the tools to do the work, then summarise briefly."
                ),
            },
        ]
        outcome = WorkerOutcome(
            status="BLOCKED",
            summary="the Worker loop did not reach a conclusion",
            steps=0,
            tool_calls=0,
            started_at=started_at,
        )
        for step in range(1, self.max_steps + 1):
            outcome.steps = step
            blocked_by_budget = False
            if self.clock() - started_clock > self.max_seconds:
                outcome.status = "BLOCKED"
                outcome.summary = "BLOCKED: the Worker deadline elapsed"
                break
            reservation_id = self._reserve(
                estimated_input=self.provider.estimate_input_tokens(messages)
                + self.provider.estimate_input_tokens([{"tools": tools}])
            )
            outcome.reservation_ids.append(reservation_id)
            try:
                completion = self.provider.complete(
                    messages,
                    tools=tools,
                    max_tokens=self.max_completion_tokens,
                )
                # the call happened: record it before any settlement can fail
                outcome.live_calls += 1
            except ProviderError as exc:
                if exc.outcome == "OUTCOME_UNKNOWN":
                    outcome.unknown_outcomes += 1
                    self.ledger.settle(
                        reservation_id,
                        self._unknown_report(),
                        reconciled_by="worker-loop",
                    )
                    outcome.status = "OUTCOME_UNKNOWN"
                    outcome.summary = f"OUTCOME_UNKNOWN: {exc}"
                    break
                self.ledger.release_never_started(
                    reservation_id, evidence=f"provider refused: {exc.kind}"
                )
                outcome.status = "BLOCKED"
                outcome.summary = f"BLOCKED: {exc}"
                break
            outcome.identity = self.provider.enrich_identity(completion).to_dict()
            for key, value in completion.usage.items():
                outcome.usage[key] = outcome.usage.get(key, 0) + int(value)
            self._settle(reservation_id, completion.usage, calls=1)

            if not completion.tool_calls:
                text = completion.content.strip()
                if text.startswith("BLOCKED:"):
                    outcome.status = "BLOCKED"
                    outcome.summary = text
                else:
                    outcome.status = "WORKER_COMPLETE"
                    outcome.summary = text or "(no summary)"
                break
            messages.append(
                {
                    "role": "assistant",
                    "content": completion.content,
                    "tool_calls": [
                        {
                            "id": call.id,
                            "type": "function",
                            "function": {
                                "name": call.name,
                                "arguments": call.raw_arguments,
                            },
                        }
                        for call in completion.tool_calls
                    ],
                }
            )
            for call in completion.tool_calls:
                outcome.tool_calls += 1
                # a tool invocation is a physical effect of its own: it is charged
                # to the scope before it is dispatched, so the scope's tool-call
                # ceiling is a real bound rather than a declared dimension
                try:
                    tool_reservation = self._reserve_tool_call(call.name)
                except BudgetDenied as exc:
                    outcome.status = "BLOCKED"
                    outcome.summary = (
                        f"BLOCKED: the budget ledger refused tool call {call.name}: {exc}"
                    )
                    outcome.notes.append(f"tool call {call.name} was not dispatched")
                    blocked_by_budget = True
                    break
                try:
                    result = self._execute_tool(call.name, call.arguments)
                except V1Error as exc:
                    result = ToolResult(
                        ok=False,
                        action=call.name,
                        operation_id=None,
                        status="NOT_DISPATCHED",
                        output={"error": str(exc)},
                        output_bytes=0,
                        truncated=False,
                        output_digest="",
                        error_code=getattr(exc, "code", "ERROR"),
                        started_at="",
                        finished_at="",
                        duration_ms=0.0,
                    )
                finally:
                    self._settle_tool_call(tool_reservation)
                trace = ToolTrace(
                    step=step,
                    name=call.name,
                    arguments=call.arguments,
                    ok=result.ok,
                    status=result.status,
                    error_code=result.error_code,
                    output_digest=result.output_digest,
                    duration_ms=result.duration_ms,
                )
                outcome.traces.append(trace)
                if (
                    self._receipt_recorder is not None
                    and call.name == "test_run"
                    and result.ok
                    and isinstance(result.output, Mapping)
                    and isinstance(result.output.get("receipt"), Mapping)
                ):
                    # an executor profile run is durable evidence: hand it to the
                    # Orchestrator so the Manager can review a real receipt id.
                    # When the executor already persisted the receipt itself, its
                    # own id is used, so one run never becomes two receipts.
                    try:
                        durable_id = result.output.get("receipt_id")
                        if isinstance(durable_id, str) and durable_id:
                            receipt_id = durable_id
                        else:
                            receipt_id = self._receipt_recorder(
                                {
                                    "tool": result.to_dict(),
                                    "receipt": dict(result.output["receipt"]),
                                    "step": step,
                                    "at": datetime.now(timezone.utc).isoformat(),
                                }
                            )
                        outcome.receipt_ids.append(str(receipt_id))
                    except Exception as exc:  # noqa: BLE001 - recording is best effort
                        outcome.notes.append(
                            f"receipt recording failed: {type(exc).__name__}"
                        )
                messages.append(
                    {
                        "role": "tool",
                        "tool_call_id": call.id,
                        "content": json.dumps(result.to_dict(), ensure_ascii=False, default=str),
                    }
                )
            if blocked_by_budget:
                # the scope is spent: asking the model for another step would only
                # spend more of a budget that has already refused this run
                break
        else:
            outcome.status = "BLOCKED"
            outcome.summary = f"BLOCKED: the Worker used its {self.max_steps} step budget"
        outcome.finished_at = datetime.now(timezone.utc).isoformat()
        outcome.duration_ms = round((self.clock() - started_clock) * 1000.0, 3)
        return outcome
