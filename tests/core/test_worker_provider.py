"""Provider parsing and Worker-loop tests, with no live model call.

A fake HTTP transport supplies real-shaped response bodies, so the strict parsing
rules can be exercised for free: an empty content with tool calls must be accepted,
malformed arguments must be refused, a substituted model must be refused, and a
transport failure must be OUTCOME_UNKNOWN rather than a silent retry.
"""

from __future__ import annotations

import json

import httpx
import pytest

from kvflow.credentials import CredentialStatus
from kvflow.core.budget import BudgetLedger, Charge
from kvflow.core.contracts import BudgetScope
from kvflow.core.errors import ContractError, ProviderError
from kvflow.core.providers import DeepSeekProvider
from kvflow.core.store import Store
from kvflow.core.worker import WorkerLoop, mcp_tools_to_function_schemas

from .conftest import AUTH, deadline_in


def provider_with(body: dict | None = None, *, status: int = 200, exc: Exception | None = None):
    def handler(request: httpx.Request) -> httpx.Response:
        if exc is not None:
            raise exc
        return httpx.Response(status, json=body or {})

    client = httpx.Client(transport=httpx.MockTransport(handler))
    return DeepSeekProvider(
        client=client,
        credential="FAKE_SENTINEL_FOR_TESTS",
        credential_status=CredentialStatus(source="test", available=True),
    ), client


def reply(content: str | None = None, tool_calls: list | None = None, **extra) -> dict:
    message: dict = {"role": "assistant", "content": content}
    if tool_calls is not None:
        message["tool_calls"] = tool_calls
    body = {
        "model": "deepseek-flash",
        "choices": [{"index": 0, "message": message, "finish_reason": extra.pop("finish", "stop")}],
        "usage": extra.pop("usage", {"prompt_tokens": 10, "completion_tokens": 4, "total_tokens": 14}),
    }
    body.update(extra)
    return body


# ------------------------------------------------------------------ parsing


def test_tool_calls_with_empty_content_are_accepted():
    provider, client = provider_with(
        reply(None, [{"id": "c1", "function": {"name": "repo_read", "arguments": '{"path": "a"}'}}],
              finish="tool_calls")
    )
    completion = provider.complete([{"role": "user", "content": "hi"}])
    client.close()
    assert completion.content == ""
    assert completion.has_output is True
    assert completion.tool_calls[0].name == "repo_read"
    assert completion.tool_calls[0].arguments == {"path": "a"}
    assert completion.finish_reason == "tool_calls"


def test_tool_arguments_may_arrive_as_an_object():
    provider, client = provider_with(
        reply(None, [{"id": "c1", "function": {"name": "repo_read",
                                              "arguments": {"path": "a"}}}])
    )
    completion = provider.complete([{"role": "user", "content": "hi"}])
    client.close()
    assert completion.tool_calls[0].arguments == {"path": "a"}


def test_invalid_json_arguments_are_refused():
    provider, client = provider_with(
        reply(None, [{"id": "c1", "function": {"name": "repo_read", "arguments": "{not json"}}])
    )
    with pytest.raises(ProviderError) as excinfo:
        provider.complete([{"role": "user", "content": "hi"}])
    client.close()
    assert excinfo.value.outcome == "OUTCOME_UNKNOWN"


def test_neither_content_nor_tool_calls_is_a_failure():
    provider, client = provider_with(reply(None, None))
    with pytest.raises(ProviderError):
        provider.complete([{"role": "user", "content": "hi"}])
    client.close()


def test_a_substituted_model_is_refused():
    provider, client = provider_with(reply("hello", None, model="some-other-model"))
    with pytest.raises(ProviderError) as excinfo:
        provider.complete([{"role": "user", "content": "hi"}])
    client.close()
    assert excinfo.value.kind == "identity_mismatch"


def test_provider_returned_model_is_reported_separately():
    provider, client = provider_with(reply("hello", None))
    completion = provider.complete([{"role": "user", "content": "hi"}])
    identity = provider.enrich_identity(completion)
    client.close()
    assert identity.provider_returned_model == "deepseek-flash"
    assert identity.verification_level == "PROVIDER_MODEL"
    assert identity.requested_model == "deepseek-flash"


def test_usage_is_recorded_verbatim_including_cache_split():
    provider, client = provider_with(
        reply("hi", None, usage={"prompt_tokens": 100, "completion_tokens": 20,
                                 "total_tokens": 120, "prompt_cache_hit_tokens": 60,
                                 "prompt_cache_miss_tokens": 40})
    )
    completion = provider.complete([{"role": "user", "content": "hi"}])
    client.close()
    assert completion.usage["prompt_cache_hit_tokens"] == 60
    assert completion.usage["prompt_cache_miss_tokens"] == 40


def test_a_5xx_is_unknown_and_a_429_is_not_dispatched():
    provider, client = provider_with({}, status=503)
    with pytest.raises(ProviderError) as excinfo:
        provider.complete([{"role": "user", "content": "hi"}])
    client.close()
    assert excinfo.value.outcome == "OUTCOME_UNKNOWN"

    provider, client = provider_with({}, status=429)
    with pytest.raises(ProviderError) as excinfo:
        provider.complete([{"role": "user", "content": "hi"}])
    client.close()
    assert excinfo.value.outcome == "NOT_DISPATCHED"


def test_a_transport_error_is_unknown_not_a_retry():
    provider, client = provider_with(exc=httpx.ConnectTimeout("no route"))
    with pytest.raises(ProviderError) as excinfo:
        provider.complete([{"role": "user", "content": "hi"}])
    client.close()
    assert excinfo.value.outcome == "OUTCOME_UNKNOWN"
    assert excinfo.value.kind == "transport"


def test_token_ceilings_are_enforced_before_dispatch():
    provider, client = provider_with(reply("hi", None))
    with pytest.raises(ContractError):
        provider.complete([{"role": "user", "content": "x" * 5000}], input_token_ceiling=10)
    with pytest.raises(ContractError):
        provider.complete([{"role": "user", "content": "hi"}], max_tokens=999_999)
    client.close()


def test_identity_stays_not_exposed_without_a_credential():
    provider = DeepSeekProvider(
        credential=None,
        credential_status=CredentialStatus(source="test", available=False),
    )
    identity = provider.identity()
    assert identity.provider_returned_model == "NOT_EXPOSED"
    assert identity.verification_level == "CONFIGURED_ONLY"


# --------------------------------------------------------------- tool mapping


def test_mcp_tools_map_onto_function_schemas():
    schemas = mcp_tools_to_function_schemas(
        [
            {
                "name": "repo_read",
                "description": "read one file",
                "inputSchema": {"type": "object", "properties": {"path": {"type": "string"}}},
            }
        ]
    )
    assert schemas[0]["type"] == "function"
    assert schemas[0]["function"]["name"] == "repo_read"
    assert "path" in schemas[0]["function"]["parameters"]["properties"]


def test_a_tool_without_a_schema_is_a_hard_error():
    with pytest.raises(ContractError):
        mcp_tools_to_function_schemas([{"name": "repo_read"}])


# ---------------------------------------------------------------- worker loop


class FakeToolResult:
    def __init__(self, ok: bool, name: str, payload: dict):
        self.ok = ok
        self.action = name
        self.operation_id = "op_test"
        self.status = "SUCCEEDED" if ok else "FAILED"
        self.output = payload
        self.output_bytes = len(json.dumps(payload))
        self.truncated = False
        self.output_digest = "d" * 64
        self.error_code = None if ok else "PATH_DENIED"
        self.started_at = ""
        self.finished_at = ""
        self.duration_ms = 1.0
        self.audit: dict = {}

    def to_dict(self):
        return {"ok": self.ok, "action": self.action, "output": self.output,
                "output_digest": self.output_digest, "status": self.status}


class SequencedProvider:
    """Returns prepared completions in order and records every request payload."""

    def __init__(self, completions):
        self._completions = list(completions)
        self.requests: list[dict] = []

    def estimate_input_tokens(self, messages) -> int:
        return 1_000

    def complete(self, messages, **kwargs):
        self.requests.append({"messages": [dict(m) for m in messages], **kwargs})
        if not self._completions:
            raise ProviderError("no more scripted responses", outcome="NOT_DISPATCHED")
        return self._completions.pop(0)

    def enrich_identity(self, completion):
        from kvflow.core.providers import ProviderIdentity

        return ProviderIdentity(
            display_name="DeepSeek V4.1 Flash",
            requested_model="deepseek-flash",
            requested_effort="NOT_EXPOSED",
            provider_returned_model=completion.provider_model or "deepseek-flash",
            verification_level="PROVIDER_MODEL",
        )


class ScriptedCompletion:
    def __init__(self, content="", tool_calls=(), usage=None, provider_model="deepseek-flash"):
        from kvflow.core.providers import ToolCall

        self.content = content
        self.tool_calls = [
            ToolCall(id=i, name=n, arguments=a, raw_arguments=json.dumps(a))
            for i, n, a in tool_calls
        ]
        self.finish_reason = "tool_calls" if self.tool_calls else "stop"
        self.provider_model = provider_model
        self.usage = usage if usage is not None else {"prompt_tokens": 100, "completion_tokens": 20}
        self.reasoning_present = False


@pytest.fixture()
def ledger(tmp_path):
    store = Store(tmp_path / "worker.sqlite3")
    store.initialize()
    ledger = BudgetLedger(store)
    ledger.register_scope(
        BudgetScope(
            scope_id="scope",
            scope_kind="GLOBAL",
            calls=20,
            input_tokens=200_000,
            output_tokens=100_000,
            tool_calls=100,
            storage_bytes=1 << 20,
            wall_seconds=3600,
            micro_cny=5_000_000,
            concurrency=4,
            deadline=deadline_in(3600),
            authorization_digest=AUTH,
        )
    )
    return ledger


BINDING = {
    "project_id": "p",
    "job_id": "j",
    "node_id": "n",
    "run_id": "r",
    "attempt": 1,
    "fence": 1,
}

DISCOVERED = [
    {"name": "repo_read", "description": "r", "inputSchema": {"type": "object", "properties": {}}},
    {"name": "repo_patch", "description": "p", "inputSchema": {"type": "object", "properties": {}}},
    {"name": "test_run", "description": "t", "inputSchema": {"type": "object", "properties": {}}},
]


def make_loop(provider, ledger, executor, **overrides):
    return WorkerLoop(
        provider=provider,
        tools=None,
        ledger=ledger,
        ticket="ticket",
        binding=BINDING,
        scope_id="scope",
        discovered_tools=overrides.pop("discovered", DISCOVERED),
        max_steps=overrides.pop("max_steps", 6),
        max_completion_tokens=256,
        tool_executor=executor,
        **overrides,
    )


def test_a_durable_test_receipt_is_reused_instead_of_re_recorded(ledger):
    """One executor run must produce exactly one receipt, not two."""
    recorded: list[dict] = []

    def executor(name, arguments):
        return FakeToolResult(
            True,
            name,
            {"receipt": {"exit_code": 0, "profile_id": "unit"}, "receipt_id": "rcpt_abc"},
        )

    def recorder(payload):
        recorded.append(payload)
        return "rcpt_should_not_be_used"

    provider = SequencedProvider(
        [
            ScriptedCompletion(tool_calls=[("c1", "test_run", {"profile_id": "unit"})]),
            ScriptedCompletion(content="Done: tested."),
        ]
    )
    outcome = make_loop(provider, ledger, executor, receipt_recorder=recorder).run("test it")
    assert outcome.status == "WORKER_COMPLETE"
    assert outcome.receipt_ids == ["rcpt_abc"]
    assert recorded == []


def test_a_receipt_without_a_durable_id_is_recorded_by_the_hook(ledger):
    recorded: list[dict] = []

    def executor(name, arguments):
        return FakeToolResult(
            True, name, {"receipt": {"exit_code": 0, "profile_id": "unit"}}
        )

    def recorder(payload):
        recorded.append(payload)
        return "rcpt_hook"

    provider = SequencedProvider(
        [
            ScriptedCompletion(tool_calls=[("c1", "test_run", {"profile_id": "unit"})]),
            ScriptedCompletion(content="Done: tested."),
        ]
    )
    outcome = make_loop(provider, ledger, executor, receipt_recorder=recorder).run("test it")
    assert outcome.receipt_ids == ["rcpt_hook"]
    assert len(recorded) == 1


def test_the_loop_charges_every_tool_call_to_the_scope(ledger):
    """The scope's tool-call ceiling must be a real bound, not a label."""
    executed: list[str] = []

    def executor(name, arguments):
        executed.append(name)
        return FakeToolResult(True, name, {"ok": True})

    provider = SequencedProvider(
        [
            ScriptedCompletion(tool_calls=[("c1", "repo_read", {}), ("c2", "repo_patch", {})]),
            ScriptedCompletion(content="done"),
        ]
    )
    outcome = make_loop(provider, ledger, executor).run("charge me")
    usage = ledger.usage("scope")["used"]
    assert outcome.tool_calls == 2
    assert usage["tool_calls"] == 2
    assert usage["calls"] == 2  # two model calls, not four effects
    assert executed == ["repo_read", "repo_patch"]
    assert not [
        row for row in ledger.store.pending_operations() if row["operation_id"].startswith("res_")
    ]


def test_a_spent_tool_budget_blocks_the_loop_instead_of_dispatching(ledger):
    executed: list[str] = []

    def executor(name, arguments):
        executed.append(name)
        return FakeToolResult(True, name, {"ok": True})

    # spend the whole tool-call ceiling on the scripted model calls first
    from kvflow.core.budget import Charge, ReservationRequest, UsageReport, new_effect_id

    for _ in range(100):
        admission = ledger.reserve(
            ReservationRequest(
                scope_id="scope", project_id="p", job_id="j", node_id="n", run_id="r",
                attempt=1, fence=1, effect_id=new_effect_id("tool-call"),
                purpose="pre-spend", charge=Charge(tool_calls=1),
            )
        )
        ledger.settle(
            admission.reservation_id,
            UsageReport.known(Charge(tool_calls=1)),
            reconciled_by="test",
        )

    provider = SequencedProvider(
        [ScriptedCompletion(tool_calls=[("c1", "repo_read", {})])]
    )
    outcome = make_loop(provider, ledger, executor).run("should be refused")
    assert outcome.status == "BLOCKED"
    assert "refused tool call" in outcome.summary
    assert executed == []
    assert outcome.tool_calls == 1
    assert ledger.usage("scope")["used"]["tool_calls"] == 100


def test_the_loop_executes_tool_calls_and_finishes(ledger):
    calls: list[tuple[str, dict]] = []

    def executor(name, arguments):
        calls.append((name, arguments))
        return FakeToolResult(True, name, {"path": arguments.get("path", "?")})

    provider = SequencedProvider(
        [
            ScriptedCompletion(tool_calls=[("c1", "repo_read", {"path": "src/calc.py"})]),
            ScriptedCompletion(tool_calls=[("c2", "test_run", {"profile_id": "unit"})]),
            ScriptedCompletion(content="Done: read and tested."),
        ]
    )
    outcome = make_loop(provider, ledger, executor).run("do the thing")
    assert outcome.status == "WORKER_COMPLETE"
    assert outcome.summary.startswith("Done")
    assert outcome.tool_calls == 2
    assert outcome.live_calls == 3
    assert [name for name, _ in calls] == ["repo_read", "test_run"]


def test_the_second_request_carries_the_tool_result(ledger):
    def executor(name, arguments):
        return FakeToolResult(True, name, {"value": 42})

    provider = SequencedProvider(
        [
            ScriptedCompletion(tool_calls=[("c1", "repo_read", {"path": "a"})]),
            ScriptedCompletion(content="ok"),
        ]
    )
    make_loop(provider, ledger, executor).run("objective")
    second = provider.requests[1]["messages"]
    roles = [message["role"] for message in second]
    assert roles[:2] == ["system", "user"]
    assert "assistant" in roles and "tool" in roles
    tool_message = next(m for m in second if m["role"] == "tool")
    assert tool_message["tool_call_id"] == "c1"
    assert json.loads(tool_message["content"])["output"]["value"] == 42


def test_a_refused_tool_is_reported_but_does_not_crash_the_loop(ledger):
    def executor(name, arguments):
        return FakeToolResult(False, name, {"error": "denied"})

    provider = SequencedProvider(
        [
            ScriptedCompletion(tool_calls=[("c1", "repo_patch", {"path": "../escape"})]),
            ScriptedCompletion(content="I could not patch that path."),
        ]
    )
    outcome = make_loop(provider, ledger, executor).run("objective")
    assert outcome.status == "WORKER_COMPLETE"
    assert outcome.traces[0].ok is False
    assert outcome.traces[0].error_code == "PATH_DENIED"


def test_a_blocked_reply_is_reported_as_blocked(ledger):
    provider = SequencedProvider([ScriptedCompletion(content="BLOCKED: the file is absent")])
    outcome = make_loop(provider, ledger, lambda n, a: FakeToolResult(True, n, {})).run("objective")
    assert outcome.status == "BLOCKED"
    assert outcome.summary.startswith("BLOCKED:")


def test_the_step_budget_is_enforced(ledger):
    def executor(name, arguments):
        return FakeToolResult(True, name, {})

    provider = SequencedProvider(
        [ScriptedCompletion(tool_calls=[(f"c{i}", "repo_read", {})]) for i in range(10)]
    )
    outcome = make_loop(provider, ledger, executor, max_steps=3).run("objective")
    assert outcome.status == "BLOCKED"
    assert "step budget" in outcome.summary
    assert outcome.live_calls == 3


def test_the_wall_clock_budget_is_enforced(ledger):
    clock = {"t": 0.0}

    def now() -> float:
        clock["t"] += 10.0
        return clock["t"]

    provider = SequencedProvider([ScriptedCompletion(content="ok")])
    outcome = make_loop(
        provider, ledger, lambda n, a: FakeToolResult(True, n, {}),
        max_seconds=5.0, clock=now,
    ).run("objective")
    assert outcome.status == "BLOCKED"
    assert "deadline" in outcome.summary


def test_every_call_is_reserved_and_settled(ledger):
    provider = SequencedProvider([ScriptedCompletion(content="done")])
    outcome = make_loop(provider, ledger, lambda n, a: FakeToolResult(True, n, {})).run("objective")
    assert len(outcome.reservation_ids) == 1
    usage = ledger.usage("scope")
    assert usage["pending_reservations"] == 0
    assert usage["used"]["calls"] == 1
    # 100 prompt + 20 completion tokens at 3/12 micro-CNY per million
    assert usage["used"]["micro_cny"] == 540


def test_absent_usage_keeps_the_hold_and_is_unknown(ledger):
    completion = ScriptedCompletion(content="done", usage={})
    provider = SequencedProvider([completion])
    outcome = make_loop(provider, ledger, lambda n, a: FakeToolResult(True, n, {})).run("objective")
    usage = ledger.usage("scope")
    assert usage["unknown_reservations"] == 1
    assert usage["used"]["calls"] == 1, "an unreported call is never refunded"
    assert outcome.usage == {}


def test_a_transport_failure_is_unknown_and_not_retried(ledger):
    class Failing:
        def estimate_input_tokens(self, messages):
            return 100

        def complete(self, messages, **kwargs):
            raise ProviderError("no route", outcome="OUTCOME_UNKNOWN", kind="transport")

        def enrich_identity(self, completion):  # pragma: no cover - never reached
            raise AssertionError

    outcome = make_loop(Failing(), ledger, lambda n, a: FakeToolResult(True, n, {})).run("obj")
    assert outcome.status == "OUTCOME_UNKNOWN"
    assert outcome.unknown_outcomes == 1
    assert outcome.live_calls == 0
    assert ledger.usage("scope")["unknown_reservations"] == 1


def test_a_refused_dispatch_releases_the_reservation(ledger):
    class Refusing:
        def estimate_input_tokens(self, messages):
            return 100

        def complete(self, messages, **kwargs):
            raise ProviderError("rate limited", outcome="NOT_DISPATCHED", kind="rate_limited")

        def enrich_identity(self, completion):  # pragma: no cover
            raise AssertionError

    outcome = make_loop(Refusing(), ledger, lambda n, a: FakeToolResult(True, n, {})).run("obj")
    assert outcome.status == "BLOCKED"
    usage = ledger.usage("scope")
    assert usage["used"]["calls"] == 0
    assert usage["pending_reservations"] == 0


def test_the_worker_only_keeps_its_own_tools(ledger):
    provider = SequencedProvider([ScriptedCompletion(content="done")])
    loop = make_loop(
        provider, ledger, lambda n, a: FakeToolResult(True, n, {}),
        discovered=[*DISCOVERED, {"name": "knowledge_search", "inputSchema": {"type": "object"}}],
    )
    assert [tool["name"] for tool in loop._discovered] == ["repo_read", "repo_patch", "test_run"]


def test_a_loop_with_no_usable_tool_is_refused(ledger):
    with pytest.raises(ContractError):
        make_loop(
            SequencedProvider([]), ledger, lambda n, a: FakeToolResult(True, n, {}),
            discovered=[{"name": "knowledge_search", "inputSchema": {"type": "object"}}],
        )
