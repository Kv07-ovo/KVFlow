"""Real model providers: DeepSeek V4.1 Flash Worker and Codex Astra Manager.

The provider speaks the vendor's real interface - the DeepSeek chat completions
endpoint with native ``tools``/``tool_calls`` - and never rewrites identity. A
command-line argument is never reported as a provider-confirmed field: the
provider-returned model is whatever the response body actually says, and it is
left ``NOT_EXPOSED`` when the provider does not return one.

Budget rules implemented here
-----------------------------

* The caller must supply a *reservation id* that is already held by the budget
  ledger; this module never spends money on its own initiative.
* Input and output token ceilings are enforced before the request is sent.
* Usage reported by the provider is returned verbatim so the ledger can settle
  KNOWN dimensions; if the provider omits usage, it stays unknown.
* An empty ``content`` with ``tool_calls`` is a legal, complete response.
* Credentials are read through the existing adapter path and are never logged,
  returned, hashed or placed in an error message.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence

import httpx

from ..credentials import CredentialStatus, deepseek_credential
from .errors import AuthorityDenied, ConfigError, ContractError, ProviderError

DEFAULT_MODEL = "deepseek-flash"
DEFAULT_BASE_URL = "https://api.deepseek.com"
#: DeepSeek V4.1 Flash supports a thinking mode that must be able to finish
MAX_COMPLETION_TOKENS = 32_768
DEFAULT_COMPLETION_TOKENS = 8_192


@dataclass(frozen=True)
class ProviderIdentity:
    """Configured versus provider-observed identity, reported separately."""

    display_name: str
    requested_model: str
    requested_effort: str
    provider_returned_model: str = "NOT_EXPOSED"
    provider_returned_effort: str = "NOT_EXPOSED"
    verification_level: str = "CONFIGURED_ONLY"

    def to_dict(self) -> dict[str, Any]:
        return {
            "display_name": self.display_name,
            "requested_model": self.requested_model,
            "requested_effort": self.requested_effort,
            "provider_returned_model": self.provider_returned_model,
            "provider_returned_effort": self.provider_returned_effort,
            "verification_level": self.verification_level,
        }


@dataclass
class ToolCall:
    id: str
    name: str
    arguments: dict[str, Any]
    raw_arguments: str


@dataclass
class Completion:
    """One provider response, parsed strictly and never silently reinterpreted."""

    content: str
    tool_calls: list[ToolCall]
    finish_reason: str
    provider_model: str
    usage: dict[str, int] = field(default_factory=dict)
    reasoning_present: bool = False

    @property
    def has_output(self) -> bool:
        """An empty content with tool calls is a complete, legal response."""
        return bool(self.content) or bool(self.tool_calls)


class DeepSeekProvider:
    """Minimal, strict client for the DeepSeek chat completions endpoint."""

    def __init__(
        self,
        *,
        model: str = DEFAULT_MODEL,
        base_url: str = DEFAULT_BASE_URL,
        timeout_seconds: float = 120.0,
        client: httpx.Client | None = None,
        credential: str | None = None,
        credential_status: CredentialStatus | None = None,
    ) -> None:
        self.model = model
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = float(timeout_seconds)
        self._client = client
        self._credential = credential
        self._status = credential_status

    # ------------------------------------------------------------- identity
    def credential_status(self) -> CredentialStatus:
        if self._status is None:
            secret, status = deepseek_credential()
            self._credential = secret
            self._status = status
        return self._status

    def identity(self) -> ProviderIdentity:
        status = self.credential_status()
        return ProviderIdentity(
            display_name="DeepSeek V4.1 Flash",
            requested_model=self.model,
            requested_effort="NOT_EXPOSED",
            verification_level="CONFIGURED_ONLY" if status.available else "CONFIGURED_ONLY",
        )

    # ---------------------------------------------------------------- budget
    @staticmethod
    def estimate_input_tokens(messages: Sequence[Mapping[str, Any]]) -> int:
        """A conservative upper bound: UTF-8 bytes of the serialized request."""
        blob = json.dumps(list(messages), ensure_ascii=False, sort_keys=True, default=str)
        return len(blob.encode("utf-8"))

    # -------------------------------------------------------------- dispatch
    def complete(
        self,
        messages: Sequence[Mapping[str, Any]],
        *,
        tools: Sequence[Mapping[str, Any]] | None = None,
        max_tokens: int = DEFAULT_COMPLETION_TOKENS,
        temperature: float = 0.0,
        input_token_ceiling: int | None = None,
        base_url: str | None = None,
    ) -> Completion:
        """One real request. Never retried here; the caller owns retry policy."""
        status = self.credential_status()
        if not status.available or not self._credential:
            raise AuthorityDenied(
                "no DeepSeek credential is available to the provider",
                source=status.source,
            )
        if isinstance(max_tokens, bool) or not isinstance(max_tokens, int):
            raise ContractError("max_tokens must be an integer")
        if max_tokens < 1 or max_tokens > MAX_COMPLETION_TOKENS:
            raise ContractError("max_tokens is out of range", max_tokens=max_tokens)
        estimated = self.estimate_input_tokens(messages)
        if input_token_ceiling is not None and estimated > int(input_token_ceiling):
            raise ContractError(
                "the request would exceed the reserved input budget",
                estimated_bytes=estimated,
                ceiling=int(input_token_ceiling),
            )
        payload: dict[str, Any] = {
            "model": self.model,
            "messages": [dict(message) for message in messages],
            "max_tokens": max_tokens,
            "temperature": temperature,
            "stream": False,
        }
        if tools:
            payload["tools"] = [dict(tool) for tool in tools]
            payload["tool_choice"] = "auto"
        url = f"{(base_url or self.base_url).rstrip('/')}/chat/completions"
        headers = {
            "Authorization": f"Bearer {self._credential}",
            "Content-Type": "application/json",
        }
        client = self._client or httpx.Client(timeout=self.timeout_seconds)
        owns_client = self._client is None
        try:
            response = client.post(url, json=payload, headers=headers)
        except httpx.HTTPError as exc:
            # a transport failure is genuinely unknown: no automatic retry here
            raise ProviderError(
                "the provider request did not complete", outcome="OUTCOME_UNKNOWN",
                kind="transport",
            ) from exc
        finally:
            if owns_client:
                client.close()
        if response.status_code >= 500:
            raise ProviderError(
                "the provider returned a server error",
                outcome="OUTCOME_UNKNOWN",
                kind="provider_5xx",
                status_code=response.status_code,
            )
        if response.status_code == 429:
            raise ProviderError(
                "the provider rate limited the request",
                outcome="NOT_DISPATCHED",
                kind="rate_limited",
                status_code=429,
            )
        if response.status_code != 200:
            raise ProviderError(
                "the provider rejected the request",
                outcome="NOT_DISPATCHED",
                kind="provider_4xx",
                status_code=response.status_code,
            )
        try:
            body = response.json()
        except ValueError as exc:
            raise ProviderError(
                "the provider response was not JSON", outcome="OUTCOME_UNKNOWN"
            ) from exc
        return self._parse(body)

    def _parse(self, body: Mapping[str, Any]) -> Completion:
        if not isinstance(body, Mapping):
            raise ProviderError("the provider response was not an object")
        choices = body.get("choices")
        if not isinstance(choices, list) or not choices:
            raise ProviderError("the provider returned no choices")
        choice = choices[0]
        message = choice.get("message") if isinstance(choice, Mapping) else None
        if not isinstance(message, Mapping):
            raise ProviderError("the provider returned no message object")
        content = message.get("content")
        if content is not None and not isinstance(content, str):
            raise ProviderError("the provider returned a non-text content field")
        calls: list[ToolCall] = []
        raw_calls = message.get("tool_calls")
        if raw_calls is not None:
            if not isinstance(raw_calls, list):
                raise ProviderError("the provider returned a malformed tool_calls field")
            for entry in raw_calls:
                if not isinstance(entry, Mapping):
                    raise ProviderError("a tool call was not an object")
                function = entry.get("function")
                if not isinstance(function, Mapping):
                    raise ProviderError("a tool call had no function object")
                name = function.get("name")
                if not isinstance(name, str) or not name:
                    raise ProviderError("a tool call had no function name")
                arguments = function.get("arguments")
                if isinstance(arguments, Mapping):
                    parsed = dict(arguments)
                    raw = json.dumps(arguments, ensure_ascii=False)
                elif isinstance(arguments, str):
                    raw = arguments
                    try:
                        parsed = json.loads(arguments or "{}")
                    except ValueError as exc:
                        raise ProviderError(
                            "a tool call carried invalid JSON arguments", tool=name
                        ) from exc
                else:
                    raise ProviderError("a tool call carried unusable arguments", tool=name)
                if not isinstance(parsed, dict):
                    raise ProviderError("tool arguments were not an object", tool=name)
                calls.append(
                    ToolCall(
                        id=str(entry.get("id") or f"call_{len(calls)}"),
                        name=name,
                        arguments=parsed,
                        raw_arguments=raw,
                    )
                )
        usage = body.get("usage") if isinstance(body.get("usage"), Mapping) else {}
        reported: dict[str, int] = {}
        for key in (
            "prompt_tokens",
            "completion_tokens",
            "total_tokens",
            "prompt_cache_hit_tokens",
            "prompt_cache_miss_tokens",
        ):
            value = usage.get(key) if isinstance(usage, Mapping) else None
            if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
                reported[key] = value
        provider_model = body.get("model")
        completion = Completion(
            content=content or "",
            tool_calls=calls,
            finish_reason=str(choice.get("finish_reason") or "unknown"),
            provider_model=str(provider_model) if isinstance(provider_model, str) else "",
            usage=reported,
            reasoning_present=bool(message.get("reasoning_content")),
        )
        if not completion.has_output:
            raise ProviderError(
                "the provider returned neither content nor tool calls",
                finish_reason=completion.finish_reason,
            )
        if (
            completion.provider_model
            and completion.provider_model != self.model
            and self.model not in completion.provider_model
            and completion.provider_model not in self.model
        ):
            # a silent model substitution is refused rather than billed as ours
            raise ProviderError(
                "the provider reported a different model identity",
                requested=self.model,
                returned=completion.provider_model,
                kind="identity_mismatch",
            )
        return completion

    def enrich_identity(self, completion: Completion) -> ProviderIdentity:
        if not completion.provider_model:
            return self.identity()
        return ProviderIdentity(
            display_name="DeepSeek V4.1 Flash",
            requested_model=self.model,
            requested_effort="NOT_EXPOSED",
            provider_returned_model=completion.provider_model,
            provider_returned_effort="NOT_EXPOSED",
            verification_level="PROVIDER_MODEL",
        )
