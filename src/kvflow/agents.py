"""Agent callers: the manager session and the worker provider, chosen by profile.

The core knows roles; this module is where a *model* becomes a role. It builds:

* a manager caller for the configured adapter -- the local Codex CLI for the
  ``hybrid`` profile, or the DeepSeek endpoint itself for ``deepseek_only``, so a
  machine with one credential still completes the whole loop;
* a worker provider (DeepSeek) for the implementation role;
* an availability report that says which adapters really work here, without
  making a paid call.

Nothing here rewrites identity: a model name that an interface does not return is
reported as ``NOT_EXPOSED``, and a manager call on the Codex account is recorded
with cost ``UNKNOWN`` rather than zero.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path
from typing import Any, Callable, Mapping

from .core.errors import ConfigError, ProviderError
from .core.manager import Manager
from .core.providers import DeepSeekProvider
from .model_profiles import ModelProfile, RoleModel

#: variables forwarded to the manager child only: the authenticated CLI needs
#: its endpoint. Test and git children never inherit them.
PROXY_VARIABLES = (
    "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY",
    "WINHTTP_PROXY", "WINHTTPS_PROXY",
)

#: a manager child must not inherit any model credential of ours
STRIPPED_FROM_MANAGER = (
    "DEEPSEEK_API_KEY", "OPENAI_API_KEY", "KVFLOW_MCP_CAPABILITY", "KVFLOW_CREDENTIALS",
)

CODEX_TIMEOUT_SECONDS = 900.0


def parse_event_stream(stdout: str) -> tuple[str | None, dict[str, Any]]:
    """The model's answer is the last ``item.completed`` with an agent message.

    ``turn.completed`` carries usage metadata only and is never the answer; other
    ``item.completed`` types (reasoning, tool calls) are not answers either.
    """
    text: str | None = None
    meta: dict[str, Any] = {}
    for line in (stdout or "").splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            event = json.loads(line)
        except ValueError:
            continue
        kind = event.get("type") or event.get("msg", {}).get("type")
        if kind == "turn.completed":
            usage = event.get("usage") or event.get("msg", {}).get("usage")
            if isinstance(usage, Mapping):
                meta["usage"] = dict(usage)
            continue
        item = event.get("item") or event.get("msg", {}).get("item")
        if kind == "item.completed" and isinstance(item, Mapping):
            if str(item.get("type")) == "agent_message" and item.get("text") is not None:
                text = str(item["text"])
    return text, meta


def codex_caller(
    role: RoleModel,
    *,
    cwd: str | os.PathLike[str] | None = None,
    binary: str = "codex",
    timeout_seconds: float = CODEX_TIMEOUT_SECONDS,
) -> Callable[[str, str], Mapping[str, Any]]:
    """A manager caller over the verified ``codex exec --json`` shape."""

    def caller(kind: str, prompt: str) -> Mapping[str, Any]:
        argv = [binary, "exec", "--json", "--model", role.model]
        argv += ["-c", f'model_reasoning_effort="{role.effort}"']
        argv += ["--ignore-user-config", "--ephemeral",
                 "--sandbox", "read-only", "--skip-git-repo-check", "-"]
        env = {key: value for key, value in os.environ.items()
               if key not in STRIPPED_FROM_MANAGER}
        for name in PROXY_VARIABLES:
            if os.environ.get(name):
                env[name] = os.environ[name]
        env["PYTHONIOENCODING"] = "utf-8"
        try:
            completed = subprocess.run(
                argv, input=prompt, capture_output=True, text=True, encoding="utf-8",
                errors="replace", env=env, cwd=str(cwd) if cwd else None,
                timeout=timeout_seconds, check=False,
            )
        except FileNotFoundError as exc:
            raise ProviderError(
                "the configured manager CLI is not installed",
                outcome="NOT_DISPATCHED", adapter="codex", binary=binary,
            ) from exc
        except subprocess.TimeoutExpired as exc:
            # the outcome of a timed-out manager turn is unknown, never "failed"
            raise ProviderError(
                "the manager CLI did not finish in time",
                outcome="OUTCOME_UNKNOWN", kind=kind, timeout_seconds=timeout_seconds,
            ) from exc
        text, meta = parse_event_stream(completed.stdout or "")
        if not text:
            raise ProviderError(
                "the manager CLI returned no agent message",
                outcome="OUTCOME_UNKNOWN", kind=kind,
                exit_code=completed.returncode, stderr=(completed.stderr or "")[-400:],
            )
        model = "NOT_EXPOSED"
        for candidate in (
            meta.get("model"),
            (meta.get("usage") or {}).get("model") if isinstance(meta.get("usage"), Mapping) else None,
        ):
            if isinstance(candidate, str) and candidate:
                model = candidate
        return {
            "text": text,
            "model": model if model != "NOT_EXPOSED" else role.model,
            "reports_identity": model != "NOT_EXPOSED",
            "exit_code": completed.returncode,
        }

    return caller


def deepseek_caller(
    role: RoleModel,
    *,
    provider: DeepSeekProvider | None = None,
    max_tokens: int = 8192,
) -> Callable[[str, str], Mapping[str, Any]]:
    """A manager caller on the DeepSeek endpoint itself (the ``deepseek_only`` path).

    The manager runs in its own context with a planning/review system prompt; it
    never receives the worker's tool tickets, and its reply is parsed by the same
    strict JSON gate as any other manager answer.
    """
    session = provider or DeepSeekProvider(model=role.model)
    system = (
        "You are the manager role of a development workflow. You plan work and you"
        " review evidence. You never write code yourself, you never claim a test"
        " passed without a receipt, and you answer with one JSON object only."
    )

    def caller(kind: str, prompt: str) -> Mapping[str, Any]:
        messages = [
            {"role": "system", "content": system},
            {"role": "user", "content": f"[{kind}]\n{prompt}"},
        ]
        completion = session.complete(messages, max_tokens=max_tokens)
        reason = getattr(completion, "finish_reason", "")
        if str(reason).lower() in {"length", "max_tokens"}:
            raise ProviderError(
                "the manager reply was truncated",
                outcome="OUTCOME_UNKNOWN", kind=kind, finish_reason=str(reason),
            )
        returned = getattr(completion, "provider_model", "") or "NOT_EXPOSED"
        return {
            "text": completion.content,
            "model": returned,
            "reports_identity": returned != "NOT_EXPOSED",
        }

    return caller


def build_manager(
    profile: ModelProfile,
    *,
    home: str | os.PathLike[str] | None = None,
    cwd: str | os.PathLike[str] | None = None,
    max_calls: int = 6,
) -> tuple[Manager, dict[str, Any]]:
    """Build the manager for this profile, or fail with an actionable reason."""
    role = profile.manager
    if role.adapter == "codex":
        path = shutil.which("codex")
        if path is None:
            raise ConfigError(
                "this model profile needs the Codex CLI, which is not on PATH",
                adapter="codex", model=role.model,
                alternative="use --model-profile deepseek_only",
            )
        caller = codex_caller(role, cwd=cwd)
    elif role.adapter == "deepseek":
        caller = deepseek_caller(role)
    else:
        raise ConfigError("unsupported manager adapter", adapter=role.adapter)
    manager = Manager(caller=caller, max_calls=max_calls, display_name=profile.title)
    return manager, manager.identity()


def build_worker_provider(profile: ModelProfile) -> DeepSeekProvider:
    """The worker's provider, configured from the profile rather than a default."""
    role = profile.worker
    if role.adapter != "deepseek":
        raise ConfigError(
            "no worker adapter is implemented for this profile", adapter=role.adapter
        )
    return DeepSeekProvider(model=role.model)


def diagnose(profile: ModelProfile, *, home: str | os.PathLike[str] | None = None) -> dict:
    """Which adapters really work on this machine, without spending a call."""
    codex_path = shutil.which("codex")
    provider = DeepSeekProvider(model=profile.worker.model)
    credential = provider.credential_status()
    return {
        "profile": profile.id,
        "adapters": {
            "codex": {
                "available": codex_path is not None,
                "path": codex_path or None,
                "used_for": [role for role in ("manager", "reviewer", "worker")
                             if getattr(profile, role).adapter == "codex"],
            },
            "deepseek": {
                "available": bool(credential.available),
                "source": credential.source,
                "used_for": [role for role in ("manager", "reviewer", "worker")
                             if getattr(profile, role).adapter == "deepseek"],
            },
        },
        "resolved_roles": {
            role: {
                "adapter": getattr(profile, role).adapter,
                "model": getattr(profile, role).model,
                "effort": getattr(profile, role).effort,
            }
            for role in ("manager", "worker", "reviewer")
        },
        "note": (
            "availability is checked by looking for the program and reading the"
            " credential reference; no model call is made here"
        ),
    }


def role_report(manager: Manager, profile: ModelProfile, provider: DeepSeekProvider) -> dict:
    """Configured and provider-returned identity per role, never merged."""
    return {
        "model_profile": profile.id,
        "manager": manager.identity(),
        "worker": provider.identity().to_dict(),
        "reviewer": (
            manager.identity() if profile.reviewer.adapter == "codex" else
            {"configured_model": profile.reviewer.model,
             "provider_returned_model": "NOT_EXPOSED",
             "verification_level": "CONFIGURED_ONLY",
             "source": "deepseek adapter, separate session"}
        ),
        "frozen_for_run": True,
    }
