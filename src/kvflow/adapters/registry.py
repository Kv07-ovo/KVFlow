"""Explicit, durable opt-in for the optional project adapters.

An adapter knows about one product's domain. Nothing in :mod:`kvflow.core` or in
the template/planner surface may import one, and an adapter is *never* active
because a file happens to exist: it is active only when a user has enabled it
here, in the runtime home this process actually opened.

The state is a small JSON document (``<home>/adapters.json``) so a human can read
what is enabled, why, and which roots were approved. Enabling is a deliberate,
auditable write; disabling removes the entry rather than pretending to sandbox
it. Every read path calls :func:`require_enabled` first, so "the adapter is off"
is a refusal with the exact command that would turn it on, not an empty result.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Mapping

from ..core.errors import ContractError, NotFoundError

#: the adapters this product ships. A name that is not here cannot be enabled,
#: so a typo can never silently produce a configuration that does nothing.
KNOWN_ADAPTERS: dict[str, str] = {
    "kvstock": "read-only KVStock journals, artifacts and lifecycle records",
}

SCHEMA_VERSION = 1


def _adapters_path(home: str | os.PathLike[str]) -> Path:
    return Path(home) / "adapters.json"


def _empty() -> dict[str, Any]:
    return {"schema_version": SCHEMA_VERSION, "enabled": {}}


def load_state(home: str | os.PathLike[str]) -> dict[str, Any]:
    """Read the opt-in document; a malformed document is a refusal, not a reset."""
    path = _adapters_path(home)
    if not path.is_file():
        return _empty()
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ContractError(
            f"the adapter opt-in document is unreadable: {type(exc).__name__}"
        ) from exc
    if not isinstance(document, dict) or document.get("schema_version") != SCHEMA_VERSION:
        raise ContractError("the adapter opt-in document has an unsupported schema version")
    enabled = document.get("enabled")
    if not isinstance(enabled, dict):
        raise ContractError("the adapter opt-in document has no enabled map")
    return document


def _write_state(home: str | os.PathLike[str], state: Mapping[str, Any]) -> Path:
    path = _adapters_path(home)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".json.tmp")
    temporary.write_text(
        json.dumps(dict(state), ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    os.replace(temporary, path)
    return path


def enable(
    home: str | os.PathLike[str],
    adapter_id: str,
    *,
    options: Mapping[str, Any] | None = None,
    reason: str = "",
) -> dict[str, Any]:
    """Turn one adapter on for this runtime home, recording what was approved."""
    if adapter_id not in KNOWN_ADAPTERS:
        raise ContractError(
            f"unknown adapter {adapter_id!r}", known=sorted(KNOWN_ADAPTERS)
        )
    state = load_state(home)
    entry = {
        "adapter": adapter_id,
        "capability": KNOWN_ADAPTERS[adapter_id],
        "read_only": True,
        "options": dict(options or {}),
        "reason": reason,
    }
    state["enabled"][adapter_id] = entry
    _write_state(home, state)
    return entry


def disable(home: str | os.PathLike[str], adapter_id: str) -> dict[str, Any]:
    """Turn one adapter off. Removing the entry is the whole effect."""
    state = load_state(home)
    removed = state["enabled"].pop(adapter_id, None)
    _write_state(home, state)
    return {"adapter": adapter_id, "was_enabled": removed is not None, "removed": removed}


def require_enabled(home: str | os.PathLike[str], adapter_id: str) -> dict[str, Any]:
    """Return the opt-in entry, or refuse with the command that would enable it.

    This is the single gate every adapter read path goes through, so an adapter
    cannot be used because an environment variable, a leftover file or a model
    suggestion implied it should be.
    """
    state = load_state(home)
    entry = state["enabled"].get(adapter_id)
    if entry is None:
        raise NotFoundError(
            f"the {adapter_id} adapter is not enabled in this runtime home",
            adapter=adapter_id,
            enable_command=f"kvflow adapter enable {adapter_id}",
            note="optional adapters are deny-by-default and never auto-detected",
        )
    return entry


def describe(home: str | os.PathLike[str]) -> list[dict[str, Any]]:
    """Every known adapter with its opt-in state, for diagnostics."""
    state = load_state(home)
    rows: list[dict[str, Any]] = []
    for adapter_id, capability in sorted(KNOWN_ADAPTERS.items()):
        entry = state["enabled"].get(adapter_id)
        rows.append(
            {
                "adapter": adapter_id,
                "capability": capability,
                "enabled": entry is not None,
                "read_only": True,
                "entry": entry,
            }
        )
    return rows
