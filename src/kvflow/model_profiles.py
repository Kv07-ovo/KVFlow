"""Model profiles: which model plays which role, frozen per run.

The core knows roles and permissions; it never knows a model name. A model
profile supplies the names, so "this build happened to use one model" never
becomes "the product can only use that model".

Two profiles ship with the product:

``deepseek_only``
    Manager, Worker and Reviewer all run on DeepSeek. This is the profile that
    must complete the whole development loop on a machine with no other
    credential.

``hybrid``
    Manager on an already-configured Codex/Astra adapter, Worker on DeepSeek.
    Optional: without that credential the product still runs ``deepseek_only``.

A profile is resolved once, at run start, and recorded in the run lineage: a run
never silently downgrades or upgrades its model because a budget ran low.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path

from .core.contracts import Contract, Id, StrictBool, StrictStr, Text
from .core.errors import ConfigError, NotFoundError

#: environment variable that selects the default profile
PROFILE_ENV = "KVFLOW_MODEL_PROFILE"
DEFAULT_PROFILE = "deepseek_only"
OVERRIDE_FILE = "model_profiles.json"

#: effort values the adapters actually accept today
EFFORTS = ("minimal", "low", "medium", "high", "max", "ultra", "NOT_EXPOSED")
ADAPTERS = ("deepseek", "codex", "fixture")


class RoleModel(Contract):
    """One role's configured model and reasoning effort."""

    adapter: StrictStr
    model: StrictStr
    effort: StrictStr
    notes: Text | None = None


class ModelProfile(Contract):
    """A complete role -> model assignment, with an honest identity report."""

    id: Id
    title: Text
    manager: RoleModel
    worker: RoleModel
    reviewer: RoleModel
    requires_credentials: list[StrictStr] = []
    optional_credential: StrictBool = False
    summary: Text


def _profile(identifier: str, title: str, manager: RoleModel, worker: RoleModel,
             reviewer: RoleModel, requires: list[str], optional: bool, summary: str
             ) -> ModelProfile:
    for role in (manager, worker, reviewer):
        if role.adapter not in ADAPTERS:
            raise ConfigError("unknown model adapter", adapter=role.adapter)
        if role.effort not in EFFORTS:
            raise ConfigError("unknown reasoning effort", effort=role.effort)
    return ModelProfile(
        id=identifier, title=title, manager=manager, worker=worker, reviewer=reviewer,
        requires_credentials=requires, optional_credential=optional, summary=summary,
    )


BUILTIN: dict[str, ModelProfile] = {
    "deepseek_only": _profile(
        "deepseek_only",
        "DeepSeek only",
        manager=RoleModel(adapter="deepseek", model="deepseek-flash", effort="max",
                          notes="the same adapter, in its own session and context"),
        worker=RoleModel(adapter="deepseek", model="deepseek-flash", effort="high"),
        reviewer=RoleModel(adapter="deepseek", model="deepseek-flash", effort="high",
                           notes="a separate session, not an independent third party"),
        requires=["deepseek"],
        optional=False,
        summary=(
            "The whole loop on one provider. Roles are permissions and context, not"
            " model names: the manager never codes and the reviewer sees only evidence."
        ),
    ),
    "hybrid": _profile(
        "hybrid",
        "Hybrid (configured manager adapter + DeepSeek worker)",
        manager=RoleModel(adapter="codex", model="gpt-6-astra", effort="ultra"),
        worker=RoleModel(adapter="deepseek", model="deepseek-flash", effort="high"),
        reviewer=RoleModel(adapter="codex", model="gpt-6-astra", effort="ultra"),
        requires=["deepseek", "codex"],
        optional=True,
        summary=(
            "Planning and review on the already-configured host adapter, implementation"
            " on DeepSeek. Optional: deepseek_only remains a complete product."
        ),
    ),
}


def _override_path(home: str | os.PathLike[str] | None) -> Path | None:
    if home is None:
        return None
    return Path(home).expanduser() / OVERRIDE_FILE


def load(profile_id: str | None = None, *, home: str | os.PathLike[str] | None = None
         ) -> ModelProfile:
    """Resolve one profile from the built-ins plus an optional user override file."""
    identifier = profile_id or os.environ.get(PROFILE_ENV) or DEFAULT_PROFILE
    overrides: dict[str, ModelProfile] = {}
    path = _override_path(home)
    if path is not None and path.exists():
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except ValueError as exc:
            raise ConfigError("the model profile override file is not valid JSON",
                              path=str(path)) from exc
        entries = raw.get("profiles") if isinstance(raw, dict) else None
        if not isinstance(entries, dict):
            raise ConfigError("the model profile override file needs a profiles object",
                              path=str(path))
        for key, value in entries.items():
            overrides[str(key)] = ModelProfile.model_validate(value)
    table = {**BUILTIN, **overrides}
    try:
        return table[identifier]
    except KeyError as exc:
        raise NotFoundError(
            "unknown model profile", profile_id=identifier, known=sorted(table)
        ) from exc


def catalogue(*, home: str | os.PathLike[str] | None = None) -> list[dict]:
    """Every known profile, with the configured identities and honest unknowns."""
    table = {**BUILTIN}
    path = _override_path(home)
    if path is not None and path.exists():
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
            for key, value in (raw.get("profiles") or {}).items():
                table[str(key)] = ModelProfile.model_validate(value)
        except (ValueError, AttributeError):
            pass
    return [describe(table[key]) for key in sorted(table)]


def describe(profile: ModelProfile) -> dict:
    """Configured identity is known; what a provider returns is not, until it runs."""
    return {
        "id": profile.id,
        "title": profile.title,
        "summary": profile.summary,
        "requires_credentials": list(profile.requires_credentials),
        "optional_credential": profile.optional_credential,
        "roles": {
            role: {
                "configured_adapter": getattr(profile, role).adapter,
                "configured_model": getattr(profile, role).model,
                "configured_effort": getattr(profile, role).effort,
                "provider_returned_model": "NOT_EXPOSED",
                "provider_returned_effort": "NOT_EXPOSED",
                "verification_level": "CONFIGURED_ONLY",
                "notes": getattr(profile, role).notes,
            }
            for role in ("manager", "worker", "reviewer")
        },
    }
