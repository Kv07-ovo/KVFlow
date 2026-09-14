"""Credential discovery and availability checks.

Credentials come from environment variables. Reading the existing local DSH
credential YAML is an explicit, read-only opt-in (``KVFLOW_CREDENTIALS``)
and values are never copied into the database, logs or receipts; only a boolean
availability flag and the key path are ever reported.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

DSH_ENV_VAR = "KVFLOW_CREDENTIALS"
DSH_POINTER = "refs.DEEPSEEK_API_KEY"


def _read_dsh_key(path: Path, pointer: str) -> str | None:
    """Minimal dotted-pointer reader for the DSH YAML (no YAML dependency).

    Reads only; the file is never written, copied or cached.
    """
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return None
    parts = pointer.split(".")
    target_leaf = parts[-1]
    parents = parts[:-1]
    stack: list[tuple[int, str]] = []
    for raw in lines:
        if not raw.strip() or raw.lstrip().startswith("#"):
            continue
        indent = len(raw) - len(raw.lstrip(" "))
        stripped = raw.strip()
        if ":" not in stripped:
            continue
        key, _, rest = stripped.partition(":")
        key = key.strip().strip('"').strip("'")
        while stack and stack[-1][0] >= indent:
            stack.pop()
        current_path = [entry[1] for entry in stack]
        if not rest.strip():
            stack.append((indent, key))
            continue
        if key != target_leaf or current_path != parents:
            continue
        value = rest.strip().strip('"').strip("'")
        if value in ("", "null", "~"):
            return None
        return value
    return None


@dataclass(frozen=True)
class CredentialStatus:
    source: str
    available: bool
    pointer: str | None = None

    def public(self) -> dict[str, object]:
        out: dict[str, object] = {"source": self.source, "available": self.available}
        if self.pointer:
            out["key_path"] = self.pointer
        return out


def deepseek_credential(
    env: Mapping[str, str] | None = None, *, allow_dsh_store: bool | None = None
) -> tuple[str | None, CredentialStatus]:
    """Return ``(secret_or_None, public_status)``. Never logs the secret."""
    source_env = dict(os.environ if env is None else env)
    direct = source_env.get("DEEPSEEK_API_KEY")
    if direct:
        return direct, CredentialStatus(source="env:DEEPSEEK_API_KEY", available=True)

    if allow_dsh_store is None:
        store_path = source_env.get(DSH_ENV_VAR, "")
    elif allow_dsh_store:
        store_path = source_env.get(DSH_ENV_VAR, "")
    else:
        store_path = ""

    if store_path:
        path = Path(store_path).expanduser()
        if path.is_file():
            value = _read_dsh_key(path, DSH_POINTER)
            if value:
                return value, CredentialStatus(
                    source="dsh_store(read-only)", available=True, pointer=DSH_POINTER
                )
            return None, CredentialStatus(
                source="dsh_store(read-only)", available=False, pointer=DSH_POINTER
            )
        return None, CredentialStatus(
            source="dsh_store(read-only)", available=False, pointer=str(path)
        )
    return None, CredentialStatus(source="env:DEEPSEEK_API_KEY", available=False)


def codex_available(env: Mapping[str, str] | None = None) -> CredentialStatus:
    """Codex CLI relies on normal user home/auth context, not a project secret."""
    source_env = dict(os.environ if env is None else env)
    home = source_env.get("USERPROFILE") or source_env.get("HOME")
    if home and Path(home).is_dir():
        return CredentialStatus(source="user-home", available=True)
    return CredentialStatus(source="user-home", available=False)
