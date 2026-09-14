"""Public credential surface.

The implementation lives in :mod:`kvflow.core.credentials`; this module is the
stable import path the product, the tests and any adapter use. Credential values
are never printed, copied or handed to a child process.
"""

from __future__ import annotations

from .core.credentials import (
    DSH_ENV_VAR,
    DSH_POINTER,
    CredentialStatus,
    codex_available,
    deepseek_credential,
)

__all__ = [
    "DSH_ENV_VAR",
    "DSH_POINTER",
    "CredentialStatus",
    "codex_available",
    "deepseek_credential",
]
