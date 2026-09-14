"""Content addressing helpers. Hashes are always computed by the scheduler."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

_CHUNK = 1 << 20


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_text(text: str) -> str:
    return sha256_bytes(text.encode("utf-8"))


def sha256_file(path: str | Path) -> tuple[str, int]:
    """Return ``(hexdigest, size_bytes)`` for an existing file."""
    digest = hashlib.sha256()
    size = 0
    with open(path, "rb") as handle:
        while True:
            chunk = handle.read(_CHUNK)
            if not chunk:
                break
            size += len(chunk)
            digest.update(chunk)
    return digest.hexdigest(), size


def canonical_json(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def hash_json(obj: Any) -> str:
    return sha256_text(canonical_json(obj))


def manifest_hash(entries: dict[str, str]) -> str:
    """Deterministic hash of a ``relative path -> sha256`` mapping."""
    return hash_json({k: v for k, v in sorted(entries.items())})
