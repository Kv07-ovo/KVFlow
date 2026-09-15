"""KVStock as an *optional*, read-only adapter.

Nothing here is imported by the core, by a template or by the planner. The module
becomes reachable only when a user has enabled the adapter in their runtime home
(:mod:`kvflow.adapters.registry`), and it only ever reads: the backing
:class:`~kvflow.adapters.kvstock.Reader` opens every artifact deny-by-default, and
no function in this file can write to the adapted project.

The surface is deliberately small:

``build_reader(entry)``
    turn one opt-in entry into a Reader over the explicitly registered sources;
``status(entry)``
    the registered lifecycle records, reported verbatim and never upgraded;
``read(entry, source_key, ...)``
    one registered source, with the reader's own output budget and provenance;
``fingerprint(root, ...)``
    a digest manifest of a tree, used to prove an adapter read changed nothing.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Iterable, Mapping

from ..core.errors import ContractError, NotFoundError
from ..core.hashing import manifest_hash, sha256_file
from . import registry as optin
from .kvstock import Freshness, Reader, SourceKind

__all__ = ["build_reader", "describe", "status", "read", "fingerprint", "verify_unchanged"]

#: file names never counted as content when a tree is fingerprinted
_DEFAULT_SKIP_DIRS = frozenset(
    {".git", "__pycache__", ".pytest_cache", ".mypy_cache", ".ruff_cache", "node_modules"}
)


def _options(entry: Mapping[str, Any]) -> dict[str, Any]:
    options = entry.get("options")
    if not isinstance(options, Mapping):
        raise ContractError("the adapter entry has no options object")
    return dict(options)


def build_reader(entry: Mapping[str, Any]) -> Reader:
    """A deny-by-default Reader over this entry's registered sources.

    ``options.root`` is the only readable tree and must be an existing absolute
    directory; ``options.sources`` is the explicit source registry, so there is no
    "read the whole project" path even with the adapter enabled.
    """
    if not entry.get("read_only", False):
        raise ContractError("this adapter entry is not marked read-only")
    options = _options(entry)
    root = options.get("root")
    if not isinstance(root, str) or not root:
        raise ContractError("the adapter needs an absolute root directory")
    if not os.path.isabs(root):
        raise ContractError("the adapter root must be an absolute path")
    if not Path(root).is_dir():
        raise NotFoundError("the adapter root does not exist or is not a directory",
                            root=root)
    sources = options.get("sources")
    if not isinstance(sources, list) or not sources:
        raise ContractError(
            "the adapter needs at least one explicitly registered source"
        )
    return Reader(root, sources)


def describe(entry: Mapping[str, Any]) -> dict[str, Any]:
    """Read-only view of what this enabled adapter can reach."""
    reader = build_reader(entry)
    return {
        "adapter": entry.get("adapter"),
        "capability": entry.get("capability"),
        "read_only": True,
        "root": str(reader.root),
        "sources": reader.sources(),
    }


def status(entry: Mapping[str, Any]) -> dict[str, Any]:
    """Lifecycle state of every journal source that registers one.

    The strings are reported as stored. Nothing here turns a stored lifecycle
    value into a claim that a strategy, a forward test or a research result is
    valid, and a source without a registered lifecycle selector is reported as
    unregistered rather than guessed.
    """
    reader = build_reader(entry)
    rows: list[dict[str, Any]] = []
    for key, spec in reader.sources().items():
        if spec.get("kind") != SourceKind.JOURNAL.value:
            continue
        if not spec.get("lifecycle_prefix"):
            rows.append({"source_key": key, "state": "NOT_REGISTERED",
                         "note": "this journal registers no lifecycle selector"})
            continue
        rows.append(reader.journal_status(key))
    return {"adapter": entry.get("adapter"), "read_only": True, "journals": rows}


def read(
    entry: Mapping[str, Any],
    source_key: str,
    *,
    selector: str | None = None,
    offset: int = 0,
    limit: int = 4096,
    prefix: str = "",
    cursor: str | None = None,
    page_limit: int = 50,
) -> dict[str, Any]:
    """One registered source, read through the reader's own authorization."""
    reader = build_reader(entry)
    spec = reader.spec(source_key)
    if spec.kind is SourceKind.JOURNAL:
        if selector:
            return reader.journal_record(source_key, selector).to_dict()
        return reader.journal_page(source_key, prefix, cursor=cursor,
                                   limit=page_limit).to_dict()
    if spec.kind is SourceKind.JSON:
        return reader.json_read(source_key, selector).to_dict()
    return reader.text_chunk(source_key, offset=offset, limit=limit).to_dict()


def _iter_files(root: Path, skip: Iterable[str] = _DEFAULT_SKIP_DIRS) -> list[Path]:
    skip = frozenset(skip)
    found: list[Path] = []
    for current, dirnames, filenames in os.walk(root):
        dirnames[:] = sorted(name for name in dirnames if name not in skip)
        for name in sorted(filenames):
            found.append(Path(current) / name)
    return found


def fingerprint(root: str | os.PathLike[str], *,
                skip: Iterable[str] = _DEFAULT_SKIP_DIRS) -> dict[str, Any]:
    """A digest manifest of every regular file under ``root``.

    Only regular files inside the tree are counted: a symlink is never followed,
    so a fingerprint cannot be made to depend on something outside the project it
    claims to describe.
    """
    base = Path(root)
    if not base.is_dir():
        raise NotFoundError("the fingerprinted root is not a directory", root=str(root))
    entries: dict[str, str] = {}
    for path in _iter_files(base, skip):
        if path.is_symlink() or not path.is_file():
            continue
        digest, _size = sha256_file(path)
        entries[path.relative_to(base).as_posix()] = digest
    return {"root": str(base), "files": len(entries), "manifest_sha256": manifest_hash(entries),
            "entries": entries}


def verify_unchanged(root: str | os.PathLike[str], before: Mapping[str, Any], *,
                     skip: Iterable[str] = _DEFAULT_SKIP_DIRS) -> dict[str, Any]:
    """Compare a tree against an earlier fingerprint and explain any difference."""
    after = fingerprint(root, skip=skip)
    if not isinstance(before, Mapping) or "entries" not in before:
        raise ContractError("a before-fingerprint with entries is required")
    expected = {str(k): str(v) for k, v in dict(before["entries"]).items()}
    current = after["entries"]
    changed = sorted(k for k in expected if k in current and current[k] != expected[k])
    missing = sorted(k for k in expected if k not in current)
    added = sorted(k for k in current if k not in expected)
    return {
        "root": after["root"],
        "unchanged": not (changed or missing or added),
        "changed": changed,
        "missing": missing,
        "added": added,
        "before_manifest_sha256": before.get("manifest_sha256"),
        "after_manifest_sha256": after["manifest_sha256"],
        "files_before": len(expected),
        "files_after": len(current),
    }


def enabled_entry(home: str | os.PathLike[str], adapter_id: str = "kvstock") -> dict[str, Any]:
    """The opt-in entry for ``adapter_id``, refusing when it is not enabled."""
    return optin.require_enabled(home, adapter_id)
