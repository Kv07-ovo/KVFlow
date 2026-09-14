"""Named, read-only KVStock source reader with strict provenance.

This module is the only way the product may look at existing KVStock artifacts.
It is a pure library: it never writes, never executes a research command and
never opens a network connection. Authentication and authorization live in the
Orchestrator (``kvflow.core.security``); everything here is deny-by-default and
assumes it is already behind a verified capability.

Boundaries enforced by this module
----------------------------------

* **Explicit registry.** Every readable artifact is a named :class:`SourceSpec`
  the trusted caller registered. There is no arbitrary path, no arbitrary SQL
  and no "read the whole project" shortcut.
* **No link traversal.** The registered root and every one of its ancestors is
  inspected without following links *before* the path is resolved. A junction or
  symlink anywhere in the chain is refused, and an inspection failure is refused
  rather than treated as "not a link".
* **Real journal hash contract.** A record hash is
  ``sha256(canonical_json({"key": key, "body": <parsed body object>,
  "previous_hash": prior}))``. The body is parsed and strictly validated first;
  hashing the raw body *string* is wrong and is treated as an integrity failure.
* **Strict JSON.** ``NaN``/``Infinity`` are refused at parse time, in nested
  arrays and objects, and are never re-emitted.
* **Complete output budget.** The single budget covers the whole serialized
  result - provenance, escaping, row metadata - not just the selected payload.
  Truncation is explicit and always leaves a usable continuation cursor.
* **Honest freshness.** A caller-supplied mapping is never described as
  Reader-observed; only an opaque observation token this Reader issued can be
  compared, and an unchecked source is reported as such.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import sqlite3
import uuid
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Any, Iterable, Mapping, Sequence

__all__ = [
    "SourceKind",
    "SourceSpec",
    "ReadResult",
    "JournalRecord",
    "JournalPage",
    "Observation",
    "Reader",
    "KvStockReaderError",
    "UnknownSourceError",
    "InvalidSelectorError",
    "PathRejectedError",
    "SourceMissingError",
    "PermissionDeniedError",
    "MalformedContentError",
    "IntegrityError",
    "SizeError",
    "ConcurrentModificationError",
    "ZERO_HASH",
    "record_hash",
]

ZERO_HASH = "0" * 64

DEFAULT_MAX_FILE_BYTES = 4 * 1024 * 1024
DEFAULT_MAX_OUTPUT_BYTES = 256 * 1024
DEFAULT_MAX_PAGE = 200
MAX_BODY_BYTES = 8 * 1024 * 1024
HARD_MAX_OUTPUT_BYTES = 4 * 1024 * 1024
HARD_MAX_FILE_BYTES = 64 * 1024 * 1024

#: SQLite VM opcode budget per journal statement, so a hostile database cannot
#: make a page scan quadratic in wall-clock time
_MAX_PROGRESS_CALLS = 4000

_KEY_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:/-]{0,127}$")
_PREFIX_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:/%\-]{0,127}$")
_SELECTOR_RE = re.compile(
    r"^(?:[A-Za-z_][A-Za-z0-9_]{0,63})(?:\[[0-9]{1,6}\])?"
    r"(?:\.[A-Za-z_][A-Za-z0-9_]{0,63}(?:\[[0-9]{1,6}\])?)*$"
)

#: lifecycle strings are reported verbatim; none of them means "validated"
LIFECYCLE_STATES = frozenset(
    {
        "NOT_STARTED",
        "NOT_RUN",
        "PREPARED",
        "PREPARED_REVISION",
        "RUNNING",
        "ACTIVATION_PENDING_DATA",
        "DATA_BLOCKED",
        "NOT_ELIGIBLE",
        "COMPLETED",
        "FAILED",
        "CANCELLED",
        "INCONCLUSIVE",
        "REJECTED",
        "UNKNOWN",
    }
)

_RESERVED_NAMES = frozenset(
    {"con", "prn", "aux", "nul", "conin$", "conout$"}
    | {f"{p}{i}" for p in ("com", "lpt") for i in "123456789"}
)


class KvStockReaderError(Exception):
    """Base class for every reader failure, with a stable ``code``."""

    code = "KVSTOCK_READ_ERROR"


class UnknownSourceError(KvStockReaderError):
    code = "UNKNOWN_SOURCE"


class InvalidSelectorError(KvStockReaderError):
    code = "INVALID_SELECTOR"


class PathRejectedError(KvStockReaderError):
    code = "PATH_REJECTED"


class SourceMissingError(KvStockReaderError):
    code = "SOURCE_MISSING"


class PermissionDeniedError(KvStockReaderError):
    code = "PERMISSION_DENIED"


class MalformedContentError(KvStockReaderError):
    code = "MALFORMED_CONTENT"


class SchemaError(KvStockReaderError):
    code = "SCHEMA_ERROR"


class IntegrityError(KvStockReaderError):
    code = "INTEGRITY_ERROR"


class SizeError(KvStockReaderError):
    code = "SIZE_LIMIT"


class ResourceLimitError(KvStockReaderError):
    code = "RESOURCE_LIMIT"


class ConcurrentModificationError(KvStockReaderError):
    code = "CONCURRENT_MODIFICATION"


class SourceKind(str, Enum):
    JOURNAL = "journal"
    JSON = "json"
    TEXT = "text"


class Freshness(str, Enum):
    CHECKED = "CHECKED"
    NOT_CHECKED = "NOT_CHECKED"
    UNKNOWN = "UNKNOWN"


def canonical_json_bytes(value: Any) -> bytes:
    """Canonical UTF-8 JSON. Non-finite numbers are refused, never normalised."""
    _assert_finite(value, "$")
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _assert_finite(value: Any, path: str) -> None:
    if isinstance(value, bool) or value is None or isinstance(value, (str, int)):
        return
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            raise MalformedContentError(f"non-finite number at {path}")
        return
    if isinstance(value, list):
        for index, item in enumerate(value):
            _assert_finite(item, f"{path}[{index}]")
        return
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str):
                raise MalformedContentError(f"non-string object key at {path}")
            _assert_finite(item, f"{path}.{key}")
        return
    raise MalformedContentError(f"unsupported JSON type {type(value).__name__} at {path}")


def _reject_constant(name: str) -> Any:
    raise MalformedContentError(f"non-finite JSON constant {name} is not accepted")


def strict_json_loads(text: str) -> Any:
    """``json.loads`` with non-finite constants refused at parse time."""
    try:
        value = json.loads(text, parse_constant=_reject_constant)
    except MalformedContentError:
        raise
    except (ValueError, TypeError) as exc:
        raise MalformedContentError(f"invalid JSON: {exc}") from exc
    _assert_finite(value, "$")
    return value


def record_hash(key: str, body: Any, previous_hash: str) -> str:
    """The real KVStock journal record hash.

    ``body`` must be the *parsed* body object. Hashing the raw body string is a
    different (and wrong) function, which is exactly what the F3 review found.
    """
    if not isinstance(previous_hash, str) or not re.fullmatch(r"[0-9a-f]{64}", previous_hash):
        raise IntegrityError("previous_hash must be 64 lowercase hex characters")
    envelope = {"key": key, "body": body, "previous_hash": previous_hash}
    return hashlib.sha256(canonical_json_bytes(envelope)).hexdigest()


def _strict_int(value: Any, name: str, minimum: int, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise InvalidSelectorError(f"{name} must be a strict integer")
    if value < minimum or value > maximum:
        raise InvalidSelectorError(f"{name} must be between {minimum} and {maximum}")
    return value


def _awaitable_sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class SourceSpec:
    """One explicitly registered, read-only artifact."""

    key: str
    kind: SourceKind
    path: Path
    description: str = ""
    allowed_prefixes: tuple[str, ...] = ()
    allowed_exact_keys: tuple[str, ...] = ()
    allowed_selectors: tuple[str, ...] = ()
    permitted_empty_prefix: bool = False
    lifecycle_prefix: str | None = None
    expected_sha256: str | None = None
    max_bytes: int = DEFAULT_MAX_FILE_BYTES
    effective_time: str | None = None
    observed_time: str | None = None
    kind_label: str = ""

    def __post_init__(self) -> None:
        if not isinstance(self.key, str) or not _KEY_RE.match(self.key):
            raise InvalidSelectorError(f"invalid source key: {self.key!r}")
        if not isinstance(self.kind, SourceKind):
            try:
                object.__setattr__(self, "kind", SourceKind(self.kind))
            except ValueError as exc:
                raise InvalidSelectorError(f"unknown source kind: {self.kind!r}") from exc
        if not isinstance(self.path, Path):
            object.__setattr__(self, "path", Path(self.path))
        for prefix in self.allowed_prefixes:
            _validate_prefix(prefix)
        for key in self.allowed_exact_keys:
            _validate_prefix(key)
        for selector in self.allowed_selectors:
            _validate_selector(selector)
        if self.lifecycle_prefix is not None:
            _validate_prefix(self.lifecycle_prefix)
        if self.expected_sha256 is not None:
            if not re.fullmatch(r"[0-9a-f]{64}", str(self.expected_sha256)):
                raise InvalidSelectorError("expected_sha256 must be 64 lowercase hex characters")
            if self.kind is SourceKind.JOURNAL:
                # the live main database file is not a consistent journal
                # identity (WAL excluded), so this combination is refused
                # instead of silently pretending to be enforced
                raise InvalidSelectorError(
                    "expected_sha256 is not supported for a journal source;"
                    " use record-level integrity instead"
                )
        _strict_int(self.max_bytes, "max_bytes", 1, HARD_MAX_FILE_BYTES)

    def to_dict(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "kind": self.kind.value,
            "path": str(self.path),
            "description": self.description,
            "allowed_prefixes": list(self.allowed_prefixes),
            "allowed_exact_keys": list(self.allowed_exact_keys),
            "allowed_selectors": list(self.allowed_selectors),
            "permitted_empty_prefix": self.permitted_empty_prefix,
            "lifecycle_prefix": self.lifecycle_prefix,
            "expected_sha256": self.expected_sha256,
            "max_bytes": self.max_bytes,
            "kind_label": self.kind_label,
        }


@dataclass(frozen=True)
class Observation:
    """An opaque snapshot token issued by this Reader.

    A token can never be constructed by a caller because the nonce is generated
    here and never exposed; passing a plain mapping to a comparison is always
    reported as caller-supplied and unverified.
    """

    token: str
    source_key: str
    head_hash: str
    last_seq: int
    observed_at: str
    verified: bool = True


@dataclass(frozen=True)
class JournalRecord:
    seq: int
    key: str
    body: Any
    hash: str
    previous_hash: str
    body_bytes: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "seq": self.seq,
            "key": self.key,
            "body": self.body,
            "hash": self.hash,
            "previous_hash": self.previous_hash,
            "body_bytes": self.body_bytes,
        }


@dataclass(frozen=True)
class JournalPage:
    source_key: str
    prefix: str
    records: tuple[JournalRecord, ...]
    next_cursor: str | None
    truncated: bool
    truncation_reason: str | None
    output_bytes: int
    max_output_bytes: int
    head_hash: str | None
    last_seq: int | None
    freshness: Freshness
    integrity_scope: str
    observed_time: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_key": self.source_key,
            "prefix": self.prefix,
            "records": [r.to_dict() for r in self.records],
            "next_cursor": self.next_cursor,
            "truncated": self.truncated,
            "truncation_reason": self.truncation_reason,
            "output_bytes": self.output_bytes,
            "max_output_bytes": self.max_output_bytes,
            "head_hash": self.head_hash,
            "last_seq": self.last_seq,
            "freshness": self.freshness.value,
            "integrity_scope": self.integrity_scope,
            "observed_time": self.observed_time,
        }


@dataclass(frozen=True)
class ReadResult:
    source_key: str
    kind: str
    selector: str | None
    value: Any
    content_sha256: str | None
    payload_sha256: str | None
    output_bytes: int
    max_output_bytes: int
    truncated: bool
    truncation_reason: str | None
    next_cursor: str | None
    integrity_scope: str
    freshness: Freshness
    observed_time: str
    source_effective_time: str | None
    source_observed_time: str | None
    warnings: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_key": self.source_key,
            "kind": self.kind,
            "selector": self.selector,
            "value": self.value,
            "content_sha256": self.content_sha256,
            "payload_sha256": self.payload_sha256,
            "output_bytes": self.output_bytes,
            "max_output_bytes": self.max_output_bytes,
            "truncated": self.truncated,
            "truncation_reason": self.truncation_reason,
            "next_cursor": self.next_cursor,
            "integrity_scope": self.integrity_scope,
            "freshness": self.freshness.value,
            "observed_time": self.observed_time,
            "source_effective_time": self.source_effective_time,
            "source_observed_time": self.source_observed_time,
            "warnings": list(self.warnings),
        }


def _validate_selector(selector: Any) -> None:
    if not isinstance(selector, str) or not selector:
        raise InvalidSelectorError("a selector is required")
    if len(selector) > 512:
        raise InvalidSelectorError("selector is too long")
    if not _SELECTOR_RE.match(selector):
        raise InvalidSelectorError(f"selector syntax is not allowed: {selector!r}")


def _validate_prefix(prefix: Any) -> None:
    """A journal key prefix: the real syntax permits ``/`` (e.g. ``STATUS/``)."""
    if not isinstance(prefix, str) or not prefix:
        raise InvalidSelectorError("a prefix must be a nonempty string")
    if not _PREFIX_RE.match(prefix):
        raise InvalidSelectorError(f"prefix syntax is not allowed: {prefix!r}")
    if prefix.endswith("/") and len(prefix) == 1:
        raise InvalidSelectorError("a bare '/' prefix is not a selector")


def _validate_component(component: str) -> None:
    if not component or component in {".", ".."}:
        raise PathRejectedError("relative path contains a traversal component")
    if len(component) > 255 or component != component.strip():
        raise PathRejectedError("relative path component is malformed")
    if component[-1] in {" ", "."}:
        raise PathRejectedError("relative path component ends with a space or dot")
    if component[1:2] == ":" or ":" in component:
        raise PathRejectedError("alternate data streams are not allowed")
    if any(ord(ch) < 32 or ch in '<>"|?*' for ch in component):
        raise PathRejectedError("illegal character in relative path")
    if component.split(".")[0].casefold() in _RESERVED_NAMES:
        raise PathRejectedError("reserved device name in relative path")


def _is_reparse(path: Path) -> bool:
    try:
        stat = path.lstat()
    except FileNotFoundError:
        return False
    except OSError as exc:
        # an inspection failure is not evidence of a normal path
        raise PathRejectedError(f"cannot inspect path component ({exc.__class__.__name__})") from exc
    return bool(getattr(stat, "st_file_attributes", 0) & 0x400) or path.is_symlink()


def _inspect_chain(path: Path) -> list[Path]:
    """Return the full ancestor chain (volume root first) without following links."""
    target = Path(os.path.abspath(str(path)))
    chain: list[Path] = []
    current = target
    while True:
        chain.append(current)
        parent = current.parent
        if parent == current:
            break
        current = parent
    chain.reverse()
    return chain


class Reader:
    """Deny-by-default reader over an explicit registry of named sources."""

    def __init__(
        self,
        root: str | os.PathLike[str],
        sources: Iterable[SourceSpec | Mapping[str, Any]],
        *,
        max_output_bytes: int = DEFAULT_MAX_OUTPUT_BYTES,
        max_page: int = DEFAULT_MAX_PAGE,
        trust_project: bool = True,
    ) -> None:
        raw_root = str(root)
        if not raw_root or not os.path.isabs(raw_root):
            raise PathRejectedError("the reader root must be an explicit absolute path")
        if raw_root.startswith(("\\\\", "//")) or raw_root[1:2] == ":" and raw_root[:2] in {"\\\\"}:
            raise PathRejectedError("UNC and device roots are not accepted")
        if raw_root.startswith(("\\\\?\\", "\\\\.\\")):
            raise PathRejectedError("device roots are not accepted")
        self._root = Path(raw_root)
        # inspect the *original* path and every ancestor before resolving it
        for component in _inspect_chain(self._root):
            if not component.exists():
                raise SourceMissingError(f"reader root component is missing: {component}")
            if _is_reparse(component):
                raise PathRejectedError(
                    f"reader root or an ancestor is a link: {component}"
                )
        self._resolved_root = self._root.resolve(strict=True)
        self._max_output_bytes = _strict_int(
            max_output_bytes, "max_output_bytes", 256, HARD_MAX_OUTPUT_BYTES
        )
        self._max_page = _strict_int(max_page, "max_page", 1, 1000)
        self._trust_project = bool(trust_project)
        self._sources: dict[str, SourceSpec] = {}
        for entry in sources:
            spec = entry if isinstance(entry, SourceSpec) else SourceSpec(**entry)
            if spec.key in self._sources:
                raise InvalidSelectorError(f"duplicate source key: {spec.key}")
            self._verify_spec_path(spec)
            self._sources[spec.key] = spec
        self._observations: dict[str, Observation] = {}
        #: the real KVStock journals use ``record_key``; the schema check below
        #: confirms which column this particular database actually has
        self._key_column = "record_key"

    # ------------------------------------------------------------- registry
    def _verify_spec_path(self, spec: SourceSpec) -> None:
        path = spec.path if spec.path.is_absolute() else self._root / spec.path
        try:
            relative = path.relative_to(self._root)
        except ValueError:
            try:
                relative = path.relative_to(self._resolved_root)
            except ValueError as exc:
                raise PathRejectedError(
                    f"source {spec.key} is outside the registered root"
                ) from exc
        for component in relative.parts:
            _validate_component(component)
        object.__setattr__(spec, "path", self._root / relative)

    @property
    def root(self) -> Path:
        return self._resolved_root

    def sources(self) -> dict[str, dict[str, Any]]:
        """Defensive copy: a caller cannot mutate the registry through this."""
        return {key: spec.to_dict() for key, spec in self._sources.items()}

    def spec(self, source_key: str) -> SourceSpec:
        try:
            return self._sources[source_key]
        except KeyError as exc:
            raise UnknownSourceError(f"unknown source: {source_key!r}") from exc

    def _safe_path(self, spec: SourceSpec) -> Path:
        """Re-verify the whole chain, then return the candidate path."""
        for component in _inspect_chain(self._root):
            if not component.exists():
                raise SourceMissingError(f"root component disappeared: {component}")
            if _is_reparse(component):
                raise PathRejectedError(f"root is linked at {component}")
        for component in _inspect_chain(spec.path.parent):
            if not component.exists():
                raise SourceMissingError(f"source directory is missing: {component}")
            if _is_reparse(component):
                raise PathRejectedError(f"source path traverses a link: {component}")
        if not spec.path.exists():
            raise SourceMissingError(f"source {spec.key} is missing")
        if _is_reparse(spec.path):
            raise PathRejectedError(f"source {spec.key} is a link")
        return spec.path

    # ------------------------------------------------------------- reading
    def _read_bytes(self, spec: SourceSpec) -> bytes:
        path = self._safe_path(spec)
        try:
            with open(path, "rb") as handle:
                before = os.fstat(handle.fileno())
                if before.st_size > spec.max_bytes:
                    raise SizeError(
                        f"source {spec.key} exceeds its {spec.max_bytes} byte cap"
                    )
                data = handle.read(spec.max_bytes + 1)
                after = os.fstat(handle.fileno())
        except PermissionError as exc:
            raise PermissionDeniedError(f"permission denied reading {spec.key}") from exc
        if len(data) > spec.max_bytes:
            raise SizeError(f"source {spec.key} grew past its cap while reading")
        if (before.st_mtime_ns, before.st_size) != (after.st_mtime_ns, after.st_size):
            raise ConcurrentModificationError(f"source {spec.key} changed while reading")
        return data

    def _budget(self, payload: Any, extra: dict[str, Any]) -> tuple[int, bool]:
        envelope = {**extra, "value": payload}
        size = len(canonical_json_bytes(envelope))
        return size, size <= self._max_output_bytes

    def _check_expected(self, spec: SourceSpec, raw: bytes) -> str:
        digest = hashlib.sha256(raw).hexdigest()
        if spec.expected_sha256 is not None and digest != spec.expected_sha256:
            raise IntegrityError(f"source {spec.key} does not match its expected hash")
        return digest

    def json_read(self, source_key: str, selector: str | None = None, *,
                  cursor: str | None = None) -> ReadResult:
        spec = self.spec(source_key)
        if spec.kind is not SourceKind.JSON:
            raise InvalidSelectorError(f"source {source_key} is not a JSON source")
        if selector is not None:
            _validate_selector(selector)
            if spec.allowed_selectors and selector not in spec.allowed_selectors:
                raise InvalidSelectorError(
                    f"selector {selector!r} is not registered for {source_key}"
                )
        raw = self._read_bytes(spec)
        digest = self._check_expected(spec, raw)
        try:
            document = strict_json_loads(raw.decode("utf-8"))
        except UnicodeDecodeError as exc:
            raise MalformedContentError(f"source {source_key} is not valid UTF-8") from exc
        value = document if selector is None else _traverse(document, selector)
        # the enclosing document's own timestamps are retained even when a single
        # field was selected, so freshness is not lost by narrowing the read
        effective = _first_time(document, ("effective_time", "effective_at", "generated_at",
                                           "evaluated_at", "updated_at")) or spec.effective_time
        observed = _first_time(document, ("observed_time", "observed_at", "read_at")) or (
            spec.observed_time
        )
        warnings: list[str] = []
        freshness = Freshness.CHECKED if effective else Freshness.UNKNOWN
        if effective is None:
            warnings.append("the source carries no usable timestamp")
        payload_digest = hashlib.sha256(canonical_json_bytes(value)).hexdigest()
        result = ReadResult(
            source_key=source_key,
            kind=spec.kind.value,
            selector=selector,
            value=value,
            content_sha256=digest,
            payload_sha256=payload_digest,
            output_bytes=0,
            max_output_bytes=self._max_output_bytes,
            truncated=False,
            truncation_reason=None,
            next_cursor=None,
            integrity_scope=(
                "FILE_BYTES_AND_EXPECTED_HASH" if spec.expected_sha256 else "FILE_BYTES"
            ),
            freshness=freshness,
            observed_time=_utc_now(),
            source_effective_time=effective,
            source_observed_time=observed,
            warnings=tuple(warnings),
        )
        size, fits = self._budget(value, {"source_key": source_key, "selector": selector})
        object.__setattr__(result, "output_bytes", size)
        if not fits:
            raise SizeError(
                f"the result for {source_key} needs {size} bytes, above the"
                f" {self._max_output_bytes} byte output budget"
            )
        return result

    def text_chunk(self, source_key: str, *, offset: int = 0, limit: int = 4096) -> ReadResult:
        spec = self.spec(source_key)
        if spec.kind is not SourceKind.TEXT:
            raise InvalidSelectorError(f"source {source_key} is not a text source")
        offset = _strict_int(offset, "offset", 0, 1 << 30)
        limit = _strict_int(limit, "limit", 1, 1 << 20)
        raw = self._read_bytes(spec)
        digest = self._check_expected(spec, raw)
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise MalformedContentError(f"source {source_key} is not valid UTF-8") from exc
        chunk = text[offset : offset + limit]
        truncated = offset + len(chunk) < len(text)
        payload_digest = _awaitable_sha(chunk)
        result = ReadResult(
            source_key=source_key,
            kind=spec.kind.value,
            selector=None,
            value=chunk,
            content_sha256=digest,
            payload_sha256=payload_digest,
            output_bytes=0,
            max_output_bytes=self._max_output_bytes,
            truncated=truncated,
            truncation_reason="requested window" if truncated else None,
            next_cursor=str(offset + len(chunk)) if truncated else None,
            integrity_scope=(
                "FILE_BYTES_AND_EXPECTED_HASH" if spec.expected_sha256 else "FILE_BYTES"
            ),
            freshness=Freshness.CHECKED if spec.effective_time else Freshness.NOT_CHECKED,
            observed_time=_utc_now(),
            source_effective_time=spec.effective_time,
            source_observed_time=spec.observed_time,
        )
        size, fits = self._budget(
            chunk,
            {"source_key": source_key, "offset": offset, "content_sha256": digest},
        )
        object.__setattr__(result, "output_bytes", size)
        if not fits:
            # shrink the window until the *complete* result fits; the search is a
            # bisection because metadata overhead is not a simple subtraction
            low, high = 0, len(chunk)
            best: str | None = None
            while low <= high:
                middle = (low + high) // 2
                candidate = text[offset : offset + middle]
                candidate_size, candidate_fits = self._budget(
                    candidate,
                    {"source_key": source_key, "offset": offset, "content_sha256": digest},
                )
                if candidate_fits:
                    best = candidate
                    low = middle + 1
                else:
                    high = middle - 1
            if best is None:
                raise SizeError(
                    f"even the provenance of {source_key} exceeds the output budget"
                )
            truncated = offset + len(best) < len(text)
            result = replace(
                result,
                value=best,
                truncated=truncated,
                truncation_reason="output budget" if truncated else None,
                next_cursor=str(offset + len(best)) if truncated else None,
                payload_sha256=_awaitable_sha(best),
            )
            size, fits = self._budget(
                best,
                {"source_key": source_key, "offset": offset, "content_sha256": digest},
            )
            object.__setattr__(result, "output_bytes", size)
            if not fits:  # pragma: no cover - defensive
                raise SizeError(
                    f"even the provenance of {source_key} exceeds the output budget"
                )
        return result

    # -------------------------------------------------------------- journal
    def _open_journal(self, spec: SourceSpec) -> sqlite3.Connection:
        path = self._safe_path(spec)
        uri = f"file:{path.as_posix()}?mode=ro"
        try:
            connection = sqlite3.connect(uri, uri=True, isolation_level=None)
        except sqlite3.OperationalError as exc:
            raise PermissionDeniedError(f"cannot open journal {spec.key}: {exc}") from exc
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only = ON")
        connection.execute("PRAGMA trusted_schema = OFF")
        counter = {"n": 0}

        def _progress() -> int:
            counter["n"] += 1
            return 1 if counter["n"] > _MAX_PROGRESS_CALLS else 0

        connection.set_progress_handler(_progress, 1000)
        try:
            connection.execute("BEGIN")
            schema = {
                row["name"]
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                )
            }
        except sqlite3.DatabaseError as exc:
            connection.close()
            raise SchemaError(f"journal {spec.key} is unreadable: {exc}") from exc
        if "records" not in schema:
            connection.close()
            raise SchemaError(f"journal {spec.key} has no records table")
        columns = {
            row["name"]
            for row in connection.execute("PRAGMA table_info(records)")
        }
        # the real KVStock journals use ``record_key``; ``key`` is accepted so a
        # renamed column is an explicit schema decision rather than a silent pass
        key_column = "record_key" if "record_key" in columns else "key"
        required = {"seq", key_column, "body", "hash", "previous_hash"}
        if not required <= columns:
            connection.close()
            raise SchemaError(
                f"journal {spec.key} is missing columns {sorted(required - columns)}"
            )
        self._key_column = key_column
        return connection

    @property
    def _record_key_column(self) -> str:
        return getattr(self, "_key_column", "record_key")

    def _authorize_selector(self, spec: SourceSpec, key: str, *, exact: bool) -> None:
        if exact:
            if key in spec.allowed_exact_keys:
                return
            if spec.allowed_prefixes and any(
                key.startswith(prefix) for prefix in spec.allowed_prefixes
            ):
                return
            raise InvalidSelectorError(f"record key {key!r} is not registered for {spec.key}")
        if not key:
            if not spec.permitted_empty_prefix:
                raise InvalidSelectorError(
                    f"an empty prefix is not authorized for {spec.key}"
                )
            return
        if not any(key.startswith(prefix) for prefix in spec.allowed_prefixes):
            raise InvalidSelectorError(f"prefix {key!r} is not registered for {spec.key}")

    def _parse_record(self, row: sqlite3.Row, spec: SourceSpec) -> JournalRecord:
        seq = int(row["seq"])
        key = row["record_key"] if "record_key" in row.keys() else row["key"]
        if not isinstance(key, str) or not _KEY_RE.match(key):
            raise IntegrityError(f"journal {spec.key} seq {seq} has an invalid key")
        body_text = row["body"]
        if not isinstance(body_text, (str, bytes)):
            raise MalformedContentError(f"journal {spec.key} seq {seq} body is not text")
        raw = body_text.encode("utf-8") if isinstance(body_text, str) else body_text
        if len(raw) > MAX_BODY_BYTES:
            raise SizeError(f"journal {spec.key} seq {seq} body is too large")
        try:
            body = strict_json_loads(raw.decode("utf-8"))
        except UnicodeDecodeError as exc:
            raise MalformedContentError(f"journal {spec.key} seq {seq} body is not UTF-8") from exc
        previous = row["previous_hash"]
        stored = row["hash"]
        if not isinstance(stored, str) or not re.fullmatch(r"[0-9a-f]{64}", stored):
            raise IntegrityError(f"journal {spec.key} seq {seq} hash is malformed")
        if not isinstance(previous, str) or not re.fullmatch(r"[0-9a-f]{64}", previous):
            raise IntegrityError(f"journal {spec.key} seq {seq} previous_hash is malformed")
        expected = record_hash(key, body, previous)
        if expected != stored:
            raise IntegrityError(
                f"journal {spec.key} seq {seq} record hash mismatch"
            )
        return JournalRecord(
            seq=seq,
            key=key,
            body=body,
            hash=stored,
            previous_hash=previous,
            body_bytes=len(raw),
        )

    def _head(self, connection: sqlite3.Connection, spec: SourceSpec) -> tuple[str | None, int | None]:
        row = connection.execute(
            "SELECT hash, seq, previous_hash FROM records ORDER BY seq DESC LIMIT 1"
        ).fetchone()
        if row is None:
            return None, None
        # the genesis record must link to the documented zero hash
        if int(row["seq"]) == 1 and row["previous_hash"] != ZERO_HASH:
            raise IntegrityError(f"journal {spec.key} genesis does not link to the zero hash")
        return str(row["hash"]), int(row["seq"])

    def journal_record(self, source_key: str, key: str) -> JournalRecord:
        spec = self.spec(source_key)
        if spec.kind is not SourceKind.JOURNAL:
            raise InvalidSelectorError(f"source {source_key} is not a journal")
        _validate_prefix(key)
        self._authorize_selector(spec, key, exact=True)
        connection = self._open_journal(spec)
        column = self._record_key_column
        try:
            row = connection.execute(
                f"SELECT seq, {column}, body, hash, previous_hash FROM records"
                f" WHERE {column} = ? ORDER BY seq DESC LIMIT 1",
                (key,),
            ).fetchone()
            if row is None:
                raise SourceMissingError(f"record {key!r} is absent from {source_key}")
            record = self._parse_record(row, spec)
            previous = connection.execute(
                "SELECT seq, body, hash FROM records WHERE seq = ?", (record.seq - 1,)
            ).fetchone()
            if previous is not None and previous["hash"] != record.previous_hash:
                raise IntegrityError(
                    f"journal {source_key} seq {record.seq} predecessor link is broken"
                )
            return record
        finally:
            connection.execute("ROLLBACK")
            connection.close()

    def journal_page(
        self,
        source_key: str,
        prefix: str = "",
        *,
        cursor: str | None = None,
        limit: int = 50,
    ) -> JournalPage:
        spec = self.spec(source_key)
        if spec.kind is not SourceKind.JOURNAL:
            raise InvalidSelectorError(f"source {source_key} is not a journal")
        if prefix:
            _validate_prefix(prefix)
        self._authorize_selector(spec, prefix, exact=False)
        limit = _strict_int(limit, "limit", 1, self._max_page)
        after_seq = 0
        if cursor:
            after_seq = _strict_int(int(cursor) if cursor.isdigit() else -1,
                                    "cursor", 0, 1 << 62)
        connection = self._open_journal(spec)
        column = self._record_key_column
        try:
            if prefix:
                rows = connection.execute(
                    f"SELECT seq, {column}, body, hash, previous_hash FROM records"
                    f" WHERE {column} LIKE ? ESCAPE '\\' AND seq > ? ORDER BY seq LIMIT ?",
                    (_escape_like(prefix) + "%", after_seq, limit + 1),
                ).fetchall()
            else:
                rows = connection.execute(
                    f"SELECT seq, {column}, body, hash, previous_hash FROM records"
                    " WHERE seq > ? ORDER BY seq LIMIT ?",
                    (after_seq, limit + 1),
                ).fetchall()
            head_hash, last_seq = self._head(connection, spec)
        finally:
            connection.execute("ROLLBACK")
            connection.close()

        has_more = len(rows) > limit
        rows = rows[:limit]
        records: list[JournalRecord] = []
        truncated = False
        reason: str | None = None
        # reserve the provenance of the complete result before adding rows, so a
        # page can never exceed the single output budget
        base = {
            "source_key": source_key,
            "prefix": prefix,
            "head_hash": head_hash,
            "last_seq": last_seq,
            "max_output_bytes": self._max_output_bytes,
        }
        running = len(canonical_json_bytes({**base, "records": []}))
        for row in rows:
            record = self._parse_record(row, spec)
            entry = canonical_json_bytes(record.to_dict())
            if running + len(entry) + 1 > self._max_output_bytes and records:
                truncated = True
                reason = "output budget"
                break
            if running + len(entry) + 1 > self._max_output_bytes:
                raise SizeError(
                    f"a single record of {source_key} exceeds the output budget"
                )
            # prefix authorization is re-checked against the literal stored key
            self._authorize_selector(spec, record.key, exact=False)
            records.append(record)
            running += len(entry) + 1
        next_cursor = None
        if truncated or has_more:
            next_cursor = str(records[-1].seq if records else after_seq)
        page = JournalPage(
            source_key=source_key,
            prefix=prefix,
            records=tuple(records),
            next_cursor=next_cursor,
            truncated=truncated or has_more,
            truncation_reason=reason or ("more rows available" if has_more else None),
            output_bytes=running,
            max_output_bytes=self._max_output_bytes,
            head_hash=head_hash,
            last_seq=last_seq,
            freshness=Freshness.CHECKED if last_seq is not None else Freshness.UNKNOWN,
            integrity_scope="PER_RECORD_CANONICAL_HASH_CHAIN",
            observed_time=_utc_now(),
        )
        if page.output_bytes > self._max_output_bytes:  # pragma: no cover - defensive
            raise SizeError("the assembled page exceeds the output budget")
        return page

    def journal_status(self, source_key: str) -> dict[str, Any]:
        """The registered lifecycle record for this source, reported verbatim.

        Only the source's own ``lifecycle_prefix`` is consulted, so a decoy key
        such as ``STATUS_FAKE`` can never stand in for ``STATUS/``. A missing
        lifecycle record is reported as UNKNOWN rather than as a success.
        """
        spec = self.spec(source_key)
        if spec.kind is not SourceKind.JOURNAL:
            raise InvalidSelectorError(f"source {source_key} is not a journal")
        if not spec.lifecycle_prefix:
            raise InvalidSelectorError(
                f"source {source_key} has no registered lifecycle selector"
            )
        prefix = spec.lifecycle_prefix
        page = self.journal_page(source_key, prefix, limit=self._max_page)
        latest: dict[str, Any] | None = None
        state = "UNKNOWN"
        observed: str | None = None
        reason: str | None = None
        # the newest lifecycle record wins: an appended record must not be
        # hidden behind the oldest one
        for record in reversed(page.records):
            body = record.body if isinstance(record.body, dict) else {}
            payload = body.get("payload") if isinstance(body.get("payload"), dict) else {}
            candidate = (
                body.get("status")
                or body.get("state")
                or body.get("lifecycle")
                or payload.get("status")
            )
            if isinstance(candidate, str):
                state = candidate
                latest = body
                observed = _string_or_none(
                    body.get("observed_at") or body.get("observed_time") or body.get("date")
                )
                reason = _string_or_none(payload.get("reason") or body.get("reason"))
                break
        return {
            "source_key": source_key,
            "selector": prefix,
            "state": state,
            "state_is_registered": state in LIFECYCLE_STATES,
            "payload": latest,
            "reason": reason,
            "source_observed_at": observed,
            "head_hash": page.head_hash,
            "last_seq": page.last_seq,
            "registered_prefixes": list(spec.allowed_prefixes),
            "observed_time": page.observed_time,
            "integrity_scope": page.integrity_scope,
            "note": (
                "the lifecycle string is reported as stored; it is never upgraded"
                " into a claim that a strategy or research result is valid"
            ),
        }

    # ---------------------------------------------------------- observation
    def observe(self, source_key: str) -> Observation:
        """Record an opaque head observation for later staleness comparison.

        For a journal source this is the stored record-chain head, which is the
        only identity that survives a WAL append. When the registration does not
        authorize a whole-journal scan, the observation falls back to the file
        byte identity and is *not* described as a chain head.
        """
        spec = self.spec(source_key)
        if spec.kind is SourceKind.JOURNAL:
            try:
                page = self.journal_page(source_key, "", limit=1)
            except InvalidSelectorError:
                raw = self._read_bytes(spec)
                observation = Observation(
                    token=uuid.uuid4().hex,
                    source_key=source_key,
                    head_hash=hashlib.sha256(raw).hexdigest(),
                    last_seq=0,
                    observed_at=_utc_now(),
                )
            else:
                observation = Observation(
                    token=uuid.uuid4().hex,
                    source_key=source_key,
                    head_hash=page.head_hash or ZERO_HASH,
                    last_seq=page.last_seq or 0,
                    observed_at=_utc_now(),
                )
        else:
            raw = self._read_bytes(spec)
            observation = Observation(
                token=uuid.uuid4().hex,
                source_key=source_key,
                head_hash=hashlib.sha256(raw).hexdigest(),
                last_seq=0,
                observed_at=_utc_now(),
            )
        self._observations[observation.token] = observation
        return observation

    def compare_observation(self, source_key: str, claimed: Any) -> dict[str, Any]:
        """Compare against a *Reader-issued* token, never a caller mapping.

        A plain mapping is always reported as caller-supplied and unverified so
        that a model-authored dict can never acquire reader provenance.
        """
        current = self.observe(source_key)
        if isinstance(claimed, Observation):
            previous = self._observations.get(claimed.token)
            if previous is None or previous.source_key != source_key:
                return {
                    "source_key": source_key,
                    "verified": False,
                    "provenance": "CALLER_SUPPLIED_UNVERIFIED",
                    "stale": None,
                    "reason": "observation token was not issued for this source",
                }
            return {
                "source_key": source_key,
                "verified": True,
                "provenance": "READER_OBSERVED",
                "stale": previous.head_hash != current.head_hash,
                "previous_head_hash": previous.head_hash,
                "current_head_hash": current.head_hash,
                "previous_last_seq": previous.last_seq,
                "current_last_seq": current.last_seq,
                "previous_observed_at": previous.observed_at,
                "current_observed_at": current.observed_at,
            }
        return {
            "source_key": source_key,
            "verified": False,
            "provenance": "CALLER_SUPPLIED_UNVERIFIED",
            "stale": None,
            "reason": "a caller-supplied mapping is not a reader observation",
        }

    # ------------------------------------------------------------- lifespan
    def close(self) -> None:
        """Nothing to release: every connection is closed inside its call."""
        return None

    def __enter__(self) -> "Reader":
        return self

    def __exit__(self, *exc: object) -> bool:
        self.close()
        return False


def _escape_like(value: str) -> str:
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _traverse(document: Any, selector: str) -> Any:
    current = document
    for part in selector.split("."):
        name, _, index = part.partition("[")
        if index:
            index_value = int(index.rstrip("]"))
        else:
            index_value = None
        if name:
            if not isinstance(current, dict) or name not in current:
                raise InvalidSelectorError(f"selector field {name!r} is absent")
            current = current[name]
        if index_value is not None:
            if not isinstance(current, list) or index_value >= len(current):
                raise InvalidSelectorError(f"selector index {index_value} is out of range")
            current = current[index_value]
    return current


def _first_time(document: Any, names: Sequence[str], depth: int = 0) -> str | None:
    """Find the first timestamp for ``names`` anywhere in the document.

    A scalar field selection must not lose the enclosing document's own
    timestamps, so the search walks nested objects and lists (bounded depth).
    """
    if depth > 6:
        return None
    if isinstance(document, dict):
        for name in names:
            value = document.get(name)
            if isinstance(value, str) and value:
                return value
        for value in document.values():
            found = _first_time(value, names, depth + 1)
            if found:
                return found
        return None
    if isinstance(document, list):
        for item in document[:32]:
            found = _first_time(item, names, depth + 1)
            if found:
                return found
    return None


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _string_or_none(value: Any) -> str | None:
    return value if isinstance(value, str) and value else None
