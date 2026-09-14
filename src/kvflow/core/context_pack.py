"""Deterministic bounded model-context packaging.

Pure library: no file, network or process operations and no model calls. It
does not persist or authorize checkpoints; the trusted Orchestrator supplies
already-authorized header and source data and remains responsible for
authorization and sanitization of source content.

Guarantees:

* Strict frozen Pydantic v2 contracts with ``extra="forbid"`` and strict
  scalar types (no bool-as-int, no numeric strings, no NaN/Infinity).
* Header fields (objective, constraints, acceptance, authorization, plan
  version, checkpoint counters/budgets/unknown operations) are preserved
  exactly. If they cannot fit the budget the call raises
  ``ContextBudgetExceeded`` instead of compressing or dropping them.
* Only optional source *content* may be truncated, at UTF-8 character
  boundaries. Provenance is always retained for included/truncated sources.
* Every supplied source appears exactly once in the manifest as included,
  truncated or omitted, and every entry carries a digest of the full
  supplied content (``supplied_content_sha256``) plus, when the caller
  provided one, the separately scoped ``original_sha256``.
* The conservative token upper bound is the UTF-8 byte length of the final
  serialized message; it is an estimate, not measured tokenizer usage.
* The wire payload embeds a per-source manifest but *not* the final message
  hash/size receipts; those live only in the external manifest, so no
  self-referential hashing is required.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

__all__ = [
    "ContextBudgetExceeded",
    "ContextPackError",
    "SourceScope",
    "SourceKind",
    "VerificationStatus",
    "SourceStatus",
    "Authorization",
    "Checkpoint",
    "ContextHeader",
    "ContextSource",
    "ContextLimits",
    "SourceManifestEntry",
    "ContextManifest",
    "AssembledContext",
    "assemble_context",
]

MAX_SOURCES = 256
MAX_SOURCE_TEXT_BYTES = 1 << 20  # 1 MiB per source content
MAX_SUPPLIED_BYTES = 4 << 20  # 4 MiB total supplied content
MAX_METADATA_BYTES = 4 << 20  # 4 MiB aggregate supplied metadata
MAX_FIELD_CHARS = 4096  # per free-text header field
#: identifiers are short by contract; a megabyte-long "id" is an attack, not data
MAX_ID_CHARS = 256
MAX_SOURCE_REF_CHARS = 4096

_TS_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,6})?(Z|[+-]\d{2}:\d{2})$"
)


def _valid_timestamp(value: str) -> bool:
    if value == "UNKNOWN":
        return True
    if not _TS_RE.match(value):
        return False
    try:
        year = int(value[0:4])
        month = int(value[5:7])
        day = int(value[8:10])
        hour = int(value[11:13])
        minute = int(value[14:16])
        second = int(value[17:19])
    except ValueError:
        return False
    if not (1 <= month <= 12 and 1 <= day <= 31):
        return False
    if not (0 <= hour <= 23 and 0 <= minute <= 59 and 0 <= second <= 59):
        return False
    return 1 <= year <= 9999


class ContextPackError(Exception):
    """Base error for context packaging failures."""


class ContextBudgetExceeded(ContextPackError):
    """Raised when required (non-truncatable) content cannot fit the budget."""


class SourceScope(str, Enum):
    GLOBAL = "GLOBAL"
    PROJECT = "PROJECT"
    RESTRICTED = "RESTRICTED"


class SourceKind(str, Enum):
    SOURCE = "source"
    ARTIFACT = "artifact"
    DECISION = "decision"
    STATUS = "status"
    SUMMARY = "summary"


class VerificationStatus(str, Enum):
    VERIFIED = "VERIFIED"
    REPORTED = "REPORTED"
    UNVERIFIED = "UNVERIFIED"
    UNKNOWN = "UNKNOWN"


class SourceStatus(str, Enum):
    INCLUDED = "included"
    TRUNCATED = "truncated"
    OMITTED = "omitted"


def _is_sha256(value: str) -> bool:
    if len(value) != 64:
        return False
    return all(c in "0123456789abcdef" for c in value)


def _canonical_bytes(obj: Any) -> bytes:
    return json.dumps(
        obj,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _check_json_value(value: Any, path: str) -> None:
    """Reject non-finite floats and non-JSON types recursively."""
    if isinstance(value, bool):
        return
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            raise ValueError(f"{path} must be finite")
        return
    if isinstance(value, int):
        return
    if isinstance(value, str):
        return
    if value is None:
        return
    if isinstance(value, list):
        for i, item in enumerate(value):
            _check_json_value(item, f"{path}[{i}]")
        return
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str):
                raise ValueError(f"{path} keys must be strings")
            _check_json_value(item, f"{path}.{key}")
        return
    raise ValueError(f"{path} contains unsupported type {type(value).__name__}")


class _Frozen(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class Authorization(_Frozen):
    """Explicit authorization boundaries supplied by the trusted caller."""

    digest: str = Field(min_length=1, max_length=MAX_FIELD_CHARS)
    allowed_roots: List[str] = Field(default_factory=list)
    tool_profiles: List[str] = Field(default_factory=list)
    approval_boundaries: List[str] = Field(default_factory=list)
    authorized_source_ids: List[str] = Field(default_factory=list)
    authorized_project_ids: List[str] = Field(default_factory=list)


class Checkpoint(_Frozen):
    """Resumable checkpoint state. Preserved exactly, never reconstructed."""

    completed_ids: List[str]
    pending_ids: List[str]
    code_identity: str = Field(min_length=1, max_length=MAX_FIELD_CHARS)
    data_identity: str = Field(min_length=1, max_length=MAX_FIELD_CHARS)
    retry_counters: Dict[str, int]
    budget_remaining: Dict[str, Any]
    unresolved_operations: List[str]
    latest_review_decisions: List[str]

    @field_validator("retry_counters", mode="before")
    @classmethod
    def _check_counters(cls, v: Any) -> Any:
        if not isinstance(v, dict):
            raise ValueError("retry_counters must be a mapping")
        for key, item in v.items():
            if not isinstance(key, str):
                raise ValueError("retry counter keys must be strings")
            if isinstance(item, bool) or not isinstance(item, int):
                raise ValueError("retry counters must be strict integers")
            if item < 0:
                raise ValueError("retry counters must be nonnegative")
        return v

    @field_validator("budget_remaining", mode="before")
    @classmethod
    def _check_budget(cls, v: Any) -> Any:
        if not isinstance(v, dict):
            raise ValueError("budget_remaining must be a mapping")
        _check_json_value(v, "budget_remaining")
        return v


class ContextHeader(_Frozen):
    """Immutable task header. All fields are preserved exactly."""

    project_id: str = Field(min_length=1, max_length=MAX_FIELD_CHARS)
    task_id: str = Field(min_length=1, max_length=MAX_FIELD_CHARS)
    run_id: str = Field(min_length=1, max_length=MAX_FIELD_CHARS)
    attempt: int = Field(ge=1)
    role: str = Field(min_length=1, max_length=MAX_FIELD_CHARS)
    objective: str = Field(min_length=1, max_length=MAX_FIELD_CHARS)
    constraints: List[str]
    acceptance: List[str]
    authorization: Authorization
    plan_version: str = Field(min_length=1, max_length=MAX_FIELD_CHARS)
    checkpoint: Checkpoint

    @field_validator("attempt", mode="before")
    @classmethod
    def _check_attempt(cls, v: Any) -> Any:
        if isinstance(v, bool) or not isinstance(v, int):
            raise ValueError("attempt must be a strict integer")
        if v < 1:
            raise ValueError("attempt must be >= 1")
        return v


class ContextSource(_Frozen):
    """A single candidate source. Content is the only truncatable field."""

    id: str = Field(min_length=1, max_length=MAX_ID_CHARS)
    scope: SourceScope
    project_id: Optional[str] = Field(default=None, max_length=MAX_ID_CHARS)
    kind: SourceKind
    source_ref: str = Field(min_length=1, max_length=MAX_SOURCE_REF_CHARS)
    content: str = ""
    effective_time: Optional[str] = None
    observed_time: str = Field(min_length=1, max_length=64)
    verification_status: VerificationStatus
    original_sha256: Optional[str] = None
    priority: int = 0

    @field_validator("original_sha256")
    @classmethod
    def _check_hash(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        if not _is_sha256(v):
            raise ValueError("original_sha256 must be 64 lowercase hex chars")
        return v

    @field_validator("effective_time", "observed_time")
    @classmethod
    def _check_time(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        if not _valid_timestamp(v):
            raise ValueError("timestamp must be ISO-8601 UTC/offset or UNKNOWN")
        return v

    @field_validator("priority", mode="before")
    @classmethod
    def _check_priority(cls, v: Any) -> Any:
        if isinstance(v, bool) or not isinstance(v, int):
            raise ValueError("priority must be a strict integer")
        return v

    @model_validator(mode="after")
    def _consistent_scope(self) -> "ContextSource":
        """Scope and project ownership must not contradict each other.

        A ``GLOBAL`` source that carries another project's id would let a
        project-scoped fact travel as a global one, so it is refused before any
        authorization decision is made.
        """
        if self.scope is SourceScope.GLOBAL and self.project_id is not None:
            raise ValueError("a GLOBAL source must not carry a project_id")
        if self.scope is not SourceScope.GLOBAL and not self.project_id:
            raise ValueError(
                f"a {self.scope.value} source must carry its owning project_id"
            )
        return self


class ContextLimits(_Frozen):
    """Hard caps. Booleans and non-finite floats are rejected."""

    max_input_bytes: int = Field(gt=0)
    max_input_tokens: int = Field(gt=0)
    max_sources: int = Field(default=MAX_SOURCES, gt=0, le=MAX_SOURCES)

    @field_validator("max_input_bytes", "max_input_tokens", "max_sources", mode="before")
    @classmethod
    def _reject_bool(cls, v: Any) -> Any:
        if isinstance(v, bool):
            raise ValueError("boolean is not a valid integer limit")
        if isinstance(v, float):
            if math.isnan(v) or math.isinf(v):
                raise ValueError("limit must be finite")
            if not float(v).is_integer():
                raise ValueError("limit must be an integer")
        return v


class SourceManifestEntry(_Frozen):
    id: str
    scope: SourceScope
    project_id: Optional[str] = None
    kind: SourceKind
    source_ref: str
    status: SourceStatus
    priority: int
    verification_status: VerificationStatus
    effective_time: Optional[str] = None
    observed_time: str
    original_sha256: Optional[str] = None
    supplied_content_sha256: str
    included_sha256: Optional[str] = None
    included_bytes: int = 0
    original_bytes: int = 0
    reason: Optional[str] = None


class ContextManifest(_Frozen):
    header_sha256: str
    message_sha256: str
    total_bytes: int
    conservative_token_upper_bound: int
    source_count: int
    included_count: int
    truncated_count: int
    omitted_count: int
    sources: List[SourceManifestEntry]


class AssembledContext(_Frozen):
    """Ready model-message content plus its manifest."""

    content: str
    manifest: ContextManifest


def _source_sort_key(src: ContextSource) -> Tuple[int, str]:
    # Deterministic: higher priority first, then stable ID ascending.
    return (-src.priority, src.id)


def _validate_scope(src: ContextSource) -> None:
    if src.scope is SourceScope.GLOBAL:
        if src.project_id is not None:
            raise ContextPackError(
                f"source {src.id} is GLOBAL and must not carry project_id"
            )
        return
    if not src.project_id:
        raise ContextPackError(
            f"source {src.id} requires project_id for scope {src.scope.value}"
        )


def _authorized(src: ContextSource, header: ContextHeader) -> bool:
    auth = header.authorization
    if src.id not in auth.authorized_source_ids:
        return False
    if src.scope is SourceScope.GLOBAL:
        return True
    if src.scope is SourceScope.PROJECT:
        return src.project_id == header.project_id
    # RESTRICTED requires explicit project match and authorization listing.
    return (
        src.project_id == header.project_id
        and src.project_id in auth.authorized_project_ids
    )


def _validate_sources(
    sources: List[ContextSource], header: ContextHeader, limits: ContextLimits
) -> None:
    if len(sources) > limits.max_sources:
        raise ContextPackError(
            f"too many sources: {len(sources)} > {limits.max_sources}"
        )
    if len(sources) > MAX_SOURCES:
        raise ContextPackError(f"too many sources: {len(sources)} > {MAX_SOURCES}")
    seen: set = set()
    total_content = 0
    total_meta = 0
    for src in sources:
        if src.id in seen:
            raise ContextPackError(f"duplicate source id: {src.id}")
        seen.add(src.id)
        _validate_scope(src)
        if not _authorized(src, header):
            raise ContextPackError(f"source {src.id} is not authorized for this task")
        raw = src.content.encode("utf-8")
        if len(raw) > MAX_SOURCE_TEXT_BYTES:
            raise ContextPackError(f"source {src.id} exceeds per-source byte cap")
        total_content += len(raw)
        # every metadata field that will be serialized is accounted for here,
        # before any candidate message is assembled from it
        meta = _canonical_bytes(
            {
                "id": src.id,
                "scope": src.scope.value,
                "project_id": src.project_id,
                "kind": src.kind.value,
                "source_ref": src.source_ref,
                "effective_time": src.effective_time,
                "observed_time": src.observed_time,
                "verification_status": src.verification_status.value,
                "original_sha256": src.original_sha256,
                "priority": src.priority,
            }
        )
        total_meta += len(meta)
        if total_meta > MAX_METADATA_BYTES:
            raise ContextPackError("total supplied source metadata exceeds cap")
    if total_content > MAX_SUPPLIED_BYTES:
        raise ContextPackError("total supplied source bytes exceed cap")
    header_meta = len(_canonical_bytes(header.model_dump(mode="json")))
    if header_meta > MAX_METADATA_BYTES:
        raise ContextPackError("supplied header metadata exceeds cap")


def _header_payload(header: ContextHeader) -> Dict[str, Any]:
    return {
        "kind": "context_header",
        "data": header.model_dump(mode="json"),
    }


def _source_payload(src: ContextSource, content: str) -> Dict[str, Any]:
    return {
        "kind": "context_source",
        "data": {
            "id": src.id,
            "scope": src.scope.value,
            "project_id": src.project_id,
            "kind": src.kind.value,
            "source_ref": src.source_ref,
            "effective_time": src.effective_time,
            "observed_time": src.observed_time,
            "verification_status": src.verification_status.value,
            "original_sha256": src.original_sha256,
            "priority": src.priority,
            "content": content,
        },
    }


def _manifest_payload(entries: List[SourceManifestEntry]) -> Dict[str, Any]:
    """The per-source manifest block carried inside the wire message.

    It deliberately contains no self-referential accounting: byte counts, token
    bounds and content hashes of the *message* live only in the external
    :class:`ContextManifest`, whose hash domain is the final wire bytes. That
    keeps one canonical payload instead of two disagreeing descriptions of it.
    """
    return {
        "kind": "context_manifest",
        "data": {
            "sources": [e.model_dump(mode="json") for e in entries],
        },
    }


def _build_message(
    header: ContextHeader,
    included: List[Tuple[ContextSource, str]],
    entries: List[SourceManifestEntry],
) -> str:
    blocks: List[Dict[str, Any]] = [_header_payload(header)]
    for src, content in included:
        blocks.append(_source_payload(src, content))
    blocks.append(_manifest_payload(entries))
    return json.dumps(
        {"context": blocks},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _truncate_utf8(text: str, max_bytes: int) -> str:
    """Truncate to at most max_bytes at a UTF-8 character boundary."""
    if max_bytes <= 0:
        return ""
    raw = text.encode("utf-8")
    if len(raw) <= max_bytes:
        return text
    cut = raw[:max_bytes]
    while cut:
        try:
            return cut.decode("utf-8")
        except UnicodeDecodeError:
            cut = cut[:-1]
    return ""


def _make_entry(
    src: ContextSource,
    status: SourceStatus,
    included_content: Optional[str],
    reason: Optional[str] = None,
) -> SourceManifestEntry:
    """Build one manifest entry, always retaining the *supplied* identity.

    ``supplied_content_sha256`` is computed from the bytes the caller actually
    supplied, so it is present for included, truncated and omitted entries
    alike. Two different supplied sources can therefore never share an identity
    merely because both were omitted or because their included prefixes match.
    ``original_sha256`` is the caller's separately-scoped claim about the
    upstream original and is never conflated with the supplied content.
    """
    original_bytes = len(src.content.encode("utf-8"))
    supplied_hash = _sha256_hex(src.content.encode("utf-8"))
    if included_content is None:
        return SourceManifestEntry(
            id=src.id,
            scope=src.scope,
            project_id=src.project_id,
            kind=src.kind,
            source_ref=src.source_ref,
            status=status,
            priority=src.priority,
            verification_status=src.verification_status,
            effective_time=src.effective_time,
            observed_time=src.observed_time,
            original_sha256=src.original_sha256,
            supplied_content_sha256=supplied_hash,
            included_sha256=None,
            included_bytes=0,
            original_bytes=original_bytes,
            reason=reason,
        )
    inc_bytes = included_content.encode("utf-8")
    return SourceManifestEntry(
        id=src.id,
        scope=src.scope,
        project_id=src.project_id,
        kind=src.kind,
        source_ref=src.source_ref,
        status=status,
        priority=src.priority,
        verification_status=src.verification_status,
        effective_time=src.effective_time,
        observed_time=src.observed_time,
        original_sha256=src.original_sha256,
        supplied_content_sha256=supplied_hash,
        included_sha256=_sha256_hex(inc_bytes),
        included_bytes=len(inc_bytes),
        original_bytes=original_bytes,
        reason=reason,
    )


def _assemble_manifest(
    header: ContextHeader,
    entries: List[SourceManifestEntry],
    message: str,
) -> ContextManifest:
    included = sum(1 for e in entries if e.status is SourceStatus.INCLUDED)
    truncated = sum(1 for e in entries if e.status is SourceStatus.TRUNCATED)
    omitted = sum(1 for e in entries if e.status is SourceStatus.OMITTED)
    msg_bytes = message.encode("utf-8")
    header_bytes = _canonical_bytes(_header_payload(header))
    return ContextManifest(
        header_sha256=_sha256_hex(header_bytes),
        message_sha256=_sha256_hex(msg_bytes),
        total_bytes=len(msg_bytes),
        conservative_token_upper_bound=len(msg_bytes),
        source_count=len(entries),
        included_count=included,
        truncated_count=truncated,
        omitted_count=omitted,
        sources=entries,
    )


def _fits(message: str, limits: ContextLimits) -> bool:
    n = len(message.encode("utf-8"))
    return n <= limits.max_input_bytes and n <= limits.max_input_tokens


def assemble_context(
    header: ContextHeader,
    sources: List[ContextSource],
    limits: ContextLimits,
) -> AssembledContext:
    """Assemble a bounded, deterministic model-context message.

    Raises ``ContextBudgetExceeded`` when pinned header plus minimal manifest
    metadata cannot fit, or when required content cannot be represented.
    """
    _validate_sources(sources, header, limits)

    ordered = sorted(sources, key=_source_sort_key)

    def entries_for(inc: List[Tuple[ContextSource, str]]) -> List[SourceManifestEntry]:
        inc_map = {s.id: c for s, c in inc}
        out: List[SourceManifestEntry] = []
        for s in ordered:
            if s.id in inc_map:
                content = inc_map[s.id]
                status = (
                    SourceStatus.INCLUDED
                    if content == s.content
                    else SourceStatus.TRUNCATED
                )
                out.append(_make_entry(s, status, content))
            else:
                out.append(_make_entry(s, SourceStatus.OMITTED, None, reason="budget"))
        return out

    def build(inc: List[Tuple[ContextSource, str]]) -> Tuple[str, ContextManifest]:
        entries = entries_for(inc)
        msg = _build_message(header, inc, entries)
        manifest = _assemble_manifest(header, entries, msg)
        return msg, manifest

    # Fast path: everything fits.
    full = [(s, s.content) for s in ordered]
    message, manifest = build(full)
    if _fits(message, limits):
        return AssembledContext(content=message, manifest=manifest)

    # Greedily include sources in priority order. The all-omitted packet emitted
    # here is the canonical floor: it is the smallest admissible rendering.
    minimal_msg, minimal_manifest = build([])
    if not _fits(minimal_msg, limits):
        raise ContextBudgetExceeded(
            "pinned header and minimal manifest metadata exceed the budget"
        )
    # Admission is mostly all-or-nothing so a caller can predict what it gets:
    # every source except the first one that does not fit is included whole.
    # That one source may contribute a UTF-8 aligned prefix, and everything with
    # lower priority stays omitted with full provenance.
    accepted: List[Tuple[ContextSource, str]] = []
    for src in ordered:
        trial_msg, _ = build(accepted + [(src, src.content)])
        if _fits(trial_msg, limits):
            accepted = accepted + [(src, src.content)]
            continue
        low, high = 0, len(src.content.encode("utf-8"))
        best: Optional[str] = None
        while low <= high:
            middle = (low + high) // 2
            candidate = _truncate_utf8(src.content, middle)
            trial_msg, _ = build(accepted + [(src, candidate)])
            if _fits(trial_msg, limits):
                best = candidate
                low = middle + 1
            else:
                high = middle - 1
        if best:
            accepted = accepted + [(src, best)]
        break

    final_msg, manifest = build(accepted)
    if not _fits(final_msg, limits):
        raise ContextBudgetExceeded("assembled context exceeds budget")
    return AssembledContext(content=final_msg, manifest=manifest)
