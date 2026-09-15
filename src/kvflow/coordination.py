"""Semantic coordination: contracts, ownership, decisions, artifacts, idempotency.

This module is the product's answer to a specific class of failure: several workers
produce work that is locally correct and textually mergeable, yet semantically
incompatible - two spellings of one enum, a number where a string was promised, a
result computed against a contract version that no longer exists.

Everything here is durable and project-scoped. Nothing is inferred from a model's
memory, a chat summary or a worker's own claim:

* **Semantic contracts** are append-only. A version is written once and never
  edited; a change is a new version, produced only by an approved
  :class:`ContractChangeRequest`.
* **Canonical ownership** gives every shared object exactly one authoritative
  writer. A non-owner may read and may propose, and a write from anyone else is
  refused - never merged last-write-wins.
* **Compare-and-swap** guards every shared mutable row with a ``revision`` and a
  ``content_hash``; a write that names a stale revision is refused.
* **The operation ledger** makes internal effects idempotent: one idempotency key
  plus one effect hash is applied once, and the same key with different content is
  a conflict rather than a second execution.
* **Staleness** is decided by comparing a frozen identity (contract versions,
  decision set hash, artifact versions, revisions, lease fence) against the
  authoritative rows at submit time.

The module never calls a model and never writes outside the store it was given.
Evidence that a *model* would have to judge is recorded as evidence; it is not
invented here.
"""

from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping, Sequence

from .core.errors import ConflictError, ContractError, NotFoundError, V1Error
from .core.hashing import canonical_json, hash_json, sha256_text
from .core.store import Store

__all__ = [
    "integration_layers",
    "CoordinationError", "VersionConflict", "WriteDenied", "StaleResult",
    "IdempotencyConflict", "OutcomeUnknown", "SemanticCoordinator",
    "COORDINATION_DDL", "CONTRACT_CHANGE_CLASSES",
]


class CoordinationError(V1Error):
    code = "SEMANTIC_CONFLICT"


class VersionConflict(CoordinationError):
    """A compare-and-swap write named a revision that is no longer current."""

    code = "CONCURRENT_MODIFICATION"


class WriteDenied(CoordinationError):
    """A writer that is not the canonical owner tried to change a shared object."""

    code = "WRITE_DENIED"


class StaleResult(CoordinationError):
    """A result was produced against an identity that has since changed."""

    code = "STALE_RESULT"


class IdempotencyConflict(CoordinationError):
    """One idempotency key was reused for a different effect."""

    code = "IDEMPOTENCY_CONFLICT"


class OutcomeUnknown(CoordinationError):
    """An external effect may or may not have happened; reconcile before retrying."""

    code = "OUTCOME_UNKNOWN"


#: how a contract bump affects the tasks that depend on it, worst case first
CONTRACT_CHANGE_CLASSES = (
    "NOT_STARTED_UPDATE_DEPENDENCY",
    "RUNNING_STALE_PENDING_REVIEW",
    "COMPLETED_STALE_RESULT",
    "INTEGRATED_CHANGE_IMPACT_REVIEW",
)

OPERATION_STATES = ("PLANNED", "IN_PROGRESS", "APPLIED", "FAILED", "OUTCOME_UNKNOWN",
                    "RECONCILED")

COORDINATION_DDL = """
CREATE TABLE IF NOT EXISTS semantic_contracts (
    contract_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    project_id TEXT NOT NULL,
    job_id TEXT,
    scope TEXT NOT NULL,
    document TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    created_by TEXT NOT NULL,
    effective_at TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (project_id, contract_id, version)
);

CREATE TABLE IF NOT EXISTS canonical_resources (
    resource_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    owner_id TEXT NOT NULL,
    revision INTEGER NOT NULL,
    content_hash TEXT NOT NULL,
    document TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (resource_id, project_id)
);

CREATE TABLE IF NOT EXISTS contract_change_requests (
    request_id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    job_id TEXT,
    contract_id TEXT NOT NULL,
    base_version INTEGER NOT NULL,
    task_id TEXT,
    problem TEXT NOT NULL,
    proposed_change TEXT NOT NULL,
    reason TEXT NOT NULL,
    affected_artifacts TEXT NOT NULL,
    affected_tasks TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('PENDING', 'APPROVED', 'REJECTED', 'MODIFIED')),
    decided_by TEXT,
    decided_at TEXT,
    resulting_version INTEGER,
    document TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS decisions (
    decision_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    job_id TEXT,
    scope TEXT NOT NULL,
    version INTEGER NOT NULL,
    summary TEXT NOT NULL,
    reason_summary TEXT NOT NULL,
    manager_id TEXT NOT NULL,
    effective_at TEXT NOT NULL,
    affected_contracts TEXT NOT NULL,
    affected_tasks TEXT NOT NULL,
    supersedes TEXT,
    status TEXT NOT NULL CHECK (status IN ('ACTIVE', 'SUPERSEDED', 'REVOKED')),
    content_hash TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (project_id, decision_id, version)
);

CREATE TABLE IF NOT EXISTS artifact_registry (
    artifact_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    job_id TEXT,
    version INTEGER NOT NULL,
    producer_task TEXT NOT NULL,
    producer_run TEXT,
    semantic_contract_version INTEGER,
    contract_id TEXT,
    content_hash TEXT NOT NULL,
    document TEXT,
    created_at TEXT NOT NULL,
    PRIMARY KEY (artifact_id, project_id, version)
);

CREATE TABLE IF NOT EXISTS artifact_requirements (
    artifact_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    consumer_task TEXT NOT NULL,
    required_version INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (artifact_id, project_id, version, consumer_task)
);

CREATE TABLE IF NOT EXISTS operation_ledger (
    op_key TEXT PRIMARY KEY,
    idempotency_key TEXT NOT NULL,
    project_id TEXT NOT NULL,
    job_id TEXT,
    task_id TEXT,
    run_id TEXT,
    attempt INTEGER,
    tool TEXT NOT NULL,
    effect_type TEXT NOT NULL,
    target TEXT NOT NULL,
    expected_state_version INTEGER,
    arguments_hash TEXT NOT NULL,
    effect_hash TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN
        ('PLANNED', 'IN_PROGRESS', 'APPLIED', 'FAILED', 'OUTCOME_UNKNOWN', 'RECONCILED')),
    detail TEXT,
    reconciled_by TEXT,
    created_at TEXT NOT NULL,
    completed_at TEXT,
    UNIQUE (project_id, idempotency_key)
);

CREATE TABLE IF NOT EXISTS requirements (
    req_key TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    job_id TEXT,
    req_id TEXT NOT NULL,
    source TEXT NOT NULL,
    original_request_hash TEXT NOT NULL,
    normalized_requirement TEXT NOT NULL,
    acceptance TEXT NOT NULL,
    criticality TEXT NOT NULL CHECK (criticality IN ('CRITICAL', 'IMPORTANT', 'MINOR')),
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS requirement_links (
    req_key TEXT NOT NULL,
    project_id TEXT NOT NULL,
    node_id TEXT,
    role TEXT NOT NULL CHECK (role IN ('IMPLEMENTS', 'VALIDATES')),
    evidence TEXT,
    created_at TEXT NOT NULL,
    PRIMARY KEY (req_key, project_id, node_id, role)
);

CREATE TABLE IF NOT EXISTS global_invariants (
    invariant_key TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    job_id TEXT,
    invariant_id TEXT NOT NULL,
    description TEXT NOT NULL,
    severity TEXT NOT NULL CHECK (severity IN ('CRITICAL', 'MAJOR', 'MINOR')),
    validation_method TEXT NOT NULL,
    required_evidence TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS invariant_results (
    invariant_key TEXT NOT NULL,
    project_id TEXT NOT NULL,
    result TEXT NOT NULL CHECK (result IN ('PASS', 'FAIL', 'NOT_TESTABLE')),
    evidence TEXT,
    validated_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (invariant_key, project_id, validated_by, created_at)
);

CREATE TABLE IF NOT EXISTS validation_paths (
    validation_id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    job_id TEXT,
    subject_node TEXT NOT NULL,
    validator_node TEXT NOT NULL,
    path_kind TEXT NOT NULL,
    independent INTEGER NOT NULL,
    result TEXT NOT NULL CHECK (result IN ('MATCH', 'MISMATCH', 'NOT_RUN')),
    evidence TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS submissions (
    submission_id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    job_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    frozen TEXT NOT NULL,
    verdict TEXT NOT NULL CHECK (verdict IN ('CURRENT', 'STALE_RESULT')),
    reasons TEXT NOT NULL,
    created_at TEXT NOT NULL
);
"""


def _iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:20]}"


def _loads(text: str | None, default: Any) -> Any:
    if not text:
        return default
    try:
        return json.loads(text)
    except ValueError:  # pragma: no cover - defensive
        return default


def _dump(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


def _rows(conn: sqlite3.Connection, sql: str, args: Sequence[Any] = ()) -> list[dict]:
    return [dict(row) for row in conn.execute(sql, tuple(args)).fetchall()]


def ensure_schema(store: Store) -> None:
    """Create the coordination tables. Idempotent, and never touches core tables."""
    conn = store.connect()
    try:
        conn.executescript(COORDINATION_DDL)
    finally:
        conn.close()


class SemanticCoordinator:
    """Durable semantic state for one project (optionally narrowed to one job)."""

    def __init__(self, store: Store, *, project_id: str, job_id: str | None = None) -> None:
        self.store = store
        self.project_id = project_id
        self.job_id = job_id
        ensure_schema(store)

    # ------------------------------------------------------------- contracts
    def publish_contract(self, *, contract_id: str, scope: str, document: Mapping[str, Any],
                         created_by: str, expected_version: int | None = None) -> dict[str, Any]:
        """Append one contract version, refusing to edit history.

        ``expected_version`` turns the append into a compare-and-swap: a writer that
        read v1 and arrives after v2 exists is refused instead of silently creating
        v3 on top of someone else's change.
        """
        if not contract_id or not scope:
            raise ContractError("a contract needs an id and a scope")
        payload = dict(document)
        content_hash = hash_json({"scope": scope, "document": payload})
        with self.store.tx() as conn:
            latest = conn.execute(
                "SELECT version FROM semantic_contracts WHERE project_id = ?"
                " AND contract_id = ? ORDER BY version DESC LIMIT 1",
                (self.project_id, contract_id),
            ).fetchone()
            current = int(latest["version"]) if latest else 0
            if expected_version is not None and int(expected_version) != current:
                raise VersionConflict(
                    "the contract changed since this writer read it",
                    contract_id=contract_id, expected_version=int(expected_version),
                    current_version=current,
                )
            version = current + 1
            conn.execute(
                "INSERT INTO semantic_contracts(contract_id, version, project_id, job_id,"
                " scope, document, content_hash, created_by, effective_at, created_at)"
                " VALUES (?,?,?,?,?,?,?,?,?,?)",
                (contract_id, version, self.project_id, self.job_id, scope, _dump(payload),
                 content_hash, created_by, _iso(), _iso()),
            )
        return {
            "contract_id": contract_id, "version": version, "scope": scope,
            "content_hash": content_hash, "created_by": created_by,
            "document": payload,
        }

    def contract(self, contract_id: str, version: int | None = None) -> dict[str, Any]:
        with self.store.read() as conn:
            if version is None:
                row = conn.execute(
                    "SELECT * FROM semantic_contracts WHERE project_id = ? AND contract_id = ?"
                    " ORDER BY version DESC LIMIT 1", (self.project_id, contract_id)).fetchone()
            else:
                row = conn.execute(
                    "SELECT * FROM semantic_contracts WHERE project_id = ? AND contract_id = ?"
                    " AND version = ?", (self.project_id, contract_id, int(version))).fetchone()
        if row is None:
            raise NotFoundError("no such semantic contract", contract_id=contract_id,
                                version=version, project_id=self.project_id)
        record = dict(row)
        record["document"] = _loads(record["document"], {})
        return record

    def contract_versions(self, contract_id: str) -> list[int]:
        with self.store.read() as conn:
            rows = conn.execute(
                "SELECT version FROM semantic_contracts WHERE project_id = ? AND"
                " contract_id = ? ORDER BY version", (self.project_id, contract_id)).fetchall()
        return [int(row["version"]) for row in rows]

    def contracts(self) -> list[dict[str, Any]]:
        with self.store.read() as conn:
            rows = _rows(conn,
                         "SELECT contract_id, MAX(version) AS version, scope, content_hash,"
                         " created_by FROM semantic_contracts WHERE project_id = ?"
                         " GROUP BY contract_id ORDER BY contract_id", (self.project_id,))
        return rows

    # ------------------------------------------------------------- ownership
    def register_resource(self, *, resource_id: str, kind: str, owner_id: str,
                          document: Mapping[str, Any]) -> dict[str, Any]:
        """Declare the single authoritative owner of a shared object."""
        content_hash = hash_json(document)
        with self.store.tx() as conn:
            conn.execute(
                "INSERT INTO canonical_resources(resource_id, project_id, kind, owner_id,"
                " revision, content_hash, document, updated_at) VALUES (?,?,?,?,?,?,?,?)"
                " ON CONFLICT(resource_id, project_id) DO NOTHING",
                (resource_id, self.project_id, kind, owner_id, 1, content_hash,
                 _dump(document), _iso()),
            )
        return self.resource(resource_id)

    def resource(self, resource_id: str) -> dict[str, Any]:
        with self.store.read() as conn:
            row = conn.execute(
                "SELECT * FROM canonical_resources WHERE project_id = ? AND resource_id = ?",
                (self.project_id, resource_id)).fetchone()
        if row is None:
            raise NotFoundError("no such canonical resource", resource_id=resource_id,
                                project_id=self.project_id)
        record = dict(row)
        record["document"] = _loads(record["document"], {})
        return record

    def write_canonical(self, *, resource_id: str, writer_id: str,
                        document: Mapping[str, Any],
                        expected_revision: int | None = None,
                        expected_hash: str | None = None) -> dict[str, Any]:
        """The only way a canonical object changes: owner + CAS, or a refusal.

        Three distinct refusals, because they mean different things to the caller:
        a non-owner is told to raise a contract change request, a stale revision is
        told to re-read, and a hash mismatch is told the bytes moved.
        """
        content_hash = hash_json(document)
        with self.store.tx() as conn:
            row = conn.execute(
                "SELECT * FROM canonical_resources WHERE project_id = ? AND resource_id = ?",
                (self.project_id, resource_id)).fetchone()
            if row is None:
                raise NotFoundError("no such canonical resource", resource_id=resource_id)
            current = dict(row)
            if current["owner_id"] != writer_id:
                raise WriteDenied(
                    "only the canonical owner may change this object",
                    resource_id=resource_id, owner=current["owner_id"], writer=writer_id,
                    remedy="submit a CONTRACT_CHANGE_REQUEST to the owner or the manager",
                    next_action="CONTRACT_CHANGE_REQUIRED",
                )
            if expected_revision is not None and int(expected_revision) != int(current["revision"]):
                raise VersionConflict(
                    "the canonical object changed since this writer read it",
                    resource_id=resource_id, expected_revision=int(expected_revision),
                    current_revision=int(current["revision"]),
                )
            if expected_hash is not None and expected_hash != current["content_hash"]:
                raise VersionConflict(
                    "the canonical object's content hash moved",
                    resource_id=resource_id, expected_hash=expected_hash,
                    current_hash=current["content_hash"],
                )
            revision = int(current["revision"]) + 1
            conn.execute(
                "UPDATE canonical_resources SET revision = ?, content_hash = ?, document = ?,"
                " updated_at = ? WHERE project_id = ? AND resource_id = ?",
                (revision, content_hash, _dump(document), _iso(), self.project_id,
                 resource_id),
            )
        return {"resource_id": resource_id, "revision": revision,
                "content_hash": content_hash, "owner_id": writer_id,
                "previous_revision": int(current["revision"])}

    # --------------------------------------------------- contract change flow
    def request_contract_change(self, *, contract_id: str, base_version: int, problem: str,
                                proposed_change: Mapping[str, Any], reason: str,
                                task_id: str | None = None,
                                affected_artifacts: Sequence[str] = (),
                                affected_tasks: Sequence[str] = ()) -> dict[str, Any]:
        """A worker never edits a contract: it files one of these."""
        self.contract(contract_id)  # existence is checked in this project
        request_id = _new_id("crq")
        with self.store.tx() as conn:
            conn.execute(
                "INSERT INTO contract_change_requests(request_id, project_id, job_id,"
                " contract_id, base_version, task_id, problem, proposed_change, reason,"
                " affected_artifacts, affected_tasks, status, document, created_at)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (request_id, self.project_id, self.job_id, contract_id, int(base_version),
                 task_id, problem, _dump(proposed_change), reason,
                 _dump(list(affected_artifacts)), _dump(list(affected_tasks)), "PENDING",
                 _dump(proposed_change), _iso()),
            )
        return {"request_id": request_id, "contract_id": contract_id,
                "base_version": int(base_version), "status": "PENDING"}

    def decide_change_request(self, *, request_id: str, decision: str, manager_id: str,
                              modified: Mapping[str, Any] | None = None,
                              task_states: Mapping[str, str] | None = None) -> dict[str, Any]:
        """Manager decides; approval writes a new version and classifies the fallout.

        The classification is derived from the durable task states the caller
        passes (or from the node table), never from what a worker claims about
        itself, and every affected task is reported with the action it needs.
        """
        decision = decision.upper()
        if decision not in {"APPROVE", "REJECT", "MODIFY"}:
            raise ContractError("a change request decision is APPROVE, REJECT or MODIFY")
        with self.store.tx() as conn:
            row = conn.execute(
                "SELECT * FROM contract_change_requests WHERE project_id = ? AND request_id = ?",
                (self.project_id, request_id)).fetchone()
            if row is None:
                raise NotFoundError("no such contract change request", request_id=request_id)
            request = dict(row)
            if request["status"] != "PENDING":
                raise ConflictError("this change request was already decided",
                                    request_id=request_id, status=request["status"])
            if decision == "REJECT":
                conn.execute(
                    "UPDATE contract_change_requests SET status = 'REJECTED', decided_by = ?,"
                    " decided_at = ? WHERE request_id = ?",
                    (manager_id, _iso(), request_id))
                return {"request_id": request_id, "status": "REJECTED",
                        "contract_id": request["contract_id"], "version": None}
            document = dict(modified if decision == "MODIFY" and modified is not None
                            else _loads(request["document"], {}))
            latest = conn.execute(
                "SELECT version FROM semantic_contracts WHERE project_id = ? AND"
                " contract_id = ? ORDER BY version DESC LIMIT 1",
                (self.project_id, request["contract_id"])).fetchone()
            version = (int(latest["version"]) if latest else 0) + 1
            content_hash = hash_json({"scope": request["contract_id"], "document": document})
            conn.execute(
                "INSERT INTO semantic_contracts(contract_id, version, project_id, job_id,"
                " scope, document, content_hash, created_by, effective_at, created_at)"
                " VALUES (?,?,?,?,?,?,?,?,?,?)",
                (request["contract_id"], version, self.project_id, self.job_id,
                 request["contract_id"], _dump(document), content_hash,
                 f"manager:{manager_id}", _iso(), _iso()),
            )
            stored_status = {"APPROVE": "APPROVED", "MODIFY": "MODIFIED"}[decision]
            conn.execute(
                "UPDATE contract_change_requests SET status = ?, decided_by = ?, decided_at = ?,"
                " resulting_version = ?, document = ? WHERE request_id = ?",
                (stored_status, manager_id, _iso(), version, _dump(document), request_id),
            )

        affected = list(_loads(request["affected_tasks"], []))
        states = dict(task_states or {})
        if not states and affected:
            with self.store.read() as conn:
                for node_id in affected:
                    node = conn.execute(
                        "SELECT state FROM nodes WHERE job_id = ? AND node_id = ?",
                        (self.job_id, node_id)).fetchone()
                    states[node_id] = str(node["state"]) if node else "UNKNOWN"
        fallout = [{"task_id": node_id,
                    "state": states.get(node_id, "UNKNOWN"),
                    "action": _change_action(states.get(node_id, "UNKNOWN"))}
                   for node_id in affected]
        stale = [item["task_id"] for item in fallout
                 if item["action"] in {"STALE_PENDING_REVIEW", "STALE_RESULT",
                                       "CHANGE_IMPACT_REVIEW"}]
        for node_id in stale:
            self.mark_submission_stale(node_id, reason=f"contract {request['contract_id']}"
                                                     f" changed to v{version}")
        return {
            "request_id": request_id, "status": decision,
            "contract_id": request["contract_id"], "version": version,
            "previous_version": int(request["base_version"]), "content_hash": content_hash,
            "fallout": fallout, "stale_tasks": stale,
            "artifact_impact": list(_loads(request["affected_artifacts"], [])),
        }

    def change_requests(self, *, status: str | None = None) -> list[dict[str, Any]]:
        sql = ("SELECT request_id, contract_id, base_version, status, task_id,"
               " resulting_version, decided_by FROM contract_change_requests"
               " WHERE project_id = ?")
        args: list[Any] = [self.project_id]
        if status:
            sql += " AND status = ?"
            args.append(status.upper())
        sql += " ORDER BY created_at"
        with self.store.read() as conn:
            return _rows(conn, sql, args)

    # -------------------------------------------------------------- decisions
    def record_decision(self, *, scope: str, summary: str, reason_summary: str,
                        manager_id: str, affected_contracts: Sequence[str] = (),
                        affected_tasks: Sequence[str] = (),
                        supersedes: str | None = None) -> dict[str, Any]:
        """Append one design decision, superseding an earlier one when asked."""
        decision_id = supersedes or _new_id("dec")
        body = {"scope": scope, "summary": summary, "reason_summary": reason_summary,
                "affected_contracts": list(affected_contracts),
                "affected_tasks": list(affected_tasks)}
        content_hash = hash_json(body)
        with self.store.tx() as conn:
            if supersedes:
                previous = conn.execute(
                    "SELECT MAX(version) AS version FROM decisions WHERE project_id = ?"
                    " AND decision_id = ?", (self.project_id, supersedes)).fetchone()
                version = int(previous["version"] or 0) + 1
                conn.execute(
                    "UPDATE decisions SET status = 'SUPERSEDED' WHERE project_id = ?"
                    " AND decision_id = ? AND status = 'ACTIVE'",
                    (self.project_id, supersedes))
            else:
                version = 1
            conn.execute(
                "INSERT INTO decisions(decision_id, project_id, job_id, scope, version,"
                " summary, reason_summary, manager_id, effective_at, affected_contracts,"
                " affected_tasks, supersedes, status, content_hash, created_at)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (decision_id, self.project_id, self.job_id, scope, version, summary,
                 reason_summary, manager_id, _iso(), _dump(list(affected_contracts)),
                 _dump(list(affected_tasks)), supersedes, "ACTIVE", content_hash, _iso()),
            )
        return {"decision_id": decision_id, "version": version, "status": "ACTIVE",
                "content_hash": content_hash, "supersedes": supersedes}

    def revoke_decision(self, *, decision_id: str) -> dict[str, Any]:
        with self.store.tx() as conn:
            changed = conn.execute(
                "UPDATE decisions SET status = 'REVOKED' WHERE project_id = ?"
                " AND decision_id = ? AND status = 'ACTIVE'",
                (self.project_id, decision_id)).rowcount
        if not changed:
            raise NotFoundError("no active decision to revoke", decision_id=decision_id)
        return {"decision_id": decision_id, "status": "REVOKED"}

    def active_decisions(self) -> list[dict[str, Any]]:
        """The decision set a worker's context must be built from."""
        with self.store.read() as conn:
            rows = _rows(conn,
                         "SELECT decision_id, version, scope, summary, reason_summary,"
                         " manager_id, effective_at, affected_contracts, affected_tasks,"
                         " content_hash FROM decisions WHERE project_id = ?"
                         " AND status = 'ACTIVE' ORDER BY decision_id", (self.project_id,))
        for row in rows:
            row["affected_contracts"] = _loads(row["affected_contracts"], [])
            row["affected_tasks"] = _loads(row["affected_tasks"], [])
        return rows

    def decision_set_hash(self) -> str:
        return hash_json(self.active_decisions())

    # -------------------------------------------------------------- artifacts
    def register_artifact(self, *, artifact_id: str, producer_task: str,
                          content_hash: str, producer_run: str | None = None,
                          contract_id: str | None = None,
                          semantic_contract_version: int | None = None,
                          requires: Sequence[tuple[str, int]] = (),
                          document: Mapping[str, Any] | None = None,
                          version: int | None = None) -> dict[str, Any]:
        """Record a produced artifact version and the versions it depends on."""
        with self.store.tx() as conn:
            if version is None:
                row = conn.execute(
                    "SELECT MAX(version) AS version FROM artifact_registry WHERE project_id = ?"
                    " AND artifact_id = ?", (self.project_id, artifact_id)).fetchone()
                version = int(row["version"] or 0) + 1
            conn.execute(
                "INSERT INTO artifact_registry(artifact_id, project_id, job_id, version,"
                " producer_task, producer_run, semantic_contract_version, contract_id,"
                " content_hash, document, created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (artifact_id, self.project_id, self.job_id, int(version), producer_task,
                 producer_run, semantic_contract_version, contract_id, content_hash,
                 _dump(document or {}), _iso()),
            )
            for required_id, required_version in requires:
                conn.execute(
                    "INSERT OR REPLACE INTO artifact_requirements(artifact_id, project_id,"
                    " version, consumer_task, required_version, created_at)"
                    " VALUES (?,?,?,?,?,?)",
                    (required_id, self.project_id, int(version), producer_task,
                     int(required_version), _iso()),
                )
        return {"artifact_id": artifact_id, "version": int(version),
                "content_hash": content_hash, "producer_task": producer_task,
                "requires": [{"artifact_id": key, "version": value}
                             for key, value in requires]}

    def artifacts(self) -> list[dict[str, Any]]:
        with self.store.read() as conn:
            return _rows(conn,
                         "SELECT artifact_id, MAX(version) AS version, producer_task,"
                         " content_hash, contract_id, semantic_contract_version"
                         " FROM artifact_registry WHERE project_id = ?"
                         " GROUP BY artifact_id ORDER BY artifact_id", (self.project_id,))

    def missing_required_artifacts(self, *, needed: Mapping[str, int]) -> list[dict[str, Any]]:
        """Which required artifact versions do not exist yet, or exist too old."""
        missing: list[dict[str, Any]] = []
        with self.store.read() as conn:
            for artifact_id, version in needed.items():
                row = conn.execute(
                    "SELECT MAX(version) AS version FROM artifact_registry WHERE project_id = ?"
                    " AND artifact_id = ?", (self.project_id, artifact_id)).fetchone()
                current = int(row["version"] or 0)
                if current < int(version):
                    missing.append({"artifact_id": artifact_id, "required": int(version),
                                    "available": current})
        return missing

    # ------------------------------------------------------------- operations
    def begin_operation(self, *, idempotency_key: str, tool: str, effect_type: str,
                        target: str, arguments: Mapping[str, Any],
                        effect: Mapping[str, Any] | None = None, task_id: str | None = None,
                        run_id: str | None = None, attempt: int | None = None,
                        expected_state_version: int | None = None) -> dict[str, Any]:
        """Claim one side effect. A repeat of the same effect is never executed twice.

        Returns ``status`` of ``NEW``, ``ALREADY_APPLIED`` (same key, same effect, the
        effect is on record) or ``IN_PROGRESS``. A key reused with different content
        raises :class:`IdempotencyConflict` and nothing is written.
        """
        arguments_hash = hash_json(arguments)
        effect_hash = hash_json(effect if effect is not None else arguments)
        op_key = f"{self.project_id}:{idempotency_key}"
        with self.store.tx() as conn:
            row = conn.execute(
                "SELECT * FROM operation_ledger WHERE project_id = ? AND idempotency_key = ?",
                (self.project_id, idempotency_key)).fetchone()
            if row is not None:
                existing = dict(row)
                if (existing["arguments_hash"] != arguments_hash
                        or existing["effect_hash"] != effect_hash):
                    raise IdempotencyConflict(
                        "this idempotency key was used for a different effect",
                        idempotency_key=idempotency_key, target=target,
                        recorded_effect=existing["effect_hash"], offered_effect=effect_hash,
                    )
                if existing["status"] in {"APPLIED", "RECONCILED"}:
                    return {**existing, "operation_id": existing["op_key"],
                            "status": "ALREADY_APPLIED", "duplicate": True,
                            "previous_status": existing["status"]}
                if existing["status"] == "OUTCOME_UNKNOWN":
                    raise OutcomeUnknown(
                        "this effect's outcome is unknown; reconcile before retrying",
                        idempotency_key=idempotency_key, target=target,
                        next_action="RECONCILE",
                    )
                conn.execute(
                    "UPDATE operation_ledger SET status = 'IN_PROGRESS' WHERE op_key = ?",
                    (existing["op_key"],))
                return {**existing, "operation_id": existing["op_key"],
                        "status": "IN_PROGRESS", "duplicate": False}
            conn.execute(
                "INSERT INTO operation_ledger(op_key, idempotency_key, project_id, job_id,"
                " task_id, run_id, attempt, tool, effect_type, target, expected_state_version,"
                " arguments_hash, effect_hash, status, created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (op_key, idempotency_key, self.project_id, self.job_id, task_id, run_id,
                 attempt, tool, effect_type, target, expected_state_version, arguments_hash,
                 effect_hash, "IN_PROGRESS", _iso()),
            )
        return {"operation_id": op_key, "idempotency_key": idempotency_key,
                "status": "NEW", "duplicate": False, "effect_hash": effect_hash}

    def complete_operation(self, *, idempotency_key: str, status: str = "APPLIED",
                           detail: str | None = None) -> dict[str, Any]:
        if status not in OPERATION_STATES:
            raise ContractError("unknown operation status", status=status)
        with self.store.tx() as conn:
            changed = conn.execute(
                "UPDATE operation_ledger SET status = ?, detail = ?, completed_at = ?"
                " WHERE project_id = ? AND idempotency_key = ?",
                (status, detail, _iso(), self.project_id, idempotency_key)).rowcount
        if not changed:
            raise NotFoundError("no such operation", idempotency_key=idempotency_key)
        return {"idempotency_key": idempotency_key, "status": status}

    def mark_outcome_unknown(self, *, idempotency_key: str, detail: str) -> dict[str, Any]:
        """An effect that may or may not have happened. Never retried blindly."""
        return self.complete_operation(idempotency_key=idempotency_key,
                                       status="OUTCOME_UNKNOWN", detail=detail)

    def reconcile_operation(self, *, idempotency_key: str, applied: bool,
                            evidence: str, reconciled_by: str) -> dict[str, Any]:
        with self.store.tx() as conn:
            row = conn.execute(
                "SELECT status FROM operation_ledger WHERE project_id = ? AND"
                " idempotency_key = ?", (self.project_id, idempotency_key)).fetchone()
            if row is None:
                raise NotFoundError("no such operation", idempotency_key=idempotency_key)
            if str(row["status"]) != "OUTCOME_UNKNOWN":
                raise ConflictError("only an unknown outcome needs reconciliation",
                                    idempotency_key=idempotency_key, status=row["status"])
            status = "RECONCILED" if applied else "FAILED"
            conn.execute(
                "UPDATE operation_ledger SET status = ?, detail = ?, reconciled_by = ?,"
                " completed_at = ? WHERE project_id = ? AND idempotency_key = ?",
                (status, evidence, reconciled_by, _iso(), self.project_id, idempotency_key))
        return {"idempotency_key": idempotency_key, "status": status,
                "retry_allowed": not applied, "evidence": evidence,
                "reconciled_by": reconciled_by}

    def operations(self, *, status: str | None = None) -> list[dict[str, Any]]:
        sql = ("SELECT idempotency_key, tool, effect_type, target, status, detail,"
               " created_at, completed_at FROM operation_ledger WHERE project_id = ?")
        args: list[Any] = [self.project_id]
        if status:
            sql += " AND status = ?"
            args.append(status)
        sql += " ORDER BY created_at"
        with self.store.read() as conn:
            return _rows(conn, sql, args)

    # ----------------------------------------------------------- requirements
    def record_requirements(self, *, original_request: str,
                            requirements: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
        """Freeze the user's original request and the requirements read from it."""
        original_hash = sha256_text(original_request)
        stored = []
        with self.store.tx() as conn:
            for item in requirements:
                req_id = str(item["req_id"])
                req_key = f"{self.project_id}:{req_id}"
                conn.execute(
                    "INSERT INTO requirements(req_key, project_id, job_id, req_id, source,"
                    " original_request_hash, normalized_requirement, acceptance, criticality,"
                    " created_at) VALUES (?,?,?,?,?,?,?,?,?,?)"
                    " ON CONFLICT(req_key) DO UPDATE SET normalized_requirement = excluded."
                    "normalized_requirement, acceptance = excluded.acceptance,"
                    " criticality = excluded.criticality",
                    (req_key, self.project_id, self.job_id, req_id,
                     str(item.get("source") or "ORIGINAL_USER_REQUEST"), original_hash,
                     str(item["normalized_requirement"]), _dump(item.get("acceptance") or []),
                     str(item.get("criticality") or "IMPORTANT").upper(), _iso()),
                )
                stored.append({"req_id": req_id, "criticality":
                               str(item.get("criticality") or "IMPORTANT").upper()})
        return {"original_request_hash": original_hash, "requirements": stored,
                "count": len(stored)}

    def link_requirement(self, *, req_id: str, node_id: str, role: str,
                         evidence: str | None = None) -> dict[str, Any]:
        role = role.upper()
        if role not in {"IMPLEMENTS", "VALIDATES"}:
            raise ContractError("a requirement link is IMPLEMENTS or VALIDATES")
        with self.store.tx() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO requirement_links(req_key, project_id, node_id, role,"
                " evidence, created_at) VALUES (?,?,?,?,?,?)",
                (f"{self.project_id}:{req_id}", self.project_id, node_id, role, evidence,
                 _iso()),
            )
        return {"req_id": req_id, "node_id": node_id, "role": role}

    def requirement_status(self, *, node_states: Mapping[str, str] | None = None,
                           receipts_exit_zero: bool = True) -> dict[str, Any]:
        """Which requirements are implemented, validated, and actually passing.

        A requirement is only PASS when a task that claims to implement it reached a
        successful state *and* a validator covered it. Local tests passing is not
        evidence about the user's requirement.
        """
        with self.store.read() as conn:
            reqs = _rows(conn, "SELECT * FROM requirements WHERE project_id = ?"
                               " ORDER BY req_id", (self.project_id,))
            links = _rows(conn, "SELECT * FROM requirement_links WHERE project_id = ?",
                          (self.project_id,))
        states = dict(node_states or {})
        by_req: dict[str, list[dict]] = {}
        for link in links:
            by_req.setdefault(link["req_key"].split(":", 1)[1], []).append(link)
        results = []
        for req in reqs:
            req_id = req["req_id"]
            related = by_req.get(req_id, [])
            implementers = [link for link in related if link["role"] == "IMPLEMENTS"]
            validators = [link for link in related if link["role"] == "VALIDATES"]
            implemented = [link for link in implementers
                           if states.get(link["node_id"]) in {"WORKER_COMPLETE",
                                                              "MANAGER_APPROVED", "INTEGRATED",
                                                              "APPLIED", "DONE"}]
            if not implementers:
                verdict, reason = "NOT_LINKED", "no task claims to implement this requirement"
            elif not implemented:
                verdict, reason = "FAIL", "no implementing task reached a successful state"
            elif not validators:
                verdict = "UNVALIDATED"
                reason = "implemented but no task validates it"
            elif not receipts_exit_zero:
                verdict, reason = "UNVERIFIED", "the executor receipts did not exit zero"
            else:
                verdict, reason = "PASS", "implemented and independently validated"
            results.append({
                "req_id": req_id, "criticality": req["criticality"],
                "normalized_requirement": req["normalized_requirement"],
                "implementers": [link["node_id"] for link in implementers],
                "validators": [link["node_id"] for link in validators],
                "verdict": verdict, "reason": reason,
                "original_request_hash": req["original_request_hash"],
            })
        failed = [item for item in results if item["verdict"] != "PASS"]
        critical_failed = [item["req_id"] for item in results
                           if item["verdict"] == "FAIL"
                           and item["criticality"] == "CRITICAL"]
        critical_unresolved = [item["req_id"] for item in failed
                               if item["criticality"] == "CRITICAL"]
        return {"requirements": results, "failed": [item["req_id"] for item in failed],
                "critical_failed": critical_failed,
                "critical_unresolved": critical_unresolved,
                "original_request_hash": (reqs[0]["original_request_hash"] if reqs else None),
                "all_pass": not failed}

    # -------------------------------------------------------------- invariants
    def record_invariants(self, invariants: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
        stored = []
        with self.store.tx() as conn:
            for item in invariants:
                invariant_id = str(item["invariant_id"])
                conn.execute(
                    "INSERT INTO global_invariants(invariant_key, project_id, job_id,"
                    " invariant_id, description, severity, validation_method, required_evidence,"
                    " created_at) VALUES (?,?,?,?,?,?,?,?,?)"
                    " ON CONFLICT(invariant_key) DO UPDATE SET description = excluded."
                    "description, severity = excluded.severity, validation_method = excluded."
                    "validation_method, required_evidence = excluded.required_evidence",
                    (f"{self.project_id}:{invariant_id}", self.project_id, self.job_id,
                     invariant_id, str(item["description"]),
                     str(item.get("severity") or "MAJOR").upper(),
                     str(item.get("validation_method") or "UNSPECIFIED"),
                     _dump(item.get("required_evidence") or []), _iso()),
                )
                stored.append(invariant_id)
        return {"invariants": stored, "count": len(stored)}

    def validate_invariant(self, *, invariant_id: str, result: str, validated_by: str,
                           evidence: str | None = None) -> dict[str, Any]:
        result = result.upper()
        if result not in {"PASS", "FAIL", "NOT_TESTABLE"}:
            raise ContractError("an invariant result is PASS, FAIL or NOT_TESTABLE")
        with self.store.tx() as conn:
            key = f"{self.project_id}:{invariant_id}"
            exists = conn.execute(
                "SELECT 1 FROM global_invariants WHERE invariant_key = ?", (key,)).fetchone()
            if exists is None:
                raise NotFoundError("no such invariant", invariant_id=invariant_id)
            conn.execute(
                "INSERT INTO invariant_results(invariant_key, project_id, result, evidence,"
                " validated_by, created_at) VALUES (?,?,?,?,?,?)",
                (key, self.project_id, result, evidence, validated_by, _iso()),
            )
        return {"invariant_id": invariant_id, "result": result,
                "validated_by": validated_by}

    def invariant_status(self) -> dict[str, Any]:
        with self.store.read() as conn:
            rows = _rows(conn,
                         "SELECT i.invariant_id, i.description, i.severity, i.validation_method,"
                         " r.result, r.evidence, r.validated_by, r.created_at"
                         " FROM global_invariants i LEFT JOIN invariant_results r"
                         " ON r.invariant_key = i.invariant_key AND r.created_at ="
                         " (SELECT MAX(created_at) FROM invariant_results WHERE invariant_key ="
                         " i.invariant_key) WHERE i.project_id = ? ORDER BY i.invariant_id",
                         (self.project_id,))
        blocking = []
        for row in rows:
            outcome = row["result"] or "NOT_RUN"
            row["result"] = outcome
            if outcome == "FAIL":
                blocking.append({"invariant_id": row["invariant_id"], "result": outcome})
            elif outcome == "NOT_TESTABLE" and row["severity"] == "CRITICAL":
                blocking.append({"invariant_id": row["invariant_id"],
                                 "result": "NOT_TESTABLE", "severity": "CRITICAL"})
        return {"invariants": rows, "blocking": blocking, "all_pass": not blocking}

    # ------------------------------------------------- independent validation
    def record_validation(self, *, subject_node: str, validator_node: str, path_kind: str,
                          independent: bool, result: str, evidence: str | None = None,
                          job_id: str | None = None) -> dict[str, Any]:
        result = result.upper()
        if result not in {"MATCH", "MISMATCH", "NOT_RUN"}:
            raise ContractError("a validation result is MATCH, MISMATCH or NOT_RUN")
        validation_id = _new_id("val")
        with self.store.tx() as conn:
            conn.execute(
                "INSERT INTO validation_paths(validation_id, project_id, job_id, subject_node,"
                " validator_node, path_kind, independent, result, evidence, created_at)"
                " VALUES (?,?,?,?,?,?,?,?,?,?)",
                (validation_id, self.project_id, job_id or self.job_id, subject_node,
                 validator_node, path_kind, 1 if independent else 0, result, evidence, _iso()))
        return {"validation_id": validation_id, "independent": bool(independent),
                "result": result}

    def validation_status(self) -> dict[str, Any]:
        with self.store.read() as conn:
            rows = _rows(conn, "SELECT * FROM validation_paths WHERE project_id = ?"
                               " ORDER BY created_at", (self.project_id,))
        mismatches = [row for row in rows if row["result"] == "MISMATCH"]
        missing = [row for row in rows if not row["independent"]]
        return {"validations": rows, "mismatches": mismatches,
                "non_independent": missing,
                "all_pass": not mismatches}

    # -------------------------------------------------------------- staleness
    def freeze_identity(self, *, node_id: str, contracts: Mapping[str, int] | None = None,
                        artifacts: Mapping[str, int] | None = None,
                        revisions: Mapping[str, int] | None = None,
                        lease: Mapping[str, Any] | None = None) -> dict[str, Any]:
        """The identity a result is produced against, captured before work starts."""
        with self.store.read() as conn:
            contract_rows = _rows(conn,
                                  "SELECT contract_id, MAX(version) AS version FROM"
                                  " semantic_contracts WHERE project_id = ? GROUP BY contract_id",
                                  (self.project_id,))
            artifact_rows = _rows(conn,
                                  "SELECT artifact_id, MAX(version) AS version FROM"
                                  " artifact_registry WHERE project_id = ? GROUP BY artifact_id",
                                  (self.project_id,))
        frozen = {
            "node_id": node_id,
            "plan_version": (revisions or {}).get("plan_version"),
            "task_revision": (revisions or {}).get("task_revision"),
            "project_revision": (revisions or {}).get("project_revision"),
            "contracts": dict(contracts) if contracts is not None
            else {row["contract_id"]: int(row["version"]) for row in contract_rows},
            "artifacts": dict(artifacts) if artifacts is not None
            else {row["artifact_id"]: int(row["version"]) for row in artifact_rows},
            "decision_set_hash": self.decision_set_hash(),
            "lease": dict(lease or {}),
            "frozen_at": _iso(),
        }
        frozen["identity_hash"] = hash_json({key: value for key, value in frozen.items()
                                             if key != "frozen_at"})
        return frozen

    def check_staleness(self, frozen: Mapping[str, Any]) -> dict[str, Any]:
        """Compare a frozen identity with the authoritative state, literally."""
        node_id = str(frozen.get("node_id") or "")
        current = self.freeze_identity(node_id=node_id)
        changes: list[dict[str, Any]] = []
        for field in ("contracts", "artifacts"):
            before = dict(frozen.get(field) or {})
            after = dict(current.get(field) or {})
            for key in sorted(set(before) | set(after)):
                if before.get(key) != after.get(key):
                    changes.append({"kind": field[:-1] + "_changed", "name": key,
                                    "frozen": before.get(key), "current": after.get(key)})
        if frozen.get("decision_set_hash") != current["decision_set_hash"]:
            changes.append({"kind": "decision_changed",
                            "frozen": frozen.get("decision_set_hash"),
                            "current": current["decision_set_hash"]})
        return {
            "node_id": node_id,
            "stale": bool(changes),
            "verdict": "STALE_RESULT" if changes else "CURRENT",
            "changes": changes,
            "checked_at": current["frozen_at"],
        }

    def record_submission(self, *, node_id: str, frozen: Mapping[str, Any],
                          plan_version: int | None = None,
                          expected_plan_version: int | None = None,
                          lease_ok: bool = True) -> dict[str, Any]:
        """Record a submission and its verdict; a stale one cannot be integrated."""
        check = self.check_staleness(frozen)
        reasons = list(check["changes"])
        if (plan_version is not None and expected_plan_version is not None
                and int(plan_version) != int(expected_plan_version)):
            reasons.append({"kind": "plan_superseded", "frozen": expected_plan_version,
                            "current": plan_version})
        if not lease_ok:
            reasons.append({"kind": "lease_stale"})
        verdict = "STALE_RESULT" if reasons else "CURRENT"
        submission_id = _new_id("sub")
        with self.store.tx() as conn:
            conn.execute(
                "INSERT INTO submissions(submission_id, project_id, job_id, node_id, frozen,"
                " verdict, reasons, created_at) VALUES (?,?,?,?,?,?,?,?)",
                (submission_id, self.project_id, self.job_id, node_id, _dump(frozen),
                 verdict, _dump(reasons), _iso()))
        return {"submission_id": submission_id, "node_id": node_id, "verdict": verdict,
                "reasons": reasons, "stale": verdict == "STALE_RESULT"}

    def mark_submission_stale(self, node_id: str, *, reason: str) -> dict[str, Any]:
        """A contract/artifact change invalidates work that was produced against it."""
        with self.store.tx() as conn:
            rows = conn.execute(
                "SELECT submission_id, reasons FROM submissions WHERE project_id = ?"
                " AND job_id = ? AND node_id = ? AND verdict = 'CURRENT'",
                (self.project_id, self.job_id, node_id)).fetchall()
            for row in rows:
                reasons = _loads(row["reasons"], [])
                reasons.append({"kind": "invalidated", "reason": reason})
                conn.execute(
                    "UPDATE submissions SET verdict = 'STALE_RESULT', reasons = ?"
                    " WHERE submission_id = ?", (_dump(reasons), row["submission_id"]))
        return {"node_id": node_id, "newly_stale": len(rows)}

    def stale_submissions(self) -> list[dict[str, Any]]:
        with self.store.read() as conn:
            rows = _rows(conn, "SELECT submission_id, node_id, verdict, created_at FROM"
                               " submissions WHERE project_id = ? AND verdict = 'STALE_RESULT'"
                               " ORDER BY created_at", (self.project_id,))
        return rows

    # ------------------------------------------------------------- final gate
    def final_gate(self, *, node_states: Mapping[str, str] | None = None,
                   receipts_exit_zero: bool = True, required_artifacts: Mapping[str, int]
                   | None = None, integration: Mapping[str, Any] | None = None,
                   unresolved_conflicts: int = 0) -> dict[str, Any]:
        """The program gate a manager's APPROVE cannot bypass.

        Every check reports the state it actually observed. A check whose inputs do
        not exist for this project reports NOT_APPLICABLE and does not block: the
        gate is strict about what it knows, not about what it was never told.
        """
        states = dict(node_states or {})
        non_terminal = sorted(node_id for node_id, state in states.items()
                              if state not in {"WORKER_COMPLETE", "MANAGER_APPROVED",
                                               "INTEGRATED", "APPLIED", "DONE"})
        with self.store.read() as conn:
            pending = conn.execute(
                "SELECT COUNT(*) AS n FROM contract_change_requests WHERE project_id = ?"
                " AND status = 'PENDING'", (self.project_id,)).fetchone()["n"]
            contracts = conn.execute(
                "SELECT COUNT(*) AS n FROM semantic_contracts WHERE project_id = ?",
                (self.project_id,)).fetchone()["n"]
            invariants = conn.execute(
                "SELECT COUNT(*) AS n FROM global_invariants WHERE project_id = ?",
                (self.project_id,)).fetchone()["n"]
            requirement_rows = conn.execute(
                "SELECT COUNT(*) AS n FROM requirements WHERE project_id = ?",
                (self.project_id,)).fetchone()["n"]
            known_outcomes = conn.execute(
                "SELECT COUNT(*) AS n FROM operation_ledger WHERE project_id = ?"
                " AND status = 'OUTCOME_UNKNOWN'", (self.project_id,)).fetchone()["n"]
            conflicts = conn.execute(
                "SELECT COUNT(*) AS n FROM operation_ledger WHERE project_id = ?"
                " AND status = 'FAILED'", (self.project_id,)).fetchone()["n"]

        missing_artifacts = (self.missing_required_artifacts(needed=required_artifacts)
                             if required_artifacts else [])
        requirement_report = (self.requirement_status(node_states=states,
                                                      receipts_exit_zero=receipts_exit_zero)
                              if requirement_rows else None)
        invariant_report = self.invariant_status() if invariants else None
        validation_report = self.validation_status()
        stale = self.stale_submissions()
        integration = dict(integration or {})

        checks: list[dict[str, Any]] = []

        def add(name: str, status: str, detail: Any = None) -> None:
            checks.append({"check": name, "status": status, "detail": detail})

        add("ALL_REQUIRED_TASKS_TERMINAL",
            "NOT_APPLICABLE" if not states else ("PASS" if not non_terminal else "FAIL"),
            non_terminal or None)
        add("NO_UNRESOLVED_CONTRACT_CHANGE",
            "NOT_APPLICABLE" if not contracts else ("PASS" if not pending else "FAIL"),
            {"pending": int(pending)} if contracts else None)
        stale_names = [row["node_id"] for row in stale]
        add("NO_STALE_RESULTS",
            "PASS" if not stale_names else "FAIL", stale_names or None)
        add("NO_CONCURRENT_CONFLICT", "PASS" if unresolved_conflicts == 0 else "FAIL",
            {"unresolved": int(unresolved_conflicts)})
        add("NO_UNKNOWN_SIDE_EFFECTS",
            "NOT_APPLICABLE" if contracts == 0 and requirement_rows == 0
            else ("PASS" if not known_outcomes else "FAIL"),
            {"outcome_unknown": int(known_outcomes)} if known_outcomes else None)
        add("NO_MISSING_REQUIRED_ARTIFACT",
            "NOT_APPLICABLE" if not required_artifacts else
            ("PASS" if not missing_artifacts else "FAIL"), missing_artifacts or None)
        add("NO_FAILED_CRITICAL_REQUIREMENT",
            "NOT_APPLICABLE" if requirement_report is None else
            ("PASS" if not requirement_report["critical_unresolved"] else "FAIL"),
            (requirement_report["critical_unresolved"] or None) if requirement_report else None)
        for layer in ("textual", "semantic", "behavioral"):
            status = str(integration.get(layer) or "NOT_RUN").upper()
            # NOT_RUN means there was nothing to judge for this layer (an ordinary
            # run with no contracts); FAIL always stays FAIL.
            if status == "PASS":
                verdict = "PASS"
            elif status == "NOT_RUN":
                verdict = "NOT_APPLICABLE"
            else:
                verdict = "FAIL"
            add(f"{layer.upper()}_INTEGRATION", verdict,
                {"status": status} if integration else None)
        add("GLOBAL_INVARIANTS",
            "NOT_APPLICABLE" if invariant_report is None else
            ("PASS" if invariant_report["all_pass"] else "FAIL"),
            (invariant_report["blocking"] or None) if invariant_report else None)
        add("USER_INTENT_VALIDATION",
            "NOT_APPLICABLE" if requirement_report is None else
            ("PASS" if requirement_report["all_pass"] else "FAIL"),
            (requirement_report["failed"] or None) if requirement_report else None)
        add("UNRESOLVED_SEMANTIC_CONFLICTS", "PASS" if unresolved_conflicts == 0 else "FAIL",
            {"unresolved": int(unresolved_conflicts)})
        if validation_report["validations"]:
            add("INDEPENDENT_VALIDATION",
                "PASS" if validation_report["all_pass"] else "FAIL",
                [row["validation_id"] for row in validation_report["mismatches"]] or None)

        failed = [item["check"] for item in checks if item["status"] == "FAIL"]
        return {
            "gate": "FINAL_MANAGER_GATE",
            "checks": checks,
            "failed": failed,
            "passed": not failed,
            "blocking_count": len(failed),
            "requirement_traceability": requirement_report,
            "invariants": invariant_report,
            "stale_submissions": stale,
            "operation_conflicts": int(conflicts),
            "note": ("A FAIL here outranks any manager verdict: an approval cannot turn"
                     " a failed program gate into DONE."),
        }


def _change_action(state: str) -> str:
    state = (state or "").upper()
    if state in {"NEW", "ASSIGNED", "WAITING_DEPENDENCIES", "WAITING_CAPACITY"}:
        return "UPDATE_DEPENDENCY"
    if state in {"WORKING", "FIX", "OUTCOME_UNKNOWN"}:
        return "STALE_PENDING_REVIEW"
    if state in {"WORKER_COMPLETE", "MANAGER_APPROVED", "READY_TO_APPLY"}:
        return "STALE_RESULT"
    if state in {"INTEGRATED", "APPLIED", "DONE"}:
        return "CHANGE_IMPACT_REVIEW"
    return "REVIEW_REQUIRED"


def integration_layers(coordinator: "SemanticCoordinator", *, applied: Sequence[str],
                       receipts_exit_zero: bool,
                       node_states: Mapping[str, str] | None = None) -> dict[str, Any]:
    """The three integration layers, each judged from evidence - not from a merge.

    A textual merge proves bytes landed. It says nothing about whether the modules
    agree on a schema, and nothing about whether the system behaves as the user
    asked, so those are separate verdicts with their own evidence.
    """
    textual = "PASS" if applied else "FAIL"
    invariant_report = coordinator.invariant_status()
    validation = coordinator.validation_status()
    with coordinator.store.read() as conn:
        contracts = int(conn.execute(
            "SELECT COUNT(*) AS n FROM semantic_contracts WHERE project_id = ?",
            (coordinator.project_id,)).fetchone()["n"])
        submissions = int(conn.execute(
            "SELECT COUNT(*) AS n FROM submissions WHERE project_id = ?",
            (coordinator.project_id,)).fetchone()["n"])
        stale = int(conn.execute(
            "SELECT COUNT(*) AS n FROM submissions WHERE project_id = ?"
            " AND verdict = 'STALE_RESULT'", (coordinator.project_id,)).fetchone()["n"])
        validations = int(conn.execute(
            "SELECT COUNT(*) AS n FROM validation_paths WHERE project_id = ?",
            (coordinator.project_id,)).fetchone()["n"])
        artifacts = int(conn.execute(
            "SELECT COUNT(*) AS n FROM artifact_registry WHERE project_id = ?",
            (coordinator.project_id,)).fetchone()["n"])
    coordinated = bool(contracts or submissions or validations or artifacts
                       or invariant_report["invariants"])
    if not coordinated:
        semantic = "NOT_RUN"
        behavioral = "PASS" if applied else "NOT_RUN"
    else:
        semantic = ("PASS" if not stale and invariant_report["all_pass"]
                    and validation["all_pass"] else "FAIL")
        behavioral = ("PASS" if receipts_exit_zero and invariant_report["all_pass"]
                      else "FAIL")
    return {"textual": textual, "semantic": semantic, "behavioral": behavioral,
            "applied": list(applied), "stale_submissions": stale,
            "invariants_blocking": invariant_report["blocking"],
            "note": ("textual integration only proves the bytes landed; the semantic and"
                     " behavioral layers are judged from contracts, invariants and"
                     " independent validation")}


def propagate_contract_change(coordinator: "SemanticCoordinator", *,
                              node_states: Mapping[str, str]) -> dict[str, Any]:
    """Classify every affected task after a contract bump, worst case first."""
    classified = {node_id: _change_action(state) for node_id, state in node_states.items()}
    return {
        "classified": classified,
        "stale_pending_review": [key for key, value in classified.items()
                                 if value == "STALE_PENDING_REVIEW"],
        "stale_results": [key for key, value in classified.items()
                          if value == "STALE_RESULT"],
        "change_impact_review": [key for key, value in classified.items()
                                 if value == "CHANGE_IMPACT_REVIEW"],
        "dependency_updates": [key for key, value in classified.items()
                               if value == "UPDATE_DEPENDENCY"],
    }
