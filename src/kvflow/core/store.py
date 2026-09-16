"""Durable v1 state: versioned SQLite with compare-and-swap and durable lineage.

Design rules enforced here (each one answers a specific F1 Manager finding in
``.runtime/v1-development/audit/f1-forensic/MANAGER_QA.md``):

* Short ``BEGIN IMMEDIATE`` transactions only. No model call or tool execution
  ever happens while a write transaction is open; connections are opened per
  operation and always closed.
* A state change is legal only when the caller's ``expected_state`` equals the
  persisted state *and* the edge exists in
  :data:`kvflow.core.state.LEGAL_TRANSITIONS`; every accepted change appends an
  event row.
* ``node_attempts`` is keyed by ``(project_id, lineage_key)``, not by the
  replaceable node id, so renaming a node, replacing a plan or starting a new
  job/run cannot hand out a fresh initial+2 worker budget.
* ``plans`` stores the complete validated plan document plus its digest and
  revision; replacing a plan never deletes node history or resets counters.
* ``leases`` enforces one active owner epoch per node with a monotonically
  increasing fence, and reopening the database cannot lower it.
* Cross-entity references are real foreign keys, so a capability or receipt can
  never reference a job/node/run that does not exist.
"""

from __future__ import annotations

import json
import sqlite3
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from .contracts import (
    ContextManifest,
    Contract,
    ExperimentRun,
    ExperimentSpec,
    KnowledgeRecord,
    MAX_EXPERIMENT_ATTEMPTS,
    Plan,
    Project,
    Review,
    TestReceipt,
    canonical_json,
    content_digest,
)
from .errors import (
    AttemptExhausted,
    AuthorizationError,
    ConcurrencyError,
    ContractError,
    NotFoundError,
    StateTransitionError,
)
from .state import (
    ACTIVE_STATES,
    ATTEMPT_STATES,
    MAX_WORKER_ATTEMPTS,
    PAUSE_STATES,
    RECONCILIATION_STATES,
    TERMINAL_STATES,
    TaskState,
    assert_transition,
    parse_state,
)

SCHEMA_VERSION = 1
BUSY_TIMEOUT_MS = 5_000
DEFAULT_LEASE_SECONDS = 120.0


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def now_ts() -> float:
    return time.time()


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:20]}"


class _TimeSource:
    """Injectable clock so deadline/lapse tests are deterministic, not sleepy."""

    def __init__(self) -> None:
        self._offset = 0.0

    def now(self) -> float:
        return time.time() + self._offset

    def advance(self, seconds: float) -> None:
        self._offset += float(seconds)


DDL = """
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS projects (
    project_id TEXT PRIMARY KEY,
    document TEXT NOT NULL,
    digest TEXT NOT NULL,
    authorization_digest TEXT NOT NULL,
    source_root TEXT NOT NULL,
    managed_root TEXT NOT NULL,
    trusted INTEGER NOT NULL CHECK (trusted IN (0, 1)),
    registered_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS jobs (
    job_id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL REFERENCES projects(project_id),
    objective TEXT NOT NULL,
    authorization_digest TEXT NOT NULL,
    state TEXT NOT NULL,
    revision INTEGER NOT NULL DEFAULT 1,
    prior_state TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS plans (
    job_id TEXT NOT NULL REFERENCES jobs(job_id),
    revision INTEGER NOT NULL,
    plan_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    document TEXT NOT NULL,
    digest TEXT NOT NULL,
    authorization_digest TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (job_id, revision)
);

CREATE TABLE IF NOT EXISTS nodes (
    job_id TEXT NOT NULL REFERENCES jobs(job_id),
    node_id TEXT NOT NULL,
    lineage_key TEXT NOT NULL,
    objective TEXT NOT NULL,
    role TEXT NOT NULL,
    test_profile TEXT NOT NULL,
    write_scopes TEXT NOT NULL,
    allowed_tools TEXT NOT NULL,
    state TEXT NOT NULL,
    revision INTEGER NOT NULL DEFAULT 1,
    plan_revision INTEGER,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (job_id, node_id)
);

-- SQLite skips a composite foreign key whenever one column is NULL, so the
-- plan linkage is enforced by triggers instead: a node may have no plan yet,
-- but any non-null plan_revision must reference a plan revision that exists.
CREATE TRIGGER IF NOT EXISTS nodes_plan_revision_insert
BEFORE INSERT ON nodes
WHEN NEW.plan_revision IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM plans WHERE job_id = NEW.job_id AND revision = NEW.plan_revision)
BEGIN
    SELECT RAISE(ABORT, 'node references an unknown plan revision');
END;

CREATE TRIGGER IF NOT EXISTS nodes_plan_revision_update
BEFORE UPDATE OF plan_revision ON nodes
WHEN NEW.plan_revision IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM plans WHERE job_id = NEW.job_id AND revision = NEW.plan_revision)
BEGIN
    SELECT RAISE(ABORT, 'node references an unknown plan revision');
END;

CREATE TABLE IF NOT EXISTS node_edges (
    job_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    depends_on TEXT NOT NULL,
    PRIMARY KEY (job_id, node_id, depends_on),
    FOREIGN KEY (job_id, node_id) REFERENCES nodes(job_id, node_id),
    FOREIGN KEY (job_id, depends_on) REFERENCES nodes(job_id, node_id)
);

CREATE TABLE IF NOT EXISTS node_attempts (
    project_id TEXT NOT NULL REFERENCES projects(project_id),
    lineage_key TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 3,
    first_job_id TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (project_id, lineage_key)
);

CREATE TABLE IF NOT EXISTS runs (
    run_id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL REFERENCES jobs(job_id),
    node_id TEXT NOT NULL,
    project_id TEXT NOT NULL REFERENCES projects(project_id),
    attempt INTEGER NOT NULL CHECK (attempt >= 1),
    phase TEXT NOT NULL,
    role TEXT NOT NULL,
    fence INTEGER NOT NULL CHECK (fence >= 1),
    lease_id TEXT,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (job_id, node_id) REFERENCES nodes(job_id, node_id)
);

CREATE TABLE IF NOT EXISTS events (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL REFERENCES jobs(job_id),
    node_id TEXT,
    run_id TEXT,
    actor TEXT NOT NULL,
    kind TEXT NOT NULL,
    from_state TEXT,
    to_state TEXT,
    payload TEXT NOT NULL,
    at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS leases (
    lease_id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    run_id TEXT NOT NULL REFERENCES runs(run_id),
    owner TEXT NOT NULL,
    fence INTEGER NOT NULL CHECK (fence >= 1),
    status TEXT NOT NULL CHECK (status IN ('ACTIVE', 'RELEASED', 'EXPIRED', 'REVOKED')),
    issued_at TEXT NOT NULL,
    expires_at REAL NOT NULL,
    FOREIGN KEY (job_id, node_id) REFERENCES nodes(job_id, node_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS leases_one_active
    ON leases(job_id, node_id) WHERE status = 'ACTIVE';

CREATE TABLE IF NOT EXISTS capabilities (
    token_hash TEXT PRIMARY KEY,
    project_id TEXT NOT NULL REFERENCES projects(project_id),
    job_id TEXT NOT NULL REFERENCES jobs(job_id),
    node_id TEXT NOT NULL,
    run_id TEXT NOT NULL REFERENCES runs(run_id),
    attempt INTEGER NOT NULL,
    role TEXT NOT NULL,
    fence INTEGER NOT NULL,
    lease_id TEXT NOT NULL REFERENCES leases(lease_id),
    actions TEXT NOT NULL,
    issued_at TEXT NOT NULL,
    expires_at REAL NOT NULL,
    revoked_at TEXT,
    FOREIGN KEY (job_id, node_id) REFERENCES nodes(job_id, node_id)
);

CREATE TABLE IF NOT EXISTS budget_scopes (
    scope_id TEXT PRIMARY KEY,
    scope_kind TEXT NOT NULL CHECK (scope_kind IN ('GLOBAL', 'PROJECT', 'JOB')),
    parent_scope_id TEXT REFERENCES budget_scopes(scope_id),
    project_id TEXT,
    job_id TEXT,
    document TEXT NOT NULL,
    digest TEXT NOT NULL,
    authorization_digest TEXT NOT NULL,
    deadline REAL NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS reservations (
    reservation_id TEXT PRIMARY KEY,
    scope_id TEXT NOT NULL,
    request_digest TEXT NOT NULL,
    identity_digest TEXT NOT NULL,
    project_id TEXT,
    job_id TEXT,
    node_id TEXT,
    run_id TEXT,
    attempt INTEGER,
    fence INTEGER,
    effect_id TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('RESERVED', 'SETTLED', 'UNKNOWN', 'RELEASED')),
    reserved TEXT NOT NULL,
    actual TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS reservations_identity ON reservations(identity_digest, state);
CREATE INDEX IF NOT EXISTS reservations_scope ON reservations(scope_id, state);

CREATE TABLE IF NOT EXISTS test_receipts (
    receipt_id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL REFERENCES jobs(job_id),
    node_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    project_id TEXT NOT NULL REFERENCES projects(project_id),
    profile_id TEXT NOT NULL,
    document TEXT NOT NULL,
    digest TEXT NOT NULL,
    exit_code INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (job_id, node_id) REFERENCES nodes(job_id, node_id)
);

CREATE TABLE IF NOT EXISTS reviews (
    review_id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL REFERENCES jobs(job_id),
    node_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    project_id TEXT NOT NULL REFERENCES projects(project_id),
    verdict TEXT NOT NULL,
    kind TEXT NOT NULL,
    content_digest TEXT NOT NULL,
    document TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (job_id, node_id) REFERENCES nodes(job_id, node_id)
);

CREATE TABLE IF NOT EXISTS experiments (
    experiment_id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL REFERENCES projects(project_id),
    job_id TEXT NOT NULL REFERENCES jobs(job_id),
    node_id TEXT NOT NULL,
    run_id TEXT,
    attempt INTEGER NOT NULL DEFAULT 0,
    document TEXT NOT NULL,
    digest TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS experiment_runs (
    experiment_run_id TEXT PRIMARY KEY,
    experiment_id TEXT NOT NULL REFERENCES experiments(experiment_id),
    project_id TEXT NOT NULL REFERENCES projects(project_id),
    job_id TEXT NOT NULL REFERENCES jobs(job_id),
    node_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    attempt INTEGER NOT NULL,
    fence INTEGER NOT NULL,
    outcome TEXT NOT NULL,
    document TEXT NOT NULL,
    digest TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS knowledge (
    record_id TEXT PRIMARY KEY,
    scope TEXT NOT NULL,
    privacy TEXT NOT NULL,
    project_id TEXT,
    job_id TEXT,
    run_id TEXT,
    topic TEXT NOT NULL,
    kind TEXT NOT NULL,
    author TEXT NOT NULL,
    verification TEXT NOT NULL,
    document TEXT NOT NULL,
    source_digest TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS knowledge_lookup ON knowledge(project_id, topic);
CREATE INDEX IF NOT EXISTS knowledge_scope ON knowledge(scope, privacy);

CREATE TABLE IF NOT EXISTS context_manifests (
    manifest_id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL REFERENCES jobs(job_id),
    node_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    attempt INTEGER NOT NULL,
    document TEXT NOT NULL,
    digest TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS publications (
    publication_id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL REFERENCES projects(project_id),
    job_id TEXT NOT NULL REFERENCES jobs(job_id),
    node_id TEXT NOT NULL,
    state TEXT NOT NULL,
    integration_branch TEXT NOT NULL,
    content_digest TEXT NOT NULL,
    review_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    target TEXT NOT NULL,
    document TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (job_id, node_id) REFERENCES nodes(job_id, node_id)
);

CREATE TABLE IF NOT EXISTS operations (
    operation_id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL REFERENCES jobs(job_id),
    node_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    action TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN
        ('PENDING', 'RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELLED', 'OUTCOME_UNKNOWN')),
    request_digest TEXT NOT NULL,
    result_digest TEXT,
    result_artifact TEXT,
    error_code TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (job_id, node_id) REFERENCES nodes(job_id, node_id)
);

CREATE TABLE IF NOT EXISTS mailbox (
    message_id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL REFERENCES jobs(job_id),
    node_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    sender_role TEXT NOT NULL,
    kind TEXT NOT NULL,
    body TEXT NOT NULL,
    document TEXT NOT NULL,
    created_at TEXT NOT NULL,
    read_at TEXT,
    FOREIGN KEY (job_id, node_id) REFERENCES nodes(job_id, node_id)
);
"""


def _iso(value: float) -> str:
    return datetime.fromtimestamp(value, tz=timezone.utc).isoformat()


class Store:
    """Connection-per-operation v1 store."""

    def __init__(self, db_path: str | Path, *, clock: _TimeSource | None = None) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.clock = clock or _TimeSource()

    # ------------------------------------------------------------ plumbing
    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            str(self.db_path), timeout=BUSY_TIMEOUT_MS / 1000.0, isolation_level=None
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute(f"PRAGMA busy_timeout = {BUSY_TIMEOUT_MS}")
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = FULL")
        return conn

    @contextmanager
    def read(self) -> Iterator[sqlite3.Connection]:
        conn = self.connect()
        try:
            yield conn
        finally:
            conn.close()

    @contextmanager
    def tx(self) -> Iterator[sqlite3.Connection]:
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            yield conn
            if conn.in_transaction:
                conn.execute("COMMIT")
        except BaseException:
            try:
                if conn.in_transaction:
                    conn.execute("ROLLBACK")
            except sqlite3.Error:  # pragma: no cover - best-effort rollback
                pass
            raise
        finally:
            conn.close()

    def initialize(self) -> None:
        conn = self.connect()
        try:
            conn.executescript(DDL)
        finally:
            conn.close()
        with self.tx() as txn:
            txn.execute(
                "INSERT INTO meta(key, value) VALUES ('schema_version', ?)"
                " ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (str(SCHEMA_VERSION),),
            )

    def schema_version(self) -> int:
        try:
            with self.read() as conn:
                row = conn.execute(
                    "SELECT value FROM meta WHERE key = 'schema_version'"
                ).fetchone()
        except sqlite3.OperationalError:
            return 0
        return int(row["value"]) if row else 0

    #: durable tables whose row counts must survive a backup or a migration
    COUNTED_TABLES = (
        "projects",
        "jobs",
        "plans",
        "nodes",
        "node_attempts",
        "runs",
        "leases",
        "reservations",
        "test_receipts",
        "reviews",
        "knowledge",
        "publications",
        "operations",
        "events",
        "experiments",
        "experiment_runs",
    )

    def row_counts_for_test(self) -> dict[str, int]:
        """Row counts per durable table, for backup and migration assertions."""
        counts: dict[str, int] = {}
        with self.read() as conn:
            present = {
                row["name"]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                )
            }
            for table in self.COUNTED_TABLES:
                if table not in present:
                    counts[table] = -1
                    continue
                counts[table] = int(
                    conn.execute(f"SELECT COUNT(*) AS c FROM {table}").fetchone()["c"]
                )
        return counts

    # ------------------------------------------------------------ projects
    def register_project(self, project: Project) -> str:
        if not project.trusted:
            # unreachable through the contract, kept as a second line of defence
            raise AuthorizationError("untrusted project cannot be registered")
        with self.tx() as conn:
            existing = conn.execute(
                "SELECT digest, authorization_digest FROM projects WHERE project_id = ?",
                (project.id,),
            ).fetchone()
            digest = content_digest(project)
            if existing is not None:
                if existing["authorization_digest"] != project.authorization_digest:
                    raise AuthorizationError(
                        "project re-registration must keep its original authorization",
                        project_id=project.id,
                    )
                if existing["digest"] != digest:
                    raise AuthorizationError(
                        "project registration is immutable; amend it explicitly instead",
                        project_id=project.id,
                    )
                return project.id
            conn.execute(
                "INSERT INTO projects(project_id, document, digest, authorization_digest,"
                " source_root, managed_root, trusted, registered_at)"
                " VALUES (?,?,?,?,?,?,?,?)",
                (
                    project.id,
                    canonical_json(project),
                    digest,
                    project.authorization_digest,
                    project.source_root,
                    project.managed_root,
                    1,
                    project.registered_at.isoformat(),
                ),
            )
        return project.id

    def amend_project(self, project: Project, *, actor: str = "user") -> dict:
        """Replace a project's approved configuration with a newly approved one.

        ``register_project`` is immutable on purpose: a run may never widen its own
        scope. But the *user* can approve a new configuration - a project that gains a
        test profile, or changes its write roots - and without an explicit amend the
        durable row keeps the old authorization, so every later run reports drift and
        silently executes against the stale scope. This is that explicit path: it
        records the previous digest, the new digest and who did it.
        """
        if not project.trusted:
            raise AuthorizationError("untrusted project cannot be registered")
        digest = content_digest(project)
        with self.tx() as conn:
            existing = conn.execute(
                "SELECT digest, authorization_digest FROM projects WHERE project_id = ?",
                (project.id,),
            ).fetchone()
            if existing is None:
                raise NotFoundError("no such project to amend", project_id=project.id)
            previous = str(existing["authorization_digest"])
            # the authorization digest *is* the identity of what the user approved, so
            # an amend presenting the same authorization changes nothing; the row's own
            # document digest also carries a registration timestamp, which must not be
            # mistaken for a new approval
            if previous == project.authorization_digest:
                return {"project_id": project.id, "changed": False,
                        "authorization_digest": previous}
            conn.execute(
                "UPDATE projects SET document = ?, digest = ?, authorization_digest = ?,"
                " source_root = ?, managed_root = ?, registered_at = ?"
                " WHERE project_id = ?",
                (
                    canonical_json(project), digest, project.authorization_digest,
                    project.source_root, project.managed_root,
                    project.registered_at.isoformat(), project.id,
                ),
            )
        return {"project_id": project.id, "changed": True,
                "previous_authorization_digest": previous,
                "authorization_digest": project.authorization_digest}

    def project(self, project_id: str) -> Project:
        with self.read() as conn:
            row = conn.execute(
                "SELECT document FROM projects WHERE project_id = ?", (project_id,)
            ).fetchone()
        if row is None:
            raise NotFoundError("unknown project", project_id=project_id)
        return Project.model_validate(json.loads(row["document"]))

    def list_projects(self) -> list[Project]:
        with self.read() as conn:
            rows = conn.execute(
                "SELECT document FROM projects ORDER BY project_id"
            ).fetchall()
        return [Project.model_validate(json.loads(r["document"])) for r in rows]

    # ------------------------------------------------------------ jobs
    def create_job(self, project_id: str, objective: str, authorization_digest: str) -> str:
        job_id = new_id("job")
        at = utcnow().isoformat()
        with self.tx() as conn:
            if conn.execute(
                "SELECT 1 FROM projects WHERE project_id = ?", (project_id,)
            ).fetchone() is None:
                raise NotFoundError("unknown project", project_id=project_id)
            conn.execute(
                "INSERT INTO jobs(job_id, project_id, objective, authorization_digest, state,"
                " revision, created_at, updated_at) VALUES (?,?,?,?,?,1,?,?)",
                (job_id, project_id, objective, authorization_digest, TaskState.NEW.value, at, at),
            )
            self._event(conn, job_id, None, None, "orchestrator", "JOB_CREATED",
                        None, TaskState.NEW.value, {"objective": objective})
        return job_id

    def job(self, job_id: str) -> sqlite3.Row:
        with self.read() as conn:
            row = conn.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
        if row is None:
            raise NotFoundError("unknown job", job_id=job_id)
        return row

    # ------------------------------------------------------ state changes
    def _event(
        self,
        conn: sqlite3.Connection,
        job_id: str,
        node_id: str | None,
        run_id: str | None,
        actor: str,
        kind: str,
        from_state: str | None,
        to_state: str | None,
        payload: dict[str, Any],
    ) -> None:
        conn.execute(
            "INSERT INTO events(job_id, node_id, run_id, actor, kind, from_state, to_state,"
            " payload, at) VALUES (?,?,?,?,?,?,?,?,?)",
            (
                job_id,
                node_id,
                run_id,
                actor,
                kind,
                from_state,
                to_state,
                json.dumps(payload, ensure_ascii=False, sort_keys=True),
                utcnow().isoformat(),
            ),
        )

    def transition_job(
        self,
        job_id: str,
        expected_state: str | TaskState,
        target_state: str | TaskState,
        *,
        actor: str,
        expected_revision: int | None = None,
        reason: str | None = None,
    ) -> int:
        """Compare-and-swap one legal job transition; returns the new revision."""
        expected = parse_state(expected_state)
        target = parse_state(target_state)
        if target is TaskState.PAUSED_BUDGET and actor != "budget":
            raise AuthorizationError(
                "PAUSED_BUDGET is written by the budget ledger, not by a caller",
                actor=actor,
            )
        entering_pause = target in PAUSE_STATES and expected not in PAUSE_STATES
        if entering_pause:
            # A pause is a cross-cutting user action: any live state may be
            # suspended, and the previous state is persisted so resume is exact.
            if expected not in ACTIVE_STATES:
                raise StateTransitionError(
                    f"cannot pause a job in {expected.value}", src=expected.value,
                    dst=target.value,
                )
        else:
            assert_transition(expected, target, actor=actor)
        if target in RECONCILIATION_STATES:
            raise ConcurrencyError(
                "OUTCOME_UNKNOWN is entered by the executor, not by a direct transition",
                job_id=job_id,
            )
        with self.tx() as conn:
            row = conn.execute(
                "SELECT state, revision, prior_state FROM jobs WHERE job_id = ?", (job_id,)
            ).fetchone()
            if row is None:
                raise NotFoundError("unknown job", job_id=job_id)
            if row["state"] != expected.value:
                raise ConcurrencyError(
                    f"expected {expected.value} but persisted state is {row['state']}",
                    job_id=job_id,
                    expected=expected.value,
                    actual=row["state"],
                )
            if expected_revision is not None and int(row["revision"]) != int(expected_revision):
                raise ConcurrencyError(
                    "stale job revision",
                    job_id=job_id,
                    expected=expected_revision,
                    actual=int(row["revision"]),
                )
            prior = row["prior_state"]
            if target in PAUSE_STATES:
                prior = expected.value
            elif expected in PAUSE_STATES:
                if prior != target.value:
                    raise StateTransitionError(
                        f"a paused job may only resume to its recorded state {prior!r}",
                        src=expected.value,
                        dst=target.value,
                    )
                prior = None
            new_revision = int(row["revision"]) + 1
            conn.execute(
                "UPDATE jobs SET state = ?, revision = ?, prior_state = ?, updated_at = ?"
                " WHERE job_id = ?",
                (target.value, new_revision, prior, utcnow().isoformat(), job_id),
            )
            self._event(conn, job_id, None, None, actor, "STATE", expected.value, target.value,
                        {"reason": reason} if reason else {})
        return new_revision

    def job_state(self, job_id: str) -> TaskState:
        return parse_state(self.job(job_id)["state"])

    def job_events(self, job_id: str, limit: int = 500) -> list[dict[str, Any]]:
        with self.read() as conn:
            rows = conn.execute(
                "SELECT * FROM events WHERE job_id = ? ORDER BY seq LIMIT ?", (job_id, limit)
            ).fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------ plans
    def set_plan(self, plan: Plan, *, expected_job_revision: int) -> int:
        """Persist a complete immutable plan revision. Never resets lineage."""
        digest = content_digest(plan)
        with self.tx() as conn:
            job = conn.execute(
                "SELECT project_id, revision, state FROM jobs WHERE job_id = ?", (plan.job_id,)
            ).fetchone()
            if job is None:
                raise NotFoundError("unknown job", job_id=plan.job_id)
            if int(job["revision"]) != int(expected_job_revision):
                raise ConcurrencyError(
                    "stale job revision for replan",
                    job_id=plan.job_id,
                    expected=expected_job_revision,
                    actual=int(job["revision"]),
                )
            state = parse_state(job["state"])
            if state in TERMINAL_STATES:
                raise StateTransitionError(
                    f"cannot replan a job in {state.value}", src=state.value
                )
            if state is TaskState.WORKING:
                raise ConcurrencyError(
                    "refusing to replace the plan of a job with running work",
                    job_id=plan.job_id,
                )
            last = conn.execute(
                "SELECT MAX(revision) AS r FROM plans WHERE job_id = ?", (plan.job_id,)
            ).fetchone()
            if last and last["r"] is not None and int(plan.revision) != int(last["r"]) + 1:
                raise ConcurrencyError(
                    "plan revisions must increase by exactly one",
                    job_id=plan.job_id,
                    supplied=plan.revision,
                    expected=int(last["r"]) + 1,
                )
            if last is None and int(plan.revision) != 1:
                raise ContractError("the first plan revision must be 1", job_id=plan.job_id)
            conn.execute(
                "INSERT INTO plans(job_id, revision, plan_id, version, document, digest,"
                " authorization_digest, created_at) VALUES (?,?,?,?,?,?,?,?)",
                (
                    plan.job_id,
                    plan.revision,
                    plan.id,
                    plan.version,
                    canonical_json(plan),
                    digest,
                    plan.authorization_digest,
                    plan.created_at.isoformat(),
                ),
            )
            for node in plan.nodes:
                existing = conn.execute(
                    "SELECT lineage_key FROM nodes WHERE job_id = ? AND node_id = ?",
                    (plan.job_id, node.id),
                ).fetchone()
                if existing is not None and existing["lineage_key"] != node.lineage_key:
                    raise ConcurrencyError(
                        "a node id cannot change lineage",
                        job_id=plan.job_id,
                        node_id=node.id,
                    )
                conn.execute(
                    "INSERT INTO nodes(job_id, node_id, lineage_key, objective, role,"
                    " test_profile, write_scopes, allowed_tools, state, revision, plan_revision,"
                    " created_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?,1,?,?,?)"
                    " ON CONFLICT(job_id, node_id) DO UPDATE SET objective = excluded.objective,"
                    " role = excluded.role, test_profile = excluded.test_profile,"
                    " write_scopes = excluded.write_scopes, allowed_tools = excluded.allowed_tools,"
                    " plan_revision = excluded.plan_revision, updated_at = excluded.updated_at",
                    (
                        plan.job_id,
                        node.id,
                        node.lineage_key,
                        node.objective,
                        node.role.value,
                        node.test_profile,
                        json.dumps(node.write_scopes),
                        json.dumps([a.value for a in node.allowed_tools]),
                        TaskState.NEW.value,
                        plan.revision,
                        utcnow().isoformat(),
                        utcnow().isoformat(),
                    ),
                )
                _ensure_attempt_row(conn, job["project_id"], node.lineage_key, plan.job_id)
            for node in plan.nodes:
                for dep in node.dependencies:
                    conn.execute(
                        "INSERT OR IGNORE INTO node_edges(job_id, node_id, depends_on)"
                        " VALUES (?,?,?)",
                        (plan.job_id, node.id, dep),
                    )
            conn.execute(
                "UPDATE jobs SET revision = revision + 1, updated_at = ? WHERE job_id = ?",
                (utcnow().isoformat(), plan.job_id),
            )
            self._event(conn, plan.job_id, None, None, "manager", "PLAN",
                        None, None, {"revision": plan.revision, "digest": digest})
        return plan.revision

    def current_plan(self, job_id: str) -> Plan:
        with self.read() as conn:
            row = conn.execute(
                "SELECT document FROM plans WHERE job_id = ? ORDER BY revision DESC LIMIT 1",
                (job_id,),
            ).fetchone()
        if row is None:
            raise NotFoundError("job has no plan", job_id=job_id)
        return Plan.model_validate(json.loads(row["document"]))

    def plan_history(self, job_id: str) -> list[dict[str, Any]]:
        with self.read() as conn:
            rows = conn.execute(
                "SELECT revision, plan_id, version, digest, authorization_digest, created_at"
                " FROM plans WHERE job_id = ? ORDER BY revision",
                (job_id,),
            ).fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------ lineage
    def lineage_attempts(self, project_id: str, lineage_key: str) -> tuple[int, int]:
        with self.read() as conn:
            row = conn.execute(
                "SELECT attempts, max_attempts FROM node_attempts"
                " WHERE project_id = ? AND lineage_key = ?",
                (project_id, lineage_key),
            ).fetchone()
        if row is None:
            return 0, MAX_WORKER_ATTEMPTS
        return int(row["attempts"]), int(row["max_attempts"])

    def start_attempt(
        self,
        job_id: str,
        node_id: str,
        *,
        role: str = "worker",
        phase: str = TaskState.WORKING.value,
        lease_seconds: float = DEFAULT_LEASE_SECONDS,
        owner: str = "orchestrator",
    ) -> dict[str, Any]:
        """Consume one durable lineage attempt and open a fenced run + lease."""
        if role != "worker":
            raise AuthorizationError("only the worker role consumes worker attempts", role=role)
        with self.tx() as conn:
            job = conn.execute(
                "SELECT project_id, state FROM jobs WHERE job_id = ?", (job_id,)
            ).fetchone()
            if job is None:
                raise NotFoundError("unknown job", job_id=job_id)
            node = conn.execute(
                "SELECT lineage_key, state FROM nodes WHERE job_id = ? AND node_id = ?",
                (job_id, node_id),
            ).fetchone()
            if node is None:
                raise NotFoundError("unknown node", job_id=job_id, node_id=node_id)
            row = conn.execute(
                "SELECT attempts, max_attempts FROM node_attempts"
                " WHERE project_id = ? AND lineage_key = ?",
                (job["project_id"], node["lineage_key"]),
            ).fetchone()
            attempts = int(row["attempts"]) if row else 0
            max_attempts = int(row["max_attempts"]) if row else MAX_WORKER_ATTEMPTS
            if attempts >= max_attempts:
                raise AttemptExhausted(
                    f"worker attempt budget exhausted ({attempts}/{max_attempts}) for lineage"
                    f" {node['lineage_key']}",
                    lineage_key=node["lineage_key"],
                    attempts=attempts,
                    max_attempts=max_attempts,
                )
            attempts += 1
            conn.execute(
                "UPDATE node_attempts SET attempts = ?, updated_at = ?"
                " WHERE project_id = ? AND lineage_key = ?",
                (attempts, utcnow().isoformat(), job["project_id"], node["lineage_key"]),
            )
            fence_row = conn.execute(
                "SELECT MAX(fence) AS f FROM leases WHERE job_id = ? AND node_id = ?",
                (job_id, node_id),
            ).fetchone()
            fence = int(fence_row["f"]) + 1 if fence_row and fence_row["f"] is not None else 1
            run_id = new_id("run")
            lease_id = new_id("lease")
            at = utcnow().isoformat()
            conn.execute(
                "INSERT INTO runs(run_id, job_id, node_id, project_id, attempt, phase, role,"
                " fence, lease_id, status, created_at, updated_at)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    run_id,
                    job_id,
                    node_id,
                    job["project_id"],
                    attempts,
                    phase,
                    role,
                    fence,
                    lease_id,
                    "OPEN",
                    at,
                    at,
                ),
            )
            conn.execute(
                "UPDATE leases SET status = 'REVOKED' WHERE job_id = ? AND node_id = ?"
                " AND status = 'ACTIVE'",
                (job_id, node_id),
            )
            expires = self.clock.now() + float(lease_seconds)
            conn.execute(
                "INSERT INTO leases(lease_id, job_id, node_id, run_id, owner, fence, status,"
                " issued_at, expires_at) VALUES (?,?,?,?,?,?,'ACTIVE',?,?)",
                (lease_id, job_id, node_id, run_id, owner, fence, at, expires),
            )
            self._event(conn, job_id, node_id, run_id, owner, "ATTEMPT", None, phase,
                        {"attempt": attempts, "fence": fence})
            conn.execute(
                "UPDATE nodes SET state = ?, updated_at = ? WHERE job_id = ? AND node_id = ?",
                (phase, at, job_id, node_id),
            )
        return {
            "run_id": run_id,
            "lease_id": lease_id,
            "attempt": attempts,
            "fence": fence,
            "expires_at": expires,
            "lineage_key": node["lineage_key"],
            "max_attempts": max_attempts,
        }

    def close_run(self, run_id: str, status: str) -> None:
        """Close one run and return its node to a claimable state.

        A closed run must not leave the node parked in WORKING: that would make
        the retry path unreachable without a manual state edit. The durable
        attempt counter is what bounds retries, not a stuck node state.
        """
        with self.tx() as conn:
            row = conn.execute(
                "SELECT job_id, node_id, lease_id FROM runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            if row is None:
                raise NotFoundError("unknown run", run_id=run_id)
            conn.execute(
                "UPDATE runs SET status = ?, updated_at = ? WHERE run_id = ?",
                (status, utcnow().isoformat(), run_id),
            )
            if row["lease_id"]:
                conn.execute(
                    "UPDATE leases SET status = CASE WHEN status = 'ACTIVE' THEN 'RELEASED'"
                    " ELSE status END WHERE lease_id = ?",
                    (row["lease_id"],),
                )
            node = conn.execute(
                "SELECT state FROM nodes WHERE job_id = ? AND node_id = ?",
                (row["job_id"], row["node_id"]),
            ).fetchone()
            if node is not None and parse_state(node["state"]) is TaskState.WORKING:
                conn.execute(
                    "UPDATE nodes SET state = ?, updated_at = ?"
                    " WHERE job_id = ? AND node_id = ?",
                    (TaskState.FIX.value, utcnow().isoformat(), row["job_id"], row["node_id"]),
                )
                self._event(
                    conn,
                    row["job_id"],
                    row["node_id"],
                    run_id,
                    "orchestrator",
                    "NODE_STATE",
                    TaskState.WORKING.value,
                    TaskState.FIX.value,
                    {"reason": f"run closed as {status}"},
                )
            self._event(conn, row["job_id"], row["node_id"], run_id, "orchestrator",
                        "RUN_CLOSE", None, None, {"status": status})

    # ------------------------------------------------------------ leases
    def current_lease(self, job_id: str, node_id: str) -> sqlite3.Row | None:
        with self.read() as conn:
            return conn.execute(
                "SELECT * FROM leases WHERE job_id = ? AND node_id = ? AND status = 'ACTIVE'"
                " ORDER BY fence DESC LIMIT 1",
                (job_id, node_id),
            ).fetchone()

    def lease_is_current(self, lease_id: str, fence: int) -> bool:
        with self.read() as conn:
            row = conn.execute(
                "SELECT l.fence, l.status, l.expires_at, j.state AS job_state,"
                " n.state AS node_state"
                " FROM leases l JOIN jobs j ON j.job_id = l.job_id"
                " JOIN nodes n ON n.job_id = l.job_id AND n.node_id = l.node_id"
                " WHERE l.lease_id = ?",
                (lease_id,),
            ).fetchone()
        if row is None:
            return False
        if row["status"] != "ACTIVE" or int(row["fence"]) != int(fence):
            return False
        if self.clock.now() > float(row["expires_at"]):
            return False
        job_state = parse_state(row["job_state"])
        if job_state in TERMINAL_STATES or job_state in PAUSE_STATES:
            return False
        node_state = parse_state(row["node_state"])
        if node_state in TERMINAL_STATES or node_state in PAUSE_STATES:
            return False
        return True

    def renew_lease(self, lease_id: str, owner: str, seconds: float = DEFAULT_LEASE_SECONDS) -> float:
        with self.tx() as conn:
            row = conn.execute(
                "SELECT owner, status, fence FROM leases WHERE lease_id = ?", (lease_id,)
            ).fetchone()
            if row is None:
                raise NotFoundError("unknown lease", lease_id=lease_id)
            if row["owner"] != owner or row["status"] != "ACTIVE":
                raise ConcurrencyError("lease is not held by this owner", lease_id=lease_id)
            expires = self.clock.now() + float(seconds)
            conn.execute("UPDATE leases SET expires_at = ? WHERE lease_id = ?", (expires, lease_id))
        return expires

    def expire_leases(self) -> list[str]:
        """Mark lapsed leases EXPIRED. Fences are never reused or lowered."""
        expired: list[str] = []
        with self.tx() as conn:
            rows = conn.execute(
                "SELECT lease_id, expires_at FROM leases WHERE status = 'ACTIVE'"
            ).fetchall()
            now = self.clock.now()
            for row in rows:
                if float(row["expires_at"]) < now:
                    conn.execute(
                        "UPDATE leases SET status = 'EXPIRED' WHERE lease_id = ?",
                        (row["lease_id"],),
                    )
                    expired.append(row["lease_id"])
        return expired

    # ------------------------------------------------------------ receipts
    def record_receipt(self, receipt: TestReceipt) -> str:
        if not receipt.binding.run_id:
            raise ContractError("receipt requires a run binding")
        with self.tx() as conn:
            exists = conn.execute(
                "SELECT digest FROM test_receipts WHERE receipt_id = ?", (receipt.id,)
            ).fetchone()
            digest = content_digest(receipt)
            if exists is not None:
                if exists["digest"] != digest:
                    raise ConcurrencyError("receipt id reused with different content",
                                           receipt_id=receipt.id)
                return receipt.id
            run = conn.execute(
                "SELECT job_id, node_id, project_id FROM runs WHERE run_id = ?",
                (receipt.binding.run_id,),
            ).fetchone()
            if run is None:
                raise NotFoundError("receipt references an unknown run",
                                    run_id=receipt.binding.run_id)
            if (
                run["job_id"] != receipt.binding.job_id
                or run["node_id"] != receipt.binding.node_id
                or run["project_id"] != receipt.binding.project_id
            ):
                raise AuthorizationError(
                    "receipt binding does not match the run it claims",
                    run_id=receipt.binding.run_id,
                )
            conn.execute(
                "INSERT INTO test_receipts(receipt_id, job_id, node_id, run_id, project_id,"
                " profile_id, document, digest, exit_code, created_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
                (
                    receipt.id,
                    receipt.binding.job_id,
                    receipt.binding.node_id,
                    receipt.binding.run_id,
                    receipt.binding.project_id,
                    receipt.profile_id,
                    canonical_json(receipt),
                    digest,
                    receipt.exit_code,
                    receipt.finished_at.isoformat(),
                ),
            )
        return receipt.id

    def receipts(self, job_id: str, node_id: str | None = None) -> list[dict[str, Any]]:
        query = "SELECT * FROM test_receipts WHERE job_id = ?"
        params: list[Any] = [job_id]
        if node_id:
            query += " AND node_id = ?"
            params.append(node_id)
        with self.read() as conn:
            rows = conn.execute(query + " ORDER BY created_at", params).fetchall()
        return [dict(r) for r in rows]

    def receipt(self, receipt_id: str) -> TestReceipt:
        with self.read() as conn:
            row = conn.execute(
                "SELECT document FROM test_receipts WHERE receipt_id = ?", (receipt_id,)
            ).fetchone()
        if row is None:
            raise NotFoundError("unknown receipt", receipt_id=receipt_id)
        return TestReceipt.model_validate(json.loads(row["document"]))

    # --------------------------------------------------------- experiments
    def record_experiment(self, spec: ExperimentSpec) -> str:
        """Persist one frozen research spec, bound to a project/job/node."""
        digest = content_digest(spec)
        with self.tx() as conn:
            existing = conn.execute(
                "SELECT digest FROM experiments WHERE experiment_id = ?", (spec.id,)
            ).fetchone()
            if existing is not None:
                if existing["digest"] != digest:
                    raise ConcurrencyError(
                        "experiment id reused with different content", experiment_id=spec.id
                    )
                return spec.id
            node = conn.execute(
                "SELECT 1 FROM nodes WHERE job_id = ? AND node_id = ?",
                (spec.job_id, spec.node_id),
            ).fetchone()
            if node is None:
                raise NotFoundError(
                    "the job has no such research node for this experiment",
                    job_id=spec.job_id,
                    node_id=spec.node_id,
                )
            conn.execute(
                "INSERT INTO experiments(experiment_id, project_id, job_id, node_id, document,"
                " digest, created_at) VALUES (?,?,?,?,?,?,?)",
                (
                    spec.id,
                    spec.project_id,
                    spec.job_id,
                    spec.node_id,
                    json.dumps(spec.model_dump(mode="json"), ensure_ascii=False, sort_keys=True),
                    digest,
                    utcnow().isoformat(),
                ),
            )
        return spec.id

    def experiment(self, experiment_id: str) -> ExperimentSpec:
        with self.read() as conn:
            row = conn.execute(
                "SELECT document FROM experiments WHERE experiment_id = ?",
                (experiment_id,),
            ).fetchone()
        if row is None:
            raise NotFoundError("unknown experiment", experiment_id=experiment_id)
        return ExperimentSpec.model_validate(json.loads(row["document"]))

    def experiments(self, job_id: str) -> list[dict[str, Any]]:
        with self.read() as conn:
            rows = conn.execute(
                "SELECT experiment_id, project_id, job_id, node_id, digest, created_at"
                " FROM experiments WHERE job_id = ? ORDER BY created_at",
                (job_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    def experiment_attempts(self, experiment_id: str) -> int:
        """How many authorized research runs this frozen spec has consumed."""
        with self.read() as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS c FROM experiment_runs WHERE experiment_id = ?",
                (experiment_id,),
            ).fetchone()
        return int(row["c"]) if row else 0

    def record_experiment_run(self, run: ExperimentRun) -> str:
        """Persist one authorized research outcome; attempts are never reset."""
        digest = content_digest(run)
        with self.tx() as conn:
            existing = conn.execute(
                "SELECT digest FROM experiment_runs WHERE experiment_run_id = ?", (run.id,)
            ).fetchone()
            if existing is not None:
                if existing["digest"] != digest:
                    raise ConcurrencyError(
                        "experiment run id reused with different content",
                        experiment_run_id=run.id,
                    )
                return run.id
            spec = conn.execute(
                "SELECT project_id, job_id FROM experiments WHERE experiment_id = ?",
                (run.experiment_id,),
            ).fetchone()
            if spec is None:
                raise NotFoundError("unknown experiment", experiment_id=run.experiment_id)
            if spec["project_id"] != run.project_id or spec["job_id"] != run.job_id:
                raise AuthorizationError(
                    "the experiment run does not belong to its spec's project/job",
                    experiment_id=run.experiment_id,
                )
            if self.experiment_attempts(run.experiment_id) >= MAX_EXPERIMENT_ATTEMPTS:
                raise AttemptExhausted(
                    f"experiment attempt budget exhausted ({MAX_EXPERIMENT_ATTEMPTS})",
                    experiment_id=run.experiment_id,
                )
            conn.execute(
                "INSERT INTO experiment_runs(experiment_run_id, experiment_id, project_id,"
                " job_id, node_id, run_id, attempt, fence, outcome, document, digest,"
                " created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    run.id,
                    run.experiment_id,
                    run.project_id,
                    run.job_id,
                    run.node_id,
                    run.run_id,
                    int(run.attempt),
                    int(run.fence),
                    run.outcome,
                    json.dumps(run.model_dump(mode="json"), ensure_ascii=False, sort_keys=True),
                    digest,
                    utcnow().isoformat(),
                ),
            )
        return run.id

    def experiment_runs(self, experiment_id: str) -> list[dict[str, Any]]:
        with self.read() as conn:
            rows = conn.execute(
                "SELECT experiment_run_id, outcome, attempt, fence, digest, created_at"
                " FROM experiment_runs WHERE experiment_id = ? ORDER BY created_at",
                (experiment_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    # ------------------------------------------------------------ reviews
    def record_review(self, review: Review, *, kind: str = "manager") -> str:
        with self.tx() as conn:
            exists = conn.execute(
                "SELECT 1 FROM reviews WHERE review_id = ?", (review.id,)
            ).fetchone()
            if exists is not None:
                return review.id
            conn.execute(
                "INSERT INTO reviews(review_id, job_id, node_id, run_id, project_id, verdict,"
                " kind, content_digest, document, created_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
                (
                    review.id,
                    review.job_id,
                    review.node_id,
                    review.run_id,
                    review.project_id,
                    review.verdict,
                    kind,
                    review.content_digest,
                    canonical_json(review),
                    review.created_at.isoformat(),
                ),
            )
            self._event(conn, review.job_id, review.node_id, review.run_id,
                        review.reviewer_role, "REVIEW", None, None,
                        {"verdict": review.verdict, "kind": kind})
        return review.id

    def reviews(self, job_id: str, node_id: str | None = None) -> list[dict[str, Any]]:
        query = "SELECT * FROM reviews WHERE job_id = ?"
        params: list[Any] = [job_id]
        if node_id:
            query += " AND node_id = ?"
            params.append(node_id)
        with self.read() as conn:
            rows = conn.execute(query + " ORDER BY created_at", params).fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------ knowledge
    def append_knowledge(self, record: KnowledgeRecord) -> str:
        with self.tx() as conn:
            # project isolation: a record may only reference entities of its project
            if record.project_id is not None:
                owner = conn.execute(
                    "SELECT project_id FROM projects WHERE project_id = ?",
                    (record.project_id,),
                ).fetchone()
                if owner is None:
                    raise NotFoundError("unknown project", project_id=record.project_id)
            for ref in list(record.supersedes) + list(record.contradicts):
                found = conn.execute(
                    "SELECT project_id, scope FROM knowledge WHERE record_id = ?", (ref,)
                ).fetchone()
                if found is None:
                    raise NotFoundError("dangling knowledge reference", record_id=ref)
                if found["scope"] == "PROJECT" and found["project_id"] not in (None, record.project_id):
                    raise AuthorizationError(
                        "a record cannot supersede knowledge from another project",
                        record_id=ref,
                    )
            conn.execute(
                "INSERT INTO knowledge(record_id, scope, privacy, project_id, job_id, run_id,"
                " topic, kind, author, verification, document, source_digest, observed_at,"
                " created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    record.id,
                    record.scope,
                    record.privacy,
                    record.project_id,
                    record.job_id,
                    record.run_id,
                    record.topic,
                    record.kind,
                    record.author,
                    record.verification,
                    canonical_json(record),
                    record.source_digest,
                    record.observed_at.isoformat(),
                    utcnow().isoformat(),
                ),
            )
        return record.id

    def search_knowledge(
        self,
        *,
        project_id: str | None,
        topic: str | None = None,
        limit: int = 50,
        include_global: bool = True,
    ) -> list[dict[str, Any]]:
        """Scoped retrieval: private project knowledge never crosses projects."""
        clauses: list[str] = []
        params: list[Any] = []
        scope_clauses: list[str] = []
        if project_id is not None:
            scope_clauses.append("(scope = 'PROJECT' AND project_id = ?)")
            params.append(project_id)
        if include_global:
            scope_clauses.append("(scope = 'GLOBAL_PREFERENCE' AND privacy = 'SHARED')")
        if not scope_clauses:
            return []
        clauses.append("(" + " OR ".join(scope_clauses) + ")")
        if topic:
            clauses.append("topic = ?")
            params.append(topic)
        params.append(int(limit))
        query = "SELECT * FROM knowledge WHERE " + " AND ".join(clauses) + " ORDER BY created_at LIMIT ?"
        with self.read() as conn:
            rows = conn.execute(query, params).fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------ artifacts
    def record_artifact(self, job_id: str, run_id: str, node_id: str, ref: Any) -> None:
        """Artifacts are recorded through the executor receipt/manifest path only."""
        raise NotImplementedError(
            "artifact persistence is bound to an executor receipt; see record_context_manifest"
        )

    def record_context_manifest(self, manifest: ContextManifest) -> int:
        with self.tx() as conn:
            cur = conn.execute(
                "INSERT INTO context_manifests(job_id, node_id, run_id, attempt, document,"
                " digest, created_at) VALUES (?,?,?,?,?,?,?)",
                (
                    manifest.binding.job_id,
                    manifest.binding.node_id,
                    manifest.binding.run_id,
                    manifest.binding.attempt,
                    canonical_json(manifest),
                    content_digest(manifest),
                    manifest.created_at.isoformat(),
                ),
            )
            return int(cur.lastrowid)

    def context_manifests(self, job_id: str) -> list[dict[str, Any]]:
        with self.read() as conn:
            rows = conn.execute(
                "SELECT * FROM context_manifests WHERE job_id = ? ORDER BY manifest_id",
                (job_id,),
            ).fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------ recovery
    def crashed_runs(self) -> list[dict[str, Any]]:
        with self.read() as conn:
            rows = conn.execute(
                "SELECT * FROM runs WHERE status = 'OPEN' ORDER BY created_at"
            ).fetchall()
        return [dict(r) for r in rows]

    def checkpoint(self) -> dict[str, Any]:
        """Durable resumption facts: no model call, no queue side effect."""
        with self.read() as conn:
            jobs = conn.execute(
                "SELECT job_id, project_id, state, revision, updated_at FROM jobs ORDER BY job_id"
            ).fetchall()
            active = conn.execute(
                "SELECT COUNT(*) AS c FROM leases WHERE status = 'ACTIVE'"
            ).fetchone()
            open_runs = conn.execute(
                "SELECT COUNT(*) AS c FROM runs WHERE status = 'OPEN'"
            ).fetchone()
        return {
            "schema_version": self.schema_version(),
            "jobs": [dict(r) for r in jobs],
            "active_leases": int(active["c"]),
            "open_runs": int(open_runs["c"]),
            "pending_operations": self.pending_operations(),
        }

    # ------------------------------------------------------------ operations
    def open_operation(
        self, job_id: str, node_id: str, run_id: str, project_id: str, action: str,
        request_digest: str,
    ) -> str:
        operation_id = new_id("op")
        at = utcnow().isoformat()
        with self.tx() as conn:
            conn.execute(
                "INSERT INTO operations(operation_id, job_id, node_id, run_id, project_id, action,"
                " status, request_digest, created_at, updated_at) VALUES (?,?,?,?,?,?,'PENDING',?,?,?)",
                (operation_id, job_id, node_id, run_id, project_id, action, request_digest, at, at),
            )
        return operation_id

    def finish_operation(
        self, operation_id: str, status: str, *, result_digest: str | None = None,
        result_artifact: str | None = None, error_code: str | None = None, owner_run: str = "",
    ) -> None:
        if status not in {"SUCCEEDED", "FAILED", "CANCELLED", "OUTCOME_UNKNOWN", "RUNNING"}:
            raise ContractError("invalid operation status", status=status)
        with self.tx() as conn:
            row = conn.execute(
                "SELECT run_id, project_id FROM operations WHERE operation_id = ?",
                (operation_id,),
            ).fetchone()
            if row is None:
                raise NotFoundError("unknown operation", operation_id=operation_id)
            if owner_run and row["run_id"] != owner_run:
                raise AuthorizationError(
                    "operation belongs to another run", operation_id=operation_id
                )
            conn.execute(
                "UPDATE operations SET status = ?, result_digest = ?, result_artifact = ?,"
                " error_code = ?, updated_at = ? WHERE operation_id = ?",
                (status, result_digest, result_artifact, error_code,
                 utcnow().isoformat(), operation_id),
            )

    def operation(self, operation_id: str) -> dict[str, Any]:
        with self.read() as conn:
            row = conn.execute(
                "SELECT * FROM operations WHERE operation_id = ?", (operation_id,)
            ).fetchone()
        if row is None:
            raise NotFoundError("unknown operation", operation_id=operation_id)
        return dict(row)

    def pending_operations(self) -> list[dict[str, Any]]:
        with self.read() as conn:
            rows = conn.execute(
                "SELECT operation_id, run_id, status FROM operations"
                " WHERE status IN ('PENDING', 'RUNNING') ORDER BY created_at"
            ).fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------ mailbox
    def post_message(
        self, job_id: str, node_id: str, run_id: str, sender_role: str, kind: str, body: str
    ) -> str:
        message_id = new_id("msg")
        at = utcnow().isoformat()
        with self.tx() as conn:
            run = conn.execute(
                "SELECT job_id, node_id FROM runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            if run is None:
                raise NotFoundError("mailbox message references an unknown run", run_id=run_id)
            if run["job_id"] != job_id or run["node_id"] != node_id:
                raise AuthorizationError(
                    "mailbox message binding does not match its run", run_id=run_id
                )
            conn.execute(
                "INSERT INTO mailbox(message_id, job_id, node_id, run_id, sender_role, kind,"
                " body, document, created_at) VALUES (?,?,?,?,?,?,?,?,?)",
                (message_id, job_id, node_id, run_id, sender_role, kind, body,
                 json.dumps({"body": body, "kind": kind}), at),
            )
        return message_id

    def mailbox(self, job_id: str, node_id: str | None = None) -> list[dict[str, Any]]:
        query = "SELECT * FROM mailbox WHERE job_id = ?"
        params: list[Any] = [job_id]
        if node_id:
            query += " AND node_id = ?"
            params.append(node_id)
        with self.read() as conn:
            rows = conn.execute(query + " ORDER BY created_at", params).fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------ helpers
    def node(self, job_id: str, node_id: str) -> sqlite3.Row:
        with self.read() as conn:
            row = conn.execute(
                "SELECT * FROM nodes WHERE job_id = ? AND node_id = ?", (job_id, node_id)
            ).fetchone()
        if row is None:
            raise NotFoundError("unknown node", job_id=job_id, node_id=node_id)
        return row

    def ready_nodes(self, job_id: str) -> list[str]:
        """Nodes whose dependencies all reached a successful completion state."""
        with self.read() as conn:
            rows = conn.execute(
                "SELECT n.node_id AS node_id, n.state AS state,"
                " (SELECT COUNT(*) FROM node_edges e JOIN nodes d"
                "   ON d.job_id = e.job_id AND d.node_id = e.depends_on"
                "   WHERE e.job_id = n.job_id AND e.node_id = n.node_id"
                "     AND d.state NOT IN ('WORKER_COMPLETE','MANAGER_APPROVED','INTEGRATED',"
                "                         'READY_TO_APPLY','APPLIED','DONE')) AS unmet"
                " FROM nodes n WHERE n.job_id = ? ORDER BY n.node_id",
                (job_id,),
            ).fetchall()
        ready: list[str] = []
        for row in rows:
            state = parse_state(row["state"])
            if state is TaskState.WORKING:
                continue
            if state in TERMINAL_STATES or state in PAUSE_STATES:
                continue
            if int(row["unmet"]) == 0:
                ready.append(row["node_id"])
        return ready

    def set_node_state(
        self, job_id: str, node_id: str, expected: str | TaskState, target: str | TaskState,
        *, actor: str,
    ) -> None:
        src, dst = parse_state(expected), parse_state(target)
        assert_transition(src, dst, actor=actor)
        with self.tx() as conn:
            row = conn.execute(
                "SELECT state FROM nodes WHERE job_id = ? AND node_id = ?", (job_id, node_id)
            ).fetchone()
            if row is None:
                raise NotFoundError("unknown node", job_id=job_id, node_id=node_id)
            if row["state"] != src.value:
                raise ConcurrencyError(
                    "stale node state", job_id=job_id, node_id=node_id,
                    expected=src.value, actual=row["state"],
                )
            conn.execute(
                "UPDATE nodes SET state = ?, updated_at = ? WHERE job_id = ? AND node_id = ?",
                (dst.value, utcnow().isoformat(), job_id, node_id),
            )
            self._event(conn, job_id, node_id, None, actor, "NODE_STATE", src.value, dst.value, {})


def _ensure_attempt_row(
    conn: sqlite3.Connection, project_id: str, lineage_key: str, job_id: str
) -> None:
    conn.execute(
        "INSERT OR IGNORE INTO node_attempts(project_id, lineage_key, attempts, max_attempts,"
        " first_job_id, updated_at) VALUES (?,?,0,?,?,?)",
        (project_id, lineage_key, MAX_WORKER_ATTEMPTS, job_id, utcnow().isoformat()),
    )


def is_attempt_state(state: str | TaskState) -> bool:
    return parse_state(state) in ATTEMPT_STATES
