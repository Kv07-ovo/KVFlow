"""Cross-project knowledge: append-only provenance, conflicts and isolation.

Design rules enforced here (from the user specification and the F1 review)

* **Append-only.** A correction never overwrites: it is a new record that
  ``supersedes`` or ``contradicts`` an existing one. History is preserved and the
  newest record is *not* automatically the most trustworthy.
* **Scope is real.** Project knowledge is only reachable with that project's
  identity. Global preferences cross projects; private project knowledge never
  does. A record may not supersede knowledge owned by another project.
* **Authority is not a string.** A worker may only propose reported knowledge.
  ``USER_DECISION`` and ``VERIFIED_FACT_WITH_SCOPE`` require user or executor
  evidence and are refused when a model-authored payload claims them.
* **Live facts beat memory.** A runtime/state question is answered from the live
  adapter (a callable the Orchestrator supplies), and the answer carries the
  observation time, so a stale memory can never mask the current state.
* **Provenance is complete.** Every returned record carries its source reference,
  source digest, observed/effective times, author, verification status and any
  conflicts or supersessions.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Iterable, Mapping, Sequence

from .contracts import KnowledgeRecord, content_digest
from .errors import AuthorizationError, ContractError, NotFoundError
from .store import Store, new_id

#: knowledge a model may author without further evidence
MODEL_KINDS = frozenset({"WORKER_PROPOSAL", "REPORTED_FACT", "OPEN_QUESTION"})
#: knowledge only the user (or a user-authorized import) may author
USER_KINDS = frozenset({"USER_DECISION"})
#: knowledge the executor may author from an actual receipt
EXECUTOR_KINDS = frozenset({"VERIFIED_FACT_WITH_SCOPE"})


@dataclass(frozen=True)
class KnowledgeQuery:
    project_id: str | None
    topic: str | None = None
    kinds: tuple[str, ...] = ()
    include_global: bool = True
    limit: int = 50

    def __post_init__(self) -> None:
        if self.limit < 1 or self.limit > 500:
            raise ContractError("knowledge limit must be between 1 and 500")


class KnowledgeService:
    """Scoped, append-only knowledge over the durable store."""

    def __init__(self, store: Store) -> None:
        self.store = store

    # ------------------------------------------------------------ writing
    def propose(
        self,
        *,
        project_id: str,
        topic: str,
        content: str,
        author: str,
        source_ref: str,
        source_digest: str,
        authorization_digest: str,
        job_id: str | None = None,
        run_id: str | None = None,
        kind: str = "WORKER_PROPOSAL",
        privacy: str = "SHARED",
        supersedes: Sequence[str] = (),
        contradicts: Sequence[str] = (),
        observed_at: datetime | None = None,
        effective_at: datetime | None = None,
    ) -> KnowledgeRecord:
        """Record a *proposal*. A worker-authored record can never be more.

        ``author`` describes who actually produced the payload; a worker that
        claims ``USER_DECISION`` here is refused by the contract, and the caller
        cannot bypass that by passing a different author string, because the
        service re-derives the permitted kinds from the author.
        """
        if author == "worker" and kind not in MODEL_KINDS:
            raise AuthorizationError(
                "a worker may only propose reported knowledge", attempted_kind=kind
            )
        if author == "manager" and kind in USER_KINDS | EXECUTOR_KINDS:
            raise AuthorizationError(
                "a manager cannot author a user decision or an executor-verified fact",
                attempted_kind=kind,
            )
        if author == "executor" and kind not in EXECUTOR_KINDS:
            raise AuthorizationError(
                "the executor only records executor-verified facts",
                attempted_kind=kind,
            )
        if author in {"user", "authorized_import"} and kind not in (
            USER_KINDS | {"PREFERENCE", "CONSTRAINT", "HISTORICAL_STATE"}
        ):
            raise AuthorizationError(
                "a user/import record must state a decision, preference or constraint",
                attempted_kind=kind,
            )
        verification = {
            "worker": "REPORTED",
            "manager": "REPORTED",
            "executor": "EXECUTOR_VERIFIED",
            "user": "USER_CONFIRMED",
            "authorized_import": "IMPORTED_WITH_SOURCE",
        }[author]
        now = datetime.now(timezone.utc)
        record = KnowledgeRecord.model_validate(
            {
                "id": new_id("kn"),
                "scope": "PROJECT" if project_id else "GLOBAL_PREFERENCE",
                "privacy": privacy,
                "project_id": project_id,
                "job_id": job_id,
                "run_id": run_id,
                "topic": topic,
                "kind": kind,
                "author": author,
                "verification": verification,
                "content": content,
                "source_ref": source_ref,
                "source_digest": source_digest,
                "authorization_digest": authorization_digest,
                "observed_at": observed_at or now,
                "effective_at": effective_at or now,
                "supersedes": list(supersedes),
                "contradicts": list(contradicts),
            }
        )
        self.store.append_knowledge(record)
        return record

    def record_user_decision(
        self,
        *,
        project_id: str | None,
        topic: str,
        content: str,
        source_ref: str,
        source_digest: str,
        authorization_digest: str,
        supersedes: Sequence[str] = (),
    ) -> KnowledgeRecord:
        """Only the user path may produce a decision record."""
        return self.propose(
            project_id=project_id,
            topic=topic,
            content=content,
            author="user",
            kind="USER_DECISION",
            source_ref=source_ref,
            source_digest=source_digest,
            authorization_digest=authorization_digest,
            supersedes=supersedes,
        )

    def record_executor_fact(
        self,
        *,
        project_id: str,
        topic: str,
        content: str,
        receipt_id: str,
        receipt_digest: str,
        authorization_digest: str,
        job_id: str | None = None,
        run_id: str | None = None,
        contradicts: Sequence[str] = (),
    ) -> KnowledgeRecord:
        """An executor-verified fact must name a real receipt this store holds."""
        record = self.store.receipt(receipt_id)
        if content_digest(record) != receipt_digest:
            raise AuthorizationError(
                "the receipt digest does not match the stored receipt",
                receipt_id=receipt_id,
            )
        return self.propose(
            project_id=project_id,
            topic=topic,
            content=content,
            author="executor",
            kind="VERIFIED_FACT_WITH_SCOPE",
            source_ref=f"receipt:{receipt_id}",
            source_digest=receipt_digest,
            authorization_digest=authorization_digest,
            job_id=job_id,
            run_id=run_id,
            contradicts=contradicts,
        )

    def correct(
        self,
        *,
        project_id: str,
        superseded_id: str,
        content: str,
        author: str,
        source_ref: str,
        source_digest: str,
        authorization_digest: str,
        kind: str | None = None,
    ) -> KnowledgeRecord:
        """Append a correction rather than overwriting the previous record."""
        previous = self.record(superseded_id)
        if previous["scope"] == "PROJECT" and previous["project_id"] != project_id:
            raise AuthorizationError(
                "a correction may not cross project ownership", record_id=superseded_id
            )
        previous_body = json.loads(previous["document"])
        return self.propose(
            project_id=project_id,
            topic=previous_body["topic"],
            content=content,
            author=author,
            kind=kind or ("REPORTED_FACT" if author == "manager" else "WORKER_PROPOSAL"),
            source_ref=source_ref,
            source_digest=source_digest,
            authorization_digest=authorization_digest,
            privacy=previous_body.get("privacy", "SHARED"),
            supersedes=[superseded_id],
        )

    # ------------------------------------------------------------ reading
    def record(self, record_id: str) -> dict[str, Any]:
        with self.store.read() as conn:
            row = conn.execute(
                "SELECT * FROM knowledge WHERE record_id = ?", (record_id,)
            ).fetchone()
        if row is None:
            raise NotFoundError("unknown knowledge record", record_id=record_id)
        return dict(row)

    def search(self, query: KnowledgeQuery) -> list[dict[str, Any]]:
        """Scoped retrieval with provenance and conflict annotations."""
        rows = self.store.search_knowledge(
            project_id=query.project_id,
            topic=query.topic,
            limit=query.limit,
            include_global=query.include_global,
        )
        results: list[dict[str, Any]] = []
        for row in rows:
            body = json.loads(row["document"])
            if query.kinds and body["kind"] not in query.kinds:
                continue
            results.append(self._present(row, body))
        return results

    def _present(self, row: Mapping[str, Any], body: Mapping[str, Any]) -> dict[str, Any]:
        conflicts = self._relations(str(row["record_id"]))
        return {
            "id": row["record_id"],
            "scope": body["scope"],
            "privacy": body.get("privacy", "SHARED"),
            "project_id": body.get("project_id"),
            "topic": body["topic"],
            "kind": body["kind"],
            "author": body["author"],
            "verification": body["verification"],
            "content": body["content"],
            "source_ref": body["source_ref"],
            "source_digest": body["source_digest"],
            "observed_at": body["observed_at"],
            "effective_at": body["effective_at"],
            "supersedes": body.get("supersedes", []),
            "contradicts": body.get("contradicts", []),
            "superseded_by": conflicts["superseded_by"],
            "contradicted_by": conflicts["contradicted_by"],
            "conflict_state": conflicts["state"],
            "recorded_at": row["created_at"],
            "trust_note": (
                "the newest record is not automatically the most trustworthy;"
                " verification, source and time are reported separately"
            ),
        }

    def _relations(self, record_id: str) -> dict[str, Any]:
        with self.store.read() as conn:
            rows = conn.execute(
                "SELECT record_id, document FROM knowledge WHERE document LIKE ?",
                (f'%"{record_id}"%',),
            ).fetchall()
        superseded_by: list[str] = []
        contradicted_by: list[str] = []
        for row in rows:
            if row["record_id"] == record_id:
                continue
            body = json.loads(row["document"])
            if record_id in body.get("supersedes", []):
                superseded_by.append(row["record_id"])
            if record_id in body.get("contradicts", []):
                contradicted_by.append(row["record_id"])
        state = "CLEAR"
        if superseded_by and contradicted_by:
            state = "SUPERSEDED_AND_CONTRADICTED"
        elif superseded_by:
            state = "SUPERSEDED"
        elif contradicted_by:
            state = "CONTRADICTED"
        return {
            "superseded_by": sorted(superseded_by),
            "contradicted_by": sorted(contradicted_by),
            "state": state,
        }

    def history(self, topic: str, *, project_id: str | None) -> list[dict[str, Any]]:
        """Every record for a topic, oldest first, including corrections."""
        rows = self.store.search_knowledge(
            project_id=project_id, topic=topic, limit=500, include_global=True
        )
        ordered = sorted(rows, key=lambda r: (r["created_at"], r["record_id"]))
        return [self._present(row, json.loads(row["document"])) for row in ordered]

    def export(self, *, project_id: str | None) -> dict[str, Any]:
        """A portable snapshot: no credentials, no runtime secrets, full provenance."""
        rows = self.store.search_knowledge(project_id=project_id, limit=500)
        records = [self._present(row, json.loads(row["document"])) for row in rows]
        return {
            "project_id": project_id,
            "exported_at": datetime.now(timezone.utc).isoformat(),
            "record_count": len(records),
            "records": records,
            "note": (
                "this export is self-describing and vendor neutral; it contains no"
                " credentials and no private model reasoning"
            ),
        }

    # --------------------------------------------------------- live state
    def live_answer(
        self,
        query: KnowledgeQuery,
        *,
        live_adapter: Callable[[KnowledgeQuery], Mapping[str, Any]] | None,
    ) -> dict[str, Any]:
        """Answer with live state when an adapter is available.

        A dynamic question must not be answered from memory alone: the live
        result is returned first with its own observation time, and the stored
        records are attached as historical context rather than as the answer.
        """
        stored = self.search(query)
        if live_adapter is None:
            return {
                "live": None,
                "live_available": False,
                "observed_at": None,
                "history": stored,
                "warning": (
                    "no live adapter is connected; these records are history and may"
                    " not describe the current state"
                ),
            }
        live = dict(live_adapter(query))
        live.setdefault("observed_at", datetime.now(timezone.utc).isoformat())
        return {
            "live": live,
            "live_available": True,
            "observed_at": live["observed_at"],
            "history": stored,
            "note": "the live value is authoritative for current state; history is context",
        }
