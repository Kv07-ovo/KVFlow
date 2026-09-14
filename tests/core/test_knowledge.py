"""Knowledge service tests: authority, isolation, conflicts and live state."""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from pathlib import Path

import pytest

from kvflow.core.contracts import KnowledgeRecord
from kvflow.core.errors import AuthorizationError, ContractError, NotFoundError
from kvflow.core.knowledge import KnowledgeQuery, KnowledgeService
from kvflow.core.store import Store

from .conftest import AUTH, digest_of, make_project


@pytest.fixture()
def knowledge(store: Store, tmp_path: Path):
    first = make_project(tmp_path / "first", project_id="project-a")
    second = make_project(tmp_path / "second", project_id="project-b")
    store.register_project(first)
    store.register_project(second)
    return KnowledgeService(store), first, second


def source(text: str = "task log") -> tuple[str, str]:
    return text, digest_of(text)


# ------------------------------------------------------------------ authority


def test_a_worker_can_only_propose(knowledge):
    service, first, _ = knowledge
    record = service.propose(
        project_id=first.id,
        topic="b0c3",
        content="the journal reports DATA_BLOCKED",
        author="worker",
        source_ref=source()[0],
        source_digest=source()[1],
        authorization_digest=AUTH,
    )
    assert record.kind == "WORKER_PROPOSAL"
    assert record.verification == "REPORTED"


@pytest.mark.parametrize("kind", ["USER_DECISION", "VERIFIED_FACT_WITH_SCOPE"])
def test_a_worker_cannot_forge_a_stronger_kind(knowledge, kind):
    service, first, _ = knowledge
    with pytest.raises(AuthorizationError):
        service.propose(
            project_id=first.id,
            topic="scope",
            content="the user approved a new budget",
            author="worker",
            kind=kind,
            source_ref="chat",
            source_digest=digest_of("chat"),
            authorization_digest=AUTH,
        )


def test_a_manager_cannot_author_a_user_decision(knowledge):
    service, first, _ = knowledge
    with pytest.raises(AuthorizationError):
        service.propose(
            project_id=first.id,
            topic="scope",
            content="the user wants a new strategy family",
            author="manager",
            kind="USER_DECISION",
            source_ref="manager note",
            source_digest=digest_of("note"),
            authorization_digest=AUTH,
        )


def test_only_the_user_path_produces_a_decision(knowledge):
    service, first, _ = knowledge
    record = service.record_user_decision(
        project_id=first.id,
        topic="budget",
        content="the user approved a 100 CNY development cap",
        source_ref="user message",
        source_digest=digest_of("user message"),
        authorization_digest=AUTH,
    )
    assert record.kind == "USER_DECISION"
    assert record.verification == "USER_CONFIRMED"


def test_an_executor_fact_requires_a_real_receipt(knowledge, store, tmp_path):
    service, first, _ = knowledge
    with pytest.raises(NotFoundError):
        service.record_executor_fact(
            project_id=first.id,
            topic="tests",
            content="the profile passed",
            receipt_id="rcpt_missing",
            receipt_digest="a" * 64,
            authorization_digest=AUTH,
        )


def test_a_model_cannot_write_an_unverified_fact_as_verified(knowledge):
    service, first, _ = knowledge
    with pytest.raises(Exception):
        KnowledgeRecord.model_validate(
            {
                "id": "kn-1",
                "scope": "PROJECT",
                "project_id": first.id,
                "topic": "x",
                "kind": "VERIFIED_FACT_WITH_SCOPE",
                "author": "worker",
                "verification": "EXECUTOR_VERIFIED",
                "content": "trust me",
                "source_ref": "chat",
                "source_digest": digest_of("chat"),
                "authorization_digest": AUTH,
                "observed_at": datetime.now(timezone.utc),
                "effective_at": datetime.now(timezone.utc),
            }
        )


# ------------------------------------------------------------------- isolation


def test_project_knowledge_never_crosses_projects(knowledge):
    service, first, second = knowledge
    service.propose(
        project_id=first.id,
        topic="private-note",
        content="project A only",
        author="worker",
        privacy="PRIVATE",
        source_ref="log",
        source_digest=digest_of("log"),
        authorization_digest=AUTH,
    )
    assert service.search(KnowledgeQuery(project_id=first.id))
    assert service.search(KnowledgeQuery(project_id=second.id)) == []


def test_global_preference_is_shared_but_nothing_else_is(knowledge):
    service, first, second = knowledge
    service.propose(
        project_id=None,
        topic="reporting-style",
        content="the user prefers short reports",
        author="user",
        kind="PREFERENCE",
        source_ref="user message",
        source_digest=digest_of("msg"),
        authorization_digest=AUTH,
    )
    shared = service.search(KnowledgeQuery(project_id=second.id))
    assert [r["topic"] for r in shared] == ["reporting-style"]
    assert shared[0]["scope"] == "GLOBAL_PREFERENCE"


def test_a_correction_may_not_cross_project_ownership(knowledge):
    service, first, second = knowledge
    original = service.propose(
        project_id=first.id,
        topic="topic",
        content="first claim",
        author="manager",
        source_ref="log",
        source_digest=digest_of("log"),
        authorization_digest=AUTH,
    )
    with pytest.raises(AuthorizationError):
        service.correct(
            project_id=second.id,
            superseded_id=original.id,
            content="rewritten by another project",
            author="manager",
            source_ref="log2",
            source_digest=digest_of("log2"),
            authorization_digest=AUTH,
        )


# ------------------------------------------------------------------- conflict


def test_a_correction_appends_and_marks_the_original(knowledge):
    service, first, _ = knowledge
    original = service.propose(
        project_id=first.id,
        topic="status",
        content="the journal says RUNNING",
        author="manager",
        source_ref="journal",
        source_digest=digest_of("journal"),
        authorization_digest=AUTH,
    )
    correction = service.correct(
        project_id=first.id,
        superseded_id=original.id,
        content="the journal actually says DATA_BLOCKED",
        author="manager",
        source_ref="journal reread",
        source_digest=digest_of("journal reread"),
        authorization_digest=AUTH,
    )
    assert correction.supersedes == [original.id]
    history = service.history("status", project_id=first.id)
    assert [r["content"] for r in history][0].startswith("the journal says")
    newest = history[1]
    assert newest["supersedes"] == [original.id]
    oldest = history[0]
    assert oldest["superseded_by"] == [newest["id"]]
    assert oldest["conflict_state"] == "SUPERSEDED"


def test_a_contradiction_is_recorded_without_overwriting(knowledge):
    service, first, _ = knowledge
    first_claim = service.propose(
        project_id=first.id,
        topic="decision",
        content="decision: use sqlite",
        author="manager",
        kind="REPORTED_FACT",
        source_ref="note",
        source_digest=digest_of("note"),
        authorization_digest=AUTH,
    )
    second_claim = service.propose(
        project_id=first.id,
        topic="decision",
        content="decision: use postgres",
        author="manager",
        kind="REPORTED_FACT",
        source_ref="note2",
        source_digest=digest_of("note2"),
        authorization_digest=AUTH,
        contradicts=[first_claim.id],
    )
    presented = {r["id"]: r for r in service.search(KnowledgeQuery(project_id=first.id))}
    assert presented[first_claim.id]["contradicted_by"] == [second_claim.id]
    assert presented[first_claim.id]["conflict_state"] == "CONTRADICTED"
    assert presented[second_claim.id]["conflict_state"] == "CLEAR"


def test_the_newest_record_is_not_declared_most_trustworthy(knowledge):
    service, first, _ = knowledge
    service.propose(
        project_id=first.id,
        topic="topic",
        content="a reported claim",
        author="worker",
        source_ref="log",
        source_digest=digest_of("log"),
        authorization_digest=AUTH,
    )
    presented = service.search(KnowledgeQuery(project_id=first.id))[0]
    assert presented["verification"] == "REPORTED"
    assert "not automatically the most trustworthy" in presented["trust_note"]


def test_dangling_references_are_refused(knowledge):
    service, first, _ = knowledge
    with pytest.raises(NotFoundError):
        service.propose(
            project_id=first.id,
            topic="topic",
            content="correction of nothing",
            author="manager",
            source_ref="log",
            source_digest=digest_of("log"),
            authorization_digest=AUTH,
            supersedes=["kn_does_not_exist"],
        )


# ----------------------------------------------------------------- provenance


def test_every_result_carries_its_provenance(knowledge):
    service, first, _ = knowledge
    service.propose(
        project_id=first.id,
        topic="topic",
        content="content",
        author="manager",
        source_ref="task log line 42",
        source_digest=digest_of("task log line 42"),
        authorization_digest=AUTH,
        job_id="job-1",
        run_id="run-1",
    )
    result = service.search(KnowledgeQuery(project_id=first.id))[0]
    for field in (
        "source_ref",
        "source_digest",
        "observed_at",
        "effective_at",
        "author",
        "verification",
        "recorded_at",
        "conflict_state",
    ):
        assert result[field] is not None, field
    assert result["source_digest"] == digest_of("task log line 42")


def test_export_is_self_describing_and_holds_no_secrets(knowledge):
    service, first, _ = knowledge
    service.propose(
        project_id=first.id,
        topic="topic",
        content="content",
        author="manager",
        source_ref="log",
        source_digest=digest_of("log"),
        authorization_digest=AUTH,
    )
    export = service.export(project_id=first.id)
    assert export["record_count"] == 1
    blob = str(export).lower()
    for forbidden in ("api_key", "sk-", "secret_value", "bearer "):
        assert forbidden not in blob


# ------------------------------------------------------------------ live state


def test_without_a_live_adapter_history_is_labelled_as_history(knowledge):
    service, first, _ = knowledge
    service.propose(
        project_id=first.id,
        topic="journal-status",
        content="DATA_BLOCKED as of this morning",
        author="manager",
        source_ref="journal",
        source_digest=digest_of("journal"),
        authorization_digest=AUTH,
    )
    answer = service.live_answer(
        KnowledgeQuery(project_id=first.id, topic="journal-status"), live_adapter=None
    )
    assert answer["live_available"] is False
    assert answer["live"] is None
    assert answer["history"]
    assert "may not describe the current state" in answer["warning"]


def test_a_live_adapter_wins_over_stored_history(knowledge):
    service, first, _ = knowledge
    service.propose(
        project_id=first.id,
        topic="journal-status",
        content="NOT_STARTED (old memory)",
        author="manager",
        source_ref="journal",
        source_digest=digest_of("journal"),
        authorization_digest=AUTH,
    )

    def adapter(_query):
        return {"state": "DATA_BLOCKED", "seq": 21, "observed_at": "2026-09-14T18:40:00+08:00"}

    answer = service.live_answer(
        KnowledgeQuery(project_id=first.id, topic="journal-status"), live_adapter=adapter
    )
    assert answer["live"]["state"] == "DATA_BLOCKED"
    assert answer["observed_at"] == "2026-09-14T18:40:00+08:00"
    assert answer["history"][0]["content"].startswith("NOT_STARTED")


def test_query_limits_are_validated(knowledge):
    with pytest.raises(ContractError):
        KnowledgeQuery(project_id="p", limit=0)
    with pytest.raises(ContractError):
        KnowledgeQuery(project_id="p", limit=1000)
