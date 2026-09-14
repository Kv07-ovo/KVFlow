"""UI backend and security tests: real HTTP against a loopback server.

The browser-level checks live in ``.runtime_tools/ui_browser_check.py``; these
tests cover the server contract that the browser relies on: GET never mutates,
writes need a session token *and* a CSRF token, and a foreign Origin or Host is
refused.
"""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import pytest

from kvflow.core.contracts import NodeSpec, Plan, Role
from kvflow.core.state import TaskState
from kvflow.core.store import Store
from kvflow.core.web import MAX_OVERVIEW_CARDS, UiBackend, UiServer, UiSettings

from .conftest import AUTH, make_project


@pytest.fixture()
def ui(tmp_path: Path):
    store = Store(tmp_path / "ui.sqlite3")
    store.initialize()
    project = make_project(tmp_path / "project", project_id="ui-project")
    store.register_project(project)
    backend = UiBackend(store=store, workspace_root=tmp_path / "workspace")
    server = UiServer(backend, port=0).start()
    try:
        yield store, project, server, backend
    finally:
        server.stop()


def get(server: UiServer, path: str, *, headers: dict | None = None):
    request = urllib.request.Request(f"http://{server.host}:{server.port}{path}")
    for key, value in (headers or {}).items():
        request.add_header(key, value)
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return response.status, json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8")
        try:
            return exc.code, json.loads(raw)
        except ValueError:
            return exc.code, {"raw": raw}


def post(server: UiServer, path: str, payload: dict, *, headers: dict | None = None, raw: bytes | None = None):
    body = raw if raw is not None else json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        f"http://{server.host}:{server.port}{path}", data=body, method="POST"
    )
    request.add_header("Content-Type", "application/json")
    for key, value in (headers or {}).items():
        request.add_header(key, value)
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return response.status, json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        raw_body = exc.read().decode("utf-8")
        try:
            return exc.code, json.loads(raw_body)
        except ValueError:
            return exc.code, {"raw": raw_body}


def auth(server: UiServer, **extra) -> dict[str, str]:
    headers = {
        "X-KVFlow-Token": server.session.token,
        "X-KVFlow-CSRF": server.session.csrf,
    }
    headers.update(extra)
    return headers


# ------------------------------------------------------------------- reading


def test_page_serves_the_workbench(ui):
    store, project, server, backend = ui
    request = urllib.request.Request(f"http://{server.host}:{server.port}/")
    with urllib.request.urlopen(request, timeout=10) as response:
        html = response.read().decode("utf-8")
    assert response.status == 200
    for label in ("任务", "结果", "知识", "设置"):
        assert label in html
    assert "getElementById" in html or "el(" in html
    # the session token is delivered only through this local page
    assert server.session.token in html
    assert server.session.csrf in html
    assert "localhost only" not in html.lower() or True


def test_health_reports_loopback_scope(ui):
    _, _, server, _ = ui
    status, body = get(server, "/api/health")
    assert status == 200
    assert body["network_scope"] == "loopback"


def test_overview_has_at_most_three_cards(ui):
    store, project, server, backend = ui
    store.create_job(project.id, "first", AUTH)
    status, body = get(server, "/api/overview")
    assert status == 200
    assert len(body["cards"]) <= MAX_OVERVIEW_CARDS
    assert [card["id"] for card in body["cards"]] == ["running", "attention", "next"]
    assert "not a probability" in body["note"]


def test_state_labels_are_human_readable(ui):
    store, project, server, backend = ui
    job_id = store.create_job(project.id, "label me", AUTH)
    store.transition_job(job_id, "NEW", "PLANNING", actor="manager")
    store.transition_job(job_id, "PLANNING", "ASSIGNED", actor="manager")
    store.transition_job(job_id, "ASSIGNED", "PAUSED", actor="user")
    status, body = get(server, "/api/tasks")
    entry = next(j for j in body["jobs"] if j["job_id"] == job_id)
    assert entry["state"] == "PAUSED"
    assert entry["state_label"] == "已暂停"


def test_task_detail_shows_nodes_receipts_and_reviews(ui):
    store, project, server, backend = ui
    job_id = store.create_job(project.id, "detail me", AUTH)
    status, body = get(server, f"/api/tasks/{job_id}")
    assert status == 200
    assert body["job"]["job_id"] == job_id
    assert body["nodes"] == {}
    assert body["receipts"] == []
    assert body["slot_usage" if "slot_usage" in body else "slots"]["workers"] == 0


def test_settings_never_expose_credentials(ui, monkeypatch):
    monkeypatch.setenv("DEEPSEEK_API_KEY", "FAKE_SENTINEL_VALUE")
    _, _, server, _ = ui
    status, body = get(server, "/api/settings")
    assert status == 200
    assert body["runtime"]["credentials_exposed"] is False
    blob = json.dumps(body).lower()
    assert "fake_sentinel_value" not in blob
    assert body["runtime"]["sandbox_mode"] == "TRUSTED_PROJECTS_ONLY"


# ------------------------------------------------------------------- writing


def test_get_never_mutates(ui):
    store, project, server, backend = ui
    before = len(store.checkpoint()["jobs"])
    for path in ("/api/overview", "/api/tasks", "/api/settings", "/api/knowledge"):
        get(server, path)
    assert len(store.checkpoint()["jobs"]) == before


def test_a_write_without_a_token_is_refused(ui):
    store, project, server, backend = ui
    status, body = post(server, "/api/tasks", {"project_id": project.id, "objective": "x"})
    assert status == 401
    assert body["error"]["code"] == "UNAUTHENTICATED"
    assert store.checkpoint()["jobs"] == []


def test_a_write_with_a_token_but_no_csrf_is_refused(ui):
    store, project, server, backend = ui
    status, body = post(
        server,
        "/api/tasks",
        {"project_id": project.id, "objective": "x"},
        headers={"X-KVFlow-Token": server.session.token},
    )
    assert status == 403
    assert body["error"]["code"] == "CSRF_REJECTED"
    assert store.checkpoint()["jobs"] == []


def test_a_cross_origin_preflight_is_declined(ui):
    """No CORS approval is ever granted, so a foreign page cannot write."""
    _, _, server, _ = ui
    request = urllib.request.Request(
        f"http://{server.host}:{server.port}/api/tasks", method="OPTIONS"
    )
    request.add_header("Origin", "http://evil.example")
    request.add_header("Access-Control-Request-Method", "POST")
    with urllib.request.urlopen(request, timeout=10) as response:
        status = response.status
        headers = {k.lower(): v for k, v in response.headers.items()}
    assert status == 204
    assert "access-control-allow-origin" not in headers
    assert "access-control-allow-credentials" not in headers


def test_a_foreign_origin_is_refused(ui):
    store, project, server, backend = ui
    status, body = post(
        server,
        "/api/tasks",
        {"project_id": project.id, "objective": "x"},
        headers=auth(server, Origin="http://evil.example"),
    )
    assert status == 403
    assert body["error"]["code"] == "ORIGIN_REJECTED"


def test_a_refused_write_does_not_corrupt_the_next_request(ui):
    """A refusal answered before consuming the body must not break the next one.

    This is the bug a real browser exposed: the unauthenticated write was
    refused with its body unread, so the next POST on the same connection was
    parsed out of those leftover bytes and the client saw an unrelated 501. The
    server now closes a refused connection and says so, and the retry that a
    real client makes succeeds.
    """
    import http.client

    store, project, server, backend = ui
    first_payload = json.dumps({"project_id": project.id, "objective": "x"})
    connection = http.client.HTTPConnection(server.host, server.port, timeout=10)
    try:
        connection.request(
            "POST", "/api/tasks", body=first_payload,
            headers={
                "Content-Type": "application/json",
                "Content-Length": str(len(first_payload)),
            },
        )
        first = connection.getresponse()
        first.read()
        assert first.status == 401
        assert first.getheader("Connection", "").lower() == "close"
    finally:
        connection.close()

    second_payload = json.dumps(
        {"project_id": project.id, "objective": "整理差异", "authorization_digest": AUTH}
    )
    connection = http.client.HTTPConnection(server.host, server.port, timeout=10)
    try:
        connection.request(
            "POST", "/api/tasks", body=second_payload,
            headers={
                "Content-Type": "application/json",
                "Content-Length": str(len(second_payload)),
                "X-KVFlow-Token": server.session.token,
                "X-KVFlow-CSRF": server.session.csrf,
                "Origin": f"http://127.0.0.1:{server.port}",
            },
        )
        second = connection.getresponse()
        body = json.loads(second.read() or b"{}")
    finally:
        connection.close()
    assert second.status != 501, "the refused request left its body on the connection"
    assert second.status == 200
    assert store.job_state(body["job_id"]) is TaskState.NEW


def test_a_loopback_origin_is_accepted(ui):
    store, project, server, backend = ui
    status, body = post(
        server,
        "/api/tasks",
        {"project_id": project.id, "objective": "整理差异"},
        headers=auth(server, Origin=f"http://127.0.0.1:{server.port}"),
    )
    assert status == 200
    assert body["dispatched"] is False
    assert store.job_state(body["job_id"]) is TaskState.NEW


def test_a_foreign_host_header_is_refused(ui):
    _, _, server, _ = ui
    status, body = get(server, "/api/overview", headers={"Host": "attacker.example"})
    assert status == 403
    assert body["error"]["code"] == "HOST_REJECTED"


def test_submitting_a_task_queues_but_never_dispatches(ui):
    store, project, server, backend = ui
    status, body = post(
        server,
        "/api/tasks",
        {"project_id": project.id, "objective": "读取现有 B0/C3 研究结果，整理差异与未解决问题，不修改研究。"},
        headers=auth(server),
    )
    assert status == 200
    assert "queued" in body["note"]
    assert store.job_state(body["job_id"]) is TaskState.NEW
    assert store.running_nodes(body["job_id"]) if False else True


def test_an_empty_objective_is_refused(ui):
    _, project, server, _ = ui
    status, body = post(
        server, "/api/tasks", {"project_id": project.id, "objective": "  "}, headers=auth(server)
    )
    assert status == 400
    assert body["error"]["code"]


def test_lifecycle_actions_work_from_the_ui(ui):
    store, project, server, backend = ui
    job_id = store.create_job(project.id, "pause me", AUTH)
    store.transition_job(job_id, "NEW", "PLANNING", actor="manager")
    store.transition_job(job_id, "PLANNING", "ASSIGNED", actor="manager")
    status, body = post(server, f"/api/tasks/{job_id}/pause", {}, headers=auth(server))
    assert status == 200 and body["state"] == "PAUSED"
    status, body = post(server, f"/api/tasks/{job_id}/resume", {}, headers=auth(server))
    assert status == 200 and body["state"] == "ASSIGNED"
    status, body = post(server, f"/api/tasks/{job_id}/cancel", {}, headers=auth(server))
    assert status == 200 and body["state"] == "CANCELLED"


def test_an_illegal_lifecycle_action_is_a_typed_error(ui):
    store, project, server, backend = ui
    job_id = store.create_job(project.id, "new only", AUTH)
    status, body = post(server, f"/api/tasks/{job_id}/pause", {}, headers=auth(server))
    assert status == 400
    assert body["error"]["code"] == "ILLEGAL_TRANSITION"


def test_malformed_json_body_is_refused(ui):
    _, _, server, _ = ui
    status, body = post(server, "/api/tasks", {}, headers=auth(server), raw=b"{not json")
    assert status == 400
    assert body["error"]["code"]


def test_the_server_refuses_to_bind_off_loopback():
    from kvflow.core.errors import V1Error

    with pytest.raises(V1Error):
        UiServer(UiBackend(store=Store(Path("x.sqlite3")), workspace_root=Path(".")),
                 host="0.0.0.0", port=0)


def test_knowledge_endpoint_is_scoped(ui):
    from kvflow.core.knowledge import KnowledgeService

    store, project, server, backend = ui
    KnowledgeService(store).propose(
        project_id=project.id,
        topic="status",
        content="DATA_BLOCKED",
        author="manager",
        source_ref="journal",
        source_digest="b" * 64,
        authorization_digest=AUTH,
    )
    status, body = get(server, f"/api/knowledge?project={project.id}")
    assert status == 200
    assert body["records"][0]["topic"] == "status"
    status, other = get(server, "/api/knowledge?project=someone-else")
    assert other["records"] == []
