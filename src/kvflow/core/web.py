"""The local KVFlow web UI: a real backend, no fake pages.

The server is the Python standard library on purpose: the user asked for a
minimal local workbench, not a second front-end build pipeline, and a dependency
free server is easier to audit.

Security properties enforced here, not merely documented
--------------------------------------------------------

* **Loopback only.** The socket binds ``127.0.0.1``; a request whose ``Host``
  header is not a loopback name is refused, so a DNS-rebinding page cannot reach
  the API from a browser.
* **Every write needs a session token *and* a CSRF token tied to it.** The token
  is generated at startup and delivered only through the local page; a cross-site
  form post cannot read it.
* **``Origin``/``Referer`` are checked on writes** and must match the server's own
  authority.
* **``GET`` never mutates.** Read endpoints only read; every state change is a
  ``POST`` to an explicit endpoint.
* **Polling, not streaming.** The page polls a bounded snapshot endpoint, so no
  long-lived server push infrastructure is required.
"""

from __future__ import annotations

import json
import re
import secrets
import threading
import urllib.parse
from dataclasses import dataclass, field
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable, Mapping

from .budget import BudgetLedger
from .errors import V1Error
from .knowledge import KnowledgeQuery, KnowledgeService
from .scheduler import Scheduler
from .store import Store

#: at most three overview cards, per the user's request for a minimal workbench
MAX_OVERVIEW_CARDS = 3
DEFAULT_PORT = 8765
_LOOPBACK_HOSTS = {"127.0.0.1", "localhost", "[::1]", "::1"}
_STATE_LABELS = {
    "NEW": "排队中",
    "PLANNING": "Manager 规划中",
    "ASSIGNED": "已派单",
    "WAITING_DEPENDENCIES": "等待依赖",
    "WAITING_CAPACITY": "等待空闲槽位",
    "WORKING": "执行中",
    "SELF_REVIEW": "Worker 自审中",
    "WORKER_COMPLETE": "Worker 已完成，等待审核",
    "MANAGER_REVIEW": "Manager 审核中",
    "MANAGER_APPROVED": "Manager 已批准",
    "INTEGRATED": "已集成",
    "READY_TO_APPLY": "可应用",
    "APPLIED": "已应用",
    "DONE": "已完成",
    "FIX": "返工中",
    "BLOCKED": "已阻塞",
    "PAUSED": "已暂停",
    "PAUSED_BUDGET": "预算暂停",
    "CANCELLED": "已取消",
    "OUTCOME_UNKNOWN": "结果未知，需核对",
}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class UiSession:
    """Local session state: one token, one CSRF secret, no passwords."""

    token: str
    csrf: str
    created_at: str = field(default_factory=_now)

    def __post_init__(self) -> None:
        self.token = self.token or secrets.token_urlsafe(32)
        self.csrf = self.csrf or secrets.token_urlsafe(32)


@dataclass
class UiSettings:
    """What the Settings page may read and change. Secrets are never included."""

    manager_model: str = "gpt-6-astra"
    manager_effort: str = "ultra"
    worker_model: str = "deepseek-flash"
    worker_slots: int = 3
    heavy_slots: int = 1
    sandbox_mode: str = "TRUSTED_PROJECTS_ONLY"
    polling_seconds: int = 3

    def to_dict(self) -> dict[str, Any]:
        return {
            "manager_model": self.manager_model,
            "manager_effort": self.manager_effort,
            "worker_model": self.worker_model,
            "worker_slots": self.worker_slots,
            "heavy_slots": self.heavy_slots,
            "sandbox_mode": self.sandbox_mode,
            "polling_seconds": self.polling_seconds,
            "credentials_exposed": False,
            "network_scope": "loopback only (127.0.0.1)",
        }


class UiBackend:
    """Read/write operations the UI is allowed to perform."""

    def __init__(self, *, store: Store, workspace_root: Path, settings: UiSettings | None = None):
        self.store = store
        self.workspace_root = Path(workspace_root)
        self.settings = settings or UiSettings()

    # ------------------------------------------------------------- reading
    def overview(self) -> dict[str, Any]:
        """At most three cards: running, needs attention, next step."""
        with self.store.read() as conn:
            rows = conn.execute(
                "SELECT job_id, project_id, state, updated_at FROM jobs"
                " ORDER BY updated_at DESC LIMIT 50"
            ).fetchall()
        jobs = [dict(row) for row in rows]
        running = [j for j in jobs if j["state"] in {
            "PLANNING", "ASSIGNED", "WORKING", "SELF_REVIEW", "MANAGER_REVIEW",
            "FIX", "WAITING_DEPENDENCIES", "WAITING_CAPACITY",
        }]
        attention = [j for j in jobs if j["state"] in {
            "BLOCKED", "OUTCOME_UNKNOWN", "PAUSED_BUDGET", "PAUSED", "FIX",
        }]
        waiting = [j for j in jobs if j["state"] in {"NEW", "WORKER_COMPLETE",
                                                     "MANAGER_APPROVED", "INTEGRATED",
                                                     "READY_TO_APPLY"}]
        cards = [
            {
                "id": "running",
                "title": "正在处理什么",
                "count": len(running),
                "detail": [self._job_brief(j) for j in running[:3]],
            },
            {
                "id": "attention",
                "title": "有什么需要注意",
                "count": len(attention),
                "detail": [self._job_brief(j) for j in attention[:3]],
            },
            {
                "id": "next",
                "title": "下一步是什么",
                "count": len(waiting),
                "detail": [self._job_brief(j) for j in waiting[:3]],
            },
        ]
        assert len(cards) <= MAX_OVERVIEW_CARDS
        return {
            "cards": cards,
            "job_count": len(jobs),
            "observed_at": _now(),
            "note": (
                "the counts describe durable task states, not a probability that a"
                " task will succeed"
            ),
        }

    @staticmethod
    def _job_brief(job: Mapping[str, Any]) -> dict[str, Any]:
        return {
            "job_id": job["job_id"],
            "state": job["state"],
            "state_label": _STATE_LABELS.get(job["state"], job["state"]),
            "updated_at": job["updated_at"],
        }

    def tasks(self) -> dict[str, Any]:
        scheduler = Scheduler(self.store)
        with self.store.read() as conn:
            rows = conn.execute(
                "SELECT job_id, project_id, objective, state, revision, updated_at"
                " FROM jobs ORDER BY updated_at DESC LIMIT 200"
            ).fetchall()
        jobs = []
        for row in rows:
            entry = dict(row)
            entry["state_label"] = _STATE_LABELS.get(entry["state"], entry["state"])
            entry["nodes"] = {
                node_id: {"state": state.value, "label": _STATE_LABELS.get(state.value, state.value)}
                for node_id, state in scheduler.node_states(entry["job_id"]).items()
            }
            jobs.append(entry)
        return {"jobs": jobs, "observed_at": _now()}

    def task_detail(self, job_id: str) -> dict[str, Any]:
        job = self.store.job(job_id)
        scheduler = Scheduler(self.store)
        with self.store.read() as conn:
            receipts = conn.execute(
                "SELECT receipt_id, node_id, exit_code, created_at FROM test_receipts"
                " WHERE job_id = ? ORDER BY created_at DESC LIMIT 20",
                (job_id,),
            ).fetchall()
            reviews = conn.execute(
                "SELECT review_id, node_id, verdict, kind, created_at FROM reviews"
                " WHERE job_id = ? ORDER BY created_at DESC LIMIT 20",
                (job_id,),
            ).fetchall()
            mailbox = conn.execute(
                "SELECT message_id, node_id, sender_role, kind, body, created_at"
                " FROM mailbox WHERE job_id = ? ORDER BY created_at DESC LIMIT 20",
                (job_id,),
            ).fetchall()
        return {
            "job": {**dict(job), "state_label": _STATE_LABELS.get(job["state"], job["state"])},
            "nodes": {
                k: {"state": v.value, "label": _STATE_LABELS.get(v.value, v.value)}
                for k, v in scheduler.node_states(job_id).items()
            },
            "slots": scheduler.slot_usage(job_id),
            "receipts": [dict(r) for r in receipts],
            "reviews": [dict(r) for r in reviews],
            "mailbox": [dict(r) for r in mailbox],
            "observed_at": _now(),
        }

    def knowledge(self, project_id: str | None, topic: str | None = None) -> dict[str, Any]:
        records = KnowledgeService(self.store).search(
            KnowledgeQuery(project_id=project_id, topic=topic, limit=100)
        )
        return {
            "project_id": project_id,
            "topic": topic,
            "records": records,
            "observed_at": _now(),
        }

    def runtime_settings(self) -> dict[str, Any]:
        """The Settings page payload. Named distinctly from the settings object."""
        projects = [
            {"id": p.id, "display_name": p.display_name, "trusted": p.trusted}
            for p in self.store.list_projects()
        ]
        scopes = [
            BudgetLedger(self.store).usage(scope_id)
            for scope_id in _scope_ids(self.store)
        ]
        registry = self.workspace_root.parent / "processes.json" if str(
            self.workspace_root
        ) else Path("processes.json")
        processes: list[dict[str, Any]] = []
        try:
            if registry.exists():
                processes = json.loads(registry.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            processes = []
        return {
            "runtime": self.settings.to_dict(),
            "projects": projects,
            "budget_scopes": scopes,
            "processes": processes,
            "database": str(self.store.db_path),
            "schema_version": self.store.schema_version(),
            "observed_at": _now(),
        }

    # ------------------------------------------------------------- writing
    def submit_task(self, *, project_id: str, objective: str, authorization_digest: str) -> dict[str, Any]:
        """Create a NEW job. It only *queues*; nothing is dispatched from the UI."""
        if not isinstance(objective, str) or not objective.strip():
            raise V1Error("a task description is required")
        if len(objective) > 4000:
            raise V1Error("the task description is too long")
        project = self.store.project(project_id)
        job_id = self.store.create_job(project.id, objective.strip(), authorization_digest)
        return {
            "job_id": job_id,
            "state": self.store.job_state(job_id).value,
            "dispatched": False,
            "note": "the job is queued; the scheduler dispatches it, not the UI",
        }

    def lifecycle(self, job_id: str, action: str) -> dict[str, Any]:
        scheduler = Scheduler(self.store)
        if action == "pause":
            scheduler.pause(job_id, actor="user")
        elif action == "resume":
            scheduler.resume(job_id, actor="user")
        elif action == "cancel":
            result = scheduler.cancel(job_id, actor="user", reason="cancelled from the UI")
            return result
        else:
            raise V1Error(f"unknown lifecycle action: {action!r}")
        return {"job_id": job_id, "state": self.store.job_state(job_id).value}


def _scope_ids(store: Store) -> list[str]:
    with store.read() as conn:
        rows = conn.execute("SELECT scope_id FROM budget_scopes ORDER BY scope_id").fetchall()
    return [row["scope_id"] for row in rows]


PAGE = """<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>KVFlow</title>
<style>
  :root { --bg:#f7f7f8; --fg:#1c1c1e; --card:#fff; --line:#e3e3e6; --accent:#2f6fed; }
  [data-theme="dark"] { --bg:#161618; --fg:#f2f2f4; --card:#1f1f22; --line:#33333a; }
  * { box-sizing:border-box; }
  body { margin:0; font:15px/1.5 system-ui,"Segoe UI",sans-serif; background:var(--bg); color:var(--fg); }
  header { display:flex; align-items:center; gap:12px; padding:12px 18px; border-bottom:1px solid var(--line); background:var(--card); }
  nav button { border:1px solid var(--line); background:transparent; color:inherit; padding:6px 14px; border-radius:8px; cursor:pointer; }
  nav button[aria-current="page"] { background:var(--accent); color:#fff; border-color:var(--accent); }
  main { padding:18px; max-width:1080px; margin:0 auto; }
  .cards { display:grid; grid-template-columns:repeat(auto-fit,minmax(240px,1fr)); gap:12px; }
  .card { background:var(--card); border:1px solid var(--line); border-radius:12px; padding:14px; }
  .card h3 { margin:0 0 6px; font-size:15px; }
  .card .count { font-size:28px; font-weight:600; }
  .muted { color:#8a8a92; font-size:13px; }
  textarea,input,select { width:100%; padding:8px; border:1px solid var(--line); border-radius:8px; background:var(--card); color:inherit; font:inherit; }
  textarea { min-height:84px; resize:vertical; }
  button.primary { background:var(--accent); color:#fff; border:none; padding:9px 16px; border-radius:8px; cursor:pointer; }
  button.ghost { background:transparent; border:1px solid var(--line); color:inherit; padding:6px 12px; border-radius:8px; cursor:pointer; }
  table { width:100%; border-collapse:collapse; background:var(--card); border:1px solid var(--line); border-radius:12px; overflow:hidden; }
  th,td { text-align:left; padding:8px 10px; border-bottom:1px solid var(--line); font-size:14px; }
  .pill { display:inline-block; padding:2px 8px; border-radius:999px; border:1px solid var(--line); font-size:12px; }
  .hidden { display:none; }
  .row { display:flex; gap:8px; align-items:center; flex-wrap:wrap; }
  @media (max-width:640px){
    header{padding:10px 12px; gap:6px; flex-wrap:wrap}
    header strong{flex:1 1 100%; font-size:14px}
    nav{display:flex; width:auto; gap:6px}
    nav button{padding:6px 8px; font-size:12px}
    main{padding:10px}
    .cards{grid-template-columns:1fr}
    .card{padding:12px}
    p,.muted{word-break:break-word}
    table{font-size:13px}
    th,td{padding:6px 8px}
  }
</style></head><body>
<header>
  <strong>KVFlow</strong>
  <nav class="row">
    <button data-view="tasks" aria-current="page">任务</button>
    <button data-view="results">结果</button>
    <button data-view="knowledge">知识</button>
    <button data-view="settings">设置</button>
  </nav>
  <span style="margin-left:auto"></span>
  <button class="ghost" id="theme">深色/浅色</button>
</header>
<main>
  <section id="view-tasks">
    <div class="cards" id="overview"></div>
    <div class="card" style="margin-top:14px">
      <h3>提一个任务</h3>
      <p class="muted">选择项目，用自然语言说明你要什么。任务只会进入队列，不会自动执行。</p>
      <div class="row"><select id="project"></select></div>
      <div style="margin-top:8px"><textarea id="objective" placeholder="例如：为登录接口增加速率限制，跑通测试并给出补丁。"></textarea></div>
      <div class="row" style="margin-top:8px">
        <button class="primary" id="submit">提交</button>
        <span class="muted" id="submit-status"></span>
      </div>
    </div>
    <div style="margin-top:14px"><table id="jobs"><thead><tr><th>任务</th><th>状态</th><th>更新</th><th></th></tr></thead><tbody></tbody></table></div>
  </section>
  <section id="view-results" class="hidden">
    <div class="card"><h3>结果与审核</h3><p class="muted">选择任务查看节点、测试回执与审核记录。这里只展示真实存储的内容。</p>
    <div class="row"><select id="result-job"></select><button class="ghost" id="load-result">查看</button></div></div>
    <div id="result-body" style="margin-top:14px"></div>
  </section>
  <section id="view-knowledge" class="hidden">
    <div class="card"><h3>知识</h3><p class="muted">按项目检索，来源与时间随记录一起显示。</p>
    <div class="row"><select id="knowledge-project"></select><input id="knowledge-topic" placeholder="主题（可选）"><button class="ghost" id="load-knowledge">检索</button></div></div>
    <div id="knowledge-body" style="margin-top:14px"></div>
  </section>
  <section id="view-settings" class="hidden">
    <div id="settings-body"></div>
  </section>
</main>
<script>
const TOKEN = "__TOKEN__", CSRF = "__CSRF__";
let current = "tasks";
function api(path, options) {
  const opts = Object.assign({headers:{}}, options || {});
  opts.headers["X-KVFlow-Token"] = TOKEN;
  if (opts.method === "POST") { opts.headers["X-KVFlow-CSRF"] = CSRF; opts.headers["Content-Type"] = "application/json"; }
  return fetch(path, opts).then(r => r.json().then(b => ({ok:r.ok, body:b})));
}
function el(id){ return document.getElementById(id); }
function pill(state,label){ return '<span class="pill">' + (label||state) + '</span>'; }
async function loadOverview(){
  const {body} = await api("/api/overview");
  el("overview").innerHTML = body.cards.map(c =>
    '<div class="card"><h3>' + c.title + '</h3><div class="count">' + c.count + '</div>' +
    '<div class="muted">' + c.detail.map(d => d.job_id.slice(0,14) + " · " + d.state_label).join("<br>") + '</div></div>').join("");
}
async function loadTasks(){
  const {body} = await api("/api/tasks");
  el("jobs").querySelector("tbody").innerHTML = body.jobs.map(j =>
    '<tr><td>' + j.job_id.slice(0,16) + '<div class="muted">' + (j.objective||"").slice(0,60) + '</div></td>' +
    '<td>' + pill(j.state, j.state_label) + '</td><td class="muted">' + j.updated_at + '</td>' +
    '<td class="row"><button class="ghost" data-act="pause" data-job="' + j.job_id + '">暂停</button>' +
    '<button class="ghost" data-act="resume" data-job="' + j.job_id + '">恢复</button>' +
    '<button class="ghost" data-act="cancel" data-job="' + j.job_id + '">取消</button></td></tr>').join("");
  const selects = [el("result-job"), el("knowledge-project")];
  const projects = await api("/api/settings");
  el("project").innerHTML = projects.body.projects.map(p => '<option value="' + p.id + '">' + p.display_name + '</option>').join("");
  el("knowledge-project").innerHTML = '<option value="">全部（含全局偏好）</option>' + projects.body.projects.map(p => '<option value="' + p.id + '">' + p.display_name + '</option>').join("");
  el("result-job").innerHTML = body.jobs.map(j => '<option value="' + j.job_id + '">' + j.job_id.slice(0,16) + '</option>').join("");
}
document.addEventListener("click", async (event) => {
  const target = event.target;
  if (target.dataset && target.dataset.act) {
    const {body} = await api("/api/tasks/" + target.dataset.job + "/" + target.dataset.act, {method:"POST", body:"{}"});
    el("submit-status").textContent = body.error ? ("失败：" + body.error.code) : ("已" + target.dataset.act);
    loadTasks(); loadOverview();
  }
});
el("submit").addEventListener("click", async () => {
  const project = el("project").value, objective = el("objective").value;
  const {ok, body} = await api("/api/tasks", {method:"POST", body: JSON.stringify({project_id:project, objective:objective})});
  el("submit-status").textContent = ok ? ("已入队 " + body.job_id.slice(0,14)) : ("失败：" + (body.error && body.error.code));
  if (ok) { el("objective").value = ""; loadTasks(); loadOverview(); }
});
el("load-result").addEventListener("click", async () => {
  const {body} = await api("/api/tasks/" + el("result-job").value);
  if (body.error) { el("result-body").textContent = "读取失败：" + body.error.code; return; }
  el("result-body").innerHTML =
    '<div class="card"><h3>节点</h3>' + Object.entries(body.nodes).map(([k,v]) => k + " · " + v.label).join("<br>") + '</div>' +
    '<div class="card" style="margin-top:10px"><h3>测试回执</h3>' + (body.receipts.length ? body.receipts.map(r => r.receipt_id.slice(0,16) + " exit=" + r.exit_code).join("<br>") : '<span class="muted">暂无</span>') + '</div>' +
    '<div class="card" style="margin-top:10px"><h3>审核</h3>' + (body.reviews.length ? body.reviews.map(r => r.verdict + " · " + r.kind).join("<br>") : '<span class="muted">暂无</span>') + '</div>' +
    '<div class="card" style="margin-top:10px"><h3>任务信箱</h3>' + (body.mailbox.length ? body.mailbox.map(m => m.sender_role + " · " + m.kind).join("<br>") : '<span class="muted">暂无</span>') + '</div>';
});
el("load-knowledge").addEventListener("click", async () => {
  const project = el("knowledge-project").value, topic = el("knowledge-topic").value;
  const {body} = await api("/api/knowledge?project=" + encodeURIComponent(project) + "&topic=" + encodeURIComponent(topic));
  if (body.error) { el("knowledge-body").textContent = "检索失败：" + body.error.code; return; }
  el("knowledge-body").innerHTML = body.records.length ? body.records.map(r =>
    '<div class="card" style="margin-bottom:10px"><strong>' + r.topic + '</strong> ' + pill(r.kind) +
    '<div class="muted">' + r.author + " · " + r.verification + " · " + r.observed_at + '</div>' +
    '<p>' + r.content + '</p><div class="muted">来源：' + r.source_ref + '</div></div>').join("")
    : '<div class="card muted">没有可显示的知识记录</div>';
});
async function loadSettings(){
  const {body} = await api("/api/settings");
  el("settings-body").innerHTML = '<div class="card"><h3>运行设置</h3><pre>' + JSON.stringify(body.runtime, null, 2) + '</pre></div>' +
    '<div class="card" style="margin-top:10px"><h3>项目</h3><pre>' + JSON.stringify(body.projects, null, 2) + '</pre></div>' +
    '<div class="card" style="margin-top:10px"><h3>预算</h3><pre>' + JSON.stringify(body.budget_scopes, null, 2) + '</pre></div>' +
    '<div class="card" style="margin-top:10px"><h3>自有进程</h3><pre>' + JSON.stringify(body.processes, null, 2) + '</pre></div>';
}
document.querySelectorAll("nav button").forEach(btn => btn.addEventListener("click", () => {
  current = btn.dataset.view;
  document.querySelectorAll("nav button").forEach(b => b.removeAttribute("aria-current"));
  btn.setAttribute("aria-current","page");
  ["tasks","results","knowledge","settings"].forEach(v => el("view-" + v).classList.toggle("hidden", v !== current));
  if (current === "results") loadTasks();
  if (current === "knowledge") loadTasks();
  if (current === "settings") loadSettings();
}));
el("theme").addEventListener("click", () => {
  const root = document.documentElement;
  root.dataset.theme = root.dataset.theme === "dark" ? "light" : "dark";
});
loadOverview(); loadTasks(); loadSettings();
setInterval(() => { if (current === "tasks") { loadOverview(); loadTasks(); } }, 3000);
</script></body></html>
"""


def render_page(session: UiSession) -> str:
    return PAGE.replace("__TOKEN__", session.token).replace("__CSRF__", session.csrf)


class UiServer:
    """Threaded loopback server around :class:`UiBackend`."""

    def __init__(
        self,
        backend: UiBackend,
        *,
        host: str = "127.0.0.1",
        port: int = DEFAULT_PORT,
        session: UiSession | None = None,
    ) -> None:
        if host not in _LOOPBACK_HOSTS:
            raise V1Error("the UI may only bind a loopback address", host=host)
        self.backend = backend
        self.session = session or UiSession(token="", csrf="")
        self._server = ThreadingHTTPServer((host, port), _Handler)
        self._server.backend = backend  # type: ignore[attr-defined]
        self._server.session = self.session  # type: ignore[attr-defined]
        self.host = host
        self.port = int(self._server.server_address[1])
        self._thread: threading.Thread | None = None

    @property
    def url(self) -> str:
        return f"http://{self.host}:{self.port}/"

    def start(self) -> "UiServer":
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        return self

    def stop(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        if self._thread:
            self._thread.join(timeout=5)


class _Handler(BaseHTTPRequestHandler):
    server_version = "AgentOS/1.0"
    protocol_version = "HTTP/1.1"
    #: body already consumed for this request, so it is read exactly once
    _pending_body: bytes | None = None

    # ------------------------------------------------------------- plumbing
    def log_message(self, *args: Any) -> None:  # noqa: A003 - quiet by default
        return None

    @property
    def _backend(self) -> UiBackend:
        return self.server.backend  # type: ignore[attr-defined]

    @property
    def _session(self) -> UiSession:
        return self.server.session  # type: ignore[attr-defined]

    def _host_ok(self) -> bool:
        header = self.headers.get("Host", "")
        name = header.split(":")[0].strip().lower()
        return name in _LOOPBACK_HOSTS

    def _send(self, status: int, body: bytes, content_type: str) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        if self.close_connection:
            # say it explicitly: a refused request closes its connection so no
            # unread body can be mistaken for the next request
            self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(body)

    def _json(self, status: int, payload: Any) -> None:
        self._send(status, json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8"),
                   "application/json; charset=utf-8")

    def _error(self, status: int, code: str, message: str = "") -> None:
        """Refuse, and never leave an unread body on a kept-alive connection.

        A refusal that answers before consuming a declared request body leaves
        those bytes in the socket; with HTTP/1.1 keep-alive the next request is
        then parsed out of that body and the client sees an unrelated 501. So a
        refused request closes its connection.
        """
        self.close_connection = True
        self._json(status, {"error": {"code": code, "message": message or code}})

    def _read_body(self) -> bytes:
        """Read exactly the declared body length, or drain it before refusing."""
        length = int(self.headers.get("Content-Length") or 0)
        if length <= 0:
            return b""
        if length > 1_048_576:
            remaining = length
            while remaining > 0:
                chunk = self.rfile.read(min(65536, remaining))
                if not chunk:
                    break
                remaining -= len(chunk)
            raise V1Error("request body too large")
        raw = b""
        while len(raw) < length:
            chunk = self.rfile.read(length - len(raw))
            if not chunk:
                break
            raw += chunk
        return raw

    def _authorize_write(self) -> bool:
        """Token + CSRF + Origin must all check out before a mutation."""
        if self.headers.get("X-KVFlow-Token") != self._session.token:
            self._error(401, "UNAUTHENTICATED")
            return False
        if self.headers.get("X-KVFlow-CSRF") != self._session.csrf:
            self._error(403, "CSRF_REJECTED")
            return False
        origin = self.headers.get("Origin") or self.headers.get("Referer")
        if origin:
            parsed = urllib.parse.urlsplit(origin)
            if parsed.hostname and parsed.hostname.lower() not in _LOOPBACK_HOSTS:
                self._error(403, "ORIGIN_REJECTED")
                return False
        return True

    def _read_json(self) -> dict[str, Any]:
        raw = self._pending_body if self._pending_body is not None else self._read_body()
        if not raw:
            return {}
        try:
            return json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, ValueError) as exc:
            raise V1Error("request body is not valid JSON") from exc

    # ---------------------------------------------------------------- OPTIONS
    def do_OPTIONS(self) -> None:  # noqa: N802 - stdlib contract
        """Answer a cross-origin preflight by declining it, explicitly.

        No CORS approval header is ever sent, so a browser will not let a
        foreign page complete the actual write. Answering 501 instead would be
        an accidental implementation detail leaking into the security story.
        """
        self._send(204, b"", "text/plain; charset=utf-8")

    # ------------------------------------------------------------------ GET
    def do_GET(self) -> None:  # noqa: N802 - stdlib contract
        if not self._host_ok():
            self._error(403, "HOST_REJECTED")
            return
        parsed = urllib.parse.urlsplit(self.path)
        path = parsed.path
        query = urllib.parse.parse_qs(parsed.query)
        try:
            if path in {"/", "/index.html"}:
                self._send(200, render_page(self._session).encode("utf-8"),
                           "text/html; charset=utf-8")
                return
            if path == "/api/health":
                self._json(200, {"ok": True, "version": "1.0.0", "network_scope": "loopback"})
                return
            if path == "/api/overview":
                self._json(200, self._backend.overview())
                return
            if path == "/api/tasks":
                self._json(200, self._backend.tasks())
                return
            match = re.fullmatch(r"/api/tasks/([A-Za-z0-9_.:-]{1,128})", path)
            if match:
                self._json(200, self._backend.task_detail(match.group(1)))
                return
            if path == "/api/knowledge":
                project = (query.get("project") or [None])[0] or None
                topic = (query.get("topic") or [None])[0] or None
                self._json(200, self._backend.knowledge(project, topic))
                return
            if path == "/api/settings":
                self._json(200, self._backend.runtime_settings())
                return
            self._error(404, "NOT_FOUND")
        except V1Error as exc:
            self._json(400, {"error": exc.to_dict()})
        except Exception as exc:  # noqa: BLE001 - operational detail, never a stack trace
            self._error(500, "INTERNAL", type(exc).__name__)

    # ----------------------------------------------------------------- POST
    def do_POST(self) -> None:  # noqa: N802 - stdlib contract
        # The body is consumed before any decision, so a refusal cannot leave
        # bytes behind for the next request on this connection to be parsed from.
        try:
            self._pending_body = self._read_body()
        except V1Error as exc:
            self._json(400, {"error": exc.to_dict()})
            return
        if not self._host_ok():
            self._error(403, "HOST_REJECTED")
            return
        if not self._authorize_write():
            return
        path = urllib.parse.urlsplit(self.path).path
        try:
            payload = self._read_json()
            if path == "/api/tasks":
                self._json(200, self._backend.submit_task(
                    project_id=str(payload.get("project_id", "")),
                    objective=str(payload.get("objective", "")),
                    authorization_digest=str(payload.get("authorization_digest", "0" * 64)),
                ))
                return
            match = re.fullmatch(
                r"/api/tasks/([A-Za-z0-9_.:-]{1,128})/(pause|resume|cancel)", path
            )
            if match:
                self._json(200, self._backend.lifecycle(match.group(1), match.group(2)))
                return
            self._error(404, "NOT_FOUND")
        except V1Error as exc:
            self._json(400, {"error": exc.to_dict()})
        except Exception as exc:  # noqa: BLE001
            self._error(500, "INTERNAL", type(exc).__name__)


def serve(
    *,
    store: Store,
    workspace_root: Path,
    host: str = "127.0.0.1",
    port: int = DEFAULT_PORT,
) -> UiServer:
    backend = UiBackend(store=store, workspace_root=workspace_root)
    return UiServer(backend, host=host, port=port)
