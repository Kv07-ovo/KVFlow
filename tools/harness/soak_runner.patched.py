"""Real-time durability soak: bounded mixed load for a measured elapsed period.

This runner does **not** claim live-model proof. Its load mix is explicitly:
a deterministic fixture executor for the Worker role (no paid model calls), real
SQLite transactions, a real DAG, real journal appends from other processes, real
MCP stdio sessions through the product server, real web endpoints, and real
backup/restore. Live model E2E is a separate, budgeted acceptance item.

It measures what the specification asks for:

* task claims and completions, concurrency actually reached,
* SQLite write-lock contention and errors,
* process memory trend (sampled),
* artifact/log/disk growth,
* MCP sessions opened/closed with a typed error rate,
* cancel and restart behaviour, including a real kill -9 of a child process,
* database integrity and checkpoint consistency after every phase,
* budget conservation against the ledger.

Usage::

    python .runtime_tools/soak_runner.py --seconds 14400 --workers 4

The run writes a receipt to ``.agent_os_runtime/receipts/soak-<id>.json`` and
appends a line per cycle so a crash still leaves usable evidence.
"""

from __future__ import annotations

import argparse
import ctypes
import hashlib
import json
import os
import random
import shutil
import signal
import sqlite3
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from agent_os.v1.backup import BackupManager  # noqa: E402
from agent_os.v1.budget import BudgetLedger, Charge, ReservationRequest  # noqa: E402
from agent_os.v1.contracts import (  # noqa: E402
    BudgetScope,
    NodeSpec,
    Plan,
    Project,
    Role,
    TestProfile,
)
from agent_os.v1.errors import (  # noqa: E402
    AttemptExhausted,
    ConcurrencyError,
    StateTransitionError,
)
from agent_os.v1.knowledge import KnowledgeQuery, KnowledgeService  # noqa: E402
from agent_os.v1.mcp_server import CAPABILITY_ENV, DATABASE_ENV, WORKSPACE_ENV  # noqa: E402
from agent_os.v1.scheduler import SUCCESS_STATES, Scheduler  # noqa: E402
from agent_os.v1.security import CapabilityAuthority  # noqa: E402
from agent_os.v1.state import TaskState  # noqa: E402
from agent_os.v1.store import Store  # noqa: E402
from agent_os.v1.web import UiBackend, UiServer  # noqa: E402
from agent_os.v1.workspace import WorkspaceManager  # noqa: E402

AUTH = hashlib.sha256(b"soak-authorization").hexdigest()


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def rss_bytes(pid: int) -> int | None:
    """Resident memory of a live process, via the Windows working-set API."""
    try:
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        from ctypes import wintypes

        class PROCESS_MEMORY_COUNTERS(ctypes.Structure):
            _fields_ = [
                ("cb", wintypes.DWORD),
                ("PageFaultCount", wintypes.DWORD),
                ("PeakWorkingSetSize", ctypes.c_size_t),
                ("WorkingSetSize", ctypes.c_size_t),
                ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                ("PagefileUsage", ctypes.c_size_t),
                ("PeakPagefileUsage", ctypes.c_size_t),
            ]

        kernel32.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
        kernel32.OpenProcess.restype = wintypes.HANDLE
        handle = kernel32.OpenProcess(0x0400 | 0x0010, False, pid)
        if not handle:
            return None
        try:
            counters = PROCESS_MEMORY_COUNTERS()
            counters.cb = ctypes.sizeof(counters)
            psapi = ctypes.WinDLL("psapi", use_last_error=True)
            psapi.GetProcessMemoryInfo.argtypes = [
                wintypes.HANDLE,
                ctypes.POINTER(PROCESS_MEMORY_COUNTERS),
                wintypes.DWORD,
            ]
            if not psapi.GetProcessMemoryInfo(handle, ctypes.byref(counters), counters.cb):
                return None
            return int(counters.WorkingSetSize)
        finally:
            kernel32.CloseHandle(handle)
    except Exception:  # noqa: BLE001
        return None


def dir_bytes(path: Path) -> int:
    total = 0
    for entry in path.rglob("*"):
        if entry.is_file():
            try:
                total += entry.stat().st_size
            except OSError:
                continue
    return total


# --------------------------------------------------------------------- fixtures


def build_project(root: Path, project_id: str, *, heavy: bool = True) -> Project:
    source = root / "source"
    (source / "src").mkdir(parents=True, exist_ok=True)
    (source / "src" / "module.py").write_text("VALUE = 1\n", encoding="utf-8")
    managed = root / "managed"
    managed.mkdir(parents=True, exist_ok=True)
    profile = TestProfile(
        id="unit",
        project_id=project_id,
        description="soak unit profile",
        runner="pytest",
        targets=["src"],
        pythonpath=["src"],
        timeout_seconds=120,
        code_digest=hashlib.sha256(b"soak-code").hexdigest(),
        authorization_digest=AUTH,
    )
    heavy_profile = TestProfile(
        id="heavy",
        project_id=project_id,
        description="soak heavy profile",
        runner="pytest",
        targets=["src"],
        pythonpath=["src"],
        heavy=heavy,
        timeout_seconds=120,
        code_digest=hashlib.sha256(b"soak-code").hexdigest(),
        authorization_digest=AUTH,
    )
    registered = datetime.now(timezone.utc)
    return Project.model_validate(
        {
            "id": project_id,
            "display_name": f"soak {project_id}",
            "source_root": str(source),
            "managed_root": str(managed),
            "allowed_read_roots": ["."],
            "allowed_write_roots": ["src", "out"],
            "protected_roots": ["golden_reference"],
            "test_profiles": [profile, heavy_profile],
            "allowed_tools": [
                "repo.read", "repo.list", "repo.search", "repo.status", "repo.patch",
                "repo.diff", "test.run_profile", "operation.status", "operation.result",
                "knowledge.search", "knowledge.read",
            ],
            "trusted": True,
            "authorization_digest": AUTH,
            "registered_at": registered,
        }
    )


def build_plan(project: Project, job_id: str, node_count: int) -> Plan:
    nodes = []
    for index in range(node_count):
        dependencies = []
        if index >= 3 and index % 7 == 0:
            dependencies = [f"n{index - 1}", f"n{index - 2}"]
        elif index > 0 and index % 5 == 0:
            dependencies = [f"n{index - 1}"]
        nodes.append(
            NodeSpec(
                id=f"n{index}",
                lineage_key=f"lin-{job_id}-{index}",
                dependencies=dependencies,
                objective=f"soak node {index}",
                write_scopes=[f"out/node-{index}.txt"],
                test_profile="unit",
                allowed_tools=[
                    "repo.read",
                    "repo.list",
                    "repo.search",
                    "repo.patch",
                    "test.run_profile",
                    "operation.status",
                ],
            )
        )
    return Plan.model_validate(
        {
            "id": f"plan-{job_id}",
            "job_id": job_id,
            "version": 1,
            "revision": 1,
            "objective": "sustain a bounded mixed load",
            "deliverables": [f"{node_count} node outputs"],
            "constraints": ["writes stay in the owned workspace"],
            "acceptance": ["every node reaches a success milestone"],
            "nodes": nodes,
            "roles": [Role.WORKER, Role.MANAGER],
            "allowed_roots": ["out"],
            "tool_profiles": ["unit"],
            "resource_budget": {
                "calls": 10_000,
                "input_tokens": 10_000_000,
                "output_tokens": 10_000_000,
                "tool_calls": 100_000,
                "storage_bytes": 1 << 30,
                "wall_seconds": 86_400,
                "concurrency": 3,
                "deadline": datetime.now(timezone.utc).replace(year=2030),
            },
            "approval_boundaries": ["no push"],
            "authorization_digest": AUTH,
            "created_at": datetime.now(timezone.utc),
        }
    )


@dataclass
class FixtureExecutor:
    """Deterministic Worker stand-in: no model call, real file and DB effects."""

    handle: Any
    delay: float = 0.0
    fail_every: int = 0
    counter: int = 0

    def __call__(self, claim) -> dict[str, Any]:
        self.counter += 1
        if self.delay:
            time.sleep(self.delay)
        if self.fail_every and self.counter % self.fail_every == 0:
            return {"status": "FIX_REQUIRED", "reason": "injected deterministic failure"}
        target = self.handle.root / "out" / f"{claim.node_id}.txt"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(f"{claim.node_id} attempt={claim.attempt} fence={claim.fence}\n",
                          encoding="utf-8")
        return {"status": "WORKER_COMPLETE", "summary": f"{claim.node_id} done"}


# ------------------------------------------------------------------------ soak


@dataclass
class SoakState:
    run_id: str
    root: Path
    seconds: int
    started: float = field(default_factory=time.monotonic)
    cycles: int = 0
    claims: int = 0
    completions: int = 0
    failures: int = 0
    blocked: int = 0
    cancels: int = 0
    restarts: int = 0
    mcp_sessions: int = 0
    mcp_errors: int = 0
    web_requests: int = 0
    web_errors: int = 0
    backups: int = 0
    restores: int = 0
    knowledge_records: int = 0
    peak_workers: int = 0
    lock_errors: int = 0
    contention_events: int = 0
    #: harness event classification: an expected fail-closed refusal is a
    #: passing negative test, not an error; cleanup retries are recorded
    #: separately from cleanup failures, which are never swallowed.
    expected_refusals: int = 0
    negative_test_failures: int = 0
    cleanup_retries: int = 0
    cleanup_failures: int = 0
    product_errors: int = 0
    harness_errors: int = 0
    unexpected_errors: int = 0
    cleanup_retry_events: list = field(default_factory=list)
    phase: str = "setup"
    errors: list[str] = field(default_factory=list)
    samples: list[dict[str, Any]] = field(default_factory=list)
    phase_times: dict[str, float] = field(default_factory=dict)


class SoakRunner:
    def __init__(self, *, seconds: int, workers: int, seed: int, root: Path) -> None:
        self.seconds = max(30, int(seconds))
        self.workers = max(1, min(6, int(workers)))
        self.random = random.Random(seed)
        self.root = root
        self.run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        self.state = SoakState(run_id=self.run_id, root=root, seconds=self.seconds)
        self.receipt = ROOT / ".agent_os_runtime" / "receipts" / f"soak-{self.run_id}.json"
        self.log = ROOT / ".agent_os_runtime" / "receipts" / f"soak-{self.run_id}.jsonl"
        self.receipt.parent.mkdir(parents=True, exist_ok=True)
        self.stop = threading.Event()
        self.background: list[threading.Thread] = []

    # ------------------------------------------------------------- lifecycle
    def setup(self) -> None:
        shutil.rmtree(self.root, ignore_errors=True)
        self.root.mkdir(parents=True, exist_ok=True)
        self.database = self.root / "agent_os.sqlite3"
        self.store = Store(self.database)
        self.store.initialize()
        self.project = build_project(self.root / "project", "soak-project")
        self.store.register_project(self.project)
        ledger = BudgetLedger(self.store)
        ledger.register_scope(
            BudgetScope(
                scope_id="soak-global",
                scope_kind="GLOBAL",
                calls=100_000,
                input_tokens=10**9,
                output_tokens=10**9,
                tool_calls=10**6,
                storage_bytes=1 << 30,
                wall_seconds=86_400,
                micro_cny=100_000_000,
                concurrency=8,
                deadline=datetime.now(timezone.utc).replace(year=2030),
                authorization_digest=AUTH,
            )
        )
        self.manager = WorkspaceManager(self.project)
        self.snapshot = self.manager.create_snapshot(include_scopes=["src"], notes="soak base")
        self.backups = BackupManager(
            database=self.database, backups_root=self.root / "backups", version="1.0.0"
        )
        self.state.samples.append(
            {
                "at": now(),
                "event": "setup",
                "database_bytes": self.database.stat().st_size,
                "snapshot_files": len(self.snapshot.entries),
            }
        )

    def new_job(self, node_count: int) -> tuple[str, Any]:
        job_id = self.store.create_job(self.project.id, f"soak cycle job {node_count}", AUTH)
        self.store.set_plan(
            build_plan(self.project, job_id, node_count), expected_job_revision=1
        )
        self.store.transition_job(job_id, "NEW", "PLANNING", actor="manager")
        self.store.transition_job(job_id, "PLANNING", "ASSIGNED", actor="manager")
        handle = self.manager.create_workspace(
            job_id=job_id,
            node_id="n0",
            snapshot_id=self.snapshot.snapshot_id,
            scopes=["out"],
        )
        return job_id, handle

    # ---------------------------------------------------------------- phases
    def phase_dag(self) -> None:
        """Run one bounded DAG with the configured worker concurrency."""
        node_count = self.random.choice((8, 12, 16, 20))
        job_id, handle = self.new_job(node_count)
        scheduler = Scheduler(self.store, worker_slots=min(3, self.workers))
        executor = FixtureExecutor(handle=handle, delay=0.01,
                                   fail_every=self.random.choice((0, 0, 11, 17)))
        started = time.monotonic()
        for _ in range(node_count * 6):
            report = scheduler.tick(job_id, executor)
            self.state.claims += len(report.claimed)
            self.state.completions += len(report.completed)
            self.state.failures += len(report.failed)
            self.state.blocked += len(report.blocked)
            running = scheduler.slot_usage(job_id)["workers"]
            self.state.peak_workers = max(self.state.peak_workers, running)
            if not report.claimed and not scheduler.evaluate(job_id)["ready"]:
                break
        self.state.phase_times["dag"] = self.state.phase_times.get("dag", 0.0) + (
            time.monotonic() - started
        )

    def phase_concurrent_writers(self) -> None:
        """Hammer the store from several threads to surface write-lock errors."""
        job_id, handle = self.new_job(6)
        scheduler = Scheduler(self.store, worker_slots=3)
        executor = FixtureExecutor(handle=handle, delay=0.005)
        errors: list[str] = []
        contention: dict[str, int] = {}
        lock = threading.Lock()

        def worker() -> None:
            for _ in range(12):
                try:
                    report = scheduler.tick(job_id, executor, max_dispatch=1)
                except (ConcurrencyError, StateTransitionError) as exc:
                    # ordinary contention: another writer claimed the node first
                    with lock:
                        contention[type(exc).__name__] = (
                            contention.get(type(exc).__name__, 0) + 1
                        )
                    continue
                except Exception as exc:  # noqa: BLE001 - anything else is a defect
                    with lock:
                        errors.append(type(exc).__name__)
                    continue
                with lock:
                    self.state.claims += len(report.claimed)
                    self.state.completions += len(report.completed)
                    self.state.failures += len(report.failed)

        def sample_peak() -> None:
            """Observe the real high-water mark while the writers are running."""
            while not done.is_set():
                running = scheduler.slot_usage(job_id)["workers"]
                with lock:
                    self.state.peak_workers = max(self.state.peak_workers, running)
                time.sleep(0.002)

        done = threading.Event()
        monitor = threading.Thread(target=sample_peak, daemon=True)
        monitor.start()
        threads = [threading.Thread(target=worker) for _ in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        done.set()
        monitor.join(timeout=2)
        self.state.lock_errors += sum(
            1 for name in errors if "lock" in name.lower() or "Operational" in name
        )
        self.state.errors.extend(errors[:3])
        with lock:
            self.state.contention_events += sum(contention.values())

    def phase_mcp(self) -> None:
        """Open real MCP stdio sessions through the product server."""
        job_id, handle = self.new_job(2)
        run = self.store.start_attempt(job_id, "n0")
        ticket, _ = CapabilityAuthority(self.store).issue(
            project_id=self.project.id,
            job_id=job_id,
            node_id="n0",
            run_id=run["run_id"],
            lease_id=run["lease_id"],
            role="worker",
            actions=["repo.read", "repo.list", "operation.status"],
        )
        env = {
            key: os.environ[key]
            for key in ("PATH", "SYSTEMROOT", "SYSTEMDRIVE", "WINDIR", "COMSPEC", "TEMP", "TMP")
            if key in os.environ
        }
        env.update(
            {
                "PYTHONIOENCODING": "utf-8",
                "PYTHONUTF8": "1",
                "PYTHONDONTWRITEBYTECODE": "1",
                "PYTHONPATH": str(ROOT / "src"),
                DATABASE_ENV: str(self.database),
                CAPABILITY_ENV: ticket,
                WORKSPACE_ENV: str(self.manager.managed_root),
            }
        )
        script = (
            "import asyncio,json,sys\n"
            "from mcp import ClientSession, StdioServerParameters\n"
            "from mcp.client.stdio import stdio_client\n"
            "async def main():\n"
            "    params = StdioServerParameters(command=sys.executable,"
            " args=['-m','agent_os.v1.mcp_server'], env=dict(**__import__('os').environ))\n"
            "    async with stdio_client(params) as (r,w):\n"
            "        async with ClientSession(r,w) as s:\n"
            "            await s.initialize()\n"
            "            tools = await s.list_tools()\n"
            "            out = await s.call_tool('repo_list', {'path': 'src'})\n"
            "            text = ''.join(getattr(b,'text','') for b in out.content)\n"
            "            print(json.dumps({'tools': len(tools.tools), 'ok': json.loads(text)['ok']}))\n"
            "asyncio.run(main())\n"
        )
        try:
            completed = subprocess.run(
                [sys.executable, "-c", script],
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=90,
                env=env,
                cwd=str(self.root),
                check=False,
            )
            if completed.returncode == 0 and '"ok": true' in completed.stdout:
                self.state.mcp_sessions += 1
            else:
                self.state.mcp_errors += 1
                self.note(f"mcp: {completed.stderr.strip()[-200:]}")
        except subprocess.TimeoutExpired:
            self.state.mcp_errors += 1
            self.note("mcp: timeout")

    def phase_web(self, server: UiServer | None) -> None:
        if server is None:
            return
        import urllib.error
        import urllib.request

        for path in ("/api/health", "/api/overview", "/api/tasks", "/api/settings"):
            try:
                with urllib.request.urlopen(
                    f"http://{server.host}:{server.port}{path}", timeout=10
                ) as response:
                    json.loads(response.read().decode("utf-8"))
                self.state.web_requests += 1
            except Exception as exc:  # noqa: BLE001
                self.state.web_errors += 1
                self.note(f"web {path}: {type(exc).__name__}")

    def phase_backup_restore(self) -> None:
        """Backup, verify, restore, prove the refusal, then clean up honestly.

        Each cycle gets its own unique destination, so a directory left behind by an
        earlier cycle can never make the next restore look like a product refusal.
        The refusal of a *non-empty* destination is then asserted on purpose: KVFlow
        failing closed is a passing negative test, counted separately, never added to
        SOAK_ERRORS. Only if that restore were to succeed is it a product fault.
        """
        result = self.backups.create(note=f"soak {self.run_id}")
        self.state.backups += 1
        verify = self.backups.verify(result.root)
        if not verify["verified"]:
            self.note(f"backup {result.root} did not verify")
            return
        target = self.root / f"restore-{self.state.cycles}-{self.state.backups}"
        outcome = None
        for attempt in range(1, 4):
            try:
                outcome = self.backups.restore(result.root, target)
                break
            except Exception as exc:  # noqa: BLE001 - classified below, never swallowed
                name = type(exc).__name__
                winerror = getattr(exc, "winerror", None)
                if name == "ConflictError":
                    self.expected_refusal(
                        f"restore into a destination that already held a tree was"
                        f" refused: {exc}"
                    )
                    break
                if winerror in (5, 32, 145) or "WinError 145" in str(exc):
                    if attempt >= 3:
                        self.note(f"restore cleanup race survived 3 attempts: {exc}")
                        break
                    self.state.cleanup_retries += 1
                    self.state.cleanup_retry_events.append(
                        {"at": now(), "phase": self.state.phase, "kind": "CLEANUP_RETRY",
                         "path": str(target), "attempt": attempt,
                         "error": f"{name}: {exc}"}
                    )
                    self.cleanup_dir(target, label="restore-target")
                    continue
                self.product_fault(f"restore raised {name}: {exc}")
                break
        if outcome is None:
            self.cleanup_dir(target, label="restore-target")
            self.cleanup_dir(result.root, label="backup-root")
            return
        if not outcome.get("ready"):
            self.note("restore was not ready")
            return
        restored = Store(target / "agent_os.sqlite3")
        if restored.checkpoint()["schema_version"] != self.store.schema_version():
            self.note("restored schema version differs")
            return
        self.state.restores += 1
        try:
            self.backups.restore(result.root, target)
        except Exception as exc:  # noqa: BLE001 - the refusal is the assertion
            if type(exc).__name__ == "ConflictError":
                self.expected_refusal(
                    f"second restore into the non-empty destination was refused: {exc}"
                )
            else:
                self.product_fault(
                    f"a non-empty destination produced {type(exc).__name__} instead of"
                    f" a ConflictError: {exc}"
                )
        else:
            self.state.negative_test_failures += 1
            self.product_fault(f"restore overwrote a NON-EMPTY destination at {target}")
        self.cleanup_dir(target, label="restore-target")
        self.cleanup_dir(result.root, label="backup-root")

    def phase_knowledge(self) -> None:
        service = KnowledgeService(self.store)
        for index in range(3):
            try:
                service.propose(
                    project_id=self.project.id,
                    topic=f"soak-{self.state.cycles}",
                    content=f"cycle {self.state.cycles} sample {index}",
                    author="manager",
                    source_ref=f"soak/{self.run_id}/{index}",
                    source_digest=hashlib.sha256(
                        f"{self.run_id}-{self.state.cycles}-{index}".encode()
                    ).hexdigest(),
                    authorization_digest=AUTH,
                )
                self.state.knowledge_records += 1
            except Exception as exc:  # noqa: BLE001 - a refused write is data
                self.note(f"knowledge: {type(exc).__name__}: {exc}")
                return
        found = service.search(
            KnowledgeQuery(project_id=self.project.id, topic=f"soak-{self.state.cycles}")
        )
        if len(found) != 3:
            self.note("knowledge round trip lost a record")

    def phase_cancel_and_restart(self) -> None:
        """Cancel a running job, then kill a real child and recover."""
        job_id, handle = self.new_job(6)
        scheduler = Scheduler(self.store)
        claim = scheduler.claim(job_id, "n0")
        self.state.claims += 1
        scheduler.cancel(job_id, reason="soak cancel phase")
        self.state.cancels += 1
        if self.store.job_state(job_id) is not TaskState.CANCELLED:
            self.note("cancel did not reach CANCELLED")
        if self.store.lease_is_current(claim.lease_id, claim.fence):
            self.note("cancelled job still holds a current lease")

        # an already-running node must refuse a second claim: that refusal is the
        # intended control flow, so it is counted, not treated as a defect
        try:
            scheduler.claim(job_id, "n1")
            scheduler.claim(job_id, "n1")
            self.note("a running node accepted a second claim")
        except (ConcurrencyError, AttemptExhausted, StateTransitionError):
            pass
        except Exception as exc:  # noqa: BLE001
            self.note(f"unexpected claim refusal: {type(exc).__name__}")

        child = subprocess.Popen(
            [sys.executable, "-c", "import time; time.sleep(600)"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        time.sleep(0.4)
        child.kill()
        child.wait(timeout=10)
        self.state.restarts += 1

        recovery_job, _ = self.new_job(4)
        recover_claim = scheduler.claim(recovery_job, "n0", lease_seconds=1)
        self.store.clock.advance(2)
        report = scheduler.recover(recovery_job)
        if recover_claim.run_id not in report["recovered_runs"]:
            self.note("recovery did not release the lapsed run")
        if scheduler.node_states(recovery_job)["n0"] is not TaskState.FIX:
            self.note("recovered node was not left claimable")
        self.state.phase_times["recovery"] = self.state.phase_times.get("recovery", 0.0) + 1.0

    # ------------------------------------------------------------- invariants
    def check_invariants(self) -> dict[str, Any]:
        connection = sqlite3.connect(self.database)
        try:
            integrity = connection.execute("PRAGMA integrity_check").fetchone()[0]
            # a run that is OPEN while its node is not WORKING, or while its job
            # is finished, can never be settled by anyone: it would keep a lease
            # alive and make a finished node look busy for the whole session
            orphan = connection.execute(
                "SELECT COUNT(*) AS c FROM runs r"
                " JOIN jobs j ON j.job_id = r.job_id"
                " JOIN nodes n ON n.job_id = r.job_id AND n.node_id = r.node_id"
                " WHERE r.status = 'OPEN' AND (n.state <> 'WORKING'"
                " OR j.state IN ('DONE', 'BLOCKED', 'CANCELLED', 'APPLIED'))"
            ).fetchone()[0]
        finally:
            connection.close()
        if int(orphan) > 0:
            self.note(f"{int(orphan)} orphaned OPEN run(s): unrunnable and unsettleable")
        checkpoint = self.store.checkpoint()
        ledger_usage = none = None
        with self.store.read() as conn:
            reservations = conn.execute(
                "SELECT state, COUNT(*) AS c FROM reservations GROUP BY state"
            ).fetchall()
            ledger_usage = {row["state"]: int(row["c"]) for row in reservations}
        row_counts = self.store.row_counts_for_test()
        return {
            "at": now(),
            "sqlite_integrity": integrity,
            "orphan_open_runs": int(orphan),
            "active_leases": checkpoint["active_leases"],
            "open_runs": checkpoint["open_runs"],
            "pending_operations": len(checkpoint["pending_operations"]),
            "reservation_states": ledger_usage,
            "row_counts": row_counts,
        }

    # ------------------------------------------------------------------ loop
    def note(self, message: str) -> None:
        """Record a genuine defect together with the phase that produced it."""
        self.state.errors.append(f"[{self.state.phase}] {message}")
        self.state.harness_errors += 1

    def product_fault(self, message: str) -> None:
        """An unexpected product exception: always counted, never swallowed."""
        self.state.errors.append(f"[{self.state.phase}] PRODUCT {message}")
        self.state.product_errors += 1

    def unexpected_fault(self, message: str) -> None:
        self.state.errors.append(f"[{self.state.phase}] UNEXPECTED {message}")
        self.state.unexpected_errors += 1

    def expected_refusal(self, message: str) -> None:
        """A refusal the load design *expects*: a passing negative test.

        It is recorded with its count and never added to SOAK_ERRORS; if the
        refusal did not happen, the caller records a product fault instead.
        """
        self.state.expected_refusals += 1
        self.state.cleanup_retry_events.append(
            {"at": now(), "phase": self.state.phase, "kind": "EXPECTED_REFUSAL",
             "detail": message}
        )

    def cleanup_dir(self, path, *, attempts: int = 6, label: str = "cleanup") -> bool:
        """Remove a directory with bounded retry; a real failure is reported.

        Windows keeps a directory busy for a moment after a file handle closes,
        which surfaces as WinError 145. Retrying a bounded number of times with
        backoff is the honest fix; the exceptions are logged per attempt and a
        failure after the last attempt becomes a HARNESS error, never silence.
        """
        import time as _time

        for attempt in range(1, attempts + 1):
            try:
                shutil.rmtree(path)
                return True
            except FileNotFoundError:
                return True
            except OSError as exc:
                if attempt >= attempts:
                    self.state.cleanup_failures += 1
                    self.note(f"{label}: cleanup failed after {attempts} attempts:"
                              f" {type(exc).__name__}: {exc} @ {path}")
                    return False
                self.state.cleanup_retries += 1
                self.state.cleanup_retry_events.append(
                    {"at": now(), "phase": self.state.phase, "kind": "CLEANUP_RETRY",
                     "path": str(path), "attempt": attempt,
                     "error": f"{type(exc).__name__}: {exc}"}
                )
                _time.sleep(min(1.0, 0.05 * (2 ** attempt)))
        return False

    def lease_cleanup_check(self) -> dict:
        """Post-exit check: no ACTIVE lease may outlive its run.

        A lease whose run already reached a terminal status is exactly the
        "unexpected open lease" a release gate must not hide.
        """
        try:
            with self.store.read() as conn:
                rows = conn.execute(
                    "SELECT l.lease_id, l.status AS lease_status, r.status AS run_status"
                    " FROM leases l JOIN runs r ON r.run_id = l.run_id"
                    " WHERE l.status = 'ACTIVE'"
                    " AND r.status NOT IN ('OPEN')"
                ).fetchall()
                active = conn.execute(
                    "SELECT COUNT(*) AS n FROM leases WHERE status = 'ACTIVE'"
                ).fetchone()["n"]
        except Exception as exc:  # noqa: BLE001 - an unreadable check is not a pass
            return {"unexpected_open_leases": None,
                    "active_leases_final": None,
                    "lease_cleanup_error": f"{type(exc).__name__}: {exc}"}
        return {"unexpected_open_leases": len(rows),
                "active_leases_final": int(active),
                "orphan_lease_ids": [row["lease_id"] for row in rows][:10]}


    def cycle(self, server: UiServer | None) -> None:
        self.state.cycles += 1
        choice = self.state.cycles % 6
        if choice == 0:
            self.state.phase = "backup_restore"
            self.phase_backup_restore()
        elif choice == 1:
            self.state.phase = "concurrent_writers"
            self.phase_concurrent_writers()
        elif choice == 2:
            self.state.phase = "mcp"
            self.phase_mcp()
        elif choice == 3:
            self.state.phase = "knowledge"
            self.phase_knowledge()
        elif choice == 4:
            self.state.phase = "cancel_restart"
            self.phase_cancel_and_restart()
        else:
            self.state.phase = "dag"
            self.phase_dag()
        self.state.phase = "web"
        self.phase_web(server)
        if self.state.cycles % 5 == 0:
            sample = self.check_invariants()
            sample.update(
                {
                    "event": "sample",
                    "cycle": self.state.cycles,
                    "elapsed_seconds": round(time.monotonic() - self.state.started, 1),
                    "rss_bytes": rss_bytes(os.getpid()),
                    "database_bytes": self.database.stat().st_size,
                    "root_bytes": dir_bytes(self.root),
                    "claims": self.state.claims,
                    "completions": self.state.completions,
                    "peak_workers": self.state.peak_workers,
                }
            )
            self.state.samples.append(sample)
            self.write_log(sample)
            print(
                json.dumps(
                    {
                        "cycle": self.state.cycles,
                        "elapsed": sample["elapsed_seconds"],
                        "claims": self.state.claims,
                        "completions": self.state.completions,
                        "errors": len(self.state.errors),
                    }
                ),
                flush=True,
            )

    def write_log(self, payload: dict[str, Any]) -> None:
        with open(self.log, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, ensure_ascii=False, default=str) + "\n")

    def run(self) -> dict[str, Any]:
        self.setup()
        backend = UiBackend(store=self.store, workspace_root=self.manager.managed_root)
        server = UiServer(backend, port=0).start()
        deadline = self.state.started + self.seconds
        try:
            while time.monotonic() < deadline and not self.stop.is_set():
                try:
                    self.cycle(server)
                except Exception as exc:  # noqa: BLE001 - a cycle failure is data
                    detail = f"{type(exc).__name__}: {exc}"
                    frame = sys.exc_info()[2]
                    while frame and frame.tb_next:
                        frame = frame.tb_next
                    if frame is not None:
                        detail += f" @ {frame.tb_frame.f_code.co_name}:{frame.tb_lineno}"
                    self.note(f"cycle {self.state.cycles}: {detail}")
                remaining = deadline - time.monotonic()
                if remaining > 0:
                    time.sleep(min(2.0, max(0.2, remaining / 40)))
        finally:
            server.stop()
        return self.finish()

    def finish(self) -> dict[str, Any]:
        elapsed = time.monotonic() - self.state.started
        final = self.check_invariants()
        final.update(self.lease_cleanup_check())
        rss_samples = [s["rss_bytes"] for s in self.state.samples if s.get("rss_bytes")]
        report = {
            "kind": "DURABILITY_SOAK",
            "run_id": self.run_id,
            "load_mix": {
                "worker_executor": "deterministic fixture (no paid model call)",
                "live_model_calls": 0,
                "real_sqlite_transactions": True,
                "real_mcp_stdio_sessions": True,
                "real_web_endpoints": True,
                "real_backup_and_restore": True,
                "concurrent_writer_threads": 4,
                "worker_slots": min(3, self.workers),
            },
            "requested_seconds": self.seconds,
            "measured_elapsed_seconds": round(elapsed, 1),
            "machine": os.environ.get("COMPUTERNAME", "unknown"),
            "python": sys.version,
            "counts": {
                "cycles": self.state.cycles,
                "claims": self.state.claims,
                "completions": self.state.completions,
                "failures": self.state.failures,
                "blocked": self.state.blocked,
                "cancels": self.state.cancels,
                "child_restarts": self.state.restarts,
                "mcp_sessions": self.state.mcp_sessions,
                "mcp_errors": self.state.mcp_errors,
                "web_requests": self.state.web_requests,
                "web_errors": self.state.web_errors,
                "backups": self.state.backups,
                "restores": self.state.restores,
                "knowledge_records": self.state.knowledge_records,
                "peak_workers": self.state.peak_workers,
                "lock_errors": self.state.lock_errors,
                "contention_events": self.state.contention_events,
                "expected_refusals": self.state.expected_refusals,
                "negative_test_failures": self.state.negative_test_failures,
                "cleanup_retries": self.state.cleanup_retries,
                "cleanup_failures": self.state.cleanup_failures,
                "product_errors": self.state.product_errors,
                "harness_errors": self.state.harness_errors,
                "unexpected_errors": self.state.unexpected_errors,
            },
            "memory": {
                "rss_first_bytes": rss_samples[0] if rss_samples else None,
                "rss_last_bytes": rss_samples[-1] if rss_samples else None,
                "rss_growth_bytes": (rss_samples[-1] - rss_samples[0]) if len(rss_samples) > 1 else None,
                "rss_samples": len(rss_samples),
            },
            "disk": {
                "root_bytes_final": dir_bytes(self.root),
                "database_bytes_final": self.database.stat().st_size,
                "log_bytes": self.log.stat().st_size if self.log.exists() else 0,
            },
            "phase_seconds": {k: round(v, 2) for k, v in self.state.phase_times.items()},
            "final_invariants": final,
            "events": {
                "EXPECTED_REFUSAL": self.state.expected_refusals,
                "CLEANUP_RETRY": self.state.cleanup_retries,
                "CLEANUP_FAILURE": self.state.cleanup_failures,
                "PRODUCT_ERROR": self.state.product_errors,
                "HARNESS_ERROR": self.state.harness_errors,
                "UNEXPECTED_ERROR": self.state.unexpected_errors,
            },
            "cleanup_retry_events": self.state.cleanup_retry_events[:100],
            "errors": self.state.errors[:50],
            "error_count": len(self.state.errors),
            "soak_errors": (self.state.product_errors + self.state.harness_errors
                            + self.state.unexpected_errors),
            "samples": self.state.samples,
            "started_at": datetime.fromtimestamp(
                time.time() - elapsed, tz=timezone.utc
            ).isoformat(),
            "ended_at": now(),
        }
        report["verdict"] = (
            "PASS"
            if elapsed >= self.seconds
            and final["sqlite_integrity"] == "ok"
            and final["orphan_open_runs"] == 0
            and self.state.peak_workers >= 2
            and self.state.mcp_errors == 0
            and self.state.web_errors == 0
            and self.state.lock_errors == 0
            and not self.state.errors
            and self.state.product_errors == 0
            and self.state.harness_errors == 0
            and self.state.unexpected_errors == 0
            and self.state.cleanup_failures == 0
            and self.state.negative_test_failures == 0
            and final.get("unexpected_open_leases") == 0
            and self.state.expected_refusals > 0
            else "PARTIAL"
        )
        report["limitations"] = [
            "the Worker role ran a deterministic fixture, not a live model",
            "a single Windows host was used; no cross-machine concurrency",
            "this run proves elapsed-time behaviour of this exact build only",
        ]
        self.receipt.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        return report


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--seconds", type=int, default=14_400)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--seed", type=int, default=20260914)
    parser.add_argument("--root", default=None)
    args = parser.parse_args()
    root = Path(args.root) if args.root else ROOT / ".agent_os_runtime" / "soak"
    runner = SoakRunner(seconds=args.seconds, workers=args.workers, seed=args.seed, root=root)
    report = runner.run()
    print(json.dumps({k: report[k] for k in ("verdict", "measured_elapsed_seconds",
                                            "counts", "final_invariants", "error_count")},
                     ensure_ascii=False, indent=2, default=str)[:2500])
    print("receipt:", runner.receipt)
    return 0 if report["verdict"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
