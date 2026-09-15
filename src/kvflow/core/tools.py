"""The four fixed tool groups: Repo, Test, Research and Knowledge.

Everything here is named and pre-registered. There is deliberately no
arbitrary-shell, arbitrary-SQL or arbitrary-URL action: the model may only ask
for a registered operation, and every request is authorized against an
Orchestrator-issued capability ticket before it runs (``kvflow.core.security``).

Cross-cutting rules
-------------------

* **No long-held transactions.** A tool runs *outside* every database
  transaction. Durable state is written before dispatch (an ``operation`` row)
  and after it (the result), never during.
* **Long calls hand back an operation id.** ``operation.status`` /
  ``operation.result`` / ``operation.cancel`` read and settle that row; an
  operation can never be read across a task boundary.
* **Bounded output.** Every textual result is capped; the receipt records the
  digests and exact byte counts, so a truncated result is never passed off as
  complete.
* **Identity comes from the ticket, not the arguments.** A model that passes
  ``role=manager`` or another ``run_id`` changes nothing.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import subprocess
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Callable, Iterable, Mapping, Sequence

from .contracts import (
    ARGV_EXECUTABLE_ALLOWLIST,
    Action,
    ExperimentRun,
    MAX_EXPERIMENT_ATTEMPTS,
    TestProfile,
    TestReceipt,
    content_digest,
)
from .errors import (
    AttemptExhausted,
    AuthorizationError,
    CapabilityDenied,
    ConcurrencyError,
    ContractError,
    NotFoundError,
    PathDenied,
    SizeError,
)
from .security import CapabilityAuthority, PathPolicy
from .store import Store, new_id
from .workspace import WorkspaceHandle, WorkspaceManager, is_product_artifact

#: hard ceiling on any single textual tool result before it is truncated
DEFAULT_MAX_RESULT_BYTES = 262_144
#: a test profile may never run longer than this, whatever it declares
HARD_MAX_TEST_SECONDS = 1800
#: the only interpreters a test profile may invoke
_ALLOWED_RUNNERS = {
    "pytest": ("-m", "pytest"),
    "unittest": ("-m", "unittest"),
    "readonly_inventory": ("-m", "pytest"),
}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _resolve_executable(name: str) -> list[str]:
    """Resolve one allowlisted executable from a fixed-argv profile.

    ``python``/``python3`` always mean *this* interpreter, so a profile that asks
    for KVFlow's own checker cannot accidentally pick up another environment.
    Any other program is resolved from PATH, or from the absolute path the project
    pinned when it was onboarded - a machine can carry a toolchain (a bundled Node
    runtime, for instance) that is deliberately not on PATH, and the approved
    profile is the right place to record exactly which binary that is. A missing
    program is a typed refusal instead of a failed run discovered halfway through.
    """
    raw = str(name)
    key = Path(raw).name.casefold()
    if key not in ARGV_EXECUTABLE_ALLOWLIST:
        raise ContractError("the profile's executable is not allowlisted", executable=key)
    if key in {"python", "python3", "python.exe"} and not os.path.isabs(raw):
        return [os.sys.executable]
    if key == "pytest" and not os.path.isabs(raw):
        return [os.sys.executable, "-m", "pytest"]
    if os.path.isabs(raw):
        candidate = Path(raw)
        if candidate.is_file():
            return [str(candidate)]
        raise ContractError(
            "the profile's pinned executable does not exist on this machine",
            executable=raw,
        )
    found = shutil.which(key)
    if found is None:
        raise ContractError(
            "the profile's executable is not installed on this machine", executable=key
        )
    return [found]


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _bound(text: str, limit: int) -> tuple[str, bool, int]:
    raw = text.encode("utf-8")
    if len(raw) <= limit:
        return text, False, len(raw)
    clipped = raw[:limit]
    while clipped:
        try:
            return clipped.decode("utf-8"), True, len(clipped)
        except UnicodeDecodeError:
            clipped = clipped[:-1]
    return "", True, 0


def _search_files(root: Path, pattern: str, *, max_matches: int, max_bytes: int) -> dict[str, Any]:
    """A regex content search with hard match and byte bounds.

    Matches accumulate until either bound is reached; the result reports exactly
    which bound stopped it, so a caller can tell "that is all there is" from
    "there is more and you must narrow the search".
    """
    try:
        expression = re.compile(pattern)
    except re.error as exc:
        raise ContractError(f"invalid search pattern: {exc}") from exc
    matches: list[dict[str, Any]] = []
    scanned = 0
    truncated = False
    truncated_by: str | None = None
    running = 0
    for path in sorted(root.rglob("*")):
        if path.is_dir() or not path.is_file():
            continue
        if ".git" in path.parts or "__pycache__" in path.parts:
            continue
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        scanned += 1
        for number, line in enumerate(text.splitlines(), start=1):
            if not expression.search(line):
                continue
            relative = PurePosixPath(*path.relative_to(root).parts).as_posix()
            entry = {"path": relative, "line": number, "text": line[:400]}
            entry_bytes = len(json.dumps(entry, ensure_ascii=False)) + 1
            if len(matches) >= max_matches:
                truncated, truncated_by = True, "match_count"
                break
            if running + entry_bytes > max_bytes:
                truncated, truncated_by = True, "output_bytes"
                break
            matches.append(entry)
            running += entry_bytes
        if truncated:
            break
    return {
        "matches": matches,
        "match_count": len(matches),
        "files_scanned": scanned,
        "approx_bytes": running,
        "truncated": truncated,
        "truncated_by": truncated_by,
    }


@dataclass(frozen=True)
class ToolResult:
    ok: bool
    action: str
    operation_id: str | None
    status: str
    output: dict[str, Any]
    output_bytes: int
    truncated: bool
    output_digest: str
    error_code: str | None
    started_at: str
    finished_at: str
    duration_ms: float
    audit: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "action": self.action,
            "operation_id": self.operation_id,
            "status": self.status,
            "output": self.output,
            "output_bytes": self.output_bytes,
            "truncated": self.truncated,
            "output_digest": self.output_digest,
            "error_code": self.error_code,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "duration_ms": self.duration_ms,
            "audit": self.audit,
        }


class ToolService:
    """Executes the registered operations against owned workspaces only."""

    def __init__(
        self,
        store: Store,
        authority: CapabilityAuthority,
        *,
        workspace_root: Path,
        max_result_bytes: int = DEFAULT_MAX_RESULT_BYTES,
        test_timeout_default: int = 180,
    ) -> None:
        self.store = store
        self.authority = authority
        self.workspace_root = Path(workspace_root)
        self.max_result_bytes = int(max_result_bytes)
        self.test_timeout_default = int(test_timeout_default)

    # ------------------------------------------------------------- dispatch
    def invoke(
        self,
        ticket: str,
        action: str | Action,
        arguments: Mapping[str, Any] | None = None,
    ) -> ToolResult:
        """Authorize, then run exactly one named operation."""
        started = _now()
        clock = time.perf_counter()
        value = action.value if isinstance(action, Action) else str(action)
        args = dict(arguments or {})
        try:
            capability = self.authority.verify(ticket, value)
        except CapabilityDenied as exc:
            return self._refused(value, exc, started, clock)
        try:
            handler = self._handler(value)
        except ContractError as exc:
            return self._refused(value, exc, started, clock)
        operation_id = self.store.open_operation(
            capability.job_id,
            capability.node_id,
            capability.run_id,
            capability.project_id,
            value,
            content_digest({k: str(v) for k, v in args.items()}),
        )
        try:
            output = handler(capability, args)
        except (PathDenied, AuthorizationError, ContractError, NotFoundError, SizeError) as exc:
            self.store.finish_operation(
                operation_id, "FAILED", error_code=getattr(exc, "code", "ERROR"),
                owner_run=capability.run_id,
            )
            return self._failed(value, operation_id, exc, started, clock, capability)
        except Exception as exc:  # noqa: BLE001 - a tool failure is data
            # an unexpected failure after dispatch is genuinely unknown: the
            # effect may have happened, so it is never silently retried
            self.store.finish_operation(
                operation_id, "OUTCOME_UNKNOWN", error_code="UNEXPECTED",
                owner_run=capability.run_id,
            )
            return self._failed(
                value, operation_id, exc, started, clock, capability,
                status="OUTCOME_UNKNOWN",
            )
        text = json.dumps(output, ensure_ascii=False, sort_keys=True, default=str)
        bounded, truncated, size = _bound(text, self.max_result_bytes)
        if truncated:
            # one bounded representation: the payload is replaced by a preview and
            # the exact original size, so nothing claims to be complete
            preview, _, _ = _bound(text, max(1, self.max_result_bytes - 256))
            output = {
                "truncated": True,
                "preview_text": preview,
                "original_bytes": size,
                "limit_bytes": self.max_result_bytes,
            }
        digest = _sha256(json.dumps(output, ensure_ascii=False, sort_keys=True,
                                    default=str).encode("utf-8"))
        self.store.finish_operation(
            operation_id, "SUCCEEDED", result_digest=digest, owner_run=capability.run_id
        )
        return ToolResult(
            ok=True,
            action=value,
            operation_id=operation_id,
            status="SUCCEEDED",
            output=output,
            output_bytes=size,
            truncated=truncated,
            output_digest=digest,
            error_code=None,
            started_at=started,
            finished_at=_now(),
            duration_ms=round((time.perf_counter() - clock) * 1000.0, 3),
            audit={
                "project_id": capability.project_id,
                "job_id": capability.job_id,
                "node_id": capability.node_id,
                "run_id": capability.run_id,
                "attempt": capability.attempt,
                "fence": capability.fence,
                "role": capability.role,
            },
        )

    def _refused(self, action: str, exc: Exception, started: str, clock: float) -> ToolResult:
        return ToolResult(
            ok=False,
            action=action,
            operation_id=None,
            status="NOT_DISPATCHED",
            output={"error": str(exc)},
            output_bytes=0,
            truncated=False,
            output_digest=_sha256(b""),
            error_code=getattr(exc, "code", "ERROR"),
            started_at=started,
            finished_at=_now(),
            duration_ms=round((time.perf_counter() - clock) * 1000.0, 3),
        )

    def _failed(
        self,
        action: str,
        operation_id: str,
        exc: Exception,
        started: str,
        clock: float,
        capability: Any,
        *,
        status: str = "FAILED",
    ) -> ToolResult:
        return ToolResult(
            ok=False,
            action=action,
            operation_id=operation_id,
            status=status,
            output={"error": str(exc)},
            output_bytes=0,
            truncated=False,
            output_digest=_sha256(b""),
            error_code=getattr(exc, "code", "ERROR"),
            started_at=started,
            finished_at=_now(),
            duration_ms=round((time.perf_counter() - clock) * 1000.0, 3),
            audit={
                "project_id": capability.project_id,
                "job_id": capability.job_id,
                "node_id": capability.node_id,
                "run_id": capability.run_id,
                "fence": capability.fence,
            },
        )

    def _handler(self, action: str) -> Callable[[Any, dict[str, Any]], dict[str, Any]]:
        handlers = {
            Action.REPO_READ.value: self._repo_read,
            Action.REPO_SEARCH.value: self._repo_search,
            Action.REPO_LIST.value: self._repo_list,
            Action.REPO_STATUS.value: self._repo_status,
            Action.REPO_DIFF.value: self._repo_diff,
            Action.REPO_PATCH.value: self._repo_patch,
            Action.REPO_COMMIT.value: self._repo_commit,
            Action.TEST_RUN.value: self._test_run,
            Action.OPERATION_STATUS.value: self._operation_status,
            Action.OPERATION_RESULT.value: self._operation_result,
            Action.OPERATION_CANCEL.value: self._operation_cancel,
            Action.KNOWLEDGE_SEARCH.value: self._knowledge_search,
            Action.KNOWLEDGE_READ.value: self._knowledge_read,
            Action.RESEARCH_RUN.value: self._research_run,
        }
        try:
            return handlers[action]
        except KeyError as exc:
            raise ContractError(f"unknown tool action: {action!r}") from exc

    # ------------------------------------------------------------ workspace
    def _workspace(self, capability: Any) -> tuple[WorkspaceHandle, WorkspaceManager, PathPolicy]:
        """Resolve the owned workspace for this exact job/node."""
        project = self.store.project(capability.project_id)
        manager = WorkspaceManager(project)
        root = manager.worktrees_dir / capability.job_id / capability.node_id
        handle_path = root / "WORKSPACE.json"
        if not handle_path.exists():
            raise NotFoundError(
                "no owned workspace for this node", node_id=capability.node_id
            )
        data = json.loads(handle_path.read_text(encoding="utf-8"))
        handle = WorkspaceHandle(
            job_id=data["job_id"],
            node_id=data["node_id"],
            project_id=data["project_id"],
            root=root,
            snapshot_id=data["snapshot_id"],
            scopes=tuple(data["scopes"]),
            created_at=data["created_at"],
        )
        if handle.project_id != capability.project_id or handle.job_id != capability.job_id:
            raise AuthorizationError("workspace does not belong to this capability")
        policy = PathPolicy(project)
        return handle, manager, policy

    def _target(self, handle: WorkspaceHandle, scope: str, *, write: bool) -> Path:
        return (
            workspace_write_target(handle, scope)
            if write
            else workspace_read_target(handle, scope)
        )

    # ---------------------------------------------------------------- repo

    def _repo_list(self, capability: Any, args: dict[str, Any]) -> dict[str, Any]:
        handle, _, _ = self._workspace(capability)
        scope = str(args.get("path", "."))
        target = self._target(handle, scope, write=False)
        if not target.exists():
            raise NotFoundError("path is absent", path=scope)
        entries = []
        for path in sorted(target.iterdir()):
            if is_product_artifact(path.name):
                continue
            entries.append(
                {
                    "name": path.name,
                    "is_dir": path.is_dir(),
                    "size_bytes": path.stat().st_size if path.is_file() else None,
                }
            )
        return {"path": scope, "entries": entries[:512], "entry_count": len(entries)}

    def _repo_read(self, capability: Any, args: dict[str, Any]) -> dict[str, Any]:
        handle, _, _ = self._workspace(capability)
        scope = str(args.get("path", ""))
        if not scope:
            raise ContractError("a path is required")
        target = self._target(handle, scope, write=False)
        if not target.is_file():
            raise NotFoundError("file is absent", path=scope)
        limit = int(args.get("max_bytes", 32768))
        if limit < 1 or limit > 1_048_576:
            raise ContractError("max_bytes is out of range")
        raw = target.read_bytes()
        text, truncated, size = _bound(raw.decode("utf-8", errors="replace"), limit)
        return {
            "path": scope,
            "text": text,
            "truncated": truncated,
            "file_bytes": len(raw),
            "included_bytes": size,
            "sha256": _sha256(raw),
        }

    def _repo_search(self, capability: Any, args: dict[str, Any]) -> dict[str, Any]:
        handle, _, _ = self._workspace(capability)
        pattern = str(args.get("pattern", ""))
        if not pattern:
            raise ContractError("a search pattern is required")
        scope = str(args.get("path", "."))
        root = self._target(handle, scope, write=False)
        result = _search_files(
            root, pattern, max_matches=int(args.get("max_matches", 200)),
            max_bytes=max(1024, self.max_result_bytes // 2),
        )
        result["root"] = scope
        return result

    def _repo_status(self, capability: Any, args: dict[str, Any]) -> dict[str, Any]:
        handle, manager, _ = self._workspace(capability)
        return manager.workspace_diff(handle)

    def _repo_diff(self, capability: Any, args: dict[str, Any]) -> dict[str, Any]:
        handle, manager, _ = self._workspace(capability)
        diff = manager.workspace_diff(handle)
        requested = args.get("path")
        if requested:
            clean = _safe_relative(str(requested))
            diff["changed"] = [
                c for c in diff["changed"] if c["relative_path"] == clean
            ]
            diff["change_count"] = len(diff["changed"])
            diff["filtered_to"] = clean
        for change in diff["changed"]:
            change.pop("base_sha256", None)
        return diff

    def _repo_patch(self, capability: Any, args: dict[str, Any]) -> dict[str, Any]:
        """Write files inside the node's declared scopes only."""
        handle, _, _ = self._workspace(capability)
        edits = args.get("edits")
        if not isinstance(edits, list) or not edits:
            raise ContractError("edits must be a nonempty list")
        if len(edits) > 128:
            raise ContractError("too many edits in one patch")
        applied: list[dict[str, Any]] = []
        total_bytes = sum(len(str(e.get("content", "")).encode("utf-8")) for e in edits)
        if total_bytes > 4 * 1024 * 1024:
            raise SizeError("patch content exceeds the per-call ceiling")
        for edit in edits:
            if not isinstance(edit, dict):
                raise ContractError("each edit must be an object")
            path = str(edit.get("path", ""))
            content = edit.get("content")
            if not path or not isinstance(content, str):
                raise ContractError("each edit needs a path and string content")
            # the write scope check is the node's own declaration
            target = workspace_write_target(handle, path)
            target.parent.mkdir(parents=True, exist_ok=True)
            previous = target.read_bytes() if target.exists() else None
            target.write_text(content, encoding="utf-8", newline="")
            digest = _sha256(target.read_bytes())
            applied.append(
                {
                    "path": _safe_relative(path),
                    "bytes": len(content.encode("utf-8")),
                    "sha256": digest,
                    "previous_sha256": _sha256(previous) if previous is not None else None,
                }
            )
        return {"applied": applied, "applied_count": len(applied)}

    def _repo_commit(self, capability: Any, args: dict[str, Any]) -> dict[str, Any]:
        handle, manager, _ = self._workspace(capability)
        message = str(args.get("message", "")).strip()
        if not message:
            raise ContractError("a commit message is required")
        # the author is the authenticated role, never a caller-supplied string
        author = f"{capability.role} (kvflow)"
        result = manager.commit_workspace(handle, message=message, author=author)
        if result["exit_code"] != 0:
            raise ContractError(f"commit failed: {result['stderr'] or result['exit_code']}")
        return result

    # --------------------------------------------------------------- test
    def _test_run(self, capability: Any, args: dict[str, Any]) -> dict[str, Any]:
        handle, _, _ = self._workspace(capability)
        project = self.store.project(capability.project_id)
        profile_id = str(args.get("profile_id", ""))
        profile = next((p for p in project.test_profiles if p.id == profile_id), None)
        if profile is None:
            raise AuthorizationError(
                "the requested test profile is not registered for this project",
                profile_id=profile_id,
            )
        return self.run_profile(handle, project, capability, profile)

    def run_profile(
        self,
        handle: WorkspaceHandle,
        project: Any,
        capability: Any,
        profile: TestProfile,
        *,
        extra_argv: Sequence[str] = (),
    ) -> dict[str, Any]:
        """Run one registered profile with a fixed argv and a hard timeout."""
        if profile.project_id != project.id:
            raise AuthorizationError("the profile belongs to another project")
        cwd = self._target(handle, profile.cwd or ".", write=False)
        if not cwd.exists():
            raise NotFoundError("the profile cwd does not exist", cwd=profile.cwd)
        if profile.runner == "argv":
            # a project-approved fixed argv: re-validated here, executed as a list
            argv = [*_resolve_executable(profile.argv[0]), *profile.argv[1:], *extra_argv]
        else:
            runner = _ALLOWED_RUNNERS.get(profile.runner)
            if runner is None:
                raise ContractError("unsupported test runner", runner=profile.runner)
            argv = [
                os.sys.executable,
                *runner,
                "-p",
                "no:cacheprovider",
                "-o",
                "addopts=",
                "-c",
                str(workspace_pytest_config(handle)),
                *profile.targets,
                *extra_argv,
            ]
        timeout = min(int(profile.timeout_seconds), HARD_MAX_TEST_SECONDS)
        before = content_digest(
            {
                "change": workspace_change_fingerprint(handle),
                "profile": profile.id,
            }
        )
        started_at = _now()
        clock = time.perf_counter()
        env = {
            key: os.environ[key]
            for key in ("PATH", "PATHEXT", "SYSTEMROOT", "SYSTEMDRIVE", "WINDIR", "COMSPEC",
                        "TEMP", "TMP", "NUMBER_OF_PROCESSORS", "PROCESSOR_ARCHITECTURE")
            if key in os.environ
        }
        env["PYTHONIOENCODING"] = "utf-8"
        env["PYTHONUTF8"] = "1"
        env["PYTHONDONTWRITEBYTECODE"] = "1"
        # the profile declares which owned subdirectories become import roots
        roots = [str(handle.root)]
        for entry in profile.pythonpath:
            candidate = workspace_read_target(handle, entry)
            roots.append(str(candidate))
        if profile.runner == "argv":
            # A fixed-argv profile may run one of the product's own commands (the
            # artifact checker is the shipped example). The child gets a controlled
            # environment, so the product's own import root has to be added here or
            # `python -m kvflow...` cannot be found from a source checkout. This is
            # the product's own path, never a caller-supplied one.
            package_root = str(Path(__file__).resolve().parents[2])
            if package_root not in roots:
                roots.append(package_root)
        env["PYTHONPATH"] = os.pathsep.join(roots)
        # credentials are never inherited by a test child
        for forbidden in ("DEEPSEEK_API_KEY", "OPENAI_API_KEY", "HTTPS_PROXY",
                          "HTTP_PROXY", "KVFLOW_MCP_CAPABILITY"):
            env.pop(forbidden, None)
        timed_out = False
        logs_dir = handle.root / ".kvflow_logs"
        logs_dir.mkdir(parents=True, exist_ok=True)
        stdout_path = logs_dir / f"test-{profile.id}-{os.getpid()}-{int(clock * 1000)}.out"
        stderr_path = logs_dir / f"test-{profile.id}-{os.getpid()}-{int(clock * 1000)}.err"
        # the child writes into owned files rather than pipes: a nested pipe is
        # unavailable inside the MCP server's Windows Job Object, and file output
        # also removes any chance of a pipe-buffer deadlock on a chatty profile
        try:
            with open(stdout_path, "wb") as out_handle, open(stderr_path, "wb") as err_handle:
                try:
                    completed = subprocess.run(
                        argv,
                        cwd=str(cwd),
                        stdout=out_handle,
                        stderr=err_handle,
                        stdin=subprocess.DEVNULL,
                        timeout=timeout,
                        env=env,
                        check=False,
                    )
                    code = completed.returncode if completed.returncode is not None else -1
                except subprocess.TimeoutExpired:
                    timed_out = True
                    code = -1
            stdout = stdout_path.read_bytes().decode("utf-8", errors="replace")
            stderr = stderr_path.read_bytes().decode("utf-8", errors="replace")
        finally:
            for path in (stdout_path, stderr_path):
                try:
                    path.unlink()
                except OSError:  # pragma: no cover - best effort cleanup
                    pass
        duration_ms = round((time.perf_counter() - clock) * 1000.0, 3)
        limit = int(profile.output_limit_bytes)
        out_text, out_truncated, out_bytes = _bound(stdout, limit)
        err_text, err_truncated, err_bytes = _bound(stderr, limit // 2 or 1024)
        reported = _reported_tests(out_text)
        receipt = {
            "profile_id": profile.id,
            "runner": profile.runner,
            "argv": argv,
            "cwd": str(cwd.relative_to(handle.root)) if cwd != handle.root else ".",
            "exit_code": code,
            "timed_out": timed_out,
            "duration_ms": duration_ms,
            "timeout_seconds": timeout,
            "reported_tests": reported,
            "stdout_sha256": _sha256(stdout.encode("utf-8")),
            "stderr_sha256": _sha256(stderr.encode("utf-8")),
            "stdout_bytes": out_bytes,
            "stderr_bytes": err_bytes,
            "stdout_truncated": out_truncated,
            "stderr_truncated": err_truncated,
            "code_digest": before,
            "started_at": started_at,
            "finished_at": _now(),
        }
        # The receipt is written by the executor itself, bound to this exact
        # project/job/node/run/attempt/fence, so approval evidence survives the
        # process that produced it and can never be a Worker-authored JSON blob.
        durable = TestReceipt.model_validate(
            {
                "id": new_id("rcpt"),
                "binding": {
                    "project_id": capability.project_id,
                    "job_id": capability.job_id,
                    "node_id": capability.node_id,
                    "run_id": capability.run_id,
                    "attempt": int(capability.attempt),
                    "fence": int(capability.fence),
                },
                "profile_id": profile.id,
                "code_digest": before,
                "argv": [str(part) for part in argv],
                "cwd": receipt["cwd"],
                "exit_code": code,
                "duration_ms": int(round(duration_ms)),
                "timeout_seconds": timeout,
                "reported_tests": reported,
                "stdout_digest": _sha256(stdout.encode("utf-8")),
                "stderr_digest": _sha256(stderr.encode("utf-8")),
                "stdout_bytes": out_bytes,
                "stderr_bytes": err_bytes,
                "truncated": bool(out_truncated or err_truncated),
                "started_at": started_at,
                "finished_at": receipt["finished_at"],
            }
        )
        try:
            self.store.record_receipt(durable)
        except (ConcurrencyError, NotFoundError):
            # an identical receipt already exists: re-running the same profile in
            # the same attempt must not create a second durable receipt
            existing = self.store.receipts(capability.job_id, capability.node_id)
            if not any(row["receipt_id"] == durable.id for row in existing):
                raise
        receipt["receipt_id"] = durable.id
        return {
            "receipt": receipt,
            "receipt_id": durable.id,
            "stdout": out_text,
            "stderr": err_text,
            "passed": code == 0 and not timed_out,
        }

    # ----------------------------------------------------------- research
    def _research_run(self, capability: Any, args: dict[str, Any]) -> dict[str, Any]:
        """Run one frozen, isolated research spec inside its declared bounds.

        This is an *entry point*, never a general research runner:

        * the spec must already be persisted and bound to this exact
          project/job/node;
        * the profile must be registered with ``research_authorized``, so an
          ordinary test profile cannot be reused as a research route;
        * the frozen code digest must be the registered profile's own code and
          the live input scope must still match the spec's ``data_digest``;
        * the declared compute budget may not exceed the profile's own timeout;
        * the node's write scopes must contain the output scope, which may not
          overlap a protected root, and every changed file must land inside it.

        The recorded outcome can only ever be an isolated research result: the
        conclusion boundary is a literal and cannot be widened by a caller.
        """
        experiment_id = str(args.get("experiment_id", ""))
        if not experiment_id:
            raise ContractError("an experiment_id is required")
        spec = self.store.experiment(experiment_id)
        if (
            spec.project_id != capability.project_id
            or spec.job_id != capability.job_id
            or spec.node_id != capability.node_id
        ):
            raise AuthorizationError(
                "the experiment is not bound to this project/job/node",
                experiment_id=experiment_id,
            )
        if spec.conclusion_boundary != "NO_ALPHA_OR_FORWARD_VALIDATION":
            raise AuthorizationError(
                "an isolated research run may not claim an Alpha/Forward conclusion",
                experiment_id=experiment_id,
            )
        handle, manager, _ = self._workspace(capability)
        project = self.store.project(capability.project_id)
        profile = next((p for p in project.test_profiles if p.id == spec.profile_id), None)
        if profile is None:
            raise AuthorizationError(
                "the experiment profile is not registered", profile_id=spec.profile_id
            )
        if not profile.research_authorized:
            raise AuthorizationError(
                "that profile is not registered as a research entry point",
                profile_id=profile.id,
            )
        if spec.code_digest != profile.code_digest:
            raise AuthorizationError(
                "the experiment code digest is not the registered profile's code",
                profile_id=profile.id,
            )
        node = self.store.node(capability.job_id, capability.node_id)
        write_scopes = [str(scope) for scope in json.loads(node["write_scopes"] or "[]")]
        if not any(_scope_contains(scope, spec.output_scope) for scope in write_scopes):
            raise PathDenied(
                "the output scope exceeds the node's declared write scopes",
                output_scope=spec.output_scope,
                write_scopes=write_scopes,
            )
        for protected in project.protected_roots:
            if _scope_contains(protected, spec.output_scope) or _scope_contains(
                spec.output_scope, protected
            ):
                raise PathDenied(
                    "the output scope overlaps a protected root",
                    output_scope=spec.output_scope,
                    protected_root=protected,
                )
        if int(spec.computational_budget_seconds) > int(profile.timeout_seconds):
            raise ContractError(
                "the experiment asks for more compute time than the registered profile allows",
                budget_seconds=int(spec.computational_budget_seconds),
                profile_timeout_seconds=int(profile.timeout_seconds),
            )
        live_input = workspace_scope_fingerprint(handle, spec.input_scope)
        if live_input != spec.data_digest:
            raise ContractError(
                "the frozen input digest does not match the workspace data",
                input_scope=spec.input_scope,
                expected=spec.data_digest,
                actual=live_input,
            )
        consumed = self.store.experiment_attempts(experiment_id)
        allowed = min(int(spec.maximum_attempts), MAX_EXPERIMENT_ATTEMPTS)
        if consumed >= allowed:
            raise AttemptExhausted(
                f"experiment attempt budget exhausted ({consumed}/{allowed})",
                experiment_id=experiment_id,
                attempts=consumed,
                max_attempts=allowed,
            )

        run = self.run_profile(handle, project, capability, profile)
        payload = run["receipt"]
        produced = payload.get("reported_tests")
        if produced is not None and int(produced) > int(spec.maximum_candidates):
            raise ContractError(
                "the experiment produced more candidates than its frozen spec allows",
                produced=int(produced),
                maximum_candidates=int(spec.maximum_candidates),
            )
        diff = manager.workspace_diff(handle)
        outside = [
            change["relative_path"]
            for change in diff["changed"]
            if not _scope_contains(spec.output_scope, str(change["relative_path"]))
        ]
        if outside:
            raise PathDenied(
                "the experiment wrote outside its declared output scope",
                output_scope=spec.output_scope,
                paths=outside[:16],
            )
        if payload.get("timed_out"):
            outcome = "FAILED_TIME"
        elif int(payload.get("exit_code", 1)) == 0:
            outcome = "COMPLETED"
        else:
            outcome = "FAILED_EXIT_CODE"
        output_digest = workspace_scope_fingerprint(handle, spec.output_scope)
        record = ExperimentRun.model_validate(
            {
                "id": new_id("exp"),
                "experiment_id": experiment_id,
                "project_id": capability.project_id,
                "job_id": capability.job_id,
                "node_id": capability.node_id,
                "run_id": capability.run_id,
                "attempt": int(capability.attempt),
                "fence": int(capability.fence),
                "outcome": outcome,
                "receipt_id": run.get("receipt_id"),
                "candidate_count": int(produced or 0),
                "output_scope": spec.output_scope,
                "output_digest": output_digest,
                "observed_at": datetime.now(timezone.utc),
                "notes": [
                    f"exit_code={payload.get('exit_code')}",
                    f"timed_out={payload.get('timed_out')}",
                    f"changed_files={len(diff['changed'])}",
                ],
            }
        )
        self.store.record_experiment_run(record)
        return {
            "experiment_id": experiment_id,
            "experiment_run_id": record.id,
            "outcome": outcome,
            "conclusion_boundary": record.conclusion_boundary,
            "input_scope": spec.input_scope,
            "output_scope": spec.output_scope,
            "output_digest": output_digest,
            "candidate_count": int(produced or 0),
            "attempts_used": consumed + 1,
            "attempts_allowed": allowed,
            "receipt_id": run.get("receipt_id"),
            "profile": payload,
            "passed": bool(run.get("passed")),
            "isolation_note": (
                "this run used the node's own workspace copy only; nothing was written"
                " to the registered source project, protected roots, Forward or Golden"
            ),
        }

    # ---------------------------------------------------------- knowledge
    def _knowledge_search(self, capability: Any, args: dict[str, Any]) -> dict[str, Any]:
        """Search only the scope this capability's project may see."""
        from .knowledge import KnowledgeQuery, KnowledgeService

        query = KnowledgeQuery(
            project_id=capability.project_id,
            topic=args.get("topic") or None,
            limit=int(args.get("limit", 25)),
            include_global=bool(args.get("include_global", True)),
        )
        service = KnowledgeService(self.store)
        records = service.search(query)
        return {
            "project_id": capability.project_id,
            "topic": query.topic,
            "record_count": len(records),
            "records": records,
            "isolation_note": (
                "only this project's records and expressly shared global preferences"
                " are reachable"
            ),
        }

    def _knowledge_read(self, capability: Any, args: dict[str, Any]) -> dict[str, Any]:
        from .knowledge import KnowledgeService

        record_id = str(args.get("record_id", ""))
        if not record_id:
            raise ContractError("a record_id is required")
        row = KnowledgeService(self.store).record(record_id)
        body = json.loads(row["document"])
        if body["scope"] == "PROJECT" and body.get("project_id") != capability.project_id:
            raise AuthorizationError(
                "that knowledge record belongs to another project", record_id=record_id
            )
        return {
            "id": row["record_id"],
            "topic": body["topic"],
            "kind": body["kind"],
            "author": body["author"],
            "verification": body["verification"],
            "content": body["content"],
            "source_ref": body["source_ref"],
            "source_digest": body["source_digest"],
            "observed_at": body["observed_at"],
            "effective_at": body["effective_at"],
        }

    # ---------------------------------------------------------- operations
    def _own_operation(self, capability: Any, args: dict[str, Any]) -> dict[str, Any] | None:
        operation_id = str(args.get("operation_id", ""))
        if not operation_id:
            raise ContractError("an operation_id is required")
        row = self.store.operation(operation_id)
        if row["run_id"] != capability.run_id or row["job_id"] != capability.job_id:
            raise AuthorizationError(
                "the operation belongs to another run", operation_id=operation_id
            )
        return row

    def _operation_status(self, capability: Any, args: dict[str, Any]) -> dict[str, Any]:
        row = self._own_operation(capability, args)
        return {
            "operation_id": row["operation_id"],
            "action": row["action"],
            "status": row["status"],
            "result_digest": row["result_digest"],
            "error_code": row["error_code"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def _operation_result(self, capability: Any, args: dict[str, Any]) -> dict[str, Any]:
        row = self._own_operation(capability, args)
        return {
            "operation_id": row["operation_id"],
            "status": row["status"],
            "result_digest": row["result_digest"],
            "result_artifact": row["result_artifact"],
            "note": "a result is only complete when the status is SUCCEEDED",
        }

    def _operation_cancel(self, capability: Any, args: dict[str, Any]) -> dict[str, Any]:
        row = self._own_operation(capability, args)
        if row["status"] in {"SUCCEEDED", "FAILED", "OUTCOME_UNKNOWN"}:
            return {"operation_id": row["operation_id"], "status": row["status"],
                    "cancelled": False, "note": "the operation already settled"}
        self.store.finish_operation(
            row["operation_id"], "CANCELLED", owner_run=capability.run_id
        )
        return {"operation_id": row["operation_id"], "status": "CANCELLED",
                "cancelled": True}


def workspace_pytest_config(handle: WorkspaceHandle) -> Path:
    """An owned pytest config so a test child never inherits an outer project's.

    Without this, pytest walks up from the workspace and picks up whatever
    ``pyproject.toml`` happens to sit above it, which would make the profile's
    result depend on the host checkout rather than on the node's own tree.
    """
    path = handle.root / ".kvflow_pytest.ini"
    if not path.exists():
        path.write_text(
            "[pytest]\n"
            "addopts =\n"
            "cache_dir = .kvflow_pytest_cache\n",
            encoding="utf-8",
        )
    return path


def workspace_read_target(handle: WorkspaceHandle, scope: str) -> Path:
    """Read target inside the owned workspace; ``.`` means the workspace root."""
    clean = _safe_relative(scope)
    target = handle.root.joinpath(*clean.split("/")) if clean != "." else handle.root
    resolved = Path(os.path.abspath(str(target)))
    base = Path(os.path.abspath(str(handle.root)))
    inside = os.path.normcase(str(resolved)) == os.path.normcase(str(base)) or (
        os.path.normcase(str(resolved)).startswith(os.path.normcase(str(base)) + os.sep)
    )
    if not inside:
        raise PathDenied("read target escaped the workspace", scope=scope)
    return resolved


def workspace_write_target(handle: WorkspaceHandle, scope: str) -> Path:
    """Write target inside a node's declared scopes only."""
    clean = _safe_relative(scope)
    if not any(_contains(allowed, clean) for allowed in handle.scopes):
        raise AuthorizationError(
            "the node did not declare this write scope", scope=clean
        )
    target = handle.root.joinpath(*clean.split("/"))
    resolved = Path(os.path.abspath(str(target)))
    base = Path(os.path.abspath(str(handle.root)))
    if not os.path.normcase(str(resolved)).startswith(os.path.normcase(str(base)) + os.sep):
        raise PathDenied("write scope escaped the workspace", scope=clean)
    return resolved


def _contains(container: str, candidate: str) -> bool:
    if container == ".":
        return True
    container_parts = [p for p in container.replace("\\", "/").casefold().split("/") if p]
    candidate_parts = [p for p in candidate.replace("\\", "/").casefold().split("/") if p]
    if len(candidate_parts) < len(container_parts):
        return False
    return candidate_parts[: len(container_parts)] == container_parts


def _safe_relative(scope: str) -> str:
    """Validate a project-relative path used inside an owned workspace."""
    if not isinstance(scope, str) or not scope or len(scope) > 1024:
        raise PathDenied("invalid path")
    clean = scope.replace("\\", "/")
    if clean == ".":
        return "."
    if (
        clean.startswith("/")
        or clean.startswith("//")
        or (len(clean) > 1 and clean[1] == ":")
        or clean.startswith("\\\\")
    ):
        raise PathDenied("absolute and device paths are not accepted", scope=scope)
    parts = clean.split("/")
    for part in parts:
        if part in {"", ".", ".."}:
            raise PathDenied("traversal components are not accepted", scope=scope)
        if part[-1] in {" ", "."}:
            raise PathDenied("a component may not end with a space or dot", scope=scope)
        if any(ord(ch) < 32 or ch in '<>"|?*' for ch in part):
            raise PathDenied("illegal character in path", scope=scope)
        if part.split(".")[0].casefold() in {
            "con", "prn", "aux", "nul", "conin$", "conout$",
            *{f"{p}{i}" for p in ("com", "lpt") for i in "123456789"},
        }:
            raise PathDenied("reserved device name in path", scope=scope)
    return "/".join(parts)


def workspace_change_fingerprint(handle: WorkspaceHandle) -> str:
    entries = []
    for path in sorted(handle.root.rglob("*")):
        if path.is_dir() or path.name == "WORKSPACE.json":
            continue
        relative = PurePosixPath(*path.relative_to(handle.root).parts).as_posix()
        entries.append({"path": relative, "sha256": _sha256(path.read_bytes())})
    return content_digest(entries)


def workspace_scope_fingerprint(handle: WorkspaceHandle, scope: str) -> str:
    """Digest of every file under one declared scope, for frozen-input checks.

    A research spec declares the input scope it was frozen against; this digest
    is what proves the workspace still holds exactly that data.
    """
    target = workspace_read_target(handle, scope)
    entries: list[dict[str, str]] = []
    if target.is_file():
        entries.append({"path": _safe_relative(scope), "sha256": _sha256(target.read_bytes())})
    elif target.is_dir():
        for path in sorted(target.rglob("*")):
            if path.is_dir() or is_product_artifact(path.name):
                continue
            relative = PurePosixPath(*path.relative_to(handle.root).parts).as_posix()
            entries.append({"path": relative, "sha256": _sha256(path.read_bytes())})
    return content_digest({"scope": _safe_relative(scope) if scope else ".", "entries": entries})


def _scope_contains(container: str, candidate: str) -> bool:
    """Whether one declared relative scope contains another (or is it)."""
    if container in ("", "."):
        return True
    container_parts = [
        part for part in str(container).replace("\\", "/").casefold().split("/") if part
    ]
    candidate_parts = [
        part for part in str(candidate).replace("\\", "/").casefold().split("/") if part
    ]
    if len(candidate_parts) < len(container_parts):
        return False
    return candidate_parts[: len(container_parts)] == container_parts


def _reported_tests(stdout: str) -> int | None:
    match = re.search(r"(\d+) passed", stdout or "")
    if match:
        return int(match.group(1))
    match = re.search(r"Ran (\d+) test", stdout or "")
    return int(match.group(1)) if match else None


def _is_json(text: str) -> bool:
    try:
        json.loads(text)
    except ValueError:
        return False
    return True
