"""Workspace path containment and Orchestrator-issued capability tickets.

Two independent security surfaces live here, because they answer two different
questions:

``PathPolicy``
    "may this project-relative path be read/written at all?" It resolves the
    path against the *owned managed workspace* (never the registered source
    root), normalises Windows case and separators component-wise, rejects
    absolute/UNC/device/ADS/traversal/reserved-name input, and inspects every
    component — including every ancestor of the registered root — for reparse
    points. Anything it cannot inspect is denied (fail closed).

``CapabilityAuthority``
    "is this specific actor, on this specific run, at this specific ownership
    epoch, still allowed to perform this action?" The ticket is an opaque
    cryptographically random string; the database only stores its SHA-256 hash.
    Verification re-reads the complete project -> job -> node -> run -> current
    lease relation transactionally on *every* call, intersects the requested
    action with the immutable authorized allowlist, and rejects stale fences,
    expired leases and terminal/cancelled/paused runs. A model that writes
    ``role: manager`` into JSON gains nothing: role strings are not authority.
"""

from __future__ import annotations

import hashlib
import os
import secrets
from dataclasses import dataclass
from pathlib import Path, PureWindowsPath
from typing import Any, Iterable, Sequence

from .contracts import WINDOWS_RESERVED, Action, Project, Role
from .errors import (
    AuthorizationError,
    CapabilityDenied,
    ContractError,
    LeaseDenied,
    NotFoundError,
    PathDenied,
)
from .state import PAUSE_STATES, TERMINAL_STATES, parse_state
from .store import Store, utcnow

#: capability ticket lifetime ceiling; the lease must also still be valid
MAX_CAPABILITY_TTL_SECONDS = 900.0

_ADS_SEPARATOR = ":"
_DEVICE_PREFIXES = ("\\\\?\\", "\\\\.\\", "\\??\\", "\\Device\\")


def _is_reparse(path: Path) -> bool:
    try:
        stat = path.lstat()
    except OSError:
        return False
    return bool(getattr(stat, "st_file_attributes", 0) & 0x400) or path.is_symlink()


def _fold(value: str) -> str:
    normalized = value.replace("\\", "/")
    while "//" in normalized:
        normalized = normalized.replace("//", "/")
    return normalized.rstrip("/").casefold() or "/"


def _components(value: str) -> list[str]:
    return [c for c in _fold(value).split("/") if c]


class ReparseScanner:
    """Ancestry inspection that fails closed on any unverifiable link."""

    def __init__(self) -> None:
        self._cache: dict[str, None] = {}

    def inspect_ancestors(self, path: Path, *, up_to: Path | None = None) -> None:
        """Walk from the volume root down to ``path`` rejecting reparse points."""
        target = Path(os.path.abspath(str(path)))
        stop = Path(os.path.abspath(str(up_to))) if up_to else None
        chain: list[Path] = []
        current = target
        while True:
            chain.append(current)
            if stop is not None and os.path.normcase(str(current)) == os.path.normcase(str(stop)):
                break
            parent = current.parent
            if parent == current:
                break
            current = parent
        for candidate in reversed(chain):
            key = os.path.normcase(str(candidate))
            if key in self._cache:
                continue
            self._cache[key] = None
            if not candidate.exists():
                raise PathDenied("path component does not exist", component=str(candidate))
            if _is_reparse(candidate):
                raise PathDenied(
                    "refusing to traverse a symbolic link, junction or reparse point",
                    component=str(candidate),
                )

    def assert_no_links(self, path: Path) -> None:
        """Reject any component of ``path`` that already exists as a link."""
        current = Path(os.path.abspath(str(path)))
        parts: list[Path] = []
        while True:
            parts.append(current)
            parent = current.parent
            if parent == current:
                break
            current = parent
        for candidate in reversed(parts):
            if candidate.exists() and _is_reparse(candidate):
                raise PathDenied("path traverses a link", component=str(candidate))


@dataclass(frozen=True)
class ResolvedPath:
    scope: str
    absolute: Path
    relative_to_root: str
    operation: str


class PathPolicy:
    """Containment policy for one registered project."""

    def __init__(self, project: Project, scanner: ReparseScanner | None = None) -> None:
        self.project = project
        self.scanner = scanner or ReparseScanner()
        self.managed_root = Path(project.managed_root)
        self.source_root = Path(project.source_root)
        # fail closed if the registered roots themselves are links or sit under
        # an unverifiable link anywhere up to the volume root
        self.scanner.inspect_ancestors(self.managed_root)
        if os.path.normcase(str(self.managed_root)) == os.path.normcase(str(self.source_root)):
            raise AuthorizationError("managed and source roots must be distinct")

    # ------------------------------------------------------------ lexical
    @staticmethod
    def normalise(scope: str) -> str:
        if not isinstance(scope, str) or isinstance(scope, bool) or not scope:
            raise PathDenied("an empty path is not a project scope")
        if len(scope) > 2048:
            raise PathDenied("path is too long")
        raw = scope
        if raw.startswith(_DEVICE_PREFIXES) or raw.startswith("\\\\") or raw.startswith("//"):
            raise PathDenied("UNC and device paths are not project scopes", scope=raw)
        if len(raw) > 1 and raw[1] == _ADS_SEPARATOR:
            raise PathDenied("drive-qualified paths are not project scopes", scope=raw)
        candidate = PureWindowsPath(raw)
        if candidate.is_absolute() or candidate.drive:
            raise PathDenied("absolute paths are not project scopes", scope=raw)
        if candidate.root:
            raise PathDenied("rooted paths are not project scopes", scope=raw)
        cleaned = raw.replace("\\", "/")
        if cleaned in {".", "./"}:
            return "."
        parts = cleaned.split("/")
        for part in parts:
            if part in {"", ".", ".."}:
                raise PathDenied("traversal components are not allowed", scope=raw)
            if _ADS_SEPARATOR in part:
                raise PathDenied(
                    "alternate data stream separators are not allowed", scope=raw, component=part
                )
            if part[-1] in {" ", "."}:
                raise PathDenied(
                    "a Windows path component may not end with a space or dot",
                    scope=raw,
                    component=part,
                )
            if any(ord(ch) < 32 or ch in '<>"|?*' for ch in part):
                raise PathDenied("illegal character in path component", scope=raw, component=part)
            stem = part.split(".")[0].casefold()
            if stem in WINDOWS_RESERVED:
                raise PathDenied("reserved device name in path component", scope=raw, component=part)
        return "/".join(parts)

    # ------------------------------------------------------------ policies
    def _rules(self, operation: str) -> tuple[Sequence[str], Sequence[str]]:
        if operation in {"write", "patch", "commit"}:
            return self.project.allowed_write_roots, self.project.protected_roots
        if operation in {"read", "search", "list", "status", "diff"}:
            return self.project.allowed_read_roots, self.project.protected_roots
        raise ContractError("unknown path operation", operation=operation)

    @staticmethod
    def _contains(container: str, candidate: str) -> bool:
        """Component-wise containment; never a raw ``startswith``."""
        if container == ".":
            return True
        container_parts = _components(container)
        candidate_parts = _components(candidate)
        if len(candidate_parts) < len(container_parts):
            return False
        return candidate_parts[: len(container_parts)] == container_parts

    def _select_root(self, scope: str, roots: Sequence[str]) -> str:
        matches = [root for root in roots if self._contains(root, scope)]
        if not matches:
            raise PathDenied(
                "path is outside every allowed root for this operation",
                scope=scope,
                roots=list(roots),
            )
        # most specific allowed root wins so a scoped write root keeps its prefix
        return max(matches, key=lambda r: len(_components(r)))

    def resolve(self, scope: str, *, operation: str) -> ResolvedPath:
        normalised = self.normalise(scope)
        if normalised == ".":
            raise PathDenied("the project root itself is not a writable/readable target")
        roots, protected = self._rules(operation)
        selected = self._select_root(normalised, roots)
        for guard in protected:
            if self._contains(guard, normalised):
                raise PathDenied(
                    "path is inside a protected root",
                    scope=normalised,
                    protected=guard,
                )
        self.scanner.assert_no_links(self.managed_root)
        absolute = self.managed_root.joinpath(*normalised.split("/"))
        # prove the resolved candidate is still the intended path: reject links
        # inside the tree and confirm containment after canonicalisation
        self.scanner.assert_no_links(absolute)
        canonical = Path(os.path.abspath(str(absolute)))
        if not self._contains(
            os.path.normcase(str(self.managed_root)).replace("\\", "/"),
            os.path.normcase(str(canonical)).replace("\\", "/"),
        ):
            raise PathDenied("resolved path escaped the managed root", scope=normalised)
        if os.path.normcase(str(self.managed_root)) == os.path.normcase(
            os.path.normpath(str(absolute))
        ):
            raise PathDenied("resolved path collapsed onto the managed root", scope=normalised)
        return ResolvedPath(
            scope=normalised,
            absolute=canonical,
            relative_to_root=selected,
            operation=operation,
        )

    def assert_writable(self, scope: str) -> ResolvedPath:
        return self.resolve(scope, operation="write")

    def assert_readable(self, scope: str) -> ResolvedPath:
        return self.resolve(scope, operation="read")

    def within_source_root(self, path: str) -> bool:
        return self._contains(
            os.path.normcase(str(self.source_root)).replace("\\", "/"),
            os.path.normcase(os.path.abspath(path)).replace("\\", "/"),
        )

    def describe(self) -> dict[str, Any]:
        return {
            "project_id": self.project.id,
            "managed_root": str(self.managed_root),
            "source_root": str(self.source_root),
            "allowed_read_roots": list(self.project.allowed_read_roots),
            "allowed_write_roots": list(self.project.allowed_write_roots),
            "protected_roots": list(self.project.protected_roots),
        }


# --------------------------------------------------------------- capability


def hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class Capability:
    """What the bearer of a ticket is allowed to do, as verified from the DB."""

    project_id: str
    job_id: str
    node_id: str
    run_id: str
    attempt: int
    fence: int
    role: str
    lease_id: str
    actions: frozenset[str]
    expires_at: float
    token_hash: str

    def may(self, action: str | Action) -> bool:
        value = action.value if isinstance(action, Action) else str(action)
        return value in self.actions


class CapabilityAuthority:
    """Issues and verifies opaque capability tickets."""

    def __init__(self, store: Store) -> None:
        self.store = store

    # ------------------------------------------------------------ issuance
    def issue(
        self,
        *,
        project_id: str,
        job_id: str,
        node_id: str,
        run_id: str,
        lease_id: str,
        role: str,
        actions: Iterable[str | Action],
        ttl_seconds: float = 300.0,
    ) -> tuple[str, Capability]:
        if role not in {r.value for r in Role}:
            raise CapabilityDenied("unknown role", role=role)
        if role == Role.ORCHESTRATOR.value:
            raise CapabilityDenied("models never receive the Orchestrator role", role=role)
        if ttl_seconds <= 0 or ttl_seconds > MAX_CAPABILITY_TTL_SECONDS:
            raise ContractError("capability ttl is out of range", ttl_seconds=ttl_seconds)
        requested = frozenset(
            a.value if isinstance(a, Action) else str(a) for a in actions
        )
        if not requested:
            raise CapabilityDenied("a capability must allow at least one action")
        token = secrets.token_urlsafe(32)
        token_hash = hash_token(token)
        with self.store.tx() as conn:
            run = conn.execute(
                "SELECT r.*, j.project_id AS job_project, j.state AS job_state,"
                " n.state AS node_state, n.allowed_tools AS node_tools,"
                " p.document AS project_document"
                " FROM runs r JOIN jobs j ON j.job_id = r.job_id"
                " JOIN nodes n ON n.job_id = r.job_id AND n.node_id = r.node_id"
                " JOIN projects p ON p.project_id = j.project_id"
                " WHERE r.run_id = ?",
                (run_id,),
            ).fetchone()
            if run is None:
                raise NotFoundError("unknown run", run_id=run_id)
            if run["job_id"] != job_id or run["node_id"] != node_id:
                raise CapabilityDenied(
                    "run does not belong to the claimed job/node", run_id=run_id
                )
            if run["job_project"] != project_id:
                raise CapabilityDenied(
                    "run belongs to another project",
                    claimed_project=project_id,
                    actual_project=run["job_project"],
                )
            lease = conn.execute(
                "SELECT * FROM leases WHERE lease_id = ? AND run_id = ?",
                (lease_id, run_id),
            ).fetchone()
            if lease is None:
                raise LeaseDenied("lease does not belong to this run", lease_id=lease_id)
            fence = int(lease["fence"])
            self._assert_current(conn, run, lease, fence)
            import json as _json

            node_tools = frozenset(_json.loads(run["node_tools"]))
            try:
                project = Project.model_validate(_json.loads(run["project_document"]))
            except Exception as exc:  # pragma: no cover - corrupt registration
                raise AuthorizationError("project registration is unreadable") from exc
            project_tools = frozenset(a.value for a in project.allowed_tools)
            allowed = requested & project_tools & node_tools
            if allowed != requested:
                raise CapabilityDenied(
                    "requested actions exceed the immutable authorized allowlist",
                    denied=sorted(requested - allowed),
                )
            expires = min(
                self.store.clock.now() + float(ttl_seconds), float(lease["expires_at"])
            )
            conn.execute(
                "INSERT INTO capabilities(token_hash, project_id, job_id, node_id, run_id,"
                " attempt, role, fence, lease_id, actions, issued_at, expires_at)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    token_hash,
                    project_id,
                    job_id,
                    node_id,
                    run_id,
                    int(run["attempt"]),
                    role,
                    fence,
                    lease_id,
                    _json.dumps(sorted(allowed)),
                    utcnow().isoformat(),
                    expires,
                ),
            )
        capability = Capability(
            project_id=project_id,
            job_id=job_id,
            node_id=node_id,
            run_id=run_id,
            attempt=int(run["attempt"]),
            fence=fence,
            role=role,
            lease_id=lease_id,
            actions=allowed,
            expires_at=expires,
            token_hash=token_hash,
        )
        return token, capability

    # ---------------------------------------------------------- verification
    def _assert_current(self, conn, run, lease, fence: int) -> None:
        if lease["status"] != "ACTIVE":
            raise LeaseDenied("lease is not active", lease_id=lease["lease_id"])
        if int(lease["fence"]) != int(fence):
            raise LeaseDenied("lease fence does not match", lease_id=lease["lease_id"])
        if self.store.clock.now() > float(lease["expires_at"]):
            raise LeaseDenied("lease has expired", lease_id=lease["lease_id"])
        current = conn.execute(
            "SELECT lease_id, fence FROM leases WHERE job_id = ? AND node_id = ?"
            " AND status = 'ACTIVE' ORDER BY fence DESC LIMIT 1",
            (lease["job_id"], lease["node_id"]),
        ).fetchone()
        if current is None or current["lease_id"] != lease["lease_id"]:
            raise LeaseDenied(
                "a newer ownership epoch exists for this node", lease_id=lease["lease_id"]
            )
        if int(current["fence"]) != int(fence):
            raise LeaseDenied("current ownership epoch has advanced", fence=fence)
        job_state = parse_state(run["job_state"])
        if job_state in TERMINAL_STATES or job_state in PAUSE_STATES:
            raise CapabilityDenied("job is not accepting work", job_state=job_state.value)
        node_state = parse_state(run["node_state"])
        if node_state in TERMINAL_STATES or node_state in PAUSE_STATES:
            raise CapabilityDenied("node is not accepting work", node_state=node_state.value)
        if run["status"] != "OPEN":
            raise CapabilityDenied("run is closed", run_status=run["status"])

    def verify(
        self, token: str, action: str | Action, *,
        project_id: str | None = None,
        job_id: str | None = None,
        node_id: str | None = None,
        run_id: str | None = None,
        path: str | None = None,
    ) -> Capability:
        """Re-read the whole relation on every call. Nothing is cached."""
        if not token or not isinstance(token, str):
            raise CapabilityDenied("a capability ticket is required")
        token_hash = hash_token(token)
        value = action.value if isinstance(action, Action) else str(action)
        import json as _json

        with self.store.read() as conn:
            row = conn.execute(
                "SELECT * FROM capabilities WHERE token_hash = ?", (token_hash,)
            ).fetchone()
            if row is None:
                raise CapabilityDenied("unknown capability ticket")
            if row["revoked_at"] is not None:
                raise CapabilityDenied("capability has been revoked")
            if self.store.clock.now() > float(row["expires_at"]):
                raise CapabilityDenied("capability has expired")
            if project_id is not None and row["project_id"] != project_id:
                raise CapabilityDenied("capability belongs to another project", project_id=project_id)
            if job_id is not None and row["job_id"] != job_id:
                raise CapabilityDenied("capability belongs to another job", job_id=job_id)
            if node_id is not None and row["node_id"] != node_id:
                raise CapabilityDenied("capability belongs to another node", node_id=node_id)
            if run_id is not None and row["run_id"] != run_id:
                raise CapabilityDenied("capability belongs to another run", run_id=run_id)
            actions = frozenset(_json.loads(row["actions"]))
            if value not in actions:
                raise CapabilityDenied("action is not allowed by this capability", action=value)
            run = conn.execute(
                "SELECT r.*, j.project_id AS job_project, j.state AS job_state,"
                " n.state AS node_state, n.allowed_tools AS node_tools"
                " FROM runs r JOIN jobs j ON j.job_id = r.job_id"
                " JOIN nodes n ON n.job_id = r.job_id AND n.node_id = r.node_id"
                " WHERE r.run_id = ?",
                (row["run_id"],),
            ).fetchone()
            if run is None:
                raise CapabilityDenied("capability references an unknown run")
            if run["job_project"] != row["project_id"]:
                raise CapabilityDenied("capability project no longer matches its job")
            lease = conn.execute(
                "SELECT * FROM leases WHERE lease_id = ?", (row["lease_id"],)
            ).fetchone()
            if lease is None:
                raise LeaseDenied("capability references an unknown lease")
            self._assert_current(conn, run, lease, int(row["fence"]))
            node_tools = frozenset(_json.loads(run["node_tools"]))
            if value not in node_tools:
                raise CapabilityDenied(
                    "node allowlist no longer includes this action", action=value
                )
            if path is not None:
                policy = PathPolicy(Project.model_validate(
                    _json.loads(
                        conn.execute(
                            "SELECT document FROM projects WHERE project_id = ?",
                            (row["project_id"],),
                        ).fetchone()["document"]
                    )
                ))
                operation = "write" if value.startswith("repo.patch") or value.startswith(
                    "repo.commit"
                ) else "read"
                policy.resolve(path, operation=operation)
        return Capability(
            project_id=row["project_id"],
            job_id=row["job_id"],
            node_id=row["node_id"],
            run_id=row["run_id"],
            attempt=int(row["attempt"]),
            fence=int(row["fence"]),
            role=row["role"],
            lease_id=row["lease_id"],
            actions=actions,
            expires_at=float(row["expires_at"]),
            token_hash=token_hash,
        )

    def revoke(self, token_hash: str, *, reason: str) -> None:
        with self.store.tx() as conn:
            cur = conn.execute(
                "UPDATE capabilities SET revoked_at = ? WHERE token_hash = ? AND revoked_at IS NULL",
                (utcnow().isoformat(), token_hash),
            )
            if cur.rowcount != 1:
                raise CapabilityDenied(
                    "capability is unknown or already revoked", token_hash=token_hash
                )

    def revoke_for_run(self, run_id: str, *, reason: str) -> int:
        with self.store.tx() as conn:
            cur = conn.execute(
                "UPDATE capabilities SET revoked_at = ? WHERE run_id = ? AND revoked_at IS NULL",
                (utcnow().isoformat(), run_id),
            )
        return int(cur.rowcount)

    def active_capabilities(self, job_id: str) -> list[dict[str, Any]]:
        with self.store.read() as conn:
            rows = conn.execute(
                "SELECT token_hash, project_id, job_id, node_id, run_id, attempt, role, fence,"
                " lease_id, actions, issued_at, expires_at, revoked_at FROM capabilities"
                " WHERE job_id = ? ORDER BY issued_at",
                (job_id,),
            ).fetchall()
        return [dict(r) for r in rows]


def assert_scope_intersection(
    declared: Sequence[str], authorized: Sequence[str]
) -> None:
    """Every declared node scope must sit inside an authorized root."""
    from .contracts import relative_scope

    normalized_authorized = [relative_scope(r) for r in authorized]
    for scope in declared:
        clean = relative_scope(scope)
        if not any(PathPolicy._contains(root, clean) for root in normalized_authorized):
            raise AuthorizationError(
                "declared scope exceeds the authorized roots", scope=clean
            )
