"""Bounded, reusable transport for the official MCP stdio client.

This module is a deliberately *independent* slice of Agent OS v1: it imports
nothing from the unfinished F1 foundation, the scheduler, the store or any
v0.1 module. It wraps the official ``mcp`` SDK (``mcp==1.30.0``) so a trusted
Orchestrator can run one local, stdio-bound MCP server and invoke an explicit
allowlist of discovered tools.

Design rules enforced here
--------------------------

* **Trusted configuration only.** :class:`MCPServerConfig` is produced by the
  Orchestrator, never by model output. The command must resolve to the caller's
  own Python executable (or an explicitly trusted entrypoint), so a model can
  never choose what gets launched and no URL/HTTP transport exists.
* **Minimal, explicit environment.** The child receives a fixed allowlist of
  process variables, UTF-8 settings and the Orchestrator-supplied server
  bootstrap fields only. Credentials, proxy variables and unrelated user
  variables are never forwarded. The opaque capability is server-bound and
  never appears in public representations, schemas, results or audit events.
* **Bounded everything.** Startup, discovery, call, read and cleanup are all
  bounded; serialized arguments, discovery payloads and results are size
  limited before they can be trusted or returned.
* **Local validation.** Discovered JSON schemas are checked with ``jsonschema``.
  Call arguments are validated *before* dispatch and declared structured output
  is validated before it is returned. Rejected calls are never dispatched, so
  they cannot produce side effects.
* **Honest outcomes.** Timeout, cancellation, disconnect, tool ``isError``,
  invalid arguments, invalid results and oversized payloads are separate
  classifications. A dispatched call whose effects cannot be established keeps
  :attr:`Outcome.UNKNOWN`; nothing is retried automatically and exactly-once is
  never claimed.

Cancellation-safety note: the SDK contexts are entered and exited by the same
asyncio task. Always use the connection as an ``async with`` block (or call
:meth:`MCPConnection.start` and :meth:`MCPConnection.aclose` from one task) so
no ``anyio`` cancel scope is ever exited from a foreign task.
"""

from __future__ import annotations

import asyncio
import hashlib
import importlib.metadata
import json
import os
import re
import sys
import tempfile
import threading
import time
from contextlib import AsyncExitStack
from dataclasses import dataclass, field
from datetime import timedelta
from enum import Enum
from types import MappingProxyType
from typing import Any, Callable, Mapping

import jsonschema

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

try:  # pragma: no cover - anyio is a hard dependency of the official SDK
    import anyio
except Exception:  # pragma: no cover
    anyio = None  # type: ignore[assignment]

try:  # pragma: no cover - kept optional so a rename cannot break the module
    from mcp import McpError
except Exception:  # pragma: no cover
    McpError = None  # type: ignore[assignment]


__all__ = [
    "DEFAULT_CAPABILITY_ENV",
    "MCPCancelledError",
    "MCPConfigurationError",
    "MCPConnection",
    "MCPDisallowedToolError",
    "MCPDisconnectedError",
    "MCPInvalidArgumentsError",
    "MCPInvalidResultError",
    "MCPLaunchError",
    "MCPOversizedError",
    "MCPProtocolError",
    "MCPServerConfig",
    "MCPTimeoutError",
    "MCPToolError",
    "MCPTransportError",
    "Outcome",
    "ToolCallResult",
    "ToolDefinition",
    "classify_exception",
]


#: Environment variable name the opaque capability is delivered under.
DEFAULT_CAPABILITY_ENV = "KVFLOW_MCP_CAPABILITY"

_ENV_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

#: The complete set of host variables forwarded to the MCP child process.
#: Nothing here is a credential, a proxy or a user preference.
_ENV_ALLOW: tuple[str, ...] = (
    "PATH",
    "PATHEXT",
    "SYSTEMROOT",
    "SYSTEMDRIVE",
    "WINDIR",
    "COMSPEC",
    "TEMP",
    "TMP",
    "TMPDIR",
    "NUMBER_OF_PROCESSORS",
    "PROCESSOR_ARCHITECTURE",
)

#: The official SDK additionally merges its own safe-inheritance list into the
#: child environment (``mcp.client.stdio.get_default_environment``). Those names
#: are documented here on purpose: the point of the boundary is that the *actual*
#: spawned child matches a reviewed list, not that the list looks short.
_SDK_INHERITED_ENV: tuple[str, ...] = (
    "APPDATA",
    "HOMEDRIVE",
    "HOMEPATH",
    "LOCALAPPDATA",
    "PATH",
    "PATHEXT",
    "PROCESSOR_ARCHITECTURE",
    "SYSTEMDRIVE",
    "SYSTEMROOT",
    "TEMP",
    "USERNAME",
    "USERPROFILE",
)

#: Names a trusted Orchestrator still may not inject: credentials, proxies,
#: billing and model configuration belong to the transport process, never to a
#: tool server.
_FORBIDDEN_ENV_TOKENS: tuple[str, ...] = (
    "KEY",
    "TOKEN",
    "SECRET",
    "PASSWORD",
    "PASSWD",
    "CREDENTIAL",
    "AUTH",
    "PROXY",
    "COOKIE",
    "BILLING",
    "BEARER",
    "PRIVATE",
)

#: Exact-name allowlist for what the Orchestrator may add. Anything else is
#: refused even though the configuration is trusted.
_ENV_OVERRIDE_ALLOW: tuple[str, ...] = (
    "PYTHONIOENCODING",
    "PYTHONUTF8",
    "PYTHONUNBUFFERED",
    "PYTHONDONTWRITEBYTECODE",
    "KVFLOW_PROFILE",
    "KVFLOW_SERVER_NAME",
)

#: Agent-OS-owned prefixes the Orchestrator may use for server bootstrap
#: settings. They are still refused when the name looks like a credential.
_ENV_OVERRIDE_PREFIXES: tuple[str, ...] = ("KVFLOW_",)

def _env_name_offence(name: str) -> str | None:
    """Return why an injected environment name is refused, or ``None``.

    Windows environment names are case-insensitive, so every comparison here is
    case-folded: ``path`` cannot be used to slip past a ``PATH`` rule.
    """
    upper = name.upper()
    for token in _FORBIDDEN_ENV_TOKENS:
        if token in upper:
            return f"{name!r} looks like a credential, proxy or billing variable"
    if upper in {n.upper() for n in _SDK_INHERITED_ENV}:
        return f"{name!r} would override a host variable the SDK already inherits"
    if upper in {n.upper() for n in _ENV_ALLOW}:
        return f"{name!r} would override a required process variable"
    allowed = upper in {n.upper() for n in _ENV_OVERRIDE_ALLOW}
    allowed = allowed or any(upper.startswith(p) for p in _ENV_OVERRIDE_PREFIXES)
    if not allowed:
        return f"{name!r} is not on the approved server bootstrap allowlist"
    return None


def _capability_name_offence(name: str) -> str | None:
    """The capability variable must not impersonate a credential name."""
    upper = name.upper()
    for token in _FORBIDDEN_ENV_TOKENS:
        if token in upper:
            return f"{name!r} looks like a credential, proxy or billing variable"
    if upper in {n.upper() for n in _SDK_INHERITED_ENV}:
        return f"{name!r} would collide with a host variable the SDK inherits"
    if upper in {n.upper() for n in _ENV_ALLOW}:
        return f"{name!r} would collide with a required process variable"
    return None


def _schema_external_reference(node: Any, path: str = "$") -> str | None:
    """Find the first ``$ref``/``$dynamicRef`` that is not a local pointer.

    A remote or file reference must never be dereferenced during discovery or
    argument validation: that would turn a normalised schema into an outbound
    request (and a bypass of the call timeout). Local ``#/$defs/...`` references
    stay valid.
    """
    if isinstance(node, dict):
        for keyword in ("$ref", "$dynamicRef", "$recursiveRef"):
            target = node.get(keyword)
            if isinstance(target, str) and not target.startswith("#"):
                return f"{path}/{keyword}={target[:120]}"
        for key, value in node.items():
            found = _schema_external_reference(value, f"{path}/{key}")
            if found:
                return found
    elif isinstance(node, list):
        for index, value in enumerate(node):
            found = _schema_external_reference(value, f"{path}/{index}")
            if found:
                return found
    return None


def _no_fetch_registry() -> Any:
    """A resolution registry that has nothing to fetch.

    ``jsonschema``'s default ``referencing`` registry resolves remote URIs by
    retrieving them. Registering an explicitly empty registry instead means an
    unresolved reference raises locally rather than performing I/O.
    """
    try:
        from referencing import Registry
    except Exception:  # pragma: no cover - installed jsonschema always ships it
        return None
    return Registry()


def _validator(
    schema: Mapping[str, Any] | None,
) -> "jsonschema.Draft202012Validator | None":
    """Build a draft-2020-12 validator that can never fetch anything."""
    if not isinstance(schema, dict):
        return None
    external = _schema_external_reference(schema)
    if external is not None:
        raise MCPProtocolError(
            f"schema uses a non-local reference, which is refused: {external}"
        )
    registry = _no_fetch_registry()
    if registry is None:  # pragma: no cover - defensive
        return jsonschema.Draft202012Validator(schema)
    return jsonschema.Draft202012Validator(schema, registry=registry)


def _tool_stub(tool: Any) -> dict[str, Any]:
    """Best-effort complete view of one advertised tool for byte accounting."""
    dump = getattr(tool, "model_dump", None)
    if callable(dump):
        try:
            data = dump(mode="json", by_alias=True, exclude_none=True)
            if isinstance(data, dict):
                return data
        except Exception:  # noqa: BLE001 - fall back to field access
            pass
    fields = (
        "name",
        "title",
        "description",
        "inputSchema",
        "input_schema",
        "outputSchema",
        "output_schema",
        "annotations",
        "icons",
        "_meta",
    )
    stub: dict[str, Any] = {}
    for field_name in fields:
        value = _field(tool, field_name, default=None)
        if value is not None:
            stub[field_name] = value
    return stub


_DISCONNECT_NAMES = ("BrokenResourceError", "ClosedResourceError", "EndOfStream")

_DISCONNECT_TYPES: tuple[type[BaseException], ...] = (
    ConnectionError,
    BrokenPipeError,
    EOFError,
)


class Outcome(str, Enum):
    """What the transport can honestly say happened to a call."""

    SUCCESS = "SUCCESS"
    ERROR = "ERROR"
    UNKNOWN = "OUTCOME_UNKNOWN"
    NOT_DISPATCHED = "NOT_DISPATCHED"


class MCPTransportError(Exception):
    """Base class for every failure the transport surfaces.

    ``kind`` is a stable, machine-readable classification. ``outcome`` states
    whether the call was never dispatched, completed, or left the effective
    result unestablished.
    """

    kind = "transport"
    default_outcome = Outcome.UNKNOWN

    def __init__(
        self,
        message: str,
        *,
        kind: str | None = None,
        outcome: Outcome | None = None,
    ) -> None:
        super().__init__(message)
        if kind is not None:
            self.kind = kind
        self.outcome: Outcome = outcome if outcome is not None else self.default_outcome


class MCPConfigurationError(MCPTransportError):
    """The Orchestrator supplied configuration the transport refuses."""

    kind = "configuration"
    default_outcome = Outcome.NOT_DISPATCHED


class MCPLaunchError(MCPConfigurationError):
    """The requested server command is not an approved launch target."""

    kind = "launch"


class MCPProtocolError(MCPTransportError):
    """Handshake, discovery or JSON-RPC level failure."""

    kind = "protocol"
    default_outcome = Outcome.NOT_DISPATCHED


class MCPDisconnectedError(MCPTransportError):
    """The server process or its stream went away mid-flight."""

    kind = "disconnect"


class MCPTimeoutError(MCPTransportError):
    """A bounded wait expired before the server produced an answer."""

    kind = "timeout"


class MCPCancelledError(MCPTransportError):
    """A caller cancellation was observed during a dispatched call."""

    kind = "cancel"


class MCPToolError(MCPTransportError):
    """The server answered a call with ``isError`` set."""

    kind = "tool_error"
    default_outcome = Outcome.ERROR


class MCPInvalidArgumentsError(MCPTransportError):
    """Arguments failed local schema validation and were not dispatched."""

    kind = "invalid_arguments"
    default_outcome = Outcome.NOT_DISPATCHED


class MCPDisallowedToolError(MCPTransportError):
    """The named tool is not in the exact discovered allowlist."""

    kind = "disallowed_tool"
    default_outcome = Outcome.NOT_DISPATCHED


class MCPOversizedError(MCPTransportError):
    """A payload crossed a configured byte bound."""

    kind = "oversized"
    default_outcome = Outcome.NOT_DISPATCHED


class MCPInvalidResultError(MCPTransportError):
    """A completed result did not match its declared output schema."""

    kind = "invalid_result"


def classify_exception(exc: BaseException) -> MCPTransportError:
    """Map an SDK/OS exception onto one of the transport's honest classes."""
    if isinstance(exc, MCPTransportError):
        return exc
    if McpError is not None and isinstance(exc, McpError):
        return MCPProtocolError(
            "the server returned a JSON-RPC error for the request",
            outcome=Outcome.UNKNOWN,
        )
    if anyio is not None:
        for name in _DISCONNECT_NAMES:
            candidate = getattr(anyio, name, None)
            if candidate is not None and isinstance(exc, candidate):
                return MCPDisconnectedError("the MCP stream was closed by the peer")
    if type(exc).__name__ in _DISCONNECT_NAMES:
        return MCPDisconnectedError("the MCP stream was closed by the peer")
    if isinstance(exc, TimeoutError):
        return MCPTimeoutError("the MCP server did not answer in time")
    if isinstance(exc, _DISCONNECT_TYPES):
        return MCPDisconnectedError("the MCP server connection failed")
    return MCPTransportError(f"the MCP call failed ({type(exc).__name__})")


def _field(obj: Any, *names: str, default: Any = None) -> Any:
    """Read the first present attribute, tolerating SDK field-name drift."""
    for name in names:
        if hasattr(obj, name):
            return getattr(obj, name)
    return default


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _canonical(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=str,
    )


def _byte_len(text: str) -> int:
    return len(text.encode("utf-8"))


def _sdk_version() -> str:
    try:
        return importlib.metadata.version("mcp")
    except Exception:  # pragma: no cover - defensive
        return "unknown"


def _collect_text(content: Any) -> tuple[str | None, dict[str, int]]:
    """Collect text from supported content blocks and count unsupported kinds.

    Dropping a block the transport cannot represent would report a successful
    empty result for content the server never produced, so unsupported kinds are
    counted and surfaced instead of silently discarded.
    """
    parts: list[str] = []
    unsupported: dict[str, int] = {}
    for block in content or ():
        if isinstance(block, str):
            parts.append(block)
            continue
        kind = _field(block, "type", default=None)
        if kind in (None, "text"):
            text = _field(block, "text")
            if isinstance(text, str):
                parts.append(text)
                continue
        if kind == "resource":
            resource = _field(block, "resource", default=None)
            text = _field(resource, "text", default=None)
            if isinstance(text, str):
                parts.append(text)
                continue
        name = str(kind) if kind is not None else type(block).__name__
        unsupported[name] = unsupported.get(name, 0) + 1
    return ("\n".join(parts) if parts else None), unsupported


class _BoundedLog:
    """A private stderr channel with a hard byte cap.

    Server stderr is private diagnostics, never a caller-visible channel, but
    privacy is not a storage budget: an uncapped temporary file lets a noisy or
    hostile server fill the disk. The child writes into an OS pipe; this object
    (and its drain thread) keeps only the first ``limit`` bytes and discards the
    rest, so nothing downstream can grow past the cap regardless of how much the
    server emits.
    """

    def __init__(self, limit: int) -> None:
        self._limit = max(0, int(limit))
        self._file = tempfile.TemporaryFile(mode="w+b")
        self._read_fd, self._write_fd = os.pipe()
        self._written = 0
        self._discarded = 0
        self._stored = 0
        self.closed = False
        self._drain = threading.Thread(target=self._drain_pipe, daemon=True)
        self._drain.start()

    # ----------------------------------------------------------- writer side
    @property
    def write_fd(self) -> int:
        """The descriptor handed to the child process for its stderr."""
        return self._write_fd

    def fileno(self) -> int:
        return self._write_fd

    def write(self, data: Any) -> int:
        payload = data.encode("utf-8") if isinstance(data, str) else bytes(data or b"")
        try:
            return os.write(self._write_fd, payload)
        except OSError:  # pragma: no cover - the drain thread owns the read end
            return len(payload)

    def flush(self) -> None:  # pragma: no cover - nothing buffered here
        return None

    # ------------------------------------------------------------ drain side
    def _drain_pipe(self) -> None:
        while True:
            try:
                chunk = os.read(self._read_fd, 65536)
            except OSError:  # pragma: no cover - descriptor closed
                break
            if not chunk:
                break
            self._written += len(chunk)
            room = self._limit - self._stored
            if room <= 0:
                self._discarded += len(chunk)
                continue
            keep = chunk[:room]
            self._discarded += len(chunk) - len(keep)
            try:
                self._file.write(keep)
                self._file.flush()
            except Exception:  # noqa: BLE001 - diagnostics must never raise
                self._discarded += len(keep)
                continue
            self._stored += len(keep)

    # -------------------------------------------------------------- reports
    @property
    def bytes_written(self) -> int:
        return self._written

    @property
    def bytes_discarded(self) -> int:
        return self._discarded

    @property
    def bytes_stored(self) -> int:
        return self._stored

    def tell(self) -> int:
        return self._stored

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        for fd in (self._write_fd, self._read_fd):
            try:
                os.close(fd)
            except OSError:  # pragma: no cover - already closed
                pass
        self._drain.join(timeout=2.0)
        try:
            self._file.close()
        except Exception:  # noqa: BLE001
            pass

    def __getattr__(self, item: str) -> Any:  # pragma: no cover - passthrough
        if item.startswith("_"):
            raise AttributeError(item)
        return getattr(self._file, item)


@dataclass(frozen=True)
class ToolDefinition:
    """One locally validated tool discovered from the server."""

    name: str
    description: str
    input_schema: Mapping[str, Any]
    output_schema: Mapping[str, Any] | None
    input_schema_hash: str

    def function_tool(self) -> dict[str, Any]:
        """Return the DeepSeek/completions function-tool representation."""
        return {
            "type": "function",
            "function": {
                "name": self.name,
                "description": self.description,
                "parameters": json.loads(json.dumps(self.input_schema, default=str)),
            },
        }


@dataclass(frozen=True)
class ToolCallResult:
    """A bounded, structured result for one successful tool call."""

    tool: str
    outcome: Outcome
    text: str | None
    structured: Any | None
    input_hash: str
    output_hash: str
    duration_ms: float
    protocol_version: str | None
    sdk_version: str


@dataclass(frozen=True, repr=False)
class MCPServerConfig:
    """Immutable launch and policy configuration.

    Every field is supplied by the trusted Orchestrator. ``capability`` is an
    opaque token delivered to the server through ``capability_env_name`` only;
    it is never returned, logged, hashed or included in any public string.
    """

    name: str
    args: tuple[str, ...] = ()
    cwd: str | None = None
    command: str | None = None
    trusted_entrypoints: tuple[str, ...] = ()
    server_env: Mapping[str, str] = field(default_factory=dict)
    capability: str | None = None
    capability_env_name: str = DEFAULT_CAPABILITY_ENV
    allowed_tools: tuple[str, ...] = ()
    max_input_bytes: int = 262_144
    max_output_bytes: int = 1_048_576
    max_discovery_bytes: int = 1_048_576
    max_tools: int = 128
    max_stderr_bytes: int = 65_536
    startup_timeout_s: float = 15.0
    call_timeout_s: float = 30.0
    read_timeout_s: float = 60.0
    cleanup_timeout_s: float = 5.0

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not self.name:
            raise MCPConfigurationError("an MCP server name is required")
        if not self.allowed_tools:
            raise MCPConfigurationError("an explicit tool allowlist is required")
        if not _ENV_NAME_RE.match(self.capability_env_name):
            raise MCPConfigurationError(
                "capability_env_name must be a valid environment variable name"
            )
        capability_offence = _capability_name_offence(self.capability_env_name)
        if capability_offence is not None:
            raise MCPConfigurationError(
                "capability_env_name would impersonate a protected variable: "
                f"{capability_offence}"
            )
        for key in self.server_env:
            if not _ENV_NAME_RE.match(key):
                raise MCPConfigurationError(
                    f"invalid environment variable name: {key!r}"
                )
            offence = _env_name_offence(key)
            if offence is not None:
                raise MCPConfigurationError(
                    f"refusing to inject an environment variable: {offence}"
                )
        for attr in (
            "max_input_bytes",
            "max_output_bytes",
            "max_discovery_bytes",
            "max_tools",
            "max_stderr_bytes",
        ):
            if int(getattr(self, attr)) <= 0:
                raise MCPConfigurationError(f"{attr} must be positive")
        for attr in ("startup_timeout_s", "call_timeout_s", "read_timeout_s", "cleanup_timeout_s"):
            if float(getattr(self, attr)) <= 0.0:
                raise MCPConfigurationError(f"{attr} must be positive")
        # freeze the injected mapping so later mutation cannot bypass validation
        object.__setattr__(self, "server_env", MappingProxyType(dict(self.server_env)))
        object.__setattr__(self, "allowed_tools", tuple(self.allowed_tools))
        object.__setattr__(self, "args", tuple(self.args))
        object.__setattr__(
            self, "trusted_entrypoints", tuple(self.trusted_entrypoints)
        )

    def __repr__(self) -> str:
        return (
            "MCPServerConfig("
            f"name={self.name!r}, args={self.args!r}, cwd={self.cwd!r}, "
            f"allowed_tools={self.allowed_tools!r}, capability=<opaque>)"
        )


class MCPConnection:
    """Async context manager owning one bounded MCP stdio session.

    Usage::

        async with MCPConnection(config, audit=events.append) as conn:
            tools = conn.function_tools()
            result = await conn.call_tool("echo", {"text": "hi"})

    An optional ``audit`` callback receives metadata-only event mappings
    (event name, tool name, timings, hashes, protocol, SDK version, status and
    outcome). Callback failures never alter an established outcome and never
    trigger a replay.
    """

    def __init__(
        self,
        config: MCPServerConfig,
        *,
        audit: Callable[[Mapping[str, Any]], None] | None = None,
    ) -> None:
        self._config = config
        self._audit = audit
        self._stack: AsyncExitStack | None = None
        self._session: ClientSession | None = None
        self._errlog: Any = None
        self._tools: dict[str, ToolDefinition] = {}
        self._protocol_version: str | None = None
        self._server_name: str | None = None
        self._server_version: str | None = None
        self._sdk_version = _sdk_version()
        self._last_outcome: Outcome = Outcome.NOT_DISPATCHED

    # ------------------------------------------------------------------ #
    # public surface
    # ------------------------------------------------------------------ #

    def __repr__(self) -> str:
        return (
            f"MCPConnection(server={self._config.name!r}, "
            f"active={self._session is not None})"
        )

    async def __aenter__(self) -> "MCPConnection":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        await self.aclose()
        return False

    @property
    def session_active(self) -> bool:
        """True while a started session is still owned by this connection."""
        return self._session is not None

    @property
    def protocol_version(self) -> str | None:
        """Protocol version negotiated by the real ``initialize`` handshake."""
        return self._protocol_version

    @property
    def sdk_version(self) -> str:
        """Installed official SDK version actually used for this session."""
        return self._sdk_version

    @property
    def server_name(self) -> str | None:
        return self._server_name

    @property
    def last_outcome(self) -> Outcome:
        """Outcome of the most recent call on this connection."""
        return self._last_outcome

    @property
    def discovered_tools(self) -> Mapping[str, ToolDefinition]:
        """Discovered tools that are both allowlisted and schema-valid."""
        return dict(self._tools)

    def function_tools(self) -> list[dict[str, Any]]:
        """DeepSeek-ready function tools for the discovered allowlist only."""
        ordered = [name for name in self._config.allowed_tools if name in self._tools]
        return [self._tools[name].function_tool() for name in ordered]

    async def start(self) -> "MCPConnection":
        """Launch the server, negotiate ``initialize`` and discover tools."""
        if self._stack is not None or self._session is not None:
            raise MCPConfigurationError("this connection is already started")
        command = self._resolve_command()
        child_env = self._build_env()
        args = [str(item) for item in self._config.args]
        params = StdioServerParameters(
            command=command,
            args=args,
            env=child_env,
            cwd=self._config.cwd,
        )
        # stderr is captured through a bounded pipe so server noise can neither
        # become an unfiltered log channel nor an unbounded file
        self._errlog = _BoundedLog(self._config.max_stderr_bytes)
        self._stack = AsyncExitStack()
        try:
            read, write = await self._stack.enter_async_context(
                stdio_client(params, errlog=self._errlog)
            )
            session = await self._stack.enter_async_context(
                ClientSession(
                    read,
                    write,
                    read_timeout_seconds=timedelta(seconds=self._config.read_timeout_s),
                )
            )
            self._session = session
            handshake = await asyncio.wait_for(
                session.initialize(), timeout=self._config.startup_timeout_s
            )
            self._protocol_version = _field(handshake, "protocolVersion", "protocol_version")
            server_info = _field(handshake, "serverInfo", "server_info")
            self._server_name = _field(server_info, "name")
            self._server_version = _field(server_info, "version")
            self._emit(
                "initialize",
                protocol=self._protocol_version,
                sdk=self._sdk_version,
                server=self._server_name,
                server_version=self._server_version,
            )
            await self._discover()
        except asyncio.CancelledError:
            await self.aclose()
            raise
        except MCPTransportError:
            await self.aclose()
            raise
        except (asyncio.TimeoutError, TimeoutError) as exc:
            await self.aclose()
            raise MCPTimeoutError("the MCP server did not finish initializing") from exc
        except Exception as exc:  # noqa: BLE001 - normalised into a typed error
            await self.aclose()
            raise MCPProtocolError(
                f"MCP initialization failed ({type(exc).__name__})"
            ) from exc
        return self

    async def aclose(self) -> None:
        """Close session, streams, owned child process and temp resources.

        Safe to call more than once, and must be awaited from the same task
        that ran :meth:`start` so ``anyio`` cancel scopes unwind correctly.
        """
        stack, self._stack = self._stack, None
        self._session = None
        self._tools = {}
        close_error: str | None = None
        if stack is not None:
            try:
                await stack.aclose()
            except Exception as exc:  # noqa: BLE001 - cleanup must not mask the cause
                close_error = type(exc).__name__
        errlog, self._errlog = self._errlog, None
        stderr_bytes = 0
        stderr_discarded = 0
        if errlog is not None:
            stderr_bytes = int(getattr(errlog, "bytes_written", 0))
            stderr_discarded = int(getattr(errlog, "bytes_discarded", 0))
            try:
                errlog.close()
            except Exception:  # noqa: BLE001
                pass
        # the official SDK owns a Windows Job Object and terminates the whole
        # tree; its own bound is ~2 s and cannot be shortened from here, so the
        # configured value is reported as advisory rather than pretended to apply
        self._emit(
            "close",
            server=self._config.name,
            status="closed" if close_error is None else "close_failed",
            cleanup_error=close_error,
            configured_cleanup_timeout_s=self._config.cleanup_timeout_s,
            sdk_cleanup_bound_s=2.0,
            stderr_bytes=stderr_bytes,
            stderr_discarded=stderr_discarded,
        )

    async def reconnect(self) -> None:
        """Tear down the session and negotiate a completely fresh one.

        Prior calls are never replayed: the new session renegotiates
        ``initialize`` and re-discovers tools only.
        """
        await self.aclose()
        self._protocol_version = None
        self._server_name = None
        self._server_version = None
        self._last_outcome = Outcome.NOT_DISPATCHED
        await self.start()

    async def call_tool(
        self,
        name: str,
        arguments: Mapping[str, Any] | None = None,
    ) -> ToolCallResult:
        """Validate locally, dispatch once, and return a bounded result.

        Raises a typed :class:`MCPTransportError` subclass on any failure. The
        call is dispatched at most once; there is no automatic retry.
        """
        session = self._require_session()
        definition = self._tools.get(name)
        if definition is None:
            self._last_outcome = Outcome.NOT_DISPATCHED
            raise MCPDisallowedToolError(f"tool is not authorized: {name!r}")
        args: dict[str, Any] = {} if arguments is None else dict(arguments)
        serialized = _canonical(args)
        if _byte_len(serialized) > self._config.max_input_bytes:
            self._last_outcome = Outcome.NOT_DISPATCHED
            raise MCPOversizedError(
                f"arguments for tool {name!r} exceed the input limit",
                outcome=Outcome.NOT_DISPATCHED,
            )
        try:
            validator = _validator(definition.input_schema)
            if validator is not None:
                validator.validate(args)
        except MCPProtocolError:
            self._last_outcome = Outcome.NOT_DISPATCHED
            raise
        except jsonschema.ValidationError as exc:
            self._last_outcome = Outcome.NOT_DISPATCHED
            raise MCPInvalidArgumentsError(
                f"arguments rejected for tool {name!r}"
            ) from exc
        input_hash = _sha256(serialized)
        started = time.perf_counter()
        try:
            raw = await asyncio.wait_for(
                session.call_tool(name, args),
                timeout=self._config.call_timeout_s,
            )
        except asyncio.CancelledError:
            self._last_outcome = Outcome.UNKNOWN
            self._emit(
                "call",
                tool=name,
                status="cancelled",
                outcome=Outcome.UNKNOWN.value,
                input_hash=input_hash,
                duration_ms=self._elapsed(started),
                protocol=self._protocol_version,
                sdk=self._sdk_version,
            )
            raise
        except (asyncio.TimeoutError, TimeoutError) as exc:
            self._last_outcome = Outcome.UNKNOWN
            self._emit(
                "call",
                tool=name,
                status="timeout",
                outcome=Outcome.UNKNOWN.value,
                input_hash=input_hash,
                duration_ms=self._elapsed(started),
                protocol=self._protocol_version,
                sdk=self._sdk_version,
            )
            raise MCPTimeoutError(
                f"tool {name!r} did not respond within the call timeout"
            ) from exc
        except MCPTransportError:
            raise
        except Exception as exc:  # noqa: BLE001 - normalised into a typed error
            classified = classify_exception(exc)
            self._last_outcome = classified.outcome
            self._emit(
                "call",
                tool=name,
                status=classified.kind,
                outcome=classified.outcome.value,
                input_hash=input_hash,
                duration_ms=self._elapsed(started),
                protocol=self._protocol_version,
                sdk=self._sdk_version,
            )
            raise classified from exc
        duration_ms = self._elapsed(started)
        is_error = bool(_field(raw, "isError", "is_error", default=False))
        content = _field(raw, "content", default=None) or ()
        structured = _field(raw, "structuredContent", "structured_content", default=None)
        text, unsupported = _collect_text(content)
        bounded = _canonical(
            {"text": text, "structured": structured, "unsupported": unsupported}
        )
        if _byte_len(bounded) > self._config.max_output_bytes:
            self._last_outcome = Outcome.UNKNOWN
            self._emit(
                "call",
                tool=name,
                status="oversized",
                outcome=Outcome.UNKNOWN.value,
                input_hash=input_hash,
                duration_ms=duration_ms,
                protocol=self._protocol_version,
                sdk=self._sdk_version,
            )
            # the call *was* dispatched: the effect is unestablished, so the
            # typed error must agree with the connection and the audit trail
            raise MCPOversizedError(
                f"the result of tool {name!r} exceeds the output limit",
                outcome=Outcome.UNKNOWN,
            )
        if unsupported:
            self._last_outcome = Outcome.UNKNOWN
            self._emit(
                "call",
                tool=name,
                status="unsupported_content",
                outcome=Outcome.UNKNOWN.value,
                input_hash=input_hash,
                duration_ms=duration_ms,
                protocol=self._protocol_version,
                sdk=self._sdk_version,
            )
            # returning a silently thinned result would misreport what the
            # server produced, so the content kinds are named explicitly
            raise MCPInvalidResultError(
                f"the result of tool {name!r} contains unsupported content blocks:"
                f" {sorted(unsupported)}",
                outcome=Outcome.UNKNOWN,
            )
        output_hash = _sha256(bounded)
        if is_error:
            self._last_outcome = Outcome.ERROR
            self._emit(
                "call",
                tool=name,
                status="tool_error",
                outcome=Outcome.ERROR.value,
                input_hash=input_hash,
                output_hash=output_hash,
                duration_ms=duration_ms,
                protocol=self._protocol_version,
                sdk=self._sdk_version,
            )
            # The server's own error text is deliberately not forwarded.
            raise MCPToolError(f"tool {name!r} reported an error")
        if definition.output_schema is not None and structured is not None:
            try:
                validator = _validator(definition.output_schema)
                if validator is not None:
                    validator.validate(structured)
            except MCPProtocolError:
                self._last_outcome = Outcome.UNKNOWN
                raise
            except jsonschema.ValidationError as exc:
                self._last_outcome = Outcome.UNKNOWN
                self._emit(
                    "call",
                    tool=name,
                    status="invalid_result",
                    outcome=Outcome.UNKNOWN.value,
                    input_hash=input_hash,
                    output_hash=output_hash,
                    duration_ms=duration_ms,
                    protocol=self._protocol_version,
                    sdk=self._sdk_version,
                )
                raise MCPInvalidResultError(
                    f"the result of tool {name!r} does not match its declared schema"
                ) from exc
        self._last_outcome = Outcome.SUCCESS
        self._emit(
            "call",
            tool=name,
            status="ok",
            outcome=Outcome.SUCCESS.value,
            input_hash=input_hash,
            output_hash=output_hash,
            duration_ms=duration_ms,
            protocol=self._protocol_version,
            sdk=self._sdk_version,
        )
        return ToolCallResult(
            tool=name,
            outcome=Outcome.SUCCESS,
            text=text,
            structured=structured,
            input_hash=input_hash,
            output_hash=output_hash,
            duration_ms=duration_ms,
            protocol_version=self._protocol_version,
            sdk_version=self._sdk_version,
        )

    # ------------------------------------------------------------------ #
    # internals
    # ------------------------------------------------------------------ #

    @staticmethod
    def _elapsed(started: float) -> float:
        return round((time.perf_counter() - started) * 1000.0, 3)

    def _require_session(self) -> ClientSession:
        if self._session is None:
            raise MCPTransportError("the MCP session is not active", kind="lifecycle")
        return self._session

    def _emit(self, event: str, **fields: Any) -> None:
        if self._audit is None:
            return
        payload: dict[str, Any] = {"event": event}
        payload.update(fields)
        try:
            self._audit(payload)
        except Exception:  # noqa: BLE001 - audit failure must be inert
            pass

    def _resolve_command(self) -> str:
        """Refuse anything but the caller's Python or a trusted entrypoint."""
        exe = self._config.command or sys.executable
        real = os.path.normcase(os.path.realpath(exe))
        own = os.path.normcase(os.path.realpath(sys.executable))
        if real == own:
            return sys.executable
        for candidate in self._config.trusted_entrypoints:
            if os.path.normcase(os.path.realpath(candidate)) == real:
                return exe
        raise MCPLaunchError(
            "refusing to launch: command is neither the caller's Python "
            "executable nor a trusted entrypoint"
        )

    def _build_env(self) -> dict[str, str]:
        """Construct the exact, credential-free child environment.

        The returned mapping is the *complete* environment of the spawned
        process. Every name the official SDK's safe-inheritance merge would add
        is already decided here, so the child's real environment matches this
        reviewed mapping. Names are compared case-insensitively so a
        differently-cased duplicate cannot smuggle in a second value.
        """
        source = {k.upper(): v for k, v in os.environ.items()}
        out: dict[str, str] = {}
        for key in _ENV_ALLOW:
            value = source.get(key.upper())
            if value:
                out[key] = value
        for key in _SDK_INHERITED_ENV:
            if key in out:
                continue
            value = source.get(key.upper())
            if value:
                out[key] = value
        out.setdefault("PYTHONIOENCODING", "utf-8")
        out.setdefault("PYTHONUTF8", "1")
        overrides = {k.upper(): (k, v) for k, v in self._config.server_env.items()}
        for upper, (original, value) in overrides.items():
            out.pop(upper, None)
            out[original] = str(value)
        if self._config.capability is not None:
            out.pop(self._config.capability_env_name.upper(), None)
            out[self._config.capability_env_name] = self._config.capability
        return out

    def _approved_env_names(self) -> frozenset[str]:
        """The reviewed set of environment names the child may observe."""
        return frozenset(
            {k.upper() for k in _ENV_ALLOW}
            | {k.upper() for k in _SDK_INHERITED_ENV}
            | {k.upper() for k in _ENV_OVERRIDE_ALLOW}
            | {self._config.capability_env_name.upper()}
        )

    async def _discover(self) -> None:
        session = self._require_session()
        budget = int(self._config.max_discovery_bytes)
        accounted = 0
        page = 0
        tools: list[Any] = []
        while True:
            try:
                listing = await asyncio.wait_for(
                    session.list_tools(), timeout=self._config.startup_timeout_s
                )
            except asyncio.CancelledError:
                raise
            except (asyncio.TimeoutError, TimeoutError) as exc:
                raise MCPTimeoutError("MCP tool discovery timed out") from exc
            except Exception as exc:  # noqa: BLE001
                raise classify_exception(exc) from exc
            page_tools = list(_field(listing, "tools", default=()) or ())
            # account for the *complete* advertised listing - every tool, and
            # every field including descriptions, titles, annotations and meta -
            # before a single definition is validated or installed
            accounted += _byte_len(_canonical([_tool_stub(t) for t in page_tools]))
            if accounted > budget:
                raise MCPProtocolError(
                    "the discovery payload exceeds the size limit"
                )
            cursor = _field(listing, "nextCursor", "next_cursor", default=None)
            if cursor:
                # pagination is not implemented: refuse rather than silently
                # installing a partial catalogue with an unreviewed remainder
                raise MCPProtocolError(
                    "the server requested paginated discovery, which is not supported"
                )
            tools.extend(page_tools)
            page += 1
            if page > 1:  # pragma: no cover - cursor is refused above
                break
            break
        self._install_tools(tools)
        self._emit(
            "discovery",
            discovered=len(tools),
            allowed=len(self._tools),
            discovery_bytes=accounted,
            protocol=self._protocol_version,
            sdk=self._sdk_version,
        )

    def _install_tools(self, tools: list[Any]) -> None:
        """Validate discovered definitions and keep only the allowed subset."""
        if len(tools) > self._config.max_tools:
            raise MCPProtocolError("the server advertised more tools than are permitted")
        allowed = set(self._config.allowed_tools)
        seen: set[str] = set()
        total_bytes = 0
        for tool in tools:
            name = _field(tool, "name")
            if not isinstance(name, str) or not name:
                raise MCPProtocolError("a discovered tool has no usable name")
            if name in seen:
                raise MCPProtocolError(f"duplicate tool definition: {name!r}")
            seen.add(name)
            # every advertised tool is accounted for, including disallowed ones
            total_bytes += _byte_len(_canonical(_tool_stub(tool)))
            if total_bytes > self._config.max_discovery_bytes:
                raise MCPProtocolError("the discovery payload exceeds the size limit")
            if name not in allowed:
                continue
            schema = _field(tool, "inputSchema", "input_schema")
            if not isinstance(schema, dict):
                raise MCPProtocolError(f"tool {name!r} has no usable input schema")
            external = _schema_external_reference(schema)
            if external is not None:
                raise MCPProtocolError(
                    f"tool {name!r} input schema uses a non-local reference: {external}"
                )
            try:
                jsonschema.Draft202012Validator.check_schema(schema)
            except jsonschema.SchemaError as exc:
                raise MCPProtocolError(f"tool {name!r} input schema is invalid") from exc
            if schema.get("type") not in (None, "object"):
                raise MCPProtocolError(f"tool {name!r} input schema is not an object")
            output_schema = _field(tool, "outputSchema", "output_schema", default=None)
            if output_schema is not None:
                if not isinstance(output_schema, dict):
                    raise MCPProtocolError(f"tool {name!r} output schema is unusable")
                external = _schema_external_reference(output_schema)
                if external is not None:
                    raise MCPProtocolError(
                        f"tool {name!r} output schema uses a non-local reference: {external}"
                    )
                try:
                    jsonschema.Draft202012Validator.check_schema(output_schema)
                except jsonschema.SchemaError as exc:
                    raise MCPProtocolError(
                        f"tool {name!r} output schema is invalid"
                    ) from exc
            description = _field(tool, "description", default=None)
            self._tools[name] = ToolDefinition(
                name=name,
                description=description if isinstance(description, str) else "",
                input_schema=schema,
                output_schema=output_schema,
                input_schema_hash=_sha256(_canonical(schema)),
            )
        # the model-facing function payload is capped too, not only the schemas
        model_payload = _canonical(self.function_tools())
        if _byte_len(model_payload) > self._config.max_discovery_bytes:
            self._tools = {}
            raise MCPProtocolError(
                "the model-facing tool payload exceeds the discovery size limit"
            )
        missing = allowed - set(self._tools)
        if missing:
            raise MCPProtocolError(
                f"required allowlisted tools were not discovered: {sorted(missing)}"
            )
        if not self._tools:
            raise MCPProtocolError("no allowlisted tools were discovered")
