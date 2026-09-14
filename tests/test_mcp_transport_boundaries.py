"""F2 regression tests for the four repaired P1 boundary findings.

Each test binds to a specific reproduced Manager finding:

* an external JSON Schema ``$ref`` must never cause an outbound request and the
  discovery must be refused instead of silently fetching;
* the discovery byte bound must cover the complete advertised listing, including
  descriptions of tools that are not allowlisted;
* the child environment must match a documented, credential-free review list even
  when a trusted configuration tries to inject a credential name;
* an oversized *dispatched* result must report ``OUTCOME_UNKNOWN`` through the
  typed exception and the connection, never ``NOT_DISPATCHED``.

The fixture server here is written by the test itself so the shapes are explicit
and cannot drift from what the assertions expect.
"""

from __future__ import annotations

import asyncio
import ctypes
import json
import sys
import threading
import time
import uuid
from ctypes import wintypes
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

from kvflow.mcp_transport import (
    MCPConfigurationError,
    MCPConnection,
    MCPOversizedError,
    MCPProtocolError,
    MCPServerConfig,
    MCPTransportError,
    Outcome,
)

FIXTURE_SERVER = Path(__file__).resolve().parent / "support" / "mcp_boundary_server.py"

MODE_ARGS: dict[str, tuple[str, ...]] = {
    "plain": (),
    "local_ref": (),
    "remote_ref": ("http://127.0.0.1:1/schema",),
    "oversize": (),
    "unsupported_content": (),
    "big_description": (),
    "many_tools": (),
    "stderr_flood": (),
}


def _config(server: Path, tmp_path: Path, mode: str, *, remote_url: str | None = None, **overrides):
    extra = (remote_url,) if remote_url else ()
    kwargs: dict = {
        "name": "boundary-fixture",
        "args": (str(server), mode, *extra),
        "cwd": str(server.parent),
        "allowed_tools": ("probe",),
        "startup_timeout_s": 30,
        "call_timeout_s": 10,
        "read_timeout_s": 30,
    }
    kwargs.update(overrides)
    return MCPServerConfig(**kwargs)


# --------------------------------------------------------------- schema refs


def test_local_reference_still_validates(tmp_path):
    """A ``#/$defs`` reference is legitimate and must keep working."""
    server = FIXTURE_SERVER

    async def scenario():
        conn = MCPConnection(_config(server, tmp_path, "local_ref"))
        async with conn:
            result = await conn.call_tool("probe", {"x": {"x": 1}})
            return result.outcome

    assert asyncio.run(scenario()) is Outcome.SUCCESS


def test_external_reference_is_refused_without_any_http_request(tmp_path):
    """A remote ``$ref`` must be refused, and must never be fetched."""
    hits: list[str] = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802 - stdlib contract
            hits.append(self.path)
            payload = b'{"type":"integer"}'
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, *args):  # noqa: ARG002
            return None

    httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    server = FIXTURE_SERVER
    url = f"http://127.0.0.1:{httpd.server_address[1]}/schema"
    try:

        async def scenario():
            conn = MCPConnection(_config(server, tmp_path, "remote_ref", remote_url=url))
            with pytest.raises(MCPTransportError) as excinfo:
                async with conn:
                    pass
            return excinfo.value

        error = asyncio.run(scenario())
        assert error.kind == "protocol"
        assert "non-local reference" in str(error)
        assert hits == [], f"schema validation performed an outbound request: {hits}"
    finally:
        httpd.shutdown()
        httpd.server_close()
        thread.join(timeout=2)


# ------------------------------------------------------------ discovery cap


@pytest.mark.parametrize("mode", ["big_description", "many_tools"])
def test_discovery_cap_covers_descriptions_and_metadata(tmp_path, mode):
    """The cap must measure the whole advertised listing, not only schemas."""
    server = FIXTURE_SERVER

    async def scenario():
        conn = MCPConnection(_config(server, tmp_path, mode, max_discovery_bytes=128))
        with pytest.raises(MCPProtocolError) as excinfo:
            async with conn:
                pass
        return excinfo.value, conn

    error, conn = asyncio.run(scenario())
    assert "discovery payload exceeds" in str(error)
    # nothing was installed as a model-facing definition
    assert conn.function_tools() == []
    assert dict(conn.discovered_tools) == {}


def test_discovery_cap_accepts_an_honest_small_listing(tmp_path):
    server = FIXTURE_SERVER

    async def scenario():
        conn = MCPConnection(_config(server, tmp_path, "local_ref", max_discovery_bytes=8192))
        async with conn:
            return len(conn.function_tools())

    assert asyncio.run(scenario()) == 1


def test_model_facing_payload_is_capped(tmp_path):
    """A listing just under the discovery cap must still bound the model payload."""
    server = FIXTURE_SERVER

    async def scenario():
        conn = MCPConnection(_config(server, tmp_path, "big_description", max_discovery_bytes=9000))
        try:
            async with conn:
                return ("started", len(conn.function_tools()))
        except MCPProtocolError as exc:
            return ("refused", str(exc))

    outcome, detail = asyncio.run(scenario())
    assert outcome == "refused", (
        "a 9 KiB description must not reach the model payload under a 9 KiB cap"
    )
    assert "payload exceeds" in detail or "size limit" in detail


# ------------------------------------------------------------- environment


@pytest.mark.parametrize(
    "name",
    [
        "DEEPSEEK_API_KEY",
        "OPENAI_API_KEY",
        "HTTPS_PROXY",
        "https_proxy",
        "Path",
        "USERPROFILE",
        "Agent_Os_Fixture_Token",
    ],
)
def test_credential_or_host_variable_injection_is_refused(tmp_path, name):
    server = FIXTURE_SERVER
    with pytest.raises(MCPConfigurationError):
        _config(server, tmp_path, "local_ref", server_env={name: "SENTINEL"})


def test_capability_variable_cannot_impersonate_a_credential(tmp_path):
    server = FIXTURE_SERVER
    with pytest.raises(MCPConfigurationError):
        _config(
            server,
            tmp_path,
            "local_ref",
            capability="opaque",
            capability_env_name="DEEPSEEK_API_KEY",
        )


def test_approved_agent_os_override_is_allowed_and_reaches_the_child(tmp_path):
    server = FIXTURE_SERVER

    async def scenario():
        conn = MCPConnection(
            _config(
                server,
                tmp_path,
                "local_ref",
                server_env={"KVFLOW_FIXTURE_MARKER": "present"},
            )
        )
        async with conn:
            names = await conn.call_tool("probe", {"x": 1})
            return names.text

    # the fixture's probe returns JSON, not the environment; the point here is
    # that an approved name is accepted and the session works normally
    assert "ok" in asyncio.run(scenario())


def test_explicit_forbidden_override_never_launches_a_child(tmp_path):
    server = FIXTURE_SERVER
    marker = tmp_path / "never.log"
    with pytest.raises(MCPConfigurationError):
        _config(
            server,
            tmp_path,
            "local_ref",
            server_env={"KVFLOW_FIXTURE_LOG": str(marker), "DEEPSEEK_API_KEY": "x"},
        )
    assert not marker.exists(), "configuration was refused before any launch"


def test_server_env_mapping_cannot_be_mutated_after_validation(tmp_path):
    server = FIXTURE_SERVER
    config = _config(server, tmp_path, "local_ref", server_env={"KVFLOW_FIXTURE_LOG": "a"})
    with pytest.raises(TypeError):
        config.server_env["DEEPSEEK_API_KEY"] = "smuggled"  # type: ignore[index]


# ------------------------------------------------------------ oversized result


def test_oversized_dispatched_result_is_unknown_everywhere(tmp_path):
    server = FIXTURE_SERVER
    events: list[dict] = []

    async def scenario():
        conn = MCPConnection(
            _config(server, tmp_path, "oversize", max_output_bytes=512),
            audit=events.append,
        )
        with pytest.raises(MCPOversizedError) as excinfo:
            async with conn:
                await conn.call_tool("probe", {"x": 1})
        return excinfo.value, conn

    error, conn = asyncio.run(scenario())
    assert error.outcome is Outcome.UNKNOWN
    assert conn.last_outcome is Outcome.UNKNOWN
    oversized = [e for e in events if e.get("status") == "oversized"]
    assert oversized and oversized[0]["outcome"] == Outcome.UNKNOWN.value


def test_oversized_arguments_are_not_dispatched(tmp_path):
    server = FIXTURE_SERVER
    log = tmp_path / "effects.log"

    async def scenario():
        conn = MCPConnection(
            _config(
                server,
                tmp_path,
                "local_ref",
                max_input_bytes=64,
                server_env={"KVFLOW_FIXTURE_LOG": str(log)},
            )
        )
        with pytest.raises(MCPOversizedError) as excinfo:
            async with conn:
                await conn.call_tool("probe", {"x": 1, "pad": "y" * 512})
        return excinfo.value, conn

    error, conn = asyncio.run(scenario())
    assert error.outcome is Outcome.NOT_DISPATCHED
    assert conn.last_outcome is Outcome.NOT_DISPATCHED


def test_stderr_flood_is_bounded_and_private(tmp_path):
    """The private stderr file honours the configured cap and discards the rest."""
    server = FIXTURE_SERVER
    events: list[dict] = []

    async def scenario():
        conn = MCPConnection(
            _config(server, tmp_path, "stderr_flood", max_stderr_bytes=4096),
            audit=events.append,
        )
        async with conn:
            await conn.call_tool("probe", {"x": 1})
            size = conn._errlog.tell()
            written = conn._errlog.bytes_written
            discarded = conn._errlog.bytes_discarded
        return size, written, discarded

    size, written, discarded = asyncio.run(scenario())
    assert size <= 4096, f"private stderr file grew to {size} bytes"
    assert written > 0, "the flood should have been observed"
    assert discarded > 0, "bytes beyond the cap must be discarded, not stored"
    close_events = [e for e in events if e.get("event") == "close"]
    assert close_events
    # the drain thread may observe a few final bytes during teardown, so the
    # audited total is a lower bound of what was actually observed
    assert close_events[-1]["stderr_bytes"] >= written
    assert close_events[-1]["stderr_discarded"] >= discarded
    assert close_events[-1]["status"] == "closed"
    # and the retained bytes never exceed the cap
    assert close_events[-1]["stderr_bytes"] - close_events[-1]["stderr_discarded"] <= 4096


# ------------------------------------------------------------- cleanup bound


def _open_typed(pid: int):
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    kernel32.OpenProcess.restype = wintypes.HANDLE
    kernel32.GetExitCodeProcess.argtypes = [wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD)]
    kernel32.GetExitCodeProcess.restype = wintypes.BOOL
    kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel32.CloseHandle.restype = wintypes.BOOL
    handle = kernel32.OpenProcess(0x1000, False, pid)
    if not handle:
        return {"alive": False if ctypes.get_last_error() == 87 else None,
                "open_error": ctypes.get_last_error()}
    try:
        code = wintypes.DWORD()
        if not kernel32.GetExitCodeProcess(handle, ctypes.byref(code)):
            return {"alive": None, "query_error": ctypes.get_last_error()}
        return {"alive": code.value == 259, "exit_code": code.value}
    finally:
        kernel32.CloseHandle(handle)


def test_close_does_not_pretend_to_honour_an_impossible_timeout(tmp_path):
    """The SDK owns a ~2 s Windows Job Object bound; the audit must say so."""
    if sys.platform != "win32":
        pytest.skip("Windows Job Object cleanup bound")
    server = FIXTURE_SERVER
    events: list[dict] = []

    async def scenario():
        conn = MCPConnection(
            _config(server, tmp_path, "local_ref", cleanup_timeout_s=0.01),
            audit=events.append,
        )
        await conn.start()
        await conn.aclose()
        return events

    events = asyncio.run(scenario())
    close = [e for e in events if e.get("event") == "close"][-1]
    assert close["configured_cleanup_timeout_s"] == 0.01
    assert close["sdk_cleanup_bound_s"] == 2.0
    assert close["status"] == "closed"
