"""Low-level MCP fixture serving *explicit* schemas for boundary testing.

``FastMCP`` rewrites tool schemas from Python type hints, so it cannot advertise
a schema containing a remote ``$ref``. This fixture uses the low-level SDK so the
transport's schema handling is tested against exactly the bytes under test.

It serves only loopback stdio; it opens no network connection of its own.
"""

from __future__ import annotations

import asyncio
import json
import sys
from typing import Any

import mcp.types as types
from mcp.server.lowlevel import Server
from mcp.server.stdio import stdio_server

MODE = sys.argv[1] if len(sys.argv) > 1 else "plain"
REMOTE_URL = sys.argv[2] if len(sys.argv) > 2 else "http://127.0.0.1:1/schema"

SERVER = Server("f2-lowlevel-fixture")


def _local_ref_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "$defs": {
            "payload": {
                "type": "object",
                "properties": {"x": {"type": "integer"}},
                "required": ["x"],
            }
        },
        "properties": {"payload": {"$ref": "#/$defs/payload"}},
    }


def _remote_ref_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "properties": {"x": {"$ref": f"{REMOTE_URL}#/definitions/value"}},
    }


def _plain_schema() -> dict[str, Any]:
    return {"type": "object", "properties": {"x": {"type": "integer"}}}


def _schema_for_mode() -> dict[str, Any]:
    if MODE == "local_ref":
        return _local_ref_schema()
    if MODE == "remote_ref":
        return _remote_ref_schema()
    return _plain_schema()


def _tools() -> list[types.Tool]:
    tools = [
        types.Tool(
            name="probe",
            description="a bounded boundary probe",
            inputSchema=_schema_for_mode(),
        )
    ]
    if MODE in ("big_description", "many_tools"):
        count = 40 if MODE == "many_tools" else 1
        for index in range(count):
            tools.append(
                types.Tool(
                    name=f"noise_{index}" if MODE == "many_tools" else "noise",
                    description="N" * 9000,
                    inputSchema={"type": "object", "properties": {}},
                )
            )
    return tools


@SERVER.list_tools()
async def list_tools() -> list[types.Tool]:
    return _tools()


@SERVER.call_tool()
async def call_tool(name: str, arguments: dict[str, Any]) -> list[types.ContentBlock]:
    if name != "probe":
        return [types.TextContent(type="text", text="noise")]
    if MODE == "oversize":
        return [types.TextContent(type="text", text="z" * 4096)]
    if MODE == "unsupported_content":
        return [types.ImageContent(type="image", data="AAAA", mimeType="image/png")]
    return [types.TextContent(type="text", text=json.dumps({"ok": True, "args": arguments}))]


async def _flood_stderr() -> None:
    for _ in range(2048):
        sys.stderr.write("E" * 100 + "\n")
        sys.stderr.flush()
        await asyncio.sleep(0)


async def main() -> None:
    if MODE == "stderr_flood":
        asyncio.get_running_loop().create_task(_flood_stderr())
    async with stdio_server() as (read, write):
        await SERVER.run(read, write, SERVER.create_initialization_options())


if __name__ == "__main__":
    asyncio.run(main())
