"""The KVFlow MCP server: the standard tool entrance for any host.

A real MCP server over stdio, built on the official SDK. It is the *control*
surface (what work exists, what it is doing, what it produced), not a second
scheduler: every call goes through :mod:`kvflow.api`, the same handlers the CLI
bridge and the DSH plugin use, over the same registry, durable store and
workflow runner.

Authority rules live in ``kvflow.api``: a caller may supply a requirement, a
registered ``project_id`` and a template id, and nothing else.

    KVFLOW_HOME=<runtime dir> python -m kvflow.mcp_server
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path
from typing import Any

import mcp.types as types
from mcp.server.lowlevel import Server
from mcp.server.stdio import stdio_server

from . import api
from .core.errors import V1Error

#: environment variable naming the runtime directory this server serves
HOME_ENV = api.HOME_ENV
SERVER_NAME = "kvflow"


def build_server(*, home: Path | None = None, name: str = SERVER_NAME) -> Server:
    """Build the MCP server bound to one KVFlow runtime directory."""
    server: Server = Server(name)
    resolved = api.home_from(str(home) if home else None)

    @server.list_tools()
    async def list_tools() -> list[types.Tool]:
        return [
            types.Tool(name=spec["name"], description=spec["description"],
                       inputSchema=spec["schema"])
            for spec in api.TOOLS
        ]

    @server.call_tool()
    async def call_tool(tool_name: str, arguments: dict[str, Any]) -> list[types.ContentBlock]:
        try:
            payload = api.invoke(tool_name, dict(arguments or {}), home=str(resolved))
            return [types.TextContent(type="text", text=json.dumps(
                {"ok": True, "tool": tool_name, "result": payload},
                ensure_ascii=False, default=str))]
        except V1Error as exc:
            return [types.TextContent(type="text", text=json.dumps(
                {"ok": False, "tool": tool_name, "error_code": exc.code,
                 "error": exc.to_dict()}, ensure_ascii=False, default=str))]
        except Exception as exc:  # noqa: BLE001 - a tool failure is a reported result
            return [types.TextContent(type="text", text=json.dumps(
                {"ok": False, "tool": tool_name, "error_code": "UNEXPECTED",
                 "error": f"{type(exc).__name__}: {exc}"}, ensure_ascii=False))]

    return server


async def serve_stdio(*, home: Path | None = None) -> None:
    server = build_server(home=home)
    async with stdio_server() as (read, write):
        await server.run(read, write, server.create_initialization_options())


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="kvflow-mcp")
    parser.add_argument("--home", default=None)
    args = parser.parse_args(argv)
    if args.home:
        os.environ[HOME_ENV] = str(Path(args.home).expanduser().resolve())
    if not os.environ.get(HOME_ENV):
        print(f"{HOME_ENV} or --home is required", file=sys.stderr)
        return 2
    asyncio.run(serve_stdio(home=Path(os.environ[HOME_ENV])))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
