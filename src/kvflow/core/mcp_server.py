"""The product MCP server: the four tool groups over real stdio MCP.

This is a genuine MCP server built on the official SDK, not a Python function
with the same name. It serves exactly one Orchestrator-issued capability per
process: the ticket arrives through the same reviewed child environment channel
the transport documents, and every tool call is authorized through
``kvflow.core.tools.ToolService`` before anything happens.

Boundaries
----------

* **stdout is protocol only.** Diagnostics go to stderr through the SDK's own
  logging, never to stdout.
* **No new authority.** The server holds no credentials and cannot widen the
  ticket it was given: it only forwards the action name and arguments. A model
  that asks for an action outside the ticket's allowlist is refused by the
  service, not by this file.
* **Bounded responses.** The service already caps every result; this server
  returns that bounded JSON as text and marks truncation explicitly.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import mcp.types as types
from mcp.server.lowlevel import Server
from mcp.server.stdio import stdio_server

from .contracts import Action
from .errors import V1Error
from .security import CapabilityAuthority
from .store import Store
from .tools import ToolService

#: environment variable the Orchestrator uses to hand the ticket to this process
CAPABILITY_ENV = "KVFLOW_MCP_CAPABILITY"
#: environment variable naming the v1 database this server owns
DATABASE_ENV = "KVFLOW_MCP_DATABASE"
#: environment variable naming the managed workspace root
WORKSPACE_ENV = "KVFLOW_MCP_WORKSPACE_ROOT"

#: the tools this server exposes, with their JSON schemas
TOOL_SPECS: tuple[dict[str, Any], ...] = (
    {
        "name": "repo_read",
        "action": Action.REPO_READ.value,
        "description": "Read one UTF-8 file from the node's owned workspace.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "path": {"type": "string", "description": "workspace-relative path"},
                "max_bytes": {"type": "integer", "minimum": 1, "maximum": 1048576},
            },
            "required": ["path"],
        },
        "map": {"path": "path", "max_bytes": "max_bytes"},
    },
    {
        "name": "repo_list",
        "action": Action.REPO_LIST.value,
        "description": "List one directory of the owned workspace.",
        "inputSchema": {
            "type": "object",
            "properties": {"path": {"type": "string"}},
        },
        "map": {"path": "path"},
    },
    {
        "name": "repo_search",
        "action": Action.REPO_SEARCH.value,
        "description": "Regex search inside the owned workspace, with hard bounds.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "pattern": {"type": "string"},
                "path": {"type": "string"},
                "max_matches": {"type": "integer", "minimum": 1, "maximum": 500},
            },
            "required": ["pattern"],
        },
        "map": {"pattern": "pattern", "path": "path", "max_matches": "max_matches"},
    },
    {
        "name": "repo_status",
        "action": Action.REPO_STATUS.value,
        "description": "Report the node's changes against its snapshot base.",
        "inputSchema": {"type": "object", "properties": {}},
        "map": {},
    },
    {
        "name": "repo_diff",
        "action": Action.REPO_DIFF.value,
        "description": "Report changed files with content digests.",
        "inputSchema": {
            "type": "object",
            "properties": {"path": {"type": "string"}},
        },
        "map": {"path": "path"},
    },
    {
        "name": "repo_patch",
        "action": Action.REPO_PATCH.value,
        "description": "Write files inside the node's declared write scopes only.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "edits": {
                    "type": "array",
                    "minItems": 1,
                    "maxItems": 128,
                    "items": {
                        "type": "object",
                        "properties": {
                            "path": {"type": "string"},
                            "content": {"type": "string"},
                        },
                        "required": ["path", "content"],
                    },
                }
            },
            "required": ["edits"],
        },
        "map": {"edits": "edits"},
    },
    {
        "name": "repo_commit",
        "action": Action.REPO_COMMIT.value,
        "description": "Commit the node's workspace; the author is the authenticated role.",
        "inputSchema": {
            "type": "object",
            "properties": {"message": {"type": "string", "minLength": 1}},
            "required": ["message"],
        },
        "map": {"message": "message"},
    },
    {
        "name": "test_run",
        "action": Action.TEST_RUN.value,
        "description": "Run one pre-registered test profile and return its executor receipt.",
        "inputSchema": {
            "type": "object",
            "properties": {"profile_id": {"type": "string", "minLength": 1}},
            "required": ["profile_id"],
        },
        "map": {"profile_id": "profile_id"},
    },
    {
        "name": "operation_status",
        "action": Action.OPERATION_STATUS.value,
        "description": "Read the durable status of one of this run's operations.",
        "inputSchema": {
            "type": "object",
            "properties": {"operation_id": {"type": "string"}},
            "required": ["operation_id"],
        },
        "map": {"operation_id": "operation_id"},
    },
    {
        "name": "operation_result",
        "action": Action.OPERATION_RESULT.value,
        "description": "Read the result handle of one of this run's operations.",
        "inputSchema": {
            "type": "object",
            "properties": {"operation_id": {"type": "string"}},
            "required": ["operation_id"],
        },
        "map": {"operation_id": "operation_id"},
    },
    {
        "name": "knowledge_search",
        "action": Action.KNOWLEDGE_SEARCH.value,
        "description": "Search knowledge visible to the current project.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "topic": {"type": "string"},
                "limit": {"type": "integer", "minimum": 1, "maximum": 200},
            },
        },
        "map": {"topic": "topic", "limit": "limit"},
    },
    {
        "name": "knowledge_read",
        "action": Action.KNOWLEDGE_READ.value,
        "description": "Read one knowledge record that this project may see.",
        "inputSchema": {
            "type": "object",
            "properties": {"record_id": {"type": "string", "minLength": 1}},
            "required": ["record_id"],
        },
        "map": {"record_id": "record_id"},
    },
    {
        "name": "research_run",
        "action": Action.RESEARCH_RUN.value,
        "description": (
            "Run one frozen, isolated research spec inside its declared bounds."
            " It never concludes anything about Alpha or Forward."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {"experiment_id": {"type": "string", "minLength": 1}},
            "required": ["experiment_id"],
        },
        "map": {"experiment_id": "experiment_id"},
    },
)


def build_server(
    *,
    database: Path,
    ticket: str,
    workspace_root: Path | None = None,
    name: str = "kvflow",
) -> Server:
    """Build the MCP server bound to one durable database and one ticket."""
    store = Store(database)
    authority = CapabilityAuthority(store)
    service = ToolService(
        store,
        authority,
        workspace_root=workspace_root or database.parent,
    )
    server: Server = Server(name)

    by_name = {spec["name"]: spec for spec in TOOL_SPECS}

    @server.list_tools()
    async def list_tools() -> list[types.Tool]:
        return [
            types.Tool(
                name=spec["name"],
                description=spec["description"],
                inputSchema=spec["inputSchema"],
            )
            for spec in TOOL_SPECS
        ]

    @server.call_tool()
    async def call_tool(name: str, arguments: dict[str, Any]) -> list[types.ContentBlock]:
        spec = by_name.get(name)
        if spec is None:
            return [
                types.TextContent(
                    type="text",
                    text=json.dumps({"ok": False, "error_code": "UNKNOWN_TOOL", "tool": name}),
                )
            ]
        arguments = dict(arguments or {})
        unknown = set(arguments) - set(spec["map"])
        if unknown:
            return [
                types.TextContent(
                    type="text",
                    text=json.dumps(
                        {
                            "ok": False,
                            "error_code": "CONTRACT_INVALID",
                            "unknown_arguments": sorted(unknown),
                        }
                    ),
                )
            ]
        payload = {
            target: arguments[source]
            for source, target in spec["map"].items()
            if source in arguments
        }
        try:
            result = service.invoke(ticket, spec["action"], payload)
        except V1Error as exc:
            return [
                types.TextContent(
                    type="text",
                    text=json.dumps({"ok": False, "error_code": exc.code, "error": str(exc)}),
                )
            ]
        return [
            types.TextContent(
                type="text",
                text=json.dumps(result.to_dict(), ensure_ascii=False, default=str),
            )
        ]

    return server


async def serve_stdio(
    *, database: Path, ticket: str, workspace_root: Path | None = None
) -> None:
    """Serve the tool groups over stdio until the client disconnects."""
    server = build_server(database=database, ticket=ticket, workspace_root=workspace_root)
    async with stdio_server() as (read, write):
        await server.run(read, write, server.create_initialization_options())


def main() -> int:  # pragma: no cover - exercised through the real stdio test
    import asyncio
    import os
    import sys

    database = os.environ.get(DATABASE_ENV)
    ticket = os.environ.get(CAPABILITY_ENV)
    if not database or not ticket:
        print(f"{DATABASE_ENV} and {CAPABILITY_ENV} are required", file=sys.stderr)
        return 2
    root = os.environ.get(WORKSPACE_ENV)
    asyncio.run(
        serve_stdio(
            database=Path(database),
            ticket=ticket,
            workspace_root=Path(root) if root else None,
        )
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
