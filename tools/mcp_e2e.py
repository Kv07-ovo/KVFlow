"""KVFlow MCP end-to-end: one real stdio session driving the real server.

Proves the standard tool entrance for any host:

* the child is a genuine MCP server process speaking the official SDK protocol;
* discovery lists the nine KVFlow tools;
* project/template/status/result/knowledge reads work over the protocol;
* an unknown project is refused with a typed error instead of being registered;
* **one real workflow is started through MCP and polled to a finished receipt in
  the same session** — with the real model unless ``--no-live`` is passed, in
  which case the live leg is skipped rather than faked.

The session stays open for the whole run on purpose: an MCP stdio server lives as
long as its client session, so a run started inline belongs to that session, while
``--mode process`` hands the run to a detached child that owns its own lifetime.

    set KVFLOW_CREDENTIALS=%USERPROFILE%\\.dsh\\.credentials.yaml
    python tools/mcp_e2e.py --project <project_id> [--mode auto|inline|process]
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from pathlib import Path

PRODUCT = Path(r"C:\Users\90428\Desktop\KVStock-restored\kvflow")
sys.path.insert(0, str(PRODUCT / "src"))

SMOKE = PRODUCT / ".runtime" / "smoke"
HOME = SMOKE / "home"
EXPECTED_TOOLS = (
    "kvflow_control", "kvflow_knowledge", "kvflow_project_status", "kvflow_projects",
    "kvflow_result", "kvflow_runs", "kvflow_start", "kvflow_status", "kvflow_templates",
)


def server_env() -> dict:
    env = {key: value for key, value in os.environ.items()
           if key not in {"KVFLOW_MCP_CAPABILITY"}}
    env["KVFLOW_HOME"] = str(HOME)
    env["PYTHONPATH"] = str(PRODUCT / "src")
    env["PYTHONIOENCODING"] = "utf-8"
    return env


async def scenario(args, receipt: dict) -> None:
    from mcp import ClientSession, StdioServerParameters
    from mcp.client.stdio import stdio_client

    params = StdioServerParameters(
        command=sys.executable, args=["-m", "kvflow.mcp_server", "--home", str(HOME)],
        env=server_env(),
    )
    async with stdio_client(params) as (read, write):
        async with ClientSession(read, write) as client:
            handshake = await client.initialize()
            listed = await client.list_tools()
            receipt["server"] = handshake.serverInfo.name
            receipt["protocol"] = handshake.protocolVersion
            receipt["tools"] = sorted(tool.name for tool in listed.tools)
            if receipt["tools"] != list(EXPECTED_TOOLS):
                receipt["problems"].append(f"unexpected tool surface: {receipt['tools']}")

            async def call(name: str, arguments: dict) -> dict:
                response = await client.call_tool(name, arguments)
                text = "".join(getattr(block, "text", "") for block in response.content)
                return json.loads(text)

            projects = (await call("kvflow_projects", {}))["result"]
            receipt["project_count"] = projects["count"]
            templates = (await call("kvflow_templates", {}))["result"]
            receipt["templates"] = [item["id"] for item in templates["templates"]]
            status = (await call("kvflow_project_status",
                                 {"project_id": args.project}))["result"]
            receipt["approved_scope"] = {
                "write_roots": status["approval"]["write_roots"],
                "profiles": [profile["id"] for profile in status["approval"]["profiles"]],
                "model_profile": status["approval"]["model_profile"],
                "integration_policy": status["approval"]["integration_policy"],
            }
            unknown = await call("kvflow_project_status", {"project_id": "not-registered"})
            receipt["unknown_project_refused"] = {"ok": unknown["ok"],
                                                 "error_code": unknown.get("error_code")}
            if unknown["ok"] is not False:
                receipt["problems"].append("an unknown project was not refused")

            if args.no_live:
                receipt["live_run"] = {"started": False, "reason": "--no-live"}
                return

            start = await call("kvflow_start", {
                "requirement": args.requirement, "project_id": args.project,
                "template": args.template, "mode": args.mode,
            })
            if start.get("ok") is not True:
                receipt["start"] = {"ok": False, "error": start}
                receipt["problems"].append("kvflow_start failed")
                return
            started = start["result"]
            job_id = started["job_id"]
            receipt["job_id"] = job_id
            receipt["start"] = {
                key: started.get(key) for key in
                ("job_id", "template", "model_profile", "plan_source", "nodes",
                 "acceptance", "runner")
            }
            seen: list[dict] = []
            final = None
            deadline = time.monotonic() + args.timeout
            while time.monotonic() < deadline:
                poll = (await call("kvflow_result", {"job_id": job_id}))["result"]
                if poll.get("finished"):
                    final = poll
                    break
                seen.append({"job_state": poll.get("state"), "nodes": poll.get("nodes")})
                await asyncio.sleep(3.0)
            receipt["poll_samples"] = seen[:10]
            receipt["poll_samples_last"] = seen[-3:] if seen else []
            receipt["poll_count"] = len(seen)
            if final is None:
                receipt["problems"].append("the run did not finish inside the timeout")
                return
            receipt["result"] = {
                key: final.get(key) for key in
                ("finished", "template", "plan_source", "status", "problems",
                 "node_states_final", "test_receipts", "review", "integration",
                 "source_not_modified", "worker_totals", "budget", "diff")
            }
            findings = (final.get("review") or {}).get("findings")
            receipt["review_verdict"] = (final.get("review") or {}).get("verdict")
            receipt["review_findings"] = findings
            if final.get("status") != "PASS":
                receipt["problems"].append(f"the live run finished with {final.get('status')}")
            knowledge = (await call("kvflow_knowledge",
                                    {"project_id": args.project, "limit": 10}))["result"]
            receipt["knowledge_records"] = len(knowledge["records"])


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", required=True)
    parser.add_argument("--template", default="feature")
    parser.add_argument("--requirement", default=(
        "add a multiply(a, b) helper to src/calc.py and prove it with the registered"
        " test profile"
    ))
    parser.add_argument("--timeout", type=float, default=1200.0)
    parser.add_argument("--no-live", action="store_true")
    parser.add_argument("--mode", choices=["auto", "inline", "process"], default="inline",
                        help="how the MCP server should run the workflow")
    args = parser.parse_args()

    receipt: dict = {
        "kind": "KVFLOW_MCP_E2E",
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "home": str(HOME),
        "project_id": args.project,
        "live_model": not args.no_live,
        "mode": args.mode,
        "problems": [],
    }
    asyncio.run(scenario(args, receipt))
    receipt["status"] = "PASS" if not receipt["problems"] else "PARTIAL"
    receipt["ended_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    out = PRODUCT / ".runtime" / "receipts"
    out.mkdir(parents=True, exist_ok=True)
    path = out / "kvflow-mcp-e2e.json"
    path.write_text(json.dumps(receipt, indent=2, ensure_ascii=False, default=str),
                    encoding="utf-8")
    print(json.dumps(receipt, ensure_ascii=False, indent=2, default=str)[:3000])
    print("receipt:", path)
    return 0 if receipt["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
