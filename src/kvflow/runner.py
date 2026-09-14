"""Background runner: execute one prepared KVFlow run in its own process.

``kvflow.mcp_server`` and the DSH plugin start a run by *preparing* it (durable
job, plan, budget chain) and then launching this module as a detached child. That
keeps three promises honest:

* a long run does not hold an MCP session or a model turn open while it works;
* the run survives the host that started it, because the child owns its own
  process and the durable state lives in SQLite;
* cancelling a run can stop exactly that process, and nothing else, because its
  pid is recorded in the run manifest.

    python -m kvflow.runner --home <runtime> --job <job_id>
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from . import workflow
from .core.errors import V1Error


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="kvflow-runner")
    parser.add_argument("--home", required=True)
    parser.add_argument("--job", required=True)
    parser.add_argument("--max-steps", type=int, default=10)
    parser.add_argument("--max-completion-tokens", type=int, default=1024)
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args(argv)

    try:
        receipt = workflow.execute_run(
            job_id=args.job, home=args.home, max_steps=args.max_steps,
            max_completion_tokens=args.max_completion_tokens,
        )
    except V1Error as exc:
        print(json.dumps({"job_id": args.job, "status": "REFUSED",
                          "error": exc.to_dict()}, ensure_ascii=False), file=sys.stderr)
        return 1
    if not args.quiet:
        print(json.dumps({
            "job_id": receipt.get("job_id"),
            "status": receipt.get("status"),
            "problems": receipt.get("problems"),
            "node_states": receipt.get("node_states_final"),
            "review": (receipt.get("review") or {}).get("verdict"),
            "receipts": receipt.get("test_receipts"),
        }, ensure_ascii=False, default=str))
    return 0 if receipt.get("status") == "PASS" else 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
