"""Artifact checker: the executor evidence for a documentation or data deliverable.

A documentation or data project has no code build to run, and pretending it does
would be dishonest. What *can* be verified mechanically is the artifact itself:
that the declared outputs exist inside the node's owned workspace, that they are
non-empty, and that each one can be recorded by digest. That is what this checker
does, and its exit code is the receipt the review judges.

It is invoked through a pre-registered ``argv`` profile that the user approved
with the project, for example:

    python -m kvflow.checks.artifact --require docs/report.md:200 --require out/data.csv

Each ``--require`` is ``relative-path[:minimum-bytes]``. The checker never writes,
never reads outside the workspace and reports one JSON object on stdout.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path


def _parse_requirement(value: str) -> tuple[str, int]:
    path, _, minimum = value.rpartition(":")
    if not path:
        path, minimum = value, "1"
    try:
        return path, max(1, int(minimum))
    except ValueError:
        return value, 1


def check(requirements: list[tuple[str, int]], *, root: Path) -> dict:
    entries = []
    failures = []
    for relative, minimum in requirements:
        candidate = (root / relative).resolve()
        try:
            candidate.relative_to(root.resolve())
        except ValueError:
            failures.append({"path": relative, "problem": "escapes the workspace"})
            continue
        if not candidate.is_file():
            failures.append({"path": relative, "problem": "missing"})
            continue
        data = candidate.read_bytes()
        digest = hashlib.sha256(data).hexdigest()
        entry = {"path": relative, "bytes": len(data), "sha256": digest,
                 "minimum_bytes": minimum}
        if len(data) < minimum:
            entry["problem"] = f"smaller than the declared minimum ({minimum} bytes)"
            failures.append({"path": relative, "problem": entry["problem"]})
        entries.append(entry)
    return {
        "kind": "ARTIFACT_CHECK",
        "root": str(root),
        "checked": len(requirements),
        "artifacts": entries,
        "failures": failures,
        "ok": not failures,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="kvflow-artifact-check")
    parser.add_argument("--require", action="append", default=[],
                        help="relative-path[:minimum-bytes]")
    parser.add_argument("--root", default=".")
    args = parser.parse_args(argv)
    if not args.require:
        print(json.dumps({"kind": "ARTIFACT_CHECK", "ok": False,
                          "error": "at least one --require is needed"}))
        return 2
    report = check([_parse_requirement(value) for value in args.require],
                   root=Path(args.root))
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
