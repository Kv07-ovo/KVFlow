"""Prove the original KVStock product was never touched by KVFlow work.

Two independent obligations, both checked here against the real trees:

**Protection.** A fingerprint of the protected trees of the original product
(``agent_os`` sources and the release revision, the Forward/Golden/research
material and the schedules store) is taken as a baseline, and every operation
under test runs between the baseline and the verification. The verdict is
``unchanged`` only when the manifest hash is identical, and a difference is
reported file by file - never summarised away.

**Read-only adaptation.** The KVStock adapter is enabled against the real tree and
reads through it with the product's own reader. The adapted tree must fingerprint
identically afterwards, and a project that names the KVStock root as a protected
path must have a write there refused by the core's own path policy.

Nothing in this file writes inside the protected trees. The receipt goes to
``.runtime/receipts/kvstock-protection.json``; the baseline is a separate file so a
second run can be compared against the original bytes rather than against itself.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
from pathlib import Path

PRODUCT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PRODUCT / "src"))

from kvflow import registry  # noqa: E402
from kvflow.adapters import kvstock_adapter as adapter  # noqa: E402
from kvflow.adapters import registry as optin  # noqa: E402
from kvflow.adapters.kvstock import KvStockReaderError  # noqa: E402
from kvflow.core.errors import V1Error  # noqa: E402
from kvflow.core.security import PathPolicy  # noqa: E402
from kvflow.core.workspace import WorkspaceManager  # noqa: E402

PLATFORM = Path(r"C:\Users\90428\Desktop\KVStock-restored\kvstock-platform")
RECEIPTS = PRODUCT / ".runtime" / "receipts"
BASELINE = RECEIPTS / "kvstock-protection-baseline.json"
OUT = RECEIPTS / "kvstock-protection.json"
RUNTIME_HOME = PRODUCT / ".runtime" / "protection-home"

#: the trees that must never change. Runtime scratch (``.agent_os_runtime``) is
#: deliberately excluded: it belongs to the v1 product's own soak, which is still
#: writing there, and it is protected by that product's own revision fingerprint.
PROTECTED = {
    "agent_os_release_src": PLATFORM / "agent_os" / "releases" / "1.0.0-rc1" / "src",
    "agent_os_release_tests": PLATFORM / "agent_os" / "releases" / "1.0.0-rc1" / "tests",
    "agent_os_platform_src": PLATFORM / "agent_os" / "src",
    "kvstock_package": PLATFORM / "kvstock",
    "golden_reference": PLATFORM / "golden_reference",
    "data": PLATFORM / "data",
}

#: substrings whose presence means a tree is not part of the frozen product
SKIP = (".git", "__pycache__", ".pytest_cache", ".pytest-basetemp", ".pytest-",
        ".mypy_cache", ".ruff_cache", ".runtime", ".bootstrap", ".recovery",
        ".agent_os_runtime", ".kvflow", "node_modules", ".venv", ".kvflow_logs")

#: a fingerprint is bounded so a proof run cannot take unbounded time
MAX_FILES_PER_TREE = 20000


def log(message: str) -> None:
    print(f"[protection] {message}", flush=True)


def fingerprint_tree(root: Path) -> dict:
    if not root.is_dir():
        return {"root": str(root), "missing": True, "files": 0,
                "manifest_sha256": None, "entries": {}}
    entries: dict[str, str] = {}
    capped = False
    for path in sorted(root.rglob("*")):
        if not path.is_file() or path.is_symlink():
            continue
        parts = set(path.relative_to(root).parts)
        if any(any(s in part for s in SKIP) for part in parts):
            continue
        if len(entries) >= MAX_FILES_PER_TREE:
            capped = True
            break
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        entries[path.relative_to(root).as_posix()] = digest
    manifest = hashlib.sha256(
        json.dumps(entries, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return {"root": str(root), "missing": False, "files": len(entries),
            "capped": capped, "manifest_sha256": manifest, "entries": entries}


def fingerprint_all() -> dict:
    return {name: fingerprint_tree(root) for name, root in PROTECTED.items()}


def compare(before: dict, after: dict) -> dict:
    problems: list[dict] = []
    for name in sorted(set(before) | set(after)):
        old = before.get(name, {})
        new = after.get(name, {})
        if old.get("manifest_sha256") != new.get("manifest_sha256"):
            old_entries = old.get("entries", {})
            new_entries = new.get("entries", {})
            problems.append({
                "tree": name,
                "root": new.get("root") or old.get("root"),
                "changed": sorted(k for k in old_entries
                                  if k in new_entries and old_entries[k] != new_entries[k]),
                "missing": sorted(k for k in old_entries if k not in new_entries),
                "added": sorted(k for k in new_entries if k not in old_entries),
                "before_manifest": old.get("manifest_sha256"),
                "after_manifest": new.get("manifest_sha256"),
            })
    return {
        "unchanged": not problems,
        "problems": problems,
        "trees_checked": len(after),
        "files_checked": sum(int(tree.get("files") or 0) for tree in after.values()),
    }


def find_a_journal(root: Path) -> Path | None:
    """A real KVStock journal database inside the tree, if one is present."""
    for candidate in sorted(root.rglob("*.sqlite3")):
        if any(s in candidate.as_posix() for s in SKIP):
            continue
        try:
            import sqlite3

            connection = sqlite3.connect(f"file:{candidate.as_posix()}?mode=ro", uri=True)
            try:
                names = {
                    row[0] for row in connection.execute(
                        "SELECT name FROM sqlite_master WHERE type = 'table'")
                }
            finally:
                connection.close()
        except Exception:  # noqa: BLE001 - a candidate that cannot be opened is not a journal
            continue
        if "records" in names:
            return candidate
    return None


def adapter_read_proof(receipt: dict) -> None:
    """Enable the adapter against the real tree and read through it once."""
    tree = PLATFORM / "agent_os"
    journal = find_a_journal(tree)
    # a journal is preferred; when the tree holds none, one real source file of the
    # frozen product is registered as a text source instead
    text_source = None
    if journal is None:
        for candidate in ("README.md", "pyproject.toml", "requirements.txt"):
            path = PLATFORM / "agent_os" / "releases" / "1.0.0-rc1" / candidate
            if path.is_file():
                text_source = path.relative_to(tree)
                break
    sources = []
    if journal is not None:
        sources = [{
            "key": "agent-os-journal",
            "kind": "journal",
            "path": str(journal.relative_to(tree)).replace("\\", "/"),
            "description": "a real KVStock journal, read denied-by-default",
            "allowed_prefixes": ["STATUS", "TASK", "WORKFLOW", "RUN"],
            "permitted_empty_prefix": False,
            "lifecycle_prefix": "STATUS",
        }]
    elif text_source is not None:
        sources = [{
            "key": "frozen-release-file",
            "kind": "text",
            "path": text_source.as_posix(),
            "description": "one file of the frozen release, read-only",
            "max_bytes": 262144,
        }]
    entry = optin.enable(
        RUNTIME_HOME, "kvstock",
        options={"root": str(tree), "sources": sources},
        reason="protection proof: read-only adaptation of the real tree",
    )
    receipt["adapter"] = {
        "enabled": True,
        "root": entry["options"].get("root"),
        "journal_found": journal is not None,
        "source": (str(journal) if journal else str(text_source)),
    }
    if not sources:
        receipt["adapter"]["read"] = "no registered source was available in the tree"
        receipt["problems"].append("the adapter read proof found nothing to read")
        return
    described = adapter.describe(entry)
    receipt["adapter"]["sources"] = sorted(described["sources"])
    if journal is None:
        chunk = adapter.read(entry, "frozen-release-file", offset=0, limit=200)
        receipt["adapter"]["read"] = {
            "kind": "text", "bytes": chunk.get("output_bytes"),
            "sha256": chunk.get("content_sha256"), "truncated": chunk.get("truncated"),
            "freshness": chunk.get("freshness"),
        }
        return
    # only a registered prefix may be paged: an empty-prefix scan is refused by
    # design, which is itself recorded as the first refusal of this proof
    try:
        adapter.read(entry, "agent-os-journal", prefix="", page_limit=5)
    except KvStockReaderError as exc:
        receipt["adapter"]["empty_prefix_refused"] = f"{type(exc).__name__}: {exc}"
    else:
        receipt["problems"].append("an unauthorized empty-prefix scan was allowed")
    page = adapter.read(entry, "agent-os-journal", prefix="STATUS", page_limit=5)
    receipt["adapter"]["read"] = {
        "prefix": "STATUS",
        "records": len(page.get("records") or []),
        "next_cursor": page.get("next_cursor"),
        "truncated": page.get("truncated"),
        "integrity_scope": page.get("integrity_scope"),
        "freshness": page.get("freshness"),
        "first_body_digest": (
            hashlib.sha256(json.dumps(
                (page.get("records") or [{}])[0].get("body"),
                sort_keys=True, default=str).encode("utf-8")).hexdigest()
            if page.get("records") else None
        ),
    }
    receipt["adapter"]["lifecycle"] = adapter.status(entry)


def refusal_proof(receipt: dict) -> None:
    """The product's own policy refuses writes where the product is protected.

    Two real refusals, both raised by shipped code rather than by this script:
    a project configuration whose write root overlaps a protected root is invalid,
    and the runtime path policy refuses a write scope inside a protected root.
    """
    source = RUNTIME_HOME / "refusal-source"
    (source / "docs").mkdir(parents=True, exist_ok=True)
    (source / "agent_os").mkdir(parents=True, exist_ok=True)
    (source / "docs" / "notes.md").write_text("# notes\n", encoding="utf-8")
    (source / "agent_os" / "frozen.py").write_text("FROZEN = True\n", encoding="utf-8")

    attempts: list[dict] = []
    # 1. onboarding refuses a write root that overlaps a protected path
    try:
        registry.draft(
            source, display_name="refusal-proof", template="docs_or_data",
            project_id="refusal-proof", write_roots=["agent_os"],
            protected_paths=["agent_os"],
        )
    except Exception as exc:  # noqa: BLE001 - the refusal is the evidence
        attempts.append({"check": "draft write root inside a protected path",
                         "refused": True, "error": f"{type(exc).__name__}: {exc}"})
    else:
        attempts.append({"check": "draft write root inside a protected path",
                         "refused": False})

    # 2. the runtime path policy refuses a write scope inside a protected root
    config = registry.draft(
        source, display_name="refusal-proof", template="docs_or_data",
        project_id="refusal-proof", write_roots=["docs"],
        protected_paths=["agent_os"],
    )
    registry.write_config(config, approve=True)
    project, _config = registry.compile_project(registry.read_config(source),
                                                home=RUNTIME_HOME)
    policy = PathPolicy(project)
    for relative in ("agent_os", "agent_os/frozen.py", "docs"):
        try:
            policy.assert_writable(relative)
        except V1Error as exc:
            attempts.append({"check": f"write scope {relative}", "refused": True,
                             "error": f"{type(exc).__name__}: {exc}",
                             "code": getattr(exc, "code", None)})
        else:
            attempts.append({"check": f"write scope {relative}", "refused": False})
    receipt["refusal"] = {
        "attempts": attempts,
        "all_refused": (all(item["refused"] for item in attempts[:-1])
                        and attempts[-1]["refused"] is False),
        "managed_root": str(policy.managed_root),
        "source_root": str(policy.source_root),
    }


def main() -> int:
    parser = argparse.ArgumentParser(prog="kvstock-protection-proof")
    parser.add_argument("--refresh-baseline", action="store_true",
                        help="replace the stored baseline with the current bytes")
    parser.add_argument("--baseline-only", action="store_true")
    args = parser.parse_args()

    RECEIPTS.mkdir(parents=True, exist_ok=True)
    RUNTIME_HOME.mkdir(parents=True, exist_ok=True)
    started = time.time()
    receipt: dict = {
        "kind": "KVFLOW_KVSTOCK_PROTECTION_PROOF",
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "platform": str(PLATFORM),
        "protected_trees": {name: str(root) for name, root in PROTECTED.items()},
        "problems": [],
    }

    if BASELINE.is_file() and not args.refresh_baseline:
        baseline = json.loads(BASELINE.read_text(encoding="utf-8"))
        log(f"baseline from {baseline['created_at']}")
    else:
        baseline = {"created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                    "trees": fingerprint_all()}
        BASELINE.write_text(json.dumps(baseline, indent=2), encoding="utf-8")
        log(f"baseline written to {BASELINE}")
    receipt["baseline"] = {"created_at": baseline["created_at"],
                           "file": str(BASELINE),
                           "manifests": {
                               name: tree["manifest_sha256"]
                               for name, tree in baseline["trees"].items()}}
    missing = sorted(name for name, tree in baseline["trees"].items() if tree["missing"])
    if missing:
        receipt["problems"].append(f"protected trees that do not exist: {missing}")
    if args.baseline_only:
        print(json.dumps({"baseline": receipt["baseline"], "missing": missing}, indent=2))
        return 0

    # ---- operations under test, in order, between baseline and verification ----
    log("reading through the enabled adapter (read-only) ...")
    try:
        adapter_read_proof(receipt)
    except V1Error as exc:
        receipt["problems"].append(f"the adapter read failed: {type(exc).__name__}: {exc}")
    log("asking the core's path policy to write inside the protected root ...")
    try:
        refusal_proof(receipt)
    except V1Error as exc:
        receipt["problems"].append(f"the refusal proof failed: {type(exc).__name__}: {exc}")
    if not receipt.get("refusal", {}).get("all_refused", False):
        receipt["problems"].append("a write into the protected product root was not refused")

    log("verifying the protected trees against the baseline ...")
    after = fingerprint_all()
    receipt["verification"] = compare(baseline["trees"], after)
    if not receipt["verification"]["unchanged"]:
        receipt["problems"].append("a protected tree changed")
    receipt["ended_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    receipt["seconds"] = round(time.time() - started, 2)
    receipt["status"] = "PASS" if not receipt["problems"] else "FAILED"
    OUT.write_text(json.dumps(receipt, indent=2, ensure_ascii=False), encoding="utf-8")
    log(f"status={receipt['status']} files_checked="
        f"{receipt['verification']['files_checked']} receipt={OUT}")
    print(json.dumps({key: receipt[key] for key in
                      ("status", "problems", "verification", "adapter", "refusal")
                      if key in receipt}, ensure_ascii=False, indent=2)[:2500])
    return 0 if receipt["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
