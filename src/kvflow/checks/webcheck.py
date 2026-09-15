"""Web source checker: a deterministic executor profile for an ESM/TypeScript project.

A web project's strongest proof is running its own test runner. When the runtime
for that is not installed on the machine (no ``node``, no ``deno``, no ``bun``),
the honest options are to refuse the node or to substitute a check that is real,
reproducible and clearly weaker. This checker is the second option, and it says so
in its own report: it parses the sources, it does not execute them.

What it verifies, all from the files on disk in the node's owned workspace:

``--require-export FILE:NAME``
    the file declares ``NAME`` as a top-level export;
``--require-import FILE:SPECIFIER:NAME``
    the file imports ``NAME`` from ``SPECIFIER`` **and** that specifier resolves to
    a workspace file which itself exports ``NAME``;
``--require-test FILE:NAME``
    the file declares at least one test and its body actually calls ``NAME``;
``--require-token FILE:TOKEN``
    the file contains the literal token (a hand-checkable escape hatch).

Exit code 0 means every requirement held, 1 means at least one did not, 2 means
the invocation itself was malformed. One JSON report is printed on stdout, so the
receipt records exactly which requirement failed.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from pathlib import Path
from typing import Any

MAX_FILE_BYTES = 2 * 1024 * 1024

_TEST_CALL = re.compile(r"\b(?:test|it|describe)\s*\(\s*[`'\"]([^`'\"]*)[`'\"]", re.M)
_IMPORT_BLOCK = re.compile(
    r"^\s*import\s+(?:(?P<clause>[^;]+?))\s+from\s+[`'\"](?P<spec>[^`'\"]+)[`'\"]\s*;?",
    re.M,
)
_EXPORT_DECL = re.compile(
    r"^\s*export\s+(?:async\s+)?(?:function|class|const|let|var)\s+([A-Za-z_$][\w$]*)",
    re.M,
)
_EXPORT_LIST = re.compile(r"^\s*export\s*\{([^}]*)\}", re.M)
_IDENTIFIER = re.compile(r"^[A-Za-z_$][\w$]*$")


def _read(root: Path, relative: str) -> tuple[Path | None, str, str | None]:
    """Return (resolved path, text, problem) for one workspace-relative file."""
    resolved_root = root.resolve()
    candidate = (root / relative)
    try:
        resolved = candidate.resolve()
        resolved.relative_to(resolved_root)
    except (ValueError, OSError):
        return None, "", "escapes the workspace"
    if not resolved.is_file():
        return None, "", "missing"
    data = resolved.read_bytes()
    if len(data) > MAX_FILE_BYTES:
        return None, "", f"larger than {MAX_FILE_BYTES} bytes"
    try:
        return resolved, data.decode("utf-8"), None
    except UnicodeDecodeError:
        return None, "", "is not valid UTF-8"


def _exported_names(text: str) -> set[str]:
    names = set(_EXPORT_DECL.findall(text))
    for match in _EXPORT_LIST.finditer(text):
        for part in match.group(1).split(","):
            candidate = part.split(" as ")[-1].strip()
            if _IDENTIFIER.match(candidate):
                names.add(candidate)
    return names


def _imported_names(text: str) -> list[tuple[str, set[str]]]:
    """Every ``from`` import as ``(specifier, imported names)``."""
    rows: list[tuple[str, set[str]]] = []
    for match in _IMPORT_BLOCK.finditer(text):
        clause = match.group("clause")
        names: set[str] = set()
        braces = re.search(r"\{([^}]*)\}", clause)
        if braces:
            for part in braces.group(1).split(","):
                candidate = part.split(" as ")[-1].strip()
                if _IDENTIFIER.match(candidate):
                    names.add(candidate)
        default = clause.split(",")[0].strip()
        if _IDENTIFIER.match(default):
            names.add(default)
        rows.append((match.group("spec"), names))
    return rows


def _resolve_specifier(source_file: Path, specifier: str) -> Path | None:
    if not specifier.startswith("."):
        return None
    base = source_file.parent / specifier
    for candidate in (
        base,
        base.with_suffix(".mjs"),
        base.with_suffix(".js"),
        base.with_suffix(".cjs"),
        base.with_suffix(".ts"),
        base / "index.mjs",
        base / "index.js",
    ):
        if candidate.is_file():
            return candidate
    return None


def _declares_a_call(text: str, name: str) -> tuple[bool, int, list[str]]:
    """At least one declared test, and at least one call of ``name`` in the file."""
    tests = _TEST_CALL.findall(text)
    calls = len(re.findall(rf"(?<![\w$.]){re.escape(name)}\s*\(", text))
    return bool(tests) and calls > 0, calls, tests


def _check_one(requirement: dict[str, Any], root: Path) -> tuple[dict[str, Any], str | None]:
    kind = str(requirement.get("kind", ""))
    relative = str(requirement.get("file", ""))
    path, text, problem = _read(root, relative)
    entry: dict[str, Any] = {"kind": kind, "file": relative}
    if problem:
        entry.update({"ok": False, "problem": problem})
        return entry, problem
    assert path is not None
    entry["bytes"] = len(text.encode("utf-8"))
    entry["sha256"] = hashlib.sha256(text.encode("utf-8")).hexdigest()

    if kind == "export":
        name = str(requirement["name"])
        found = name in _exported_names(text)
        entry.update({"name": name, "ok": found, "exports": sorted(_exported_names(text))})
        return entry, None if found else f"{name} is not a top-level export"

    if kind == "import":
        name = str(requirement["name"])
        specifier = str(requirement["specifier"])
        imported = any(
            names and name in names for spec, names in _imported_names(text) if spec == specifier
        )
        resolved = _resolve_specifier(path, specifier)
        exporter_exports: set[str] = set()
        resolved_relative: str | None = None
        if resolved is not None:
            # report the resolved path the way a reader expects it, without the
            # '..' the import specifier happened to contain
            resolved_relative = resolved.resolve().relative_to(root.resolve()).as_posix()
            _path, exporter_text, _problem = _read(root, resolved_relative)
            if exporter_text:
                exporter_exports = _exported_names(exporter_text)
        ok = bool(imported and resolved is not None and name in exporter_exports)
        entry.update({
            "name": name, "specifier": specifier, "ok": ok, "imported": imported,
            "resolved": resolved_relative, "resolved_exports": sorted(exporter_exports),
        })
        return entry, None if ok else (
            f"{name} is not imported from {specifier}, or that specifier does not"
            " resolve to a workspace file exporting it"
        )

    if kind == "test":
        name = str(requirement["name"])
        ok, calls, tests = _declares_a_call(text, name)
        entry.update({"name": name, "ok": ok, "call_count": calls, "tests": tests})
        return entry, None if ok else (
            f"no declared test calls {name} (tests found: {len(tests)})"
        )

    if kind == "token":
        token = str(requirement["token"])
        found = token in text
        entry.update({"token": token, "ok": found})
        return entry, None if found else f"the literal {token!r} is absent"

    entry.update({"ok": False, "problem": "unknown requirement kind"})
    return entry, "unknown requirement kind"


def check(requirements: list[dict[str, Any]], *, root: Path) -> dict[str, Any]:
    entries: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    for requirement in requirements:
        entry, problem = _check_one(requirement, root)
        entries.append(entry)
        if problem:
            failures.append({"kind": entry["kind"], "file": entry["file"],
                             "problem": problem})
    return {
        "kind": "WEB_SOURCE_CHECK",
        "root": str(root),
        "executes_the_code": False,
        "note": ("this is a source-level check: it verifies the exports, the import"
                 " graph and the tests that exercise them, but it does not run a web"
                 " test runner"),
        "checked": len(requirements),
        "files": entries,
        "failures": failures,
        "ok": not failures,
    }


def _pair(value: str, flag: str) -> tuple[str, str]:
    file, _, subject = value.partition(":")
    if not file or not subject:
        raise argparse.ArgumentTypeError(f"{flag} expects FILE:NAME")
    return file, subject


def _triple(value: str, flag: str) -> tuple[str, str, str]:
    file, _, rest = value.partition(":")
    specifier, _, name = rest.partition(":")
    if not file or not specifier or not name:
        raise argparse.ArgumentTypeError(f"{flag} expects FILE:SPECIFIER:NAME")
    return file, specifier, name


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="kvflow-web-check")
    parser.add_argument("--require-export", action="append", default=[],
                        help="FILE:NAME")
    parser.add_argument("--require-import", action="append", default=[],
                        help="FILE:SPECIFIER:NAME")
    parser.add_argument("--require-test", action="append", default=[],
                        help="FILE:NAME")
    parser.add_argument("--require-token", action="append", default=[],
                        help="FILE:TOKEN")
    parser.add_argument("--root", default=".")
    args = parser.parse_args(argv)
    requirements: list[dict[str, Any]] = []
    try:
        for value in args.require_export:
            file, name = _pair(value, "--require-export")
            requirements.append({"kind": "export", "file": file, "name": name})
        for value in args.require_import:
            file, specifier, name = _triple(value, "--require-import")
            requirements.append({"kind": "import", "file": file,
                                 "specifier": specifier, "name": name})
        for value in args.require_test:
            file, name = _pair(value, "--require-test")
            requirements.append({"kind": "test", "file": file, "name": name})
        for value in args.require_token:
            file, token = _pair(value, "--require-token")
            requirements.append({"kind": "token", "file": file, "token": token})
    except argparse.ArgumentTypeError as exc:
        print(json.dumps({"kind": "WEB_SOURCE_CHECK", "ok": False, "error": str(exc)}))
        return 2
    if not requirements:
        print(json.dumps({"kind": "WEB_SOURCE_CHECK", "ok": False,
                          "error": "at least one --require-* is needed"}))
        return 2
    report = check(requirements, root=Path(args.root))
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
