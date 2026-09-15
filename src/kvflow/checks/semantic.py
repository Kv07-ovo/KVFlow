"""Semantic validator: compare observations of shared facts against a contract.

A project that declares a semantic contract needs a *deterministic* way to check
that its modules still agree with it. This checker is that tool, and it is
deliberately literal: it reads the values the sources actually declare and compares
them with the value the contract declares. It never asks a model whether two things
"look consistent".

Contract document (JSON)::

    {
      "contract_id": "USER_STATUS",
      "version": 2,
      "entities": {"User.status": {"enum": ["active", "disabled"]}},
      "values":   {"User.age_type": {"value": "number"}}
    }

Observations:

``--enum ENTITY=LABEL=FILE:SYMBOL``
    read the list literal assigned to ``SYMBOL`` in ``FILE``. Python
    (``SYMBOL = ["a"]``), JavaScript (``export const SYMBOL = ['a']``) and JSON
    (``"SYMBOL": ["a"]``) literals are understood; anything else is a refusal, not a
    guess.
``--value ENTITY=LABEL=FILE:POINTER``
    read one JSON value at a dotted pointer (``a.b`` or ``a.0``) and compare it.

Exit code 0 means every observation matches the contract, 1 means at least one does
not, 2 means the invocation or an input was unusable. One JSON report goes to
stdout, so the receipt records exactly which module disagreed and how.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any

MAX_FILE_BYTES = 2 * 1024 * 1024


def _read_text(path: Path) -> tuple[str | None, str | None]:
    if not path.is_file():
        return None, "missing"
    data = path.read_bytes()
    if len(data) > MAX_FILE_BYTES:
        return None, f"larger than {MAX_FILE_BYTES} bytes"
    try:
        return data.decode("utf-8"), None
    except UnicodeDecodeError:
        return None, "is not valid UTF-8"


def _literal_at(text: str, symbol: str) -> tuple[Any, str | None]:
    """The list literal assigned to ``symbol``, whatever the language it is written in."""
    patterns = (
        rf"^\s*(?:export\s+)?(?:const|let|var)\s+{re.escape(symbol)}\s*=\s*(\[[^\]]*\])",
        rf"^\s*{re.escape(symbol)}\s*[:=]\s*(\[[^\]]*\])",
        rf'"{re.escape(symbol)}"\s*:\s*(\[[^\]]*\])',
    )
    for pattern in patterns:
        match = re.search(pattern, text, re.M)
        if not match:
            continue
        raw = match.group(1)
        try:
            return json.loads(raw.replace("'", '"')), None
        except ValueError:
            body = raw.strip("[]")
            items = [part.strip().strip("'\"") for part in body.split(",") if part.strip()]
            return items, None
    return None, f"no list literal named {symbol} was found"


def _pointer(document: Any, pointer: str) -> tuple[Any, str | None]:
    current = document
    for part in pointer.split("."):
        if isinstance(current, dict):
            if part not in current:
                return None, f"{part} is absent"
            current = current[part]
        elif isinstance(current, list):
            try:
                current = current[int(part)]
            except (ValueError, IndexError):
                return None, f"{part} is out of range"
        else:
            return None, f"{part} cannot be traversed"
    return current, None


def _expected(contract: dict[str, Any], entity: str) -> tuple[Any, str | None]:
    entities = contract.get("entities") or {}
    if entity in entities and isinstance(entities[entity], dict) and "enum" in entities[entity]:
        return entities[entity]["enum"], None
    values = contract.get("values") or {}
    if entity in values and isinstance(values[entity], dict) and "value" in values[entity]:
        return values[entity]["value"], None
    return None, f"the contract declares no fact named {entity}"


def check(*, contract: dict[str, Any], root: Path,
          enums: list[tuple[str, str, str, str]],
          values: list[tuple[str, str, str, str]]) -> dict[str, Any]:
    observations: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []

    for entity, label, relative, symbol in enums:
        expected, problem = _expected(contract, entity)
        path = root / relative
        text, read_problem = _read_text(path)
        entry = {"entity": entity, "label": label, "source": relative,
                 "kind": "enum", "symbol": symbol, "expected": expected}
        if problem or read_problem:
            entry.update({"ok": False, "problem": problem or read_problem})
        else:
            observed, extract_problem = _literal_at(text or "", symbol)
            entry["observed"] = observed
            same = (isinstance(observed, list) and isinstance(expected, list)
                    and sorted(str(item) for item in observed)
                    == sorted(str(item) for item in expected))
            entry["ok"] = bool(same and not extract_problem)
            if extract_problem:
                entry["problem"] = extract_problem
            elif not same:
                entry["problem"] = (f"{label} declares {observed} but the contract"
                                    f" declares {expected}")
        observations.append(entry)
        if not entry["ok"]:
            failures.append({"entity": entity, "label": label, "source": relative,
                             "problem": entry.get("problem")})

    for entity, label, relative, pointer in values:
        expected, problem = _expected(contract, entity)
        path = root / relative
        text, read_problem = _read_text(path)
        entry = {"entity": entity, "label": label, "source": relative,
                 "kind": "value", "pointer": pointer, "expected": expected}
        if problem or read_problem:
            entry.update({"ok": False, "problem": problem or read_problem})
        else:
            try:
                document = json.loads(text or "")
            except ValueError as exc:
                entry.update({"ok": False, "problem": f"not JSON: {exc}"})
                document = None
            if document is not None:
                observed, pointer_problem = _pointer(document, pointer)
                entry["observed"] = observed
                entry["ok"] = bool(observed == expected and not pointer_problem)
                if pointer_problem:
                    entry["problem"] = pointer_problem
                elif observed != expected:
                    entry["problem"] = (f"{label} declares {observed!r} but the contract"
                                        f" declares {expected!r}")
        observations.append(entry)
        if not entry["ok"]:
            failures.append({"entity": entity, "label": label, "source": relative,
                             "problem": entry.get("problem")})

    return {
        "kind": "SEMANTIC_CONTRACT_CHECK",
        "contract_id": contract.get("contract_id"),
        "contract_version": contract.get("version"),
        "executes_the_code": False,
        "note": ("this compares declared facts with declared observations; it does not"
                 " run the modules and it does not judge anything a model wrote"),
        "checked": len(observations),
        "observations": observations,
        "failures": failures,
        "ok": not failures,
    }


def _enum_arg(value: str) -> tuple[str, str, str, str]:
    entity, _, rest = value.partition("=")
    label, _, source = rest.partition("=")
    relative, _, symbol = source.rpartition(":")
    if not (entity and label and relative and symbol):
        raise argparse.ArgumentTypeError("--enum expects ENTITY=LABEL=FILE:SYMBOL")
    return entity, label, relative, symbol


def _value_arg(value: str) -> tuple[str, str, str, str]:
    entity, _, rest = value.partition("=")
    label, _, source = rest.partition("=")
    relative, _, pointer = source.rpartition(":")
    if not (entity and label and relative and pointer):
        raise argparse.ArgumentTypeError("--value expects ENTITY=LABEL=FILE:POINTER")
    return entity, label, relative, pointer


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="kvflow-semantic-check")
    parser.add_argument("--contract", required=True)
    parser.add_argument("--root", default=".")
    parser.add_argument("--enum", action="append", default=[], type=_enum_arg)
    parser.add_argument("--value", action="append", default=[], type=_value_arg)
    args = parser.parse_args(argv)
    try:
        contract = json.loads(Path(args.contract).read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        print(json.dumps({"kind": "SEMANTIC_CONTRACT_CHECK", "ok": False,
                          "error": f"the contract is unreadable: {exc}"}))
        return 2
    if not args.enum and not args.value:
        print(json.dumps({"kind": "SEMANTIC_CONTRACT_CHECK", "ok": False,
                          "error": "at least one --enum or --value is needed"}))
        return 2
    report = check(contract=contract, root=Path(args.root), enums=args.enum,
                   values=args.value)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
