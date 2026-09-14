"""Genericize the extracted core: remove product branding, keep behaviour.

Only user-visible product strings are touched. No logic, contract literal or
configuration default changes, so the copied core test suite stays the proof that
the extraction is faithful.

    python tools/genericize_core.py --product <product dir>
"""

from __future__ import annotations

import argparse
from pathlib import Path

CLI_REPLACEMENTS = (
    ("kvflow.core.cli <command>`` and ``agent-v1 <command>``",
     "kvflow.core.cli <command>`` and ``kvflow <command>``"),
    ('"argv": ["agent-v1", "ui", "serve"]', '"argv": ["kvflow", "ui", "serve"]'),
    ('f"agent-v1 --home {paths[\'home\']} stop --id {entry[\'id\']}"',
     'f"kvflow --home {paths[\'home\']} stop --id {entry[\'id\']}"'),
)

WEB_REPLACEMENTS = (
    ('"""The local Agent OS web UI: a real backend, no fake pages.',
     '"""The local KVFlow web UI: a real backend, no fake pages.'),
    ("<title>KVStock Agent OS</title>", "<title>KVFlow</title>"),
    ("<strong>KVStock Agent OS</strong>", "<strong>KVFlow</strong>"),
    ('placeholder="\u4f8b\u5982\uff1a\u8bfb\u53d6\u73b0\u6709 B0/C3 \u7814\u7a76\u7ed3\u679c\uff0c'
     '\u6574\u7406\u5dee\u5f02\u4e0e\u672a\u89e3\u51b3\u95ee\u9898\uff0c\u4e0d\u4fee\u6539\u7814\u7a76\u3002"',
     'placeholder="\u4f8b\u5982\uff1a\u4e3a\u767b\u5f55\u63a5\u53e3\u589e\u52a0\u901f\u7387\u9650\u5236\uff0c'
     '\u8dd1\u901a\u6d4b\u8bd5\u5e76\u7ed9\u51fa\u8865\u4e01\u3002"'),
)


def apply(path: Path, replacements) -> list[str]:
    text = path.read_text(encoding="utf-8")
    changed = []
    for old, new in replacements:
        if old in text:
            text = text.replace(old, new)
            changed.append(old[:48])
    path.write_text(text, encoding="utf-8")
    return changed


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--product", required=True)
    args = parser.parse_args()
    core = Path(args.product).resolve() / "src" / "kvflow" / "core"
    for name, replacements in (("cli.py", CLI_REPLACEMENTS), ("web.py", WEB_REPLACEMENTS)):
        changed = apply(core / name, replacements)
        print(f"{name}: {len(changed)} replacement(s)")
        for item in changed:
            print("   ", item)

    banned = ("KVStock", "B0/C3", "Tushare", "agent-v1", "agent_os")
    offenders = []
    for path in sorted(core.rglob("*.py")):
        text = path.read_text(encoding="utf-8")
        for word in banned:
            if word in text:
                offenders.append(f"{path.name}:{word}")
    print("remaining product-specific strings:", offenders or "none")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
