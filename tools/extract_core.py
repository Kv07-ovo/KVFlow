"""Extract the generic KVFlow core from the frozen Agent OS v1.0 release.

The Agent OS v1.0 modules are already self-contained: every one of them uses only
relative imports inside its own package. That makes the extraction mechanical and
verifiable rather than a rewrite:

* every ``agent_os/v1/*.py`` module becomes ``kvflow/core/*.py`` with its
  relative imports untouched;
* the generic support modules (MCP transport, context packaging, credential
  resolution, hashing) move next to it;
* the KVStock read-only reader becomes an *optional adapter* under
  ``kvflow/adapters/`` and is never imported by the core;
* the v1 test suite is copied with its import paths rewritten, so the extracted
  core is proven by the same tests that proved the release.

Nothing is edited here except import paths; genericisation of product names and
model configuration happens afterwards, in the core itself.

    python tools/extract_core.py --release <release dir> --product <product dir>
"""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path

CORE_MODULES = (
    "contracts.py", "state.py", "errors.py", "store.py", "budget.py", "security.py",
    "workspace.py", "scheduler.py", "knowledge.py", "tools.py", "backup.py",
    "providers.py", "worker.py", "manager.py", "mcp_server.py", "web.py", "cli.py",
    "__init__.py",
)

SUPPORT_MODULES = ("context_pack.py", "mcp_transport.py", "credentials.py", "hashing.py")

REWRITES = (
    ("from agent_os.v1.", "from kvflow.core."),
    ("import agent_os.v1.", "import kvflow.core."),
    ("from agent_os.", "from kvflow."),
    ("import agent_os.", "import kvflow."),
    ('"agent_os.v1.', '"kvflow.core.'),
    ("agent_os.v1.", "kvflow.core."),
    ('"agent_os.', '"kvflow.'),
)

PACKAGE_INIT = '''"""KVFlow core: the generic, project-independent orchestration layer.

Everything in this package was extracted from the frozen Agent OS v1.0 release.
It contains no KVStock, ETF, Forward, Golden, Tushare or strategy-specific code
and imports nothing from a host product: a project enters only through the
registry, a declarative profile and the capability ticket issued for one run.
"""

CORE_VERSION = "1.0.0"
'''

ADAPTERS_INIT = '''"""Optional, explicitly enabled project adapters.

An adapter may know about one product's domain (KVStock is the first one). It is
never imported by :mod:`kvflow.core`: the core must start and complete a task in
an environment where that product, its database, its environment variables and
its data do not exist.
"""

__all__ = ["kvstock"]
'''


def rewrite(text: str) -> str:
    for old, new in REWRITES:
        text = text.replace(old, new)
    return text


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--release", required=True)
    parser.add_argument("--product", required=True)
    args = parser.parse_args()

    release = Path(args.release).resolve()
    product = Path(args.product).resolve()
    source_root = release / "src" / "agent_os"
    target = product / "src" / "kvflow"

    if not (source_root / "v1" / "store.py").exists():
        raise SystemExit(f"not the expected release layout: {source_root}")

    for directory in (target / "core", target / "adapters", product / "tests" / "core"):
        directory.mkdir(parents=True, exist_ok=True)

    copied: list[str] = []

    for name in CORE_MODULES:
        source = source_root / "v1" / name
        text = rewrite(source.read_text(encoding="utf-8"))
        (target / "core" / name).write_text(text, encoding="utf-8")
        copied.append(f"core/{name}")

    for name in SUPPORT_MODULES:
        source = source_root / name
        (target / "core" / name).write_text(
            rewrite(source.read_text(encoding="utf-8")), encoding="utf-8"
        )
        copied.append(f"core/{name}")

    (target / "core" / "__init__.py").write_text(PACKAGE_INIT, encoding="utf-8")
    (target / "__init__.py").write_text(
        '"""KVFlow - a universal agent development workflow."""\n\n'
        'from .core import CORE_VERSION  # noqa: F401\n\n'
        '__version__ = "0.1.0"\n',
        encoding="utf-8",
    )
    (target / "adapters" / "__init__.py").write_text(ADAPTERS_INIT, encoding="utf-8")

    reader = source_root / "kvstock_reader.py"
    (target / "adapters" / "kvstock.py").write_text(
        rewrite(reader.read_text(encoding="utf-8")), encoding="utf-8"
    )
    copied.append("adapters/kvstock.py")

    for name in ("conftest.py",):
        pass
    tests_source = release / "tests" / "v1"
    for path in sorted(tests_source.glob("*.py")):
        (product / "tests" / "core" / path.name).write_text(
            rewrite(path.read_text(encoding="utf-8")), encoding="utf-8"
        )
        copied.append(f"tests/core/{path.name}")

    for helper in ("test_mcp_transport_boundaries.py",):
        source = release / "tests" / helper
        if source.exists():
            (product / "tests" / helper).write_text(
                rewrite(source.read_text(encoding="utf-8")), encoding="utf-8"
            )
            copied.append(f"tests/{helper}")

    support = release / "tests" / "support"
    if support.exists():
        destination = product / "tests" / "support"
        destination.mkdir(parents=True, exist_ok=True)
        for path in sorted(support.glob("*.py")):
            (destination / path.name).write_text(
                rewrite(path.read_text(encoding="utf-8")), encoding="utf-8"
            )
            copied.append(f"tests/support/{path.name}")

    # the copied core must not mention a project-specific product name
    banned = ("kvstock", "KvStock", "KVStock", "Tushare", "tushare", "B0/C3")
    offenders: list[str] = []
    for path in sorted((target).rglob("*.py")):
        text = path.read_text(encoding="utf-8")
        for word in banned:
            if word in text:
                offenders.append(f"{path.relative_to(product).as_posix()}:{word}")

    print(f"copied {len(copied)} files into {product}")
    for name in copied:
        print("  ", name)
    if offenders:
        print("\ncore still mentions project-specific names (to fix next):")
        for item in offenders:
            print("  ", item)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
