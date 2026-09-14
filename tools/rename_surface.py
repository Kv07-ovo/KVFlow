"""Rename the remaining baseline surface names to KVFlow names.

Two groups, both user-visible:

* product-internal file/directory names (test logs, pytest config, backup prefix,
  git author, MCP server name, version banner);
* environment variables and HTTP header names, which an operator, the DSH plugin
  or a script must be able to read from the documentation.

The copied test suite is renamed with the same table so its assertions stay
intact: if it still passes, the extraction is faithful. Test files additionally
point at the product CLI entry (``kvflow.cli``) rather than the core module, so
the shipped entry point is what the tests drive.

    python tools/rename_surface.py --product <product dir>
"""

from __future__ import annotations

import argparse
from pathlib import Path

RENAMES = (
    ('".agent_os_"', '".kvflow_"'),
    (".agent_os_logs", ".kvflow_logs"),
    (".agent_os_pytest.ini", ".kvflow_pytest.ini"),
    (".agent_os_pytest_cache", ".kvflow_pytest_cache"),
    (".agent_os_runtime", ".kvflow_runtime"),
    ("(agent-os)", "(kvflow)"),
    ("worker@agent-os.local", "worker@kvflow.local"),
    ('"agent-os-v1"', '"kvflow"'),
    ('f"agent-os-{self.version}-{identifier}"', 'f"kvflow-{self.version}-{identifier}"'),
    ('f"agent-os-v1 {PRODUCT_VERSION}"', 'f"kvflow {PRODUCT_VERSION}"'),
    ("AGENT_OS_MCP_CAPABILITY", "KVFLOW_MCP_CAPABILITY"),
    ("AGENT_OS_MCP_DATABASE", "KVFLOW_MCP_DATABASE"),
    ("AGENT_OS_MCP_WORKSPACE_ROOT", "KVFLOW_MCP_WORKSPACE_ROOT"),
    ("AGENT_OS_DSH_CREDENTIALS", "KVFLOW_CREDENTIALS"),
    ("AGENT_OS_WORKER", "KVFLOW_WORKER"),
    ("AGENT_OS_MANAGER", "KVFLOW_MANAGER"),
    ("AGENT_OS_PROFILE", "KVFLOW_PROFILE"),
    ("AGENT_OS_SERVER_NAME", "KVFLOW_SERVER_NAME"),
    ("AGENT_OS_", "KVFLOW_"),
    ("X-Agent-OS-Token", "X-KVFlow-Token"),
    ("X-Agent-OS-CSRF", "X-KVFlow-CSRF"),
)

TEST_RENAMES = (("kvflow.core.cli", "kvflow.cli"),)


def rewrite(path: Path, table) -> bool:
    text = path.read_text(encoding="utf-8")
    original = text
    for old, new in table:
        text = text.replace(old, new)
    if text != original:
        path.write_text(text, encoding="utf-8")
        return True
    return False


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--product", required=True)
    args = parser.parse_args()
    product = Path(args.product).resolve()

    updated = 0
    for path in sorted((product / "src").rglob("*.py")):
        if rewrite(path, RENAMES):
            updated += 1
            print("src  ", path.relative_to(product).as_posix())
    for path in sorted((product / "tests").rglob("*.py")):
        if rewrite(path, RENAMES + TEST_RENAMES):
            updated += 1
            print("tests", path.relative_to(product).as_posix())
    print(f"{updated} file(s) updated")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
