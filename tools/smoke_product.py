"""Smoke-test the KVFlow product surface in a clean, KVStock-free directory.

Creates a tiny Python project outside the KVStock tree, onboards it, and checks
that the core never imported anything from KVStock.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

PRODUCT = Path(r"C:\Users\90428\Desktop\KVStock-restored\kvflow")
PY = Path(
    r"C:\Users\90428\Desktop\KVStock-restored\kvstock-platform"
    r"\agent_os\releases\1.0.0-rc1\.venv\Scripts\python.exe"
)
WORK = PRODUCT / ".runtime" / "smoke"
DEMO = WORK / "demo-python-api"
HOME = WORK / "home"


def run(*argv: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [str(PY), "-m", "kvflow.cli", "--home", str(HOME), *argv],
        capture_output=True, text=True, encoding="utf-8", errors="replace",
        env={**os.environ, "PYTHONPATH": str(PRODUCT / "src"), "TEMP": str(WORK / "tmp")},
        cwd=str(WORK),
    )


def payload(proc: subprocess.CompletedProcess[str]) -> dict:
    if proc.stdout.strip():
        try:
            return json.loads(proc.stdout)
        except ValueError:
            return {"unparsed": proc.stdout[:400], "stderr": proc.stderr[-400:]}
    return {"exit": proc.returncode, "stderr": proc.stderr[-600:]}


def main() -> int:
    import shutil

    shutil.rmtree(WORK, ignore_errors=True)
    (DEMO / "src").mkdir(parents=True)
    (DEMO / "tests").mkdir(parents=True)
    (DEMO / "tmp").mkdir(parents=True)
    (DEMO / "pyproject.toml").write_text("[project]\nname='demo'\nversion='0.1.0'\n",
                                         encoding="utf-8")
    (DEMO / "src" / "calc.py").write_text("def add(a, b):\n    return a + b\n",
                                          encoding="utf-8")
    (DEMO / "tests" / "test_calc.py").write_text(
        "from calc import add\n\n\ndef test_add():\n    assert add(1, 2) == 3\n",
        encoding="utf-8",
    )

    print("version  ", run("--version").stdout.strip())
    print("doctor   ", json.dumps(payload(run("doctor")))[:160])
    print("templates", [t["id"] for t in payload(run("template", "list"))["templates"]])
    print("profiles ", [p["id"] for p in payload(run("profile", "list"))["model_profiles"]])

    draft = payload(run("project", "onboard", "--path", str(DEMO)))
    print("draft    ", draft["action"], json.dumps(draft["approval"]["profiles"], ensure_ascii=False))

    approved = payload(run("project", "onboard", "--path", str(DEMO), "--write"))
    print("approved ", approved["action"],
          approved.get("authorization_digest", "")[:16],
          (approved.get("registry_entry") or {}).get("profiles"))

    shown = payload(run("project", "show", approved["registry_entry"]["project_id"]))
    print("show     ", shown["entry"]["canonical_root"], shown["directory_exists"])

    doctor = payload(run("project", "doctor"))
    print("registry ", doctor["healthy"], [p["problems"] for p in doctor["projects"]])

    config = json.loads((DEMO / ".kvflow" / "project.json").read_text(encoding="utf-8"))
    print("config   ", sorted(config), "approved_by_user=", config["approved_by_user"])

    probe = subprocess.run(
        [str(PY), "-c",
         "import kvflow, sys; import kvflow.cli;"
         " print('kvstock-imported=', any('kvstock' in m for m in sys.modules))"],
        capture_output=True, text=True,
        env={**os.environ, "PYTHONPATH": str(PRODUCT / "src")},
    )
    print("isolation", probe.stdout.strip() or probe.stderr[-200:])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
