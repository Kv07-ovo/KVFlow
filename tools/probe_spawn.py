"""Check the detached-spawn mechanism the MCP server uses, in isolation."""

from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

PRODUCT = Path(r"C:\Users\90428\Desktop\KVStock-restored\kvflow")
LOG = PRODUCT / ".runtime" / "spawn-probe.log"
LOG.parent.mkdir(parents=True, exist_ok=True)
if LOG.exists():
    LOG.unlink()

env = {key: value for key, value in os.environ.items()}
env["PYTHONPATH"] = str(PRODUCT / "src")
env["PYTHONIOENCODING"] = "utf-8"

argv = [sys.executable, "-m", "kvflow.runner", "--help"]
flags = 0x00000008 | 0x00000200  # DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP
with open(LOG, "ab") as handle:
    process = subprocess.Popen(
        argv, stdin=subprocess.DEVNULL, stdout=handle, stderr=handle, env=env,
        cwd=str(PRODUCT), creationflags=flags, close_fds=True,
    )
print("pid", process.pid)
time.sleep(6)
print("exit code", process.poll())
if LOG.exists():
    text = LOG.read_text(encoding="utf-8", errors="replace")
    print("log bytes", len(text))
    print(text[:600])
else:  # pragma: no cover
    print("no log file")
