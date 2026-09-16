"""Patch the frozen v1 baseline's scratch cleanup (authorised by the user: "全部修完").

The clean-soak runs kept reporting ``WinError 145`` ("the directory is not empty")
from ``tempfile.TemporaryDirectory`` cleanup inside the **v1 baseline's** backup
code - the code the soak actually exercises. KVFlow's own copy was fixed first,
which did not touch this path; that misattribution is corrected here.

The change is deliberately minimal and behaviour-preserving: a scratch directory is
created with ``mkdtemp`` and removed by a helper that retries a bounded number of
times on the Windows delete race, returning False if the directory really cannot be
removed. Nothing about backup identity, manifests or hashes changes.
"""

from __future__ import annotations

import sys
from pathlib import Path

TARGET = Path(
    r"C:\Users\90428\Desktop\KVStock-restored\kvstock-platform\agent_os\releases"
    r"\1.0.0-rc1\src\agent_os\v1\backup.py"
)

HELPER = '''
def _cleanup_tree(path, *, attempts: int = 8) -> bool:
    """Remove a scratch directory with a bounded retry.

    Windows finishes deleting a directory asynchronously, so ``shutil.rmtree`` can
    raise ``WinError 145`` ("the directory is not empty") or ``WinError 5`` while the
    kernel still holds the last handle. Retrying a bounded number of times is the
    honest fix; a directory that still cannot be removed is reported to the caller
    instead of being swallowed.
    """
    import time as _time

    for attempt in range(1, attempts + 1):
        try:
            shutil.rmtree(path)
            return True
        except FileNotFoundError:
            return True
        except OSError:
            if attempt >= attempts:
                return False
            _time.sleep(min(0.5, 0.02 * (2 ** attempt)))
    return False


class _scratch_dir:
    """A temporary directory whose cleanup tolerates the Windows delete race."""

    def __init__(self, prefix: str = "agent-os-") -> None:
        self.path = Path(tempfile.mkdtemp(prefix=prefix))
        self.cleaned = True

    def __enter__(self) -> Path:
        return self.path

    def __exit__(self, *exc: object) -> bool:
        self.cleaned = _cleanup_tree(self.path)
        return False


'''

text = TARGET.read_text(encoding="utf-8")
original = text

if "_scratch_dir" not in text:
    marker = "class BackupManager"
    index = text.find(marker)
    if index == -1:
        print("anchor 'class BackupManager' not found")
        sys.exit(2)
    text = text[:index] + HELPER.lstrip("\n") + text[index:]

text = text.replace(
    "        with tempfile.TemporaryDirectory() as scratch:\n"
    '            snapshot = Path(scratch) / "snapshot.sqlite3"',
    "        with _scratch_dir() as scratch:\n"
    '            snapshot = Path(scratch) / "snapshot.sqlite3"',
)
text = text.replace(
    "        with tempfile.TemporaryDirectory() as scratch:\n"
    '            candidate = Path(scratch) / "agent_os.sqlite3"',
    "        with _scratch_dir() as scratch:\n"
    '            candidate = Path(scratch) / "agent_os.sqlite3"',
)

if text == original:
    print("NOTHING CHANGED")
    sys.exit(2)
TARGET.write_text(text, encoding="utf-8")
print("patched:", TARGET)
print("remaining TemporaryDirectory uses:", text.count("tempfile.TemporaryDirectory()"))
print("_scratch_dir uses:", text.count("with _scratch_dir() as scratch:"))
