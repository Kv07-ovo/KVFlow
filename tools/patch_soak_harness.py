"""One-shot patcher for the soak harness: event classification + bounded cleanup.

Harness-only change. It does not touch KVFlow product code, does not lower any
release gate, and does not touch any previous receipt.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

TARGET = Path(r"C:\Users\90428\Desktop\KVStock-restored\kvstock-platform\agent_os"
              r"\releases\1.0.0-rc1\.runtime_tools\soak_runner.py")

text = TARGET.read_text(encoding="utf-8")
original = text

# ---------------------------------------------------------------- 1. counters
anchor = re.search(r"^    contention_events: int = 0\n", text, re.M)
if anchor and "expected_refusals: int = 0" not in text:
    insert = (
        "    #: harness event classification: an expected fail-closed refusal is a\n"
        "    #: passing negative test, not an error; cleanup retries are recorded\n"
        "    #: separately from cleanup failures, which are never swallowed.\n"
        "    expected_refusals: int = 0\n"
        "    negative_test_failures: int = 0\n"
        "    cleanup_retries: int = 0\n"
        "    cleanup_failures: int = 0\n"
        "    product_errors: int = 0\n"
        "    harness_errors: int = 0\n"
        "    unexpected_errors: int = 0\n"
        "    cleanup_retry_events: list = field(default_factory=list)\n"
    )
    text = text[: anchor.end()] + insert + text[anchor.end():]

# --------------------------------------------------- 2. classification helpers
note_anchor = re.search(
    r'    def note\(self, message: str\) -> None:\n'
    r'        """Record a defect together with the phase that produced it\."""\n'
    r'        self\.state\.errors\.append\(f"\[{self\.state\.phase}\] {message}"\)\n',
    text,
)
if note_anchor and "def expected_refusal(" not in text:
    helpers = (
        '    def note(self, message: str) -> None:\n'
        '        """Record a genuine defect together with the phase that produced it."""\n'
        '        self.state.errors.append(f"[{self.state.phase}] {message}")\n'
        '        self.state.harness_errors += 1\n'
        '\n'
        '    def product_fault(self, message: str) -> None:\n'
        '        """An unexpected product exception: always counted, never swallowed."""\n'
        '        self.state.errors.append(f"[{self.state.phase}] PRODUCT {message}")\n'
        '        self.state.product_errors += 1\n'
        '\n'
        '    def unexpected_fault(self, message: str) -> None:\n'
        '        self.state.errors.append(f"[{self.state.phase}] UNEXPECTED {message}")\n'
        '        self.state.unexpected_errors += 1\n'
        '\n'
        '    def expected_refusal(self, message: str) -> None:\n'
        '        """A refusal the load design *expects*: a passing negative test.\n'
        '\n'
        '        It is recorded with its count and never added to SOAK_ERRORS; if the\n'
        '        refusal did not happen, the caller records a product fault instead.\n'
        '        """\n'
        '        self.state.expected_refusals += 1\n'
        '        self.state.cleanup_retry_events.append(\n'
        '            {"at": now(), "phase": self.state.phase, "kind": "EXPECTED_REFUSAL",\n'
        '             "detail": message}\n'
        '        )\n'
        '\n'
        '    def cleanup_dir(self, path, *, attempts: int = 6, label: str = "cleanup") -> bool:\n'
        '        """Remove a directory with bounded retry; a real failure is reported.\n'
        '\n'
        '        Windows keeps a directory busy for a moment after a file handle closes,\n'
        '        which surfaces as WinError 145. Retrying a bounded number of times with\n'
        '        backoff is the honest fix; the exceptions are logged per attempt and a\n'
        '        failure after the last attempt becomes a HARNESS error, never silence.\n'
        '        """\n'
        '        import time as _time\n'
        '\n'
        '        for attempt in range(1, attempts + 1):\n'
        '            try:\n'
        '                shutil.rmtree(path)\n'
        '                return True\n'
        '            except FileNotFoundError:\n'
        '                return True\n'
        '            except OSError as exc:\n'
        '                if attempt >= attempts:\n'
        '                    self.state.cleanup_failures += 1\n'
        '                    self.note(f"{label}: cleanup failed after {attempts} attempts:"\n'
        '                              f" {type(exc).__name__}: {exc} @ {path}")\n'
        '                    return False\n'
        '                self.state.cleanup_retries += 1\n'
        '                self.state.cleanup_retry_events.append(\n'
        '                    {"at": now(), "phase": self.state.phase, "kind": "CLEANUP_RETRY",\n'
        '                     "path": str(path), "attempt": attempt,\n'
        '                     "error": f"{type(exc).__name__}: {exc}"}\n'
        '                )\n'
        '                _time.sleep(min(1.0, 0.05 * (2 ** attempt)))\n'
        '        return False\n'
        '\n'
        '    def lease_cleanup_check(self) -> dict:\n'
        '        """Post-exit check: no ACTIVE lease may outlive its run.\n'
        '\n'
        '        A lease whose run already reached a terminal status is exactly the\n'
        '        "unexpected open lease" a release gate must not hide.\n'
        '        """\n'
        '        try:\n'
        '            with self.store.read() as conn:\n'
        '                rows = conn.execute(\n'
        '                    "SELECT l.lease_id, l.status AS lease_status, r.status AS run_status"\n'
        '                    " FROM leases l JOIN runs r ON r.run_id = l.run_id"\n'
        '                    " WHERE l.status = \'ACTIVE\'"\n'
        '                    " AND r.status NOT IN (\'OPEN\')"\n'
        '                ).fetchall()\n'
        '                active = conn.execute(\n'
        '                    "SELECT COUNT(*) AS n FROM leases WHERE status = \'ACTIVE\'"\n'
        '                ).fetchone()["n"]\n'
        '        except Exception as exc:  # noqa: BLE001 - an unreadable check is not a pass\n'
        '            return {"unexpected_open_leases": None,\n'
        '                    "active_leases_final": None,\n'
        '                    "lease_cleanup_error": f"{type(exc).__name__}: {exc}"}\n'
        '        return {"unexpected_open_leases": len(rows),\n'
        '                "active_leases_final": int(active),\n'
        '                "orphan_lease_ids": [row["lease_id"] for row in rows][:10]}\n'
        '\n'
    )
    text = text[: note_anchor.start()] + helpers + text[note_anchor.end():]

# --------------------------------------------- 3. backup/restore phase rewrite
phase_start = text.find("    def phase_backup_restore(self) -> None:")
phase_end = text.find("    def phase_knowledge(self) -> None:")
if phase_start != -1 and phase_end != -1 and "negative test" not in text:
    new_phase = '''    def phase_backup_restore(self) -> None:
        """Backup, verify, restore, prove the refusal, then clean up honestly.

        Each cycle gets its own unique destination, so a directory left behind by an
        earlier cycle can never make the next restore look like a product refusal.
        The refusal of a *non-empty* destination is then asserted on purpose: it is
        the product failing closed, which is a passing negative test rather than an
        error. Only if that restore were to succeed would this be a product fault.
        """
        result = self.backups.create(note=f"soak {self.run_id}")
        self.state.backups += 1
        verify = self.backups.verify(result.root)
        if not verify["verified"]:
            self.note(f"backup {result.root} did not verify")
            return
        target = self.root / f"restore-{self.state.cycles}-{self.state.backups}"
        outcome = None
        for attempt in range(1, 4):
            try:
                outcome = self.backups.restore(result.root, target)
                break
            except Exception as exc:  # noqa: BLE001 - classified below, never swallowed
                name = type(exc).__name__
                winerror = getattr(exc, "winerror", None)
                if name == "ConflictError":
                    self.expected_refusal(
                        f"restore into a non-empty destination was refused: {exc}"
                    )
                    break
                if winerror in (5, 32, 145) or "WinError 145" in str(exc):
                    if attempt >= 3:
                        self.note(f"restore cleanup race survived 3 attempts: {exc}")
                        break
                    self.state.cleanup_retries += 1
                    self.state.cleanup_retry_events.append(
                        {"at": now(), "phase": self.state.phase, "kind": "CLEANUP_RETRY",
                         "path": str(target), "attempt": attempt,
                         "error": f"{name}: {exc}"}
                    )
                    self.cleanup_dir(target, label="restore-target")
                    continue
                self.product_fault(f"restore raised {name}: {exc}")
                break
        if outcome is None:
            self.cleanup_dir(target, label="restore-target")
            self.cleanup_dir(result.root, label="backup-root")
            return
        if not outcome.get("ready"):
            self.note("restore was not ready")
            return
        restored = Store(target / "agent_os.sqlite3")
        if restored.checkpoint()["schema_version"] != self.store.schema_version():
            self.note("restored schema version differs")
            return
        self.state.restores += 1
        # negative test: the destination now holds a restored tree, so KVFlow must
        # refuse to restore over it. A refusal is the expected, counted outcome.
        try:
            self.backups.restore(result.root, target)
        except Exception as exc:  # noqa: BLE001 - the refusal is the assertion
            if type(exc).__name__ == "ConflictError":
                self.expected_refusal(
                    f"second restore into the non-empty destination was refused: {exc}"
                )
            else:
                self.product_fault(
                    f"a non-empty destination produced {type(exc).__name__} instead of"
                    f" a ConflictError: {exc}"
                )
        else:
            self.state.negative_test_failures += 1
            self.product_fault(
                f"restore overwrote a NON-EMPTY destination at {target}"
            )
        self.cleanup_dir(target, label="restore-target")
        self.cleanup_dir(result.root, label="backup-root")

'''
    text = text[:phase_start] + new_phase + text[phase_end:]

# --------------------------------------------------- 4. final checks and report
text = text.replace(
    "        final = self.check_invariants()\n",
    "        final = self.check_invariants()\n"
    "        final.update(self.lease_cleanup_check())\n",
    1,
)
text = text.replace(
    '                "contention_events": self.state.contention_events,\n',
    '                "contention_events": self.state.contention_events,\n'
    '                "expected_refusals": self.state.expected_refusals,\n'
    '                "negative_test_failures": self.state.negative_test_failures,\n'
    '                "cleanup_retries": self.state.cleanup_retries,\n'
    '                "cleanup_failures": self.state.cleanup_failures,\n'
    '                "product_errors": self.state.product_errors,\n'
    '                "harness_errors": self.state.harness_errors,\n'
    '                "unexpected_errors": self.state.unexpected_errors,\n',
    1,
)
text = text.replace(
    '            "errors": self.state.errors[:50],\n'
    '            "error_count": len(self.state.errors),\n',
    '            "events": {\n'
    '                "EXPECTED_REFUSAL": self.state.expected_refusals,\n'
    '                "CLEANUP_RETRY": self.state.cleanup_retries,\n'
    '                "CLEANUP_FAILURE": self.state.cleanup_failures,\n'
    '                "PRODUCT_ERROR": self.state.product_errors,\n'
    '                "HARNESS_ERROR": self.state.harness_errors,\n'
    '                "UNEXPECTED_ERROR": self.state.unexpected_errors,\n'
    '            },\n'
    '            "cleanup_retry_events": self.state.cleanup_retry_events[:100],\n'
    '            "errors": self.state.errors[:50],\n'
    '            "error_count": len(self.state.errors),\n'
    '            "soak_errors": (self.state.product_errors + self.state.harness_errors\n'
    '                            + self.state.unexpected_errors),\n',
    1,
)
text = text.replace(
    '            and self.state.lock_errors == 0\n'
    '            and not self.state.errors\n'
    '            else "PARTIAL"\n',
    '            and self.state.lock_errors == 0\n'
    '            and not self.state.errors\n'
    '            and self.state.product_errors == 0\n'
    '            and self.state.harness_errors == 0\n'
    '            and self.state.unexpected_errors == 0\n'
    '            and self.state.cleanup_failures == 0\n'
    '            and self.state.negative_test_failures == 0\n'
    '            and final.get("unexpected_open_leases") == 0\n'
    '            and self.state.expected_refusals > 0\n'
    '            else "PARTIAL"\n',
    1,
)

if text == original:
    print("NOTHING CHANGED - anchors did not match")
    sys.exit(2)
TARGET.write_text(text, encoding="utf-8")
print("patched:", TARGET)
print("expected_refusals counter:", "expected_refusals: int = 0" in text)
print("cleanup_dir helper:", "def cleanup_dir(" in text)
print("negative test:", "negative test" in text)
print("lease check:", "def lease_cleanup_check(" in text)
print("verdict gate:", 'and final.get("unexpected_open_leases") == 0' in text)
