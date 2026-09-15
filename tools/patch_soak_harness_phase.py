"""Second harness patch: replace the backup/restore phase (the first patch's guard
skipped it because the helper docstring already contained the guard phrase)."""

from __future__ import annotations

import sys
from pathlib import Path

TARGET = Path(r"C:\Users\90428\Desktop\KVStock-restored\kvstock-platform\agent_os"
              r"\releases\1.0.0-rc1\.runtime_tools\soak_runner.py")
text = TARGET.read_text(encoding="utf-8")
start = text.find("    def phase_backup_restore(self) -> None:")
end = text.find("    def phase_knowledge(self) -> None:")
if start == -1 or end == -1:
    print("anchors not found"); sys.exit(2)

new_phase = '''    def phase_backup_restore(self) -> None:
        """Backup, verify, restore, prove the refusal, then clean up honestly.

        Each cycle gets its own unique destination, so a directory left behind by an
        earlier cycle can never make the next restore look like a product refusal.
        The refusal of a *non-empty* destination is then asserted on purpose: KVFlow
        failing closed is a passing negative test, counted separately, never added to
        SOAK_ERRORS. Only if that restore were to succeed is it a product fault.
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
                        f"restore into a destination that already held a tree was"
                        f" refused: {exc}"
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
            self.product_fault(f"restore overwrote a NON-EMPTY destination at {target}")
        self.cleanup_dir(target, label="restore-target")
        self.cleanup_dir(result.root, label="backup-root")

'''
text = text[:start] + new_phase + text[end:]
TARGET.write_text(text, encoding="utf-8")
print("patched phase; negative test present:", "NON-EMPTY destination" in text)
print("old ignore_errors rmtree left:", text.count("shutil.rmtree(target, ignore_errors=True)"))
