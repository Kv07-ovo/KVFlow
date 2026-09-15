"""Scratch cleanup must survive the Windows delete race, and never lie about it.

The clean 4h soak found this: the product's own restore/row-count scratch directory
was removed with a plain ``shutil.rmtree`` and failed 46 times in four hours with
``WinError 145`` ("the directory is not empty") even though nothing held it open.
Windows finishes deleting a directory asynchronously, so a bounded retry is the
honest fix - and a directory that still cannot be removed must be reported.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from kvflow.core import backup as backup_module


def test_scratch_dir_removes_its_directory(tmp_path):
    with backup_module._scratch_dir() as scratch:  # noqa: SLF001
        (Path(scratch) / "file.txt").write_text("x", encoding="utf-8")
        assert Path(scratch).is_dir()
    assert not Path(scratch).exists()


def test_cleanup_retries_a_transient_windows_failure(monkeypatch):
    calls = {"n": 0}
    real_rmtree = shutil.rmtree

    def flaky(path, *args, **kwargs):
        calls["n"] += 1
        if calls["n"] < 3:
            error = OSError("the directory is not empty")
            error.winerror = 145
            raise error
        return real_rmtree(path, *args, **kwargs)

    with backup_module._scratch_dir() as scratch:  # noqa: SLF001
        (Path(scratch) / "file.txt").write_text("x", encoding="utf-8")
        monkeypatch.setattr(backup_module.shutil, "rmtree", flaky)
    assert calls["n"] >= 3, "the retry did not happen"
    assert not Path(scratch).exists()


def test_cleanup_reports_a_real_failure_instead_of_hiding_it(monkeypatch):
    def always_fails(path, *args, **kwargs):
        error = OSError("still busy")
        error.winerror = 32
        raise error

    monkeypatch.setattr(backup_module.shutil, "rmtree", always_fails)
    context = backup_module._scratch_dir()  # noqa: SLF001
    with context:
        pass
    assert context.cleaned is False, "a failed cleanup was reported as success"
    # the caller can see it; nothing was silently swallowed


def test_row_counts_still_works_through_the_scratch_path(tmp_path):
    from kvflow.core.store import Store

    database = tmp_path / "kvflow.sqlite3"
    store = Store(database)
    store.initialize()
    counts = backup_module.BackupManager(
        database=database, backups_root=tmp_path / "backups", version="0.1.0"
    ).row_counts(database)
    assert counts.get("projects") == 0
    assert counts.get("jobs") == 0
    assert list((tmp_path).glob("kvflow-*")) == [], "a scratch directory was left behind"
