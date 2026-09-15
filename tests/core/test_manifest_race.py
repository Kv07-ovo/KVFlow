"""Two writers must be able to record the same run without one of them failing.

The daily DSH acceptance found this: the host plugin writes the run manifest when it
launches a run, and the runner writes the same manifest when the run ends. Both used
one fixed temp file name, so the second ``os.replace`` lost the race and surfaced as
``[WinError 5] access denied`` - a real failure in a completely normal sequence.
"""

from __future__ import annotations

import json
import threading
from pathlib import Path

from kvflow import workflow


def _manifest(home: Path, job_id: str) -> dict:
    return json.loads((workflow.runs_dir(home) / f"{job_id}.json").read_text(
        encoding="utf-8"))


def test_two_writers_of_one_manifest_both_succeed(tmp_path):
    job_id = "job_race_probe"
    results: list[str] = []
    start = threading.Barrier(4)

    def writer(name: str) -> None:
        start.wait(timeout=30)
        payload = {"job_id": job_id, "writer": name, "status": "RUNNING"}
        try:
            workflow._write_manifest(tmp_path, job_id, payload)  # noqa: SLF001
            results.append(name)
        except OSError as exc:  # pragma: no cover - the bug this test pins
            results.append(f"{name}:{type(exc).__name__}")

    threads = [threading.Thread(target=writer, args=(f"w{index}",)) for index in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=60)
    assert len(results) == 4, results
    assert not [item for item in results if ":" in item], results
    # the file is a complete JSON document, never a torn write
    document = _manifest(tmp_path, job_id)
    assert document["job_id"] == job_id
    assert document["writer"] in {"w0", "w1", "w2", "w3"}
    # no temp file is left behind
    assert list(workflow.runs_dir(tmp_path).glob("*.tmp")) == []


def test_a_failed_replace_is_raised_not_swallowed(tmp_path, monkeypatch):
    """A real failure after the bounded retries must surface, never pass silently."""
    from pathlib import Path as _Path

    calls = {"n": 0}

    def refuse(self, target):  # noqa: ANN001 - patched Path.replace
        calls["n"] += 1
        error = OSError("sharing violation")
        error.winerror = 32
        raise error

    monkeypatch.setattr(_Path, "replace", refuse, raising=True)
    try:
        workflow._write_manifest(tmp_path, "job_fail", {"job_id": "job_fail"})  # noqa: SLF001
    except OSError as exc:
        assert "sharing violation" in str(exc)
    else:  # pragma: no cover - the point of the test
        raise AssertionError("a failed manifest write was swallowed")
    assert calls["n"] >= 3, "the bounded retry did not happen"


def test_update_manifest_merges_and_keeps_one_file(tmp_path):
    job_id = "job_merge_probe"
    workflow._write_manifest(tmp_path, job_id, {"job_id": job_id, "status": "RUNNING"})  # noqa: SLF001
    merged = workflow.update_manifest(tmp_path, job_id, status="PASS", extra=1)
    assert merged["status"] == "PASS"
    assert merged["extra"] == 1
    assert _manifest(tmp_path, job_id) == merged
    assert len(list(workflow.runs_dir(tmp_path).glob(f"{job_id}.json*"))) == 1
