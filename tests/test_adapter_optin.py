"""Optional adapters are deny-by-default, read-only and provably non-destructive.

KVStock is the shipped example, but these tests use a synthetic product so they
hold for any adapter: an adapter is off until a user enables it in the runtime
home, enabling it grants reads and nothing else, and reading through it cannot
change the adapted tree.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from kvflow.adapters import kvstock_adapter as adapter
from kvflow.adapters import registry as optin
from kvflow.core.errors import ContractError, NotFoundError


@pytest.fixture()
def adapted(tmp_path: Path) -> Path:
    """A small read-only product tree with one JSON artifact and one text file."""
    root = tmp_path / "product"
    (root / "artifacts").mkdir(parents=True)
    (root / "artifacts" / "status.json").write_text(
        json.dumps({"status": "PREPARED", "observed_at": "2026-09-15T00:00:00+00:00"}),
        encoding="utf-8",
    )
    (root / "notes.txt").write_text("line one\nline two\n", encoding="utf-8")
    return root


def _enable(tmp_path: Path, adapted: Path) -> dict:
    return optin.enable(
        tmp_path,
        "kvstock",
        options={
            "root": str(adapted),
            "sources": [
                {"key": "status", "kind": "json", "path": "artifacts/status.json"},
                {"key": "notes", "kind": "text", "path": "notes.txt"},
            ],
        },
        reason="fixture",
    )


def test_nothing_is_enabled_by_default(tmp_path: Path):
    rows = optin.describe(tmp_path)
    assert [row["adapter"] for row in rows] == ["kvstock"]
    assert all(row["enabled"] is False for row in rows)
    with pytest.raises(NotFoundError) as excinfo:
        optin.require_enabled(tmp_path, "kvstock")
    error = excinfo.value.to_dict()
    # the refusal names the exact command that would turn it on
    assert error["enable_command"] == "kvflow adapter enable kvstock"
    assert "never auto-detected" in error["note"]


def test_an_unknown_adapter_cannot_be_enabled(tmp_path: Path):
    with pytest.raises(ContractError):
        optin.enable(tmp_path, "not-a-product", options={})


def test_enabling_is_recorded_and_readable(tmp_path: Path, adapted: Path):
    _enable(tmp_path, adapted)
    document = json.loads((tmp_path / "adapters.json").read_text(encoding="utf-8"))
    assert document["enabled"]["kvstock"]["read_only"] is True
    assert optin.describe(tmp_path)[0]["enabled"] is True
    entry = optin.require_enabled(tmp_path, "kvstock")
    assert entry["reason"] == "fixture"
    removed = optin.disable(tmp_path, "kvstock")
    assert removed["was_enabled"] is True
    with pytest.raises(NotFoundError):
        optin.require_enabled(tmp_path, "kvstock")


def test_a_malformed_optin_document_is_a_refusal_not_a_reset(tmp_path: Path):
    (tmp_path / "adapters.json").write_text("{not json", encoding="utf-8")
    with pytest.raises(ContractError):
        optin.load_state(tmp_path)


def test_the_reader_needs_an_absolute_root_and_a_registered_source(tmp_path: Path,
                                                                   adapted: Path):
    entry = _enable(tmp_path, adapted)
    assert adapter.build_reader(entry).root == adapted.resolve()

    relative = dict(entry, options={"root": "product", "sources": [{"key": "x", "kind": "text",
                                                                   "path": "notes.txt"}]})
    with pytest.raises(ContractError):
        adapter.build_reader(relative)

    empty = dict(entry, options={"root": str(adapted), "sources": []})
    with pytest.raises(ContractError):
        adapter.build_reader(empty)

    not_read_only = dict(entry, read_only=False)
    with pytest.raises(ContractError):
        adapter.build_reader(not_read_only)


def test_reading_through_the_adapter_never_changes_the_tree(tmp_path: Path,
                                                            adapted: Path):
    entry = _enable(tmp_path, adapted)
    before = adapter.fingerprint(adapted)
    read = adapter.read(entry, "status")
    assert read["source_key"] == "status"
    assert read["value"]["status"] == "PREPARED"
    chunk = adapter.read(entry, "notes", offset=0, limit=8)
    assert chunk["value"] == "line one"
    described = adapter.describe(entry)
    assert sorted(described["sources"]) == ["notes", "status"]
    after = adapter.verify_unchanged(adapted, before)
    assert after["unchanged"] is True, after
    assert after["files_before"] == after["files_after"] == 2


def test_a_changed_tree_is_reported_with_the_exact_file(tmp_path: Path, adapted: Path):
    before = adapter.fingerprint(adapted)
    (adapted / "notes.txt").write_text("line one\nline two\nline three\n", encoding="utf-8")
    (adapted / "extra.txt").write_text("new\n", encoding="utf-8")
    after = adapter.verify_unchanged(adapted, before)
    assert after["unchanged"] is False
    assert after["changed"] == ["notes.txt"]
    assert after["added"] == ["extra.txt"]
    assert after["missing"] == []


def test_an_unregistered_source_is_refused(tmp_path: Path, adapted: Path):
    entry = _enable(tmp_path, adapted)
    from kvflow.adapters.kvstock import UnknownSourceError

    with pytest.raises(UnknownSourceError):
        adapter.read(entry, "not-registered")


def test_the_adapter_is_not_reachable_without_the_optin(tmp_path: Path,
                                                        adapted: Path):
    # the entry is only produced by require_enabled, which refuses while disabled
    with pytest.raises(NotFoundError):
        adapter.enabled_entry(tmp_path)
    # and the adapter modules are not imported by the core package
    import kvflow.core.tools as core_tools

    source = Path(core_tools.__file__).read_text(encoding="utf-8")
    assert "kvstock" not in source.casefold()
