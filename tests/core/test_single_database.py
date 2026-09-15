"""One product, one database: every entrance resolves the same authoritative file.

The genericisation renamed the runtime database, and the rename reached some
entrances but not others, so ``doctor``/``backup`` inspected an empty file while the
workflow wrote a different one. Two task states that disagree is exactly the failure
this release is meant to remove, so it is pinned here.
"""

from __future__ import annotations

from pathlib import Path

from kvflow import api, workflow
from kvflow.core import cli as core_cli
from kvflow.core.store import Store


def _home(tmp_path: Path) -> Path:
    home = tmp_path / "runtime"
    home.mkdir(parents=True, exist_ok=True)
    return home


def test_every_entrance_resolves_the_same_database(tmp_path):
    home = _home(tmp_path)
    paths = core_cli.runtime_paths(home)
    assert paths["database"] == home / core_cli.DATABASE_NAME
    # the workflow's store and the API's store are the same file
    store = workflow._store_for(home)  # noqa: SLF001 - the runner's own resolver
    assert store.db_path == paths["database"]
    api_store = api._store(home)  # noqa: SLF001
    assert api_store.db_path == paths["database"]
    assert Store(core_cli.database_path(home)).db_path == paths["database"]


def test_a_legacy_home_is_migrated_instead_of_read_as_empty(tmp_path):
    home = _home(tmp_path)
    legacy = home / core_cli.LEGACY_DATABASE_NAME
    store = Store(legacy)
    store.initialize()
    project = {
        "project_id": "legacy-project",
        "display_name": "legacy",
        "canonical_root": str(home),
        "authorization_digest": "a" * 64,
    }
    from kvflow.core.contracts import Project

    store.register_project(Project.model_validate({
        "id": "legacy-project", "display_name": "legacy", "source_root": str(home),
        "managed_root": str(home / "managed"), "allowed_read_roots": ["."],
        "allowed_write_roots": [], "protected_roots": [], "test_profiles": [],
        "allowed_tools": ["repo.read"], "trusted": True,
        "authorization_digest": "a" * 64,
        "registered_at": "2026-01-01T00:00:00+00:00",
    }))
    resolved = core_cli.database_path(home)
    assert resolved == home / core_cli.DATABASE_NAME
    assert not legacy.exists(), "the legacy file is moved, never copied and forgotten"
    migrated = Store(resolved)
    assert migrated.project("legacy-project").display_name == "legacy"
    # and the migration is idempotent
    assert core_cli.database_path(home) == resolved
