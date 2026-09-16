"""A re-approved project configuration must reach the durable row.

The daily DSH acceptance found this: after a project gained a test profile, every run
reported "the project configuration differs from the registered authorization; this
run uses the registered one" and executed against the stale scope, because the store
refuses an implicit re-registration (a run may never widen its own scope) and nothing
performed the explicit amend.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from kvflow import registry
from kvflow.core import cli as core_cli
from kvflow.core.errors import AuthorizationError
from kvflow.core.store import Store


def _project(tmp_path: Path) -> Path:
    root = tmp_path / "project"
    (root / "src").mkdir(parents=True)
    (root / "tests").mkdir(parents=True)
    (root / "src" / "calc.py").write_text("def add(a, b):\n    return a + b\n",
                                          encoding="utf-8")
    (root / "tests" / "test_calc.py").write_text(
        "from calc import add\n\n\ndef test_add():\n    assert add(1, 2) == 3\n",
        encoding="utf-8")
    return root


def _config(root: Path, profile_ids: list[str]):
    specs = [registry.ProfileSpec(id=name, description=f"{name} profile",
                                  runner="pytest", targets=["tests"],
                                  pythonpath=["src"], timeout_seconds=300)
             for name in profile_ids]
    draft = registry.draft(root, display_name="amend-probe", template="feature",
                           project_id="amend-probe", write_roots=["src", "tests"])
    return draft.model_copy(update={"profiles": specs})


def test_a_newly_approved_configuration_amends_the_durable_row(tmp_path):
    root = _project(tmp_path)
    home = tmp_path / "runtime"
    store = Store(core_cli.database_path(home))
    store.initialize()
    index = registry.Registry(home)

    first = _config(root, ["artifact"])
    registry.write_config(first, approve=True)
    registered = index.register(registry.read_config(root), store=store)
    assert registered["entry"]["profiles"] == ["artifact"]
    original = store.project("amend-probe").authorization_digest

    # the user approves a different configuration: a real test profile replaces the
    # artifact checker
    second = _config(root, ["test"])
    registry.write_config(second, approve=True)
    amended = index.register(registry.read_config(root), store=store)

    assert amended["amended"] is not None, "the durable row was not amended"
    assert amended["amended"]["previous_authorization_digest"] == original
    assert amended["entry"]["profiles"] == ["test"]
    assert amended["entry"]["amended_from"] == original
    assert store.project("amend-probe").authorization_digest == \
        amended["entry"]["authorization_digest"]
    # the digest the run will compile from now matches the durable row: no drift
    compiled, _profiles = registry.compile_project(registry.read_config(root), home=home)
    assert compiled.authorization_digest == store.project("amend-probe").authorization_digest


def test_the_store_still_refuses_an_implicit_re_registration(tmp_path):
    root = _project(tmp_path)
    home = tmp_path / "runtime"
    store = Store(core_cli.database_path(home))
    store.initialize()
    registry.write_config(_config(root, ["artifact"]), approve=True)
    index = registry.Registry(home)
    entry = index.register(registry.read_config(root), store=store)
    project = registry.compile_project(registry.read_config(root), home=home)[0]

    # a different authorization presented through register_project is refused, so a
    # run cannot widen its own scope behind the user's back
    widened = project.model_copy(update={"authorization_digest": "b" * 64})
    with pytest.raises(AuthorizationError):
        store.register_project(widened)
    assert store.project("amend-probe").authorization_digest == \
        entry["entry"]["authorization_digest"]


def test_amending_an_unchanged_configuration_reports_no_change(tmp_path):
    root = _project(tmp_path)
    home = tmp_path / "runtime"
    store = Store(core_cli.database_path(home))
    store.initialize()
    registry.write_config(_config(root, ["test"]), approve=True)
    index = registry.Registry(home)
    index.register(registry.read_config(root), store=store)

    project = registry.compile_project(registry.read_config(root), home=home)[0]
    result = store.amend_project(project)
    assert result["changed"] is False
    assert result["authorization_digest"] == project.authorization_digest


def test_amending_a_project_that_was_never_registered_is_a_refusal(tmp_path):
    root = _project(tmp_path)
    home = tmp_path / "runtime"
    store = Store(core_cli.database_path(home))
    store.initialize()
    registry.write_config(_config(root, ["test"]), approve=True)
    project = registry.compile_project(registry.read_config(root), home=home)[0]
    from kvflow.core.errors import NotFoundError

    with pytest.raises(NotFoundError):
        store.amend_project(project)
