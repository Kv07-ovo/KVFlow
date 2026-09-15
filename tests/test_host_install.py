"""Installing, upgrading and uninstalling the host integration.

The profile is the real integration surface: a ``package.json`` that depends on
the plugin and lists it as a bundle, plus one ``cordis.patch.yml`` row. These
tests drive that edit against a synthetic DSH home (so no real profile is ever
touched) with the ``dsh`` CLI stubbed, and they pin the parts that matter:

* a dry run writes nothing;
* an install verifies the Python first, backs the profile up, and only then edits;
* a failing install rolls the profile back to the bytes it had before;
* the KVFlow patch row is replaced, never duplicated, and no other row is touched;
* an uninstall removes exactly the KVFlow row and dependency.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from kvflow import install
from kvflow.core.errors import ConfigError, NotFoundError

PROFILE_PACKAGE = {
    "name": "dsh-profile-fixture",
    "private": True,
    "dependencies": {"some-other-plugin": "1.0.0"},
    "dsh": {"profile": {"bundles": ["@deepseek-ai/dsh-base", "some-other-plugin"],
                        "patchReload": "live"}},
}

PATCH = (
    "# a user patch layer\n"
    "- id: some-other-plugin\n"
    "  disabled: true\n"
)


@pytest.fixture()
def dsh_home(tmp_path: Path, monkeypatch) -> Path:
    home = tmp_path / "dsh"
    profile = home / "profiles" / "fixture"
    profile.mkdir(parents=True)
    (profile / "package.json").write_text(
        json.dumps(PROFILE_PACKAGE, indent=2), encoding="utf-8"
    )
    (profile / "cordis.patch.yml").write_text(PATCH, encoding="utf-8")
    monkeypatch.setenv("DSH_HOME", str(home))
    # the plugin source is found next to the package; point it at the real one
    monkeypatch.setattr(install, "plugin_source", lambda: _real_plugin())
    monkeypatch.setattr(install, "verify",
                        lambda python, *, home, timeout=120: {
                            "version": "0.1.0", "module": "kvflow/__init__.py",
                            "tools": 10, "python": "3.14.7"})
    monkeypatch.setattr(install, "host_processes", lambda: [])
    return home


def _real_plugin() -> Path:
    root = Path(install.__file__).resolve().parents[2]
    return root / "dsh-plugin"


def _fake_dsh(monkeypatch, *, add_result: int = 0, remove_result: int = 0,
              compose_ok: bool = True):
    """A stubbed ``dsh`` that edits package.json the way pnpm does.

    ``--dump-config`` is answered the way the real host does: a composed tree that
    either contains the KVFlow row or does not, so the install's compose gate is
    exercised rather than bypassed.
    """
    calls: list[list[str]] = []

    def runner(argv, **kwargs):
        calls.append(list(argv))

        class Result:
            returncode = 0
            stdout = ""
            stderr = ""

        result = Result()
        profile = install.resolve_profile(None)[1]
        if "--dump-config" in argv:
            result.stdout = (
                "- id: kvflow\n  name: kvflow-dsh\n  config:\n    pythonPath: x\n"
                if compose_ok else "- id: dsh-base\n"
            )
            return result
        document = install.read_profile(profile)
        if argv[2:4] == ["--profile", "fixture"] and argv[4] == "add":
            if add_result:
                result.returncode = add_result
                result.stderr = "pnpm add failed"
                return result
            document["dependencies"][install.PLUGIN_PACKAGE] = f"link:{argv[5]}"
            bundles = document["dsh"]["profile"]["bundles"]
            if install.PLUGIN_PACKAGE not in bundles:
                bundles.append(install.PLUGIN_PACKAGE)
        elif argv[2:4] == ["--profile", "fixture"] and argv[4] == "remove":
            if remove_result:
                result.returncode = remove_result
                result.stderr = "pnpm remove failed"
                return result
            document["dependencies"].pop(install.PLUGIN_PACKAGE, None)
            document["dsh"]["profile"]["bundles"] = [
                item for item in document["dsh"]["profile"]["bundles"]
                if item != install.PLUGIN_PACKAGE
            ]
        (profile / "package.json").write_text(
            json.dumps(document, indent=2), encoding="utf-8"
        )
        return result

    monkeypatch.setattr(install, "dsh_cli", lambda: "dsh")
    monkeypatch.setattr(install.subprocess, "run", runner)
    return calls


def test_the_profile_is_resolved_or_refused_by_name(dsh_home: Path):
    name, directory = install.resolve_profile(None)
    assert name == "fixture"
    assert install.list_profiles() == ["fixture"]
    with pytest.raises(NotFoundError):
        install.resolve_profile("missing")
    # two profiles means the caller must choose
    (dsh_home / "profiles" / "second").mkdir()
    (dsh_home / "profiles" / "second" / "package.json").write_text("{}", encoding="utf-8")
    with pytest.raises(ConfigError):
        install.resolve_profile(None)


def test_a_dry_run_writes_nothing(dsh_home: Path, tmp_path: Path):
    before = (dsh_home / "profiles" / "fixture" / "package.json").read_bytes()
    report = install.install(
        profile="fixture", home=tmp_path / "kvhome", python=Path("python.exe"),
        python_path=None, workspace=None, version="0.1.0", dry_run=True,
    )
    assert report["action"] == "dry_run"
    assert report["plan"]["dependency"] == {
        install.PLUGIN_PACKAGE: f"link:{_real_plugin()}"}
    assert (dsh_home / "profiles" / "fixture" / "package.json").read_bytes() == before
    assert not (dsh_home / "profiles" / "fixture" / "cordis.patch.yml").read_text(
        encoding="utf-8").count("kvflow")


def test_install_binds_the_plugin_and_verifies_it(dsh_home: Path, tmp_path: Path,
                                                  monkeypatch):
    calls = _fake_dsh(monkeypatch)
    report = install.install(
        profile="fixture", home=tmp_path / "kvhome", python=Path("python.exe"),
        python_path=tmp_path / "src", workspace=None, version="0.1.0",
    )
    assert report["action"] == "installed"
    assert report["verified"] is True
    assert calls and calls[0][:4] == ["dsh", "plugin", "--profile", "fixture"]
    profile = dsh_home / "profiles" / "fixture"
    document = json.loads((profile / "package.json").read_text(encoding="utf-8"))
    assert install.PLUGIN_PACKAGE in document["dependencies"]
    assert install.PLUGIN_PACKAGE in document["dsh"]["profile"]["bundles"]
    patch = (profile / "cordis.patch.yml").read_text(encoding="utf-8")
    assert install.has_kvflow_row(patch)
    assert "- id: some-other-plugin" in patch, "another plugin's row was removed"
    assert Path(report["backup"]).is_dir()
    # the config the host will read is the config that was written
    assert report["binding"]["config"]["home"] == str(tmp_path / "kvhome").replace("\\", "/")
    assert install.status("fixture")["installed"] is True


def test_installing_twice_replaces_the_row_instead_of_duplicating_it(
    dsh_home: Path, tmp_path: Path, monkeypatch
):
    _fake_dsh(monkeypatch)
    for _ in range(2):
        install.install(
            profile="fixture", home=tmp_path / "kvhome", python=Path("python.exe"),
            python_path=None, workspace=None, version="0.1.0",
        )
    patch = (dsh_home / "profiles" / "fixture" / "cordis.patch.yml").read_text(
        encoding="utf-8")
    assert patch.count("- id: kvflow") == 1
    # the other plugin's row survives both installs, unchanged and unduplicated
    assert patch.count("- id: some-other-plugin") == 1
    assert "- id: some-other-plugin\n  disabled: true" in patch


def test_an_upgrade_needs_something_installed(dsh_home: Path, tmp_path: Path,
                                              monkeypatch):
    _fake_dsh(monkeypatch)
    with pytest.raises(ConfigError):
        install.install(
            profile="fixture", home=tmp_path / "kvhome", python=Path("python.exe"),
            python_path=None, workspace=None, version="0.2.0", upgrade=True,
        )


def test_a_failed_install_is_rolled_back(dsh_home: Path, tmp_path: Path, monkeypatch):
    _fake_dsh(monkeypatch, add_result=1)
    profile = dsh_home / "profiles" / "fixture"
    before = (profile / "package.json").read_bytes()
    before_patch = (profile / "cordis.patch.yml").read_bytes()
    report = install.install(
        profile="fixture", home=tmp_path / "kvhome", python=Path("python.exe"),
        python_path=None, workspace=None, version="0.1.0",
    )
    assert report["action"] == "install_failed"
    assert report["verified"] is False
    assert report["rolled_back"]["restored"]
    assert (profile / "package.json").read_bytes() == before
    assert (profile / "cordis.patch.yml").read_bytes() == before_patch
    assert install.status("fixture")["installed"] is False


def test_uninstall_removes_only_the_kvflow_row(dsh_home: Path, tmp_path: Path,
                                               monkeypatch):
    _fake_dsh(monkeypatch)
    install.install(profile="fixture", home=tmp_path / "kvhome",
                    python=Path("python.exe"), python_path=None, workspace=None,
                    version="0.1.0")
    report = install.uninstall(profile="fixture", version="0.1.0")
    assert report["action"] == "uninstalled"
    assert report["detached"] is True
    profile = dsh_home / "profiles" / "fixture"
    document = json.loads((profile / "package.json").read_text(encoding="utf-8"))
    assert install.PLUGIN_PACKAGE not in document["dependencies"]
    assert install.PLUGIN_PACKAGE not in document["dsh"]["profile"]["bundles"]
    patch = (profile / "cordis.patch.yml").read_text(encoding="utf-8")
    assert not install.has_kvflow_row(patch)
    assert "- id: some-other-plugin" in patch


def test_a_failed_uninstall_restores_the_profile(dsh_home: Path, tmp_path: Path,
                                                 monkeypatch):
    _fake_dsh(monkeypatch)
    install.install(profile="fixture", home=tmp_path / "kvhome",
                    python=Path("python.exe"), python_path=None, workspace=None,
                    version="0.1.0")
    profile = dsh_home / "profiles" / "fixture"
    before = (profile / "package.json").read_bytes()
    _fake_dsh(monkeypatch, remove_result=1)
    report = install.uninstall(profile="fixture", version="0.1.0")
    assert report["action"] == "uninstall_failed"
    assert (profile / "package.json").read_bytes() == before
    assert install.status("fixture")["installed"] is True


def test_a_profile_the_host_cannot_compose_is_rolled_back(dsh_home: Path, tmp_path: Path,
                                                          monkeypatch):
    """The live acceptance run found this one: a written patch the host rejects."""
    _fake_dsh(monkeypatch, compose_ok=False)
    profile = dsh_home / "profiles" / "fixture"
    before = (profile / "package.json").read_bytes()
    before_patch = (profile / "cordis.patch.yml").read_bytes()
    report = install.install(
        profile="fixture", home=tmp_path / "kvhome", python=Path("python.exe"),
        python_path=None, workspace=None, version="0.1.0",
    )
    assert report["action"] == "install_failed"
    assert any("could not compose" in step.get("step", "") or
               step.get("kvflow_row_composed") is False for step in report["steps"])
    assert (profile / "package.json").read_bytes() == before
    assert (profile / "cordis.patch.yml").read_bytes() == before_patch


def test_a_bare_empty_list_document_is_replaced_not_appended_to():
    """``[]`` is a complete YAML document; a row after it is a second document."""
    text = "# a comment\n[]\n"
    written = install.write_kvflow_row(
        text, {"home": "h", "python": "p", "pythonPath": None, "workspace": None})
    assert "[]" not in written
    assert written.count("- id: kvflow") == 1
    assert install.has_kvflow_row(written)
    # repeated writes keep one banner and one row
    twice = install.write_kvflow_row(
        written, {"home": "h2", "python": "p", "pythonPath": None, "workspace": None})
    assert twice.count("- id: kvflow") == 1
    assert twice.count("written by `kvflow install`") == 1
    assert 'home: "h2"' in twice


def test_the_row_writer_is_limited_to_its_own_id():
    text = "- id: kvflow\n  config:\n    home: \"a\"\n- id: other\n  disabled: true\n"
    replaced = install.write_kvflow_row(
        text, {"home": "b", "python": "c", "pythonPath": None, "workspace": None})
    assert replaced.count("- id: kvflow") == 1
    assert 'home: "b"' in replaced
    assert "- id: other\n  disabled: true" in replaced
    removed = install.remove_kvflow_row(replaced)
    assert not install.has_kvflow_row(removed)
    assert "- id: other" in removed
