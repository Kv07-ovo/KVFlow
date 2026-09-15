"""The web source checker: real source-level proof, and honest about its limits.

The live cross-project leg found the situation this checker exists for: a web
project whose ``node`` runtime is not installed. The checker must accept a
correct web deliverable, reject a broken one, and never claim it executed code.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from kvflow.checks import webcheck

LIB = (
    "export function slugify(value) {\n"
    "  return String(value).trim().toLowerCase().replace(/\\s+/g, '-');\n"
    "}\n\n"
    "export function dekebab(value) {\n"
    "  return String(value).replace(/-/g, ' ');\n"
    "}\n"
)
TEST = (
    "import test from 'node:test';\n"
    "import assert from 'node:assert/strict';\n"
    "import { dekebab } from '../src/app.mjs';\n\n"
    "test('dekebab turns dashes into spaces', () => {\n"
    "  assert.equal(dekebab('hello-world'), 'hello world');\n"
    "});\n"
)


@pytest.fixture()
def web(tmp_path: Path) -> Path:
    (tmp_path / "src").mkdir()
    (tmp_path / "test").mkdir()
    (tmp_path / "src" / "app.mjs").write_text(LIB, encoding="utf-8")
    (tmp_path / "test" / "dekebab.test.mjs").write_text(TEST, encoding="utf-8")
    return tmp_path


def test_a_correct_web_deliverable_passes(web: Path):
    report = webcheck.check(
        [
            {"kind": "export", "file": "src/app.mjs", "name": "dekebab"},
            {"kind": "import", "file": "test/dekebab.test.mjs",
             "specifier": "../src/app.mjs", "name": "dekebab"},
            {"kind": "test", "file": "test/dekebab.test.mjs", "name": "dekebab"},
        ],
        root=web,
    )
    assert report["ok"] is True, report["failures"]
    assert report["executes_the_code"] is False
    # the checker never claims more than a source-level check
    assert "does not run a web test runner" in report["note"]
    resolved = report["files"][1]["resolved"]
    assert resolved == "src/app.mjs"


def test_a_missing_export_is_a_failure(web: Path):
    report = webcheck.check(
        [{"kind": "export", "file": "src/app.mjs", "name": "camelize"}], root=web
    )
    assert report["ok"] is False
    assert "not a top-level export" in report["failures"][0]["problem"]
    assert "dekebab" in report["files"][0]["exports"]


def test_an_import_that_does_not_resolve_is_a_failure(web: Path):
    report = webcheck.check(
        [{"kind": "import", "file": "test/dekebab.test.mjs",
          "specifier": "../src/gone.mjs", "name": "dekebab"}],
        root=web,
    )
    assert report["ok"] is False
    assert report["files"][0]["resolved"] is None


def test_a_resolved_import_that_does_not_export_the_name_is_a_failure(web: Path):
    report = webcheck.check(
        [{"kind": "import", "file": "test/dekebab.test.mjs",
          "specifier": "../src/app.mjs", "name": "camelize"}],
        root=web,
    )
    assert report["ok"] is False
    assert report["files"][0]["resolved"] == "src/app.mjs"
    assert report["files"][0]["imported"] is False


def test_a_test_file_that_never_calls_the_symbol_is_a_failure(web: Path):
    (web / "test" / "empty.test.mjs").write_text(
        "import test from 'node:test';\n\ntest('nothing', () => {\n  // no call\n});\n",
        encoding="utf-8",
    )
    report = webcheck.check(
        [{"kind": "test", "file": "test/empty.test.mjs", "name": "dekebab"}], root=web
    )
    assert report["ok"] is False
    assert "no declared test calls dekebab" in report["failures"][0]["problem"]
    # a file with no test at all fails the same way, with its own count
    (web / "test" / "untested.test.mjs").write_text(
        "export const nothing = 1;\n", encoding="utf-8"
    )
    report = webcheck.check(
        [{"kind": "test", "file": "test/untested.test.mjs", "name": "dekebab"}], root=web
    )
    assert report["ok"] is False


def test_a_file_outside_the_workspace_is_refused(web: Path):
    outside = web.parent / "outside.mjs"
    outside.write_text("export const secret = 1;\n", encoding="utf-8")
    report = webcheck.check(
        [{"kind": "export", "file": "../outside.mjs", "name": "secret"}], root=web
    )
    assert report["ok"] is False
    assert report["failures"][0]["problem"] == "escapes the workspace"


def test_the_cli_exit_code_is_the_receipt(web: Path, capsys):
    code = webcheck.main([
        "--root", str(web),
        "--require-export", "src/app.mjs:dekebab",
        "--require-import", "test/dekebab.test.mjs:../src/app.mjs:dekebab",
        "--require-test", "test/dekebab.test.mjs:dekebab",
        "--require-token", "src/app.mjs:export function",
    ])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["kind"] == "WEB_SOURCE_CHECK"
    assert payload["ok"] is True

    code = webcheck.main(["--root", str(web), "--require-export", "src/app.mjs:nope"])
    assert code == 1
    assert json.loads(capsys.readouterr().out)["ok"] is False

    # a malformed invocation is a refusal, not a silent pass
    code = webcheck.main(["--root", str(web), "--require-export", "src/app.mjs"])
    assert code == 2
    assert "error" in json.loads(capsys.readouterr().out)
