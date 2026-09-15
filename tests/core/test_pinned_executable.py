"""A project may pin the exact toolchain binary its approved profile names.

The live cross-project run found this: this machine carries a bundled Node runtime
that is deliberately not on PATH, so ``node --test`` could not be resolved even
though the runtime exists and the profile was approved. Pinning the absolute path
in the profile is the honest fix - the allowlist still applies, the file must
really exist, and a bare name still resolves from PATH as before.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

from kvflow.core.errors import ContractError
from kvflow.core.tools import _resolve_executable


def test_a_pinned_absolute_path_is_used_verbatim():
    resolved = _resolve_executable(str(Path(sys.executable)))
    assert resolved == [str(Path(sys.executable))]


def test_a_pinned_path_that_does_not_exist_is_refused():
    missing = Path(sys.executable).with_name("node.exe")
    with pytest.raises(ContractError) as excinfo:
        _resolve_executable(str(missing))
    assert excinfo.value.code == "CONTRACT_INVALID"
    assert "pinned executable does not exist" in str(excinfo.value)


def test_the_allowlist_still_applies_to_a_pinned_path():
    # an absolute path to a program KVFlow does not run is refused before anything
    # checks whether the file exists
    with pytest.raises(ContractError) as excinfo:
        _resolve_executable(r"C:\Windows\System32\cmd.exe")
    assert "not allowlisted" in str(excinfo.value)


def test_python_still_means_this_interpreter_unless_pinned():
    assert _resolve_executable("python") == [sys.executable]
    assert _resolve_executable("pytest") == [sys.executable, "-m", "pytest"]
