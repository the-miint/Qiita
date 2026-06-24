"""Tests for the import-clean console-script shim (cli._bootstrap).

The shim translates a stale-qiita_common ImportError raised when LAZILY
importing the real CLI `main` into a clean, actionable operator message,
re-raises any unrelated ImportError untouched, and otherwise delegates to
the real `main`. The key invariant is that importing the shim itself pulls
in no qiita_common (so it stays importable when qiita_common is stale).
"""

import subprocess
import sys

import pytest

from qiita_control_plane.cli import _bootstrap

# --- _is_qiita_common_staleness classifier ----------------------------------


def test_classifier_true_for_named_qiita_common_submodule() -> None:
    exc = ImportError(
        "cannot import name 'PATH_PREP_SAMPLE_RETIRED' from 'qiita_common.api_paths'",
        name="qiita_common.api_paths",
    )
    assert _bootstrap._is_qiita_common_staleness(exc) is True


def test_classifier_true_for_module_not_found_qiita_common() -> None:
    exc = ModuleNotFoundError("No module named 'qiita_common'", name="qiita_common")
    assert _bootstrap._is_qiita_common_staleness(exc) is True


def test_classifier_true_when_only_message_mentions_qiita_common() -> None:
    # No .name attribute set, but the message names the path-dep.
    exc = ImportError("cannot import name 'X' from 'qiita_common.actions'")
    assert _bootstrap._is_qiita_common_staleness(exc) is True


def test_classifier_false_for_unrelated_named_import() -> None:
    exc = ModuleNotFoundError("No module named 'numpy'", name="numpy")
    assert _bootstrap._is_qiita_common_staleness(exc) is False


def test_classifier_false_for_generic_unrelated_message() -> None:
    exc = ImportError("cannot import name 'foo' from 'some.other.module'")
    assert _bootstrap._is_qiita_common_staleness(exc) is False


# --- _run: import-time guard behaviour --------------------------------------


def test_run_translates_qiita_common_staleness_to_clean_exit(capsys) -> None:
    def _import_main():
        raise ImportError(
            "cannot import name 'PATH_PREP_SAMPLE_RETIRED' from 'qiita_common.api_paths'",
            name="qiita_common.api_paths",
        )

    with pytest.raises(SystemExit) as excinfo:
        _bootstrap._run(_import_main)

    assert excinfo.value.code == 1
    err = capsys.readouterr().err
    # Actionable: states staleness, gives the exact fix, echoes the original error.
    assert "out of date" in err
    assert "uv sync --reinstall-package qiita-common" in err
    assert "PATH_PREP_SAMPLE_RETIRED" in err
    # Clean message, not a raw traceback dump.
    assert "Traceback (most recent call last)" not in err


def test_run_reraises_unrelated_import_error_untouched() -> None:
    sentinel_exc = ModuleNotFoundError("No module named 'numpy'", name="numpy")

    def _import_main():
        raise sentinel_exc

    # An unrelated ImportError must propagate as-is, NOT become SystemExit.
    with pytest.raises(ModuleNotFoundError) as excinfo:
        _bootstrap._run(_import_main)
    assert excinfo.value is sentinel_exc


def test_run_delegates_to_main_on_success() -> None:
    sentinel = object()
    called = {}

    def _fake_main():
        called["yes"] = True
        return sentinel

    def _import_main():
        return _fake_main

    result = _bootstrap._run(_import_main)
    assert result is sentinel
    assert called == {"yes": True}


def test_run_does_not_translate_error_from_main_call() -> None:
    # An ImportError raised by main() ITSELF (after a successful import of main)
    # must NOT be translated — only the import of main is guarded.
    def _import_main():
        def _main():
            raise ImportError("lazy handler import failed", name="qiita_common")

        return _main

    with pytest.raises(ImportError) as excinfo:
        _bootstrap._run(_import_main)
    assert "lazy handler import failed" in str(excinfo.value)


# --- public entry points delegate to the real main on success ---------------


def test_qiita_entrypoint_delegates_to_real_main(monkeypatch) -> None:
    import qiita_control_plane.cli.user as user_mod

    sentinel = object()
    monkeypatch.setattr(user_mod, "main", lambda: sentinel)
    assert _bootstrap.qiita() is sentinel


def test_qiita_admin_entrypoint_delegates_to_real_main(monkeypatch) -> None:
    import qiita_control_plane.cli.admin as admin_mod

    sentinel = object()
    monkeypatch.setattr(admin_mod, "main", lambda: sentinel)
    assert _bootstrap.qiita_admin() is sentinel


# --- import-cleanliness invariant (the whole point of the shim) -------------


def test_bootstrap_import_does_not_pull_in_qiita_common() -> None:
    """Importing the shim must not import qiita_common.

    Run in a fresh subprocess so an already-imported qiita_common in this
    test session can't produce a false pass.
    """
    code = (
        "import qiita_control_plane.cli._bootstrap, sys; "
        "assert 'qiita_common' not in sys.modules, "
        "sorted(m for m in sys.modules if m.startswith('qiita_common'))"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, f"importing _bootstrap pulled in qiita_common:\n{result.stderr}"
