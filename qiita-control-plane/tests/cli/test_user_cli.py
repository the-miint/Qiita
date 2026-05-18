"""Unit tests for the qiita end-user CLI scaffold + subcommands.

Subcommand-specific helpers (loopback flow, whoami, token I/O) live in
cli._common and are tested directly there or via test_cli_login.py.
This file covers the user-CLI argparse wiring and per-subcommand
dispatch.
"""

from pathlib import Path

import pytest


def test_help_exits_cleanly(capsys):
    """`qiita --help` should print help and exit 0. Cheapest smoke test
    that the parser is well-formed and the entry point is reachable."""
    from qiita_control_plane.cli.user import main

    with pytest.raises(SystemExit) as exc_info:
        main(["--help"])
    assert exc_info.value.code == 0
    out = capsys.readouterr().out
    assert "qiita" in out
    assert "--base-url" in out


def test_no_subcommand_errors():
    """Without a subcommand argparse rejects the invocation. Locks in the
    required=True wiring on the subparser."""
    from qiita_control_plane.cli.user import main

    with pytest.raises(SystemExit) as exc_info:
        main([])
    # argparse exits 2 on required-arg-missing.
    assert exc_info.value.code == 2


def test_login_dispatches_to_do_login_with_qiita_command_string(monkeypatch):
    """`qiita login` calls `_common.do_login` with the parsed --base-url and
    --token-file, plus cli_command="qiita login" so error messages tell
    the user to re-run the right binary (not `qiita-admin login`)."""
    from qiita_control_plane.cli import _common
    from qiita_control_plane.cli.user import main

    captured: dict = {}

    def fake_do_login(*, base_url: str, token_file: Path, cli_command: str) -> int:
        captured["base_url"] = base_url
        captured["token_file"] = token_file
        captured["cli_command"] = cli_command
        return 0

    monkeypatch.setattr(_common, "do_login", fake_do_login)

    rc = main(
        ["--base-url", "https://qiita.example.test", "login", "--token-file", "/tmp/qiita-user"]
    )
    assert rc == 0
    assert captured["base_url"] == "https://qiita.example.test"
    assert captured["token_file"] == Path("/tmp/qiita-user")
    assert captured["cli_command"] == "qiita login"


def test_whoami_dispatches_with_base_url(monkeypatch):
    """`qiita whoami` calls `_common.whoami` with the parsed --base-url and
    the PAT loaded by run_http_subcommand."""
    from qiita_control_plane.cli import _common
    from qiita_control_plane.cli.user import main

    captured: dict = {}

    def fake_whoami(base_url: str, token: str) -> dict:
        captured["base_url"] = base_url
        captured["token"] = token
        return {"kind": "human", "principal_idx": 7}

    monkeypatch.setattr(_common, "whoami", fake_whoami)
    monkeypatch.setenv("QIITA_TOKEN", "qk_test_user")

    rc = main(["--base-url", "https://qiita.example.test", "whoami"])
    assert rc == 0
    assert captured["base_url"] == "https://qiita.example.test"
    assert captured["token"] == "qk_test_user"


def test_whoami_without_token_errors(monkeypatch, tmp_path, capsys):
    """If QIITA_TOKEN is unset and no token file exists, whoami exits 1 with
    a message naming QIITA_TOKEN. Mirrors the admin behavior."""
    from qiita_control_plane.cli.user import main

    monkeypatch.delenv("QIITA_TOKEN", raising=False)
    monkeypatch.setattr(
        "qiita_control_plane.cli._common.TOKEN_FILE_DEFAULT",
        tmp_path / "absent",
    )
    rc = main(["whoami"])
    assert rc == 1
    assert "QIITA_TOKEN" in capsys.readouterr().err
