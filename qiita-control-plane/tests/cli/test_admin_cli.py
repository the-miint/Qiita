"""Unit tests for qiita-admin CLI helpers (no DB, no live HTTP).

set-system-role tests use a real DB and live in tests/integration/test_admin_cli.py
(deferred — for now, set-system-role is exercised manually during the
first-deploy flow). The HTTP subcommand helpers are tested here against
a stubbed httpx response.

Shared helpers (token I/O, loopback login, whoami) live in
qiita_control_plane.cli._common and are tested directly there or via
test_cli_login.py. This file covers admin-specific surface plus the
argparse wiring on the admin entry point.
"""

import httpx
import pytest


def test_read_token_from_env(monkeypatch):
    from qiita_control_plane.cli._common import read_token

    monkeypatch.setenv("QIITA_TOKEN", "qk_test_token")
    assert read_token() == "qk_test_token"


def test_read_token_from_file(monkeypatch, tmp_path):
    from qiita_control_plane.cli._common import read_token

    monkeypatch.delenv("QIITA_TOKEN", raising=False)
    f = tmp_path / "token"
    f.write_text("qk_from_file\n")
    assert read_token(token_file=f) == "qk_from_file"


def test_read_token_raises_with_actionable_message(monkeypatch, tmp_path):
    from qiita_control_plane.cli._common import read_token

    monkeypatch.delenv("QIITA_TOKEN", raising=False)
    nonexistent = tmp_path / "nope"
    with pytest.raises(RuntimeError, match="QIITA_TOKEN"):
        read_token(token_file=nonexistent)


def test_whoami_calls_correct_url(monkeypatch):
    """`_common.whoami` round-trips through the unified `call` helper:
    base-url trimmed, API_PREFIX prepended, BEARER_PREFIX in the auth
    header. Patches httpx.request (the verb-agnostic entry point `call`
    delegates to) so a single test exercises the shared shape."""
    from qiita_control_plane.cli import _common

    captured = {}

    def fake_request(method, url, headers=None, json=None, timeout=None):
        captured["method"] = method
        captured["url"] = url
        captured["auth"] = headers["Authorization"]
        return httpx.Response(
            200,
            json={"kind": "human", "principal_idx": 7},
            request=httpx.Request(method, url),
        )

    monkeypatch.setattr(_common.httpx, "request", fake_request)
    body = _common.whoami("https://api.example.com/", "qk_X")
    assert captured["method"] == "GET"
    assert captured["url"] == "https://api.example.com/api/v1/auth/whoami"
    assert captured["auth"] == "Bearer qk_X"
    assert body == {"kind": "human", "principal_idx": 7}


def test_token_revoke_all_calls_correct_url(monkeypatch):
    from qiita_control_plane.cli import _common
    from qiita_control_plane.cli import admin as cli

    captured = {}

    def fake_request(method, url, headers=None, json=None, timeout=None):
        captured["method"] = method
        captured["url"] = url
        captured["auth"] = headers["Authorization"]
        return httpx.Response(
            200,
            json={"revoked_token_idxs": [1, 2], "already_revoked_count": 0},
            request=httpx.Request(method, url),
        )

    monkeypatch.setattr(_common.httpx, "request", fake_request)
    body = cli._token_revoke_all("http://localhost:8080", "qk_admin", 42)
    assert captured["method"] == "POST"
    assert captured["url"] == "http://localhost:8080/api/v1/admin/principal/42/revoke-all-tokens"
    assert captured["auth"] == "Bearer qk_admin"
    assert body["revoked_token_idxs"] == [1, 2]


def test_main_login_dispatches_to_do_login(monkeypatch):
    """Wiring test: `qiita-admin login` calls `_common.do_login` with the parsed
    --base-url and --token-file, plus cli_command="qiita-admin login" so
    error messages tell the user to re-run the admin binary. The actual
    flow logic is exercised by test_cli_login.py (helpers) and
    tests/integration (end-to-end)."""
    from pathlib import Path

    from qiita_control_plane.cli import _common
    from qiita_control_plane.cli import admin as cli

    captured: dict = {}

    def fake_do_login(*, base_url: str, token_file: Path, cli_command: str) -> int:
        captured["base_url"] = base_url
        captured["token_file"] = token_file
        captured["cli_command"] = cli_command
        return 0

    monkeypatch.setattr(_common, "do_login", fake_do_login)

    rc = cli.main(
        ["--base-url", "https://qiita.example.test", "login", "--token-file", "/tmp/qiita-cli-test"]
    )
    assert rc == 0
    assert captured["base_url"] == "https://qiita.example.test"
    assert captured["token_file"] == Path("/tmp/qiita-cli-test")
    assert captured["cli_command"] == "qiita-admin login"


def test_main_set_system_role_validates_role():
    import asyncio

    from qiita_control_plane.cli.admin import _set_system_role

    with pytest.raises(ValueError, match="role must be one of"):
        asyncio.run(_set_system_role("postgres://x", "x@x.com", "super_admin"))


def test_main_whoami_without_token(monkeypatch, tmp_path, capsys):
    """If QIITA_TOKEN is unset and no token file exists, whoami exits 1."""
    from qiita_control_plane.cli.admin import main

    monkeypatch.delenv("QIITA_TOKEN", raising=False)
    monkeypatch.setattr(
        "qiita_control_plane.cli._common.TOKEN_FILE_DEFAULT",
        tmp_path / "absent",
    )
    rc = main(["whoami"])
    assert rc == 1
    assert "QIITA_TOKEN" in capsys.readouterr().err
