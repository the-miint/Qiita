"""Unit tests for qiita-admin CLI helpers (no DB, no live HTTP).

set-system-role tests use a real DB and live in tests/integration/test_admin_cli.py
(deferred — for now, set-system-role is exercised manually during the
first-deploy flow). The HTTP subcommand helpers are tested here against
a stubbed httpx response.
"""

import httpx
import pytest


def test_read_token_from_env(monkeypatch):
    from qiita_control_plane.cli.admin import _read_token

    monkeypatch.setenv("QIITA_TOKEN", "qk_test_token")
    assert _read_token() == "qk_test_token"


def test_read_token_from_file(monkeypatch, tmp_path):
    from qiita_control_plane.cli.admin import _read_token

    monkeypatch.delenv("QIITA_TOKEN", raising=False)
    f = tmp_path / "token"
    f.write_text("qk_from_file\n")
    assert _read_token(token_file=f) == "qk_from_file"


def test_read_token_raises_with_actionable_message(monkeypatch, tmp_path):
    from qiita_control_plane.cli.admin import _read_token

    monkeypatch.delenv("QIITA_TOKEN", raising=False)
    nonexistent = tmp_path / "nope"
    with pytest.raises(RuntimeError, match="QIITA_TOKEN"):
        _read_token(token_file=nonexistent)


def test_whoami_calls_correct_url(monkeypatch):
    from qiita_control_plane.cli import admin as cli

    captured = {}

    def fake_get(url, headers=None, timeout=None):
        captured["url"] = url
        captured["auth"] = headers["Authorization"]
        return httpx.Response(
            200,
            json={"kind": "human", "principal_idx": 7},
            request=httpx.Request("GET", url),
        )

    monkeypatch.setattr(cli.httpx, "get", fake_get)
    body = cli._whoami("https://api.example.com/", "qk_X")
    assert captured["url"] == "https://api.example.com/api/v1/auth/whoami"
    assert captured["auth"] == "Bearer qk_X"
    assert body == {"kind": "human", "principal_idx": 7}


def test_token_revoke_all_calls_correct_url(monkeypatch):
    from qiita_control_plane.cli import admin as cli

    captured = {}

    def fake_post(url, headers=None, timeout=None):
        captured["url"] = url
        captured["auth"] = headers["Authorization"]
        return httpx.Response(
            200,
            json={"revoked_token_idxs": [1, 2], "already_revoked_count": 0},
            request=httpx.Request("POST", url),
        )

    monkeypatch.setattr(cli.httpx, "post", fake_post)
    body = cli._token_revoke_all("http://localhost:8080", "qk_admin", 42)
    assert captured["url"] == "http://localhost:8080/api/v1/admin/principal/42/revoke-all-tokens"
    assert body["revoked_token_idxs"] == [1, 2]


def test_main_login_dispatches_to_do_login(monkeypatch):
    """Wiring test: `qiita-admin login` calls `_do_login` with the parsed
    --base-url and --token-file. The actual flow logic is exercised by
    test_cli_login.py (helpers) and tests/integration (end-to-end)."""
    from pathlib import Path

    from qiita_control_plane.cli import admin as cli

    captured: dict = {}

    def fake_do_login(*, base_url: str, token_file: Path) -> int:
        captured["base_url"] = base_url
        captured["token_file"] = token_file
        return 0

    monkeypatch.setattr(cli, "_do_login", fake_do_login)

    rc = cli.main(
        ["--base-url", "https://qiita.example.test", "login", "--token-file", "/tmp/qiita-cli-test"]
    )
    assert rc == 0
    assert captured["base_url"] == "https://qiita.example.test"
    assert captured["token_file"] == Path("/tmp/qiita-cli-test")


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
        "qiita_control_plane.cli.admin._TOKEN_FILE_DEFAULT",
        tmp_path / "absent",
    )
    rc = main(["whoami"])
    assert rc == 1
    assert "QIITA_TOKEN" in capsys.readouterr().err
