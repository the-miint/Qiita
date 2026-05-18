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


# ---------------------------------------------------------------------------
# profile set
# ---------------------------------------------------------------------------


def test_profile_set_sends_only_supplied_fields(monkeypatch):
    """`qiita profile set --affiliation X --phone Y` sends a body with only
    those two keys; unset fields stay absent so the server's exclude_unset
    UPDATE never touches a field the user didn't ask about."""
    import httpx as _httpx

    from qiita_control_plane.cli import _common

    captured: dict = {}

    def fake_request(method, url, headers=None, json=None, timeout=None):
        captured["method"] = method
        captured["url"] = url
        captured["auth"] = headers["Authorization"]
        captured["json"] = json
        return _httpx.Response(200, json={"principal_idx": 7}, request=_httpx.Request(method, url))

    monkeypatch.setattr(_common.httpx, "request", fake_request)
    monkeypatch.setenv("QIITA_TOKEN", "qk_test")

    from qiita_control_plane.cli.user import main

    rc = main(
        [
            "--base-url",
            "https://q.example.test",
            "profile",
            "set",
            "--affiliation",
            "UCSD",
            "--phone",
            "+1-555-0100",
        ]
    )
    assert rc == 0
    assert captured["method"] == "PATCH"
    assert captured["url"] == "https://q.example.test/api/v1/user/me"
    assert captured["auth"] == "Bearer qk_test"
    assert captured["json"] == {"affiliation": "UCSD", "phone": "+1-555-0100"}


def test_profile_set_boolean_optional_action_distinguishes_unset_false_true(monkeypatch):
    """--receive-processing-emails sets True, --no-receive-processing-emails sets False,
    neither leaves the field absent from the PATCH body."""
    import httpx as _httpx

    from qiita_control_plane.cli import _common

    captured_bodies: list[dict] = []

    def fake_request(method, url, headers=None, json=None, timeout=None):
        captured_bodies.append(json)
        return _httpx.Response(200, json={"principal_idx": 7}, request=_httpx.Request(method, url))

    monkeypatch.setattr(_common.httpx, "request", fake_request)
    monkeypatch.setenv("QIITA_TOKEN", "qk_test")

    from qiita_control_plane.cli.user import main

    # --receive-processing-emails → True
    main(["profile", "set", "--receive-processing-emails"])
    # --no-receive-processing-emails → False
    main(["profile", "set", "--no-receive-processing-emails"])

    assert captured_bodies == [
        {"receive_processing_emails": True},
        {"receive_processing_emails": False},
    ]


def test_profile_set_requires_at_least_one_flag(capsys):
    """`qiita profile set` with no flags should error rather than POST an
    empty body. Argparse exits 2 on parser.error()."""
    from qiita_control_plane.cli.user import main

    with pytest.raises(SystemExit) as exc_info:
        main(["profile", "set"])
    assert exc_info.value.code == 2
    assert "at least one of" in capsys.readouterr().err


# ---------------------------------------------------------------------------
# study create
# ---------------------------------------------------------------------------


def test_study_create_minimal_sends_only_title(monkeypatch):
    """`qiita study create --title X` posts a body with only `title`.
    Optional fields stay absent so the server's column defaults apply
    rather than caller-supplied nulls overriding them."""
    import httpx as _httpx

    from qiita_control_plane.cli import _common

    captured: dict = {}

    def fake_request(method, url, headers=None, json=None, timeout=None):
        captured["method"] = method
        captured["url"] = url
        captured["auth"] = headers["Authorization"]
        captured["json"] = json
        return _httpx.Response(201, json={"study_idx": 42}, request=_httpx.Request(method, url))

    monkeypatch.setattr(_common.httpx, "request", fake_request)
    monkeypatch.setenv("QIITA_TOKEN", "qk_test")

    from qiita_control_plane.cli.user import main

    rc = main(["--base-url", "https://q.example.test", "study", "create", "--title", "Smoke Study"])
    assert rc == 0
    assert captured["method"] == "POST"
    assert captured["url"] == "https://q.example.test/api/v1/study"
    assert captured["auth"] == "Bearer qk_test"
    assert captured["json"] == {"title": "Smoke Study"}


def test_study_create_passes_through_optional_fields(monkeypatch):
    """Every supplied optional flag lands in the POST body verbatim;
    snake_case translations for hyphenated CLI flags happen on the
    client side so the server contract stays clean."""
    import httpx as _httpx

    from qiita_control_plane.cli import _common

    captured: dict = {}

    def fake_request(method, url, headers=None, json=None, timeout=None):
        captured["json"] = json
        return _httpx.Response(201, json={"study_idx": 42}, request=_httpx.Request(method, url))

    monkeypatch.setattr(_common.httpx, "request", fake_request)
    monkeypatch.setenv("QIITA_TOKEN", "qk_test")

    from qiita_control_plane.cli.user import main

    rc = main(
        [
            "study",
            "create",
            "--title",
            "T",
            "--alias",
            "T-2026",
            "--description",
            "smoke desc",
            "--abstract",
            "abs",
            "--funding",
            "NIH",
            "--ebi-study-accession",
            "PRJEB99999",
            "--vamps-id",
            "VAMPS-1",
            "--notes",
            "note",
            "--principal-investigator-idx",
            "5",
            "--default-tier",
            "member",
        ]
    )
    assert rc == 0
    assert captured["json"] == {
        "title": "T",
        "alias": "T-2026",
        "description": "smoke desc",
        "abstract": "abs",
        "funding": "NIH",
        "ebi_study_accession": "PRJEB99999",
        "vamps_id": "VAMPS-1",
        "notes": "note",
        "principal_investigator_idx": 5,
        "default_tier": "member",
    }


def test_study_create_requires_title(capsys):
    """--title is the only required flag; missing it should produce an
    argparse error (exit 2)."""
    from qiita_control_plane.cli.user import main

    with pytest.raises(SystemExit) as exc_info:
        main(["study", "create"])
    assert exc_info.value.code == 2
    assert "--title" in capsys.readouterr().err


def test_study_create_rejects_invalid_default_tier(capsys):
    """argparse choices= should reject a typo in --default-tier."""
    from qiita_control_plane.cli.user import main

    with pytest.raises(SystemExit) as exc_info:
        main(["study", "create", "--title", "T", "--default-tier", "owner"])
    assert exc_info.value.code == 2
    assert "default-tier" in capsys.readouterr().err


def test_study_create_pydantic_validation_error_exits_2(capsys):
    """A --title longer than StudyCreate's max_length=500 trips Pydantic
    client-side; we surface a flat error line via parser.error and exit
    2, not a traceback."""
    from qiita_control_plane.cli.user import main

    long_title = "x" * 501
    with pytest.raises(SystemExit) as exc_info:
        main(["study", "create", "--title", long_title])
    assert exc_info.value.code == 2
    err = capsys.readouterr().err
    assert "invalid StudyCreate" in err
    assert "title" in err


def test_profile_set_pydantic_validation_error_exits_2(capsys):
    """A malformed --orcid trips Pydantic client-side via UserUpdate's
    pattern constraint; surfaced as a flat parser.error."""
    from qiita_control_plane.cli.user import main

    with pytest.raises(SystemExit) as exc_info:
        main(["profile", "set", "--orcid", "not-an-orcid"])
    assert exc_info.value.code == 2
    err = capsys.readouterr().err
    assert "invalid UserUpdate" in err
    assert "orcid" in err


# ---------------------------------------------------------------------------
# --base-url http-to-non-localhost guard
# ---------------------------------------------------------------------------


def test_http_to_non_localhost_refused_without_insecure(capsys):
    """Plain http:// to a non-localhost host would send the PAT in
    cleartext. The CLI refuses (exit 2) unless --insecure is set."""
    from qiita_control_plane.cli.user import main

    with pytest.raises(SystemExit) as exc_info:
        main(["--base-url", "http://qiita.example.com", "whoami"])
    assert exc_info.value.code == 2
    err = capsys.readouterr().err
    assert "cleartext" in err
    assert "qiita.example.com" in err
    assert "--insecure" in err


def test_http_to_non_localhost_allowed_with_insecure(monkeypatch, capsys):
    """--insecure suppresses the refuse, prints a warning to stderr, and
    proceeds with the call."""
    from qiita_control_plane.cli import _common
    from qiita_control_plane.cli.user import main

    def fake_whoami(base_url, token):
        return {"kind": "human"}

    monkeypatch.setattr(_common, "whoami", fake_whoami)
    monkeypatch.setenv("QIITA_TOKEN", "qk_test")

    rc = main(["--base-url", "http://qiita.example.com", "--insecure", "whoami"])
    assert rc == 0
    err = capsys.readouterr().err
    assert "warning" in err
    assert "cleartext" in err


def test_http_to_localhost_allowed_without_insecure(monkeypatch):
    """Plain http:// to localhost / 127.0.0.1 / ::1 / 127.x.x.x is always
    permitted — traffic stays on the host."""
    from qiita_control_plane.cli import _common
    from qiita_control_plane.cli.user import main

    monkeypatch.setattr(_common, "whoami", lambda base_url, token: {})
    monkeypatch.setenv("QIITA_TOKEN", "qk_test")

    # Default base-url is already http://localhost — verify a few hostnames.
    for url in (
        "http://localhost:8080",
        "http://127.0.0.1:8080",
        "http://127.0.0.2:8080",
        "http://[::1]:8080",
    ):
        rc = main(["--base-url", url, "whoami"])
        assert rc == 0, f"expected localhost URL to be allowed: {url}"


def test_https_to_non_localhost_allowed(monkeypatch):
    """https:// is always fine regardless of hostname — the bearer is
    encrypted in transit."""
    from qiita_control_plane.cli import _common
    from qiita_control_plane.cli.user import main

    monkeypatch.setattr(_common, "whoami", lambda base_url, token: {})
    monkeypatch.setenv("QIITA_TOKEN", "qk_test")

    rc = main(["--base-url", "https://qiita.example.com", "whoami"])
    assert rc == 0
