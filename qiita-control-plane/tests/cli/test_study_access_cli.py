"""`qiita study access list|grant|set-tier|revoke`: each subcommand's method,
URL, and body. The route behaviour is covered in tests/routes/test_study_access.py."""

import httpx
import pytest
from qiita_common.api_paths import URL_STUDY_ACCESS, URL_STUDY_ACCESS_BY_PRINCIPAL

from qiita_control_plane.cli import _common
from qiita_control_plane.cli.user import main

_BASE = "https://q.example.test"


@pytest.fixture
def captured(monkeypatch):
    calls: list[dict] = []

    def fake_request(method, url, headers=None, json=None, params=None, timeout=None):
        calls.append({"method": method, "url": url, "json": json})
        body = [] if method == "GET" else {"access_tier": "viewer"}
        return httpx.Response(200, json=body, request=httpx.Request(method, url))

    monkeypatch.setattr(_common.httpx, "request", fake_request)
    monkeypatch.setenv("QIITA_TOKEN", "qk_test")
    return calls


def test_list(captured):
    assert main(["--base-url", _BASE, "study", "access", "list", "--study-idx", "7"]) == 0
    assert captured == [
        {"method": "GET", "url": _BASE + URL_STUDY_ACCESS.format(study_idx=7), "json": None}
    ]


def test_grant_sends_email_and_tier(captured):
    argv = ["--base-url", _BASE, "study", "access", "grant", "--study-idx", "7"]
    argv += ["--email", "someone@example.org", "--tier", "member"]
    assert main(argv) == 0
    assert captured == [
        {
            "method": "POST",
            "url": _BASE + URL_STUDY_ACCESS.format(study_idx=7),
            "json": {"email": "someone@example.org", "access_tier": "member"},
        }
    ]


def test_set_tier(captured):
    argv = ["--base-url", _BASE, "study", "access", "set-tier", "--study-idx", "7"]
    argv += ["--principal-idx", "12", "--tier", "viewer"]
    assert main(argv) == 0
    assert captured == [
        {
            "method": "PATCH",
            "url": _BASE + URL_STUDY_ACCESS_BY_PRINCIPAL.format(study_idx=7, principal_idx=12),
            "json": {"access_tier": "viewer"},
        }
    ]


def test_revoke(captured):
    argv = ["--base-url", _BASE, "study", "access", "revoke", "--study-idx", "7"]
    argv += ["--principal-idx", "12"]
    assert main(argv) == 0
    assert captured == [
        {
            "method": "DELETE",
            "url": _BASE + URL_STUDY_ACCESS_BY_PRINCIPAL.format(study_idx=7, principal_idx=12),
            "json": None,
        }
    ]


@pytest.mark.parametrize("tier", ["public", "owner"])
def test_grant_rejects_tiers_that_cannot_be_stored(captured, capsys, tier):
    argv = ["study", "access", "grant", "--study-idx", "7", "--email", "a@b.org", "--tier", tier]
    with pytest.raises(SystemExit) as exc:
        main(argv)
    assert exc.value.code == 2
    assert captured == []


def test_grant_rejects_a_malformed_email_before_any_request(captured, capsys):
    argv = ["study", "access", "grant", "--study-idx", "7", "--email", "not-an-email"]
    argv += ["--tier", "viewer"]
    with pytest.raises(SystemExit) as exc:
        main(argv)
    assert exc.value.code == 2
    assert captured == []
