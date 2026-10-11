"""`qiita study list`: the query it sends and how --all follows the cursor.
The route behaviour is covered in tests/routes/test_study.py."""

import json

import httpx
import pytest
from qiita_common.api_paths import URL_STUDY_PREFIX

from qiita_control_plane.cli import _common
from qiita_control_plane.cli.user import main

_BASE = "https://q.example.test"


@pytest.fixture
def pages(monkeypatch):
    """Serve the queued pages in order; record each request's params."""
    queued: list[dict] = []
    calls: list[dict] = []

    def fake_request(method, url, headers=None, json=None, params=None, timeout=None):
        assert (method, url) == ("GET", _BASE + URL_STUDY_PREFIX)
        calls.append(dict(params or {}))
        body = queued.pop(0) if queued else {"studies": [], "next_after_study_idx": None}
        return httpx.Response(200, json=body, request=httpx.Request(method, url))

    monkeypatch.setattr(_common.httpx, "request", fake_request)
    monkeypatch.setenv("QIITA_TOKEN", "qk_test")
    return queued, calls


def _row(idx: int) -> dict:
    return {"study_idx": idx}


def test_defaults_to_own_and_shared_studies(pages):
    _, calls = pages
    assert main(["--base-url", _BASE, "study", "list"]) == 0
    assert calls == [{"limit": 100, "min_tier": "viewer"}]


def test_min_tier_public_widens_to_all_readable(pages):
    _, calls = pages
    assert main(["--base-url", _BASE, "study", "list", "--min-tier", "public"]) == 0
    assert calls == [{"limit": 100, "min_tier": "public"}]


def test_explicit_min_tier_and_query(pages):
    _, calls = pages
    argv = ["--base-url", _BASE, "study", "list", "--min-tier", "member", "--query", "soil"]
    assert main(argv) == 0
    assert calls == [{"limit": 100, "min_tier": "member", "q": "soil"}]


def test_all_follows_the_cursor_and_merges_pages(pages, capsys):
    queued, calls = pages
    queued += [
        {"studies": [_row(9), _row(8)], "next_after_study_idx": 8},
        {"studies": [], "next_after_study_idx": 5},  # a short page still carries on
        {"studies": [_row(3)], "next_after_study_idx": None},
    ]
    assert main(["--base-url", _BASE, "study", "list", "--all", "--limit", "2"]) == 0
    assert [c.get("after_study_idx") for c in calls] == [None, 8, 5]
    out = json.loads(capsys.readouterr().out)
    assert out == {"studies": [_row(9), _row(8), _row(3)], "next_after_study_idx": None}


def test_without_all_returns_one_page_and_its_cursor(pages, capsys):
    queued, calls = pages
    queued.append({"studies": [_row(9)], "next_after_study_idx": 9})
    assert main(["--base-url", _BASE, "study", "list", "--limit", "1"]) == 0
    assert len(calls) == 1
    assert json.loads(capsys.readouterr().out)["next_after_study_idx"] == 9


def test_after_study_idx_zero_is_sent_not_dropped(pages):
    """0 is not a valid cursor; it goes to the server, which refuses it (422),
    rather than being dropped and the first page returned as if asked for."""
    _, calls = pages
    argv = ["--base-url", _BASE, "study", "list", "--after-study-idx", "0"]
    assert main(argv) == 0  # the fake server answers 200; the real one 422s
    assert calls == [{"limit": 100, "min_tier": "viewer", "after_study_idx": 0}]
