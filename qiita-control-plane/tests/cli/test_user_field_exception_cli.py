"""Unit tests for the `qiita <entity> {get,set,clear}-field-exception` verbs.

The routes need If-Match to replace or clear an exception, and refuse it on a
create, so `set` reads the exception first and sends its ETag only when one
exists, and `clear` always reads first. Both entities are driven off one
surface: they differ only in the subcommand and the flag naming the sample.
"""

from typing import NamedTuple

import httpx
import pytest
from qiita_common.api_paths import (
    URL_BIOSAMPLE_FIELD_EXCEPTION,
    URL_SEQUENCED_SAMPLE_FIELD_EXCEPTION,
)

from qiita_control_plane.cli import _common
from qiita_control_plane.cli.user import main

_BASE = "https://q.example.test"
_EXCEPTION_BODY = {
    "tier_override": "admin",
    "reason": "accidental PII",
    "keyed_on": "global",
    "created_by_idx": 3,
    "created_at": "2026-11-12T00:00:00+00:00",
    "updated_at": "2026-11-12T00:00:00+00:00",
}


class _Surface(NamedTuple):
    subcommand: str
    sample_flag: str
    path_key: str
    url_template: str


_SURFACES = (
    _Surface("biosample", "--biosample-idx", "biosample_idx", URL_BIOSAMPLE_FIELD_EXCEPTION),
    _Surface(
        "sequenced-sample",
        "--sequenced-sample-idx",
        "sequenced_sample_idx",
        URL_SEQUENCED_SAMPLE_FIELD_EXCEPTION,
    ),
)
_IDS = [s.subcommand for s in _SURFACES]


def _argv(surface: _Surface, verb: str, *extra: str) -> list[str]:
    return [
        "--base-url",
        _BASE,
        surface.subcommand,
        verb,
        "--study-idx",
        "5",
        surface.sample_flag,
        "7",
        "--study-field-idx",
        "9",
        *extra,
    ]


def _url(surface: _Surface) -> str:
    return _BASE + surface.url_template.format(
        study_idx=5, **{surface.path_key: 7}, study_field_idx=9
    )


def _serve(monkeypatch, *, existing: bool) -> list[dict]:
    """Stub the transport: a GET finds an exception (with an ETag) when
    `existing`, else 404; a PUT echoes an exception; a DELETE answers 204."""
    requests: list[dict] = []

    def fake_request(method, url, headers=None, json=None, params=None, timeout=None):
        requests.append({"method": method, "url": url, "headers": headers or {}, "json": json})
        req = httpx.Request(method, url)
        if method == "GET":
            if not existing:
                return httpx.Response(404, json={"detail": "no exception"}, request=req)
            return httpx.Response(
                200, json=_EXCEPTION_BODY, headers={"ETag": '"etag-v1"'}, request=req
            )
        if method == "PUT":
            return httpx.Response(
                200 if existing else 201,
                json=_EXCEPTION_BODY,
                headers={"ETag": '"etag-v2"'},
                request=req,
            )
        return httpx.Response(204, request=req)

    monkeypatch.setattr(_common.httpx, "request", fake_request)
    monkeypatch.setenv("QIITA_TOKEN", "qk_test")
    return requests


@pytest.mark.parametrize("surface", _SURFACES, ids=_IDS)
def test_get_field_exception_fills_every_placeholder(monkeypatch, surface):
    requests = _serve(monkeypatch, existing=True)

    assert main(_argv(surface, "get-field-exception")) == 0

    assert [(r["method"], r["url"], r["json"]) for r in requests] == [("GET", _url(surface), None)]


@pytest.mark.parametrize("surface", _SURFACES, ids=_IDS)
def test_set_field_exception_creates_without_if_match_when_none_exists(monkeypatch, surface):
    requests = _serve(monkeypatch, existing=False)

    rc = main(_argv(surface, "set-field-exception", "--tier-override", "admin", "--reason", "PII"))

    assert rc == 0
    assert [r["method"] for r in requests] == ["GET", "PUT"]
    put = requests[1]
    assert put["url"] == _url(surface)
    assert put["json"] == {"tier_override": "admin", "reason": "PII"}
    assert "If-Match" not in put["headers"]


@pytest.mark.parametrize("surface", _SURFACES, ids=_IDS)
def test_set_field_exception_replaces_under_the_read_etag(monkeypatch, surface):
    requests = _serve(monkeypatch, existing=True)

    rc = main(_argv(surface, "set-field-exception", "--tier-override", "member", "--reason", "PII"))

    assert rc == 0
    assert [r["method"] for r in requests] == ["GET", "PUT"]
    assert requests[1]["headers"]["If-Match"] == '"etag-v1"'
    assert requests[1]["json"] == {"tier_override": "member", "reason": "PII"}


@pytest.mark.parametrize("surface", _SURFACES, ids=_IDS)
def test_clear_field_exception_deletes_under_the_read_etag(monkeypatch, surface):
    requests = _serve(monkeypatch, existing=True)

    assert main(_argv(surface, "clear-field-exception")) == 0

    assert [(r["method"], r["url"]) for r in requests] == [
        ("GET", _url(surface)),
        ("DELETE", _url(surface)),
    ]
    assert requests[1]["headers"]["If-Match"] == '"etag-v1"'


@pytest.mark.parametrize("surface", _SURFACES, ids=_IDS)
def test_clear_field_exception_with_none_set_exits_1(monkeypatch, surface):
    requests = _serve(monkeypatch, existing=False)

    assert main(_argv(surface, "clear-field-exception")) == 1

    assert [r["method"] for r in requests] == ["GET"]


@pytest.mark.parametrize("surface", _SURFACES, ids=_IDS)
@pytest.mark.parametrize(
    "extra",
    (
        ("--tier-override", "admin"),
        ("--reason", "PII"),
        ("--tier-override", "superuser", "--reason", "PII"),
        ("--tier-override", "admin", "--reason", "   "),
    ),
    ids=("no-reason", "no-tier", "unknown-tier", "blank-reason"),
)
def test_set_field_exception_refuses_a_bad_body_before_any_request(monkeypatch, surface, extra):
    requests = _serve(monkeypatch, existing=False)

    with pytest.raises(SystemExit) as exc_info:
        main(_argv(surface, "set-field-exception", *extra))

    assert exc_info.value.code == 2
    assert requests == []
