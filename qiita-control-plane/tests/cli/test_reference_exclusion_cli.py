"""Unit tests for the `qiita-admin reference exclusion` CLI (no DB, no server).

Patches httpx.request (the verb-agnostic entry point `_common.call` delegates to)
to assert each subcommand hits the right method/URL/body, and that the parser
wires the three subcommands to their handlers with mutual exclusivity."""

import httpx
from qiita_common.api_paths import URL_REFERENCE_EXCLUSION, URL_REFERENCE_EXCLUSION_BY_IDX


def _fake_request_capturing(captured, response_json):
    def fake_request(method, url, headers=None, json=None, params=None, timeout=None):
        captured["method"] = method
        captured["url"] = url
        captured["json"] = json
        captured["params"] = params
        return httpx.Response(200, json=response_json, request=httpx.Request(method, url))

    return fake_request


def test_add_exclusion_posts_body(monkeypatch):
    from qiita_control_plane.cli import _common
    from qiita_control_plane.cli.admin import reference_exclusion as rex

    captured: dict = {}
    monkeypatch.setattr(
        _common.httpx,
        "request",
        _fake_request_capturing(captured, {"target_kind": "feature", "changed": True}),
    )

    body = rex._add_exclusion_via_route(
        "http://cp", "qk_admin", genome_idx=None, feature_idx=5, reason="bad genome"
    )
    assert captured["method"] == "POST"
    assert captured["url"] == f"http://cp{URL_REFERENCE_EXCLUSION}"
    assert captured["json"] == {"reason": "bad genome", "feature_idx": 5}
    assert body == {"target_kind": "feature", "changed": True}


def test_add_exclusion_by_genome_posts_genome_idx(monkeypatch):
    from qiita_control_plane.cli import _common
    from qiita_control_plane.cli.admin import reference_exclusion as rex

    captured: dict = {}
    monkeypatch.setattr(_common.httpx, "request", _fake_request_capturing(captured, {}))
    rex._add_exclusion_via_route(
        "http://cp", "qk_admin", genome_idx=9, feature_idx=None, reason="contaminant"
    )
    assert captured["json"] == {"reason": "contaminant", "genome_idx": 9}


def test_remove_exclusion_deletes_with_params(monkeypatch):
    from qiita_control_plane.cli import _common
    from qiita_control_plane.cli.admin import reference_exclusion as rex

    captured: dict = {}
    monkeypatch.setattr(
        _common.httpx, "request", _fake_request_capturing(captured, {"changed": True})
    )
    rex._remove_exclusion_via_route("http://cp", "qk_admin", genome_idx=7, feature_idx=None)
    assert captured["method"] == "DELETE"
    assert captured["url"] == f"http://cp{URL_REFERENCE_EXCLUSION}"
    assert captured["params"] == {"genome_idx": 7}


def test_list_exclusions_gets_scoped_url(monkeypatch):
    from qiita_control_plane.cli import _common
    from qiita_control_plane.cli.admin import reference_exclusion as rex

    captured: dict = {}
    monkeypatch.setattr(_common.httpx, "request", _fake_request_capturing(captured, []))
    body = rex._list_exclusions_via_route("http://cp", "qk_admin", 3)
    assert captured["method"] == "GET"
    assert captured["url"] == f"http://cp{URL_REFERENCE_EXCLUSION_BY_IDX.format(reference_idx=3)}"
    assert body == []


def test_parser_wires_exclusion_subcommands():
    from qiita_control_plane.cli.admin import _build_parser
    from qiita_control_plane.cli.admin.reference_exclusion import (
        _handle_exclusion_add,
        _handle_exclusion_list,
        _handle_exclusion_remove,
    )

    parser = _build_parser()

    add = parser.parse_args(
        ["reference", "exclusion", "add", "--feature-idx", "5", "--reason", "r"]
    )
    assert add.handler is _handle_exclusion_add
    assert add.feature_idx == 5 and add.genome_idx is None and add.reason == "r"

    remove = parser.parse_args(["reference", "exclusion", "remove", "--genome-idx", "7"])
    assert remove.handler is _handle_exclusion_remove
    assert remove.genome_idx == 7

    lst = parser.parse_args(["reference", "exclusion", "list", "--reference-idx", "3"])
    assert lst.handler is _handle_exclusion_list
    assert lst.reference_idx == 3


def test_parser_add_requires_exactly_one_target():
    import pytest

    from qiita_control_plane.cli.admin import _build_parser

    parser = _build_parser()
    # Both targets → argparse rejects (mutually-exclusive group).
    with pytest.raises(SystemExit):
        parser.parse_args(
            [
                "reference",
                "exclusion",
                "add",
                "--genome-idx",
                "1",
                "--feature-idx",
                "2",
                "--reason",
                "r",
            ]
        )
    # Neither target → argparse rejects (group is required).
    with pytest.raises(SystemExit):
        parser.parse_args(["reference", "exclusion", "add", "--reason", "r"])
