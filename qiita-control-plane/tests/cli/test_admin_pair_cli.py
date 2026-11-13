"""`qiita-admin pair …`: each verb calls its /admin/definition-pair route with the
body and query the server expects, and a refusal surfaces as exit 1."""

import httpx
import pytest
from qiita_common.api_paths import (
    URL_ADMIN_DEFINITION_PAIR,
    URL_ADMIN_DEFINITION_PAIR_BY_IDX,
    URL_ADMIN_DEFINITION_PAIR_DECISION,
    URL_ADMIN_DEFINITION_PAIR_REVOKE,
)

from qiita_control_plane.cli import admin as cli

_PAIR = {
    "definition_pair_idx": 4,
    "definition_kind": "mask",
    "low_definition_idx": 5,
    "high_definition_idx": 12,
    "change": {"rype": [0.05, 0.1]},
    "derived_only": False,
    "verdict": "pending",
}


def _capture(monkeypatch, response_body, status=200):
    from qiita_control_plane.cli import _common

    monkeypatch.setenv("QIITA_TOKEN", "qk_admin")
    captured: dict = {}

    def fake_request(method, url, headers=None, json=None, params=None, timeout=None):
        captured.update(method=method, url=url, json=json, params=params)
        return httpx.Response(status, json=response_body, request=httpx.Request(method, url))

    monkeypatch.setattr(_common.httpx, "request", fake_request)
    return captured


def test_open_posts_the_kind_and_both_ids(monkeypatch, capsys):
    captured = _capture(monkeypatch, _PAIR, status=201)
    assert cli.main(["pair", "open", "mask", "12", "5"]) == 0
    assert captured["method"] == "POST"
    assert captured["url"].endswith(URL_ADMIN_DEFINITION_PAIR)
    assert captured["json"] == {"definition_kind": "mask", "definition_idx": [12, 5]}
    assert "pair 4 (mask 5 ~ 12): pending" in capsys.readouterr().err


def test_list_sends_only_the_filters_given(monkeypatch, capsys):
    captured = _capture(monkeypatch, {"pairs": [_PAIR], "count": 1, "truncated": False})
    assert cli.main(["pair", "list", "--verdict", "pending", "--kind", "mask"]) == 0
    assert captured["method"] == "GET"
    assert captured["url"].endswith(URL_ADMIN_DEFINITION_PAIR)
    assert captured["params"] == {"verdict": "pending", "definition_kind": "mask"}
    assert "1 pair(s)" in capsys.readouterr().err

    captured = _capture(monkeypatch, {"pairs": [], "count": 0, "truncated": False})
    assert cli.main(["pair", "list"]) == 0
    assert captured["params"] is None


def test_show_gets_the_pair(monkeypatch):
    captured = _capture(monkeypatch, {**_PAIR, "events": []})
    assert cli.main(["pair", "show", "4"]) == 0
    assert captured["url"].endswith(URL_ADMIN_DEFINITION_PAIR_BY_IDX.format(definition_pair_idx=4))


def test_decide_posts_the_verdict_and_reason(monkeypatch, capsys):
    captured = _capture(monkeypatch, {**_PAIR, "verdict": "similar"})
    rc = cli.main(["pair", "decide", "4", "--verdict", "similar", "--reason", "same refs"])
    assert rc == 0
    assert captured["url"].endswith(
        URL_ADMIN_DEFINITION_PAIR_DECISION.format(definition_pair_idx=4)
    )
    assert captured["json"] == {"verdict": "similar", "reason": "same refs"}
    assert "similar" in capsys.readouterr().err


def test_revoke_posts_the_reason(monkeypatch):
    captured = _capture(monkeypatch, _PAIR)
    assert cli.main(["pair", "revoke", "4", "--reason", "decided too early"]) == 0
    assert captured["url"].endswith(URL_ADMIN_DEFINITION_PAIR_REVOKE.format(definition_pair_idx=4))
    assert captured["json"] == {"reason": "decided too early"}


def test_a_refusal_exits_1_and_names_the_pairs(monkeypatch, capsys):
    _capture(
        monkeypatch,
        {
            "detail": {
                "detail": "revoking pair 4 would leave ...",
                "reason": "relied_on",
                "definition_pair_idx": [9],
            }
        },
        status=409,
    )
    assert cli.main(["pair", "revoke", "4", "--reason", "x"]) == 1
    err = capsys.readouterr().err
    assert "relied_on" in err
    assert "[9]" in err


@pytest.mark.parametrize(
    "argv",
    [
        ["pair", "decide", "4", "--reason", "x"],
        ["pair", "decide", "4", "--verdict", "pending", "--reason", "x"],
        ["pair", "revoke", "4"],
        ["pair", "open", "mask", "5"],
        ["pair", "open", "masks", "5", "12"],
    ],
)
def test_the_parser_refuses_an_incomplete_command(argv):
    with pytest.raises(SystemExit):
        cli.main(argv)
