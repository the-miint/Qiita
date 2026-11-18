"""`qiita-admin output-equivalence …`: each verb calls its /admin/output-equivalence route
with the body and query the server expects, and a refusal surfaces as exit 1."""

import httpx
import pytest
from qiita_common.api_paths import (
    URL_ADMIN_OUTPUT_EQUIVALENCE,
    URL_ADMIN_OUTPUT_EQUIVALENCE_BY_IDX,
    URL_ADMIN_OUTPUT_EQUIVALENCE_DECISION,
    URL_ADMIN_OUTPUT_EQUIVALENCE_REVOKE,
)

from qiita_control_plane.cli import admin as cli

_EQUIVALENCE = {
    "output_equivalence_idx": 4,
    "definition_kind": "mask",
    "low_definition_idx": 5,
    "high_definition_idx": 12,
    "change": {"/rype": [0.05, 0.1]},
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
    captured = _capture(monkeypatch, _EQUIVALENCE, status=201)
    assert cli.main(["output-equivalence", "open", "mask", "12", "5"]) == 0
    assert captured["method"] == "POST"
    assert captured["url"].endswith(URL_ADMIN_OUTPUT_EQUIVALENCE)
    assert captured["json"] == {"definition_kind": "mask", "definition_idx": [12, 5]}
    assert "output equivalence 4 (mask 5 ~ 12): pending" in capsys.readouterr().err


def test_list_sends_only_the_filters_given(monkeypatch, capsys):
    captured = _capture(
        monkeypatch,
        {
            "output_equivalences": [_EQUIVALENCE],
            "count": 1,
            "next_after_output_equivalence_idx": None,
        },
    )
    assert cli.main(["output-equivalence", "list", "--verdict", "pending", "--kind", "mask"]) == 0
    assert captured["method"] == "GET"
    assert captured["url"].endswith(URL_ADMIN_OUTPUT_EQUIVALENCE)
    assert captured["params"] == {"verdict": "pending", "definition_kind": "mask", "limit": 100}
    assert "1 output equivalence(s)" in capsys.readouterr().err

    captured = _capture(
        monkeypatch,
        {"output_equivalences": [], "count": 0, "next_after_output_equivalence_idx": None},
    )
    assert cli.main(["output-equivalence", "list", "--limit", "5", "--after", "9"]) == 0
    assert captured["params"] == {"limit": 5, "after_output_equivalence_idx": 9}


def test_list_follows_the_cursor_with_all(monkeypatch, capsys):
    """--all follows next_after_output_equivalence_idx to the last page; without it one
    page is printed and the cursor named."""
    from qiita_control_plane.cli import _common

    monkeypatch.setenv("QIITA_TOKEN", "qk_admin")
    pages = {
        None: {
            "output_equivalences": [{**_EQUIVALENCE, "output_equivalence_idx": 9}],
            "count": 1,
            "next_after_output_equivalence_idx": 9,
        },
        9: {
            "output_equivalences": [_EQUIVALENCE],
            "count": 1,
            "next_after_output_equivalence_idx": None,
        },
    }
    calls: list = []

    def fake_request(method, url, headers=None, json=None, params=None, timeout=None):
        calls.append(dict(params))
        body = pages[params.get("after_output_equivalence_idx")]
        return httpx.Response(200, json=body, request=httpx.Request(method, url))

    monkeypatch.setattr(_common.httpx, "request", fake_request)
    assert cli.main(["output-equivalence", "list", "--all"]) == 0
    assert [c.get("after_output_equivalence_idx") for c in calls] == [None, 9]
    assert "2 output equivalence(s)" in capsys.readouterr().err

    calls.clear()
    assert cli.main(["output-equivalence", "list"]) == 0
    assert len(calls) == 1
    assert "1 output equivalence(s) (more: --after 9, or --all)" in capsys.readouterr().err


def test_show_gets_the_output_equivalence(monkeypatch):
    captured = _capture(monkeypatch, {**_EQUIVALENCE, "events": []})
    assert cli.main(["output-equivalence", "show", "4"]) == 0
    assert captured["url"].endswith(
        URL_ADMIN_OUTPUT_EQUIVALENCE_BY_IDX.format(output_equivalence_idx=4)
    )


@pytest.mark.parametrize("verdict", ["same", "similar", "different", "not_benchmarked"])
def test_decide_posts_the_verdict_and_reason(monkeypatch, capsys, verdict):
    captured = _capture(monkeypatch, {**_EQUIVALENCE, "verdict": verdict})
    rc = cli.main(
        ["output-equivalence", "decide", "4", "--verdict", verdict, "--reason", "same refs"]
    )
    assert rc == 0
    assert captured["url"].endswith(
        URL_ADMIN_OUTPUT_EQUIVALENCE_DECISION.format(output_equivalence_idx=4)
    )
    assert captured["json"] == {"verdict": verdict, "reason": "same refs"}
    assert f"): {verdict}" in capsys.readouterr().err


def test_revoke_posts_the_reason(monkeypatch):
    captured = _capture(monkeypatch, _EQUIVALENCE)
    assert cli.main(["output-equivalence", "revoke", "4", "--reason", "decided too early"]) == 0
    assert captured["url"].endswith(
        URL_ADMIN_OUTPUT_EQUIVALENCE_REVOKE.format(output_equivalence_idx=4)
    )
    assert captured["json"] == {"reason": "decided too early"}


def test_a_refusal_exits_1_and_names_the_output_equivalences(monkeypatch, capsys):
    _capture(
        monkeypatch,
        {
            "detail": {
                "detail": "revoking output equivalence 4 would leave ...",
                "reason": "relied_on",
                "output_equivalence_idx": [9],
            }
        },
        status=409,
    )
    assert cli.main(["output-equivalence", "revoke", "4", "--reason", "x"]) == 1
    err = capsys.readouterr().err
    assert "relied_on" in err
    assert "[9]" in err


@pytest.mark.parametrize(
    "argv",
    [
        ["output-equivalence", "decide", "4", "--reason", "x"],
        ["output-equivalence", "decide", "4", "--verdict", "pending", "--reason", "x"],
        ["output-equivalence", "decide", "4", "--verdict", "not_similar", "--reason", "x"],
        ["output-equivalence", "revoke", "4"],
        ["output-equivalence", "open", "mask", "5"],
        ["output-equivalence", "open", "masks", "5", "12"],
    ],
)
def test_the_parser_refuses_an_incomplete_command(argv):
    with pytest.raises(SystemExit):
        cli.main(argv)
