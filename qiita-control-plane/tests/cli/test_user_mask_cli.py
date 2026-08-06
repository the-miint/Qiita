"""Unit tests for the `qiita mask` read-only subcommands.

Each verb is one GET, so what these lock in is the wiring: the path each verb
dials, that optional filters travel as query params only when supplied, and that
the required flags are enforced by argparse rather than by the server.
"""

import pytest
from qiita_common.api_paths import (
    URL_MASK_DEFINITION_BY_IDX,
    URL_MASK_DEFINITION_PREFIX,
    URL_MASK_DEFINITION_PREP_SAMPLE,
)

from .test_user_cli import _stub_post

_BASE = "https://q.example.test"

_LIST_BODY = {"masks": [], "count": 0, "truncated": False}
_ROSTER_BODY = {"mask_idx": 9, "samples": [], "count": 0, "truncated": False}


def _run(monkeypatch, argv, *, response_json, status=200) -> dict:
    """Drive `qiita <argv>` against a stubbed transport; return the captured
    request. Asserts a clean exit so a wiring break surfaces here."""
    from qiita_control_plane.cli.user import main

    captured: dict = {}
    _stub_post(monkeypatch, captured, response_json=response_json, status=status)
    assert main(["--base-url", _BASE, *argv]) == 0
    return captured


def test_mask_list_gets_the_prefix_with_no_params(monkeypatch):
    captured = _run(monkeypatch, ["mask", "list"], response_json=_LIST_BODY)

    assert captured["method"] == "GET"
    assert captured["url"] == f"{_BASE}{URL_MASK_DEFINITION_PREFIX}"
    assert captured["json"] is None
    # Unset filters stay off the wire so an unfiltered list stays unfiltered.
    assert captured["params"] == {}


def test_mask_list_sends_both_filters(monkeypatch):
    captured = _run(
        monkeypatch,
        ["mask", "list", "--sequenced-pool-idx", "25016", "--prep-sample-idx", "7"],
        response_json=_LIST_BODY,
    )

    assert captured["params"] == {"sequenced_pool_idx": "25016", "prep_sample_idx": "7"}


def test_mask_show_gets_the_by_idx_path(monkeypatch):
    captured = _run(monkeypatch, ["mask", "show", "--mask-idx", "9"], response_json={"mask_idx": 9})

    assert captured["method"] == "GET"
    assert captured["url"] == f"{_BASE}{URL_MASK_DEFINITION_BY_IDX.format(mask_idx=9)}"


def test_mask_samples_gets_the_roster_path(monkeypatch):
    captured = _run(monkeypatch, ["mask", "samples", "--mask-idx", "9"], response_json=_ROSTER_BODY)

    assert captured["method"] == "GET"
    assert captured["url"] == f"{_BASE}{URL_MASK_DEFINITION_PREP_SAMPLE.format(mask_idx=9)}"
    assert captured["params"] == {}


def test_mask_samples_sends_the_pool_filter(monkeypatch):
    captured = _run(
        monkeypatch,
        ["mask", "samples", "--mask-idx", "9", "--sequenced-pool-idx", "25016"],
        response_json=_ROSTER_BODY,
    )

    assert captured["params"] == {"sequenced_pool_idx": "25016"}


@pytest.mark.parametrize("argv", [["mask", "show"], ["mask", "samples"]])
def test_mask_idx_is_required(argv, capsys):
    """Omitting --mask-idx exits 2 via argparse, not as a server-side 422."""
    from qiita_control_plane.cli.user import main

    with pytest.raises(SystemExit) as exc_info:
        main(argv)
    assert exc_info.value.code == 2
    assert "--mask-idx" in capsys.readouterr().err


def test_mask_requires_a_subcommand(capsys):
    from qiita_control_plane.cli.user import main

    with pytest.raises(SystemExit) as exc_info:
        main(["mask"])
    assert exc_info.value.code == 2
