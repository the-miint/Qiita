"""Unit tests for the `qiita processing` read-only subcommands.

Each verb is one GET, so what these lock in is the wiring: the path each verb
dials, that optional filters travel as query params only when supplied, and that
the required flags are enforced by argparse rather than by the server.

The mask twin is `test_user_mask_cli.py`; these cover the second identity an
`align-denovo` submission names.
"""

import pytest
from qiita_common.api_paths import (
    URL_PROCESSING_BY_IDX,
    URL_PROCESSING_PREFIX,
    URL_PROCESSING_PREP_SAMPLE,
)
from qiita_common.models import ProcessingListResponse, ProcessingPrepSampleListResponse

from .test_user_cli import _BASE, _run

_LIST_BODY = {"processing": [], "count": 0, "truncated": False}
_ROSTER_BODY = {"processing_idx": 42, "samples": [], "count": 0, "truncated": False}


def test_stub_bodies_are_valid_server_responses():
    """The stubs above stand in for the two list routes, and nothing else in
    this file would notice if they stopped resembling them. Validating each
    against its wire model makes a server-side rename fail here rather than
    leave the rest of the file asserting against a shape the server never
    sends."""
    ProcessingListResponse.model_validate(_LIST_BODY)
    ProcessingPrepSampleListResponse.model_validate(_ROSTER_BODY)


def test_processing_list_gets_the_prefix_with_no_params(monkeypatch):
    captured = _run(monkeypatch, ["processing", "list"], response_json=_LIST_BODY)

    assert captured["method"] == "GET"
    assert captured["url"] == f"{_BASE}{URL_PROCESSING_PREFIX}"
    assert captured["json"] is None
    # Unset filters stay off the wire so an unfiltered list stays unfiltered —
    # `--status` included, whose omission is what lists deprecated runs too.
    assert captured["params"] == {}


def test_processing_list_sends_every_filter(monkeypatch):
    captured = _run(
        monkeypatch,
        [
            "processing",
            "list",
            "--sequenced-pool-idx",
            "25016",
            "--prep-sample-idx",
            "7",
            "--status",
            "active",
        ],
        response_json=_LIST_BODY,
    )

    assert captured["params"] == {
        "sequenced_pool_idx": "25016",
        "prep_sample_idx": "7",
        "status": "active",
    }


def test_processing_list_rejects_an_unknown_status(capsys):
    """--status is closed over ProcessingStatus, so a typo exits 2 here rather
    than reaching the server as an unfiltered list."""
    from qiita_control_plane.cli.user import main

    with pytest.raises(SystemExit) as exc_info:
        main(["processing", "list", "--status", "retired"])
    assert exc_info.value.code == 2
    assert "--status" in capsys.readouterr().err


def test_processing_show_gets_the_by_idx_path(monkeypatch):
    captured = _run(
        monkeypatch,
        ["processing", "show", "--processing-idx", "42"],
        response_json={"processing_idx": 42},
    )

    assert captured["method"] == "GET"
    assert captured["url"] == f"{_BASE}{URL_PROCESSING_BY_IDX.format(processing_idx=42)}"


def test_processing_samples_gets_the_roster_path(monkeypatch):
    captured = _run(
        monkeypatch,
        ["processing", "samples", "--processing-idx", "42"],
        response_json=_ROSTER_BODY,
    )

    assert captured["method"] == "GET"
    assert captured["url"] == f"{_BASE}{URL_PROCESSING_PREP_SAMPLE.format(processing_idx=42)}"
    assert captured["params"] == {}


def test_processing_samples_sends_the_pool_filter(monkeypatch):
    captured = _run(
        monkeypatch,
        ["processing", "samples", "--processing-idx", "42", "--sequenced-pool-idx", "25016"],
        response_json=_ROSTER_BODY,
    )

    assert captured["params"] == {"sequenced_pool_idx": "25016"}


@pytest.mark.parametrize("argv", [["processing", "show"], ["processing", "samples"]])
def test_processing_idx_is_required(argv, capsys):
    """Omitting --processing-idx exits 2 via argparse, not as a server-side 422."""
    from qiita_control_plane.cli.user import main

    with pytest.raises(SystemExit) as exc_info:
        main(argv)
    assert exc_info.value.code == 2
    assert "--processing-idx" in capsys.readouterr().err


def test_processing_requires_a_subcommand(capsys):
    from qiita_control_plane.cli.user import main

    with pytest.raises(SystemExit) as exc_info:
        main(["processing"])
    assert exc_info.value.code == 2
