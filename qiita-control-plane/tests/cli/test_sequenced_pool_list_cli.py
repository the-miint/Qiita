"""Unit tests for `qiita sequenced-pool list`.

One GET, so what these lock in is the wiring: the path it dials, that the run idx
is enforced by argparse rather than by the server, and that the stub body it is
driven with is a shape the server actually sends.
"""

import pytest
from qiita_common.api_paths import URL_SEQUENCING_RUN_SEQUENCED_POOL
from qiita_common.models import SequencedPoolListResponse

from .test_user_cli import _BASE, _run

# One populated row, not an empty list: validating an empty `sequenced_pool`
# constructs no SequencedPoolSummary, so it would pin the envelope and leave the
# row fields — the idx this verb exists to produce, and the filename that tells
# two pools apart — unchecked.
_LIST_BODY = {
    "sequencing_run_idx": 3,
    "sequenced_pool": [
        {
            "sequenced_pool_idx": 25016,
            "sequencing_run_idx": 3,
            "run_preflight_filename": "lane-1.db",
            "extra_metadata": None,
            "created_by_idx": 7,
            "created_at": "2026-09-03T00:00:00Z",
        }
    ],
    "count": 1,
    "truncated": False,
}


def test_stub_body_is_a_valid_server_response():
    SequencedPoolListResponse.model_validate(_LIST_BODY)


def test_sequenced_pool_list_gets_the_run_scoped_path(monkeypatch):
    captured = _run(
        monkeypatch,
        ["sequenced-pool", "list", "--sequencing-run-idx", "3"],
        response_json=_LIST_BODY,
    )

    assert captured["method"] == "GET"
    assert (
        captured["url"]
        == f"{_BASE}{URL_SEQUENCING_RUN_SEQUENCED_POOL.format(sequencing_run_idx=3)}"
    )
    assert captured["json"] is None


def test_sequencing_run_idx_is_required(capsys):
    """Omitting --sequencing-run-idx exits 2 via argparse, not as a server-side 422."""
    from qiita_control_plane.cli.user import main

    with pytest.raises(SystemExit) as exc_info:
        main(["sequenced-pool", "list"])
    assert exc_info.value.code == 2
    assert "--sequencing-run-idx" in capsys.readouterr().err
