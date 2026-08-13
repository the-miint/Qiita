"""Unit tests for the alignment discovery CLI (`qiita alignment list` / `cohort`)
— no DB, no server, no data plane.

These two verbs exist so a user can answer "which alignment, and which samples of
it may I read?" before `feature-table build`, whose `--alignment-idx` is otherwise
unobtainable without a psql shell. Both are one GET printed verbatim, so what is
worth testing is the URL each builds and the one piece of interpretation on top of
them: reading the reference out of an alignment's `params`, which is what keeps
`--reference-idx` off the build's surface.
"""

import argparse

import httpx
import pytest
from qiita_common.api_paths import (
    URL_SEQUENCED_POOL_ALIGNMENT,
    URL_SEQUENCED_POOL_ALIGNMENT_COHORT,
)
from qiita_common.hashing import canonical_params_hash

from qiita_control_plane.cli.user import alignment as al

_PARAMS = {"reference_idx": 9, "aligner": "minimap2", "mask_idx": 2, "shard_ids": [0, 1]}
_ALIGNMENTS = {
    "sequencing_run_idx": 4,
    "sequenced_pool_idx": 5,
    "alignments": [
        {
            "alignment_idx": 3,
            "params": _PARAMS,
            # The real digest, not a placeholder — the client verifies it, so a
            # fixture with a made-up hash would test the refusal instead of the
            # happy path.
            "params_hash": canonical_params_hash(_PARAMS).hex(),
            "samples_completed": 2,
            "samples_total": 2,
        }
    ],
}


def _fake_request(captured, *, status=200, json_body=None, text=""):
    def fake_request(method, url, headers=None, json=None, params=None, timeout=None):
        captured["method"] = method
        captured["url"] = url
        request = httpx.Request(method, url)
        if json_body is not None:
            return httpx.Response(status, json=json_body, request=request)
        return httpx.Response(status, text=text, request=request)

    return fake_request


def test_fetch_pool_alignments_gets_the_pool_scoped_route(monkeypatch):
    captured: dict = {}
    monkeypatch.setattr(al._common.httpx, "request", _fake_request(captured, json_body=_ALIGNMENTS))

    body = al._fetch_pool_alignments(
        "http://cp", "qk_tok", sequencing_run_idx=4, sequenced_pool_idx=5
    )
    assert captured["method"] == "GET"
    assert captured["url"] == "http://cp" + URL_SEQUENCED_POOL_ALIGNMENT.format(
        sequencing_run_idx=4, sequenced_pool_idx=5
    )
    assert body == _ALIGNMENTS


def test_fetch_alignment_cohort_gets_the_route_and_keeps_the_whole_body(monkeypatch):
    """The whole body, not just the sample list: printed on its own the list says
    nothing about which alignment or pool it belongs to, and the build reads one field
    out of it."""
    captured: dict = {}
    body = {
        "sequencing_run_idx": 4,
        "sequenced_pool_idx": 5,
        "alignment_idx": 3,
        "prep_sample_idx": [1, 2],
    }
    monkeypatch.setattr(al._common.httpx, "request", _fake_request(captured, json_body=body))

    assert (
        al._fetch_alignment_cohort(
            "http://cp", "qk_tok", sequencing_run_idx=4, sequenced_pool_idx=5, alignment_idx=3
        )
        == body
    )
    assert captured["url"] == "http://cp" + URL_SEQUENCED_POOL_ALIGNMENT_COHORT.format(
        sequencing_run_idx=4, sequenced_pool_idx=5, alignment_idx=3
    )


def test_the_reference_comes_from_the_alignments_own_params():
    """Why the build has no `--reference-idx`: the alignment records the reference it
    ran against, so a caller cannot name a different one."""
    assert al._alignment_reference_idx(_ALIGNMENTS, alignment_idx=3) == 9


def test_an_alignment_absent_from_the_pool_says_what_that_means():
    """The list omits an alignment the caller can read no sample of entirely, so
    'not found' covers both a typo and a permission boundary — and the message has to
    name the second, since the first is what a user will assume."""
    with pytest.raises(ValueError, match="alignment 99"):
        al._alignment_reference_idx(_ALIGNMENTS, alignment_idx=99)


def test_params_that_do_not_hash_to_the_reported_digest_are_refused():
    """The params round trip through JSON, and everything the build derives — the
    reference, and later the manifest's record of how the table was made — is read
    off them. Recomputing the server's own dedup digest turns "the params arrived
    intact" from an assumption into a checked fact, and it is checked on the path the
    build takes rather than in a helper nobody calls.
    """
    body = {"alignments": [{"alignment_idx": 3, "params": _PARAMS, "params_hash": "00" * 32}]}
    with pytest.raises(ValueError, match="params_hash"):
        al._alignment_reference_idx(body, alignment_idx=3)


def test_an_alignment_reporting_no_params_hash_is_refused():
    """Rather than skipping the check when the field is absent. A server too old to
    report one is a server whose params this client cannot vouch for, and quietly
    proceeding is how an unverifiable manifest gets published."""
    body = {"alignments": [{"alignment_idx": 3, "params": _PARAMS}]}
    with pytest.raises(ValueError, match="params_hash"):
        al._alignment_reference_idx(body, alignment_idx=3)


def test_params_without_a_reference_is_refused_rather_than_defaulted():
    """A `params` blob this old or this different is not something to guess around: the
    reference decides which genome map the whole table is relabelled through."""
    # A genuine digest for these params, so the integrity check passes and this test
    # reaches the refusal it is about.
    params = {"aligner": "minimap2"}
    body = {
        "alignments": [
            {
                "alignment_idx": 3,
                "params": params,
                "params_hash": canonical_params_hash(params).hex(),
            }
        ]
    }
    with pytest.raises(ValueError, match="reference_idx"):
        al._alignment_reference_idx(body, alignment_idx=3)


def test_the_list_handler_prints_the_route_body(monkeypatch, capsys):
    monkeypatch.setattr(al._common, "read_token", lambda: "qk_tok")
    monkeypatch.setattr(al._common.httpx, "request", _fake_request({}, json_body=_ALIGNMENTS))

    args = argparse.Namespace(base_url="http://cp", sequencing_run_idx=4, sequenced_pool_idx=5)
    assert al._handle_alignment_list(args, parser=None) == 0
    assert '"alignment_idx": 3' in capsys.readouterr().out


def test_the_cohort_handler_prints_the_route_body(monkeypatch, capsys):
    monkeypatch.setattr(al._common, "read_token", lambda: "qk_tok")
    body = {"alignment_idx": 3, "prep_sample_idx": [1, 2]}
    monkeypatch.setattr(al._common.httpx, "request", _fake_request({}, json_body=body))

    args = argparse.Namespace(
        base_url="http://cp", sequencing_run_idx=4, sequenced_pool_idx=5, alignment_idx=3
    )
    assert al._handle_alignment_cohort(args, parser=None) == 0
    assert '"prep_sample_idx"' in capsys.readouterr().out


def test_parser_wires_both_alignment_verbs():
    from qiita_control_plane.cli.user._parser import _build_parser

    parser = _build_parser()
    listed = parser.parse_args(
        ["alignment", "list", "--sequencing-run-idx", "4", "--sequenced-pool-idx", "5"]
    )
    assert listed.handler is al._handle_alignment_list
    assert (listed.sequencing_run_idx, listed.sequenced_pool_idx) == (4, 5)

    cohort = parser.parse_args(
        [
            "alignment",
            "cohort",
            "--sequencing-run-idx",
            "4",
            "--sequenced-pool-idx",
            "5",
            "--alignment-idx",
            "3",
        ]
    )
    assert cohort.handler is al._handle_alignment_cohort
    assert cohort.alignment_idx == 3


@pytest.mark.parametrize(
    "argv",
    [
        ["alignment", "list", "--sequencing-run-idx", "4"],
        ["alignment", "cohort", "--sequencing-run-idx", "4", "--sequenced-pool-idx", "5"],
    ],
)
def test_parser_rejects_a_missing_required_flag(argv):
    from qiita_control_plane.cli.user._parser import _build_parser

    with pytest.raises(SystemExit):
        _build_parser().parse_args(argv)
