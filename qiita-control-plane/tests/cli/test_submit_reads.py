"""CLI-side tests for `qiita submit-reads`.

Drives the programmatic entry point `cli.user.reads.do_submit_reads` with a
mocked httpx transport and a fake Flight client, reusing the shared fakes in
`tests/cli/conftest.py` — the two gestures share `upload_file`, so they share
the harness that stands in for the data plane.

Exercised here: which action a file's name routes to, what lands in
`action_context`, that the uploaded bytes are the file's own (a `.gz` is not
inflated on the way out), and the local pre-flight refusals.
"""

from __future__ import annotations

import gzip

import httpx
import pytest
from qiita_common.api_paths import URL_UPLOAD_PREFIX, URL_WORK_TICKET_PREFIX

from .conftest import FakeFlightClient


def _client(transport) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=transport, base_url="http://cp.test")


async def _run(transport, flight_client, **overrides):
    from qiita_control_plane.cli.user.reads import do_submit_reads

    kwargs = dict(
        token="test-token",
        prep_sample_idx=7,
        reverse=None,
        watch=False,
        poll_interval_seconds=0.0,
        timeout_seconds=5.0,
    )
    kwargs.update(overrides)
    async with _client(transport) as http:
        return await do_submit_reads(http=http, flight_client=flight_client, **kwargs)


def _ticket_body(calls) -> dict:
    return next(b for m, p, b in calls if m == "POST" and p == URL_WORK_TICKET_PREFIX)


def _upload_bodies(calls) -> list[dict]:
    return [b for m, p, b in calls if m == "POST" and p == URL_UPLOAD_PREFIX]


@pytest.fixture
def fastq_r1(tmp_path):
    path = tmp_path / "ABC123_R1.fastq"
    path.write_text("@r1\nACGT\n+\nIIII\n")
    return path


@pytest.fixture
def fastq_r2(tmp_path):
    path = tmp_path / "ABC123_R2.fastq"
    path.write_text("@r1\nTTTT\n+\nIIII\n")
    return path


# ---------------------------------------------------------------------------
# Routing: the filename picks the loader
# ---------------------------------------------------------------------------


async def test_single_end_fastq_submits_fastq_to_parquet(cp_transport, fastq_r1):
    transport, calls = cp_transport
    flight = FakeFlightClient()
    flight.queue_response(100)

    result = await _run(transport, flight, forward=fastq_r1)

    assert result["action_id"] == "fastq-to-parquet"
    body = _ticket_body(calls)
    assert body["action_context"] == {"fastq_upload_idx": 100}
    assert body["scope_target"] == {"kind": "prep_sample", "prep_sample_idx": 7}


async def test_paired_end_fastq_submits_both_handles(cp_transport, fastq_r1, fastq_r2):
    transport, calls = cp_transport
    flight = FakeFlightClient()
    flight.queue_response(100)
    flight.queue_response(101)

    result = await _run(transport, flight, forward=fastq_r1, reverse=fastq_r2)

    assert _ticket_body(calls)["action_context"] == {
        "fastq_upload_idx": 100,
        "reverse_fastq_upload_idx": 101,
    }
    assert set(result["uploads"]) == {"ABC123_R1.fastq", "ABC123_R2.fastq"}


async def test_bam_submits_bam_to_parquet_declaring_unaligned(cp_transport, tmp_path):
    """A BAM routes to the other loader, and the gesture declares the
    unaligned-uBAM expectation the loader requires — the same declaration the
    PacBio fan-out makes."""
    transport, calls = cp_transport
    bam = tmp_path / "ABC123.bam"
    bam.write_bytes(b"BAM\x01placeholder")
    flight = FakeFlightClient()
    flight.queue_response(100)

    result = await _run(transport, flight, forward=bam)

    assert result["action_id"] == "bam-to-parquet"
    assert _ticket_body(calls)["action_context"] == {
        "bam_upload_idx": 100,
        "expect_unaligned": True,
    }


@pytest.mark.parametrize("name", ["s_R1.fq", "s_R1.fastq.gz", "s_R1.FASTQ", "s_R1.fq.gz"])
async def test_fastq_suffix_variants_all_route_to_fastq(cp_transport, tmp_path, name):
    """`.gz` is stripped before the suffix is read, and the match is
    case-insensitive, so the spellings people actually use all land."""
    transport, calls = cp_transport
    path = tmp_path / name
    path.write_bytes(b"@r1\nACGT\n+\nIIII\n")
    flight = FakeFlightClient()
    flight.queue_response(100)

    result = await _run(transport, flight, forward=path)
    assert result["action_id"] == "fastq-to-parquet"


# ---------------------------------------------------------------------------
# What actually goes on the wire
# ---------------------------------------------------------------------------


async def test_upload_records_the_source_filename(cp_transport, fastq_r1):
    """The submit gate applies the filename-prefix rule to this value — an
    upload-fed submission has no path to take a basename from."""
    transport, calls = cp_transport
    flight = FakeFlightClient()
    flight.queue_response(100)

    await _run(transport, flight, forward=fastq_r1)

    assert _upload_bodies(calls)[0]["source_filename"] == "ABC123_R1.fastq"


async def test_gzip_input_is_streamed_without_inflating(cp_transport, tmp_path):
    """The reads streamer is byte-exact: a `.gz` stays compressed on the wire.

    This is what separates it from the companion-file streamer, which inflates
    because miint's `read_newick` / `read_jplace` only take plaintext. The
    server names the stitched file from these bytes' gzip magic, so the
    compression has to survive the trip.
    """
    transport, _calls = cp_transport
    path = tmp_path / "ABC123_R1.fastq.gz"
    raw = b"@r1\nACGT\n+\nIIII\n"
    path.write_bytes(gzip.compress(raw))
    flight = FakeFlightClient()
    flight.queue_response(100)

    captured: list[bytes] = []
    original_do_put = flight.do_put

    def _capture(descriptor, schema):
        writer, reader = original_do_put(descriptor, schema)
        real_write = writer.write_batch

        def _write(batch):
            captured.append(b"".join(bytes(v.as_py()) for v in batch.column("chunk_data")))
            return real_write(batch)

        writer.write_batch = _write
        return writer, reader

    flight.do_put = _capture

    await _run(transport, flight, forward=path)

    sent = b"".join(captured)
    assert sent[:2] == b"\x1f\x8b", "gzip magic missing — the CLI inflated the reads"
    assert sent == path.read_bytes()
    assert gzip.decompress(sent) == raw


# ---------------------------------------------------------------------------
# Pre-flight refusals — before any byte is sent
# ---------------------------------------------------------------------------


async def test_unrecognized_suffix_is_refused(cp_transport, tmp_path):
    transport, calls = cp_transport
    path = tmp_path / "reads.txt"
    path.write_text("whatever")

    with pytest.raises(ValueError, match="cannot tell the format"):
        await _run(transport, FakeFlightClient(), forward=path)
    assert not calls, "refused before any call to the control plane"


async def test_reverse_with_a_bam_is_refused(cp_transport, tmp_path, fastq_r2):
    transport, calls = cp_transport
    bam = tmp_path / "ABC123.bam"
    bam.write_bytes(b"BAM\x01")

    with pytest.raises(ValueError, match="only meaningful for FASTQ"):
        await _run(transport, FakeFlightClient(), forward=bam, reverse=fastq_r2)
    assert not calls


async def test_non_202_from_submit_surfaces_the_body(tmp_path, fastq_r1):
    """The 403 a `user` gets for naming a host path, and the 422 carrying the
    filename-prefix detail, both matter to whoever ran the command — so the
    response text is surfaced rather than translated."""

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == URL_UPLOAD_PREFIX:
            return httpx.Response(201, json={"upload_idx": 100, "doput_ticket": "dGlja2V0"})
        if request.url.path.endswith("/done"):
            return httpx.Response(200, json={"upload_idx": 100, "status": "ready"})
        return httpx.Response(
            422, json={"detail": {"reason": "fastq filename must be the prep_sample's ..."}}
        )

    flight = FakeFlightClient()
    flight.queue_response(100)
    with pytest.raises(RuntimeError, match="got 422"):
        await _run(httpx.MockTransport(handler), flight, forward=fastq_r1)


# ---------------------------------------------------------------------------
# Argparse shim
# ---------------------------------------------------------------------------


def test_parser_rejects_fastq_and_bam_together():
    from qiita_control_plane.cli.user._parser import _build_parser

    with pytest.raises(SystemExit):
        _build_parser().parse_args(
            ["submit-reads", "--prep-sample-idx", "1", "--fastq", "/a.fq", "--bam", "/a.bam"]
        )


def test_parser_requires_one_read_source():
    from qiita_control_plane.cli.user._parser import _build_parser

    with pytest.raises(SystemExit):
        _build_parser().parse_args(["submit-reads", "--prep-sample-idx", "1"])


@pytest.mark.parametrize(
    ("argv_extra", "expected"),
    [
        ([], {"no_watch": False, "poll_interval_seconds": 2.0, "timeout_seconds": 24 * 3600}),
        (["--no-watch"], {"no_watch": True}),
        (["--poll-interval-seconds", "0.5"], {"poll_interval_seconds": 0.5}),
    ],
)
def test_parser_watch_defaults(argv_extra, expected):
    from qiita_control_plane.cli.user._parser import _build_parser

    ns = _build_parser().parse_args(
        ["submit-reads", "--prep-sample-idx", "1", "--fastq", "/a.fq", *argv_extra]
    )
    for key, value in expected.items():
        assert getattr(ns, key) == value


def test_handler_requires_data_plane_url(tmp_path, capsys):
    """The reads are streamed to the data plane, so there is no useful default
    to fall back on — the shim refuses before reading the PAT."""
    from qiita_control_plane.cli.user._parser import _build_parser

    fastq = tmp_path / "ABC_R1.fastq"
    fastq.write_text("@r\nA\n+\nI\n")
    parser = _build_parser()
    ns = parser.parse_args(["submit-reads", "--prep-sample-idx", "1", "--fastq", str(fastq)])
    with pytest.raises(SystemExit):
        ns.handler(ns, parser)
    assert "--data-plane-url" in capsys.readouterr().err


def test_handler_refuses_an_empty_local_file(tmp_path, capsys):
    from qiita_control_plane.cli.user._parser import _build_parser

    fastq = tmp_path / "ABC_R1.fastq"
    fastq.write_bytes(b"")
    parser = _build_parser()
    ns = parser.parse_args(
        [
            "submit-reads",
            "--prep-sample-idx",
            "1",
            "--fastq",
            str(fastq),
            "--data-plane-url",
            "grpc://dp:50051",
        ]
    )
    with pytest.raises(SystemExit):
        ns.handler(ns, parser)
    assert "is empty" in capsys.readouterr().err


def test_handler_refuses_a_missing_local_file(capsys):
    from qiita_control_plane.cli.user._parser import _build_parser

    parser = _build_parser()
    ns = parser.parse_args(
        [
            "submit-reads",
            "--prep-sample-idx",
            "1",
            "--fastq",
            "/nonexistent/ABC_R1.fastq",
            "--data-plane-url",
            "grpc://dp:50051",
        ]
    )
    with pytest.raises(SystemExit):
        ns.handler(ns, parser)
    assert "not a regular file" in capsys.readouterr().err
