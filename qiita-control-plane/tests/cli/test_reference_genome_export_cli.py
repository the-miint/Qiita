"""Unit tests for the genome-export CLI (`qiita reference export`) — no DB, no
server, no data plane.

Covers the route helpers (patching httpx.request, the entry point `_common.call`
delegates to), the parquet writer (pyarrow only, no miint), the per-genome
dispatch (which writer + output path per format), and the parser wiring. The
real FASTA writer (miint FORMAT FASTA) round-trip is the integration test's job
(miint isn't guaranteed in the pure-unit tier)."""

import base64
from pathlib import Path

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
from qiita_common.api_paths import URL_REFERENCE_DOGET, URL_REFERENCE_GENOME_MEMBER

from qiita_control_plane.cli.user import reference as ref


def _fake_request_capturing(captured, response_json):
    def fake_request(method, url, headers=None, json=None, params=None, timeout=None):
        captured["method"] = method
        captured["url"] = url
        captured["json"] = json
        return httpx.Response(200, json=response_json, request=httpx.Request(method, url))

    return fake_request


def test_resolve_genome_members_gets_route(monkeypatch):
    captured: dict = {}
    body = [{"feature_idx": 10, "accession": "NZ_A.1"}, {"feature_idx": 11, "accession": None}]
    monkeypatch.setattr(ref._common.httpx, "request", _fake_request_capturing(captured, body))

    members = ref._resolve_genome_members("http://cp", "qk_tok", reference_idx=5, genome_idx=10)
    assert captured["method"] == "GET"
    expected = f"http://cp{URL_REFERENCE_GENOME_MEMBER.format(reference_idx=5, genome_idx=10)}"
    assert captured["url"] == expected
    assert members == body


def test_create_chunks_doget_ticket_posts_and_decodes(monkeypatch):
    captured: dict = {}
    ticket_b64 = base64.b64encode(b"signed-ticket-bytes").decode()
    monkeypatch.setattr(
        ref._common.httpx, "request", _fake_request_capturing(captured, {"ticket": ticket_b64})
    )

    ticket = ref._create_chunks_doget_ticket(
        "http://cp", "qk_tok", reference_idx=5, feature_idxs=[10, 11]
    )
    assert captured["method"] == "POST"
    assert captured["url"] == f"http://cp{URL_REFERENCE_DOGET.format(reference_idx=5)}"
    assert captured["json"] == {"table": "reference_sequence_chunks", "feature_idx": [10, 11]}
    assert ticket == b"signed-ticket-bytes"


def _chunk_reader(rows):
    """A pyarrow RecordBatchReader over reference_sequence_chunks-shaped rows."""
    schema = pa.schema(
        [("feature_idx", pa.int64()), ("chunk_index", pa.int32()), ("chunk_data", pa.string())]
    )
    batch = pa.record_batch(
        {
            "feature_idx": pa.array([r[0] for r in rows], pa.int64()),
            "chunk_index": pa.array([r[1] for r in rows], pa.int32()),
            "chunk_data": pa.array([r[2] for r in rows], pa.string()),
        }
    )
    return pa.RecordBatchReader.from_batches(schema, [batch])


def test_write_genome_parquet_streams_reader(tmp_path):
    reader = _chunk_reader([(10, 0, "ACGT"), (10, 1, "TTTT"), (11, 0, "GGGG")])
    out = tmp_path / "5.10.parquet"
    ref._write_genome_parquet(reader, out)

    assert out.exists()
    assert not (tmp_path / "5.10.parquet.partial").exists()  # atomic: no leftover
    table = pq.read_table(out)
    assert table.column("feature_idx").to_pylist() == [10, 10, 11]
    assert table.column("chunk_data").to_pylist() == ["ACGT", "TTTT", "GGGG"]


def test_write_genome_parquet_cleans_partial_on_failure(tmp_path):
    """A failure mid-write leaves no committed file and no `.partial`."""

    class _Boom:
        schema = pa.schema([("feature_idx", pa.int64())])

        def __iter__(self):
            raise RuntimeError("stream died")

    out = tmp_path / "5.10.parquet"
    try:
        ref._write_genome_parquet(_Boom(), out)
    except RuntimeError:
        pass
    assert not out.exists()
    assert not (tmp_path / "5.10.parquet.partial").exists()


def test_write_genome_parquet_returns_distinct_feature_count(tmp_path):
    """The writer reports the number of DISTINCT feature_idx written (feature-
    scale, not chunk-scale — three features across four chunk rows) so the caller
    can detect a short export."""
    reader = _chunk_reader([(10, 0, "A"), (10, 1, "C"), (11, 0, "G"), (12, 0, "T")])
    out = tmp_path / "5.10.parquet"
    assert ref._write_genome_parquet(reader, out) == 3


class _FakeStream:
    def __init__(self, reader):
        self._reader = reader

    def to_reader(self):
        return self._reader


class _FakeFlightClient:
    def __init__(self, reader):
        self._reader = reader
        self.tickets = []

    def do_get(self, ticket, *options):
        # FASTA path passes FlightCallOptions (buffer realignment); parquet doesn't.
        self.tickets.append(ticket)
        return _FakeStream(self._reader)


def test_export_one_genome_dispatch_parquet(monkeypatch, tmp_path):
    monkeypatch.setattr(
        ref,
        "_resolve_genome_members",
        lambda *a, **k: [{"feature_idx": 10, "accession": "NZ_A.1"}],
    )
    monkeypatch.setattr(ref, "_create_chunks_doget_ticket", lambda *a, **k: b"tkt")
    calls: dict = {}
    # The writer returns the count of distinct features written; 1 member -> 1, so
    # no short-export failure fires.
    monkeypatch.setattr(
        ref,
        "_write_genome_parquet",
        lambda reader, target: calls.update(parquet=(reader, target)) or 1,
    )
    monkeypatch.setattr(
        ref,
        "_write_genome_fasta",
        lambda reader, acc, target, con: calls.update(fasta=(reader, acc, target, con)) or 1,
    )
    sentinel_reader = object()
    fc = _FakeFlightClient(sentinel_reader)

    out = ref._export_one_genome(
        base_url="http://cp",
        token="tok",
        flight_client=fc,
        con=None,
        reference_idx=5,
        genome_idx=10,
        fmt="parquet",
        output_dir=tmp_path,
    )
    assert out == tmp_path / "5.10.parquet"
    assert calls["parquet"] == (sentinel_reader, tmp_path / "5.10.parquet")
    assert "fasta" not in calls


def test_export_one_genome_dispatch_fasta(monkeypatch, tmp_path):
    members = [{"feature_idx": 10, "accession": "NZ_A.1"}, {"feature_idx": 11, "accession": None}]
    monkeypatch.setattr(ref, "_resolve_genome_members", lambda *a, **k: members)
    monkeypatch.setattr(ref, "_create_chunks_doget_ticket", lambda *a, **k: b"tkt")
    calls: dict = {}
    # 2 members -> the invoked writer returns 2, so no short-export failure fires.
    monkeypatch.setattr(
        ref,
        "_write_genome_parquet",
        lambda reader, target: calls.update(parquet=(reader, target)) or 2,
    )
    monkeypatch.setattr(
        ref,
        "_write_genome_fasta",
        lambda reader, acc, target, con: calls.update(fasta=(reader, acc, target, con)) or 2,
    )
    sentinel_con = object()
    fc = _FakeFlightClient(object())

    out = ref._export_one_genome(
        base_url="http://cp",
        token="tok",
        flight_client=fc,
        con=sentinel_con,
        reference_idx=5,
        genome_idx=11,
        fmt="fasta",
        output_dir=tmp_path,
    )
    assert out == tmp_path / "5.11.fasta.gz"
    reader, acc, target, con = calls["fasta"]
    assert acc == {10: "NZ_A.1", 11: None}  # accession map from the member list
    assert target == tmp_path / "5.11.fasta.gz"
    assert con is sentinel_con
    assert "parquet" not in calls


def test_export_one_genome_raises_and_removes_file_on_short_export(monkeypatch, tmp_path):
    """A genome whose writer emits fewer distinct features than its member set
    (an indexing reference whose chunks are not yet in DuckLake, or a partial
    delete) is a hard error, and the incomplete file it wrote is removed rather
    than left looking complete."""
    import pytest

    members = [
        {"feature_idx": 10, "accession": "a"},
        {"feature_idx": 11, "accession": "b"},
        {"feature_idx": 12, "accession": "c"},
    ]
    monkeypatch.setattr(ref, "_resolve_genome_members", lambda *a, **k: members)
    monkeypatch.setattr(ref, "_create_chunks_doget_ticket", lambda *a, **k: b"tkt")

    target = tmp_path / "5.10.parquet"

    def _short_writer(reader, tgt):
        tgt.write_bytes(b"incomplete")  # the committed-but-short file
        return 2  # only 2 of 3 members had chunks in DuckLake

    monkeypatch.setattr(ref, "_write_genome_parquet", _short_writer)
    fc = _FakeFlightClient(object())

    with pytest.raises(ref._IncompleteExportError, match="wrote 2 of 3"):
        ref._export_one_genome(
            base_url="http://cp",
            token="tok",
            flight_client=fc,
            con=None,
            reference_idx=5,
            genome_idx=10,
            fmt="parquet",
            output_dir=tmp_path,
        )
    assert not target.exists()  # the short file was removed


def test_handle_export_returns_1_on_short_export(monkeypatch, tmp_path):
    """The entry point maps a short export to exit 1 with a message (fail loud),
    not an uncaught traceback."""
    import argparse

    monkeypatch.setattr(ref._common, "read_token", lambda: "tok")

    def _raise_short(**kwargs):
        raise ref._IncompleteExportError("genome 10: wrote 0 of 3 member sequence(s)")

    monkeypatch.setattr(ref, "_export_one_genome", _raise_short)

    class _FakeClient:
        def __init__(self, url):
            self.url = url

        def close(self):
            pass

    monkeypatch.setattr("pyarrow.flight.FlightClient", _FakeClient)

    args = argparse.Namespace(
        reference_idx=5,
        genome_idx=[10],
        format="parquet",  # con stays None -> no miint needed
        output_dir=tmp_path,
        base_url="http://cp",
        data_plane_url="grpc://host:50051",
    )
    assert ref._handle_reference_genome_export(args, parser=None) == 1


def test_parser_wires_export():
    from qiita_control_plane.cli.user._parser import _build_parser

    parser = _build_parser()
    args = parser.parse_args(
        [
            "reference",
            "export",
            "--reference-idx",
            "5",
            "--genome-idx",
            "10",
            "--genome-idx",
            "11",
            "--output-dir",
            "/tmp/out",
            "--data-plane-url",
            "grpc://host:50051",
        ]
    )
    assert args.handler is ref._handle_reference_genome_export
    assert args.reference_idx == 5
    assert args.genome_idx == [10, 11]  # repeatable -> list
    assert args.format == "fasta"  # default
    assert args.output_dir == Path("/tmp/out")


def test_parser_export_rejects_missing_required():
    from qiita_control_plane.cli.user._parser import _build_parser

    parser = _build_parser()
    import pytest

    with pytest.raises(SystemExit):
        parser.parse_args(["reference", "export", "--genome-idx", "10"])  # no --reference-idx
