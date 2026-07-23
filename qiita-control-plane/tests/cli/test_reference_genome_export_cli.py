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
    monkeypatch.setattr(
        ref, "_write_genome_parquet", lambda reader, target: calls.update(parquet=(reader, target))
    )
    monkeypatch.setattr(
        ref,
        "_write_genome_fasta",
        lambda reader, acc, target, con: calls.update(fasta=(reader, acc, target, con)),
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
    monkeypatch.setattr(
        ref, "_write_genome_parquet", lambda reader, target: calls.update(parquet=(reader, target))
    )
    monkeypatch.setattr(
        ref,
        "_write_genome_fasta",
        lambda reader, acc, target, con: calls.update(fasta=(reader, acc, target, con)),
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
