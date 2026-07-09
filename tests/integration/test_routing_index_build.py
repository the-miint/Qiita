"""Integration smoke: build the whole-reference rype ROUTER over the live DP,
then `rype_classify` a known read and assert it routes to the expected shard.

Proves the C1 routing path end to end: `build_routing_index` streams the WHOLE
reference's chunks from the live data plane (a whole-reference DoGet — the DP
JOINs `reference_membership` to resolve `reference_idx` -> features), runs the
REAL miint `rype_index_create` with a MULTI-bucket (one-bucket-per-shard) mapping
to write the router `.ryxdi`, and a subsequent `rype_classify` emits
`(read_id, bucket_name)` = `(read, str(shard_id))` — the `read_to_shard` signal
`align_*_sharded` consumes.

`build_routing_index.open_reference_chunk_stream` (which would hop to the CP for
a ticket) is monkeypatched to sign the whole-reference chunk ticket DIRECTLY with
the fixture DP's HMAC secret (the CP mint route has its own DB-tier tests); what
is exercised here is the DP whole-reference DoGet + the real rype build, then a
real classify. Mirrors test_reference_stream_build's seeding + direct-sign trick.
"""

import asyncio
import json
from contextlib import asynccontextmanager
from pathlib import Path

import duckdb
import pytest

from qiita_common.api_paths import LOOPBACK_HOST
from qiita_compute_orchestrator.data_plane_client import stream_reference_chunks
from qiita_compute_orchestrator.jobs import build_routing_index
from qiita_compute_orchestrator.miint import open_miint_conn

from conftest import ducklake_connect

# Out-of-band reference + feature_idx for this module's seeded rows (distinct from
# the other stream modules). Three features across TWO shards: 830001+830002 ->
# shard 0, 830003 -> shard 1.
_REF_IDX = 7
_SHARD_OF = {830001: 0, 830002: 0, 830003: 1}

# STRUCTURED contigs (distinct motifs tiled) so rype sees real, reproducible,
# separable minimizer content. ~3.6 kb each.
_CONTIGS = {
    830001: "ACGTACGTGGCCTTAAACGTTGCA" * 150,
    830002: "TTGGCCAATTGGCCAAGTGTGTGT" * 150,
    830003: "ACACACGTGTGTCCGGATGCATGC" * 150,
}


@pytest.fixture(scope="module", autouse=True)
def _seed_reference_rows(data_plane):
    """Seed multi-chunk reference sequences + membership against the live DuckLake.
    Each contig is split across two chunks to exercise reassembly in the stream;
    the membership rows let the whole-reference DoGet resolve reference_idx ->
    features."""
    conn = ducklake_connect(data_plane["data_path"])
    try:
        rows = []
        for fidx, seq in _CONTIGS.items():
            mid = len(seq) // 2
            rows.append((fidx, 0, seq[:mid]))
            rows.append((fidx, 1, seq[mid:]))
        values = ", ".join(f"({f}, {c}, '{d}')" for f, c, d in rows)
        conn.execute(
            f"INSERT INTO qiita_lake.reference_sequence_chunks VALUES {values}"
        )
        member_values = ", ".join(f"({_REF_IDX}, {f})" for f in _CONTIGS)
        conn.execute(
            f"INSERT INTO qiita_lake.reference_membership VALUES {member_values}"
        )
    finally:
        conn.close()


def _fake_open_whole_reference_stream(data_plane):
    """A drop-in `open_reference_chunk_stream` that signs a WHOLE-REFERENCE chunk
    ticket (reference_idx only, no feature_idx) with the fixture DP secret and
    streams via the real `stream_reference_chunks` — bypassing the CP hop. The DP
    resolves the reference to its features via `reference_membership`."""
    from qiita_control_plane.auth.tickets import sign_ticket

    @asynccontextmanager
    async def fake(conn, *, reference_idx, feature_idx, relation="reference_chunks"):
        assert feature_idx is None, "router build must stream the WHOLE reference"
        ticket = sign_ticket(
            table="reference_sequence_chunks",
            filter={"reference_idx": [reference_idx]},
            secret=data_plane["secret"],
        )
        url = f"grpc://{LOOPBACK_HOST}:{data_plane['port']}"
        with stream_reference_chunks(
            conn, data_plane_url=url, ticket_bytes=ticket, relation=relation
        ) as rel:
            yield rel

    return fake


def _write_shard_mapping(path):
    """Write the shard_mapping Parquet `(feature_idx BIGINT, bucket_name VARCHAR)`
    — one row per feature, bucket_name = str(shard_id)."""
    with duckdb.connect(":memory:") as conn:
        values_sql = ", ".join(
            "(CAST(? AS BIGINT), CAST(? AS VARCHAR))" for _ in _SHARD_OF
        )
        params = []
        for fidx, shard_id in _SHARD_OF.items():
            params.extend([fidx, str(shard_id)])
        conn.execute(
            f"COPY (SELECT * FROM (VALUES {values_sql}) AS t(feature_idx, bucket_name)) "
            f"TO '{path}' (FORMAT PARQUET)",
            params,
        )
    return path


def _classify_bucket(router_dir, read_id, sequence):
    """Run the REAL rype_classify of a single read against the router and return
    the DISTINCT set of bucket_names it routes to (str(shard_id) values)."""
    with open_miint_conn() as conn:
        # Non-temp VIEW so rype's separate connection resolves it by name. DuckDB
        # rejects prepared params inside CREATE VIEW, so the read is inlined; it is
        # controlled ACGT content (a contig slice), quote-escaped defensively.
        seq_sql = sequence.replace("'", "''")
        conn.execute(
            f"CREATE OR REPLACE VIEW route_query AS "
            f"SELECT CAST({int(read_id)} AS BIGINT) AS read_id, "
            f"'{seq_sql}' AS sequence1"
        )
        rows = conn.execute(
            "SELECT DISTINCT bucket_name FROM "
            "rype_classify(?, 'route_query', id_column := 'read_id', threshold := ?)",
            [str(router_dir), 0.1],
        ).fetchall()
    return {r[0] for r in rows}


def test_build_routing_index_routes_reads_to_expected_shard(
    data_plane, tmp_path, monkeypatch
):
    """Build the router over the live DP, then classify a read drawn from each
    feature and assert it routes to that feature's shard bucket
    (`bucket_name == str(shard_id)`)."""
    monkeypatch.setenv("PATH_DERIVED", str(tmp_path / "derived"))
    monkeypatch.setattr(
        build_routing_index,
        "open_reference_chunk_stream",
        _fake_open_whole_reference_stream(data_plane),
    )

    mapping = _write_shard_mapping(tmp_path / "shard_mapping.parquet")
    inputs = build_routing_index.Inputs(
        reference_idx=_REF_IDX, work_ticket_idx=1, shard_mapping=mapping
    )
    out = asyncio.run(build_routing_index.execute(inputs, tmp_path / "ws"))

    meta = json.loads(Path(out["routing_index_meta"]).read_text())
    router_dir = Path(meta["fs_path"])
    assert (
        router_dir
        == tmp_path / "derived" / "references" / str(_REF_IDX) / "rype-router.ryxdi"
    )
    assert router_dir.is_dir(), "router .ryxdi was not built"
    assert meta["index_type"] == "rype_router"
    # Two shard buckets over three features (830001+830002 -> 0, 830003 -> 1).
    assert meta["params"]["shard_count"] == 2
    assert meta["params"]["feature_count"] == 3

    # A read taken verbatim from each feature's contig routes to that feature's
    # shard. A ~240 bp window (>> k=64) is unambiguous minimizer content.
    for fidx, shard_id in _SHARD_OF.items():
        read = _CONTIGS[fidx][:240]
        buckets = _classify_bucket(router_dir, fidx, read)
        assert str(shard_id) in buckets, (
            f"read from feature {fidx} routed to {buckets}, expected shard {shard_id}"
        )
