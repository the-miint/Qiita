"""Integration test: B6s stream → real per-shard aligner-subject build.

Proves a shard builder (`build_minimap2_index` / `build_bowtie2_index` in shard
mode) pulls its roster's reference sequence chunks from the live data plane over
Arrow Flight, reassembles them, and runs the REAL miint `save_*_index` to produce
the per-shard artifact at the deterministic derived-store path — end to end,
without reading staging Parquet.

The builder's `open_reference_chunk_stream` (which would hop to the CP for a
ticket) is monkeypatched with a fake that signs the ticket DIRECTLY with the
fixture data plane's HMAC secret and calls the real `stream_reference_chunks`
against the live `data_plane` — the CP route that mints the ticket has its own
DB-tier tests; what's exercised here is the DP DoGet + the builder's real
streaming build. Mirrors test_reference_stream.py's seeding + direct-sign trick.
"""

import asyncio

import duckdb
import pytest
from contextlib import asynccontextmanager

from qiita_common.api_paths import LOOPBACK_HOST

from qiita_compute_orchestrator.data_plane_client import stream_reference_chunks
from qiita_compute_orchestrator.jobs import build_bowtie2_index, build_minimap2_index

from conftest import ducklake_connect

# Out-of-band reference + feature_idx for this module's seeded rows (distinct from
# test_reference_stream.py's 5 / 800001..3). The roster covers the two seeded
# features; the third is excluded to prove the stream is roster-scoped.
_REF_IDX = 6
_SUBSET = [820001, 820002]
_EXCLUDED = 820003

# STRUCTURED contigs (distinct motifs tiled) so the aligners see real,
# reproducible content and build a non-empty index. ~3.6 kb each.
_CONTIGS = {
    820001: "ACGTACGTGGCCTTAAACGTTGCA" * 150,
    820002: "TTGGCCAATTGGCCAAGTGTGTGT" * 150,
    820003: "ACACACGTGTGTCCGGATGCATGC" * 150,
}


@pytest.fixture(scope="module", autouse=True)
def _seed_reference_rows(data_plane):
    """Seed multi-chunk reference sequences + membership against the live DuckLake.
    Each contig is split across two chunks to exercise reassembly in the stream."""
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


def _write_roster(path, rows):
    with duckdb.connect(":memory:") as conn:
        values_sql = ", ".join("(CAST(? AS BIGINT), CAST(? AS BIGINT))" for _ in rows)
        params = []
        for fidx, bp in rows:
            params.extend([fidx, bp])
        conn.execute(
            f"COPY (SELECT * FROM (VALUES {values_sql}) AS t(feature_idx, sequence_length_bp)) "
            f"TO '{path}' (FORMAT PARQUET)",
            params,
        )
    return path


def _fake_open_stream(data_plane):
    """A drop-in `open_reference_chunk_stream` that signs a feature_idx-scoped
    ticket with the fixture DP secret and streams via the real
    `stream_reference_chunks` — bypassing the CP hop."""
    from qiita_control_plane.auth.tickets import sign_ticket

    @asynccontextmanager
    async def fake(conn, *, reference_idx, feature_idx, relation="reference_chunks"):
        ticket = sign_ticket(
            table="reference_sequence_chunks",
            filter={"reference_idx": [reference_idx], "feature_idx": feature_idx},
            secret=data_plane["secret"],
        )
        url = f"grpc://{LOOPBACK_HOST}:{data_plane['port']}"
        with stream_reference_chunks(
            conn, data_plane_url=url, ticket_bytes=ticket, relation=relation
        ) as rel:
            yield rel

    return fake


@pytest.mark.parametrize(
    "module, meta_key, index_subdir",
    [
        (build_minimap2_index, "minimap2_index_meta", "minimap2"),
        (build_bowtie2_index, "bowtie2_index_meta", "bowtie2"),
    ],
    ids=["minimap2", "bowtie2"],
)
def test_shard_build_streams_and_writes_artifact(
    module, meta_key, index_subdir, data_plane, tmp_path, monkeypatch
):
    """A shard build streams the roster's chunks from the live DP and the real
    miint save_* writes the shard artifact at
    `.../references/{ref}/shards/{shard}/{minimap2,bowtie2}/index*`, with the meta
    carrying shard_id."""
    import json
    from pathlib import Path

    monkeypatch.setenv("PATH_DERIVED", str(tmp_path / "derived"))
    monkeypatch.setattr(
        module, "open_reference_chunk_stream", _fake_open_stream(data_plane)
    )

    shard_id = 4
    roster = _write_roster(
        tmp_path / "roster.parquet",
        [(f, len(_CONTIGS[f])) for f in _SUBSET],
    )

    inputs = module.Inputs(
        reference_idx=_REF_IDX,
        work_ticket_idx=1,
        shard_id=shard_id,
        shard_features=roster,
    )
    out = asyncio.run(module.execute(inputs, tmp_path / "ws"))

    meta = json.loads(Path(out[meta_key]).read_text())
    assert meta["shard_id"] == shard_id
    # num_subjects reflects only the roster's two features (the excluded one is
    # absent from the stream) — proves the ticket scoped the DoGet.
    assert meta["params"]["num_subjects"] == len(_SUBSET)

    fs_path = Path(meta["fs_path"])
    expected_dir = (
        Path(tmp_path / "derived")
        / "references"
        / str(_REF_IDX)
        / "shards"
        / str(shard_id)
        / index_subdir
    )
    assert fs_path.parent == expected_dir
    if index_subdir == "minimap2":
        assert fs_path.is_file() and fs_path.stat().st_size > 0  # single .mmi
    else:
        bt2 = list(expected_dir.glob(f"{fs_path.name}*.bt2"))  # multi-file .bt2 set
        assert bt2 and all(f.stat().st_size > 0 for f in bt2)
