"""Integration smoke: the full C1 sharded-alignment path against real miint.

Builds, over a tiny 2-shard reference and the LIVE data plane, everything C1
produces — the whole-reference rype ROUTER (`build_routing_index`) and the
per-shard minimap2 + bowtie2 indexes (`build_{minimap2,bowtie2}_index` in shard
mode, the revised `derived_store` layout) — then drives `align_sharded` over
crafted reads and asserts the routing + alignment behaviour end to end.

A read set is uniformly single-end OR paired-end by construction, so the two modes
are exercised as SEPARATE, uniform batches (never mixed — a mix is invalid input
that bowtie2 rejects at bind):

  SINGLE-END batch:
  - a read drawn from a feature's DISTINCT region aligns to that feature (its
    shard) and no other;
  - a read from a region SHARED by a feature in each shard routes to BOTH shards
    and emits TWO rows with DISTINCT feature_idx — cross-shard multiplicity, no
    dedup (a shared region so it aligns end-to-end for bowtie2 too, not just the
    soft-clipping minimap2);
  - a non-matching read emits nothing.

  PAIRED-END batch:
  - a proper pair whose mates both fall in one feature aligns as ONE read: two
    mate rows to the SAME feature, carrying their mate columns (mate_feature_idx,
    template_length, mate_reference) so the pairing is EXPLICIT — not two unrelated
    single-end rows. Pinned as an exact per-feature count PLUS a mate-column check.

Parametrized over both aligners (minimap2, bowtie2). The index BUILDS stream
reference chunks from the DP, so their `open_reference_chunk_stream` is
monkeypatched to sign a ticket directly against the fixture DP's HMAC secret
(the CP mint route has its own tests) — feature-scoped for the per-shard builders,
whole-reference for the router. `align_sharded` itself reads only local artifacts
(the staged reads Parquet + the on-disk router/shard indexes), so it needs no
patch.
"""

import asyncio
import json
from contextlib import asynccontextmanager
from pathlib import Path

import duckdb
import pytest

from qiita_common.api_paths import LOOPBACK_HOST
from qiita_compute_orchestrator.data_plane_client import stream_reference_chunks
from qiita_compute_orchestrator.derived_store import (
    shard_bowtie2_dir,
    shard_minimap2_dir,
)
from qiita_compute_orchestrator.jobs import (
    align_sharded,
    build_bowtie2_index,
    build_minimap2_index,
    build_routing_index,
)

from conftest import ducklake_connect

_REF_IDX = 8
# Each feature = a DISTINCT region + a region SHARED by both features. A read from
# a distinct region aligns to exactly one feature (one shard); a read from the
# shared region aligns END-TO-END to BOTH features — so it routes to both shards
# and yields two distinct-feature rows (the multiplicity/no-dedup case) for the
# end-to-end aligner (bowtie2) AND the soft-clip aligner (minimap2). Sequence-
# diverse 25-mers (not low-complexity), tiled to ~650 bp each (>> k=64).
_DISTINCT_A = "GATTACAGGCCTAGCATCGTTACGA" * 26  # feature 100 only
_DISTINCT_B = "TTCAGCATTGCACGTATCCTGGAAC" * 26  # feature 200 only
_SHARED = "ACGTGGCCAATTCGATTACGCATGA" * 26  # in BOTH features
_A = _DISTINCT_A + _SHARED  # feature 100 -> shard 0
_B = _DISTINCT_B + _SHARED  # feature 200 -> shard 1
_FEATURES = {100: _A, 200: _B}
_SHARD_OF = {100: 0, 200: 1}

_COMPLEMENT = str.maketrans("ACGT", "TGCA")


def _revcomp(seq: str) -> str:
    return seq.translate(_COMPLEMENT)[::-1]


@pytest.fixture(scope="module", autouse=True)
def _seed_reference_rows(data_plane):
    """Seed multi-chunk reference sequences + membership against the live DuckLake
    (two chunks per contig to exercise reassembly; membership lets the
    whole-reference router DoGet resolve reference_idx -> features)."""
    conn = ducklake_connect(data_plane["data_path"])
    try:
        rows = []
        for fidx, seq in _FEATURES.items():
            mid = len(seq) // 2
            rows.append((fidx, 0, seq[:mid]))
            rows.append((fidx, 1, seq[mid:]))
        values = ", ".join(f"({f}, {c}, '{d}')" for f, c, d in rows)
        conn.execute(
            f"INSERT INTO qiita_lake.reference_sequence_chunks VALUES {values}"
        )
        member_values = ", ".join(f"({_REF_IDX}, {f})" for f in _FEATURES)
        conn.execute(
            f"INSERT INTO qiita_lake.reference_membership VALUES {member_values}"
        )
    finally:
        conn.close()


def _fake_stream(data_plane):
    """Feature-scoped `open_reference_chunk_stream` for the per-shard builders —
    signs a `{reference_idx, feature_idx}` ticket against the fixture DP secret."""
    from qiita_control_plane.auth.tickets import sign_ticket

    @asynccontextmanager
    async def fake(conn, *, reference_idx, feature_idx, relation="reference_chunks"):
        flt = {"reference_idx": [reference_idx]}
        if feature_idx is not None:
            flt["feature_idx"] = feature_idx
        ticket = sign_ticket(
            table="reference_sequence_chunks", filter=flt, secret=data_plane["secret"]
        )
        url = f"grpc://{LOOPBACK_HOST}:{data_plane['port']}"
        with stream_reference_chunks(
            conn, data_plane_url=url, ticket_bytes=ticket, relation=relation
        ) as rel:
            yield rel

    return fake


def _write_roster(path, feature_idx):
    with duckdb.connect(":memory:") as conn:
        conn.execute(
            "COPY (SELECT CAST(? AS BIGINT) AS feature_idx, CAST(? AS BIGINT) AS sequence_length_bp) "
            f"TO '{path}' (FORMAT PARQUET)",
            [feature_idx, len(_FEATURES[feature_idx])],
        )
    return path


def _write_shard_mapping(path):
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


def _write_reads(path, rows):
    """rows = (prep_sample_idx, sequence_idx, sequence1, sequence2)."""
    with duckdb.connect(":memory:") as conn:
        values_sql = ", ".join(
            "(CAST(? AS BIGINT), CAST(? AS BIGINT), CAST(? AS VARCHAR), CAST(? AS VARCHAR))"
            for _ in rows
        )
        params = []
        for ps, sidx, s1, s2 in rows:
            params.extend([ps, sidx, s1, s2])
        conn.execute(
            f"COPY (SELECT * FROM (VALUES {values_sql}) "
            "AS t(prep_sample_idx, sequence_idx, sequence1, sequence2)) "
            f"TO '{path}' (FORMAT PARQUET)",
            params,
        )
    return path


def _build_indexes(
    aligner, module, shard_dir_fn, data_plane, derived_root, tmp_path, monkeypatch
):
    """Build the router + both per-shard indexes for `aligner`, returning
    (router_dir, shard_directory)."""
    monkeypatch.setenv("PATH_DERIVED", str(derived_root))

    # Per-shard aligner indexes (shard mode streams the shard's one feature).
    monkeypatch.setattr(module, "open_reference_chunk_stream", _fake_stream(data_plane))
    for fidx, shard_id in _SHARD_OF.items():
        roster = _write_roster(tmp_path / f"roster_{aligner}_{shard_id}.parquet", fidx)
        inputs = module.Inputs(
            reference_idx=_REF_IDX,
            work_ticket_idx=1,
            shard_id=shard_id,
            shard_features=roster,
        )
        asyncio.run(module.execute(inputs, tmp_path / f"ws_build_{aligner}_{shard_id}"))

    # Whole-reference router.
    monkeypatch.setattr(
        build_routing_index, "open_reference_chunk_stream", _fake_stream(data_plane)
    )
    mapping = _write_shard_mapping(tmp_path / "shard_mapping.parquet")
    r_out = asyncio.run(
        build_routing_index.execute(
            build_routing_index.Inputs(
                reference_idx=_REF_IDX, work_ticket_idx=1, shard_mapping=mapping
            ),
            tmp_path / "ws_router",
        )
    )
    router_dir = Path(
        json.loads(Path(r_out["routing_index_meta"]).read_text())["fs_path"]
    )
    return router_dir, shard_dir_fn(derived_root, _REF_IDX)


def _features_by_read(alignment_path):
    """Map sequence_idx -> sorted list of aligned feature_idx from alignment.parquet."""
    with duckdb.connect(":memory:") as conn:
        rows = conn.execute(
            f"SELECT sequence_idx, feature_idx, prep_sample_idx "
            f"FROM read_parquet('{alignment_path}') ORDER BY sequence_idx, feature_idx"
        ).fetchall()
    by_read: dict[int, list[int]] = {}
    prep_of: dict[int, set[int]] = {}
    for sidx, fidx, ps in rows:
        by_read.setdefault(sidx, []).append(fidx)
        prep_of.setdefault(sidx, set()).add(ps)
    return by_read, prep_of


def _mate_rows(alignment_path, sequence_idx):
    """Rows for one read as (feature_idx, mate_feature_idx, mate_reference,
    template_length), ordered by position — the mate columns that must survive so a
    PE pair's rows are an explicit pair, not two unrelated single-end rows."""
    with duckdb.connect(":memory:") as conn:
        return conn.execute(
            "SELECT feature_idx, mate_feature_idx, mate_reference, template_length "
            f"FROM read_parquet('{alignment_path}') WHERE sequence_idx = ? "
            "ORDER BY position, flags",
            [sequence_idx],
        ).fetchall()


@pytest.mark.parametrize(
    "aligner, module, shard_dir_fn",
    [
        ("minimap2", build_minimap2_index, shard_minimap2_dir),
        ("bowtie2", build_bowtie2_index, shard_bowtie2_dir),
    ],
    ids=["minimap2", "bowtie2"],
)
def test_sharded_alignment_end_to_end(
    aligner, module, shard_dir_fn, data_plane, tmp_path, monkeypatch
):
    derived_root = tmp_path / "derived"
    router_dir, shard_directory = _build_indexes(
        aligner, module, shard_dir_fn, data_plane, derived_root, tmp_path, monkeypatch
    )

    def _align(reads_path):
        inputs = align_sharded.Inputs(
            reads=reads_path,
            reference_idx=_REF_IDX,
            aligner=aligner,
            router_index_path=router_dir,
            shard_directory=shard_directory,
            work_ticket_idx=1,
        )
        return Path(
            asyncio.run(
                align_sharded.execute(inputs, tmp_path / f"ws_align_{aligner}")
            )["alignment"]
        )

    # ---- SINGLE-END batch (uniform: every sequence2 is NULL) --------------------
    # 1 = feature-100-distinct (shard 0), 2 = feature-200-distinct (shard 1),
    # 3 = SHARED region (routes to BOTH shards -> two distinct-feature rows, the
    # multiplicity/no-dedup case), 4 = non-matching (routes nowhere). prep_sample 10.
    se_reads = _write_reads(
        tmp_path / f"reads_se_{aligner}.parquet",
        [
            (10, 1, _DISTINCT_A[:240], None),
            (10, 2, _DISTINCT_B[:240], None),
            (10, 3, _SHARED[:240], None),
            (10, 4, "TTTTTTGGGGGGCCCCCCAAAAAA" * 10, None),
        ],
    )
    se_out = _align(se_reads)
    by_read, prep_of = _features_by_read(se_out)

    # Each single-feature read aligns to exactly its feature.
    assert by_read.get(1) == [100], f"read 1 -> {by_read.get(1)}"
    assert by_read.get(2) == [200], f"read 2 -> {by_read.get(2)}"
    # Chimera routes to BOTH shards and emits two DISTINCT-feature rows (no dedup).
    assert by_read.get(3) == [100, 200], f"chimera -> {by_read.get(3)}"
    # Non-matching read emits nothing.
    assert 4 not in by_read, f"non-matching read aligned: {by_read.get(4)}"
    # prep_sample_idx is stamped per row from the reads.
    assert prep_of.get(1) == {10}

    # ---- PAIRED-END batch (uniform: every sequence2 is non-NULL) ----------------
    # 5 = a proper fr pair, both mates in feature 100's distinct region (mate2 is
    # the reverse-complement of a downstream segment). prep_sample 20.
    pe_reads = _write_reads(
        tmp_path / f"reads_pe_{aligner}.parquet",
        [(20, 5, _A[:150], _revcomp(_A[300:450]))],
    )
    pe_out = _align(pe_reads)
    by_read_pe, prep_of_pe = _features_by_read(pe_out)

    # PE read aligns as ONE read: one SAM row per mate, both to feature 100 (NOT
    # collapsed). Exact count, not a set, so a change in mate-row emission is caught.
    assert by_read_pe.get(5) == [100, 100], f"PE read 5 -> {by_read_pe.get(5)}"
    assert prep_of_pe.get(5) == {20}

    # The mate columns survive so the pair is EXPLICIT (not two unrelated SE rows):
    # both rows resolve their mate to feature 100 and carry a non-zero (signed)
    # template_length. mate_reference is SAM's RNEXT ('=' or the numeric id); the
    # decoded mate_feature_idx must be 100 on both.
    mate_rows = _mate_rows(pe_out, 5)
    assert len(mate_rows) == 2, f"expected 2 mate rows, got {mate_rows}"
    for feature_idx, mate_feature_idx, mate_reference, template_length in mate_rows:
        assert feature_idx == 100
        assert mate_feature_idx == 100, (
            f"mate not resolved to feature 100: {mate_reference!r}"
        )
        assert template_length != 0, "proper pair must carry a template_length"
