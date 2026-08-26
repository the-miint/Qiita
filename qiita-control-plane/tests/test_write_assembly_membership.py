"""DB-bound tests for the write-assembly-membership primitive.

The assembly twin of `test_write_membership_stores_representative_accession`
(tests/routes/test_reference_schema.py). What it pins here is the collapse
report on the REAL call path: the unit tests in test_actions_library.py drive
`_warn_on_collapsed_records` directly and so cannot catch the primitive passing
it the wrong manifest, the wrong scope, or never calling it at all.
"""

import logging
import secrets
import uuid

import duckdb
import pytest

from qiita_control_plane.actions import library as lib
from qiita_control_plane.repositories.processing import mint_processing
from qiita_control_plane.testing.db_seeds import (
    canonical_sequence_hashes,
    seed_biosample_with_sequenced_prep_sample,
    seed_user_principal,
)

pytestmark = pytest.mark.db

# Non-palindromic, so folding a strand is distinguishable from an identity, and
# mixed case on the pair so the fixture exercises both folds the canonical hash
# applies. `_SOLO` is the control that must NOT collapse.
_CONTIG = "acgtTGCAAGGTCCATTGCA"
_SOLO = "TTTTGGGGCCCCAAAATTTT"


def _revcomp(sequence: str) -> str:
    """miint's reverse complement, case preserved
    (https://the-miint.github.io/duckdb-miint/utilities/)."""
    from qiita_control_plane.miint import connect_with_miint

    with connect_with_miint() as conn:
        return conn.execute("SELECT sequence_dna_reverse_complement(?)", [sequence]).fetchone()[0]


def _write(path, schema, rows):
    with duckdb.connect(":memory:") as c:
        c.execute(f"CREATE TEMP TABLE t ({schema})")
        c.executemany(f"INSERT INTO t VALUES ({', '.join('?' for _ in rows[0])})", rows)
        c.execute(f"COPY t TO '{path}' (FORMAT PARQUET)")


async def test_write_assembly_membership_reports_a_contig_collapse(postgres_pool, tmp_path, caplog):
    """Two contigs that are reverse complements mint one feature, so one of them
    is absent from the run — reported, and scoped to the assembly rather than to
    a reference."""
    suffix = secrets.token_hex(4)
    principal_idx = await seed_user_principal(
        postgres_pool, prefix="assembly-membership-test", suffix=suffix
    )
    _bs, prep_sample_idx = await seed_biosample_with_sequenced_prep_sample(
        postgres_pool, owner_idx=principal_idx
    )
    version = f"v-{uuid.uuid4()}"
    async with postgres_pool.acquire() as conn:
        row = await mint_processing(
            conn,
            workflow="long-read-assembly",
            version=version,
            params={"workflow": "long-read-assembly", "version": version, "assembler": "flye"},
        )
    processing_idx = row["processing_idx"]

    # The canonical hash folds the pair, so miint returns ONE hash for the two
    # contigs and a second for the control — the same dedup qiita.feature's
    # UNIQUE applies, evaluated by the production expression rather than mirrored.
    (h_pair,) = canonical_sequence_hashes([_CONTIG, _revcomp(_CONTIG)])
    (h_solo,) = canonical_sequence_hashes([_SOLO])

    feat_pair = await postgres_pool.fetchval(
        "INSERT INTO qiita.feature (sequence_hash) VALUES ($1) RETURNING feature_idx", h_pair
    )
    feat_solo = await postgres_pool.fetchval(
        "INSERT INTO qiita.feature (sequence_hash) VALUES ($1) RETURNING feature_idx", h_solo
    )

    # assembly_hash's synthetic read_id is kind:bin_id:sequence_index, so the two
    # halves of the pair land in different bins and both membership rows survive
    # the natural PK.
    bin_map = tmp_path / "bin_map.parquet"
    manifest = tmp_path / "manifest.parquet"
    feature_map = tmp_path / "feature_map.parquet"
    _write(
        bin_map,
        "read_id VARCHAR, kind VARCHAR, bin_id VARCHAR, contig_id VARCHAR",
        [
            ("MAG:b1:0", "MAG", "b1", "contig_1"),
            ("MAG:b2:0", "MAG", "b2", "contig_1_rc"),
            ("MAG:b1:1", "MAG", "b1", "contig_2"),
        ],
    )
    _write(
        manifest,
        "read_id VARCHAR, sequence_hash UUID, sequence_length_bp BIGINT",
        [
            ("MAG:b1:0", str(h_pair), len(_CONTIG)),
            ("MAG:b2:0", str(h_pair), len(_CONTIG)),
            ("MAG:b1:1", str(h_solo), len(_SOLO)),
        ],
    )
    _write(
        feature_map,
        "sequence_hash UUID, feature_idx BIGINT",
        [(str(h_pair), feat_pair), (str(h_solo), feat_solo)],
    )

    try:
        with caplog.at_level(logging.WARNING):
            linked, already = await lib.write_assembly_membership(
                postgres_pool,
                prep_sample_idx,
                processing_idx,
                bin_map,
                manifest,
                feature_map,
            )

        # One feature in two bins plus the control: the join is per placement,
        # which is exactly why the report cannot read its row count as a feature
        # count.
        assert (linked, already) == (3, 0)

        collapse = [r.getMessage() for r in caplog.records if "collapsed" in r.getMessage()]
        assert len(collapse) == 1
        assert collapse[0].startswith(
            f"assembly run (prep_sample {prep_sample_idx}, processing {processing_idx}): "
        )
        assert "3 submitted record(s) collapsed to 2 feature(s)" in collapse[0]
        assert "MAG:b1:0, MAG:b2:0" in collapse[0]
        assert "MAG:b1:1" not in collapse[0]
    finally:
        # write_assembly_membership commits; the feature rows outlive the
        # cascade from prep_sample, so drop the junction before them.
        await postgres_pool.execute(
            "DELETE FROM qiita.assembly_membership WHERE prep_sample_idx = $1", prep_sample_idx
        )
        await postgres_pool.execute(
            "DELETE FROM qiita.feature WHERE feature_idx = ANY($1::bigint[])",
            [feat_pair, feat_solo],
        )
