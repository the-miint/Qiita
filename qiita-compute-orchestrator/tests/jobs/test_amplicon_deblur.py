"""Unit tests for the amplicon_deblur finalize join, the GG2 closed-reference match.

Runs against the staged miint build; `write_feature_counts` is tested in isolation
with a hand-built `non_chimera`. Pins the canonical hash: both a forward ASV and the
RC of a GG2 feature match because both sides compute
LEAST(md5(upper(seq)), md5(rc(upper(seq))))::uuid, the hash GG2 was minted with.
"""

from __future__ import annotations

import duckdb
import pytest

from qiita_compute_orchestrator.jobs.amplicon_deblur import write_feature_counts
from qiita_compute_orchestrator.miint import open_miint_conn

# a forward GG2 feature, a second whose ASV arrives as its RC, and one absent from GG2
_GG2_FWD = "ACGTACGTACGTACGT"  # feature_idx 101
_GG2_RC_SOURCE = "TTTTGGGGCCCCAAAA"  # feature_idx 102; ASV will be its RC
_NON_GG2 = "AAAACCCCGGGGTTTT"  # no GG2 feature


def _build_inputs(conn: duckdb.DuckDBPyConnection, tmp_path):
    """seed `non_chimera`, write the gg2_features / processed_map Parquets.

    single-threaded so the output Parquet reads back in physical file order, letting
    the sort assertion observe the COPY's ORDER BY. preserve_insertion_order=false
    matches the real job's apply_duckdb_settings; the COPY's ORDER BY still sorts.
    """
    conn.execute("SET threads=1")
    conn.execute("SET preserve_insertion_order=false")

    conn.execute(
        "CREATE TABLE non_chimera "
        "(sample_id BIGINT, read_id VARCHAR, sequence1 VARCHAR, abundance BIGINT)"
    )
    # sample 11: two forward reads (sum 5), the RC of feature 102, and a non-GG2 ASV
    conn.execute(
        "INSERT INTO non_chimera VALUES "
        f"(11, 'r1', '{_GG2_FWD}', 3), "
        f"(11, 'r2', '{_GG2_FWD}', 2), "
        f"(11, 'r4', '{_NON_GG2}', 9)"
    )
    # RC arm: the ASV's stored sequence is the RC of feature 102
    conn.execute(
        "INSERT INTO non_chimera "
        f"SELECT 11, 'r3', sequence_dna_reverse_complement('{_GG2_RC_SOURCE}'), 4"
    )
    # sample 12: one forward read
    conn.execute(f"INSERT INTO non_chimera VALUES (12, 'r5', '{_GG2_FWD}', 1)")

    gg2 = tmp_path / "gg2_features.parquet"
    conn.execute(
        "COPY (SELECT feature_idx::BIGINT AS feature_idx, "
        "      LEAST(md5(upper(seq))::uuid, "
        "            md5(sequence_dna_reverse_complement(upper(seq)))::uuid) AS sequence_hash "
        "      FROM (VALUES "
        f"        (101, '{_GG2_FWD}'), "
        f"        (102, '{_GG2_RC_SOURCE}')) AS t(feature_idx, seq)) "
        f"TO '{gg2}' (FORMAT PARQUET)"
    )

    processed_map = tmp_path / "processed_map.parquet"
    conn.execute(
        "COPY (SELECT * FROM (VALUES "
        "  (11::BIGINT, 1001::BIGINT), "
        "  (12::BIGINT, 1002::BIGINT)) AS t(prep_sample_idx, processed_prep_sample_idx)) "
        f"TO '{processed_map}' (FORMAT PARQUET)"
    )
    return gg2, processed_map


def test_finalize_closed_ref_join(tmp_path):
    """forward and RC-canonical match, feature_idx collapse, abundance sum, non-GG2 exclusion."""
    out = tmp_path / "feature_counts.parquet"
    with open_miint_conn() as conn:
        gg2, processed_map = _build_inputs(conn, tmp_path)
        matched = write_feature_counts(
            conn,
            processing_idx=7,
            gg2_features=gg2,
            processed_prep_sample_map=processed_map,
            out_path=out,
        )
        rows = conn.execute(
            "SELECT prep_sample_idx, processing_idx, processed_prep_sample_idx, "
            "       feature_idx, value "
            "FROM read_parquet(?) "
            "ORDER BY prep_sample_idx, processed_prep_sample_idx, feature_idx",
            [str(out)],
        ).fetchall()

    assert matched == 3
    assert rows == [
        (11, 7, 1001, 101, 5.0),  # two forward reads summed
        (11, 7, 1001, 102, 4.0),  # RC matched via canonical hash
        (12, 7, 1002, 101, 1.0),
    ]
    # the non-GG2 ASV produced no row
    assert all(feature_idx in (101, 102) for (_, _, _, feature_idx, _) in rows)


def test_finalize_output_is_physically_sorted(tmp_path):
    """the COPY's ORDER BY writes rows in canonical identifier order on disk."""
    out = tmp_path / "feature_counts.parquet"
    with open_miint_conn() as conn:
        gg2, processed_map = _build_inputs(conn, tmp_path)
        write_feature_counts(
            conn,
            processing_idx=7,
            gg2_features=gg2,
            processed_prep_sample_map=processed_map,
            out_path=out,
        )
        physical = conn.execute(
            "SELECT prep_sample_idx, processing_idx, processed_prep_sample_idx, feature_idx "
            "FROM read_parquet(?)",
            [str(out)],
        ).fetchall()
    assert physical == sorted(physical), f"feature_counts not physically sorted: {physical}"


def test_finalize_no_gg2_match_returns_zero(tmp_path):
    """a cohort whose ASVs are all absent from GG2 yields zero matched rows."""
    out = tmp_path / "feature_counts.parquet"
    with open_miint_conn() as conn:
        conn.execute("SET threads=1")
        conn.execute("SET preserve_insertion_order=false")
        conn.execute(
            "CREATE TABLE non_chimera "
            "(sample_id BIGINT, read_id VARCHAR, sequence1 VARCHAR, abundance BIGINT)"
        )
        conn.execute(f"INSERT INTO non_chimera VALUES (11, 'r1', '{_NON_GG2}', 5)")
        gg2 = tmp_path / "gg2.parquet"
        conn.execute(
            "COPY (SELECT feature_idx::BIGINT AS feature_idx, "
            "      LEAST(md5(upper(seq))::uuid, "
            "            md5(sequence_dna_reverse_complement(upper(seq)))::uuid) AS sequence_hash "
            f"     FROM (VALUES (101, '{_GG2_FWD}')) AS t(feature_idx, seq)) "
            f"TO '{gg2}' (FORMAT PARQUET)"
        )
        processed_map = tmp_path / "pm.parquet"
        conn.execute(
            "COPY (SELECT * FROM (VALUES (11::BIGINT, 1001::BIGINT)) "
            "AS t(prep_sample_idx, processed_prep_sample_idx)) "
            f"TO '{processed_map}' (FORMAT PARQUET)"
        )
        matched = write_feature_counts(
            conn,
            processing_idx=7,
            gg2_features=gg2,
            processed_prep_sample_map=processed_map,
            out_path=out,
        )
    assert matched == 0


def test_finalize_dedups_duplicate_gg2_rows(tmp_path):
    """duplicate (feature_idx, sequence_hash) gg2 rows must not fan out and double-count."""
    out = tmp_path / "feature_counts.parquet"
    with open_miint_conn() as conn:
        conn.execute("SET threads=1")
        conn.execute("SET preserve_insertion_order=false")
        conn.execute(
            "CREATE TABLE non_chimera "
            "(sample_id BIGINT, read_id VARCHAR, sequence1 VARCHAR, abundance BIGINT)"
        )
        conn.execute(f"INSERT INTO non_chimera VALUES (11, 'r1', '{_GG2_FWD}', 5)")
        # same (feature_idx, sequence_hash) written twice, the fan-out trigger
        gg2 = tmp_path / "gg2_dup.parquet"
        conn.execute(
            "COPY (SELECT feature_idx::BIGINT AS feature_idx, "
            "      LEAST(md5(upper(seq))::uuid, "
            "            md5(sequence_dna_reverse_complement(upper(seq)))::uuid) AS sequence_hash "
            f"     FROM (VALUES (101, '{_GG2_FWD}'), (101, '{_GG2_FWD}')) AS t(feature_idx, seq)) "
            f"TO '{gg2}' (FORMAT PARQUET)"
        )
        processed_map = tmp_path / "pm.parquet"
        conn.execute(
            "COPY (SELECT * FROM (VALUES (11::BIGINT, 1001::BIGINT)) "
            "AS t(prep_sample_idx, processed_prep_sample_idx)) "
            f"TO '{processed_map}' (FORMAT PARQUET)"
        )
        write_feature_counts(
            conn,
            processing_idx=7,
            gg2_features=gg2,
            processed_prep_sample_map=processed_map,
            out_path=out,
        )
        rows = conn.execute(f"SELECT feature_idx, value FROM read_parquet('{out}')").fetchall()
    # one row, value 5 not 10: the duplicate gg2 row is deduped
    assert rows == [(101, 5.0)], f"expected no double-count, got {rows}"


def test_execute_missing_input_raises(tmp_path):
    """a missing input path fails loudly (mapped to BAD_INPUT by the dispatcher)."""
    import asyncio

    from qiita_compute_orchestrator.jobs.amplicon_deblur import Inputs, execute

    inputs = Inputs(
        pool_reads=tmp_path / "absent_reads.parquet",
        gg2_features=tmp_path / "absent_gg2.parquet",
        processed_prep_sample_map=tmp_path / "absent_map.parquet",
        primer="GTGYCAGCMGCCGCGGTAA",
        trim=150,
        sortmerna_ref_path=tmp_path / "absent_ref.fasta",
        processing_idx=7,
        sequenced_pool_idx=1,
        sequencing_run_idx=1,
        work_ticket_idx=1,
    )
    with pytest.raises(FileNotFoundError):
        asyncio.run(execute(inputs, tmp_path / "ws"))


# ---------------------------------------------------------------------------
# full-pipeline smoke: runs the real SortMeRNA, MAFFT, deblur, UCHIME, GG2-finalize
# chain on tiny self-contained 16S V4 data. the SortMeRNA reference is three real
# E. coli 16S windows that also serve as the ASVs and the GG2 features, so no
# downloads are needed. proves the ported deblur.sql runs end-to-end on the staged
# miint build.
# ---------------------------------------------------------------------------

# ~460 nt of real E. coli 16S (V3-V4); three non-overlapping 150 nt windows give
# three distinct 16S ASVs SortMeRNA aligns against a reference built from themselves
# and deblur keeps separate
_ECOLI_16S_V3V4 = (
    "CCTACGGGAGGCAGCAGTGGGGAATATTGCACAATGGGCGCAAGCCTGATGCAGCCATGCCGCGTGTATGAAGAAGGCC"
    "TTCGGGTTGTAAAGTACTTTCAGCGGGGAGGAAGGGAGTAAAGTTAATACCTTTGCTCATTGACGTTACCCGCAGAAGA"
    "AGCACCGGCTAACTCCGTGCCAGCAGCCGCGGTAATACGGAGGGTGCAAGCGTTAATCGGAATTACTGGGCGTAAAGCG"
    "CACGCAGGCGGTTTGTTAAGTCAGATGTGAAATCCCCGGGCTCAACCTGGGAACTGCATCTGATACTGGCAAGCTTGAG"
    "TCTCGTAGAGGGGGGTAGAATTCCAGGTGTAGCGGTGAAATGCGTAGAGATCTGGAGGAATACCGGTGGCGAAGGCGGC"
    "CCCCTGGACGAAGACTGACGCTCAGGTGCGAAAGCGTGGGGAGCAAACAGGATTAGATACCCTGGTAGTCCACGCCGTA"
    "AACGATGTCGACTTGGAGGTTGTGCCCTTGAGGCGTGGCTTCCGGAGCTAACGCGTTAAGTCGACCGCCTGGGGAGTAC"
)
# 515F with degenerate bases resolved to concrete ones (Y->C, M->A) so a read is
# plain ACGT yet still matches the degenerate primer regex
_CONCRETE_515F = "GTGCCAGCAGCCGCGGTAA"


def _asv_windows() -> list[str]:
    """three distinct real-16S 150 nt windows."""
    return [_ECOLI_16S_V3V4[0:150], _ECOLI_16S_V3V4[155:305], _ECOLI_16S_V3V4[310:460]]


def test_execute_full_pipeline_synthetic(tmp_path):
    """end-to-end filter, denoise, finalize over two samples, all rows map to the GG2 set."""
    import asyncio

    from qiita_compute_orchestrator.jobs.amplicon_deblur import Inputs, execute

    windows = _asv_windows()
    for w in windows:
        assert len(w) == 150

    # SortMeRNA reference FASTA is the three ASV windows themselves
    ref = tmp_path / "tiny_16s.fasta"
    ref.write_text("".join(f">ref{i}\n{w}\n" for i, w in enumerate(windows)))

    with open_miint_conn() as conn:
        conn.execute("SET threads=1")
        conn.execute("SET preserve_insertion_order=false")

        # reads: 2 samples x 3 ASVs x 5 copies; each read is concrete primer + window,
        # so primer-trim recovers the window
        rows = []
        seq_idx = 5000
        for prep in (11, 12):
            for w in windows:
                for _ in range(5):
                    seq_idx += 1
                    rows.append((prep, seq_idx, f"r{seq_idx}", _CONCRETE_515F + w))
        values = ", ".join(
            f"({p}, {s}, '{rid}', '{seq}', NULL, NULL, NULL)" for (p, s, rid, seq) in rows
        )
        conn.execute(
            "CREATE TABLE r(prep_sample_idx BIGINT, sequence_idx BIGINT, read_id VARCHAR, "
            "sequence1 VARCHAR, qual1 UTINYINT[], sequence2 VARCHAR, qual2 UTINYINT[])"
        )
        conn.execute(f"INSERT INTO r VALUES {values}")
        reads = tmp_path / "reads.parquet"
        conn.execute(f"COPY r TO '{reads}' (FORMAT PARQUET)")

        # GG2 features: canonical hash of each window to a feature_idx (101, 102, 103)
        gg2 = tmp_path / "gg2_features.parquet"
        gg2_values = ", ".join(f"({101 + i}, '{w}')" for i, w in enumerate(windows))
        conn.execute(
            "COPY (SELECT feature_idx::BIGINT AS feature_idx, "
            "      LEAST(md5(upper(seq))::uuid, "
            "            md5(sequence_dna_reverse_complement(upper(seq)))::uuid) AS sequence_hash "
            f"     FROM (VALUES {gg2_values}) AS t(feature_idx, seq)) "
            f"TO '{gg2}' (FORMAT PARQUET)"
        )

        processed_map = tmp_path / "processed_map.parquet"
        conn.execute(
            "COPY (SELECT * FROM (VALUES (11::BIGINT, 1001::BIGINT), (12::BIGINT, 1002::BIGINT)) "
            "AS t(prep_sample_idx, processed_prep_sample_idx)) "
            f"TO '{processed_map}' (FORMAT PARQUET)"
        )

    inputs = Inputs(
        pool_reads=reads,
        gg2_features=gg2,
        processed_prep_sample_map=processed_map,
        primer="GTGYCAGCMGCCGCGGTAA",
        trim=150,
        sortmerna_ref_path=ref,
        processing_idx=7,
        sequenced_pool_idx=1,
        sequencing_run_idx=1,
        work_ticket_idx=1,
    )
    out = asyncio.run(execute(inputs, tmp_path / "ws"))
    fc = out["feature_counts_staging_dir"] / "feature_counts.parquet"
    assert fc.exists()

    with open_miint_conn() as conn:
        result = conn.execute(
            "SELECT prep_sample_idx, processing_idx, processed_prep_sample_idx, feature_idx, value "
            "FROM read_parquet(?)",
            [str(fc)],
        ).fetchall()

    # the survivor set is deblur/UCHIME-dependent, so assert shape not exact counts:
    # non-empty, every row well-formed and mapped to a GG2 feature, both samples scoped
    assert result, "the full pipeline produced no feature_counts rows"
    for prep, proc, processed, feat, value in result:
        assert prep in (11, 12)
        assert proc == 7
        assert processed == {11: 1001, 12: 1002}[prep]
        assert feat in (101, 102, 103)
        assert value > 0
