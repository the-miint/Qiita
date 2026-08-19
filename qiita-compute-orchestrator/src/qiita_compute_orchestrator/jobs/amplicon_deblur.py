"""Rapid 16S amplicon denoising — the deblur pipeline as a native miint job.

Ports duckdb-miint's Rapid 16S deblur (filter → denoise) into Qiita, REFERENCE-
AGNOSTIC: it denoises the pool's reads into ASVs and emits, per sample, the ASV's
canonical sequence_hash + abundance. It does NOT match against GG2 — feature_idx
is minted from the sequence_hash (mint-features) and the closed-reference feature
table is DERIVED later by intersecting feature_idx with a reference's membership.

Refinements over the historical deblur.sql (validated to reproduce the golden):
  * primer search/orient is conditional (`orient_primer`, default off — Rapid 16S
    reads are already primer-stripped, so the old CASE was dead work);
  * chimera detection (UCHIME-denovo) runs BEFORE the expensive MSA;
  * feature identity is the system-wide canonical sequence hash.

Outputs (consumed by the amplicon workflow):
  * asv_manifest — DISTINCT `sequence_hash` for the `mint-features` action.
  * asv_counts   — `(prep_sample_idx, sequence_hash, count)` per sample; joined to
    the minted `feature_idx` into `amplicon_membership` downstream.
"""

from __future__ import annotations

from pathlib import Path

from pydantic import BaseModel
from qiita_common.backend_failure import StepNoData
from qiita_common.chunking import canonical_sequence_hash_expr
from qiita_common.parquet import validate_parquet_path

from ..miint import (
    PARQUET_OPTS,
    apply_duckdb_settings,
    duckdb_tmp_dir,
    mafft_scratch_cwd,
    open_miint_conn,
    resolve_duckdb_memory_gb,
)

YAML_STEP_NAME = "denoise"

# DuckDB's ops here are light; the GPL-linked SortMeRNA/MAFFT/VSEARCH do the heavy
# work as separate processes with their own parallelism, so DuckDB stays modest and
# leaves the cgroup's cores + RAM to them.
_DUCKDB_THREADS = 4
_DUCKDB_FALLBACK_MEMORY_GB = 8
_DUCKDB_RESERVE_GB = 32


class Inputs(BaseModel):
    """Typed input contract for the amplicon denoise step.

    pool_reads: the pool's reads Parquet (read_masked shape: prep_sample_idx,
        sequence_idx, read_id, sequence1, ...). prep_sample_idx is the per-sample
        partition; sequence1 is the read.
    sortmerna_ref: a FASTA of the SortMeRNA 16S reference, materialized by the
        runner from a loaded sequence_reference (NOT a fixed operator path).
    primer: forward primer (EMP V4 515F); only consulted when orient_primer.
    orient_primer: run the primer search/orient/extract step. Default False —
        Rapid 16S reads arrive primer-stripped.
    trim: bp truncation length (150 for the GG2 V4 catalog).
    """

    pool_reads: Path
    sortmerna_ref: Path
    primer: str
    trim: int
    orient_primer: bool = False
    sequenced_pool_idx: int
    sequencing_run_idx: int
    work_ticket_idx: int


# deblur.sql `filter` for Rapid 16S (primer-orient skipped): trim → per-sample
# derep (abundance >= 2) → SortMeRNA 16S pre-filter → drop samples with < 2 unique.
_FILTER_SQL = """
CREATE OR REPLACE TABLE trimmed AS
SELECT sample_id, sample_id || '_r' || sequence_index AS read_id,
       sequence1[:getvariable('rapid_trim')] AS sequence1
FROM _inputs
WHERE length(sequence1) >= getvariable('rapid_trim');

CREATE OR REPLACE TABLE derep AS
SELECT sample_id, MIN(read_id) AS read_id, sequence1, COUNT(*)::BIGINT AS abundance
FROM trimmed GROUP BY sample_id, sequence1 HAVING COUNT(*) >= 2;

CREATE OR REPLACE VIEW is_rrna AS
SELECT DISTINCT read_id FROM align_sortmerna_rrna('derep',
    ref_paths := [getvariable('miint_sortmerna_ref')]) WHERE aligned = 1 AND coverage >= 50;

CREATE OR REPLACE TABLE alignable AS
WITH joined AS (SELECT d.sample_id, d.read_id, d.sequence1, d.abundance
                FROM derep d JOIN is_rrna USING (read_id))
SELECT * FROM joined WHERE sample_id IN (
    SELECT sample_id FROM joined GROUP BY sample_id HAVING COUNT(DISTINCT sequence1) >= 2);
"""

# Chimera BEFORE the MSA (a chimera is a chimera irrespective of denoising, and it
# shrinks the MAFFT input), then re-apply MAFFT's >=2-unique-per-sample requirement.
_CHIMERA_SQL = """
CREATE OR REPLACE TABLE chimera_calls AS
SELECT * FROM detect_chimera_uchime_denovo('alignable', sample_id := 'sample_id',
    dn := 0.000001, xn := 1000, minh := 10000000, mindiffs := 5, count_col := 'abundance');
CREATE OR REPLACE TABLE nonchim AS
SELECT a.sample_id, a.read_id, a.sequence1, a.abundance
FROM alignable a JOIN (SELECT read_id FROM chimera_calls WHERE flag='N') USING (read_id);
CREATE OR REPLACE TABLE alignable2 AS
SELECT * FROM nonchim WHERE sample_id IN (
    SELECT sample_id FROM nonchim GROUP BY sample_id HAVING COUNT(DISTINCT sequence1) >= 2);
"""

# Per-sample MAFFT MSA → deblur denoise.
_DENOISE_SQL = """
CREATE OR REPLACE TABLE aligned AS
SELECT am.sample_id, am.read_id, am.aligned_sequence, d.abundance
FROM align_mafft('alignable2', sample_id := 'sample_id') am JOIN alignable2 d USING (read_id);
CREATE OR REPLACE TABLE deblurred AS
SELECT sample_id, read_id, sequence AS sequence1, abundance
FROM deblur('aligned', sequence_col := 'aligned_sequence', sample_id := 'sample_id')
ORDER BY abundance DESC;
"""

# primer search/orient/extract (only when orient_primer): forward or RC-orient the
# read to the primer, then trim. Kept for protocols that need it.
_ORIENT_SQL = """
CREATE OR REPLACE TABLE _inputs_oriented AS
SELECT sample_id, sequence_index,
    CASE
      WHEN regexp_matches(sequence1, getvariable('rapid_regex_fwd'))
           AND regexp_matches(sequence1, getvariable('rapid_regex_fwd_rc')) THEN NULL
      WHEN regexp_matches(sequence1, getvariable('rapid_regex_fwd'))
        THEN regexp_extract(sequence1, getvariable('rapid_regex_fwd_extract'), 2)
      WHEN regexp_matches(sequence1, getvariable('rapid_regex_fwd_rc'))
        THEN regexp_extract(sequence_dna_reverse_complement(sequence1),
                            getvariable('rapid_regex_fwd_extract'), 2)
      ELSE sequence1
    END AS sequence1
FROM _inputs;
"""


def _set_session_vars(conn, *, primer: str, trim: int, sortmerna_ref: Path, orient: bool) -> None:
    conn.execute(f"SET VARIABLE rapid_trim = {int(trim)};")
    safe_ref = str(sortmerna_ref).replace("'", "''")
    conn.execute(f"SET VARIABLE miint_sortmerna_ref = '{safe_ref}';")
    if orient:
        safe_primer = primer.replace("'", "")
        conn.execute(f"SET VARIABLE rapid_fwd_primer = '{safe_primer}';")
        conn.execute(
            "SET VARIABLE rapid_regex_fwd = "
            "sequence_dna_as_regexp(getvariable('rapid_fwd_primer'));"
        )
        conn.execute(
            "SET VARIABLE rapid_regex_fwd_rc = sequence_dna_as_regexp("
            "sequence_dna_reverse_complement(getvariable('rapid_fwd_primer')));"
        )
        conn.execute(
            "SET VARIABLE rapid_regex_fwd_extract = "
            "'(' || getvariable('rapid_regex_fwd') || ')([ATGC]+)';"
        )


async def execute(inputs: Inputs, workspace: Path) -> dict[str, Path]:
    """denoise the pool's reads into ASVs; emit the mint-features manifest + per-sample
    ASV counts. StepNoData when nothing survives filtering/denoising."""
    workspace = workspace.resolve()
    inputs.pool_reads = inputs.pool_reads.resolve()
    inputs.sortmerna_ref = inputs.sortmerna_ref.resolve()
    for p in (inputs.pool_reads, inputs.sortmerna_ref):
        if not p.exists():
            raise FileNotFoundError(f"amplicon_deblur input not found: {p}")

    workspace.mkdir(parents=True, exist_ok=True)
    counts_out = workspace / "asv_counts.parquet"
    manifest_out = workspace / "asv_manifest.parquet"
    memory_gb = resolve_duckdb_memory_gb(
        _DUCKDB_FALLBACK_MEMORY_GB, threads=_DUCKDB_THREADS, reserve_gb=_DUCKDB_RESERVE_GB
    )
    safe_reads = validate_parquet_path(inputs.pool_reads)

    with (
        duckdb_tmp_dir(workspace) as duckdb_tmp,
        mafft_scratch_cwd(workspace),
        open_miint_conn() as conn,
    ):
        apply_duckdb_settings(conn, duckdb_tmp, memory_gb=memory_gb, threads=_DUCKDB_THREADS)
        _set_session_vars(
            conn,
            primer=inputs.primer,
            trim=inputs.trim,
            sortmerna_ref=inputs.sortmerna_ref,
            orient=inputs.orient_primer,
        )
        # _inputs(sample_id, sequence_index, sequence1): prep_sample_idx is sample_id.
        conn.execute(
            "CREATE OR REPLACE VIEW _inputs AS "
            "SELECT prep_sample_idx AS sample_id, sequence_idx AS sequence_index, sequence1 "
            f"FROM read_parquet('{safe_reads}')"
        )
        if inputs.orient_primer:
            conn.execute(_ORIENT_SQL)
            conn.execute(
                "CREATE OR REPLACE VIEW _inputs AS SELECT sample_id, sequence_index, sequence1 "
                "FROM _inputs_oriented WHERE sequence1 IS NOT NULL"
            )

        conn.execute(_FILTER_SQL)
        if conn.execute("SELECT count(*) FROM alignable").fetchone()[0] == 0:
            raise StepNoData(
                step_name=YAML_STEP_NAME,
                reason="no sample retained >=2 unique 16S-matching dereplicated sequences",
            )
        conn.execute(_CHIMERA_SQL)
        if conn.execute("SELECT count(*) FROM alignable2").fetchone()[0] == 0:
            raise StepNoData(
                step_name=YAML_STEP_NAME,
                reason="no non-chimeric sample retained >=2 unique sequences",
            )
        conn.execute(_DENOISE_SQL)

        canon = canonical_sequence_hash_expr("sequence1")
        conn.execute(
            f"CREATE OR REPLACE TABLE asv AS "
            f"SELECT sample_id::BIGINT AS prep_sample_idx, {canon} AS sequence_hash, "
            f"       SUM(abundance)::BIGINT AS count "
            f"FROM deblurred GROUP BY prep_sample_idx, sequence_hash"
        )
        if conn.execute("SELECT count(*) FROM asv").fetchone()[0] == 0:
            raise StepNoData(step_name=YAML_STEP_NAME, reason="no ASV survived denoising")

        safe_counts = validate_parquet_path(counts_out)
        safe_manifest = validate_parquet_path(manifest_out)
        conn.execute(
            f"COPY (SELECT prep_sample_idx, sequence_hash, count FROM asv "
            f"ORDER BY prep_sample_idx, sequence_hash) TO '{safe_counts}' ({PARQUET_OPTS})"
        )
        conn.execute(
            f"COPY (SELECT DISTINCT sequence_hash FROM asv ORDER BY sequence_hash) "
            f"TO '{safe_manifest}' ({PARQUET_OPTS})"
        )

    return {"asv_counts": counts_out, "asv_manifest": manifest_out}
