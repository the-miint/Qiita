"""Rapid 16S amplicon denoising: the deblur.sql pipeline as a native miint job.

Ports duckdb-miint's Rapid 16S filter/denoise/finalize into one in-memory miint job,
but sources GG2 from the reference subsystem and emits the DuckLake feature_counts table
instead of BIOM/FASTA/TSV.
"""

from __future__ import annotations

from pathlib import Path

from pydantic import BaseModel
from qiita_common.backend_failure import StepNoData
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

# DuckDB's ops here are light; the heavy GPL-boundary tools (SortMeRNA/MAFFT/vsearch) run
# as separate processes with their own parallelism, so DuckDB stays modest and leaves the
# cgroup's cores + RAM to them (reserve_gb).
_DUCKDB_THREADS = 4
_DUCKDB_FALLBACK_MEMORY_GB = 8
_DUCKDB_RESERVE_GB = 32  # carved out of the cgroup for the aligners

# deblur.sql `filter` stage, verbatim (minus the `-- @stage:filter` marker and the
# `SET preserve_insertion_order` line, which apply_duckdb_settings already sets).
# `_inputs(sample_id, sequence_index, sequence1)` is a view built before this runs.
_FILTER_SQL = """
CREATE OR REPLACE TABLE trimmed AS
SELECT sample_id,
       sample_id || '_r' || sequence_index AS read_id,
       seq[:getvariable('rapid_trim')]     AS sequence1
FROM (
    SELECT
        sample_id,
        sequence_index,
        CASE
          WHEN regexp_matches(sequence1, getvariable('rapid_regex_fwd'))
               AND regexp_matches(sequence1, getvariable('rapid_regex_fwd_rc'))
            THEN NULL
          WHEN regexp_matches(sequence1, getvariable('rapid_regex_fwd'))
            THEN regexp_extract(sequence1,
                                getvariable('rapid_regex_fwd_extract'), 2)
          WHEN regexp_matches(sequence1, getvariable('rapid_regex_fwd_rc'))
            THEN regexp_extract(sequence_dna_reverse_complement(sequence1),
                                getvariable('rapid_regex_fwd_extract'), 2)
          ELSE sequence1
        END AS seq
    FROM _inputs
)
WHERE seq IS NOT NULL
  AND length(seq) >= getvariable('rapid_trim');

CREATE OR REPLACE TABLE derep AS
SELECT sample_id,
       MIN(read_id)     AS read_id,
       sequence1,
       COUNT(*)::BIGINT AS abundance
FROM trimmed
GROUP BY sample_id, sequence1
HAVING COUNT(*) >= 2;

CREATE OR REPLACE VIEW is_rrna AS
SELECT DISTINCT read_id
FROM align_sortmerna_rrna('derep',
                          ref_paths := [getvariable('miint_sortmerna_ref')])
WHERE aligned = 1 AND coverage >= 50;

CREATE OR REPLACE TABLE alignable AS
WITH joined AS (
    SELECT d.sample_id, d.read_id, d.sequence1, d.abundance
    FROM derep d JOIN is_rrna USING (read_id)
)
SELECT * FROM joined
WHERE sample_id IN (
    SELECT sample_id FROM joined
    GROUP BY sample_id
    HAVING COUNT(DISTINCT sequence1) >= 2
);
"""

# deblur.sql `denoise` stage, verbatim.
_DENOISE_SQL = """
CREATE OR REPLACE TABLE aligned AS
SELECT am.sample_id, am.read_id, am.aligned_sequence, d.abundance
FROM align_mafft('alignable', sample_id := 'sample_id') am
JOIN alignable d USING (read_id);

CREATE OR REPLACE TABLE deblurred AS
SELECT sample_id, read_id, sequence AS sequence1, abundance
FROM deblur('aligned',
            sequence_col := 'aligned_sequence',
            sample_id    := 'sample_id')
ORDER BY abundance DESC, sequence1;

CREATE OR REPLACE TABLE chimera_calls AS
SELECT * FROM detect_chimera_uchime_denovo(
    'deblurred', sample_id := 'sample_id',
    dn := 0.000001, xn := 1000, minh := 10000000,
    mindiffs := 5, count_col := 'abundance');

CREATE OR REPLACE TABLE non_chimera AS
SELECT d.sample_id, d.read_id, d.sequence1, d.abundance
FROM deblurred d
JOIN (SELECT DISTINCT read_id FROM chimera_calls WHERE flag='N')
     USING (read_id);
"""


class Inputs(BaseModel):
    """Typed input contract for the amplicon denoise step.

    pool_reads: exported masked pool reads Parquet; prep_sample_idx is the per-sample
        partition key. Named `pool_reads` (not `reads`) so the read-mask workflow's
        prep_sample-only `reads` resolver does not also fire for this pool-scoped workflow.
    gg2_features: exported GG2 closed-reference set, the canonical-hash to feature_idx map.
    processed_prep_sample_map: CP-minted per-(method, sample) leaf identity per pool member.
    primer: forward primer (e.g. EMP V4); the rapid_* regex session vars derive from it.
    trim: bp truncation length (150 for the GG2 V4 catalog).
    """

    pool_reads: Path
    gg2_features: Path
    processed_prep_sample_map: Path
    primer: str
    trim: int
    sortmerna_ref_path: Path
    processing_idx: int
    # framework-injected; accepted for contract explicitness, not all are read.
    sequenced_pool_idx: int
    sequencing_run_idx: int
    work_ticket_idx: int


def write_feature_counts(
    conn,
    *,
    processing_idx: int,
    gg2_features: Path,
    processed_prep_sample_map: Path,
    out_path: Path,
) -> int:
    """Run deblur.sql's `finalize` against the connection's existing `non_chimera` table.

    The load-bearing departure from deblur.sql: the GG2 closed-ref filter joins gg2_features
    on the canonical hash `LEAST(md5(upper(seq)), md5(rc(upper(seq))))::uuid` (as
    hash_sequences minted GG2), so a forward ASV and the RC of a GG2 feature both match.
    Factored out so the finalize join can be unit-tested without the aligners.
    """
    safe_out = validate_parquet_path(out_path)
    conn.execute(
        "COPY ("
        "  SELECT n.sample_id::BIGINT          AS prep_sample_idx,"
        "         ?::BIGINT                    AS processing_idx,"
        "         m.processed_prep_sample_idx,"
        "         g.feature_idx,"
        "         SUM(n.abundance)::DOUBLE      AS value"
        "  FROM non_chimera n"
        # DISTINCT: reference_sequences can hold >1 row per feature_idx (RC-collapsed
        # ASVs), so the join would otherwise fan out and double-count. dedup to be safe.
        "  JOIN (SELECT DISTINCT feature_idx, sequence_hash FROM read_parquet(?)) g"
        "    ON g.sequence_hash = LEAST("
        "         md5(upper(n.sequence1))::uuid,"
        "         md5(sequence_dna_reverse_complement(upper(n.sequence1)))::uuid)"
        "  JOIN read_parquet(?) m"
        "    ON m.prep_sample_idx = n.sample_id"
        "  GROUP BY n.sample_id, m.processed_prep_sample_idx, g.feature_idx"
        "  ORDER BY prep_sample_idx, processing_idx,"
        "           processed_prep_sample_idx, feature_idx"
        f") TO '{safe_out}' ({PARQUET_OPTS})",
        [processing_idx, str(gg2_features), str(processed_prep_sample_map)],
    )
    return conn.execute("SELECT count(*) FROM read_parquet(?)", [str(out_path)]).fetchone()[0]


def _set_session_vars(conn, *, primer: str, trim: int, sortmerna_ref: Path) -> None:
    """Bind the rapid_* / miint_sortmerna_ref session vars deblur.sql reads.

    miint_outdir / miint_gg2_taxonomy are deliberately not set — this job writes
    feature_counts Parquet, not the BIOM/FASTA/TSV artifacts.
    """
    safe_primer = primer.replace("'", "")
    conn.execute(f"SET VARIABLE rapid_fwd_primer = '{safe_primer}';")
    conn.execute(
        "SET VARIABLE rapid_regex_fwd = sequence_dna_as_regexp(getvariable('rapid_fwd_primer'));"
    )
    conn.execute(
        "SET VARIABLE rapid_regex_fwd_rc = "
        "sequence_dna_as_regexp("
        "  sequence_dna_reverse_complement(getvariable('rapid_fwd_primer')));"
    )
    conn.execute(
        "SET VARIABLE rapid_regex_fwd_extract = "
        "'(' || getvariable('rapid_regex_fwd') || ')([ATGC]+)';"
    )
    conn.execute(f"SET VARIABLE rapid_trim = {int(trim)};")
    safe_ref = str(sortmerna_ref).replace("'", "''")
    conn.execute(f"SET VARIABLE miint_sortmerna_ref = '{safe_ref}';")


async def execute(inputs: Inputs, workspace: Path) -> dict[str, Path]:
    """Run filter, denoise, finalize over the pool's masked reads.

    Returns `{"feature_counts_staging_dir": workspace}`. Raises StepNoData (terminal
    NO_DATA, not a failure) when no sample survives the filter or every surviving ASV
    is a chimera.
    """
    # resolve to absolute up front: these paths are inlined/bound into the SQL and finalize
    # runs under mafft_scratch_cwd's chdir, so a relative path would resolve wrong.
    inputs.pool_reads = inputs.pool_reads.resolve()
    inputs.gg2_features = inputs.gg2_features.resolve()
    inputs.processed_prep_sample_map = inputs.processed_prep_sample_map.resolve()
    for path in (inputs.pool_reads, inputs.gg2_features, inputs.processed_prep_sample_map):
        if not path.exists():
            raise FileNotFoundError(f"amplicon input not found: {path}")

    # absolute so the finalize COPY / read_parquet paths survive the mafft_scratch_cwd
    # chdir below (align_mafft writes its scratch to CWD).
    workspace = workspace.resolve()
    workspace.mkdir(parents=True, exist_ok=True)
    out_path = workspace / "feature_counts.parquet"
    memory_gb = resolve_duckdb_memory_gb(
        _DUCKDB_FALLBACK_MEMORY_GB, threads=_DUCKDB_THREADS, reserve_gb=_DUCKDB_RESERVE_GB
    )

    with (
        duckdb_tmp_dir(workspace) as duckdb_tmp,
        mafft_scratch_cwd(workspace),
        open_miint_conn() as conn,
    ):
        apply_duckdb_settings(conn, duckdb_tmp, memory_gb=memory_gb, threads=_DUCKDB_THREADS)
        _set_session_vars(
            conn, primer=inputs.primer, trim=inputs.trim, sortmerna_ref=inputs.sortmerna_ref_path
        )

        # _inputs(sample_id, sequence_index, sequence1): sample_id is the prep_sample_idx,
        # carried through every stage and emitted in finalize. The reads path is inlined
        # (sanitised) because DuckDB rejects a bound parameter inside CREATE VIEW.
        safe_reads = validate_parquet_path(inputs.pool_reads)
        conn.execute(
            "CREATE OR REPLACE VIEW _inputs AS "
            "SELECT prep_sample_idx AS sample_id, "
            "       sequence_idx    AS sequence_index, "
            "       sequence1 "
            f"FROM read_parquet('{safe_reads}')"
        )

        # filter stage. Guard before denoise: align_mafft on an empty `alignable` errors,
        # so short-circuit to NO_DATA when no sample survives the filter.
        conn.execute(_FILTER_SQL)
        if conn.execute("SELECT count(*) FROM alignable").fetchone()[0] == 0:
            raise StepNoData(
                step_name=YAML_STEP_NAME,
                reason=(
                    "no sample retained >=2 unique 16S-matching dereplicated "
                    "sequences after primer-trim / SortMeRNA filtering"
                ),
            )

        # denoise stage. Guard before finalize: every surviving ASV could be a chimera,
        # leaving nothing to match against GG2.
        conn.execute(_DENOISE_SQL)
        if conn.execute("SELECT count(*) FROM non_chimera").fetchone()[0] == 0:
            raise StepNoData(
                step_name=YAML_STEP_NAME,
                reason="no non-chimeric ASV survived deblur / UCHIME-denovo denoising",
            )

        # finalize: closed-reference match against the exported GG2 set, collapse md5 to
        # the pre-existing feature_idx, and write feature_counts sorted for DuckLake pruning.
        matched_rows = write_feature_counts(
            conn,
            processing_idx=inputs.processing_idx,
            gg2_features=inputs.gg2_features,
            processed_prep_sample_map=inputs.processed_prep_sample_map,
            out_path=out_path,
        )

    if matched_rows == 0:
        raise StepNoData(
            step_name=YAML_STEP_NAME,
            reason="no non-chimeric ASV matched the GG2 closed-reference catalog",
        )

    return {"feature_counts_staging_dir": workspace}
