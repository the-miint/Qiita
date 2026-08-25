"""Rapid 16S deblur as a native miint job.

Reference-agnostic. Denoise the pool's reads into ASVs; emit each ASV's canonical
sequence_hash and per-sample count. mint-features mints feature_idx from the hash;
the closed-reference table is derived later.

Refinements over the historical deblur.sql (reproduce the golden):
  * primer orient is optional (`orient_primer`, default off);
  * UCHIME chimera detection runs before the MSA;
  * feature identity is the shared canonical hash.

Outputs: manifest (read_id, sequence_hash, sequence_length_bp for mint-features),
asv_counts (prep_sample_idx, sequence_hash, count), and asv_chunks (a hash-keyed
chunk directory carrying each ASV's bytes so amplicon_load can store them).
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path

from pydantic import BaseModel
from qiita_common.backend_failure import StepNoData
from qiita_common.chunking import canonical_sequence_hash_expr, sequence_split_expr
from qiita_common.parquet import validate_parquet_path

from ..miint import (
    PARQUET_OPTS,
    PARQUET_OPTS_CHUNKED,
    apply_duckdb_settings,
    duckdb_tmp_dir,
    mafft_scratch_cwd,
    open_miint_conn,
    resolve_duckdb_memory_gb,
)
from ..read_source import bind_step_reads

# IUPAC codes a primer may contain; validated before use.
_IUPAC_DNA = frozenset("ACGTURYSWKMBDHVN")

YAML_STEP_NAME = "denoise"

# keep DuckDB light; the tools run as separate processes.
_DUCKDB_THREADS = 4
_DUCKDB_FALLBACK_MEMORY_GB = 8
_DUCKDB_RESERVE_GB = 32


class Inputs(BaseModel):
    """Typed input contract for the amplicon denoise step.

    reads: unset in practice; the pool's reads stream from the data plane.
        bind_step_reads binds the shared read projection.
    sortmerna_ref: SortMeRNA 16S FASTA, materialized by the runner from a reference.
    primer: forward primer (EMP V4 515F); used only when orient_primer.
    orient_primer: run the primer orient step. Default off.
    trim: truncation length (150 for the GG2 V4 catalog).
    """

    reads: Path | None = None
    sortmerna_ref: Path
    # used only when orient_primer; optional so a submit can omit it.
    primer: str = "GTGYCAGCMGCCGCGGTAA"
    trim: int
    orient_primer: bool = False
    sequenced_pool_idx: int
    sequencing_run_idx: int
    work_ticket_idx: int


# filter: trim, per-sample derep, SortMeRNA, drop thin samples.
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

# chimera filter before the MSA, then drop thin samples.
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

# per-sample MAFFT MSA, then deblur denoise.
_DENOISE_SQL = """
CREATE OR REPLACE TABLE aligned AS
SELECT am.sample_id, am.read_id, am.aligned_sequence, d.abundance
FROM align_mafft('alignable2', sample_id := 'sample_id') am JOIN alignable2 d USING (read_id);
CREATE OR REPLACE TABLE deblurred AS
SELECT sample_id, read_id, sequence AS sequence1, abundance
FROM deblur('aligned', sequence_col := 'aligned_sequence', sample_id := 'sample_id')
ORDER BY abundance DESC;
"""

# orient each read to the primer (only when orient_primer). match on upper(read)
# so mixed-case input still hits the uppercase primer regex.
_ORIENT_SQL = """
CREATE OR REPLACE TABLE _inputs_oriented AS
SELECT sample_id, sequence_index,
    CASE
      WHEN regexp_matches(upper(sequence1), getvariable('rapid_regex_fwd'))
           AND regexp_matches(upper(sequence1), getvariable('rapid_regex_fwd_rc')) THEN NULL
      WHEN regexp_matches(upper(sequence1), getvariable('rapid_regex_fwd'))
        THEN regexp_extract(upper(sequence1), getvariable('rapid_regex_fwd_extract'), 2)
      WHEN regexp_matches(upper(sequence1), getvariable('rapid_regex_fwd_rc'))
        THEN regexp_extract(sequence_dna_reverse_complement(upper(sequence1)),
                            getvariable('rapid_regex_fwd_extract'), 2)
      ELSE upper(sequence1)
    END AS sequence1
FROM _inputs;
"""


def _set_session_vars(conn, *, primer: str, trim: int, sortmerna_ref: Path, orient: bool) -> None:
    conn.execute(f"SET VARIABLE rapid_trim = {int(trim)};")
    # reject unsafe path characters (applies to the FASTA too).
    safe_ref = validate_parquet_path(sortmerna_ref)
    conn.execute(f"SET VARIABLE miint_sortmerna_ref = '{safe_ref}';")
    if orient:
        # validate, don't mangle; a bad primer must fail loud.
        upper = primer.upper()
        if not upper or set(upper) - _IUPAC_DNA:
            raise ValueError(f"primer is not valid IUPAC: {primer!r}")
        conn.execute(f"SET VARIABLE rapid_fwd_primer = '{upper}';")
        conn.execute(
            "SET VARIABLE rapid_regex_fwd = "
            "sequence_dna_as_regexp(getvariable('rapid_fwd_primer'));"
        )
        conn.execute(
            "SET VARIABLE rapid_regex_fwd_rc = sequence_dna_as_regexp("
            "sequence_dna_reverse_complement(getvariable('rapid_fwd_primer')));"
        )
        # capture the rest of the read after the primer. `.+` (not `[ATGC]+`) so an
        # ambiguity code or lowercase base does not truncate the captured sequence.
        conn.execute(
            "SET VARIABLE rapid_regex_fwd_extract = "
            "'(' || getvariable('rapid_regex_fwd') || ')(.+)';"
        )


async def execute(inputs: Inputs, workspace: Path) -> dict[str, Path]:
    """denoise the pool's reads into ASVs; emit the manifest and counts.
    StepNoData when nothing survives."""
    workspace = workspace.resolve()
    inputs.sortmerna_ref = inputs.sortmerna_ref.resolve()
    if not inputs.sortmerna_ref.exists():
        raise FileNotFoundError(f"amplicon_deblur input not found: {inputs.sortmerna_ref}")

    workspace.mkdir(parents=True, exist_ok=True)
    counts_out = workspace / "asv_counts.parquet"
    manifest_out = workspace / "manifest.parquet"
    chunks_dir = workspace / "asv_chunks"
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
            conn,
            primer=inputs.primer,
            trim=inputs.trim,
            sortmerna_ref=inputs.sortmerna_ref,
            orient=inputs.orient_primer,
        )
        # stream the pool's reads from the data plane (nothing staged).
        async with bind_step_reads(
            conn,
            reads=inputs.reads,
            work_ticket_idx=inputs.work_ticket_idx,
            workspace=duckdb_tmp,
        ) as reads_rel:
            # _inputs(sample_id, sequence_index, sequence1); sample_id is prep_sample_idx.
            conn.execute(
                "CREATE OR REPLACE VIEW _inputs AS "
                "SELECT prep_sample_idx AS sample_id, sequence_idx AS sequence_index, sequence1 "
                f"FROM {reads_rel}"
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

            # one representative sequence per canonical hash, so the ASV bytes are
            # stored (recovering them means re-running deblur). a sequence and its
            # revcomp share a hash; pick the lex-smaller bytes deterministically.
            conn.execute(
                f"CREATE OR REPLACE TABLE asv_seq AS "
                f"SELECT DISTINCT ON (sequence_hash) sequence_hash, sequence1, "
                f"       length(sequence1)::BIGINT AS sequence_length_bp "
                f"FROM (SELECT {canon} AS sequence_hash, sequence1 FROM deblurred) "
                f"ORDER BY sequence_hash, sequence1"
            )

            _write_outputs(
                conn, counts_out=counts_out, manifest_out=manifest_out, chunks_dir=chunks_dir
            )

    return {"asv_counts": counts_out, "manifest": manifest_out, "asv_chunks": chunks_dir}


def _write_outputs(conn, *, counts_out: Path, manifest_out: Path, chunks_dir: Path) -> None:
    """publish the three outputs atomically: per-sample counts, the mint-features
    manifest (read_id, sequence_hash, sequence_length_bp), and a hash-keyed chunk
    directory carrying each ASV's bytes. write to `.partial`, then rename in."""
    counts_partial = counts_out.parent / f"{counts_out.name}.partial"
    manifest_partial = manifest_out.parent / f"{manifest_out.name}.partial"
    chunks_partial = chunks_dir.parent / f"{chunks_dir.name}.partial"
    safe_counts = validate_parquet_path(counts_partial)
    safe_manifest = validate_parquet_path(manifest_partial)
    chunks_partial.mkdir(parents=True, exist_ok=True)
    chunk_part = validate_parquet_path(chunks_partial / "part_00000.parquet")
    try:
        conn.execute(
            f"COPY (SELECT prep_sample_idx, sequence_hash, count FROM asv "
            f"ORDER BY prep_sample_idx, sequence_hash) TO '{safe_counts}' ({PARQUET_OPTS})"
        )
        conn.execute(
            f"COPY (SELECT sequence_hash::VARCHAR AS read_id, sequence_hash, sequence_length_bp "
            f"FROM asv_seq ORDER BY sequence_hash) TO '{safe_manifest}' ({PARQUET_OPTS})"
        )
        conn.execute(
            f"COPY (SELECT sequence_hash, c.chunk_index, c.chunk_data FROM ("
            f"  SELECT sequence_hash, UNNEST({sequence_split_expr('sequence1')}) AS c FROM asv_seq"
            f") ORDER BY sequence_hash, c.chunk_index) TO '{chunk_part}' ({PARQUET_OPTS_CHUNKED})"
        )
        os.replace(counts_partial, counts_out)
        os.replace(manifest_partial, manifest_out)
        shutil.rmtree(chunks_dir, ignore_errors=True)  # a re-run may leave a dir
        os.replace(chunks_partial, chunks_dir)
    finally:
        counts_partial.unlink(missing_ok=True)
        manifest_partial.unlink(missing_ok=True)
        shutil.rmtree(chunks_partial, ignore_errors=True)
