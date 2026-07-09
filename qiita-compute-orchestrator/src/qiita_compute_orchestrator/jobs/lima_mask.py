"""Native job: turn lima's clipped FASTQ into a partial read mask.

`(read.parquet, lima_out.fastq) -> adapter_mask.parquet`. Last entry of the
long-read adapter chain (`lima_export -> lima -> lima_mask`); its output is the
optional `adapter_mask` the `qc` step consumes, which then extends the trims
cumulatively (see jobs/qc.py).

**Trims come from miint's `infer_trim`, not from parsing lima.** The macro takes
the original and the clipped relations, joins them on `sequence_index`, locates
the clipped sequence inside the original, and returns `(sequence_index,
trimmed_5p, trimmed_3p)` — one row per ORIGINAL read, with `NULL/NULL` for a read
the tool omitted. It fails loud if a kept read is not a contiguous substring of its
original (i.e. the tool edited internal bases rather than end-trimming). lima is a
pure end-trimmer, so that contract holds; do not suppress the failure.

**The join key is the FASTQ record name.** `lima_export` wrote `sequence_idx` as
the record name and lima preserves it verbatim (appending its BAM tags after a
space, which `read_fastx` parses into `comment`). `read_fastx`'s own
`sequence_index` is POSITIONAL and resets per file, so it is NOT our
`sequence_idx` — the key is recovered as `CAST(read_id AS BIGINT)`.

**Reads lima dropped become `twist_no_adaptor`.** A HiFi read carrying no Twist
adaptor is not a library molecule from this run — it is artifactual. It is masked
out (excluded from `read_masked`, which serves only `pass`) and, per the mask's
bucket whitelist, counts toward `raw` only.

An empty lima output — every read failed adapter detection — is a legitimate
outcome, not an error: `infer_trim`'s LEFT JOIN yields `NULL/NULL` for every read
and the mask is all `twist_no_adaptor`. `read_fastx` REJECTS an empty file
(`Error: Empty file`), so that case is routed around it with an empty typed
relation rather than allowed to crash the step.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
from pydantic import BaseModel
from qiita_common.duckdb_miint import is_empty_sequence_file
from qiita_common.models import ReadMaskReason
from qiita_common.parquet import validate_parquet_path

from ..miint import (
    PARQUET_OPTS,
    apply_duckdb_settings,
    duckdb_tmp_dir,
    open_miint_conn,
    resolve_duckdb_memory_gb,
)

YAML_STEP_NAME = "lima_mask"

# Off-SLURM fallback cap; under SLURM the real cap is sized to the cgroup.
# `infer_trim` materializes both the original and clipped sequence sets and runs a
# per-row substring search, so unlike qc this step's footprint scales with total
# SEQUENCE BYTES (millions of ~10 kb HiFi reads), not row count alone. Size the
# SLURM allocation accordingly; this constant is only the off-SLURM floor.
_DUCKDB_MEMORY_GB = 8
_DUCKDB_THREADS = 4

# In-DuckDB relation names.
_ORIG = "lima_orig"
_QCD = "lima_qcd"


class Inputs(BaseModel):
    """Typed input contract for lima_mask.

    `reads` is the raw `read.parquet` lima_export exported; `lima_out_fastq` is
    lima's clipped output. `prep_sample_idx` is accepted but unused — the mask keys
    on the globally-unique `sequence_idx`, so a BLOCK-scoped ticket (which flows no
    such scalar) validates against the same shape.
    """

    reads: Path
    lima_out_fastq: Path
    prep_sample_idx: int | None = None
    work_ticket_idx: int


def _assert_lima_reads_are_known(conn: duckdb.DuckDBPyConnection, lima_out_fastq: Path) -> None:
    """Every record lima emitted must correspond to an input read.

    `infer_trim` LEFT JOINs original→clipped, so an UNKNOWN clipped key is silently
    dropped rather than erroring — the mask would still be complete, but a stale or
    mismatched lima output would pass unnoticed. A duplicate key is worse: it fans
    the join out and emits two mask rows for one read. Assert neither."""
    (unknown,) = conn.execute(
        f"SELECT count(*) FROM {_QCD} q ANTI JOIN {_ORIG} o USING (sequence_index)"
    ).fetchone()
    if unknown:
        raise ValueError(
            f"lima output ({lima_out_fastq}) has {unknown} record(s) whose name is not "
            "a sequence_idx of the input reads; the FASTQ round-trip is broken"
        )
    n, distinct = conn.execute(
        f"SELECT count(*), count(DISTINCT sequence_index) FROM {_QCD}"
    ).fetchone()
    if n != distinct:
        raise ValueError(
            f"lima output ({lima_out_fastq}) has {n - distinct} duplicate record name(s); "
            "infer_trim would fan the join out and emit two mask rows for one read"
        )


async def execute(inputs: Inputs, workspace: Path) -> dict[str, Path]:
    if not inputs.reads.exists():
        raise FileNotFoundError(f"reads parquet not found: {inputs.reads}")
    if not inputs.lima_out_fastq.exists():
        raise FileNotFoundError(f"lima_out_fastq not found: {inputs.lima_out_fastq}")

    workspace.mkdir(parents=True, exist_ok=True)
    adapter_mask = workspace / "adapter_mask.parquet"

    reads_sql = validate_parquet_path(inputs.reads)
    lima_sql = validate_parquet_path(inputs.lima_out_fastq)
    out_sql = validate_parquet_path(adapter_mask)

    success = False
    try:
        with duckdb_tmp_dir(workspace) as duckdb_tmp, open_miint_conn() as conn:
            apply_duckdb_settings(
                conn,
                duckdb_tmp,
                memory_gb=resolve_duckdb_memory_gb(_DUCKDB_MEMORY_GB, threads=_DUCKDB_THREADS),
                threads=_DUCKDB_THREADS,
            )
            # infer_trim requires both relations to expose `sequence_index` +
            # `sequence`. On the clipped side the key is the round-tripped FASTQ
            # record NAME (`read_id`), never read_fastx's positional
            # `sequence_index`.
            conn.execute(
                f"CREATE VIEW {_ORIG} AS "
                "SELECT sequence_idx AS sequence_index, sequence1 AS sequence "
                f"FROM read_parquet('{reads_sql}')"
            )
            if is_empty_sequence_file(inputs.lima_out_fastq):
                # lima clipped nothing through: every read failed adapter detection.
                # A legitimate all-`twist_no_adaptor` mask — but `read_fastx` raises
                # `Empty file` rather than returning zero rows, so hand infer_trim an
                # empty typed relation instead.
                conn.execute(
                    f"CREATE VIEW {_QCD} AS "
                    "SELECT NULL::BIGINT AS sequence_index, NULL::VARCHAR AS sequence "
                    "WHERE false"
                )
            else:
                conn.execute(
                    f"CREATE VIEW {_QCD} AS "
                    "SELECT CAST(read_id AS BIGINT) AS sequence_index, sequence1 AS sequence "
                    f"FROM read_fastx('{lima_sql}')"
                )
            _assert_lima_reads_are_known(conn, inputs.lima_out_fastq)
            # One row per ORIGINAL read. `infer_trim` returns NULL/NULL for a read
            # lima omitted -> twist_no_adaptor with zero trims (nothing was clipped;
            # the whole read is masked out regardless). PacBio is single-end, so the
            # mate trims are NULL, matching the qc_mask shape qc consumes.
            no_adaptor = ReadMaskReason.TWIST_NO_ADAPTOR.value
            conn.execute(
                "COPY (SELECT sequence_index AS sequence_idx, "
                f"        CASE WHEN trimmed_5p IS NULL THEN '{no_adaptor}' "
                f"             ELSE '{ReadMaskReason.PASS.value}' END AS reason, "
                "        coalesce(trimmed_5p, 0)::UINTEGER AS left_trim1, "
                "        coalesce(trimmed_3p, 0)::UINTEGER AS right_trim1, "
                "        NULL::UINTEGER AS left_trim2, "
                "        NULL::UINTEGER AS right_trim2 "
                f"      FROM infer_trim({_ORIG}, {_QCD}) ORDER BY sequence_idx) "
                f"TO '{out_sql}' ({PARQUET_OPTS})"
            )
        success = True
    finally:
        if not success:
            adapter_mask.unlink(missing_ok=True)

    return {"adapter_mask": adapter_mask}
