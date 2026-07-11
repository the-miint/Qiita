"""Native job: mark SynDNA spike-in reads. FIRST step of the read-mask chain.

`read.parquet -> syndna_mask.parquet`. Emits a PARTIAL mask (the 6-column
`(sequence_idx, reason, left_trim1, right_trim1, left_trim2, right_trim2)` shape
qc / lima_mask emit), NOT the final `read_mask`.

**Why first — and this is a bug fix.** In case 5 (`syndna_is_twisted == False`)
the SynDNA spike-ins are added AFTER Twist amplification, so they carry no Twist
adaptor. If lima ran first it would find no adaptor on a spike-in read and mark it
`twist_no_adaptor`; every later step (including syndna) only re-classifies rows
still `pass`, so syndna would never see the spike-in and its count would be
STRUCTURALLY zero. Running syndna first — on the RAW reads, before lima can drop
anything — marks the spike-ins up front. lima then processes only the still-`pass`
(biological) reads, which all legitimately carry the adaptor, so `twist_no_adaptor`
becomes a correct "artifactual" signal.

The mask threads forward as a single `partial_mask` binding: syndna emits it, the
lima chain and qc each consume it (only rows still `pass` are re-classified; every
non-`pass` row is carried verbatim via `ELSE reason`), and `host_filter` folds it
into the final `read_mask`. So a `spikein_syndna` mark set here survives untouched
to the end — no step overwrites an earlier verdict.

**Classifies the RAW read.** As the first step there is no incoming mask and no
trimming yet, so rype sees `sequence1` directly. Trims are all zero (SynDNA does
not trim); the mate-trim columns follow the read_mask convention (NULL for
single-end, 0 for paired) so both-mates counting stays correct downstream.

`spikein_syndna` is not biological — a spike-in is added in the lab. It is
excluded from `read_masked` (which serves only `pass`) and gets its own count
bucket, with its rows RETAINED in `read_mask` so the counts survive.

Shares `host_filter`'s classify seam (`_rype.run_rype_classify` — DISTINCT,
BIGINT accumulator for rype's build-dependent id type): the question is the same
boolean "does this read match the index?".
"""

from __future__ import annotations

from pathlib import Path

from pydantic import BaseModel
from qiita_common.models import ReadMaskReason
from qiita_common.parquet import validate_parquet_path

from ..miint import (
    PARQUET_OPTS,
    apply_duckdb_settings,
    duckdb_tmp_dir,
    open_miint_conn,
)
from ._rype import run_rype_classify as _run_rype_classify

YAML_STEP_NAME = "syndna"

# DuckDB gets a FIXED modest share and is deliberately NOT allocation-aware here:
# rype's index lives OUT of DuckDB's heap, so growing DuckDB with the cgroup would
# starve it. Same reasoning (and same numbers) as host_filter — the right lever for
# a bigger classify is the cgroup (YAML mem_gb), which reaches rype directly.
_DUCKDB_MEMORY_GB = 8
_DUCKDB_THREADS = 4

# "spike-in = any emitted row", mirroring host_filter's aggressive depletion. A
# SynDNA spike-in is a synthetic sequence with no biological counterpart, so any
# minimizer match identifies it. Explicit, NOT rype's 0.1 default.
#
# The failure mode is asymmetric and worth stating: a FALSE POSITIVE both removes a
# real read from `biological` and inflates the spike-in count that the cell-count
# model divides by. Pinned by the smoke test; revisit with the assay owner if real
# data shows biological reads matching the spike-in index.
_RYPE_THRESHOLD = 0.0

# In-DuckDB relation names. The reads are a VIEW (both the query and the final COPY
# read them); the hit set is a TABLE (rype's `read_id` output type is
# build-dependent, so a pre-declared BIGINT column coerces it on insert — see
# `_rype.run_rype_classify`).
_READS = "syndna_reads"
_QUERY = "syndna_query"
_HITS = "syndna_hits"


class Inputs(BaseModel):
    """Typed input contract for syndna.

    First step of the chain, so it takes only the raw `reads` and the spike-in
    reference. `syndna_rype_path` is the `.ryxdi`, bound by the runner only when
    `syndna_enabled` — and the step runs under that same gate, so it is REQUIRED
    (an unbound index would mean the gate and the binding disagree).
    """

    reads: Path
    syndna_rype_path: Path
    prep_sample_idx: int | None = None
    work_ticket_idx: int


def _validate_rype_index(path: Path) -> None:
    """A rype index is a `.ryxdi` DIRECTORY; reject a missing one (fail fast) and
    an empty one (no index content -> a silent no-op classify, which would report
    zero spike-ins for a sample that has them)."""
    if not path.exists():
        raise FileNotFoundError(f"syndna_rype_path not found: {path}")
    if path.is_dir() and not any(path.iterdir()):
        raise ValueError(f"syndna_rype_path is an empty directory: {path}")


async def execute(inputs: Inputs, workspace: Path) -> dict[str, Path]:
    if not inputs.reads.exists():
        raise FileNotFoundError(f"reads parquet not found: {inputs.reads}")
    _validate_rype_index(inputs.syndna_rype_path)

    workspace.mkdir(parents=True, exist_ok=True)
    partial_mask = workspace / "syndna_mask.parquet"

    reads_sql = validate_parquet_path(inputs.reads)
    out_sql = validate_parquet_path(partial_mask)

    success = False
    try:
        with duckdb_tmp_dir(workspace) as duckdb_tmp, open_miint_conn() as conn:
            apply_duckdb_settings(
                conn, duckdb_tmp, memory_gb=_DUCKDB_MEMORY_GB, threads=_DUCKDB_THREADS
            )
            # No incoming mask and no trimming yet: rype classifies the raw read.
            conn.execute(f"CREATE VIEW {_READS} AS SELECT * FROM read_parquet('{reads_sql}')")
            conn.execute(
                f"CREATE VIEW {_QUERY} AS "
                f"SELECT sequence_idx AS read_id, sequence1, sequence2 FROM {_READS}"
            )
            conn.execute(f"CREATE TABLE {_HITS} (sequence_idx BIGINT)")
            _run_rype_classify(
                conn, inputs.syndna_rype_path, _QUERY, _HITS, threshold=_RYPE_THRESHOLD
            )

            # Emit the partial mask: one row per read, spike-in hits marked, all
            # else `pass`. Trims are zero (SynDNA does not trim); mate-trim columns
            # follow the read_mask convention (NULL single-end, 0 paired) so the
            # both-mates count(right_trim2) stays correct downstream.
            conn.execute(
                "COPY (SELECT r.sequence_idx, "
                f"        CASE WHEN h.sequence_idx IS NOT NULL "
                f"             THEN '{ReadMaskReason.SPIKEIN_SYNDNA.value}' "
                f"             ELSE '{ReadMaskReason.PASS.value}' END AS reason, "
                "        0::UINTEGER AS left_trim1, 0::UINTEGER AS right_trim1, "
                "        CASE WHEN r.sequence2 IS NULL THEN NULL ELSE 0 END::UINTEGER "
                "          AS left_trim2, "
                "        CASE WHEN r.sequence2 IS NULL THEN NULL ELSE 0 END::UINTEGER "
                "          AS right_trim2 "
                f"      FROM {_READS} r LEFT JOIN {_HITS} h USING (sequence_idx) "
                "      ORDER BY sequence_idx) "
                f"TO '{out_sql}' ({PARQUET_OPTS})"
            )
        success = True
    finally:
        if not success:
            partial_mask.unlink(missing_ok=True)

    # The partial mask threaded forward to the lima chain / qc under one binding
    # (see the module docstring). NOT the final read_mask — host_filter emits that.
    return {"partial_mask": partial_mask}
