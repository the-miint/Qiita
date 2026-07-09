"""Native job: mark SynDNA spike-in reads, extending the read mask.

`(read.parquet, read_mask.parquet) -> read_mask.parquet`.
Runs LAST in the read-mask chain, AFTER host_filter. Structurally an adaptation of
`host_filter`: classify the still-`pass` reads against a rype index, then merge the
hits into the incoming mask with a reason that falls through to `ELSE m.reason`.

**Why last.** Spike-ins are synthetic and do not align to the host, so host
filtering never removes them. Counting them in the QC'd, host-depleted space is the
correct denominator for the downstream total-cells calculation.

**Why `spikein_syndna` is not biological.** A spike-in is added in the lab; it is
not a molecule from the sample. It gets its own reason (so `read_masked`, which
serves only `pass`, excludes it) and its own count bucket, disjoint from
`biological`. Its rows are RETAINED in `read_mask` — the counts survive.

**Per-spike-in counts are NOT emitted here.** The question this step answers is
boolean — "is this read a spike-in?" — so it shares `host_filter`'s classify seam
(`_rype.run_rype_classify`). Attributing each read to a specific spike-in needs a
durable per-`(prep_sample, feature_idx)` table and a register step, neither of
which exists yet; emitting an unregistered parquet into an ephemeral workspace
would spare nothing. Nothing is lost in the meantime: `read_mask` retains every
spike-in read's `sequence_idx`, so a later step can re-classify exactly those rows
against the syndna reference. `build_rype_index --bucket-per-feature` exists so the
index is already built for that day and needs no rebuild.
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

# In-DuckDB relation names. The incoming mask is a VIEW (both the query view and
# the final COPY read it); the hit set is a TABLE (rype's `read_id` output type is
# build-dependent, so a pre-declared BIGINT column coerces it on insert — see
# `_rype.run_rype_classify`).
_MASK = "syndna_mask"
_QUERY = "syndna_query"
_HITS = "syndna_hits"

# A still-`pass` read's trimmed sequence — the same substr math the read_masked
# view applies. `r` is the read alias, `m` the mask alias. Single-end only
# (spike-ins ride the long-read protocols), but sequence2 is carried so a future
# short-read absquant needs no change here: rype reads it when present.
_TRIM_SEQ1 = (
    "substr(r.sequence1, m.left_trim1 + 1, length(r.sequence1) - m.left_trim1 - m.right_trim1)"
)
_TRIM_SEQ2 = (
    "CASE WHEN r.sequence2 IS NULL THEN NULL ELSE "
    "substr(r.sequence2, m.left_trim2 + 1, "
    "length(r.sequence2) - m.left_trim2 - m.right_trim2) END"
)


class Inputs(BaseModel):
    """Typed input contract for syndna.

    `read_mask` is host_filter's output (the 8-column mask carrying `mask_idx` and
    `prep_sample_idx`); this step extends it rather than re-deriving it.
    `syndna_rype_path` is the spike-in reference's `.ryxdi`, bound by the runner
    only when `syndna_enabled` — and the step itself only runs under that same
    gate, so it is REQUIRED here rather than optional: an unbound index would mean
    the gate and the binding disagree.
    """

    reads: Path
    read_mask: Path
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
    if not inputs.read_mask.exists():
        raise FileNotFoundError(f"read_mask parquet not found: {inputs.read_mask}")
    _validate_rype_index(inputs.syndna_rype_path)

    workspace.mkdir(parents=True, exist_ok=True)
    # `register-files` globs EVERY *.parquet in the staging dir it is handed, so the
    # emitted mask gets its own subdir — nothing else may land beside it.
    staging_dir = workspace / "read_mask"
    staging_dir.mkdir(parents=True, exist_ok=True)
    read_mask = staging_dir / "read_mask.parquet"

    reads_sql = validate_parquet_path(inputs.reads)
    mask_sql = validate_parquet_path(inputs.read_mask)
    out_sql = validate_parquet_path(read_mask)

    success = False
    try:
        with duckdb_tmp_dir(workspace) as duckdb_tmp, open_miint_conn() as conn:
            apply_duckdb_settings(
                conn, duckdb_tmp, memory_gb=_DUCKDB_MEMORY_GB, threads=_DUCKDB_THREADS
            )
            conn.execute(f"CREATE VIEW {_MASK} AS SELECT * FROM read_parquet('{mask_sql}')")
            # Classify ONLY the still-`pass` reads, on their trimmed sequence. A read
            # already marked qc_*/host_*/twist_no_adaptor keeps that verdict: the
            # earlier step's reason is never overwritten (see the CASE below).
            conn.execute(
                f"CREATE VIEW {_QUERY} AS "
                "SELECT r.sequence_idx AS read_id, "
                f"{_TRIM_SEQ1} AS sequence1, {_TRIM_SEQ2} AS sequence2 "
                f"FROM read_parquet('{reads_sql}') r "
                f"JOIN {_MASK} m USING (sequence_idx) "
                f"WHERE m.reason = '{ReadMaskReason.PASS.value}'"
            )
            conn.execute(f"CREATE TABLE {_HITS} (sequence_idx BIGINT)")
            _run_rype_classify(
                conn, inputs.syndna_rype_path, _QUERY, _HITS, threshold=_RYPE_THRESHOLD
            )

            # Extend the mask. `ELSE m.reason` is the fall-through every step in the
            # chain shares — a spike-in hit can only ever override `pass`, because
            # the query view saw nothing else.
            conn.execute(
                "COPY (SELECT m.mask_idx, m.prep_sample_idx, m.sequence_idx, "
                f"        CASE WHEN h.sequence_idx IS NOT NULL "
                f"             THEN '{ReadMaskReason.SPIKEIN_SYNDNA.value}' "
                "             ELSE m.reason END AS reason, "
                "        m.left_trim1, m.right_trim1, m.left_trim2, m.right_trim2 "
                f"      FROM {_MASK} m LEFT JOIN {_HITS} h USING (sequence_idx) "
                "      ORDER BY mask_idx, prep_sample_idx, sequence_idx) "
                f"TO '{out_sql}' ({PARQUET_OPTS})"
            )
        success = True
    finally:
        if not success:
            read_mask.unlink(missing_ok=True)

    # Same binding names host_filter emits: when syndna runs it SHADOWS them, so
    # persist-read-metrics and register-files consume the extended mask; when it is
    # skipped, host_filter's bindings stand.
    return {"read_mask": read_mask, "read_mask_staging_dir": staging_dir}
