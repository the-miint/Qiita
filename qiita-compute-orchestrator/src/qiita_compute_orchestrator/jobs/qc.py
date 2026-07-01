"""Native job: fastp-equivalent read QC, emitting a partial read mask.

A `read.parquet -> qc_mask.parquet` transform keyed by the already-minted
`sequence_idx`. Minting happened upstream in `fastq_to_parquet`; this step does
NOT drop or rewrite reads — the full reads live once in the DuckLake `read`
table, never physically filtered. Instead it records, per read, whether the read
survives QC and how it should be trimmed: one `qc_mask.parquet` row per
`sequence_idx` with `(reason, left_trim1, right_trim1, left_trim2, right_trim2)`.
Runs BEFORE `host_filter` in the bcl-convert pipeline (`fastq` -> `qc` ->
`host_filter`); `host_filter` merges its host hits into this partial mask and
emits the final `read_mask`.

Per-read QC chain (miint's fastp algorithm port — see docs/duckdb-miint.md and
tests/jobs/test_qc_miint_contract.py for the pinned contracts):

  1. **adapter trim** — SE: `trim_adapters(seq, qual, adapters)`; PE:
     `trim_adapters_pe(s1, q1, s2, q2, adapters, <overlap defaults>)` (overlap
     analysis first, then the known-adapter fallback the non-empty `adapters`
     list adds without changing overlap behavior);
  2. **polyG trim** (optional) — `trim_polyg(seq, qual)`, which removes a 3'
     G-run ONLY when its quality is low (2-color no-signal). fastp enables polyG
     only on 2-color instruments, so we gate it on `instrument_model`
     (NextSeq/NovaSeq/MiniSeq) — defaulting OFF when the model is unknown (e.g. a
     non-bcl upload);
  3. **length/quality filter** — `filter_read(seq, qual, 100, 0, 15, 40, 5, 0)`
     == fastp defaults with `-l 100`. A read failing this is NOT dropped — it is
     recorded with the matching `qc_*` reason so the `read_masked` view excludes
     it while raw `read` retains it.

**Reason mapping** (filter_read `fail_reason` -> ReadMaskReason): `length` ->
`qc_too_short`, `too_long` -> `qc_too_long`, `n_base` -> `qc_too_many_n`,
`quality` -> `qc_low_quality`; a passing read is `pass`. `filter_read` runs on
the TRIMMED sequence, so a read whose post-trim length is below min_length is
`qc_too_short` by construction — the trim-length invariant the `read_masked`
view relies on (a `pass` read's `left_trim + right_trim <= length`).

**Trims are the cumulative bases removed from each end** (adapter + polyG),
recorded even when the read fails QC so an admin reading raw `read` can
reconstruct; the masked path drops the row regardless.

**Paired-end trim shape.** SE `trim_adapters` returns `trimmed_5p` and
`trimmed_3p`, so SE populates `left_trim1` (5') and `right_trim1` (3'). PE
`trim_adapters_pe` is 3'-only (no 5' output) and `trim_polyg` trims only the 3'
end, so for PE `left_trim1`/`left_trim2` are structurally 0, `right_trim1 =
trimmed1_3p (+ polyG)`, `right_trim2 = trimmed2_3p (+ polyG)`. The four-column
schema is uniform; SE leaves `left_trim2`/`right_trim2` NULL (no mate).

**Paired-end pass/fail.** A PE row passes only when BOTH mates pass
`filter_read`; if either mate fails, the pair's reason is that mate's failure
(R1 checked first). Single-end rows (`sequence2 IS NULL`) take the SE chain. The
two layouts are routed to separate seams so each runs the right miint overload.

Adapters: the canonical adapter set is materialized by the runner
(`_resolve_qc_adapters`) into the bound `adapter_parquet` (a one-`sequence`-column
Parquet), read here with `read_parquet` — the same columnar format the rest of
the pipeline uses, so no FASTA parsing — and rendered into a constant SQL `VARCHAR[]`
(miint's QC functions require bind-time constants — the adapter list cannot be a
column/parameter). QC is always-on in this path, so `adapter_parquet` is a
REQUIRED input and an empty one is fail-fast.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
from pydantic import BaseModel
from qiita_common.models import ReadMaskReason
from qiita_common.parquet import validate_parquet_path

from ..job_resource_plan import count_read_pairs, linear_walltime
from ..miint import (
    PARQUET_OPTS,
    apply_duckdb_settings,
    duckdb_tmp_dir,
    open_miint_conn,
    resolve_duckdb_memory_gb,
)
from . import JobPlan, JobResourcePlan

YAML_STEP_NAME = "qc"

# DuckDB resource caps for this step. QC is a per-row scalar transform plus the
# final sorted COPY — no out-of-heap co-consumer (unlike host_filter's rype /
# minimap2 runtimes, which is why host_filter deliberately does NOT do this), so
# the whole footprint IS DuckDB's. Hence `_DUCKDB_MEMORY_GB` is only the
# OFF-SLURM fallback cap: under SLURM the real cap is sized to the cgroup via
# `resolve_duckdb_memory_gb()` (SLURM_MEM_PER_NODE) so a per-run `--mem-gb`
# override reaches DuckDB's `memory_limit` — the same allocation-aware pattern
# stage_local_fasta / hash_sequences use.
_DUCKDB_MEMORY_GB = 8
_DUCKDB_THREADS = 4

# fastp's `-l 100`: drop a read shorter than 100 nt AFTER trimming. The 1st int
# positional arg to filter_read. Pinned by test_qc_miint_contract.
_MIN_LENGTH = 100

# The fastp defaults for filter_read's remaining positionals
# (max_length, qualified_q, max_unqualified_pct, max_n, min_avg_q) — equal to the
# 2-arg form's implicit defaults, so the only knob we override is min_length.
# Full call: filter_read(seq, qual, _MIN_LENGTH, _FILTER_READ_TAIL).
_FILTER_READ_TAIL = "0, 15, 40, 5, 0"

# fastp's overlap defaults for trim_adapters_pe
# (overlap_require, overlap_diff_limit, overlap_diff_percent_limit, match_revcomp,
# min_match, allow_pre_start). The 11-arg form with these + a non-empty adapter
# list keeps overlap behavior identical to the 4-arg overload while adding the
# by-sequence adapter fallback. Pinned by test_qc_miint_contract.
_TRIM_PE_OVERLAP_DEFAULTS = "30, 5, 20, false, 0, false"

# Instrument-model substrings that mark a 2-color (no-signal-is-G) chemistry, for
# which fastp (and so this job) enables polyG trimming. Matched case-insensitively
# as substrings of `instrument_model` (which is persisted on qiita.sequencing_run
# and forwarded per sample; None for non-bcl uploads -> polyG OFF).
_TWO_COLOR_MODEL_SUBSTRINGS = ("nextseq", "novaseq", "miniseq")

# In-DuckDB relation names for the SE/PE source views. The QC functions are
# SCALAR, evaluated inline on this connection, so a regular view is the simplest
# named relation each seam can target. There is no output accumulator table: the
# two seams' SELECTs are UNION ALL'd straight into the final COPY so DuckDB
# streams the whole transform to Parquet without materialising it.
_SE = "qc_se"
_PE = "qc_pe"

# SQL CASE that maps a filter_read result struct to a ReadMaskReason value.
# `f` is the alias of the filter_read STRUCT in the enclosing query. A passing
# read is 'pass'; otherwise the fail_reason ('length'/'too_long'/'n_base'/
# 'quality') maps to the matching qc_* reason. fail_reason is documented to be
# one of those four when not passed, so the ELSE ('quality') is the residual.
_SE_REASON_CASE = (
    "CASE "
    f"WHEN f['passed'] THEN '{ReadMaskReason.PASS.value}' "
    f"WHEN f['fail_reason'] = 'length' THEN '{ReadMaskReason.QC_TOO_SHORT.value}' "
    f"WHEN f['fail_reason'] = 'too_long' THEN '{ReadMaskReason.QC_TOO_LONG.value}' "
    f"WHEN f['fail_reason'] = 'n_base' THEN '{ReadMaskReason.QC_TOO_MANY_N.value}' "
    f"ELSE '{ReadMaskReason.QC_LOW_QUALITY.value}' "
    "END"
)


class Inputs(BaseModel):
    """Typed input contract for qc.

    `reads` is fastq_to_parquet's `read.parquet` (binding name `reads`):
    `(prep_sample_idx, sequence_idx, read_id, sequence1, qual1, sequence2, qual2)`.
    `adapter_parquet` is the canonical adapter set the runner materializes
    (`_resolve_qc_adapters`) — REQUIRED (QC is always-on; an empty set is a
    misconfiguration). `instrument_model` gates polyG trimming (None -> OFF);
    it is forwarded from qiita.sequencing_run per sample. `prep_sample_idx` /
    `work_ticket_idx` are the framework-injected scope scalars.
    """

    reads: Path
    adapter_parquet: Path
    instrument_model: str | None = None
    prep_sample_idx: int
    work_ticket_idx: int


# plan() walltime model. qc STREAMS: a per-row scalar transform whose only
# blocking operator is the final ORDER BY on the NARROW mask (~40 B/row), which
# DuckDB spills past memory_limit. So peak RAM is ~flat in read count (bounded
# by the operator working set + memory_limit) — NOT a plan()-from-input axis —
# while runtime scales ~linearly with rows at a roughly constant throughput.
# Hence we size WALLTIME, not memory: a small sample finishes well inside the
# YAML baseline walltime, so requesting less improves SLURM backfill. These are
# conservative INITIAL coefficients to refine against telemetry; the CP clamps
# the hint down-only (never above the baseline) and TIMEOUT escalation is the
# backstop, so erring low costs a retry, not correctness.
_PLAN_BASE_WALLTIME_SECONDS = 300  # 5 min: process + DuckDB init + fixed read/scan/write overhead
_PLAN_WALLTIME_SECONDS_PER_MILLION_PAIRS = 30.0


def plan(inputs: Inputs) -> JobPlan:
    """Size qc's WALLTIME from the read count (Parquet footer, no data scan).

    Returns a walltime hint only — memory and cpu are left to the YAML baseline.
    qc streams, so its peak memory is ~flat in row count (see the coefficient
    comment above); walltime is the axis that tracks input cardinality. The
    control plane lowers the step's walltime to this value when it is below the
    baseline (a small input) and never raises above it; TIMEOUT escalation
    covers an under-estimate. Advisory: any failure here (e.g. an unreadable
    input) is caught upstream in the /step/plan route, which falls back to the
    baseline."""
    read_pairs = count_read_pairs(inputs.reads)
    walltime = linear_walltime(
        read_pairs,
        base_seconds=_PLAN_BASE_WALLTIME_SECONDS,
        seconds_per_million_pairs=_PLAN_WALLTIME_SECONDS_PER_MILLION_PAIRS,
    )
    return JobPlan(resources=JobResourcePlan(walltime=walltime))


def _is_two_color(instrument_model: str | None) -> bool:
    """True iff `instrument_model` names a 2-color-chemistry instrument (polyG
    applies). Case-insensitive substring match; None/empty -> False (polyG OFF)."""
    if not instrument_model:
        return False
    model = instrument_model.lower()
    return any(sub in model for sub in _TWO_COLOR_MODEL_SUBSTRINGS)


def _read_adapter_parquet(conn: duckdb.DuckDBPyConnection, path: Path) -> list[str]:
    """Read adapter sequences from the runner-staged Parquet (one row per record,
    column `sequence`) via `read_parquet` — the same columnar format the rest of
    the pipeline uses, so no FASTA parsing here.

    Raises ValueError when the set is empty or unreadable — an adapter reference
    with no sequences (or an unreadable file) is a misconfiguration, not a valid
    QC input. A read failure surfaces as a duckdb.Error, which we re-raise as
    ValueError (mapped to BAD_INPUT by the framework dispatcher); catching the
    exception TYPE, not its wording, keeps this robust to a future message change.
    The runner guarantees >=1 sequence, but the empty guard stays so a
    hand-staged file can't slip an empty adapter list past QC."""
    try:
        rows = conn.execute("SELECT sequence FROM read_parquet(?)", [str(path)]).fetchall()
    except duckdb.Error as exc:
        raise ValueError(f"adapter_parquet could not be read: {path}: {exc}") from exc
    adapters = [r[0] for r in rows]
    if not adapters:
        raise ValueError(f"adapter_parquet contains no sequences: {path}")
    return adapters


def _adapters_sql(adapters: list[str]) -> str:
    """Render the adapter list as a constant SQL `VARCHAR[]` literal (miint's QC
    functions require a bind-time constant adapter set, not a column/parameter).
    Single quotes are escaped — adapters are DNA from a reference DB, but the
    escape keeps this injection-safe regardless."""
    elements = ", ".join("'" + a.replace("'", "''") + "'" for a in adapters)
    return f"[{elements}]::VARCHAR[]"


def _qc_se_select(
    src_view: str,
    *,
    adapters_sql: str,
    apply_polyg: bool,
) -> str:
    """Build the single-end QC mask SELECT: adapter trim -> optional polyG ->
    length/quality filter, projected to the mask schema
    `(sequence_idx, reason, left_trim1, right_trim1, left_trim2, right_trim2)`.
    Returns SQL (no execution) so the SE and PE seams can be UNION ALL'd into one
    streaming COPY. Isolated so unit tests assert the generated SQL.

    SE trims are cumulative per end: `left_trim1 = trim_adapters.trimmed_5p`
    (polyG is 3'-only, never adds to the left), `right_trim1 = trim_adapters
    .trimmed_3p + trim_polyg.trimmed_3p` (when polyG applies, else just adapter).
    `left_trim2`/`right_trim2` are NULL (no mate). The reason comes from
    `filter_read` on the TRIMMED sequence (so a too-short trimmed read is
    qc_too_short, never pass). Struct fields use bracket syntax (`s['field']`),
    unambiguous against a column alias."""
    # `inner` materialises the trim struct(s) as named columns so the outer
    # SELECT can read the trimmed seq/qual, the per-end trim counts, and the
    # filter_read result without re-evaluating the trim functions. polyG is a
    # 3'-only second pass on the adapter-trimmed seq; when off, the trimmed
    # seq/qual and the 3' count come straight from the adapter struct.
    if apply_polyg:
        inner = (
            "SELECT sequence_idx, ta, trim_polyg(ta['sequence'], ta['quality']) AS pg FROM ("
            f"SELECT sequence_idx, trim_adapters(sequence1, qual1, {adapters_sql}) AS ta "
            f"FROM {src_view})"
        )
        seq, qual = "pg['sequence']", "pg['quality']"
        right_trim1 = "(ta['trimmed_3p'] + pg['trimmed_3p'])::UINTEGER"
    else:
        inner = (
            f"SELECT sequence_idx, trim_adapters(sequence1, qual1, {adapters_sql}) AS ta "
            f"FROM {src_view}"
        )
        seq, qual = "ta['sequence']", "ta['quality']"
        right_trim1 = "ta['trimmed_3p']::UINTEGER"
    filter_call = f"filter_read({seq}, {qual}, {_MIN_LENGTH}, {_FILTER_READ_TAIL}) AS f"
    # This SELECT is the FIRST branch of the COPY's UNION ALL, so DuckDB takes the
    # Parquet column names from here — every column is aliased to the mask schema.
    # The middle SELECT pins `f` (filter_read on the trimmed seq) alongside the
    # trim structs so the reason CASE and the trims read from one row.
    return (
        "SELECT sequence_idx, "
        f"{_SE_REASON_CASE} AS reason, "
        "ta['trimmed_5p']::UINTEGER AS left_trim1, "
        f"{right_trim1} AS right_trim1, "
        "NULL::UINTEGER AS left_trim2, "
        "NULL::UINTEGER AS right_trim2 "
        f"FROM (SELECT sequence_idx, ta, {('pg, ' if apply_polyg else '')}{filter_call} "
        f"      FROM ({inner}))"
    )


def _qc_pe_select(
    src_view: str,
    *,
    adapters_sql: str,
    apply_polyg: bool,
) -> str:
    """Build the paired-end QC mask SELECT: overlap-aware adapter trim -> optional
    per-mate polyG -> per-mate length/quality filter, projected to the mask
    schema. PE trimming is 3'-only, so `left_trim1`/`left_trim2` are 0 and the
    right trims accumulate adapter + polyG per mate. The pair's reason is `pass`
    only when BOTH mates pass; otherwise it is the failing mate's reason (R1
    checked first). Returns SQL (no execution) so it can be UNION ALL'd into the
    streaming COPY. Isolated so unit tests assert the generated SQL."""
    adapter_layer = (
        "SELECT sequence_idx, "
        "trim_adapters_pe(sequence1, qual1, sequence2, qual2, "
        f"{adapters_sql}, {_TRIM_PE_OVERLAP_DEFAULTS}) AS ta FROM {src_view}"
    )
    if apply_polyg:
        inner = (
            "SELECT sequence_idx, ta, "
            "trim_polyg(ta['sequence1'], ta['quality1']) AS pg1, "
            "trim_polyg(ta['sequence2'], ta['quality2']) AS pg2 "
            f"FROM ({adapter_layer})"
        )
        s1, q1 = "pg1['sequence']", "pg1['quality']"
        s2, q2 = "pg2['sequence']", "pg2['quality']"
        right_trim1 = "(ta['trimmed1_3p'] + pg1['trimmed_3p'])::UINTEGER"
        right_trim2 = "(ta['trimmed2_3p'] + pg2['trimmed_3p'])::UINTEGER"
    else:
        inner = adapter_layer
        s1, q1 = "ta['sequence1']", "ta['quality1']"
        s2, q2 = "ta['sequence2']", "ta['quality2']"
        right_trim1 = "ta['trimmed1_3p']::UINTEGER"
        right_trim2 = "ta['trimmed2_3p']::UINTEGER"
    f1 = f"filter_read({s1}, {q1}, {_MIN_LENGTH}, {_FILTER_READ_TAIL})"
    f2 = f"filter_read({s2}, {q2}, {_MIN_LENGTH}, {_FILTER_READ_TAIL})"
    # PE reason: pass only when both mates pass; else the failing mate's reason
    # (R1 first). Reuses the SE CASE per mate by aliasing each filter result `f`.
    reason_case = (
        "CASE "
        f"WHEN f1['passed'] AND f2['passed'] THEN '{ReadMaskReason.PASS.value}' "
        f"WHEN NOT f1['passed'] THEN ({_pe_fail_reason('f1')}) "
        f"ELSE ({_pe_fail_reason('f2')}) "
        "END"
    )
    # The middle SELECT pins the trim structs (ta + polyG structs) alongside the
    # two filter results so the reason CASE and the per-mate right trims read from
    # one row. `right_trim1`/`right_trim2` reference ta and (when polyG) pg1/pg2.
    pg_carry = "pg1, pg2, " if apply_polyg else ""
    return (
        "SELECT sequence_idx, "
        f"{reason_case} AS reason, "
        "0::UINTEGER AS left_trim1, "
        f"{right_trim1} AS right_trim1, "
        "0::UINTEGER AS left_trim2, "
        f"{right_trim2} AS right_trim2 "
        f"FROM (SELECT sequence_idx, ta, {pg_carry}{f1} AS f1, {f2} AS f2 "
        f"      FROM ({inner}))"
    )


def _pe_fail_reason(f: str) -> str:
    """SQL fragment mapping a failed mate's filter_read struct `f` to its qc_*
    reason (no pass branch — the caller has already established this mate
    failed). fail_reason is one of length/too_long/n_base/quality."""
    return (
        "CASE "
        f"WHEN {f}['fail_reason'] = 'length' THEN '{ReadMaskReason.QC_TOO_SHORT.value}' "
        f"WHEN {f}['fail_reason'] = 'too_long' THEN '{ReadMaskReason.QC_TOO_LONG.value}' "
        f"WHEN {f}['fail_reason'] = 'n_base' THEN '{ReadMaskReason.QC_TOO_MANY_N.value}' "
        f"ELSE '{ReadMaskReason.QC_LOW_QUALITY.value}' "
        "END"
    )


async def execute(inputs: Inputs, workspace: Path) -> dict[str, Path]:
    if not inputs.reads.exists():
        raise FileNotFoundError(f"reads parquet not found: {inputs.reads}")
    if not inputs.adapter_parquet.exists():
        raise FileNotFoundError(f"adapter_parquet not found: {inputs.adapter_parquet}")

    apply_polyg = _is_two_color(inputs.instrument_model)

    workspace.mkdir(parents=True, exist_ok=True)
    qc_mask = workspace / "qc_mask.parquet"

    # COPY / CREATE VIEW path literals can't take a bound param; route them
    # through validate_parquet_path (fail-fast on quote/backslash/control chars)
    # rather than inline-escaping.
    reads_sql = validate_parquet_path(inputs.reads)
    out_sql = validate_parquet_path(qc_mask)

    success = False
    try:
        with duckdb_tmp_dir(workspace) as duckdb_tmp, open_miint_conn() as conn:
            apply_duckdb_settings(
                conn,
                duckdb_tmp,
                memory_gb=resolve_duckdb_memory_gb(_DUCKDB_MEMORY_GB, threads=_DUCKDB_THREADS),
                threads=_DUCKDB_THREADS,
            )

            # Read + render the adapter set from the staged Parquet (fail fast on
            # an empty/unreadable set). The constant VARCHAR[] is inlined into the
            # QC SQL — miint requires a bind-time constant adapter list.
            adapters_sql = _adapters_sql(_read_adapter_parquet(conn, inputs.adapter_parquet))

            # Route by layout: SE (sequence2 IS NULL) and PE rows take different
            # miint overloads. The qual columns ride along (QC needs decoded
            # phred). read_id is not needed — the mask keys on sequence_idx only.
            conn.execute(
                f"CREATE VIEW {_SE} AS "
                "SELECT sequence_idx, sequence1, qual1 "
                f"FROM read_parquet('{reads_sql}') WHERE sequence2 IS NULL"
            )
            conn.execute(
                f"CREATE VIEW {_PE} AS "
                "SELECT sequence_idx, sequence1, qual1, sequence2, qual2 "
                f"FROM read_parquet('{reads_sql}') WHERE sequence2 IS NOT NULL"
            )
            # Stream the whole transform: the SE and PE seams each emit a 6-col
            # mask SELECT, UNION ALL'd and sorted straight into the COPY — no
            # intermediate accumulator table. ORDER BY keeps the lake-friendly
            # sorted `sequence_idx` layout and makes the output deterministic; an
            # empty source view contributes no rows.
            se_select = _qc_se_select(_SE, adapters_sql=adapters_sql, apply_polyg=apply_polyg)
            pe_select = _qc_pe_select(_PE, adapters_sql=adapters_sql, apply_polyg=apply_polyg)
            conn.execute(
                f"COPY (({se_select}) UNION ALL ({pe_select}) ORDER BY sequence_idx) "
                f"TO '{out_sql}' ({PARQUET_OPTS})"
            )
        success = True
    finally:
        # On failure remove a partial output so the SLURM launcher's manifest
        # walker (which runs after execute()) can't promote it as the result.
        if not success:
            qc_mask.unlink(missing_ok=True)

    # `qc_mask` is the partial mask host_filter merges its host hits into. The
    # reads are NOT re-emitted: host_filter reads the full `read.parquet` (the
    # `reads` binding fastq produced) directly.
    return {"qc_mask": qc_mask}
