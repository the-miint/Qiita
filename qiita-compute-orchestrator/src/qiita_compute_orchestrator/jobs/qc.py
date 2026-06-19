"""Native job: fastp-equivalent read QC on `reads.parquet`.

A pure `reads.parquet -> qc_reads.parquet` transform keyed by the already-minted
`sequence_idx`. Minting happened upstream in `fastq_to_parquet`; this step is an
additive downstream filter that DROPS reads (and TRIMS the surviving ones), so
the surviving `sequence_idx` are a subset of the minted range (benign gaps —
`sequence_idx` stays a unique sorted join key, exactly as for `host_filter`).
Runs BEFORE `host_filter` in the bcl-convert pipeline (`fastq` -> `qc` ->
`host_filter`).

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
     == fastp defaults with `-l 100`. A read failing this is dropped. No
     quality-trimming (fastp default-off).

**Paired-end is handled natively, not by flattening.** A row of `reads.parquet`
is one read pair: `sequence1`/`sequence2` are R1/R2 under one minted
`sequence_idx`. PE rows go through `trim_adapters_pe` (overlap-aware) and then
`filter_read` is applied to EACH mate; **the pair is dropped if EITHER mate
falls below min_length after trimming** — never moving R2 into an R1 slot.
Single-end rows (`sequence2 IS NULL`) take the SE chain. The two layouts are
routed to separate seams so each runs the right miint overload.

Adapters: the canonical adapter set is materialized by the runner
(`_resolve_qc_adapters`) into the bound `adapter_fasta`, read here with miint's
`read_fastx` (the same reader `fastq_to_parquet` uses for the input reads), and
rendered into a constant SQL `VARCHAR[]` (miint's QC functions require bind-time
constants — the adapter list cannot be a column/parameter). QC is always-on in
this path, so `adapter_fasta` is a REQUIRED input and an empty one is fail-fast.

Drop-only + trim, `sequence_idx`-preserving: the 6-column schema and the
lake-friendly `ORDER BY sequence_idx` layout are preserved. A sample where QC
drops every read yields an empty (0-row) but well-formed Parquet, not an error.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import duckdb
from pydantic import BaseModel

from ..miint import (
    PARQUET_OPTS,
    apply_duckdb_settings,
    open_miint_conn,
    resolve_duckdb_memory_gb,
)

YAML_STEP_NAME = "qc"

# DuckDB resource caps for this step. QC is a per-row scalar transform plus the
# final sorted COPY — no out-of-heap co-consumer (unlike host_filter's rype /
# minimap2 runtimes, which is why host_filter deliberately does NOT do this), so
# the whole footprint IS DuckDB's. Hence `_DUCKDB_MEMORY_GB` is only the
# OFF-SLURM fallback cap: under SLURM the real cap is sized to the cgroup via
# `resolve_duckdb_memory_gb()` (SLURM_MEM_PER_NODE) so a per-run `--mem-gb`
# override reaches DuckDB's `memory_limit` — the same allocation-aware pattern
# stage_local_fasta / hash_sequences use. The fastq-to-parquet/1.2.0 YAML's qc
# step allocates mem_gb=12, which lands DuckDB near this 8 GB fallback after the
# 4-thread headroom; bump the YAML mem_gb when sized against a real
# genome-scale sample (this fallback only bites the local backend / tests).
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

# In-DuckDB relation names. The SE/PE source views and the output accumulator
# table. (Unlike host_filter these need not be non-temp for a separate-connection
# reason — the QC functions are SCALAR, evaluated inline on this connection — but
# regular views/tables are the simplest named relations the seams can target.)
_SE = "qc_se"
_PE = "qc_pe"
_OUT = "qc_out"


class Inputs(BaseModel):
    """Typed input contract for qc.

    `reads` is fastq_to_parquet's `reads.parquet` (binding name `reads`):
    `(sequence_idx BIGINT, read_id, sequence1, qual1, sequence2, qual2)`.
    `adapter_fasta` is the canonical adapter set the runner materializes
    (`_resolve_qc_adapters`) — REQUIRED (QC is always-on; an empty set is a
    misconfiguration). `instrument_model` gates polyG trimming (None -> OFF);
    it is forwarded from qiita.sequencing_run per sample. `prep_sample_idx` /
    `work_ticket_idx` are the framework-injected scope scalars.
    """

    reads: Path
    adapter_fasta: Path
    instrument_model: str | None = None
    prep_sample_idx: int
    work_ticket_idx: int


def _is_two_color(instrument_model: str | None) -> bool:
    """True iff `instrument_model` names a 2-color-chemistry instrument (polyG
    applies). Case-insensitive substring match; None/empty -> False (polyG OFF)."""
    if not instrument_model:
        return False
    model = instrument_model.lower()
    return any(sub in model for sub in _TWO_COLOR_MODEL_SUBSTRINGS)


def _read_adapter_fasta(conn: duckdb.DuckDBPyConnection, path: Path) -> list[str]:
    """Read adapter sequences from a FASTA via miint's `read_fastx` (one row per
    record; `sequence1` is the adapter) — the same reader `fastq_to_parquet` uses
    for the input reads, so FASTA wrapping/parsing is miint's job, not ours.

    Raises ValueError when the set is empty or unreadable — an adapter reference
    with no sequences (or a malformed FASTA) is a misconfiguration, not a valid
    QC input. `read_fastx` THROWS a duckdb.Error on an empty/blank file rather
    than returning zero rows, so we catch that and re-raise as ValueError (which
    the framework dispatcher maps to BAD_INPUT); catching the exception TYPE,
    not its wording, keeps this robust to a future miint message change."""
    try:
        rows = conn.execute("SELECT sequence1 FROM read_fastx(?)", [str(path)]).fetchall()
    except duckdb.Error as exc:
        raise ValueError(f"adapter_fasta could not be read as FASTA: {path}: {exc}") from exc
    adapters = [r[0] for r in rows]
    if not adapters:
        raise ValueError(f"adapter_fasta contains no sequences: {path}")
    return adapters


def _adapters_sql(adapters: list[str]) -> str:
    """Render the adapter list as a constant SQL `VARCHAR[]` literal (miint's QC
    functions require a bind-time constant adapter set, not a column/parameter).
    Single quotes are escaped — adapters are DNA from a reference DB, but the
    escape keeps this injection-safe regardless."""
    elements = ", ".join("'" + a.replace("'", "''") + "'" for a in adapters)
    return f"[{elements}]::VARCHAR[]"


def _run_qc_se(
    conn: duckdb.DuckDBPyConnection,
    src_view: str,
    dest_table: str,
    *,
    adapters_sql: str,
    apply_polyg: bool,
) -> None:
    """Seam around the single-end miint chain: adapter trim -> optional polyG ->
    length/quality filter. Appends the surviving (trimmed) reads into
    `dest_table` with a NULL R2. Isolated so unit tests stub the real miint calls.

    The trimmed sequence/quality come from the last trim step (polyG if applied,
    else adapter); `filter_read` only decides pass/fail, so its struct is read in
    the WHERE clause and the carried seq/qual are the trim output. Struct fields
    are read with bracket syntax (`s['sequence']`) — unambiguous against a column
    alias, unlike dotted access."""
    if apply_polyg:
        inner = (
            "SELECT sequence_idx, read_id, "
            "trim_polyg(ta['sequence'], ta['quality']) AS pg "
            "FROM (SELECT sequence_idx, read_id, "
            f"trim_adapters(sequence1, qual1, {adapters_sql}) AS ta FROM {src_view})"
        )
        seq, qual = "pg['sequence']", "pg['quality']"
    else:
        inner = (
            "SELECT sequence_idx, read_id, "
            f"trim_adapters(sequence1, qual1, {adapters_sql}) AS ta FROM {src_view}"
        )
        seq, qual = "ta['sequence']", "ta['quality']"
    conn.execute(
        f"INSERT INTO {dest_table} "
        f"SELECT sequence_idx, read_id, {seq}, {qual}, NULL::VARCHAR, NULL::UTINYINT[] "
        f"FROM ({inner}) "
        f"WHERE filter_read({seq}, {qual}, {_MIN_LENGTH}, {_FILTER_READ_TAIL})['passed']"
    )


def _run_qc_pe(
    conn: duckdb.DuckDBPyConnection,
    src_view: str,
    dest_table: str,
    *,
    adapters_sql: str,
    apply_polyg: bool,
) -> None:
    """Seam around the paired-end miint chain: overlap-aware adapter trim ->
    optional per-mate polyG -> per-mate length/quality filter. The pair is kept
    only when BOTH mates pass (drop the pair if EITHER mate falls below
    min_length after trimming). Appends the surviving (trimmed) pairs into
    `dest_table`. Isolated so unit tests stub the real miint calls."""
    adapter_layer = (
        "SELECT sequence_idx, read_id, "
        "trim_adapters_pe(sequence1, qual1, sequence2, qual2, "
        f"{adapters_sql}, {_TRIM_PE_OVERLAP_DEFAULTS}) AS ta FROM {src_view}"
    )
    if apply_polyg:
        inner = (
            "SELECT sequence_idx, read_id, "
            "trim_polyg(ta['sequence1'], ta['quality1']) AS pg1, "
            "trim_polyg(ta['sequence2'], ta['quality2']) AS pg2 "
            f"FROM ({adapter_layer})"
        )
        s1, q1 = "pg1['sequence']", "pg1['quality']"
        s2, q2 = "pg2['sequence']", "pg2['quality']"
    else:
        inner = adapter_layer
        s1, q1 = "ta['sequence1']", "ta['quality1']"
        s2, q2 = "ta['sequence2']", "ta['quality2']"
    conn.execute(
        f"INSERT INTO {dest_table} "
        f"SELECT sequence_idx, read_id, {s1}, {q1}, {s2}, {q2} "
        f"FROM ({inner}) "
        f"WHERE filter_read({s1}, {q1}, {_MIN_LENGTH}, {_FILTER_READ_TAIL})['passed'] "
        f"AND filter_read({s2}, {q2}, {_MIN_LENGTH}, {_FILTER_READ_TAIL})['passed']"
    )


async def execute(inputs: Inputs, workspace: Path) -> dict[str, Path]:
    if not inputs.reads.exists():
        raise FileNotFoundError(f"reads parquet not found: {inputs.reads}")
    if not inputs.adapter_fasta.exists():
        raise FileNotFoundError(f"adapter_fasta not found: {inputs.adapter_fasta}")

    apply_polyg = _is_two_color(inputs.instrument_model)

    workspace.mkdir(parents=True, exist_ok=True)
    qc_reads = workspace / "qc_reads.parquet"
    duckdb_tmp = workspace / ".duckdb_tmp"
    duckdb_tmp.mkdir(parents=True, exist_ok=True)

    reads_sql = str(inputs.reads).replace("'", "''")
    out_sql = str(qc_reads).replace("'", "''")

    success = False
    try:
        with open_miint_conn() as conn:
            apply_duckdb_settings(
                conn,
                duckdb_tmp,
                memory_gb=resolve_duckdb_memory_gb(_DUCKDB_MEMORY_GB, threads=_DUCKDB_THREADS),
                threads=_DUCKDB_THREADS,
            )

            # Read + render the adapter set via miint's read_fastx (fail fast on
            # an empty/unreadable set). The constant VARCHAR[] is inlined into the
            # QC SQL — miint requires a bind-time constant adapter list.
            adapters_sql = _adapters_sql(_read_adapter_fasta(conn, inputs.adapter_fasta))

            # Route by layout: SE (sequence2 IS NULL) and PE rows take different
            # miint overloads. CREATE VIEW can't take prepared params, so the
            # reads path is inlined (quote-escaped — a filesystem path, no other
            # injection surface). The qual columns ride along (QC needs decoded
            # phred, unlike the sequence-only host filter).
            conn.execute(
                f"CREATE VIEW {_SE} AS "
                "SELECT sequence_idx, read_id, sequence1, qual1 "
                f"FROM read_parquet('{reads_sql}') WHERE sequence2 IS NULL"
            )
            conn.execute(
                f"CREATE VIEW {_PE} AS "
                "SELECT sequence_idx, read_id, sequence1, qual1, sequence2, qual2 "
                f"FROM read_parquet('{reads_sql}') WHERE sequence2 IS NOT NULL"
            )
            # Output accumulator (the full 6-col schema). Both seams append into
            # it; an empty source view contributes nothing.
            conn.execute(
                f"CREATE TABLE {_OUT} "
                "(sequence_idx BIGINT, read_id VARCHAR, sequence1 VARCHAR, "
                "qual1 UTINYINT[], sequence2 VARCHAR, qual2 UTINYINT[])"
            )

            _run_qc_se(conn, _SE, _OUT, adapters_sql=adapters_sql, apply_polyg=apply_polyg)
            _run_qc_pe(conn, _PE, _OUT, adapters_sql=adapters_sql, apply_polyg=apply_polyg)

            # ORDER BY keeps the lake-friendly sorted `sequence_idx` layout
            # fastq_to_parquet wrote (the two seams append unordered) and makes
            # the output deterministic.
            conn.execute(
                "COPY (SELECT sequence_idx, read_id, sequence1, qual1, sequence2, qual2 "
                f"FROM {_OUT} ORDER BY sequence_idx) TO '{out_sql}' ({PARQUET_OPTS})"
            )
        success = True
    finally:
        # Drop the spill dir before returning so the SLURM launcher's manifest
        # walker (which runs after execute()) sees only qc_reads.parquet; on
        # failure remove a partial output so it can't be promoted.
        shutil.rmtree(duckdb_tmp, ignore_errors=True)
        if not success:
            qc_reads.unlink(missing_ok=True)

    # Output binding is `reads` (not `qc_reads`): qc is a transform in the
    # fastq -> qc -> host_filter chain, re-emitting the same logical `reads`
    # artifact the next step consumes. The runner has no input aliasing — a
    # step's wire input name must equal the downstream job's `Inputs` field — and
    # host_filter (shared with 1.1.0's fastq -> host_filter) reads `reads`, so the
    # binding stays `reads`. The on-disk file is qc_reads.parquet for provenance.
    return {"reads": qc_reads}
