"""Native job: a fastqc-equivalent per-sample QC summary over a reads.parquet.

A pure `reads.parquet -> qc_report.json` reporting step — it reads but never
mutates the reads, so it changes no filtering behaviour. It computes a
single-pass summary (per-mate read/base counts, mean quality, GC and N content,
length stats) plus three distributions (per-sequence mean-quality, GC-percent,
and length histograms) straight from the decoded `qual1`/`qual2` arrays and the
`sequence1`/`sequence2` strings, with no miint extension and no container.

**Two report points, one module.** SPP runs fastqc twice per sample — on the raw
post-bcl-convert reads and on the host-filtered reads. This module is wired into
the workflow as two steps that differ only in which reads they consume: the raw
step binds the `fastq` output (`reads`), the post-filter step binds the
`host_filter` output (`filtered_reads`). The runner has no input aliasing (a
step's wire input name must equal an `Inputs` field), so the module exposes BOTH
as optional inputs and reports on whichever is bound — and names its output
binding to match (`raw_qc_report` vs `filtered_qc_report`) so both reports
coexist in the runner's `bound` for the merged-report step to consume.

Paired-end is reported per mate (r1 from sequence1/qual1, r2 from
sequence2/qual2 where present), mirroring fastqc's separate R1/R2 reports;
single-end yields `r2: null`. An empty (fully-filtered) sample yields a
well-formed all-zero report, not an error.
"""

from __future__ import annotations

import json
from pathlib import Path

import duckdb
from pydantic import BaseModel
from qiita_common.parquet import validate_parquet_path

from ..miint import apply_duckdb_settings, duckdb_tmp_dir, open_conn, resolve_duckdb_memory_gb

YAML_STEP_NAME = "qc_report"

# Report is a handful of single-pass aggregate scans (regexp GC/N counts + list
# aggregates over the quality arrays + small-cardinality histograms); the whole
# footprint IS DuckDB's (no out-of-heap co-consumer), so — like qc /
# hash_sequences / reference_load — the cap is sized to the cgroup via
# `resolve_duckdb_memory_gb()` so a per-run `--mem-gb` override reaches DuckDB.
# This literal is only the OFF-SLURM fallback; the report never approaches it.
_DUCKDB_MEMORY_GB = 6
_DUCKDB_THREADS = 2

# The output filename is fixed; the OUTPUT BINDING name (raw_/filtered_) is what
# disambiguates the two steps, so the on-disk name can be the same in each
# step's own workspace.
REPORT_FILENAME = "qc_report.json"


class Inputs(BaseModel):
    """Typed input contract for qc_report.

    Exactly one of `reads` (the raw report point, bound to the `fastq` output)
    or `filtered_reads` (the post-filter point, bound to the `host_filter`
    output) is set — the bound one selects the report point and the output
    binding name. `prep_sample_idx` / `work_ticket_idx` are the
    framework-injected scope scalars.
    """

    reads: Path | None = None
    filtered_reads: Path | None = None
    prep_sample_idx: int
    work_ticket_idx: int


def _mate_stats(conn: duckdb.DuckDBPyConnection, reads_sql: str, mate: int) -> dict | None:
    """Compute one mate's summary + distributions, or None when the mate is
    absent (an all-single-end sample has no r2 rows).

    `mate` is 1 (sequence1/qual1) or 2 (sequence2/qual2). For r2 we restrict to
    rows whose sequence2 is non-null; a sample with zero such rows returns None
    so the report carries `r2: null` rather than an all-zero r2 block.

    Single pass: GC/N base counts come from `length - length(regexp_replace(...))`
    (chars removed = chars matched), the per-read mean quality from `list_avg`
    over the decoded `UTINYINT[]`, and the three distributions from DuckDB's
    `histogram()` (returns a MAP value -> count). Means are recomputed from the
    base/quality sums (not averaged per-read) so they are exact."""
    seq = f"sequence{mate}"
    qual = f"qual{mate}"
    where = "" if mate == 1 else f" WHERE {seq} IS NOT NULL"
    row = conn.execute(
        "SELECT count(*) AS reads, sum(len) AS total_bases, sum(gc) AS gc_bases,"
        " sum(nn) AS n_bases, sum(qsum) AS total_qual,"
        " min(len) AS min_len, max(len) AS max_len, avg(len) AS mean_len,"
        " histogram(CAST(round(mean_q) AS INTEGER)) AS quality_histogram,"
        " histogram(CASE WHEN len > 0 THEN CAST(round(100.0 * gc / len) AS INTEGER) END)"
        "   AS gc_histogram,"
        " histogram(len) AS length_histogram"
        " FROM ("
        f"  SELECT length(s) AS len,"
        "   length(s) - length(regexp_replace(s, '[GCgc]', '', 'g')) AS gc,"
        "   length(s) - length(regexp_replace(s, '[Nn]', '', 'g')) AS nn,"
        "   list_sum(q) AS qsum, list_avg(q) AS mean_q"
        f"  FROM (SELECT {seq} AS s, {qual} AS q FROM read_parquet('{reads_sql}'){where})"
        " )"
    ).fetchone()

    reads = row[0]
    if reads == 0:
        return None
    total_bases, gc_bases, n_bases, total_qual = row[1], row[2], row[3], row[4]
    min_len, max_len, mean_len = row[5], row[6], row[7]
    quality_hist, gc_hist, length_hist = row[8], row[9], row[10]
    # Means over the whole mate (exact, recomputed from the sums); None when no
    # bases, and mean_quality None when there's no quality at all (FASTA).
    # mean_quality divides total quality by total SEQUENCE bases; the
    # quality_histogram uses per-read list_avg (qual-array length). The two agree
    # under the FASTQ invariant len(qual) == len(sequence) per read, which
    # fastq_to_parquet's read_fastx guarantees for the reads we report on.
    has_quality = total_qual is not None and total_bases
    return {
        "reads": reads,
        "total_bases": total_bases,
        "mean_quality": (total_qual / total_bases) if has_quality else None,
        "gc_content": (gc_bases / total_bases) if total_bases else None,
        "n_content": (n_bases / total_bases) if total_bases else None,
        "min_length": min_len,
        "max_length": max_len,
        "mean_length": mean_len,
        "quality_histogram": _hist_to_dict(quality_hist),
        "gc_histogram": _hist_to_dict(gc_hist),
        "length_histogram": _hist_to_dict(length_hist),
    }


def _hist_to_dict(hist: dict | None) -> dict[str, int]:
    """DuckDB `histogram()` returns a MAP (Python dict) keyed by the bucket
    value; render it as a JSON-friendly `{str(bucket): count}`. Over zero rows or
    an all-NULL input DuckDB returns SQL NULL for the whole histogram (the `if
    not hist` guard), so the explicit `k is not None` filter is just belt-and-
    suspenders against a stray NULL bucket."""
    if not hist:
        return {}
    return {str(k): v for k, v in hist.items() if k is not None}


async def execute(inputs: Inputs, workspace: Path) -> dict[str, Path]:
    if (inputs.reads is None) == (inputs.filtered_reads is None):
        raise ValueError(
            "qc_report requires exactly one of `reads` (raw point) or "
            "`filtered_reads` (post-filter point) to be bound"
        )
    if inputs.reads is not None:
        source, point, output_binding = inputs.reads, "raw", "raw_qc_report"
    else:
        source, point, output_binding = inputs.filtered_reads, "filtered", "filtered_qc_report"
    if not source.exists():
        raise FileNotFoundError(f"reads parquet not found: {source}")

    workspace.mkdir(parents=True, exist_ok=True)
    report_path = workspace / REPORT_FILENAME

    # read_parquet path literal can't take a bound param; route it through
    # validate_parquet_path (fail-fast on quote/backslash/control chars), matching
    # the sibling jobs' read_parquet literals.
    reads_sql = validate_parquet_path(source)

    with duckdb_tmp_dir(workspace) as duckdb_tmp:
        with open_conn() as conn:
            apply_duckdb_settings(
                conn,
                duckdb_tmp,
                memory_gb=resolve_duckdb_memory_gb(_DUCKDB_MEMORY_GB, threads=_DUCKDB_THREADS),
                threads=_DUCKDB_THREADS,
            )
            read_pairs = conn.execute(
                f"SELECT count(*) FROM read_parquet('{reads_sql}')"
            ).fetchone()[0]
            r1 = _mate_stats(conn, reads_sql, 1)
            r2 = _mate_stats(conn, reads_sql, 2)
        report = {
            "point": point,
            "layout": "paired" if r2 is not None else "single",
            "read_pairs": read_pairs,
            "mates": {"r1": r1, "r2": r2},
        }
        report_path.write_text(json.dumps(report))

    return {output_binding: report_path}
