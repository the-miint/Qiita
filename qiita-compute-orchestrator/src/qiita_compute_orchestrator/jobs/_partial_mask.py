"""Shared guards for an optional incoming partial mask.

The read-mask chain threads a partial mask (`(sequence_idx, reason, left_trim1,
right_trim1, left_trim2, right_trim2)`) through its pre-`host_filter` steps:
`syndna -> lima -> qc`. Each step optionally consumes the prior step's mask,
re-classifies only its still-`pass` rows, and carries every non-`pass` row
verbatim. These three boundary checks are common to every consumer, so they live
here rather than in three copies.

`field` is the binding name to name in the error message (e.g. `partial_mask`),
so a failure points the operator at the input they set.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
from qiita_common.models import ReadMaskReason


def assert_single_end(
    conn: duckdb.DuckDBPyConnection, reads_sql: str, field: str, path: Path
) -> None:
    """Reject a paired-end read set when an incoming mask is bound.

    The incoming-mask seams fold trims back into `sequence1`/`qual1` only; PE would
    need per-mate `in_left2`/`in_right2` math nothing produces today. Fail loudly at
    the boundary instead of shipping an untested path."""
    (pe_rows,) = conn.execute(
        f"SELECT count(*) FROM read_parquet('{reads_sql}') WHERE sequence2 IS NOT NULL"
    ).fetchone()
    if pe_rows:
        raise ValueError(
            f"{field} is bound ({path}) but reads contain {pe_rows} paired-end "
            "row(s); an incoming mask is single-end only (long reads)"
        )


def assert_covers_reads(
    conn: duckdb.DuckDBPyConnection, reads_sql: str, incoming_view: str, field: str, path: Path
) -> None:
    """The incoming mask must carry exactly one row per read — a bijection.

    A consumer JOINs the two, so an unmatched read is silently DROPPED (and the
    sample's `raw` total then under-reports), while a duplicated `sequence_idx`
    fans the join out and double-counts. Equal row counts alone catch neither:
    reads {1,2} against a mask {1,1} counts 2 == 2, drops read 2, emits read 1
    twice. Assert all three legs — no unmatched read, no duplicate mask key, equal
    cardinality — which together force a bijection."""
    (n_reads,) = conn.execute(f"SELECT count(*) FROM read_parquet('{reads_sql}')").fetchone()
    n_mask, n_mask_distinct = conn.execute(
        f"SELECT count(*), count(DISTINCT sequence_idx) FROM {incoming_view}"
    ).fetchone()
    (unmatched,) = conn.execute(
        f"SELECT count(*) FROM read_parquet('{reads_sql}') r "
        f"ANTI JOIN {incoming_view} m USING (sequence_idx)"
    ).fetchone()
    if n_mask != n_mask_distinct:
        raise ValueError(
            f"{field} ({path}) has {n_mask - n_mask_distinct} duplicate "
            "sequence_idx row(s); an incoming mask must carry exactly one row per read"
        )
    if unmatched or n_reads != n_mask:
        raise ValueError(
            f"{field} ({path}) has {n_mask} row(s) covering "
            f"{n_reads - unmatched} of {n_reads} read(s); "
            "an incoming mask must carry exactly one row per read"
        )


def assert_trims_within_read(
    conn: duckdb.DuckDBPyConnection, reads_sql: str, incoming_view: str, field: str, path: Path
) -> None:
    """Reject an incoming `pass` row whose trims exceed its read's length.

    `infer_trim` (and syndna's zero trims) cannot produce this, but a hand-staged
    mask can. It must not pass silently: DuckDB's `substr` with a negative length
    walks BACKWARDS and returns bases, while the qual list-slice returns `[]` — so
    `filter_read` would judge a sequence against an empty phred array instead of
    erroring.

    `left + right == length` is PERMITTED (strict `>`): both slices come out empty
    and the read is `qc_too_short`, the truthful verdict.

    Checked against BOTH lengths. The sequence slice bounds on `length(sequence1)`
    and the qual slice on `length(qual1)`; the DuckLake `read` table makes those
    equal by construction, but a guard that trusts one to speak for the other is
    exactly how the two silently desync."""
    (bad,) = conn.execute(
        f"SELECT count(*) FROM read_parquet('{reads_sql}') r JOIN {incoming_view} m "
        "USING (sequence_idx) "
        f"WHERE m.reason = '{ReadMaskReason.PASS.value}' "
        "AND (m.left_trim1 + m.right_trim1 > length(r.sequence1) "
        "  OR m.left_trim1 + m.right_trim1 > length(r.qual1))"
    ).fetchone()
    if bad:
        raise ValueError(
            f"{field} ({path}) has {bad} pass row(s) whose "
            "left_trim1 + right_trim1 exceeds the read length"
        )
