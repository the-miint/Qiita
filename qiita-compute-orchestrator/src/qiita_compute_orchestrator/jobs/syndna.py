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
to the end — no step overwrites an earlier verdict. NOTE the consequence for the
count: `spikein_read_count_r1r2` is therefore a RAW-space count, not a QC'd /
host-depleted one (a spike-in read that would have failed QC is still counted).
The cell-count model must know which space its denominator lives in.

**Classifies the RAW read.** As the first step there is no incoming mask and no
trimming yet, so minimap2 aligns `sequence1` directly. Trims are all zero (SynDNA
does not trim); the mate-trim columns follow the read_mask convention (NULL for
single-end, 0 for paired) so both-mates counting stays correct downstream.

`spikein_syndna` is not biological — a spike-in is added in the lab. It is
excluded from `read_masked` (which serves only `pass`) and gets its own count
bucket, with its rows RETAINED in `read_mask` so the counts survive.

**Alignment, not k-mer classification.** A read is a spike-in when it ALIGNS to a
SynDNA insert at >= `_MIN_IDENTITY` identity over the aligned region. This mirrors
`host_filter`'s minimap2 arm (same `align_minimap2` seam, same `max_secondary := 0`)
but adds an identity floor, which host filtering does not need: host depletion is
deliberately aggressive (any alignment = host), whereas a spike-in hit is a
QUANTITATIVE claim — a false positive both removes a real read from `biological`
AND inflates the count the cell-count model divides by.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
from pydantic import BaseModel
from qiita_common.models import ReadMaskReason
from qiita_common.parquet import validate_parquet_path

from ..miint import (
    PARQUET_OPTS,
    apply_duckdb_settings,
    duckdb_tmp_dir,
    open_miint_conn,
)

YAML_STEP_NAME = "syndna"

# DuckDB gets a FIXED modest share and is deliberately NOT allocation-aware here:
# the minimap2 index lives OUT of DuckDB's heap, so growing DuckDB with the cgroup
# would starve it. Same reasoning (and same numbers) as host_filter — the right
# lever for a bigger alignment is the cgroup (YAML mem_gb), which reaches minimap2
# directly.
_DUCKDB_MEMORY_GB = 8
_DUCKDB_THREADS = 4

# PacBio HiFi long-read alignment mode, matching the preset the `.mmi` is built
# with (`qiita reference load --minimap2-preset map-hifi`). SynDNA spike-ins are
# only ever quantified on the long-read (PacBio) protocols, so this is not a
# per-sample knob.
_MM2_PRESET = "map-hifi"

# Minimum identity over the ALIGNED region for a read to count as a spike-in.
#
# Identity is `1 - NM / aligned_len`, where NM (`tag_nm`) is the edit distance and
# `aligned_len` is the query bases the alignment consumes (cigar `M`/`=`/`X`/`I`) —
# the same quantity coverm calls `--min-read-percent-identity`. No coverage floor is
# imposed (coverm's `--min-read-aligned-percent 0.0`), so a soft-clipped read that
# matches an insert at high identity over its aligned part still counts.
#
# The failure mode is asymmetric and worth stating: a FALSE POSITIVE both removes a
# real read from `biological` and inflates the spike-in count that the cell-count
# model divides by. Pinned by the unit + smoke tests; revisit with the assay owner
# against real data.
_MIN_IDENTITY = 0.95

# In-DuckDB relation names. The reads are a VIEW (both the query and the final COPY
# read them); the hit set is a TABLE, pre-declared BIGINT so `read_id` coerces on
# insert.
_READS = "syndna_reads"
_QUERY = "syndna_query"
_HITS = "syndna_hits"

# Query bases the alignment consumes, from the cigar: the `M`/`=`/`X`/`I` ops (soft
# and hard clips are excluded — they are not aligned). `align_minimap2` emits an
# eqx-style cigar (`=`/`X` rather than a bare `M`), but `M` is accepted too so this
# does not silently return 0 if a future build stops splitting matches.
_ALIGNED_LEN_SQL = """
    list_sum(list_transform(
        list_filter(regexp_extract_all(cigar, '\\d+[MIDNSHP=X]'),
                    t -> regexp_matches(t, '[MI=X]$')),
        t -> CAST(regexp_extract(t, '^\\d+') AS BIGINT)))
"""


class Inputs(BaseModel):
    """Typed input contract for syndna.

    First step of the chain, so it takes only the raw `reads` and the spike-in
    reference. `syndna_minimap2_path` is the `.mmi`, bound by the runner only when
    `syndna_enabled` — and the step runs under that same gate, so it is REQUIRED
    (an unbound index would mean the gate and the binding disagree).
    """

    reads: Path
    syndna_minimap2_path: Path
    prep_sample_idx: int | None = None
    work_ticket_idx: int


def _validate_minimap2_index(path: Path) -> None:
    """A minimap2 index is a single `.mmi` FILE; reject a missing or zero-byte one.

    Fail fast: an empty index would silently align nothing and report zero spike-ins
    for a sample that has them — which the cell-count model would then divide by."""
    if not path.exists():
        raise FileNotFoundError(f"syndna_minimap2_path not found: {path}")
    if not path.is_file() or path.stat().st_size == 0:
        raise ValueError(f"syndna_minimap2_path is not a non-empty .mmi file: {path}")


def _run_align_minimap2(
    conn: duckdb.DuckDBPyConnection,
    index_path: Path,
    query_table: str,
    dest_table: str,
    *,
    preset: str,
    min_identity: float,
) -> None:
    """Seam around miint's `align_minimap2`. Appends the DISTINCT spike-in
    `sequence_idx` set into the pre-created `dest_table`.

    Differs from `host_filter._run_align_minimap2` in exactly one way: an IDENTITY
    FLOOR. host filtering takes any alignment as host (aggressive depletion is the
    safe direction there); a spike-in hit is a quantitative claim, so a low-identity
    incidental alignment must not count. `max_secondary := 0` drops secondaries and
    DISTINCT collapses any remaining per-read rows to one `sequence_idx`.

    `flags & 4 = 0` keeps only mapped rows (`samtools view -F 4`). Isolated as a
    seam so unit tests stub the real aligner."""
    conn.execute(
        f"INSERT INTO {dest_table} "
        "SELECT DISTINCT read_id AS sequence_idx FROM ("
        f"  SELECT read_id, 1.0 - tag_nm / NULLIF({_ALIGNED_LEN_SQL}, 0) AS identity"
        "   FROM align_minimap2(?, index_path := ?, preset := ?, max_secondary := 0)"
        "   WHERE flags & 4 = 0"
        ") WHERE identity >= ?",
        [query_table, str(index_path), preset, min_identity],
    )


async def execute(inputs: Inputs, workspace: Path) -> dict[str, Path]:
    if not inputs.reads.exists():
        raise FileNotFoundError(f"reads parquet not found: {inputs.reads}")
    _validate_minimap2_index(inputs.syndna_minimap2_path)

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
            # No incoming mask and no trimming yet: minimap2 aligns the raw read.
            conn.execute(f"CREATE VIEW {_READS} AS SELECT * FROM read_parquet('{reads_sql}')")
            conn.execute(
                f"CREATE VIEW {_QUERY} AS "
                f"SELECT sequence_idx AS read_id, sequence1, sequence2 FROM {_READS}"
            )
            conn.execute(f"CREATE TABLE {_HITS} (sequence_idx BIGINT)")
            _run_align_minimap2(
                conn,
                inputs.syndna_minimap2_path,
                _QUERY,
                _HITS,
                preset=_MM2_PRESET,
                min_identity=_MIN_IDENTITY,
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
