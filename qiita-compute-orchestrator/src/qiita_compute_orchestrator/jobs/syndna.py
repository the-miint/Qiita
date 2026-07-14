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

That count is a MASKING metric — it exists so the read accounting balances — and is
NOT what the cell-count model consumes. The model needs per-insert COVERAGE DEPTH
(aligned bases inside the insert / insert length), which is a different quantity and
cannot be derived from a read count: this step reduces each read to a boolean and
discards the alignment. That work is tracked separately.

**Classifies the RAW read.** As the first step there is no incoming mask and no
trimming yet, so minimap2 aligns `sequence1` directly. Trims are all zero (SynDNA
does not trim), and the mate-trim columns are NULL: syndna is where the read set
first meets a long-read-only seam, so it REJECTS a paired-end set outright rather
than after a full alignment pass (see `_partial_mask.assert_single_end`).

`spikein_syndna` is not biological — a spike-in is added in the lab. It is
excluded from `read_masked` (which serves only `pass`) and gets its own count
bucket, with its rows RETAINED in `read_mask` so the counts survive.

**Alignment, not k-mer classification.** A read is a spike-in when it has a PRIMARY
alignment to a SynDNA insert at >= `_MIN_IDENTITY` identity. This mirrors
`host_filter`'s minimap2 arm (same `align_minimap2` seam, same `max_secondary := 0`)
but adds an identity floor and a primary-only predicate, neither of which host
filtering needs: host depletion is deliberately aggressive (any alignment = host),
whereas a spike-in call is a claim about a read's ORIGIN: a false positive silently
removes a genuine biological read from `biological`, and would corrupt the per-insert
coverage-depth quantification that consumes the same classifier. Both predicates, what
the primary-only rule costs in the other direction, and the one filter deliberately NOT
applied (a coverage floor), are argued at the constants — including the open questions
that are the assay owner's rather than ours.
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
    resolve_duckdb_memory_gb,
)
from ._partial_mask import assert_single_end

YAML_STEP_NAME = "syndna"

# Cgroup-aware WITH a reserve, like build_minimap2_index — not host_filter's fixed share.
#
# The DuckDB-side work here (the alignment output plus the identity / DISTINCT pass over
# millions of HiFi rows) does scale with the allocation, so a fixed 8 GB wastes a
# `--mem-gb` bump. But `align_minimap2` runs minimap2 IN-PROCESS, and its memory lives
# OUTSIDE DuckDB's heap — so DuckDB must not claim the whole cgroup. The reserve is what
# keeps minimap2 alive; without it a bigger allocation makes DuckDB greedier and the
# aligner gets OOM-killed.
#
# The reserve is sized for minimap2's ALIGNMENT buffers (per-thread chaining over ~20 kb
# HiFi reads), NOT for the index: a SynDNA `.mmi` is a handful of kb-scale inserts, which
# is why this is half of build_minimap2_index's 16 GB (that one builds an index over
# genome-scale subjects).
_DUCKDB_MEMORY_GB = 8
_DUCKDB_THREADS = 4
_MM2_RESERVE_GB = 8

# PacBio HiFi long-read alignment mode, matching the preset the `.mmi` is built
# with (`qiita reference load --minimap2-preset map-hifi`). SynDNA spike-ins are
# only ever quantified on the long-read (PacBio) protocols, so this is not a
# per-sample knob.
_MM2_PRESET = "map-hifi"

# Minimum sequence identity for a read to count as a spike-in — coverm's
# `--min-read-percent-identity`, computed by miint's `alignment_seq_identity`.
#
# Do NOT hand-roll this from the cigar. A deletion is a gap in the QUERY but is still
# an alignment column, so a naive `1 - NM / query_len` double-counts it and can even go
# NEGATIVE on a deletion-heavy alignment.
#
# `blast` is chosen because it is what coverm computes (identity derived from NM), and
# matching the assay's existing coverm behaviour is the standing instruction. Be aware
# what it costs, because it is not a rounding difference: BLAST charges a deletion PER
# BASE, gap-compressed charges it ONCE. A spike-in read carrying a single 200 bp
# deletion (cigar `899=200D901=`, NM 200) scores
#     blast          0.9000  -> BELOW this floor, NOT counted as a spike-in
#     gap_compressed 0.9994  -> above it, counted
# so one structural deletion flips the call. Whether a real spike-in molecule is
# expected to carry such an event — and therefore which method the assay wants — is
# with the assay owner alongside the open questions below.
_IDENTITY_METHOD = "blast"
_MIN_IDENTITY = 0.95

# =============================================================================
# PRIMARY alignments only — matching coverm
# =============================================================================
#
# A read counts as a spike-in only on a PRIMARY alignment: `alignment_is_primary` is
# false for both SECONDARY (0x100) and SUPPLEMENTARY (0x800) records. `max_secondary :=
# 0` already stops minimap2 emitting secondaries, so in practice this is what excludes
# SUPPLEMENTARY ones — and that exclusion is load-bearing, not cosmetic. Identity is
# scored per ROW and then DISTINCT'd to a read, so without it one short high-identity
# supplementary segment marks the whole read: exactly the local-alignment false positive
# a chimeric or repeat-containing HiFi read produces.
#
# Measured against coverm rather than assumed (coverm 0.8.0, `contig --methods count`): a
# read whose ONLY alignment to a contig is supplementary contributes 0 to that contig's
# count, while the byte-identical alignment with the flag cleared contributes 1. Its
# `--exclude-supplementary` flag does not change that — it governs `filter`, which
# thresholds records rather than counting reads. Note what the probe does and does not
# establish: coverm's `count` is a per-contig quantification and this predicate is a
# per-read origin boolean over the union of inserts, so they are not the same question —
# what was measured is that coverm does not credit a reference on a supplementary
# alignment, which is the behaviour being ported.
#
# THE COST, which is a real trade and not free: the rule is now "the read's BEST
# alignment is a spike-in at >= _MIN_IDENTITY", not "ANY alignment is". A read whose
# primary falls below the floor but which carries a high-identity supplementary segment
# is now `pass` — i.e. it lands in `biological`. If that read is a spike-in chimera it is
# lab-added sequence, so we have both undercounted the spike-in and put it in the
# biological set. HiFi chimera rates are low, so this is very likely the right side of
# the trade, but two questions are with the assay owner alongside the coverage floor:
# whether the inserts share backbone/flanking sequence with each other (which would make
# a spurious longer chain to the WRONG insert systematic rather than incidental), and
# whether a chimeric read carrying spike-in sequence is meant to be `biological` at all.
#
# `_PRIMARY_ONLY` exists as a constant, though nothing reads it, because the control
# plane folds it into the read-mask identity (`runner/_mask.py::_resolved_syndna`,
# pinned by `test_syndna_pins.py`): it is part of the effective filter, so a mask built
# under a different rule must not silently collapse onto one built under this rule.
_PRIMARY_ONLY = True

# =============================================================================
# What is DELIBERATELY not filtered — pending the assay owner
# =============================================================================
#
# NO COVERAGE FLOOR (coverm's `--min-read-aligned-percent 0.0`). Tempting, because
# without it a long read whose short stretch happens to match an insert is counted. But
# it CANNOT simply be added: the index carries the bare INSERTS, so a read spanning the
# insert -> plasmid-backbone junction aligns only over its insert portion and comes back
# soft-clipped, with low query coverage. That read is a REAL spike-in, not a chimera — a
# coverage floor would drop true spike-in molecules.
#
# The error is costly in both directions — a misclassified read is removed from
# `biological` AND will corrupt the per-insert coverage depth that consumes the same
# classifier — so the default is not safe to guess at. Discriminating a boundary-spanning
# read from an incidental one needs the alignment scored INSIDE the insert's window
# (miint's `alignment_slice` over a plasmid-level reference), which needs per-insert
# coordinates we do not store yet. It is with the assay owner; until they answer, this
# matches the coverm spec exactly.

# In-DuckDB relation names. The reads are a VIEW (both the query and the final COPY
# read them); the hit set is a TABLE, pre-declared BIGINT so `read_id` coerces on
# insert.
_READS = "syndna_reads"
_QUERY = "syndna_query"
_HITS = "syndna_hits"


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
    for a sample that has them — every spike-in read would then be counted as
    biological."""
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

    Differs from `host_filter._run_align_minimap2` in two ways, both because a spike-in
    call is a QUANTITATIVE claim where host filtering is a deliberately aggressive
    depletion (any alignment = host, and the safe direction there is to over-remove):

      * an IDENTITY FLOOR, so an incidental low-identity alignment does not count;
      * PRIMARY alignments only, so one short high-identity supplementary segment
        cannot mark a whole read.

    Both rules — and what the second one costs — are argued at the constants; this is
    only where they are applied. Both use miint's own functions rather than arithmetic
    over the cigar or bit math on `flags`: `alignment_seq_identity` and
    `alignment_is_primary` / `alignment_is_unmapped`.

    The two flag predicates are NOT redundant with each other: `alignment_is_primary`
    means "neither secondary nor supplementary" and is TRUE for an unmapped read, so it
    does not imply mappedness. (In practice a non-matching read emits no alignment row at
    all, so the unmapped conjunct never fires — it is there so the predicate says what it
    means rather than relying on that.) `max_secondary := 0` stops minimap2 emitting
    secondaries in the first place, and DISTINCT collapses any remaining per-read rows to
    one `sequence_idx`.

    Isolated as a seam so unit tests stub the real aligner.
    """
    conn.execute(
        f"INSERT INTO {dest_table} "
        "SELECT DISTINCT read_id AS sequence_idx "
        "FROM align_minimap2(?, index_path := ?, preset := ?, max_secondary := 0) "
        "WHERE alignment_is_primary(flags) "
        "AND NOT alignment_is_unmapped(flags) "
        "AND alignment_seq_identity(cigar, tag_nm, tag_md, ?) >= ?",
        [query_table, str(index_path), preset, _IDENTITY_METHOD, min_identity],
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
                conn,
                duckdb_tmp,
                memory_gb=resolve_duckdb_memory_gb(
                    _DUCKDB_MEMORY_GB, threads=_DUCKDB_THREADS, reserve_gb=_MM2_RESERVE_GB
                ),
                threads=_DUCKDB_THREADS,
            )
            # syndna is the FIRST step of the chain, so it — not lima_export or qc —
            # is where the read set first meets a long-read-only seam. Reject a
            # paired-end set HERE rather than after a full minimap2 pass that the very
            # next consumer would reject anyway. (The gates are client-supplied; see
            # `_partial_mask.assert_single_end`.)
            assert_single_end(conn, reads_sql, "reads", inputs.reads)

            # No incoming mask and no trimming yet: minimap2 aligns the raw read.
            # `sequence2` is deliberately NOT selected into the query: a non-NULL
            # sequence2 puts align_minimap2 into PAIRED-END mode, and the guard above
            # has already established there is none.
            conn.execute(f"CREATE VIEW {_READS} AS SELECT * FROM read_parquet('{reads_sql}')")
            conn.execute(
                f"CREATE VIEW {_QUERY} AS SELECT sequence_idx AS read_id, sequence1 FROM {_READS}"
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

            # Emit the partial mask: one row per read, spike-in hits marked, all else
            # `pass`. Trims are zero (SynDNA does not trim). The mate-trim columns are
            # NULL, not 0: the guard above establishes the read set is single-end, and
            # the read_mask convention is NULL for a single-end read.
            conn.execute(
                "COPY (SELECT r.sequence_idx, "
                f"        CASE WHEN h.sequence_idx IS NOT NULL "
                f"             THEN '{ReadMaskReason.SPIKEIN_SYNDNA.value}' "
                f"             ELSE '{ReadMaskReason.PASS.value}' END AS reason, "
                "        0::UINTEGER AS left_trim1, 0::UINTEGER AS right_trim1, "
                "        NULL::UINTEGER AS left_trim2, NULL::UINTEGER AS right_trim2 "
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
