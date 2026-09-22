"""Mint the qiita.genome rows for assembly runs that predate the inline mint.

`write_assembly_membership` now mints one qiita-origin genome per assembled subject
— per refined bin, per LCG contig, per unbinned contig — and stamps it onto
`assembly_membership.genome_idx`. Runs that completed before that leave the column
NULL. What a NULL costs a reader is on the column's own comment; this converts the
rest.

**Pure Postgres replay: no compute, nothing re-read.** A subject's genome
identity is a hash of `(prep_sample_idx, processing_idx, kind, bin_id)`
(`repositories.assembly.assembly_genome_source_id`), and all four are columns on the
row being stamped. There is nothing to re-assemble and no data-plane read: the same
function the inline mint calls, over rows already in the table.

**Grouped by SUBJECT, not by row.** A refined bin's contigs share one genome, so the
unit of work is the distinct `(prep_sample_idx, processing_idx, kind, bin_id)` — one
upsert for a bin holding hundreds of contigs, then one UPDATE stamping them all.

Contract per this package: dry-run by default, and idempotent — a stamped row is out
of the query, and the genome upsert re-resolves the same `(source, source_id)`. There
is no residue class and so no blocked state, unlike `mask_adapter_hash`: a subject's
identity is a hash of its own columns, so every un-minted subject is writable and
there is nothing to attribute.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import asyncpg
from qiita_common.models import GenomeSource

from ..actions.library import upsert_genomes
from ..repositories.assembly import assembly_genome_source_id

# Subjects per transaction. Both siblings in this package take one transaction per
# UNIT, for resumability; a subject here is one bin or one contig, so an unbinned
# assembly contributes one per contig and per-subject transactions would be tens of
# thousands of round trips for a single prep_sample. Chunking keeps resumability at a
# coarser grain: an interrupted run leaves whole chunks stamped and the rest NULL,
# which the next plan picks up unchanged. Unmeasured — it bounds the array length per
# bind, not a throughput target.
_CHUNK_SIZE = 5_000

# Every un-minted subject, with the contig count it speaks for. `bin_id` is NOT NULL
# on the table, so no COALESCE is needed. Unordered: nothing reads the sequence, and
# the apply matches rows by key rather than by position.
_UNMINTED_SUBJECTS_SQL = (
    "SELECT prep_sample_idx, processing_idx, kind, bin_id, count(*) AS contig_count"
    "  FROM qiita.assembly_membership"
    " WHERE genome_idx IS NULL"
    " GROUP BY prep_sample_idx, processing_idx, kind, bin_id"
)


@dataclass(frozen=True, slots=True)
class Subject:
    """One assembled subject awaiting a genome, and the rows it would stamp."""

    prep_sample_idx: int
    processing_idx: int
    kind: str
    bin_id: str
    contig_count: int

    @property
    def source_id(self) -> str:
        """This subject's `qiita.genome.source_id` — the shared derivation, never a
        second copy of the tuple."""
        return assembly_genome_source_id(
            prep_sample_idx=self.prep_sample_idx,
            processing_idx=self.processing_idx,
            kind=self.kind,
            bin_id=self.bin_id,
        )


@dataclass(frozen=True, slots=True)
class BackfillPlan:
    """What a subsequent apply would do, and what it would leave alone."""

    subjects: list[Subject] = field(default_factory=list)
    already_stamped_rows: int = 0

    @property
    def rows_to_stamp(self) -> int:
        return sum(s.contig_count for s in self.subjects)

    @property
    def genomes_to_mint(self) -> int:
        """Distinct genomes, which is the subject count — the tuple IS the identity,
        so two subjects cannot share one."""
        return len(self.subjects)


async def plan_backfill(pool: asyncpg.Pool) -> BackfillPlan:
    """Read-only: every subject whose rows carry a NULL `genome_idx`.

    A row already stamped is out of the query entirely, so re-running this after an
    apply reports an empty plan. That emptiness is the completeness signal a
    genome-level roll-up over these contigs wants before it runs.
    """
    rows = await pool.fetch(_UNMINTED_SUBJECTS_SQL)
    already = await pool.fetchval(
        "SELECT count(*) FROM qiita.assembly_membership WHERE genome_idx IS NOT NULL"
    )
    # The five selected names ARE the five fields, so a column renamed on one side
    # and not the other is a TypeError here rather than a silently misfiled value.
    return BackfillPlan(
        subjects=[Subject(**dict(r)) for r in rows],
        already_stamped_rows=already or 0,
    )


async def apply_backfill(pool: asyncpg.Pool, plan: BackfillPlan) -> int:
    """Mint each subject's genome and stamp it onto that subject's rows. Returns the
    number of `assembly_membership` rows stamped.

    One transaction per chunk, mint-then-stamp inside it: a genome with no row
    pointing at it is unreachable (nothing else records which run minted it), which
    is the same reason `write_assembly_membership` brackets its own pair.

    The stamp re-filters `genome_idx IS NULL` rather than trusting the plan, which is
    a read taken earlier: an assembly finishing in between mints its own rows inline,
    and re-stamping them would make the reported count describe rows this call did not
    write. The genome upsert is NOT similarly re-checked — a planned subject whose
    rows are gone by now leaves an unreferenced `qiita.genome` row behind, which the
    next run over that subject re-resolves rather than duplicating.
    """
    stamped = 0
    async with pool.acquire() as conn:
        for start in range(0, len(plan.subjects), _CHUNK_SIZE):
            chunk = plan.subjects[start : start + _CHUNK_SIZE]
            async with conn.transaction():
                genome_idxs = await upsert_genomes(
                    conn,
                    [GenomeSource.QIITA.value] * len(chunk),
                    [s.source_id for s in chunk],
                    [s.prep_sample_idx for s in chunk],
                )
                rows = await conn.fetch(
                    "UPDATE qiita.assembly_membership am"
                    "   SET genome_idx = t.genome_idx"
                    "  FROM unnest("
                    "         $1::bigint[], $2::bigint[], $3::text[], $4::text[], $5::bigint[]"
                    "       ) AS t(prep_sample_idx, processing_idx, kind, bin_id, genome_idx)"
                    " WHERE am.prep_sample_idx = t.prep_sample_idx"
                    "   AND am.processing_idx = t.processing_idx"
                    "   AND am.kind = t.kind"
                    "   AND am.bin_id = t.bin_id"
                    "   AND am.genome_idx IS NULL"
                    " RETURNING am.feature_idx",
                    [s.prep_sample_idx for s in chunk],
                    [s.processing_idx for s in chunk],
                    [s.kind for s in chunk],
                    [s.bin_id for s in chunk],
                    genome_idxs,
                )
                stamped += len(rows)
    return stamped
