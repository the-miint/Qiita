"""Backfill `host_taxon_id` onto biosamples that predate the field.

`host_taxon_id` (the host organism a sample was taken FROM) was added as a
biosample global field after every sample we hold had already been ingested, so
no biosample carries it. Until it is populated, `host_filter_resolver` correctly
reports every sample as UNRESOLVED and the submit path has nothing to resolve
against. This module fills it in.

The organism is not derivable from the taxonomy tree. `qiita.terminology_term`
stores only `(term_id, label)` — no parent, no lineage — and NCBI's metagenome
taxa do not sit under their host anyway (`human gut metagenome` is not a
descendant of *Homo sapiens*). So the mapping cannot be computed; it is an
explicit, curated table, `_HOST_BY_SAMPLE_TAXON` below. That is the point: the
judgment is small, visible, and reviewable rather than buried in a heuristic.

Two facts drive each biosample, in this order:

  1. IS IT A CONTROL? A blank has no host of its own regardless of what taxon it
     carries, so this is checked FIRST. The signal is the pre-flight's own
     `is_control` (`input_sample.project_idx IS NULL`), read via
     `preflight.control_samples` — not the sample's name.
  2. WHAT IS THE SAMPLE'S OWN TAXON? `taxon_id` (populated on every biosample we
     hold) names the sample's organism — for a metagenome, the environment it
     came from. `human gut metagenome` implies a human host; `seawater
     metagenome` implies none.

Anything the two facts do not settle is REPORTED, never guessed. A sample whose
taxon is not in the table (e.g. the bare `metagenome` root, which names no
environment and so implies no host) is left unwritten, and stays UNRESOLVED at
submit — which aborts rather than silently passing an un-depleted sample
through. The residue is the curation worklist, not a rounding error.

Idempotent: a biosample that already carries `host_taxon_id` is skipped, so the
backfill can be re-run as curation lands. Adding a taxon to the curated table is
a code change plus a deploy — deliberate, given the table is expected to grow by
one or two entries, not to become a data feed. If it starts changing often, move
it to a seeded lookup table rather than growing this dict.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterable
from dataclasses import dataclass, field
from enum import StrEnum

import asyncpg
from qiita_common.models import (
    BIOSAMPLE_FIELD_HOST_TAXON_ID,
    BIOSAMPLE_FIELD_TAXON_ID,
    MISSING_REASON_CONTROL_SAMPLE,
    MISSING_REASON_NOT_APPLICABLE,
    MISSING_REASON_VALUE_COLUMN,
    NCBI_TAXONOMY_HUMAN_TERM_ID,
    NCBI_TAXONOMY_NAME,
    TERMINOLOGY_TERM_VALUE_COLUMN,
)

from ..preflight import control_samples_from_blob
from ..repositories._sample_helpers import _get_or_create_globally_linked_study_field
from ..repositories.biosample_metadata import BIOSAMPLE_METADATA_SPEC

# ---------------------------------------------------------------------------
# The curated mapping. THIS IS THE JUDGMENT — everything else is mechanism.
# ---------------------------------------------------------------------------
# Keyed on the sample's OWN taxon (`taxon_id`), which for a metagenome names the
# environment it was drawn from. The value is the host that environment implies.
#
# Deliberately NOT exhaustive over NCBI. A taxon that is absent here is not
# "assumed hostless" — it is UNRESOLVED, and the submit path aborts on it. Add a
# row only when the host is genuinely implied by the environment, and note why.
_HOST_BY_SAMPLE_TAXON: dict[str, str | None] = {
    # A human gut metagenome is, by construction, drawn from a human gut.
    "408170": NCBI_TAXONOMY_HUMAN_TERM_ID,
    # Seawater has no host. This is a decision ('not applicable'), not a gap —
    # and it means such samples are NOT host-depleted. Whether human reads should
    # still be removed from a host-LESS sample for contamination/privacy reasons
    # is a separate question this table cannot express; it is tracked separately.
    "1561972": None,
    # Deliberately ABSENT: '256318' (the bare `metagenome` root). It names no
    # environment, so it implies no host. On the live data these are almost
    # entirely blanks, which rule 1 catches before this table is consulted; what
    # is left over is genuinely under-specified metadata and must be curated, not
    # guessed at here.
}


@dataclass
class ControlScan:
    """What the pre-flight sweep found — the controls, and what it could not read.

    The two failure counts are surfaced rather than dropped because a missed blank
    resolves UNRESOLVED and aborts its whole pool. "The pre-flight said blank but
    we could not match it" is a different problem from "this sample's metadata
    needs curating", and an operator has to be able to tell them apart.
    """

    accessions: set[str] = field(default_factory=set)
    unreadable_pools: list[int] = field(default_factory=list)
    controls_without_accession: int = 0


class HostTaxonSource(StrEnum):
    """How a biosample's host assignment was decided — reported per sample so an
    operator can see which rows rest on the pre-flight and which on the curated
    taxon mapping."""

    CONTROL = "control"  # the pre-flight says it is a blank
    TAXON = "taxon"  # its own taxon implies a host
    NO_HOST = "no_host"  # its own taxon implies NO host ('not applicable')
    UNRESOLVED = "unresolved"  # nothing settles it — left unwritten, reported


@dataclass(frozen=True, slots=True)
class BiosampleAssignment:
    """What the backfill would write (or refuse to write) for one biosample."""

    biosample_idx: int
    study_idx: int
    source: HostTaxonSource
    # Exactly one of these is set, and only when source is not UNRESOLVED.
    host_term_id: str | None = None
    missing_reason: str | None = None
    # Carried for the report so an UNRESOLVED row says WHY.
    sample_taxon_term_id: str | None = None
    sample_taxon_label: str | None = None


@dataclass
class BackfillPlan:
    """Everything the backfill would do, computed without writing anything."""

    assignments: list[BiosampleAssignment] = field(default_factory=list)
    # Biosamples that already carry host_taxon_id — the idempotency skip.
    already_populated: int = 0
    # Biosamples carrying NO taxon_id at all. The backfill has no input for
    # these; counted separately from UNRESOLVED so the report distinguishes
    # "we cannot map its taxon" from "it has no taxon".
    no_taxon: int = 0
    # What the pre-flight sweep could not read. Reported, never silent.
    control_scan: ControlScan = field(default_factory=ControlScan)

    def writable(self) -> list[BiosampleAssignment]:
        return [a for a in self.assignments if a.source is not HostTaxonSource.UNRESOLVED]

    def unresolved(self) -> list[BiosampleAssignment]:
        return [a for a in self.assignments if a.source is HostTaxonSource.UNRESOLVED]


def classify(
    *,
    biosample_idx: int,
    study_idx: int,
    is_control: bool,
    sample_taxon_term_id: str | None,
    sample_taxon_label: str | None,
) -> BiosampleAssignment:
    """Decide one biosample's host assignment. Pure — no DB, no pre-flight.

    Control wins over taxon: a blank has no host of its own no matter what taxon
    it carries, and on the live data blanks DO carry a taxon (the bare
    `metagenome` root). Checking taxon first would leave every blank UNRESOLVED
    and abort every pool, since every pool contains blanks.
    """
    if is_control:
        return BiosampleAssignment(
            biosample_idx=biosample_idx,
            study_idx=study_idx,
            source=HostTaxonSource.CONTROL,
            missing_reason=MISSING_REASON_CONTROL_SAMPLE,
            sample_taxon_term_id=sample_taxon_term_id,
            sample_taxon_label=sample_taxon_label,
        )

    # An absent taxon, or one the curated table does not cover, is left for a
    # human. `not in` rather than `.get(...) is None` — the table maps a taxon to
    # None to MEAN "this environment has no host", which is a decision, and that
    # is a different answer from "we have no row for this taxon".
    if sample_taxon_term_id not in _HOST_BY_SAMPLE_TAXON:
        return BiosampleAssignment(
            biosample_idx=biosample_idx,
            study_idx=study_idx,
            source=HostTaxonSource.UNRESOLVED,
            sample_taxon_term_id=sample_taxon_term_id,
            sample_taxon_label=sample_taxon_label,
        )

    host_term_id = _HOST_BY_SAMPLE_TAXON[sample_taxon_term_id]
    if host_term_id is None:
        return BiosampleAssignment(
            biosample_idx=biosample_idx,
            study_idx=study_idx,
            source=HostTaxonSource.NO_HOST,
            missing_reason=MISSING_REASON_NOT_APPLICABLE,
            sample_taxon_term_id=sample_taxon_term_id,
            sample_taxon_label=sample_taxon_label,
        )
    return BiosampleAssignment(
        biosample_idx=biosample_idx,
        study_idx=study_idx,
        source=HostTaxonSource.TAXON,
        host_term_id=host_term_id,
        sample_taxon_term_id=sample_taxon_term_id,
        sample_taxon_label=sample_taxon_label,
    )


async def fetch_control_accessions(pool: asyncpg.Pool) -> ControlScan:
    """Union the control biosample accessions across every pool's pre-flight.

    Per-pool guarded. A single unreadable blob must NOT take down a 3300-row
    backfill, and it must not be anonymous either: the pool idx is selected so an
    unreadable one can be NAMED in the report. The pools that did parse still
    contribute their controls, and every blank in an unreadable pool simply falls
    to UNRESOLVED — which aborts that pool at submit rather than mis-depleting it.

    (Test databases hold deliberately-corrupt placeholder blobs, so this is not a
    hypothetical: without the guard, one unrelated test's `b"X"` blob would fail
    every run of this backfill.)
    """
    rows = await pool.fetch(
        "SELECT idx, run_preflight_blob"
        "  FROM qiita.sequenced_pool"
        " WHERE run_preflight_blob IS NOT NULL"
    )
    scan = ControlScan()
    for row in rows:
        try:
            controls = control_samples_from_blob(row["run_preflight_blob"])
        except sqlite3.DatabaseError, ValueError, KeyError, IndexError, TypeError:
            # Unreadable or shape-drifted. Named, not swallowed.
            scan.unreadable_pools.append(row["idx"])
            continue
        scan.accessions |= controls.accessions
        scan.controls_without_accession += controls.unusable
    return scan


async def plan_backfill(pool: asyncpg.Pool) -> BackfillPlan:
    """Compute what the backfill would do. Writes NOTHING.

    Reads every biosample that does not already carry `host_taxon_id`, together
    with its own `taxon_id` and the study that field hangs off, then classifies
    each against the pre-flight control set and the curated taxon table.

    The study is taken from the biosample's EXISTING `taxon_id` row rather than
    picked from `biosample_to_study`: a biosample can belong to several studies,
    and `biosample_to_study` names no primary, so there would be no principled
    choice. The study whose field already carries the sample's taxonomy is the
    obvious home for the host derived FROM that taxonomy, and it is guaranteed to
    exist for every row we can map.
    """
    # Prove the write CAN land before reporting that it would. These raise on an
    # unseeded term / reason; running them only under --execute would let a green
    # dry-run be followed by a crash, which is exactly the promise a dry-run makes.
    async with pool.acquire() as conn:
        await _fetch_ncbi_term_idxs(conn)
        await _fetch_missing_reason_idxs(conn)

    scan = await fetch_control_accessions(pool)

    rows = await pool.fetch(
        "SELECT bm.biosample_idx,"
        "       bsf.study_idx,"
        "       b.biosample_accession,"
        "       tt.term_id AS taxon_term_id,"
        "       tt.label   AS taxon_label"
        "  FROM qiita.biosample_metadata bm"
        "  JOIN qiita.biosample_global_field bgf"
        "    ON bgf.idx = bm.global_field_idx AND bgf.internal_name = $1"
        "  JOIN qiita.biosample_study_field bsf ON bsf.idx = bm.biosample_study_field_idx"
        "  JOIN qiita.biosample b ON b.idx = bm.biosample_idx"
        "  LEFT JOIN qiita.terminology_term tt ON tt.idx = bm.value_terminology_term_idx"
        " WHERE NOT EXISTS ("
        "          SELECT 1"
        "            FROM qiita.biosample_metadata h"
        "            JOIN qiita.biosample_global_field hgf"
        "              ON hgf.idx = h.global_field_idx AND hgf.internal_name = $2"
        "           WHERE h.biosample_idx = bm.biosample_idx"
        "      )"
        " ORDER BY bm.biosample_idx",
        BIOSAMPLE_FIELD_TAXON_ID,
        BIOSAMPLE_FIELD_HOST_TAXON_ID,
    )

    already_populated = await pool.fetchval(
        "SELECT count(*)"
        "  FROM qiita.biosample_metadata bm"
        "  JOIN qiita.biosample_global_field bgf"
        "    ON bgf.idx = bm.global_field_idx AND bgf.internal_name = $1",
        BIOSAMPLE_FIELD_HOST_TAXON_ID,
    )

    # Biosamples carrying NO taxon_id at all. The candidate query above is driven
    # FROM the taxon_id row (that row is both the backfill's input and the source
    # of the study to hang the value on), so a biosample without one cannot appear
    # in it — and would be silently invisible rather than reported. Count it here
    # so the report says so out loud. These are not writable by this backfill:
    # with no taxon row there is no study to attach the value to, and
    # biosample_to_study names no primary, so there would be no principled choice.
    no_taxon = await pool.fetchval(
        "SELECT count(*)"
        "  FROM qiita.biosample b"
        " WHERE NOT EXISTS ("
        "          SELECT 1"
        "            FROM qiita.biosample_metadata h"
        "            JOIN qiita.biosample_global_field hgf"
        "              ON hgf.idx = h.global_field_idx AND hgf.internal_name = $2"
        "           WHERE h.biosample_idx = b.idx"
        "      )"
        "   AND NOT EXISTS ("
        "          SELECT 1"
        "            FROM qiita.biosample_metadata bm"
        "            JOIN qiita.biosample_global_field bgf"
        "              ON bgf.idx = bm.global_field_idx AND bgf.internal_name = $1"
        "           WHERE bm.biosample_idx = b.idx"
        "      )",
        BIOSAMPLE_FIELD_TAXON_ID,
        BIOSAMPLE_FIELD_HOST_TAXON_ID,
    )

    plan = BackfillPlan(
        already_populated=already_populated or 0,
        no_taxon=no_taxon or 0,
        control_scan=scan,
    )
    for row in rows:
        accession = row["biosample_accession"]
        plan.assignments.append(
            classify(
                biosample_idx=row["biosample_idx"],
                study_idx=row["study_idx"],
                is_control=accession is not None and accession in scan.accessions,
                sample_taxon_term_id=row["taxon_term_id"],
                sample_taxon_label=row["taxon_label"],
            )
        )
    return plan


async def apply_backfill(
    pool: asyncpg.Pool,
    assignments: Iterable[BiosampleAssignment],
    *,
    principal_idx: int,
) -> int:
    """Write the plan's assignments. Returns the number of metadata rows created.

    Each biosample's write is one transaction: get-or-create the study's
    `host_taxon_id` field (bound to the GLOBAL field — that binding is what makes
    the trigger populate `biosample_metadata.global_field_idx`, which is the
    column the resolver reads) and insert the value row.

    ONE TRANSACTION PER BIOSAMPLE, deliberately — not one for the whole run. A
    partial run is then safe to resume: re-planning drops whatever was written, so
    the operator re-runs and it converges. A single ~3300-row transaction would
    buy atomicity nobody needs and lose resumability, which is the property that
    actually matters for a backfill.

    Re-runnable. A biosample that already has the field is not in the plan, and
    the partial unique index `biosample_metadata_one_value_per_global_field` is
    the backstop if two runs race — note that surfaces as a UniqueViolationError,
    i.e. a crash, not a silent no-op; the caller reports what was written so far.
    """
    written = 0
    async with pool.acquire() as conn:
        host_field = await conn.fetchrow(
            "SELECT idx, display_name FROM qiita.biosample_global_field WHERE internal_name = $1",
            BIOSAMPLE_FIELD_HOST_TAXON_ID,
        )
        if host_field is None:
            raise RuntimeError(
                f"{BIOSAMPLE_FIELD_HOST_TAXON_ID} global field is not seeded;"
                " run the migrations first"
            )
        host_gf_idx = host_field["idx"]
        # Take the display name from the seeded global field rather than
        # re-spelling it. `biosample_study_field` is unique on
        # (study_idx, display_name), so a guess that drifted from the seed would
        # not fail — it would quietly create a SECOND study field bound to the
        # same global field under a different name.
        display_name = host_field["display_name"]
        term_idx_by_id = await _fetch_ncbi_term_idxs(conn)
        reason_idx_by_name = await _fetch_missing_reason_idxs(conn)
        # A handful of studies cover thousands of biosamples; without this the
        # get-or-create is one round trip per row.
        field_idx_by_study: dict[int, int] = {}

        for a in assignments:
            if a.source is HostTaxonSource.UNRESOLVED:
                continue
            async with conn.transaction():
                field_idx = field_idx_by_study.get(a.study_idx)
                if field_idx is None:
                    field_idx, _ = await _get_or_create_globally_linked_study_field(
                        conn,
                        spec=BIOSAMPLE_METADATA_SPEC,
                        study_idx=a.study_idx,
                        global_field_idx=host_gf_idx,
                        display_name=display_name,
                        created_by_idx=principal_idx,
                    )
                    field_idx_by_study[a.study_idx] = field_idx
                if a.host_term_id is not None:
                    column = TERMINOLOGY_TERM_VALUE_COLUMN
                    value = term_idx_by_id[a.host_term_id]
                else:
                    column = MISSING_REASON_VALUE_COLUMN
                    value = reason_idx_by_name[a.missing_reason]
                # The column name is interpolated, so pin it to the two module-
                # controlled constants above rather than trusting the branch.
                assert column in (TERMINOLOGY_TERM_VALUE_COLUMN, MISSING_REASON_VALUE_COLUMN)
                await conn.execute(
                    "INSERT INTO qiita.biosample_metadata"
                    f" (biosample_idx, biosample_study_field_idx, {column}, created_by_idx)"
                    " VALUES ($1, $2, $3, $4)",
                    a.biosample_idx,
                    field_idx,
                    value,
                    principal_idx,
                )
            written += 1
    return written


async def _fetch_ncbi_term_idxs(conn: asyncpg.Connection) -> dict[str, int]:
    """Resolve every host term_id the curated table can assign, up front, so a
    missing seed fails once here rather than partway through a 3000-row write."""
    wanted = sorted({t for t in _HOST_BY_SAMPLE_TAXON.values() if t is not None})
    rows = await conn.fetch(
        "SELECT tt.term_id, tt.idx"
        "  FROM qiita.terminology_term tt"
        "  JOIN qiita.terminology t ON t.idx = tt.terminology_idx AND t.name = $2"
        " WHERE tt.term_id = ANY($1::text[])",
        wanted,
        NCBI_TAXONOMY_NAME,
    )
    found = {r["term_id"]: r["idx"] for r in rows}
    missing = set(wanted) - set(found)
    if missing:
        raise RuntimeError(f"NCBI Taxonomy terms not seeded: {sorted(missing)}")
    return found


async def _fetch_missing_reason_idxs(conn: asyncpg.Connection) -> dict[str, int]:
    """Same, for the two missing-reasons the backfill assigns."""
    wanted = [MISSING_REASON_NOT_APPLICABLE, MISSING_REASON_CONTROL_SAMPLE]
    rows = await conn.fetch(
        "SELECT name, idx FROM qiita.missing_value_reason WHERE name = ANY($1::text[])",
        wanted,
    )
    found = {r["name"]: r["idx"] for r in rows}
    missing = set(wanted) - set(found)
    if missing:
        raise RuntimeError(f"missing_value_reason rows not seeded: {sorted(missing)}")
    return found
