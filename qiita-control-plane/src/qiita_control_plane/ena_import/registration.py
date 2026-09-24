"""ENA study registration composer: turns one resolved ENA study
(`EnaStudyHeader` + `EnaRunRecord` list) into `biosample` / `prep_sample` /
`sequenced_sample` rows in an existing study, idempotently -- re-imports and
cross-study biosample overlap converge, never duplicate.

Order of operations:

  1. Map each run's `instrument_platform` to `qiita.platform`
     (`platform_mapping.map_ena_platform`), isolated per run: an unmappable
     value fails only that run. Successfully-mapped runs are grouped by platform.

  2. Per distinct mapped platform: get-or-create one `sequencing_run`
     (`instrument_run_id = "{study_accession}:{platform}"`) and pick the pool new
     runs go into (see `_resolve_platform_pools`).

  3. Per ENA run, in its own savepoint inside the registration transaction
     (per-run atomicity: a partial failure rolls back only this run): resolve
     or import the biosample by ENA sample accession (cross-study de-dup; only
     the import writes metadata) and link it to the study. Skip if a
     sequenced_sample already carries this run's `ena_run_accession`
     (idempotent re-import), else map library_strategy/library_source to a
     curated prep_protocol name, import via `import_sequenced_prep_sample`, and
     preserve all four ENA `library_*` fields (whitespace-trimmed, otherwise as
     deposited) as study-local prep_sample metadata. The four fields are
     resolved and vetted once for the study, before any run is written
     (`_ensure_library_fields`); each run then only fills values
     (`_library_metadata`).

Steps 2-3 share one transaction so the advisory lock taken in step 2 is held
until the runs commit; see `repositories.sequencing_run.lock_sequencing_run`
for why the download-roster read needs that window closed.

A per-run failure, harmonization included, is caught and reported on its
`EnaRunRegistrationOutcome`. No read bytes or batch fan-out here -- those live
in the download workflow and the batch driver.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from enum import StrEnum
from operator import itemgetter

import asyncpg
from qiita_common.models import FieldDataType, Platform, WorkTicketState
from qiita_common.models.ena import (
    EnaRunRecord,
    EnaSampleAttributes,
    EnaStudyHeader,
)

from qiita_control_plane.repositories import require_transaction
from qiita_control_plane.repositories._sample_helpers import (
    fetch_metadata_checklist_idx_by_name,
    insert_entity_to_study,
    resolve_local_study_field,
    write_local_metadata_on_resolved_field,
)
from qiita_control_plane.repositories.biosample import (
    resolve_or_import_biosample_by_ena_accession,
)
from qiita_control_plane.repositories.biosample_metadata import BIOSAMPLE_METADATA_SPEC
from qiita_control_plane.repositories.ena_import_batch import (
    fetch_sequenced_pool_download_states,
)
from qiita_control_plane.repositories.prep_protocol import fetch_prep_protocol_idx_by_name
from qiita_control_plane.repositories.prep_sample_metadata import PREP_SAMPLE_METADATA_SPEC
from qiita_control_plane.repositories.sequenced_sample import (
    fetch_sequenced_sample_idxs_by_ena_run_accession,
    import_sequenced_prep_sample,
)
from qiita_control_plane.repositories.sequencing_run import (
    insert_sequenced_pool,
    insert_sequencing_run,
    lock_sequencing_run,
)

from .harmonization import HarmonizationResult, build_biosample_metadata
from .platform_mapping import UnmappableEnaPlatformError, map_ena_platform
from .protocol_mapping import map_ena_run_to_prep_protocol_name
from .submit import DOWNLOAD_ENA_STUDY_ACTION_ID, DOWNLOAD_ENA_STUDY_ACTION_VERSION

# The ENA default sample checklist (seeded by db/migrations). Every ENA-imported
# biosample is bound to it -- resolved once per study import, not per run.
_ERC000011_CHECKLIST_NAME = "ERC000011"

# Holds ENA's submitter-supplied `sample_alias`. Named for what it carries in
# both cases: the alias is absent on some samples (DDBJ-brokered ones return it
# empty), and the sample accession stands in.
ENA_SAMPLE_ID_FIELD_NAME = "ena sample id"

# Study-local display names for the four deposited ENA `library_*` fields.
# Lowercase and "ena "-prefixed like ENA_SAMPLE_ID_FIELD_NAME: this import is
# what writes them, and staying clear of the byte-identical names of the
# globals prune_prep_sample_global_fields deleted keeps a re-seed of those
# globals from shadowing these local fields with a conflicting global.
ENA_LIBRARY_STRATEGY_FIELD_NAME = "ena library strategy"
ENA_LIBRARY_SOURCE_FIELD_NAME = "ena library source"
ENA_LIBRARY_SELECTION_FIELD_NAME = "ena library selection"
ENA_LIBRARY_LAYOUT_FIELD_NAME = "ena library layout"

_LIBRARY_FIELD_DISPLAY_NAMES = (
    ENA_LIBRARY_STRATEGY_FIELD_NAME,
    ENA_LIBRARY_SOURCE_FIELD_NAME,
    ENA_LIBRARY_SELECTION_FIELD_NAME,
    ENA_LIBRARY_LAYOUT_FIELD_NAME,
)


class EnaRunRegistrationStatus(StrEnum):
    """Per-run outcome discriminator for `EnaRunRegistrationOutcome.status`."""

    REGISTERED = "registered"
    SKIPPED_ALREADY_PRESENT = "skipped_already_present"
    FAILED = "failed"


@dataclass(frozen=True)
class EnaRunRegistrationOutcome:
    """One ENA run's registration outcome.

    `prep_sample_idx` is set only on `REGISTERED`; `sequenced_sample_idx` on
    both `REGISTERED` and `SKIPPED_ALREADY_PRESENT`; `failure_reason` only on
    `FAILED`. `harmonization` is set (non-`FAILED`) only when this call newly
    created the biosample -- write-once: a reused/re-imported biosample carries
    `None` because no harmonization write ran.
    """

    run_accession: str
    status: EnaRunRegistrationStatus
    prep_sample_idx: int | None = None
    sequenced_sample_idx: int | None = None
    failure_reason: str | None = None
    harmonization: HarmonizationResult | None = None


# A pool whose latest download ticket ended in one of these is downloaded
# afresh: a new ticket re-reads its roster, so it may still take new runs.
_RESUBMITTABLE_DOWNLOAD_TICKET_STATES = frozenset(
    {WorkTicketState.FAILED.value, WorkTicketState.CANCELLED.value}
)


def download_ticket_covers_pool(work_ticket_state: str | None) -> bool:
    """Whether a pool's latest download ticket has downloaded, or is downloading,
    the pool: reuse it, and put no new runs in that pool (its roster is read once
    at dispatch, so runs added after it would never be downloaded)."""
    return (
        work_ticket_state is not None
        and work_ticket_state not in _RESUBMITTABLE_DOWNLOAD_TICKET_STATES
    )


async def fetch_download_pool_states(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection, sequencing_run_idx: int
) -> list[asyncpg.Record]:
    """`fetch_sequenced_pool_download_states` for the download-ena-study action."""
    return await fetch_sequenced_pool_download_states(
        pool_or_conn,
        sequencing_run_idx=sequencing_run_idx,
        action_id=DOWNLOAD_ENA_STUDY_ACTION_ID,
        action_version=DOWNLOAD_ENA_STUDY_ACTION_VERSION,
    )


@dataclass(frozen=True)
class CreatedPool:
    """One `(platform, sequenced_pool_idx, sequencing_run_idx)` triple on a
    platform's sequencing_run, created or pre-existing. `platform` is the
    `Platform` enum value as plain `str`.
    """

    platform: str
    sequenced_pool_idx: int
    sequencing_run_idx: int


@dataclass(frozen=True)
class EnaStudyRegistrationResult:
    """Composite result of one `register_ena_study` call."""

    study_idx: int
    ena_runs: list[EnaRunRegistrationOutcome] = field(default_factory=list)
    created_pools: list[CreatedPool] = field(default_factory=list)


async def register_ena_study(
    pool: asyncpg.Pool,
    *,
    study_idx: int,
    study_header: EnaStudyHeader,
    ena_runs: list[EnaRunRecord],
    sample_attributes: list[EnaSampleAttributes],
    owner_idx: int,
    caller_idx: int,
) -> EnaStudyRegistrationResult:
    """Register one resolved ENA study's runs and samples into `study_idx`.

    The caller resolves the study (`get_or_create_study_by_ena_accessions`) and
    decides whether importing into it is allowed, so this never creates one.

    Never raises for a per-run failure (see `EnaRunRegistrationOutcome`); an
    unmappable `instrument_platform` is one such isolated per-run failure.
    It does raise -- before any run is written -- when a study-local field at
    one of the four `library_*` display names cannot hold these values (see
    `_ensure_library_fields`), so the whole accession fails loudly rather
    than run by run.
    """
    # A run whose sample has no entry here harmonizes against an empty map
    # rather than failing.
    attrs_by_sample_accession: dict[str, EnaSampleAttributes] = {
        sa.sample_accession: sa for sa in sample_attributes
    }

    async with pool.acquire() as conn:
        metadata_checklist_idx = await fetch_metadata_checklist_idx_by_name(
            conn, _ERC000011_CHECKLIST_NAME
        )

        # Map each run's platform, isolated per run: an unmappable platform fails
        # only that run. Only successfully-mapped runs are grouped by platform.
        ena_runs_by_platform: dict[Platform, list[EnaRunRecord]] = defaultdict(list)
        already_present = await fetch_sequenced_sample_idxs_by_ena_run_accession(
            conn, values=[ena_run.run_accession for ena_run in ena_runs]
        )
        outcomes_by_accession: dict[str, EnaRunRegistrationOutcome] = {}
        platform_by_accession: dict[str, Platform] = {}
        for ena_run in ena_runs:
            try:
                platform = map_ena_platform(ena_run.instrument_platform)
            except UnmappableEnaPlatformError as exc:
                outcomes_by_accession[ena_run.run_accession] = EnaRunRegistrationOutcome(
                    run_accession=ena_run.run_accession,
                    status=EnaRunRegistrationStatus.FAILED,
                    failure_reason=str(exc),
                )
                continue
            ena_runs_by_platform[platform].append(ena_run)
            platform_by_accession[ena_run.run_accession] = platform

        # Resolve the four library_* fields once for the study, before any run
        # is written -- but only when a run will be written: a pure re-import
        # of already-present runs adds no values and so mints no fields. Ahead
        # of pool resolution too, so a field-shape clash mints nothing at all.
        # Outside the transaction below: the field rows are per-study
        # constants that survive a failed attempt (see _ensure_library_fields).
        library_field_idxs: dict[str, int] = {}
        if any(ena_run.run_accession not in already_present for ena_run in ena_runs):
            library_field_idxs = await _ensure_library_fields(
                conn, study_idx=study_idx, created_by_idx=caller_idx
            )

        # One transaction spans pool resolution and every run insert so the
        # sequencing_run lock `_resolve_platform_pools` takes is held until the
        # runs commit (see `lock_sequencing_run`); savepoints below keep the
        # per-run rollback. Platforms resolve in sorted order so concurrent
        # registrations take those advisory keys in a common order.
        async with conn.transaction():
            target_pool_idx_by_platform: dict[Platform, int | None] = {}
            created_pools: list[CreatedPool] = []
            for platform, platform_runs in sorted(ena_runs_by_platform.items(), key=itemgetter(0)):
                platform_pools, target_pool_idx = await _resolve_platform_pools(
                    conn,
                    study_accession=study_header.study_accession,
                    platform=platform,
                    created_by_idx=caller_idx,
                    needs_pool=any(r.run_accession not in already_present for r in platform_runs),
                )
                target_pool_idx_by_platform[platform] = target_pool_idx
                created_pools.extend(platform_pools)

            # Runs insert in one global (sample_accession, run_accession)
            # order, not ENA's per-platform order: biosample de-dup holds
            # unique-row locks study-wide now, so concurrent imports sharing
            # NEW ENA samples must acquire those keys in a common order or
            # Postgres deadlocks one of them -- an abort that folds into a
            # per-run FAILED nothing redrives.
            runs_in_insert_order = sorted(
                (r for r in ena_runs if r.run_accession in platform_by_accession),
                key=lambda r: (r.sample_accession, r.run_accession),
            )
            for ena_run in runs_in_insert_order:
                platform = platform_by_accession[ena_run.run_accession]
                outcomes_by_accession[ena_run.run_accession] = await _register_one_ena_run(
                    conn,
                    ena_run=ena_run,
                    study_idx=study_idx,
                    platform=platform,
                    sequenced_pool_idx=target_pool_idx_by_platform[platform],
                    owner_idx=owner_idx,
                    caller_idx=caller_idx,
                    metadata_checklist_idx=metadata_checklist_idx,
                    attrs_by_sample_accession=attrs_by_sample_accession,
                    library_field_idxs=library_field_idxs,
                )

    # Return per-run outcomes in the caller's input order.
    outcomes = [outcomes_by_accession[ena_run.run_accession] for ena_run in ena_runs]

    return EnaStudyRegistrationResult(
        study_idx=study_idx,
        ena_runs=outcomes,
        created_pools=created_pools,
    )


async def _resolve_platform_pools(
    conn: asyncpg.Connection,
    *,
    study_accession: str,
    platform: Platform,
    created_by_idx: int,
    needs_pool: bool,
) -> tuple[list[CreatedPool], int | None]:
    """Get-or-create the (study, platform) sequencing_run and return every pool
    on it, plus the pool new runs go into: the newest one no download ticket
    covers, else a new pool when `needs_pool`, else None.

    Takes the sequencing_run advisory key and returns still holding it -- the
    caller commits only after inserting its runs, so selection-through-
    insertion is one critical section against the roster read (see
    `lock_sequencing_run` for the race that closes). That same lock also makes
    a concurrent batch for the same (study, platform) reuse the pool rather
    than mint a second, since the no-preflight insert has no constraint to
    arbitrate (see `insert_sequenced_pool`).

    Requires a wrapping transaction at entry, before `insert_sequencing_run`
    writes anything: without one, the advisory lock this function must return
    holding would vanish with the statement."""
    require_transaction(conn)
    sequencing_run_idx, _ = await insert_sequencing_run(
        conn,
        instrument_run_id=f"{study_accession}:{platform.value}",
        platform=platform,
        created_by_idx=created_by_idx,
    )
    await lock_sequencing_run(conn, sequencing_run_idx=sequencing_run_idx)
    states = await fetch_download_pool_states(conn, sequencing_run_idx)
    pool_idxs = [s["sequenced_pool_idx"] for s in states]
    open_pool_idxs = [
        s["sequenced_pool_idx"]
        for s in states
        if not download_ticket_covers_pool(s["work_ticket_state"])
    ]
    target_pool_idx = open_pool_idxs[-1] if open_pool_idxs else None
    if target_pool_idx is None and needs_pool:
        target_pool_idx, _ = await insert_sequenced_pool(
            conn,
            sequencing_run_idx=sequencing_run_idx,
            created_by_idx=created_by_idx,
        )
        pool_idxs.append(target_pool_idx)
    pools = [
        CreatedPool(
            platform=platform.value,
            sequenced_pool_idx=idx,
            sequencing_run_idx=sequencing_run_idx,
        )
        for idx in pool_idxs
    ]
    return pools, target_pool_idx


def _library_metadata(ena_run: EnaRunRecord) -> dict[str, str]:
    """The run's four deposited `library_*` values, whitespace-trimmed, keyed
    by the study-local display name.

    A field ENA left blank yields no entry, and the reader cannot tell the
    causes apart: ENA deposited nothing, ENA deposited only whitespace (the
    model normalizes both to None), or the run was imported before this
    writer existed -- already-present runs are skipped, so a re-import does
    not backfill them. Writing no row rather than a placeholder is deliberate:
    the slot is a record of the deposit, not a field to fill.
    """
    deposited = {
        ENA_LIBRARY_STRATEGY_FIELD_NAME: ena_run.library_strategy,
        ENA_LIBRARY_SOURCE_FIELD_NAME: ena_run.library_source,
        ENA_LIBRARY_SELECTION_FIELD_NAME: ena_run.library_selection,
        ENA_LIBRARY_LAYOUT_FIELD_NAME: ena_run.library_layout,
    }
    return {
        display_name: value.strip()
        for display_name, value in deposited.items()
        if value is not None
    }


async def _ensure_library_fields(
    conn: asyncpg.Connection, *, study_idx: int, created_by_idx: int
) -> dict[str, int]:
    """Get-or-create the study's four `library_*` fields once, before any run
    is written; return {display_name: study_field_idx} for the per-run value
    writes.

    Resolving here instead of inside each run's transaction is what makes a
    shape clash -- a pre-existing field at one of these names that is non-text,
    unique within the study, or globally linked -- fail the whole study loudly
    before the first run, instead of rolling runs back one by one mid-loop:
    the run's own data is fine, only the slot is wrong, so the run must not
    pay for it. The field rows are per-study constants and survive a later
    failure; a retry re-resolves them identically.
    """
    field_idxs: dict[str, int] = {}
    async with conn.transaction():
        for display_name in _LIBRARY_FIELD_DISPLAY_NAMES:
            field_idx, _created, _row = await resolve_local_study_field(
                conn,
                spec=PREP_SAMPLE_METADATA_SPEC,
                study_idx=study_idx,
                display_name=display_name,
                created_by_idx=created_by_idx,
                # Library values repeat across runs by construction, so a
                # unique field would take the second identical value down.
                enforce_unique_in_study=True,
            )
            field_idxs[display_name] = field_idx
    return field_idxs


async def _register_one_ena_run(
    conn: asyncpg.Connection,
    *,
    ena_run: EnaRunRecord,
    study_idx: int,
    platform: Platform,
    sequenced_pool_idx: int | None,
    owner_idx: int,
    caller_idx: int,
    metadata_checklist_idx: int,
    attrs_by_sample_accession: dict[str, EnaSampleAttributes],
    library_field_idxs: dict[str, int],
) -> EnaRunRegistrationOutcome:
    """Register one ENA run in its own savepoint within the registration
    transaction (per-run atomicity: a partial failure rolls back only this
    run). It never raises: every failure mode (platform/protocol-mapping,
    harmonization, composer/DB) is folded into a `failed` outcome. A
    harmonization gap is not a failure mode -- only a genuine parse/collision
    failure inside the biosample import raises, caught here like any other."""
    try:
        async with conn.transaction():
            sample_attrs = attrs_by_sample_accession.get(ena_run.sample_accession)
            metadata, local_metadata, harmonization = build_biosample_metadata(
                sample_attrs.attributes if sample_attrs is not None else {}
            )
            biosample_idx, biosample_created = await resolve_or_import_biosample_by_ena_accession(
                conn,
                ena_sample_accession=ena_run.sample_accession,
                study_idx=study_idx,
                owner_idx=owner_idx,
                caller_idx=caller_idx,
                owner_biosample_id_field_name=ENA_SAMPLE_ID_FIELD_NAME,
                owner_biosample_id_value=(ena_run.sample_alias or "").strip()
                or ena_run.sample_accession,
                metadata=metadata,
                local_metadata=local_metadata,
                metadata_checklist_idx=metadata_checklist_idx,
            )
            # The import links the study itself; a biosample this study is
            # reusing needs the link added, and must precede the composer below,
            # whose prep_sample_to_study insert fires reject_without_biosample_link.
            if not biosample_created:
                await insert_entity_to_study(
                    conn,
                    spec=BIOSAMPLE_METADATA_SPEC,
                    entity_idx=biosample_idx,
                    study_idx=study_idx,
                    created_by_idx=caller_idx,
                    on_conflict="ignore",
                )

            # Only the import writes metadata, so a study reusing a biosample
            # never rewrites what the first import put in the shared global slot.
            harmonization_result: HarmonizationResult | None = (
                harmonization if biosample_created else None
            )

            existing = await fetch_sequenced_sample_idxs_by_ena_run_accession(
                conn, values=[ena_run.run_accession]
            )
            if ena_run.run_accession in existing:
                return EnaRunRegistrationOutcome(
                    run_accession=ena_run.run_accession,
                    status=EnaRunRegistrationStatus.SKIPPED_ALREADY_PRESENT,
                    sequenced_sample_idx=existing[ena_run.run_accession],
                    harmonization=harmonization_result,
                )

            if sequenced_pool_idx is None:
                raise RuntimeError(
                    f"{ena_run.run_accession} was not registered when this import began,"
                    " but no pool was opened for it"
                )
            protocol_name = map_ena_run_to_prep_protocol_name(
                library_strategy=ena_run.library_strategy,
                library_source=ena_run.library_source,
                platform=platform,
            )
            prep_protocol_idx = await fetch_prep_protocol_idx_by_name(conn, protocol_name)

            result = await import_sequenced_prep_sample(
                conn,
                sequenced_pool_idx=sequenced_pool_idx,
                biosample_idx=biosample_idx,
                prep_protocol_idx=prep_protocol_idx,
                owner_idx=owner_idx,
                sequenced_pool_item_id=ena_run.run_accession,
                # This `metadata` is prep_sample-level (biosample attributes are
                # harmonized above); no current resolver output populates it.
                metadata={},
                primary_study_idx=study_idx,
                caller_idx=caller_idx,
                ena_experiment_accession=ena_run.experiment_accession,
                ena_run_accession=ena_run.run_accession,
            )
            # Value writes only: _ensure_library_fields vetted the four
            # fields before the loop. The composer's study-link insert above
            # is what lets the unconditional reject_if_link_retired trigger
            # pass on each of these inserts.
            for display_name, value in _library_metadata(ena_run).items():
                await write_local_metadata_on_resolved_field(
                    conn,
                    spec=PREP_SAMPLE_METADATA_SPEC,
                    entity_idx=result.prep_sample_idx,
                    study_idx=study_idx,
                    study_field_idx=library_field_idxs[display_name],
                    display_name=display_name,
                    data_type=FieldDataType.TEXT,
                    value=value,
                    caller_idx=caller_idx,
                )
            return EnaRunRegistrationOutcome(
                run_accession=ena_run.run_accession,
                status=EnaRunRegistrationStatus.REGISTERED,
                prep_sample_idx=result.prep_sample_idx,
                sequenced_sample_idx=result.sequenced_sample_idx,
                harmonization=harmonization_result,
            )
    except Exception as exc:  # noqa: BLE001 -- per-run isolation: a failure must
        # never abort sibling runs; it is recorded (accession + reason) in the
        # returned result, not swallowed.
        return EnaRunRegistrationOutcome(
            run_accession=ena_run.run_accession,
            status=EnaRunRegistrationStatus.FAILED,
            failure_reason=str(exc),
        )
