"""ENA study registration composer: turns one resolved ENA study
(`EnaStudyHeader` + `EnaRunRecord` list) into Qiita `study` / `biosample` /
`prep_sample` / `sequenced_sample` rows, idempotently -- re-imports and
cross-study biosample overlap converge, never duplicate.

Order of operations:

  1. Get-or-create the `study` from the header, keyed *positionally* on the two
     ENA accessions (never prefix-sniffed): `study_accession` ->
     `bioproject_accession`, `secondary_study_accession` -> `ena_study_accession`.

  2. Map each run's `instrument_platform` to `qiita.platform`
     (`platform_mapping.map_ena_platform`), isolated per run: an unmappable
     value fails only that run. Successfully-mapped runs are grouped by platform.

  3. Per distinct mapped platform: get-or-create one `sequencing_run`
     (`instrument_run_id = "{study_accession}:{platform}"`) and one
     `sequenced_pool` on it (reused if one already exists -- `insert_sequenced_
     pool` has no content key to de-dup against).

  4. Per run, in its own transaction (per-run atomicity): get-or-create the
     biosample by ENA sample accession (cross-study de-dup) and link it to the
     study. If newly created, harmonize its ENA attributes onto it once
     (write-once: cross-study reuse does not re-harmonize). Skip if a
     sequenced_sample already carries this run's `ena_run_accession` (idempotent
     re-import), else map library_strategy/library_source to a curated
     prep_protocol name and import via `import_sequenced_prep_sample`.

Harmonization gaps are reported on `RunRegistrationOutcome.harmonization`, never
raised; a genuine harmonization failure is caught by the same per-run try/except
as every other step. No read bytes or batch fan-out here -- those live in the
download workflow and the batch driver.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from enum import StrEnum

import asyncpg
from qiita_common.models import Platform
from qiita_common.models.ena import (
    EnaRunRecord,
    EnaSampleAttributes,
    EnaStudyHeader,
    ResolverKind,
    SourceArchive,
)

from qiita_control_plane.repositories._sample_helpers import (
    fetch_metadata_checklist_idx_by_name,
)
from qiita_control_plane.repositories.biosample import (
    ensure_biosample_linked_to_study,
    get_or_create_biosample_by_ena_accession,
)
from qiita_control_plane.repositories.prep_protocol import fetch_prep_protocol_idx_by_name
from qiita_control_plane.repositories.sequenced_sample import (
    fetch_sequenced_sample_idxs_by_ena_run_accession,
    import_sequenced_prep_sample,
)
from qiita_control_plane.repositories.sequencing_run import (
    fetch_sequenced_pool_idxs_for_run,
    insert_sequenced_pool,
    insert_sequencing_run,
)
from qiita_control_plane.repositories.study import get_or_create_study_by_ena_accessions

from .harmonization import HarmonizationResult, harmonize_biosample_attributes
from .platform_mapping import UnmappableEnaPlatformError, map_ena_platform
from .protocol_mapping import map_ena_run_to_prep_protocol_name

# The ENA default sample checklist (seeded by db/migrations). Every ENA-imported
# biosample is bound to it -- resolved once per study import, not per run.
_ERC000011_CHECKLIST_NAME = "ERC000011"


class RunRegistrationStatus(StrEnum):
    """Per-run outcome discriminator for `RunRegistrationOutcome.status`."""

    REGISTERED = "registered"
    SKIPPED_ALREADY_PRESENT = "skipped_already_present"
    FAILED = "failed"


@dataclass(frozen=True)
class RunRegistrationOutcome:
    """One ENA run's registration outcome.

    `prep_sample_idx` is set only on `REGISTERED`; `sequenced_sample_idx` on
    both `REGISTERED` and `SKIPPED_ALREADY_PRESENT`; `failure_reason` only on
    `FAILED`. `harmonization` is set (non-`FAILED`) only when this call newly
    created the biosample -- write-once: a reused/re-imported biosample carries
    `None` because no harmonization write ran.
    """

    run_accession: str
    status: RunRegistrationStatus
    prep_sample_idx: int | None = None
    sequenced_sample_idx: int | None = None
    failure_reason: str | None = None
    harmonization: HarmonizationResult | None = None


@dataclass(frozen=True)
class CreatedPool:
    """One `(platform, sequenced_pool_idx, sequencing_run_idx)` triple resolved
    (created or reused). The batch driver uses these to build one
    `download-ena-study` ticket per pool without re-deriving them from the DB.
    `platform` is the `Platform` enum value as plain `str` (matching the DB's
    `qiita.platform` enum text).
    """

    platform: str
    sequenced_pool_idx: int
    sequencing_run_idx: int


@dataclass(frozen=True)
class EnaStudyRegistrationResult:
    """Composite result of one `register_ena_study` call."""

    study_idx: int
    study_created: bool
    runs: list[RunRegistrationOutcome] = field(default_factory=list)
    created_pools: list[CreatedPool] = field(default_factory=list)


async def register_ena_study(
    pool: asyncpg.Pool,
    *,
    study_header: EnaStudyHeader,
    runs: list[EnaRunRecord],
    sample_attributes: list[EnaSampleAttributes],
    owner_idx: int,
    caller_idx: int,
    source_archive: SourceArchive,
    resolver_kind: ResolverKind,
) -> EnaStudyRegistrationResult:
    """Register one resolved ENA study's runs and samples.

    `sample_attributes` is indexed once by `sample_accession` and harmonized
    onto each run's biosample (or `{}` if the resolver found none) when that
    biosample is newly created, inside the run's own transaction.
    `owner_idx` / `caller_idx` / `source_archive` / `resolver_kind` are identity
    inputs the caller must supply.

    Never raises for a per-run failure (see `RunRegistrationOutcome`); an
    unmappable `instrument_platform` is one such isolated per-run failure.
    """
    # A run whose sample has no entry here harmonizes against an empty map
    # rather than failing.
    attrs_by_sample_accession: dict[str, EnaSampleAttributes] = {
        sa.sample_accession: sa for sa in sample_attributes
    }

    async with pool.acquire() as conn:
        study_row, study_created = await get_or_create_study_by_ena_accessions(
            conn,
            bioproject_accession=study_header.study_accession,
            ena_study_accession=study_header.secondary_study_accession,
            owner_idx=owner_idx,
            created_by_idx=caller_idx,
            # study.title is NOT NULL but ENA's study_title is optional; a title
            # is cosmetic, not identity, so fall back to the accession.
            title=study_header.study_title or study_header.study_accession,
        )
        study_idx = study_row["idx"]

        metadata_checklist_idx = await fetch_metadata_checklist_idx_by_name(
            conn, _ERC000011_CHECKLIST_NAME
        )

        # Map each run's platform, isolated per run: an unmappable platform fails
        # only that run. Only successfully-mapped runs are grouped by platform.
        runs_by_platform: dict[Platform, list[EnaRunRecord]] = defaultdict(list)
        outcomes_by_accession: dict[str, RunRegistrationOutcome] = {}
        for run in runs:
            try:
                platform = map_ena_platform(run.instrument_platform)
            except UnmappableEnaPlatformError as exc:
                outcomes_by_accession[run.run_accession] = RunRegistrationOutcome(
                    run_accession=run.run_accession,
                    status=RunRegistrationStatus.FAILED,
                    failure_reason=str(exc),
                )
                continue
            runs_by_platform[platform].append(run)

        # One sequencing_run + sequenced_pool per distinct platform that has
        # at least one successfully-mapped run.
        sequenced_pool_idx_by_platform: dict[Platform, int] = {}
        created_pools: list[CreatedPool] = []
        for platform in runs_by_platform:
            sequenced_pool_idx, sequencing_run_idx = await _get_or_create_pool_for_platform(
                conn,
                study_accession=study_header.study_accession,
                platform=platform,
                created_by_idx=caller_idx,
            )
            sequenced_pool_idx_by_platform[platform] = sequenced_pool_idx
            created_pools.append(
                CreatedPool(
                    platform=platform.value,
                    sequenced_pool_idx=sequenced_pool_idx,
                    sequencing_run_idx=sequencing_run_idx,
                )
            )

        for platform, platform_runs in runs_by_platform.items():
            for run in platform_runs:
                outcomes_by_accession[run.run_accession] = await _register_one_run(
                    conn,
                    run=run,
                    study_idx=study_idx,
                    platform=platform,
                    sequenced_pool_idx=sequenced_pool_idx_by_platform[platform],
                    owner_idx=owner_idx,
                    caller_idx=caller_idx,
                    source_archive=source_archive,
                    resolver_kind=resolver_kind,
                    metadata_checklist_idx=metadata_checklist_idx,
                    attrs_by_sample_accession=attrs_by_sample_accession,
                )

    # Return per-run outcomes in the caller's input order.
    outcomes = [outcomes_by_accession[run.run_accession] for run in runs]

    return EnaStudyRegistrationResult(
        study_idx=study_idx,
        study_created=study_created,
        runs=outcomes,
        created_pools=created_pools,
    )


async def _get_or_create_pool_for_platform(
    conn: asyncpg.Connection,
    *,
    study_accession: str,
    platform: Platform,
    created_by_idx: int,
) -> tuple[int, int]:
    """Get-or-create the one sequencing_run + sequenced_pool for a
    (study, platform) pair (both repo calls are single statements). Returns
    `(sequenced_pool_idx, sequencing_run_idx)` so the caller can surface them on
    `created_pools` without a second lookup."""
    instrument_run_id = f"{study_accession}:{platform.value}"
    sequencing_run_idx, _ = await insert_sequencing_run(
        conn,
        instrument_run_id=instrument_run_id,
        platform=platform,
        created_by_idx=created_by_idx,
    )

    existing_pool_idxs = await fetch_sequenced_pool_idxs_for_run(conn, sequencing_run_idx)
    if existing_pool_idxs:
        return existing_pool_idxs[0], sequencing_run_idx

    sequenced_pool_idx, _ = await insert_sequenced_pool(
        conn,
        sequencing_run_idx=sequencing_run_idx,
        created_by_idx=created_by_idx,
    )
    return sequenced_pool_idx, sequencing_run_idx


async def _register_one_run(
    conn: asyncpg.Connection,
    *,
    run: EnaRunRecord,
    study_idx: int,
    platform: Platform,
    sequenced_pool_idx: int,
    owner_idx: int,
    caller_idx: int,
    source_archive: SourceArchive,
    resolver_kind: ResolverKind,
    metadata_checklist_idx: int,
    attrs_by_sample_accession: dict[str, EnaSampleAttributes],
) -> RunRegistrationOutcome:
    """Register one ENA run inside its own transaction (per-run atomicity: a
    partial failure rolls back only this run). Never raises: every failure mode
    (platform/protocol-mapping, harmonization, composer/DB) is folded into a
    `failed` outcome. A harmonization gap is not a failure mode -- only a genuine
    parse/collision failure inside harmonize_biosample_attributes raises, caught
    here like any other."""
    try:
        async with conn.transaction():
            biosample_idx, biosample_created = await get_or_create_biosample_by_ena_accession(
                conn,
                ena_sample_accession=run.sample_accession,
                owner_idx=owner_idx,
                created_by_idx=caller_idx,
            )
            # Must precede the composer: its prep_sample_to_study insert fires
            # reject_without_biosample_link, which requires a non-retired
            # biosample_to_study row to already exist.
            await ensure_biosample_linked_to_study(
                conn,
                biosample_idx=biosample_idx,
                study_idx=study_idx,
                created_by_idx=caller_idx,
            )

            # Harmonize ENA attributes onto the biosample once -- only when THIS
            # call created it. A reused/re-imported biosample already has the
            # canonical global-field values, so it is not re-harmonized.
            harmonization_result: HarmonizationResult | None = None
            if biosample_created:
                sample_attrs = attrs_by_sample_accession.get(run.sample_accession)
                harmonization_result = await harmonize_biosample_attributes(
                    conn,
                    biosample_idx=biosample_idx,
                    study_idx=study_idx,
                    attributes=sample_attrs.attributes if sample_attrs is not None else {},
                    caller_idx=caller_idx,
                    metadata_checklist_idx=metadata_checklist_idx,
                )

            existing = await fetch_sequenced_sample_idxs_by_ena_run_accession(
                conn, values=[run.run_accession]
            )
            if run.run_accession in existing:
                return RunRegistrationOutcome(
                    run_accession=run.run_accession,
                    status=RunRegistrationStatus.SKIPPED_ALREADY_PRESENT,
                    sequenced_sample_idx=existing[run.run_accession],
                    harmonization=harmonization_result,
                )

            protocol_name = map_ena_run_to_prep_protocol_name(
                library_strategy=run.library_strategy,
                library_source=run.library_source,
                platform=platform,
            )
            prep_protocol_idx = await fetch_prep_protocol_idx_by_name(conn, protocol_name)

            result = await import_sequenced_prep_sample(
                conn,
                sequenced_pool_idx=sequenced_pool_idx,
                biosample_idx=biosample_idx,
                prep_protocol_idx=prep_protocol_idx,
                owner_idx=owner_idx,
                sequenced_pool_item_id=run.run_accession,
                # This `metadata` is prep_sample-level (biosample attributes are
                # harmonized above); no current resolver output populates it.
                metadata={},
                primary_study_idx=study_idx,
                caller_idx=caller_idx,
                ena_experiment_accession=run.experiment_accession,
                ena_run_accession=run.run_accession,
                source_archive=source_archive.value,
                resolver_kind=resolver_kind.value,
            )
            return RunRegistrationOutcome(
                run_accession=run.run_accession,
                status=RunRegistrationStatus.REGISTERED,
                prep_sample_idx=result.prep_sample_idx,
                sequenced_sample_idx=result.sequenced_sample_idx,
                harmonization=harmonization_result,
            )
    except Exception as exc:  # noqa: BLE001 -- per-run isolation: a failure must
        # never abort sibling runs; it is recorded (accession + reason) in the
        # returned result, not swallowed.
        return RunRegistrationOutcome(
            run_accession=run.run_accession,
            status=RunRegistrationStatus.FAILED,
            failure_reason=str(exc),
        )
