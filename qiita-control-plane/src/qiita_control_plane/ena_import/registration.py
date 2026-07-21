"""ENA study registration composer (T02): turns one resolved ENA study
(`EnaStudyHeader` + `EnaRunRecord` list, as produced by `ena_import`'s
resolvers) into Qiita `study` / `biosample` / `prep_sample` /
`sequenced_sample` rows, idempotently -- re-imports and cross-study
biosample overlap must converge, never duplicate.

Order of operations:

  1. Get-or-create the Qiita `study` from the resolved header, keyed
     *positionally* on the two ENA accessions (never prefix-sniffed):
     `EnaStudyHeader.study_accession` -> `study.bioproject_accession`,
     `EnaStudyHeader.secondary_study_accession` -> `study.ena_study_accession`.
     Race-safe (`repositories.study.get_or_create_study_by_ena_accessions`).

  2. Map each run's ENA `instrument_platform` to `qiita.platform`
     independently (`platform_mapping.map_ena_platform`, fail-loud on an
     unmappable value -- but isolated per run, matching protocol-mapping
     and per-run DB failures below: an unrecognized platform fails only
     that run (`failed`, offending value in the reason) and never aborts
     sibling runs or the whole study). Successfully-mapped runs are
     grouped by the mapped platform.

  3. For each distinct platform among the successfully-mapped runs:
     get-or-create one `sequencing_run`
     (`instrument_run_id = "{study_accession}:{platform}"`, race-safe via
     the existing `insert_sequencing_run`) and one `sequenced_pool`
     attached to it (reused via `fetch_sequenced_pool_idxs_for_run` if one
     already exists for this run -- a no-preflight `insert_sequenced_pool`
     call has no natural content key to de-duplicate against).

  4. For each run, independently, inside its own transaction (T02-5's
     per-run atomicity -- a partial failure rolls back only that run's
     writes and is recorded `failed` rather than aborting or half-writing
     the rest of the study): get-or-create the biosample by ENA sample
     accession (cross-study de-dup, T02-2) and link it to the study,
     skip if a sequenced_sample already carries this run's
     `ena_run_accession` (idempotent re-import, T02-5), else map the
     run's library_strategy/library_source to a curated prep_protocol
     name (`protocol_mapping.map_ena_run_to_prep_protocol_name`) and
     import the sequenced prep_sample via the existing
     `import_sequenced_prep_sample` composer.

No metadata harmonization here -- `metadata={}` is passed to the
sequencing-ingestion composer; TASK-03 maps ENA sample attributes into
harmonized metadata. No read bytes (TASK-04); no batch fan-out (TASK-06).
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

from .platform_mapping import UnmappableEnaPlatformError, map_ena_platform
from .protocol_mapping import map_ena_run_to_prep_protocol_name


class RunRegistrationStatus(StrEnum):
    """Per-run outcome discriminator for `RunRegistrationOutcome.status`."""

    REGISTERED = "registered"
    SKIPPED_ALREADY_PRESENT = "skipped_already_present"
    FAILED = "failed"


@dataclass(frozen=True)
class RunRegistrationOutcome:
    """One ENA run's registration outcome.

    `prep_sample_idx` is set only on `REGISTERED`; `sequenced_sample_idx`
    is set on both `REGISTERED` and `SKIPPED_ALREADY_PRESENT` (naming the
    pre-existing row on the skip path); `failure_reason` is set only on
    `FAILED`.
    """

    run_accession: str
    status: RunRegistrationStatus
    prep_sample_idx: int | None = None
    sequenced_sample_idx: int | None = None
    failure_reason: str | None = None


@dataclass(frozen=True)
class EnaStudyRegistrationResult:
    """Composite result of one `register_ena_study` call."""

    study_idx: int
    study_created: bool
    runs: list[RunRegistrationOutcome] = field(default_factory=list)


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

    `sample_attributes` is accepted (not `resolve_sample_attributes`'s
    plain output shape re-derived here) to keep this function's signature
    the stable home for the full resolver output ahead of TASK-03, which
    will map these attributes into harmonized metadata on the same
    per-run pass this function already makes. It is intentionally unused
    in T02 -- no metadata is written here (`metadata={}` below).

    `owner_idx` / `caller_idx` / `source_archive` / `resolver_kind` are
    identity inputs this function cannot invent from the resolver output
    and must be supplied by the caller (e.g. the batch driver, TASK-06).

    Never raises for a per-run failure -- see `RunRegistrationOutcome`.
    An unmappable `instrument_platform` (`platform_mapping.
    UnmappableEnaPlatformError`) is one such per-run failure, isolated
    exactly like a protocol-mapping or per-run DB error: that run is
    recorded `failed` with the offending platform value in the reason,
    and every other run in the study is still registered normally.
    """
    async with pool.acquire() as conn:
        study_row, study_created = await get_or_create_study_by_ena_accessions(
            conn,
            bioproject_accession=study_header.study_accession,
            ena_study_accession=study_header.secondary_study_accession,
            owner_idx=owner_idx,
            created_by_idx=caller_idx,
            # study.title is NOT NULL; ENA's study_title is optional. A
            # title is cosmetic, not identity, so fall back to the
            # accession rather than fail the whole import over it.
            title=study_header.study_title or study_header.study_accession,
        )
        study_idx = study_row["idx"]

        # Map each run's platform independently -- fail loud, but isolated
        # per run (R3): an unmappable platform fails only that run, exactly
        # like the protocol-mapping / DB failures _register_one_run already
        # isolates. Only successfully-mapped runs are grouped by platform.
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
        for platform in runs_by_platform:
            sequenced_pool_idx_by_platform[platform] = await _get_or_create_pool_for_platform(
                conn,
                study_accession=study_header.study_accession,
                platform=platform,
                created_by_idx=caller_idx,
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
                )

    # Preserve the caller's input order in the returned per-run outcomes.
    outcomes = [outcomes_by_accession[run.run_accession] for run in runs]

    return EnaStudyRegistrationResult(
        study_idx=study_idx,
        study_created=study_created,
        runs=outcomes,
    )


async def _get_or_create_pool_for_platform(
    conn: asyncpg.Connection,
    *,
    study_accession: str,
    platform: Platform,
    created_by_idx: int,
) -> int:
    """Get-or-create the one sequencing_run + sequenced_pool for a given
    (study, platform) pair. Both underlying repo calls are single
    statements (no explicit transaction required)."""
    instrument_run_id = f"{study_accession}:{platform.value}"
    sequencing_run_idx, _ = await insert_sequencing_run(
        conn,
        instrument_run_id=instrument_run_id,
        platform=platform,
        created_by_idx=created_by_idx,
    )

    existing_pool_idxs = await fetch_sequenced_pool_idxs_for_run(conn, sequencing_run_idx)
    if existing_pool_idxs:
        return existing_pool_idxs[0]

    sequenced_pool_idx, _ = await insert_sequenced_pool(
        conn,
        sequencing_run_idx=sequencing_run_idx,
        created_by_idx=created_by_idx,
    )
    return sequenced_pool_idx


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
) -> RunRegistrationOutcome:
    """Register one ENA run inside its own transaction so a partial
    failure rolls back only this run's writes -- T02-5's per-run
    atomicity. Never raises: every failure mode (protocol-mapping,
    composer/DB errors) is caught and folded into a `failed` outcome so
    one bad run cannot abort the whole study import."""
    try:
        async with conn.transaction():
            biosample_idx = await get_or_create_biosample_by_ena_accession(
                conn,
                ena_sample_accession=run.sample_accession,
                owner_idx=owner_idx,
                created_by_idx=caller_idx,
            )
            # Must precede the composer: import_sequenced_prep_sample's
            # prep_sample_to_study insert fires
            # reject_without_biosample_link, which requires a non-retired
            # biosample_to_study row to already exist.
            await ensure_biosample_linked_to_study(
                conn,
                biosample_idx=biosample_idx,
                study_idx=study_idx,
                created_by_idx=caller_idx,
            )

            existing = await fetch_sequenced_sample_idxs_by_ena_run_accession(
                conn, values=[run.run_accession]
            )
            if run.run_accession in existing:
                return RunRegistrationOutcome(
                    run_accession=run.run_accession,
                    status=RunRegistrationStatus.SKIPPED_ALREADY_PRESENT,
                    sequenced_sample_idx=existing[run.run_accession],
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
                # TASK-03 maps EnaSampleAttributes into harmonized
                # metadata; T02 writes none.
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
            )
    except Exception as exc:  # noqa: BLE001 -- per-run isolation is the point (T02-5): a
        # failure here must never abort sibling runs' registration; it is
        # recorded, not swallowed -- the caller sees every failed run
        # (accession + reason) in the returned result.
        return RunRegistrationOutcome(
            run_accession=run.run_accession,
            status=RunRegistrationStatus.FAILED,
            failure_reason=str(exc),
        )
