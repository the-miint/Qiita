"""DB-bound tests for `ena_import.registration.register_ena_study` (T02).

Covers the epic's acceptance criteria end to end against a real Postgres:
study upsert (T02-1), cross-study biosample de-dup (T02-2), one
sequenced_sample per run with accessions carried + the reserved-idx-range
invariant scoped to study/prep_sample only (T02-3), mixed-platform grouping
into multiple sequencing_run/sequenced_pool rows (R3), provenance columns
(T02-4), and idempotent re-import + per-run partial-failure isolation
(T02-5).

Pattern 2 (committed fixture + FK-reverse cleanup): `register_ena_study`
takes a pool and commits its own writes internally (each run gets its own
transaction, by design -- T02-5), so nothing here can be wrapped in one
outer rolled-back transaction. `_cleanup` below removes every row reachable
from the study_idxs / study_accessions / principal_idxs a test tracks.
"""

import pytest
import pytest_asyncio
from qiita_common.models.ena import (
    EnaRunRecord,
    EnaStudyHeader,
    ResolverKind,
    SourceArchive,
)

from qiita_control_plane.ena_import.registration import (
    RunRegistrationStatus,
    register_ena_study,
)
from qiita_control_plane.testing.db_seeds import seed_user_principal
from qiita_control_plane.testing.unique_names import unique_accession

pytestmark = pytest.mark.db


# ---------------------------------------------------------------------------
# Fixture builders
# ---------------------------------------------------------------------------


def _study_header(
    *, study_accession: str, secondary_study_accession: str | None = None
) -> EnaStudyHeader:
    return EnaStudyHeader(
        study_accession=study_accession,
        secondary_study_accession=secondary_study_accession,
        study_title=f"title for {study_accession}",
    )


def _run(
    *,
    run_accession: str,
    experiment_accession: str,
    sample_accession: str,
    study_accession: str,
    library_layout: str = "SINGLE",
    library_strategy: str | None = "WGS",
    library_source: str | None = "GENOMIC",
    instrument_platform: str | None = "ILLUMINA",
) -> EnaRunRecord:
    return EnaRunRecord(
        run_accession=run_accession,
        experiment_accession=experiment_accession,
        sample_accession=sample_accession,
        study_accession=study_accession,
        library_layout=library_layout,
        library_strategy=library_strategy,
        library_source=library_source,
        instrument_platform=instrument_platform,
    )


# ---------------------------------------------------------------------------
# Per-test tracker fixture + FK-reverse cleanup
# ---------------------------------------------------------------------------


class _Tracker:
    def __init__(self) -> None:
        self.study_idxs: list[int] = []
        self.study_accessions: list[str] = []
        self.principal_idxs: list[int] = []


async def _cleanup(pool, tracker: _Tracker) -> None:
    study_idxs = tracker.study_idxs
    if study_idxs:
        ps_rows = await pool.fetch(
            "SELECT DISTINCT prep_sample_idx FROM qiita.prep_sample_to_study"
            " WHERE study_idx = ANY($1::bigint[])",
            study_idxs,
        )
        ps_idxs = [r["prep_sample_idx"] for r in ps_rows]
        if ps_idxs:
            await pool.execute(
                "DELETE FROM qiita.sequenced_sample WHERE prep_sample_idx = ANY($1::bigint[])",
                ps_idxs,
            )
        await pool.execute(
            "DELETE FROM qiita.prep_sample_to_study WHERE study_idx = ANY($1::bigint[])",
            study_idxs,
        )
        if ps_idxs:
            await pool.execute(
                "DELETE FROM qiita.prep_sample WHERE idx = ANY($1::bigint[])", ps_idxs
            )

        bs_rows = await pool.fetch(
            "SELECT DISTINCT biosample_idx FROM qiita.biosample_to_study"
            " WHERE study_idx = ANY($1::bigint[])",
            study_idxs,
        )
        bs_idxs = [r["biosample_idx"] for r in bs_rows]
        await pool.execute(
            "DELETE FROM qiita.biosample_to_study WHERE study_idx = ANY($1::bigint[])",
            study_idxs,
        )
        if bs_idxs:
            await pool.execute("DELETE FROM qiita.biosample WHERE idx = ANY($1::bigint[])", bs_idxs)

        await pool.execute(
            "DELETE FROM qiita.study_access WHERE study_idx = ANY($1::bigint[])", study_idxs
        )
        await pool.execute("DELETE FROM qiita.study WHERE idx = ANY($1::bigint[])", study_idxs)

    if tracker.study_accessions:
        run_rows = await pool.fetch(
            "SELECT idx FROM qiita.sequencing_run WHERE instrument_run_id LIKE ANY($1::text[])",
            [f"{acc}:%" for acc in tracker.study_accessions],
        )
        run_idxs = [r["idx"] for r in run_rows]
        if run_idxs:
            await pool.execute(
                "DELETE FROM qiita.sequenced_pool WHERE sequencing_run_idx = ANY($1::bigint[])",
                run_idxs,
            )
            await pool.execute(
                "DELETE FROM qiita.sequencing_run WHERE idx = ANY($1::bigint[])", run_idxs
            )

    if tracker.principal_idxs:
        await pool.execute(
            "DELETE FROM qiita.user WHERE principal_idx = ANY($1::bigint[])",
            tracker.principal_idxs,
        )
        await pool.execute(
            "DELETE FROM qiita.principal WHERE idx = ANY($1::bigint[])", tracker.principal_idxs
        )


@pytest_asyncio.fixture
async def reg(postgres_pool):
    """Per-test (pool, owner_idx, caller_idx, tracker); tracker rows are
    cleaned up FK-reverse at teardown."""
    tracker = _Tracker()
    owner_idx = await seed_user_principal(postgres_pool, prefix="ena-owner", suffix="t02")
    caller_idx = await seed_user_principal(postgres_pool, prefix="ena-caller", suffix="t02")
    tracker.principal_idxs.extend([owner_idx, caller_idx])
    yield {
        "pool": postgres_pool,
        "owner_idx": owner_idx,
        "caller_idx": caller_idx,
        "tracker": tracker,
    }
    await _cleanup(postgres_pool, tracker)


async def _register(reg, *, study_header, runs):
    result = await register_ena_study(
        reg["pool"],
        study_header=study_header,
        runs=runs,
        sample_attributes=[],
        owner_idx=reg["owner_idx"],
        caller_idx=reg["caller_idx"],
        source_archive=SourceArchive.ENA,
        resolver_kind=ResolverKind.MIINT,
    )
    reg["tracker"].study_idxs.append(result.study_idx)
    reg["tracker"].study_accessions.append(study_header.study_accession)
    return result


# ---------------------------------------------------------------------------
# T02-1 -- study upsert
# ---------------------------------------------------------------------------


async def test_reimport_same_study_reuses_study_row(reg):
    study_accession = unique_accession("PRJNA")
    secondary = unique_accession("SRP")
    header = _study_header(study_accession=study_accession, secondary_study_accession=secondary)
    run = _run(
        run_accession=unique_accession("SRR"),
        experiment_accession=unique_accession("SRX"),
        sample_accession=unique_accession("SAMN"),
        study_accession=study_accession,
    )

    first = await _register(reg, study_header=header, runs=[run])
    second = await _register(reg, study_header=header, runs=[run])

    assert first.study_created is True
    assert second.study_created is False
    assert first.study_idx == second.study_idx

    count = await reg["pool"].fetchval(
        "SELECT count(*) FROM qiita.study WHERE bioproject_accession = $1", study_accession
    )
    assert count == 1

    row = await reg["pool"].fetchrow(
        "SELECT ena_study_accession, bioproject_accession FROM qiita.study WHERE idx = $1",
        first.study_idx,
    )
    assert row["bioproject_accession"] == study_accession
    assert row["ena_study_accession"] == secondary


# ---------------------------------------------------------------------------
# T02-2 -- cross-study biosample de-dup
# ---------------------------------------------------------------------------


async def test_shared_biosample_across_two_studies_one_row_two_links(reg):
    shared_sample_accession = unique_accession("SAMN")

    study_a_accession = unique_accession("PRJNA")
    header_a = _study_header(study_accession=study_a_accession)
    run_a = _run(
        run_accession=unique_accession("SRR"),
        experiment_accession=unique_accession("SRX"),
        sample_accession=shared_sample_accession,
        study_accession=study_a_accession,
    )

    study_b_accession = unique_accession("PRJNA")
    header_b = _study_header(study_accession=study_b_accession)
    run_b = _run(
        run_accession=unique_accession("SRR"),
        experiment_accession=unique_accession("SRX"),
        sample_accession=shared_sample_accession,
        study_accession=study_b_accession,
    )

    result_a = await _register(reg, study_header=header_a, runs=[run_a])
    result_b = await _register(reg, study_header=header_b, runs=[run_b])

    assert result_a.study_idx != result_b.study_idx

    biosample_idx = await reg["pool"].fetchval(
        "SELECT idx FROM qiita.biosample WHERE ena_sample_accession = $1",
        shared_sample_accession,
    )
    assert biosample_idx is not None

    link_count = await reg["pool"].fetchval(
        "SELECT count(*) FROM qiita.biosample_to_study WHERE biosample_idx = $1", biosample_idx
    )
    assert link_count == 2

    linked_studies = {
        r["study_idx"]
        for r in await reg["pool"].fetch(
            "SELECT study_idx FROM qiita.biosample_to_study WHERE biosample_idx = $1",
            biosample_idx,
        )
    }
    assert linked_studies == {result_a.study_idx, result_b.study_idx}


# ---------------------------------------------------------------------------
# T02-3 -- prep_sample / sequenced_sample creation, reserved-range invariant
# ---------------------------------------------------------------------------


async def test_paired_and_single_layout_runs_each_get_one_sequenced_sample(reg):
    study_accession = unique_accession("PRJNA")
    header = _study_header(study_accession=study_accession)
    single_run = _run(
        run_accession=unique_accession("SRR"),
        experiment_accession=unique_accession("SRX"),
        sample_accession=unique_accession("SAMN"),
        study_accession=study_accession,
        library_layout="SINGLE",
    )
    paired_run = _run(
        run_accession=unique_accession("SRR"),
        experiment_accession=unique_accession("SRX"),
        sample_accession=unique_accession("SAMN"),
        study_accession=study_accession,
        library_layout="PAIRED",
    )

    result = await _register(reg, study_header=header, runs=[single_run, paired_run])

    assert {o.status for o in result.runs} == {RunRegistrationStatus.REGISTERED}
    assert {o.run_accession for o in result.runs} == {
        single_run.run_accession,
        paired_run.run_accession,
    }

    for run in (single_run, paired_run):
        row = await reg["pool"].fetchrow(
            "SELECT ena_experiment_accession, ena_run_accession"
            " FROM qiita.sequenced_sample WHERE ena_run_accession = $1",
            run.run_accession,
        )
        assert row is not None
        assert row["ena_experiment_accession"] == run.experiment_accession
        assert row["ena_run_accession"] == run.run_accession

    # Reserved-range invariant (20260527000000_bump_identity_start_to_25k.sql)
    # holds ONLY for study and prep_sample -- scoped here, not asserted for
    # biosample / sequenced_sample / sequencing_run, which legitimately mint low.
    assert result.study_idx >= 25000
    prep_sample_idxs = [
        r["prep_sample_idx"]
        for r in await reg["pool"].fetch(
            "SELECT prep_sample_idx FROM qiita.prep_sample_to_study WHERE study_idx = $1",
            result.study_idx,
        )
    ]
    assert len(prep_sample_idxs) == 2
    assert all(idx >= 25000 for idx in prep_sample_idxs)


# ---------------------------------------------------------------------------
# R3 correctness -- mixed-platform study groups into multiple
# sequencing_run / sequenced_pool rows, and protocol mapping is persisted.
# ---------------------------------------------------------------------------


async def test_mixed_platform_study_creates_one_run_and_pool_per_platform(reg):
    study_accession = unique_accession("PRJNA")
    header = _study_header(study_accession=study_accession)
    illumina_run = _run(
        run_accession=unique_accession("SRR"),
        experiment_accession=unique_accession("SRX"),
        sample_accession=unique_accession("SAMN"),
        study_accession=study_accession,
        instrument_platform="ILLUMINA",
        library_strategy="AMPLICON",
    )
    nanopore_run = _run(
        run_accession=unique_accession("SRR"),
        experiment_accession=unique_accession("SRX"),
        sample_accession=unique_accession("SAMN"),
        study_accession=study_accession,
        instrument_platform="OXFORD_NANOPORE",
        library_strategy="WGS",
        library_source="METAGENOMIC",
    )

    result = await _register(reg, study_header=header, runs=[illumina_run, nanopore_run])
    assert {o.status for o in result.runs} == {RunRegistrationStatus.REGISTERED}

    runs_rows = await reg["pool"].fetch(
        "SELECT idx, platform FROM qiita.sequencing_run WHERE instrument_run_id LIKE $1",
        f"{study_accession}:%",
    )
    assert len(runs_rows) == 2
    platform_by_run_idx = {r["idx"]: r["platform"] for r in runs_rows}
    assert set(platform_by_run_idx.values()) == {"illumina", "oxford_nanopore"}

    pool_rows = await reg["pool"].fetch(
        "SELECT idx, sequencing_run_idx FROM qiita.sequenced_pool"
        " WHERE sequencing_run_idx = ANY($1::bigint[])",
        list(platform_by_run_idx.keys()),
    )
    assert len(pool_rows) == 2
    pool_idx_by_run_idx = {r["sequencing_run_idx"]: r["idx"] for r in pool_rows}

    # Each run's sequenced_sample sits in the pool matching ITS platform,
    # not a shared/merged pool.
    illumina_pool_idx = pool_idx_by_run_idx[
        next(idx for idx, p in platform_by_run_idx.items() if p == "illumina")
    ]
    nanopore_pool_idx = pool_idx_by_run_idx[
        next(idx for idx, p in platform_by_run_idx.items() if p == "oxford_nanopore")
    ]
    assert illumina_pool_idx != nanopore_pool_idx

    illumina_sample = await reg["pool"].fetchrow(
        "SELECT sequenced_pool_idx FROM qiita.sequenced_sample WHERE ena_run_accession = $1",
        illumina_run.run_accession,
    )
    nanopore_sample = await reg["pool"].fetchrow(
        "SELECT sequenced_pool_idx FROM qiita.sequenced_sample WHERE ena_run_accession = $1",
        nanopore_run.run_accession,
    )
    assert illumina_sample["sequenced_pool_idx"] == illumina_pool_idx
    assert nanopore_sample["sequenced_pool_idx"] == nanopore_pool_idx

    # Protocol mapping persisted: AMPLICON/illumina -> short_read_amplicon;
    # WGS+METAGENOMIC/nanopore -> long_read_metagenomics.
    illumina_protocol = await reg["pool"].fetchval(
        "SELECT pp.name FROM qiita.sequenced_sample ss"
        " JOIN qiita.prep_sample ps ON ps.idx = ss.prep_sample_idx"
        " JOIN qiita.prep_protocol pp ON pp.idx = ps.prep_protocol_idx"
        " WHERE ss.ena_run_accession = $1",
        illumina_run.run_accession,
    )
    nanopore_protocol = await reg["pool"].fetchval(
        "SELECT pp.name FROM qiita.sequenced_sample ss"
        " JOIN qiita.prep_sample ps ON ps.idx = ss.prep_sample_idx"
        " JOIN qiita.prep_protocol pp ON pp.idx = ps.prep_protocol_idx"
        " WHERE ss.ena_run_accession = $1",
        nanopore_run.run_accession,
    )
    assert illumina_protocol == "short_read_amplicon"
    assert nanopore_protocol == "long_read_metagenomics"


# ---------------------------------------------------------------------------
# T02-4 -- provenance columns persisted
# ---------------------------------------------------------------------------


async def test_provenance_columns_persisted(reg):
    study_accession = unique_accession("PRJNA")
    header = _study_header(study_accession=study_accession)
    run = _run(
        run_accession=unique_accession("SRR"),
        experiment_accession=unique_accession("SRX"),
        sample_accession=unique_accession("SAMN"),
        study_accession=study_accession,
    )

    await _register(reg, study_header=header, runs=[run])

    row = await reg["pool"].fetchrow(
        "SELECT source_archive, resolver_kind, transport"
        " FROM qiita.sequenced_sample WHERE ena_run_accession = $1",
        run.run_accession,
    )
    assert row["source_archive"] == "ena"
    assert row["resolver_kind"] == "miint"
    # transport stays NULL in T02 -- populated by TASK-04's download workflow.
    assert row["transport"] is None


# ---------------------------------------------------------------------------
# T02-5 -- idempotency + partial-failure semantics
# ---------------------------------------------------------------------------


async def test_reimport_is_idempotent_no_duplicates_and_runs_skipped(reg):
    study_accession = unique_accession("PRJNA")
    header = _study_header(study_accession=study_accession)
    run = _run(
        run_accession=unique_accession("SRR"),
        experiment_accession=unique_accession("SRX"),
        sample_accession=unique_accession("SAMN"),
        study_accession=study_accession,
    )

    first = await _register(reg, study_header=header, runs=[run])
    assert first.runs[0].status == RunRegistrationStatus.REGISTERED
    first_sequenced_sample_idx = first.runs[0].sequenced_sample_idx

    second = await _register(reg, study_header=header, runs=[run])
    assert second.runs[0].status == RunRegistrationStatus.SKIPPED_ALREADY_PRESENT
    assert second.runs[0].sequenced_sample_idx == first_sequenced_sample_idx

    count = await reg["pool"].fetchval(
        "SELECT count(*) FROM qiita.sequenced_sample WHERE ena_run_accession = $1",
        run.run_accession,
    )
    assert count == 1


async def test_partial_failure_run_leaves_no_orphan_rows_and_rerun_completes(reg):
    study_accession = unique_accession("PRJNA")
    header = _study_header(study_accession=study_accession)
    ok_run = _run(
        run_accession=unique_accession("SRR"),
        experiment_accession=unique_accession("SRX"),
        sample_accession=unique_accession("SAMN"),
        study_accession=study_accession,
    )
    bad_sample_accession = unique_accession("SAMN")
    bad_run_accession = unique_accession("SRR")
    bad_run = _run(
        run_accession=bad_run_accession,
        experiment_accession=unique_accession("SRX"),
        sample_accession=bad_sample_accession,
        study_accession=study_accession,
        # ChIP-Seq has no curated-protocol mapping -- this run fails inside
        # its own per-run transaction.
        library_strategy="ChIP-Seq",
        library_source="GENOMIC",
    )

    result = await _register(reg, study_header=header, runs=[ok_run, bad_run])

    outcomes_by_accession = {o.run_accession: o for o in result.runs}
    assert outcomes_by_accession[ok_run.run_accession].status == RunRegistrationStatus.REGISTERED
    bad_outcome = outcomes_by_accession[bad_run.run_accession]
    assert bad_outcome.status == RunRegistrationStatus.FAILED
    assert bad_outcome.failure_reason is not None
    assert "ChIP-Seq" in bad_outcome.failure_reason

    # No orphan sequenced_sample / prep_sample for the failed run.
    orphan_count = await reg["pool"].fetchval(
        "SELECT count(*) FROM qiita.sequenced_sample WHERE ena_run_accession = $1",
        bad_run.run_accession,
    )
    assert orphan_count == 0

    # The failed run's biosample IS committed (biosample get-or-create +
    # study link precede the failing protocol-mapping step in its own
    # per-run transaction scope, but that scope rolled back entirely on
    # the raised error) -- confirm no biosample-to-study link survived either.
    bts_count = await reg["pool"].fetchval(
        "SELECT count(*) FROM qiita.biosample_to_study bts"
        " JOIN qiita.biosample b ON b.idx = bts.biosample_idx"
        " WHERE b.ena_sample_accession = $1",
        bad_sample_accession,
    )
    assert bts_count == 0
    orphan_biosample_count = await reg["pool"].fetchval(
        "SELECT count(*) FROM qiita.biosample WHERE ena_sample_accession = $1",
        bad_sample_accession,
    )
    assert orphan_biosample_count == 0

    # Re-run with the same study: the previously-ok run is skipped
    # (already present); simulate the fix by re-submitting the bad run
    # with a mappable strategy -- it completes on retry, proving the
    # partial failure left the identity space clean for a re-run.
    fixed_run = _run(
        run_accession=bad_run_accession,
        experiment_accession=bad_run.experiment_accession,
        sample_accession=bad_sample_accession,
        study_accession=study_accession,
        library_strategy="WGS",
        library_source="GENOMIC",
    )
    rerun_result = await _register(reg, study_header=header, runs=[ok_run, fixed_run])
    rerun_by_accession = {o.run_accession: o for o in rerun_result.runs}
    assert (
        rerun_by_accession[ok_run.run_accession].status
        == RunRegistrationStatus.SKIPPED_ALREADY_PRESENT
    )
    assert rerun_by_accession[bad_run_accession].status == RunRegistrationStatus.REGISTERED

    final_count = await reg["pool"].fetchval(
        "SELECT count(*) FROM qiita.sequenced_sample WHERE ena_run_accession = $1",
        bad_run_accession,
    )
    assert final_count == 1
