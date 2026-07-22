"""DB-bound tests for `ena_import.registration.register_ena_study`.

Covers the epic's acceptance criteria end to end against a real Postgres:
study upsert, cross-study biosample de-dup, one
sequenced_sample per run with accessions carried + the reserved-idx-range
invariant scoped to study/prep_sample only, mixed-platform grouping
into multiple sequencing_run/sequenced_pool rows, provenance columns,
and idempotent re-import + per-run partial-failure isolation.

Pattern 2 (committed fixture + FK-reverse cleanup): `register_ena_study`
takes a pool and commits its own writes internally (each run gets its own
transaction, by design), so nothing here can be wrapped in one
outer rolled-back transaction. `_cleanup` below removes every row reachable
from the study_idxs / study_accessions / principal_idxs a test tracks.
"""

from decimal import Decimal

import pytest
import pytest_asyncio
from qiita_common.models.ena import (
    EnaRunRecord,
    EnaSampleAttributes,
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
        # Harmonization (T03) writes biosample_metadata / biosample_study_field
        # rows against these biosamples/studies; both reference their
        # respective parents ON DELETE RESTRICT, so they must be swept before
        # biosample_to_study / biosample / study below.
        if bs_idxs:
            await pool.execute(
                "DELETE FROM qiita.biosample_metadata WHERE biosample_idx = ANY($1::bigint[])",
                bs_idxs,
            )
        await pool.execute(
            "DELETE FROM qiita.biosample_study_field WHERE study_idx = ANY($1::bigint[])",
            study_idxs,
        )
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


async def _register(reg, *, study_header, runs, sample_attributes=()):
    result = await register_ena_study(
        reg["pool"],
        study_header=study_header,
        runs=runs,
        sample_attributes=list(sample_attributes),
        owner_idx=reg["owner_idx"],
        caller_idx=reg["caller_idx"],
        source_archive=SourceArchive.ENA,
        resolver_kind=ResolverKind.MIINT,
    )
    reg["tracker"].study_idxs.append(result.study_idx)
    reg["tracker"].study_accessions.append(study_header.study_accession)
    return result


# ---------------------------------------------------------------------------
# T03 -- metadata harmonization into the checklist model
# ---------------------------------------------------------------------------
#
# A real, doc-confirmed MIxS-tagged fixture (NOT SAMN00199006, which has no
# mappable tags): the five ena_import.attribute_mapping tags (collection
# date/geo-country/lat/long/depth) plus one "hard" value (`depth` = an
# INSDC missing-value string, proving the known_missing_reasons wiring) and
# four deliberately-unmapped tags -- `host` (free text, not
# an NCBI taxon id) and the broad-scale/local/medium environmental-context
# triad (extended on discovery: these were rebound to
# TERMINOLOGY/ENVO by `20260608000001_seed_envo_terminology.sql`, so mapping
# ENA's free-text value directly would require an ENVO-CURIE resolution this
# ticket does not own; see attribute_mapping.py's module docstring).
# ---------------------------------------------------------------------------


def _mixs_sample_attributes(sample_accession: str, **overrides: str) -> EnaSampleAttributes:
    attributes = {
        "collection date": "2019-06-01",
        "geographic location (country and/or sea)": "USA: California",
        "geographic location (latitude)": "32.88",
        "geographic location (longitude)": "-117.24",
        # Hard value: an INSDC missing-value string -- proves the
        # known_missing_reasons wiring resolves it as a MissingReasonRef
        # instead of raising MetadataParseError (numeric field, non-numeric text).
        "depth": "not collected",
        # Deliberately unmapped: TERMINOLOGY/ENVO-typed global fields --
        # mapping this free text would require an ontology resolution this
        # ticket does not own (see attribute_mapping.py). Retained as local
        # metadata, not dropped.
        "broad-scale environmental context": "marine biome",
        "local environmental context": "coastal water",
        "environmental medium": "sea water",
        # Deliberately unmapped -- free-text, not an NCBI
        # taxon id; retained as local metadata, not dropped.
        "host": "Homo sapiens",
    }
    attributes.update(overrides)
    return EnaSampleAttributes(sample_accession=sample_accession, attributes=attributes)


async def test_harmonized_attributes_land_on_global_fields_and_checklist(reg):
    study_accession = unique_accession("PRJNA")
    header = _study_header(study_accession=study_accession)
    sample_accession = unique_accession("SAMN")
    run = _run(
        run_accession=unique_accession("SRR"),
        experiment_accession=unique_accession("SRX"),
        sample_accession=sample_accession,
        study_accession=study_accession,
    )
    attrs = _mixs_sample_attributes(sample_accession)

    result = await _register(reg, study_header=header, runs=[run], sample_attributes=[attrs])

    assert result.runs[0].status == RunRegistrationStatus.REGISTERED
    harmonization = result.runs[0].harmonization
    assert harmonization is not None
    # 9 tags total, 4 unmapped (host + the environmental-context triad) --
    # 5 mapped (collection date, geo-country, lat, long, depth).
    assert harmonization.mapped_count == 5
    assert harmonization.retained_unmapped == [
        "broad-scale environmental context",
        "environmental medium",
        "host",
        "local environmental context",
    ]
    assert harmonization.checklist_name == "ERC000011"
    # Both ERC000011-mandatory fields (collection date, geo country/sea) were
    # supplied -- the non-raising report has nothing to list (import
    # still succeeds either way).
    assert harmonization.missing_required == []

    biosample_idx = await reg["pool"].fetchval(
        "SELECT idx FROM qiita.biosample WHERE ena_sample_accession = $1", sample_accession
    )

    # biosample.metadata_checklist_idx names ERC000011.
    checklist_name = await reg["pool"].fetchval(
        "SELECT mc.name FROM qiita.biosample b"
        " JOIN qiita.metadata_checklist mc ON mc.idx = b.metadata_checklist_idx"
        " WHERE b.idx = $1",
        biosample_idx,
    )
    assert checklist_name == "ERC000011"

    # Mapped attrs land on the correct biosample_global_fields (joined
    # via global_field_idx).
    rows = await reg["pool"].fetch(
        "SELECT gf.display_name, bm.value_text, bm.value_numeric,"
        " bm.value_missing_reason_idx"
        " FROM qiita.biosample_metadata bm"
        " JOIN qiita.biosample_global_field gf ON gf.idx = bm.global_field_idx"
        " WHERE bm.biosample_idx = $1",
        biosample_idx,
    )
    by_display_name = {r["display_name"]: r for r in rows}
    assert set(by_display_name) == {
        "collection date",
        "geographic location (country and/or sea)",
        "geographic location (latitude)",
        "geographic location (longitude)",
        "depth",
    }
    # collection_date is TEXT (rebound from DATE by
    # 20260616000000_collection_date_text.sql) -- verbatim ISO8601 text.
    assert by_display_name["collection date"]["value_text"] == "2019-06-01"
    assert (
        by_display_name["geographic location (country and/or sea)"]["value_text"]
        == "USA: California"
    )
    assert by_display_name["geographic location (latitude)"]["value_numeric"] == Decimal("32.88")
    assert by_display_name["geographic location (longitude)"]["value_numeric"] == Decimal("-117.24")
    # The hard value: 'depth' = 'not collected' resolves as a missing-reason
    # marker via the known_missing_reasons wiring, not a MetadataParseError
    # (depth is NUMERIC-typed; 'not collected' is not a valid Decimal).
    depth_row = by_display_name["depth"]
    assert depth_row["value_numeric"] is None
    assert depth_row["value_missing_reason_idx"] is not None

    # Unmapped tags retained as local TEXT metadata, not dropped --
    # 'host' plus the environmental-context triad.
    local_rows = await reg["pool"].fetch(
        "SELECT bsf.display_name, bm.value_text"
        " FROM qiita.biosample_metadata bm"
        " JOIN qiita.biosample_study_field bsf ON bsf.idx = bm.biosample_study_field_idx"
        " WHERE bm.biosample_idx = $1 AND bm.global_field_idx IS NULL",
        biosample_idx,
    )
    local_by_display_name = {r["display_name"]: r["value_text"] for r in local_rows}
    assert local_by_display_name == {
        "host": "Homo sapiens",
        "broad-scale environmental context": "marine biome",
        "local environmental context": "coastal water",
        "environmental medium": "sea water",
    }


async def test_shared_biosample_harmonizes_once_across_two_studies(reg):
    shared_sample_accession = unique_accession("SAMN")
    attrs = _mixs_sample_attributes(shared_sample_accession)

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

    result_a = await _register(reg, study_header=header_a, runs=[run_a], sample_attributes=[attrs])
    result_b = await _register(reg, study_header=header_b, runs=[run_b], sample_attributes=[attrs])

    # Write-once: harmonization ran only on the FIRST import (the biosample
    # was newly created then); the second study's registration reuses the
    # biosample and does not re-harmonize it.
    assert result_a.runs[0].harmonization is not None
    assert result_b.runs[0].harmonization is None

    biosample_idx = await reg["pool"].fetchval(
        "SELECT idx FROM qiita.biosample WHERE ena_sample_accession = $1",
        shared_sample_accession,
    )

    # ONE canonical biosample_metadata value per global field -- not
    # duplicated or overwritten by the second study's registration.
    count = await reg["pool"].fetchval(
        "SELECT count(*) FROM qiita.biosample_metadata"
        " WHERE biosample_idx = $1 AND global_field_idx IS NOT NULL",
        biosample_idx,
    )
    assert count == 5

    # Reachable from both studies via biosample_to_study (existing
    # cross-study de-dup, unaffected by write-once harmonization).
    linked_studies = {
        r["study_idx"]
        for r in await reg["pool"].fetch(
            "SELECT study_idx FROM qiita.biosample_to_study WHERE biosample_idx = $1",
            biosample_idx,
        )
    }
    assert linked_studies == {result_a.study_idx, result_b.study_idx}


async def test_harmonization_parse_failure_isolated_to_its_run(reg):
    """A genuine harmonization failure (an unparseable mapped value) fails
    only that run -- the same per-run isolation as a platform/protocol-
    mapping failure; it never aborts sibling runs or the whole study."""
    study_accession = unique_accession("PRJNA")
    header = _study_header(study_accession=study_accession)

    ok_sample_accession = unique_accession("SAMN")
    ok_run = _run(
        run_accession=unique_accession("SRR"),
        experiment_accession=unique_accession("SRX"),
        sample_accession=ok_sample_accession,
        study_accession=study_accession,
    )
    ok_attrs = _mixs_sample_attributes(ok_sample_accession)

    bad_sample_accession = unique_accession("SAMN")
    bad_run = _run(
        run_accession=unique_accession("SRR"),
        experiment_accession=unique_accession("SRX"),
        sample_accession=bad_sample_accession,
        study_accession=study_accession,
    )
    # 'latitude' maps to a NUMERIC-typed global field; a non-numeric,
    # non-missing-marker text fails to parse -- a genuine data error, not a
    # missing-required-field gap.
    bad_attrs = _mixs_sample_attributes(
        bad_sample_accession, **{"geographic location (latitude)": "not-a-number"}
    )

    result = await _register(
        reg,
        study_header=header,
        runs=[ok_run, bad_run],
        sample_attributes=[ok_attrs, bad_attrs],
    )

    outcomes_by_accession = {o.run_accession: o for o in result.runs}
    ok_outcome = outcomes_by_accession[ok_run.run_accession]
    assert ok_outcome.status == RunRegistrationStatus.REGISTERED
    assert ok_outcome.harmonization is not None

    bad_outcome = outcomes_by_accession[bad_run.run_accession]
    assert bad_outcome.status == RunRegistrationStatus.FAILED
    assert bad_outcome.failure_reason is not None

    # No orphan biosample / metadata / link rows for the failed run --
    # the per-run transaction rolled back entirely.
    orphan_biosample_count = await reg["pool"].fetchval(
        "SELECT count(*) FROM qiita.biosample WHERE ena_sample_accession = $1",
        bad_sample_accession,
    )
    assert orphan_biosample_count == 0


async def test_underscore_mixs_tags_harmonize_to_correct_global_fields(reg):
    """Real DDBJ submitters use the underscore MIxS vocabulary
    (`collection_date`, `geo_loc_name`, `lat_lon`, `depth`) rather than the
    GSC-MIxS display-name form -- these must land on the same
    biosample_global_fields as their display-name twins (cross-study
    comparability), not stay unmapped."""
    study_accession = unique_accession("PRJDB")
    header = _study_header(study_accession=study_accession)
    sample_accession = unique_accession("SAMD")
    run = _run(
        run_accession=unique_accession("DRR"),
        experiment_accession=unique_accession("DRX"),
        sample_accession=sample_accession,
        study_accession=study_accession,
    )
    attrs = EnaSampleAttributes(
        sample_accession=sample_accession,
        attributes={
            "collection_date": "2021-11-15",
            # Real observed shape: PRJDB40386's SAMD01820063.
            "geo_loc_name": "Japan:Shinga, Ritsumeikan University BKC",
            "lat_lon": "35.6895 N 139.6917 E",
            "depth": "10",
            # Underscore form of the ENVO-typed triad -- same deferral as
            # the display-name form, stays unmapped/local.
            "env_broad_scale": "marine biome",
        },
    )

    result = await _register(reg, study_header=header, runs=[run], sample_attributes=[attrs])

    assert result.runs[0].status == RunRegistrationStatus.REGISTERED
    harmonization = result.runs[0].harmonization
    assert harmonization is not None
    # lat_lon splits into two mapped fields -- 5 tags in, but 4 lat_lon/
    # geo_loc_name/collection_date/depth values produce 5 mapped entries.
    assert harmonization.mapped_count == 5
    assert harmonization.retained_unmapped == ["env_broad_scale"]

    biosample_idx = await reg["pool"].fetchval(
        "SELECT idx FROM qiita.biosample WHERE ena_sample_accession = $1", sample_accession
    )
    rows = await reg["pool"].fetch(
        "SELECT gf.display_name, bm.value_text, bm.value_numeric"
        " FROM qiita.biosample_metadata bm"
        " JOIN qiita.biosample_global_field gf ON gf.idx = bm.global_field_idx"
        " WHERE bm.biosample_idx = $1",
        biosample_idx,
    )
    by_display_name = {r["display_name"]: r for r in rows}
    assert set(by_display_name) == {
        "collection date",
        "geographic location (country and/or sea)",
        "geographic location (latitude)",
        "geographic location (longitude)",
        "depth",
    }
    assert by_display_name["collection date"]["value_text"] == "2021-11-15"
    assert by_display_name["geographic location (country and/or sea)"]["value_text"] == "Japan"
    assert by_display_name["geographic location (latitude)"]["value_numeric"] == Decimal("35.6895")
    assert by_display_name["geographic location (longitude)"]["value_numeric"] == Decimal(
        "139.6917"
    )
    assert by_display_name["depth"]["value_numeric"] == Decimal("10")

    local_rows = await reg["pool"].fetch(
        "SELECT bsf.display_name, bm.value_text"
        " FROM qiita.biosample_metadata bm"
        " JOIN qiita.biosample_study_field bsf ON bsf.idx = bm.biosample_study_field_idx"
        " WHERE bm.biosample_idx = $1 AND bm.global_field_idx IS NULL",
        biosample_idx,
    )
    assert {r["display_name"]: r["value_text"] for r in local_rows} == {
        "env_broad_scale": "marine biome",
    }


async def test_empty_sample_attributes_registers_with_missing_required_report(reg):
    """A sample with zero ENA attributes (e.g. `resolve_sample_attributes`
    returning `[]` for a real DDBJ sample with no `<SAMPLE_ATTRIBUTE>`
    elements) must not fail the study: it registers normally, harmonizes
    against an empty attribute map (no globally-linked metadata), and the
    ERC000011 missing-required report lists both mandatory fields -- a
    report, never a rejection."""
    study_accession = unique_accession("PRJDB")
    header = _study_header(study_accession=study_accession)
    sample_accession = unique_accession("SAMD")
    run = _run(
        run_accession=unique_accession("DRR"),
        experiment_accession=unique_accession("DRX"),
        sample_accession=sample_accession,
        study_accession=study_accession,
    )

    # sample_attributes=[] -- no EnaSampleAttributes entry at all for this
    # sample, exactly what MiintEnaResolver/HttpEnaResolver.
    # resolve_sample_attributes now return for a study whose only sample has
    # zero <SAMPLE_ATTRIBUTE> elements.
    result = await _register(reg, study_header=header, runs=[run], sample_attributes=[])

    assert result.runs[0].status == RunRegistrationStatus.REGISTERED
    harmonization = result.runs[0].harmonization
    assert harmonization is not None
    assert harmonization.mapped_count == 0
    assert harmonization.retained_unmapped == []
    assert harmonization.checklist_name == "ERC000011"
    assert harmonization.missing_required == [
        "collection date",
        "geographic location (country and/or sea)",
    ]

    biosample_idx = await reg["pool"].fetchval(
        "SELECT idx FROM qiita.biosample WHERE ena_sample_accession = $1", sample_accession
    )
    global_metadata_count = await reg["pool"].fetchval(
        "SELECT count(*) FROM qiita.biosample_metadata"
        " WHERE biosample_idx = $1 AND global_field_idx IS NOT NULL",
        biosample_idx,
    )
    assert global_metadata_count == 0

    prep_sample_count = await reg["pool"].fetchval(
        "SELECT count(*) FROM qiita.prep_sample_to_study WHERE study_idx = $1", result.study_idx
    )
    assert prep_sample_count == 1


# ---------------------------------------------------------------------------
# Study upsert
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
# Cross-study biosample de-dup
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


# The same de-dup, but under REAL concurrency (asyncio.gather over
# two register_ena_study calls sharing a sample_accession), not just two
# sequential calls. The batch driver's bounded-concurrency phase
# processes multiple studies at once, so the cross-study biosample
# get-or-create (`get_or_create_biosample_by_ena_accession`'s `ON CONFLICT
# ... DO NOTHING` + fallback SELECT, repositories/biosample.py) must hold
# up when two of its callers race for real, not just when called back to
# back. Still ONE biosample row + TWO biosample_to_study links.
async def test_concurrent_registration_of_shared_biosample_dedupes_to_one_row(reg):
    import asyncio

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

    result_a, result_b = await asyncio.gather(
        _register(reg, study_header=header_a, runs=[run_a]),
        _register(reg, study_header=header_b, runs=[run_b]),
    )

    assert result_a.study_idx != result_b.study_idx

    biosample_rows = await reg["pool"].fetch(
        "SELECT idx FROM qiita.biosample WHERE ena_sample_accession = $1",
        shared_sample_accession,
    )
    assert len(biosample_rows) == 1
    biosample_idx = biosample_rows[0]["idx"]

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

    # Exactly one of the two runs harmonized-write-once (created the
    # biosample); the other reused it -- same write-once invariant as the
    # sequential test, still holding under real concurrency.
    harmonized_flags = sorted(
        [
            result_a.runs[0].harmonization is not None,
            result_b.runs[0].harmonization is not None,
        ]
    )
    assert harmonized_flags == [False, True]


# ---------------------------------------------------------------------------
# prep_sample / sequenced_sample creation, reserved-range invariant
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
# created_pools (additive to T02) -- surfaces the
# (platform, sequenced_pool_idx, sequencing_run_idx) triples the batch
# driver needs to build one download-ena-study ticket per pool, without
# re-deriving them from the DB.
# ---------------------------------------------------------------------------


async def test_created_pools_single_platform(reg):
    study_accession = unique_accession("PRJNA")
    header = _study_header(study_accession=study_accession)
    run = _run(
        run_accession=unique_accession("SRR"),
        experiment_accession=unique_accession("SRX"),
        sample_accession=unique_accession("SAMN"),
        study_accession=study_accession,
        instrument_platform="ILLUMINA",
    )

    result = await _register(reg, study_header=header, runs=[run])

    assert len(result.created_pools) == 1
    created = result.created_pools[0]
    assert created.platform == "illumina"

    pool_row = await reg["pool"].fetchrow(
        "SELECT sp.idx AS sequenced_pool_idx, sp.sequencing_run_idx"
        " FROM qiita.sequenced_pool sp"
        " JOIN qiita.sequencing_run sr ON sr.idx = sp.sequencing_run_idx"
        " WHERE sr.instrument_run_id LIKE $1",
        f"{study_accession}:%",
    )
    assert created.sequenced_pool_idx == pool_row["sequenced_pool_idx"]
    assert created.sequencing_run_idx == pool_row["sequencing_run_idx"]


async def test_created_pools_one_per_distinct_platform(reg):
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

    assert {c.platform for c in result.created_pools} == {"illumina", "oxford_nanopore"}
    assert len({c.sequenced_pool_idx for c in result.created_pools}) == 2
    assert len({c.sequencing_run_idx for c in result.created_pools}) == 2


async def test_created_pools_empty_when_every_run_fails(reg):
    study_accession = unique_accession("PRJNA")
    header = _study_header(study_accession=study_accession)
    bad_run = _run(
        run_accession=unique_accession("SRR"),
        experiment_accession=unique_accession("SRX"),
        sample_accession=unique_accession("SAMN"),
        study_accession=study_accession,
        instrument_platform="CAPILLARY",
    )

    result = await _register(reg, study_header=header, runs=[bad_run])

    assert result.created_pools == []


# ---------------------------------------------------------------------------
# Provenance columns persisted
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
    # transport stays NULL in T02 -- populated by the download workflow.
    assert row["transport"] is None


# ---------------------------------------------------------------------------
# Idempotency + partial-failure semantics
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


# ---------------------------------------------------------------------------
# Unmappable-platform failure isolation -- a bad `instrument_platform` fails
# only that run (mirrors the protocol-mapping partial-failure test above),
# it never aborts the whole study import.
# ---------------------------------------------------------------------------


async def test_unmappable_platform_run_isolated_others_registered(reg):
    study_accession = unique_accession("PRJNA")
    header = _study_header(study_accession=study_accession)
    ok_run = _run(
        run_accession=unique_accession("SRR"),
        experiment_accession=unique_accession("SRX"),
        sample_accession=unique_accession("SAMN"),
        study_accession=study_accession,
        instrument_platform="ILLUMINA",
        library_strategy="AMPLICON",
    )
    bad_sample_accession = unique_accession("SAMN")
    bad_run = _run(
        run_accession=unique_accession("SRR"),
        experiment_accession=unique_accession("SRX"),
        sample_accession=bad_sample_accession,
        study_accession=study_accession,
        # CAPILLARY has no qiita.platform counterpart -- platform_mapping.py.
        instrument_platform="CAPILLARY",
    )

    result = await _register(reg, study_header=header, runs=[ok_run, bad_run])

    outcomes_by_accession = {o.run_accession: o for o in result.runs}
    ok_outcome = outcomes_by_accession[ok_run.run_accession]
    assert ok_outcome.status == RunRegistrationStatus.REGISTERED
    assert ok_outcome.sequenced_sample_idx is not None

    bad_outcome = outcomes_by_accession[bad_run.run_accession]
    assert bad_outcome.status == RunRegistrationStatus.FAILED
    assert bad_outcome.failure_reason is not None
    assert "CAPILLARY" in bad_outcome.failure_reason

    # Exactly one sequencing_run / sequenced_pool for this study -- only the
    # ILLUMINA platform of the good run, none for the unmappable platform.
    run_rows = await reg["pool"].fetch(
        "SELECT idx, platform FROM qiita.sequencing_run WHERE instrument_run_id LIKE $1",
        f"{study_accession}:%",
    )
    assert len(run_rows) == 1
    assert run_rows[0]["platform"] == "illumina"
    pool_count = await reg["pool"].fetchval(
        "SELECT count(*) FROM qiita.sequenced_pool WHERE sequencing_run_idx = $1",
        run_rows[0]["idx"],
    )
    assert pool_count == 1

    # No orphan rows at all for the platform-failed run -- it fails before
    # any per-run write is attempted, unlike the protocol-mapping failure
    # case above (which does commit the biosample/link before rolling back).
    orphan_sequenced_sample_count = await reg["pool"].fetchval(
        "SELECT count(*) FROM qiita.sequenced_sample WHERE ena_run_accession = $1",
        bad_run.run_accession,
    )
    assert orphan_sequenced_sample_count == 0
    orphan_biosample_count = await reg["pool"].fetchval(
        "SELECT count(*) FROM qiita.biosample WHERE ena_sample_accession = $1",
        bad_sample_accession,
    )
    assert orphan_biosample_count == 0
    orphan_link_count = await reg["pool"].fetchval(
        "SELECT count(*) FROM qiita.biosample_to_study bts"
        " JOIN qiita.biosample b ON b.idx = bts.biosample_idx"
        " WHERE b.ena_sample_accession = $1",
        bad_sample_accession,
    )
    assert orphan_link_count == 0


async def test_all_unmappable_platform_study_all_failed_no_runs_or_pools(reg):
    study_accession = unique_accession("PRJNA")
    header = _study_header(study_accession=study_accession)
    bad_run_1 = _run(
        run_accession=unique_accession("SRR"),
        experiment_accession=unique_accession("SRX"),
        sample_accession=unique_accession("SAMN"),
        study_accession=study_accession,
        instrument_platform="CAPILLARY",
    )
    bad_run_2 = _run(
        run_accession=unique_accession("SRR"),
        experiment_accession=unique_accession("SRX"),
        sample_accession=unique_accession("SAMN"),
        study_accession=study_accession,
        instrument_platform=None,
    )

    result = await _register(reg, study_header=header, runs=[bad_run_1, bad_run_2])

    assert {o.status for o in result.runs} == {RunRegistrationStatus.FAILED}
    assert {o.run_accession for o in result.runs} == {
        bad_run_1.run_accession,
        bad_run_2.run_accession,
    }

    run_count = await reg["pool"].fetchval(
        "SELECT count(*) FROM qiita.sequencing_run WHERE instrument_run_id LIKE $1",
        f"{study_accession}:%",
    )
    assert run_count == 0
