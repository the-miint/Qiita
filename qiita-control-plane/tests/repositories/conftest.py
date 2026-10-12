"""Shared fixtures and helpers for the biosample-family repository tests.

The committed-fixture / swept-teardown pattern (Pattern 2) lives here so
test_biosample.py and test_biosample_metadata.py share a single source of
truth for principal/study/checklist seeding, in-test setup, and teardown.
The transaction-rollback pattern (Pattern 1) used by the trigger tests at
the bottom of test_biosample.py keeps its own conn-style helpers inline
because those tests neither commit nor share state.

The `pool_ctx` fixture at the bottom covers the other shared shape in this
directory: one sequencing_run and one sequenced_pool, with samples attached on
demand, for the modules that read a pool rather than each seeding the same run
and pool. A module in this directory that needs neither shape defines its own
helpers.
"""

import json
import secrets
from typing import NamedTuple

import pytest_asyncio
from qiita_common.auth_constants import SYSTEM_PRINCIPAL_IDX
from qiita_common.models import FieldDataType

from qiita_control_plane.repositories._sample_helpers import (
    FieldRow,
    SampleEntityKind,
    _get_or_create_local_study_field,
    _insert_metadata,
    insert_entity_to_study,
)
from qiita_control_plane.repositories.biosample import insert_biosample
from qiita_control_plane.repositories.biosample_metadata import BIOSAMPLE_METADATA_SPEC
from qiita_control_plane.repositories.prep_sample_metadata import PREP_SAMPLE_METADATA_SPEC
from qiita_control_plane.testing.db_seeds import (
    seed_biosample_global_field,
    seed_biosample_with_sequenced_prep_sample,
    seed_local_study_field,
    seed_prep_sample_global_field,
    seed_sequenced_prep_sample,
    seed_sequenced_sample_subtype,
    seed_study,
    seed_user_principal,
)
from qiita_control_plane.testing.db_teardown import (
    delete_idxs,
    delete_principal,
    teardown_entity_graph,
)
from qiita_control_plane.testing.unique_names import unique_field_name

# Both stacks run every test parameterized over this; pytest reports ids as
# [biosample] / [prep_sample].
SPECS = [BIOSAMPLE_METADATA_SPEC, PREP_SAMPLE_METADATA_SPEC]

# Declaring a field unique and widening one both lock the metadata table, and
# neither runs without a bound on that wait. Any bound satisfies it; this one
# only has to outlast an uncontended operation, so it tracks nothing in
# production.
METADATA_WRITE_LOCK_TIMEOUT = "3s"


def _spec_id(spec):
    """Pytest id for the parametrize decorator: spec.entity_kind value."""
    return spec.entity_kind.value


# ---------------------------------------------------------------------------
# Study-field and metadata seeding, parameterized over the entity spec
# ---------------------------------------------------------------------------


async def _create_plain_field(
    ctx, spec, *, suffix, data_type=FieldDataType.TEXT, terminology_idx=None
):
    """Create a purely-local study field with no uniqueness policy. Returns
    the field idx. terminology_idx is required when data_type is terminology
    and refused otherwise, per the field row's own coupling.
    """
    async with ctx["pool"].acquire() as conn, conn.transaction():
        field_idx, _, _ = await _get_or_create_local_study_field(
            conn,
            spec=spec,
            study_idx=ctx["study_idx"],
            display_name=unique_field_name(suffix),
            created_by_idx=ctx["principal_idx"],
            data_type=data_type,
            required=False,
            terminology_idx=terminology_idx,
        )
    return field_idx


async def _set_unique_in_study(ctx, spec, field_idx, value):
    """Flip a study field's unique_in_study, driving the propagation trigger."""
    # One transaction so the bound reaches the propagation; a SET LOCAL issued
    # on its own would land in a separate implicit transaction and do nothing.
    async with ctx["pool"].acquire() as conn, conn.transaction():
        await conn.execute(f"SET LOCAL lock_timeout = '{METADATA_WRITE_LOCK_TIMEOUT}'")
        await conn.execute(
            f"UPDATE {spec.study_field_table} SET unique_in_study = $1 WHERE idx = $2",
            value,
            field_idx,
        )


async def _write_value(ctx, spec, *, entity_idx, field_idx, data_type, value):
    """Write one metadata row and return its idx."""
    async with ctx["pool"].acquire() as conn, conn.transaction():
        meta_idx = await _insert_metadata(
            conn,
            spec=spec,
            entity_idx=entity_idx,
            study_field_idx=field_idx,
            data_type=data_type,
            value=value,
            created_by_idx=ctx["principal_idx"],
        )
    return meta_idx


# ---------------------------------------------------------------------------
# Pool-based seed helpers (Pattern 2 — committed rows, swept teardown)
# ---------------------------------------------------------------------------


async def _seed_principal(pool, display_name, *, created_by_idx):
    """Insert a qiita.principal row with the given parent, return its idx.

    The parent is required so callers cannot accidentally seed a root
    principal; the system principal at idx=1 is the standard root for
    test fixtures.
    """
    return await pool.fetchval(
        "INSERT INTO qiita.principal (display_name, created_by_idx) VALUES ($1, $2) RETURNING idx",
        display_name,
        created_by_idx,
    )


async def _seed_user(pool, principal_idx, email):
    """Promote a principal to user-kind by inserting a qiita.user row.

    Required so the principal can serve as study.owner_idx (and similar
    role-typed FK columns); the trigger on those columns rejects bare
    principals. Only the required columns are populated; all other
    qiita.user columns carry NOT NULL DEFAULT '' or are nullable.
    """
    return await pool.fetchval(
        "INSERT INTO qiita.user (principal_idx, email) VALUES ($1, $2) RETURNING principal_idx",
        principal_idx,
        email,
    )


async def _seed_metadata_checklist(pool, name):
    """Insert a minimal qiita.metadata_checklist row, return its idx."""
    return await pool.fetchval(
        "INSERT INTO qiita.metadata_checklist (name) VALUES ($1) RETURNING idx",
        name,
    )


# ---------------------------------------------------------------------------
# Post-sweep cleanup
# ---------------------------------------------------------------------------


async def _cleanup_tracked(pool, created):
    """Delete the tracked rows that hang off no entity.

    Anything belonging to a study, biosample or prep_sample is swept by
    `teardown_entity_graph` before this runs. What is left is FK-reverse among
    itself: the global fields and terms reference terminology.
    """
    await delete_idxs(pool, "biosample_global_field", created["biosample_global_field"])
    await delete_idxs(pool, "prep_sample_global_field", created["prep_sample_global_field"])
    await delete_idxs(pool, "terminology_term", created["terminology_term"])
    await delete_idxs(pool, "missing_value_reason", created["missing_value_reason"])
    await delete_idxs(pool, "terminology", created["terminology"])


# ---------------------------------------------------------------------------
# Per-test fixture
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def ctx(postgres_pool):
    """Seed two principals, a user, a study, and a metadata checklist.

    Each test gets fresh seed rows (suffixed with a token to avoid collisions
    across re-runs) plus an empty `created` dict the test populates with idxs
    of any rows it inserts.

    Both principals are promoted to user-kind via qiita.user rows so
    they can serve as study.owner_idx and biosample.owner_idx; the
    role-typed FK triggers on those columns reject non-user-kind
    principals.
    """
    # Token-suffixed names avoid UNIQUE collisions if a prior run leaked rows.
    # Two principals are seeded so composer tests can exercise the case where
    # the biosample owner is a different principal than the one running the
    # call (e.g., an admin importing on behalf of an owner). principal_idx
    # is the caller / study owner; biosample_owner_idx is a peer principal.
    token = secrets.token_hex(4)
    principal_idx = await _seed_principal(
        postgres_pool, f"bs-{token}", created_by_idx=SYSTEM_PRINCIPAL_IDX
    )
    await _seed_user(postgres_pool, principal_idx, f"bs-{token}@test.local")
    biosample_owner_idx = await _seed_principal(
        postgres_pool, f"bs-owner-{token}", created_by_idx=principal_idx
    )
    await _seed_user(postgres_pool, biosample_owner_idx, f"bs-owner-{token}@test.local")
    study_idx = await seed_study(postgres_pool, owner_idx=principal_idx, title=f"bs-{token}")
    checklist_name = f"bs-checklist-{token}"
    checklist_idx = await _seed_metadata_checklist(postgres_pool, checklist_name)

    # Test-populated tracking dict; every list holds idxs. `studies` holds any
    # extra studies the test seeds beyond the one auto-seeded above. The three
    # entity lists feed the sweep; the rest hang off no entity. A row belonging
    # to an entity needs no entry here — the sweep finds it by parent FK.
    created: dict = {
        "biosample": [],
        "prep_sample": [],
        "studies": [],
        "biosample_global_field": [],
        "prep_sample_global_field": [],
        "terminology_term": [],
        "missing_value_reason": [],
        "terminology": [],
    }

    yield {
        "pool": postgres_pool,
        "principal_idx": principal_idx,
        "biosample_owner_idx": biosample_owner_idx,
        "study_idx": study_idx,
        "checklist_idx": checklist_idx,
        "checklist_name": checklist_name,
        "created": created,
    }

    # The fixture's own study and any the test added go in one list: the sweep
    # keys on the parent FK, so both must be in range before either is deleted.
    await teardown_entity_graph(
        postgres_pool,
        study_idxs=[study_idx, *created["studies"]],
        biosample_idxs=created["biosample"],
        prep_sample_idxs=created["prep_sample"],
    )
    await _cleanup_tracked(postgres_pool, created)
    await delete_idxs(postgres_pool, "metadata_checklist", checklist_idx)
    await delete_principal(postgres_pool, [principal_idx, biosample_owner_idx])


# ---------------------------------------------------------------------------
# In-test setup helpers (use the ctx fixture's principal/study)
# ---------------------------------------------------------------------------


async def _insert_owned_biosample(conn, ctx):
    """Insert a biosample owned by ctx['principal_idx'] on the given conn,
    return its idx. Shared by the standalone and linked create helpers so
    the owner/creator wiring lives in one place.
    """
    return await insert_biosample(
        conn,
        owner_idx=ctx["principal_idx"],
        created_by_idx=ctx["principal_idx"],
    )


async def _create_biosample(ctx):
    """Helper: create a biosample owned by ctx['principal_idx'].

    Recorded on the ctx fixture, which hands it to the sweep as an entity.
    """
    async with ctx["pool"].acquire() as conn:
        idx = await _insert_owned_biosample(conn, ctx)
    ctx["created"]["biosample"].append(idx)
    return idx


async def _create_biosample_with_link(ctx):
    """Helper: atomically create a biosample and link it to ctx['study_idx'].

    Both inserts share one connection and one transaction so a failed link
    cannot leak an orphan biosample row for teardown to mop up. The biosample is
    recorded on the ctx fixture so the sweep is given it as an entity; the link
    row the sweep finds by parent FK.
    """
    async with ctx["pool"].acquire() as conn:
        async with conn.transaction():
            bs_idx = await _insert_owned_biosample(conn, ctx)
            await insert_entity_to_study(
                conn,
                spec=BIOSAMPLE_METADATA_SPEC,
                entity_idx=bs_idx,
                study_idx=ctx["study_idx"],
                created_by_idx=ctx["principal_idx"],
            )
    ctx["created"]["biosample"].append(bs_idx)
    return bs_idx


async def _create_local_field(ctx, suffix=""):
    """Helper: create a purely-local biosample_study_field."""
    field_name = f"{unique_field_name()}_{suffix}"
    idx = await seed_local_study_field(
        ctx["pool"],
        spec=BIOSAMPLE_METADATA_SPEC,
        study_idx=ctx["study_idx"],
        display_name=field_name,
        created_by_idx=ctx["principal_idx"],
        required=True,
    )
    return idx


async def _create_prep_sample_with_link(ctx):
    """Helper: create a biosample+link, then a sequenced prep_sample linked
    to ctx['study_idx']. Returns the prep_sample idx; both entities are
    recorded on the ctx fixture for the sweep.

    The prep_sample requires its biosample to carry a biosample_to_study
    link (prep_sample_to_study_reject_without_biosample_link trigger), so
    this builds on _create_biosample_with_link.
    """
    bs_idx = await _create_biosample_with_link(ctx)
    ps_idx = await seed_sequenced_prep_sample(
        ctx["pool"],
        biosample_idx=bs_idx,
        owner_idx=ctx["principal_idx"],
    )
    async with ctx["pool"].acquire() as conn:
        async with conn.transaction():
            await insert_entity_to_study(
                conn,
                spec=PREP_SAMPLE_METADATA_SPEC,
                entity_idx=ps_idx,
                study_idx=ctx["study_idx"],
                created_by_idx=ctx["principal_idx"],
            )
    ctx["created"]["prep_sample"].append(ps_idx)
    return ps_idx


# ---------------------------------------------------------------------------
# Parametrized sample-family helpers (biosample + prep_sample)
# ---------------------------------------------------------------------------


async def _seed_unlinked_entity_for_spec(ctx, spec):
    """Seed the substrate for an insert_entity_to_study test under spec:
      - biosample spec: a fresh biosample (no link).
      - prep_sample spec: a biosample linked to ctx['study_idx'] (so the
        prep_sample_to_study trigger has its substrate) plus a fresh
        prep_sample on that biosample (no prep_sample_to_study link).

    Returns the entity_idx the caller should pass to insert_entity_to_study.
    The entity is recorded on the ctx fixture, which hands it to the sweep.
    """
    # biosample branch: bare biosample; the caller is expected to write
    # the link row itself in the test body.
    if spec.entity_kind is SampleEntityKind.BIOSAMPLE:
        return await _create_biosample(ctx)

    # prep_sample branch: link biosample first so the
    # reject_without_biosample_link trigger passes when the test later
    # writes prep_sample_to_study.
    bs_idx = await _create_biosample_with_link(ctx)
    ps_idx = await seed_sequenced_prep_sample(
        ctx["pool"], biosample_idx=bs_idx, owner_idx=ctx["principal_idx"]
    )
    ctx["created"]["prep_sample"].append(ps_idx)
    return ps_idx


async def _seed_secondary_studies_for_entity(ctx, spec, entity_idx, count):
    """Seed `count` additional studies the entity can be linked to. For
    prep_sample spec, also write a biosample_to_study link from the
    underlying biosample to each new study (so the
    reject_without_biosample_link trigger passes when the entity is
    later linked to that study).

    Returns the list of new study idxs in creation order.
    """
    # Recover the biosample idx for prep_sample so we can pre-link it to
    # each new secondary study before the entity's own link gets written.
    biosample_idx: int | None = None
    if spec.entity_kind is SampleEntityKind.PREP_SAMPLE:
        biosample_idx = await ctx["pool"].fetchval(
            "SELECT biosample_idx FROM qiita.prep_sample WHERE idx = $1",
            entity_idx,
        )

    # Seed each study and its (optional) biosample link.
    new_study_idxs: list[int] = []
    for _ in range(count):
        st_idx = await seed_study(
            ctx["pool"], owner_idx=ctx["principal_idx"], title=f"sec-{secrets.token_hex(4)}"
        )
        ctx["created"]["studies"].append(st_idx)
        new_study_idxs.append(st_idx)
        if biosample_idx is not None:
            async with ctx["pool"].acquire() as conn:
                await insert_entity_to_study(
                    conn,
                    spec=BIOSAMPLE_METADATA_SPEC,
                    entity_idx=biosample_idx,
                    study_idx=st_idx,
                    created_by_idx=ctx["principal_idx"],
                )
    return new_study_idxs


async def _seed_global_field_for_spec(
    ctx, spec, data_type=FieldDataType.TEXT, terminology_idx=None, internal_name=None
):
    """Seed one global field of the given data_type for spec.entity_kind.

    Returns a FieldRow shape so the caller can drive metadata writes against it
    directly. terminology_idx must be supplied when data_type=TERMINOLOGY (the
    *_global_field CHECK enforces the iff coupling) and omitted otherwise.
    internal_name
    defaults to a generated unique value; pass it when a test keys on the
    internal_name (internal-name resolution), since FieldRow carries only
    the display_name.
    """
    # Token suffix defends against unique-name collisions across re-runs.
    suffix = secrets.token_hex(4)
    internal_name = internal_name if internal_name is not None else f"gf_{suffix}"
    display_name = f"GF {suffix}"

    # Branch on the spec's entity_kind to pick the matching seed helper;
    # both write to structurally-parallel *_global_field tables.
    if spec.entity_kind is SampleEntityKind.BIOSAMPLE:
        gf_idx = await seed_biosample_global_field(
            ctx["pool"],
            internal_name=internal_name,
            display_name=display_name,
            data_type=data_type,
            created_by_idx=ctx["principal_idx"],
            terminology_idx=terminology_idx,
        )
        ctx["created"]["biosample_global_field"].append(gf_idx)
    else:
        gf_idx = await seed_prep_sample_global_field(
            ctx["pool"],
            internal_name=internal_name,
            display_name=display_name,
            data_type=data_type,
            created_by_idx=ctx["principal_idx"],
            terminology_idx=terminology_idx,
        )
        ctx["created"]["prep_sample_global_field"].append(gf_idx)
    return FieldRow(
        idx=gf_idx,
        display_name=display_name,
        data_type=data_type,
        terminology_idx=terminology_idx,
        global_field_idx=gf_idx,
        internal_name=internal_name,
    )


async def _create_linked_entity_for_spec(ctx, spec):
    """Create an entity linked to ctx['study_idx'] for spec.entity_kind.

    Returns the new entity_idx; every entity it seeds is recorded on the ctx
    fixture for the sweep.

    Dispatches to the existing per-entity helpers so the prep_sample branch
    transparently seeds the underlying biosample and biosample_to_study link
    that the prep_sample_to_study trigger requires as substrate.
    """
    # Both branches return an entity_idx whose *_to_study link is already
    # written; callers can write metadata against it without further setup.
    if spec.entity_kind is SampleEntityKind.BIOSAMPLE:
        return await _create_biosample_with_link(ctx)
    return await _create_prep_sample_with_link(ctx)


# ---------------------------------------------------------------------------
# Sequenced-pool fixture (sequencing_run → sequenced_pool → sequenced_sample)
# ---------------------------------------------------------------------------


class SeededSample(NamedTuple):
    """The three idxs one `add_sample` call wrote.

    Named rather than positional so a caller cannot bind the wrong entity: the
    pool reads are keyed on prep_sample, while the read-count and QC columns
    live on the sequenced_sample.
    """

    biosample_idx: int
    prep_sample_idx: int
    sequenced_sample_idx: int


def _qc_report(point: str) -> str:
    """One serialized QC-report payload, for a sample seeded with reports.

    The shape is all the pool-report reads care about; no test asserts on the
    values, only on whether a blob is present.
    """
    return json.dumps(
        {"point": point, "layout": "single", "read_pairs": 1, "mates": {"r1": None, "r2": None}}
    )


@pytest_asyncio.fixture
async def pool_ctx(postgres_pool):
    """Seed a principal, one sequencing_run and one sequenced_pool.

    `add_sample(...)` attaches one sequenced_sample to the pool and returns a
    `SeededSample`. Every keyword is optional, so a bare call attaches a sample
    carrying no reads, no reports and no accessions; each group of columns is
    written only when asked for, leaving the rest at their defaults.
    """
    owner_idx = await seed_user_principal(postgres_pool, prefix="pool", suffix="owner")
    run_idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.sequencing_run (instrument_run_id, platform, created_by_idx)"
        " VALUES ($1, 'illumina'::qiita.platform, $2) RETURNING idx",
        f"pool-run-{secrets.token_hex(4)}",
        owner_idx,
    )
    pool_idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.sequenced_pool (sequencing_run_idx, created_by_idx)"
        " VALUES ($1, $2) RETURNING idx",
        run_idx,
        owner_idx,
    )
    biosample_idxs: list[int] = []
    prep_sample_idxs: list[int] = []

    async def add_sample(
        *,
        raw=None,
        biological=None,
        quality_filtered=None,
        spikein=None,
        with_reports=False,
        retired=False,
        ena_status=None,
        biosample_accession=None,
        ena_sample_accession=None,
        ena_experiment_accession=None,
        ena_run_accession=None,
    ):
        bs_idx, ps_idx = await seed_biosample_with_sequenced_prep_sample(
            postgres_pool, owner_idx=owner_idx
        )
        _run, _pool, ss_idx = await seed_sequenced_sample_subtype(
            postgres_pool,
            prep_sample_idx=ps_idx,
            owner_idx=owner_idx,
            sequenced_pool_item_id=f"item-{secrets.token_hex(4)}",
            sequencing_run_idx=run_idx,
            sequenced_pool_idx=pool_idx,
        )

        # The biosample-side accessions travel together; either one asks for the write.
        if biosample_accession is not None or ena_sample_accession is not None:
            await postgres_pool.execute(
                "UPDATE qiita.biosample SET biosample_accession = $2,"
                " ena_sample_accession = $3 WHERE idx = $1",
                bs_idx,
                biosample_accession,
                ena_sample_accession,
            )
        if ena_experiment_accession is not None or ena_run_accession is not None:
            await postgres_pool.execute(
                "UPDATE qiita.sequenced_sample SET ena_experiment_accession = $2,"
                " ena_run_accession = $3 WHERE idx = $1",
                ss_idx,
                ena_experiment_accession,
                ena_run_accession,
            )

        # `raw` gates the whole read-count group so a caller naming only the
        # later stages cannot leave a half-populated row.
        if raw is not None:
            await postgres_pool.execute(
                "UPDATE qiita.sequenced_sample SET raw_read_count_r1r2 = $2,"
                " biological_read_count_r1r2 = $3, quality_filtered_read_count_r1r2 = $4,"
                " spikein_read_count_r1r2 = $5 WHERE idx = $1",
                ss_idx,
                raw,
                biological,
                quality_filtered,
                spikein,
            )
        if with_reports:
            await postgres_pool.execute(
                "UPDATE qiita.sequenced_sample SET raw_qc_report = $2::jsonb,"
                " filtered_qc_report = $3::jsonb WHERE idx = $1",
                ss_idx,
                _qc_report("raw"),
                _qc_report("filtered"),
            )

        # Retirement and the ENA flag are the two exclusion paths the pool reads
        # filter on, so a test asks for one to assert the sample drops out.
        if retired:
            await postgres_pool.execute(
                "UPDATE qiita.prep_sample SET retired = true, retired_by_idx = $2,"
                " retired_at = now(), retire_reason = 'test' WHERE idx = $1",
                ps_idx,
                owner_idx,
            )
        if ena_status is not None:
            await postgres_pool.execute(
                "UPDATE qiita.sequenced_sample SET ena_status = $2,"
                " ena_availability_checked_at = now() WHERE idx = $1",
                ss_idx,
                ena_status,
            )

        biosample_idxs.append(bs_idx)
        prep_sample_idxs.append(ps_idx)
        return SeededSample(
            biosample_idx=bs_idx, prep_sample_idx=ps_idx, sequenced_sample_idx=ss_idx
        )

    yield {
        "pool": postgres_pool,
        "owner_idx": owner_idx,
        "run_idx": run_idx,
        "pool_idx": pool_idx,
        "add_sample": add_sample,
    }

    await teardown_entity_graph(
        postgres_pool,
        study_idxs=[],
        biosample_idxs=biosample_idxs,
        prep_sample_idxs=prep_sample_idxs,
    )
    await delete_idxs(postgres_pool, "sequenced_pool", pool_idx)
    await delete_idxs(postgres_pool, "sequencing_run", run_idx)
    await delete_principal(postgres_pool, [owner_idx])
