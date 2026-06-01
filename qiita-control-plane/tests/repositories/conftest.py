"""Shared fixtures and helpers for the biosample-family repository tests.

The committed-fixture / FK-reverse-cleanup pattern (Pattern 2) lives here so
test_biosample.py and test_biosample_metadata.py share a single source of
truth for principal/study/checklist seeding, in-test setup, and teardown.
The transaction-rollback pattern (Pattern 1) used by the trigger tests at
the bottom of test_biosample.py keeps its own conn-style helpers inline
because those tests neither commit nor share state.

Other repository test files (test_study.py, test_study_access.py,
test_user_eligibility.py) define their own helpers and do not consume
this conftest's fixture; the scope here is biosample-family until a
real second consumer surfaces.
"""

import secrets

import pytest_asyncio
from qiita_common.auth_constants import SYSTEM_PRINCIPAL_IDX
from qiita_common.models import FieldDataType

from qiita_control_plane.repositories._sample_helpers import (
    GlobalFieldRow,
    SampleEntityKind,
    _get_or_create_local_study_field,
    insert_entity_to_study,
)
from qiita_control_plane.repositories.biosample import insert_biosample
from qiita_control_plane.repositories.biosample_metadata import BIOSAMPLE_METADATA_SPEC
from qiita_control_plane.repositories.prep_sample_metadata import PREP_SAMPLE_METADATA_SPEC
from qiita_control_plane.testing.db_seeds import (
    seed_biosample_global_field,
    seed_prep_sample_global_field,
    seed_sequenced_prep_sample,
)
from qiita_control_plane.testing.unique_names import unique_field_name

# ---------------------------------------------------------------------------
# Pool-based seed helpers (Pattern 2 — committed rows, FK-reverse cleanup)
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


async def _seed_study(pool, owner_idx, title):
    """Insert a minimal qiita.study row, return its idx."""
    return await pool.fetchval(
        "INSERT INTO qiita.study (owner_idx, title, created_by_idx)"
        " VALUES ($1, $2, $1) RETURNING idx",
        owner_idx,
        title,
    )


async def _seed_metadata_checklist(pool, name):
    """Insert a minimal qiita.metadata_checklist row, return its idx."""
    return await pool.fetchval(
        "INSERT INTO qiita.metadata_checklist (name) VALUES ($1) RETURNING idx",
        name,
    )


# ---------------------------------------------------------------------------
# FK-reverse cleanup
# ---------------------------------------------------------------------------


async def _delete_idxs(pool, table, idxs):
    """Delete rows by idx from qiita.<table>.

    `idxs` may be a scalar int or an iterable of ints; an empty iterable
    is a no-op. The scalar form is normalised so callers can pass a single
    auto-seeded idx without wrapping in a list.
    """
    # Normalize a bare int into a one-element list so callers can pass either.
    if isinstance(idxs, int):
        idxs = [idxs]
    if not idxs:
        return
    await pool.execute(
        f"DELETE FROM qiita.{table} WHERE idx = ANY($1::bigint[])",
        idxs,
    )


async def _cleanup_tracked(pool, created):
    """FK-reverse cleanup of every row tracked in `created`.

    The order encodes FK dependencies; do not reorder. biosample_to_study
    is composite-keyed so it is handled separately from the idx-keyed sweep.
    Empty lists for tables a given test does not seed are no-ops via
    `_delete_idxs`, so the sweep is free for tests that only touch the
    common biosample surface.
    """
    # Sweep the EAV value rows first; they reference everything else.
    await _delete_idxs(pool, "biosample_metadata", created["biosample_metadata"])
    # Field rows reference biosample_global_field and terminology.
    await _delete_idxs(pool, "biosample_study_field", created["biosample_study_field"])
    for bs, st in created["biosample_to_study"]:
        await pool.execute(
            "DELETE FROM qiita.biosample_to_study WHERE biosample_idx = $1 AND study_idx = $2",
            bs,
            st,
        )
    # prep_sample side, FK-reverse: EAV value rows, then field rows, then
    # the composite-keyed link, then the prep_sample itself — which must
    # go before its biosample (prep_sample.biosample_idx FK) is swept just
    # below. prep_sample_to_study is composite-keyed like biosample_to_study.
    await _delete_idxs(pool, "prep_sample_metadata", created["prep_sample_metadata"])
    await _delete_idxs(pool, "prep_sample_study_field", created["prep_sample_study_field"])
    for ps, st in created["prep_sample_to_study"]:
        await pool.execute(
            "DELETE FROM qiita.prep_sample_to_study WHERE prep_sample_idx = $1 AND study_idx = $2",
            ps,
            st,
        )
    # Subtype rows (sequenced_sample) reference prep_sample with ON DELETE
    # RESTRICT, so they must go before the prep_sample sweep below. Tests
    # that don't seed a subtype leave this list empty and the call is a
    # no-op via _delete_idxs.
    await _delete_idxs(pool, "sequenced_sample", created["sequenced_sample"])
    await _delete_idxs(pool, "prep_sample", created["prep_sample"])
    await _delete_idxs(pool, "biosample", created["biosample"])
    # study_access references study with ON DELETE RESTRICT, so any
    # study_access rows seeded by tests must go before the auto-seeded
    # study row deletion at the end of the fixture.
    await _delete_idxs(pool, "study_access", created["study_access"])
    # biosample_global_field and terminology_term both reference terminology;
    # missing_value_reason has no inbound refs left after biosample_metadata.
    await _delete_idxs(pool, "biosample_global_field", created["biosample_global_field"])
    # prep_sample_study_field and prep_sample_metadata were swept above, so
    # prep_sample_global_field has no inbound refs left at this tier.
    await _delete_idxs(pool, "prep_sample_global_field", created["prep_sample_global_field"])
    await _delete_idxs(pool, "terminology_term", created["terminology_term"])
    await _delete_idxs(pool, "missing_value_reason", created["missing_value_reason"])
    await _delete_idxs(pool, "terminology", created["terminology"])
    await _delete_idxs(pool, "study", created["studies"])


# ---------------------------------------------------------------------------
# Per-test fixture
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def ctx(postgres_pool):
    """Seed two principals, a user, a study, and a metadata checklist.

    Each test gets fresh seed rows (suffixed with a token to avoid collisions
    across re-runs) plus an empty `created` dict the test populates with idxs
    of any rows it inserts. Cleanup runs in FK-reverse order after the test.

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
    study_idx = await _seed_study(postgres_pool, principal_idx, f"bs-{token}")
    checklist_idx = await _seed_metadata_checklist(postgres_pool, f"bs-{token}")

    # Test-populated tracking dict; lists hold idxs (or (bs, st) tuples).
    # `studies` holds idxs of any extra studies the test seeds beyond the
    # one auto-seeded above; they are deleted after the biosample-side rows
    # are swept and before the auto-seeded study row is dropped.
    created: dict = {
        "biosample_metadata": [],
        "biosample_study_field": [],
        "biosample_to_study": [],
        "biosample": [],
        "biosample_global_field": [],
        "prep_sample_metadata": [],
        "prep_sample_study_field": [],
        "prep_sample": [],
        "prep_sample_to_study": [],
        "sequenced_sample": [],
        "prep_sample_global_field": [],
        "terminology_term": [],
        "missing_value_reason": [],
        "terminology": [],
        "study_access": [],
        "studies": [],
    }

    yield {
        "pool": postgres_pool,
        "principal_idx": principal_idx,
        "biosample_owner_idx": biosample_owner_idx,
        "study_idx": study_idx,
        "checklist_idx": checklist_idx,
        "created": created,
    }

    # Sweep test-populated rows then the auto-seeded support rows.
    await _cleanup_tracked(postgres_pool, created)
    await _delete_idxs(postgres_pool, "metadata_checklist", checklist_idx)
    await _delete_idxs(postgres_pool, "study", study_idx)
    # qiita.user → qiita.principal is ON DELETE RESTRICT, so the user rows
    # must go before the principals they reference. The role-typed
    # user_no_delete_if_study_owner and user_no_delete_if_biosample_owner
    # triggers pass because the study and biosample rows above have already
    # been removed.
    await postgres_pool.execute(
        "DELETE FROM qiita.user WHERE principal_idx = ANY($1::bigint[])",
        [principal_idx, biosample_owner_idx],
    )
    # principal FK is DEFERRABLE INITIALLY DEFERRED, so deleting both rows in
    # one statement is fine — the biosample_owner_idx → principal_idx
    # reference is checked at commit, after both rows are gone.
    await _delete_idxs(postgres_pool, "principal", [biosample_owner_idx, principal_idx])


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
    """Helper: create a biosample owned by ctx['principal_idx'], track for cleanup."""
    async with ctx["pool"].acquire() as conn:
        idx = await _insert_owned_biosample(conn, ctx)
    ctx["created"]["biosample"].append(idx)
    return idx


async def _create_biosample_with_link(ctx):
    """Helper: atomically create a biosample and link it to ctx['study_idx'],
    tracking both for cleanup.

    Both inserts share one connection and one transaction so a failed link
    cannot leak an orphan biosample row for teardown to mop up. The
    tracking-dict appends run only after the transaction commits.
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
    ctx["created"]["biosample_to_study"].append((bs_idx, ctx["study_idx"]))
    return bs_idx


async def _create_local_field(ctx, suffix=""):
    """Helper: create a purely-local biosample_study_field, track for cleanup."""
    field_name = f"{unique_field_name()}_{suffix}"
    async with ctx["pool"].acquire() as conn:
        async with conn.transaction():
            idx, _, _ = await _get_or_create_local_study_field(
                conn,
                spec=BIOSAMPLE_METADATA_SPEC,
                study_idx=ctx["study_idx"],
                display_name=field_name,
                created_by_idx=ctx["principal_idx"],
                required=True,
            )
    ctx["created"]["biosample_study_field"].append(idx)
    return idx


async def _create_prep_sample_with_link(ctx):
    """Helper: create a biosample+link, then a sequenced prep_sample linked
    to ctx['study_idx'], tracking prep_sample / prep_sample_to_study for
    cleanup. Returns the prep_sample idx.

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
    ctx["created"]["prep_sample_to_study"].append((ps_idx, ctx["study_idx"]))
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
    Cleanup is tracked on the ctx fixture.
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
        st_idx = await _seed_study(ctx["pool"], ctx["principal_idx"], f"sec-{secrets.token_hex(4)}")
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
            ctx["created"]["biosample_to_study"].append((biosample_idx, st_idx))
    return new_study_idxs


async def _seed_global_field_for_spec(
    ctx, spec, data_type=FieldDataType.TEXT, terminology_idx=None
):
    """Seed one global field of the given data_type for spec.entity_kind
    and track the row for cleanup. Returns a GlobalFieldRow shape so the
    caller can drive metadata writes against it directly. terminology_idx
    must be supplied when data_type=TERMINOLOGY (the *_global_field
    CHECK enforces the iff coupling) and omitted otherwise.
    """
    # Token suffix defends against unique-name collisions across re-runs.
    suffix = secrets.token_hex(4)
    internal_name = f"gf_{suffix}"
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
    return GlobalFieldRow(
        idx=gf_idx, display_name=display_name, data_type=data_type, terminology_idx=terminology_idx
    )


def _track_to_study_link(ctx, spec, entity_idx, study_idx):
    """Track a (entity_idx, study_idx) link row in the cleanup dict under
    the key matching spec.entity_kind.
    """
    if spec.entity_kind is SampleEntityKind.BIOSAMPLE:
        ctx["created"]["biosample_to_study"].append((entity_idx, study_idx))
    else:
        ctx["created"]["prep_sample_to_study"].append((entity_idx, study_idx))


async def _create_linked_entity_for_spec(ctx, spec):
    """Create an entity linked to ctx['study_idx'] for spec.entity_kind,
    tracking all rows for cleanup. Returns the new entity_idx.

    Dispatches to the existing per-entity helpers so the prep_sample branch
    transparently seeds the underlying biosample and biosample_to_study link
    that the prep_sample_to_study trigger requires as substrate.
    """
    # Both branches return an entity_idx with its *_to_study link already
    # tracked; callers can write metadata against it without further setup.
    if spec.entity_kind is SampleEntityKind.BIOSAMPLE:
        return await _create_biosample_with_link(ctx)
    return await _create_prep_sample_with_link(ctx)
