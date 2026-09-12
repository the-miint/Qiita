"""Route tests for the study-local field surface that hold for every
sample-family entity alike.

Parameterized over SAMPLE_FIELD_SURFACES so one statement of a behaviour covers
both entities' routes; a case that needs one entity's own bindings belongs in
that entity's own module instead.
"""

import pytest
import pytest_asyncio

from qiita_control_plane.testing.unique_names import unique_field_name

from .conftest import (
    _STUDY_FIELD_ADMIN_FLOOR_AUTHZ,
    SAMPLE_FIELD_SURFACES,
    STUDY_FIELD_CREATE_AUTHZ_CASES,
    _grant_study_access,
    _seed_field_global,
    _seed_study,
    assert_study_field_authz,
    delete_idxs,
    etag_for_row,
    get_study_field,
    patch_study_field,
    post_study_field,
)

pytestmark = pytest.mark.db


@pytest_asyncio.fixture
async def ctx(role_keyed_clients):
    """Per-test fixture: route-keyed clients plus a `created` tracker.

    Tracks both entities' study-field buckets, since one test body runs against
    either surface. Nothing here seeds samples or metadata, so the teardown
    surface is the fields, the access grants, and the studies.
    """
    created: dict = {
        "biosample_study_field": [],
        "prep_sample_study_field": [],
        "biosample_global_field": [],
        "prep_sample_global_field": [],
        "study_access": [],
        "study": [],
    }
    yield {**role_keyed_clients, "created": created}

    pool = role_keyed_clients["pool"]
    # FK-reverse: fields and access grants both reference study.
    await delete_idxs(pool, "biosample_study_field", created["biosample_study_field"])
    await delete_idxs(pool, "prep_sample_study_field", created["prep_sample_study_field"])
    for study_idx, principal_idx in created["study_access"]:
        await pool.execute(
            "DELETE FROM qiita.study_access WHERE study_idx = $1 AND principal_idx = $2",
            study_idx,
            principal_idx,
        )
    await delete_idxs(pool, "study", created["study"])
    # Global fields outlive the study-local rows that link to them.
    await delete_idxs(pool, "biosample_global_field", created["biosample_global_field"])
    await delete_idxs(pool, "prep_sample_global_field", created["prep_sample_global_field"])


def _surface_id(surface):
    """Pytest id for the parametrize decorator: the entity's wire idx key."""
    return surface.idx_key.removesuffix("_study_field_idx")


async def _study_with_admin_grant(ctx, suffix):
    """Seed a study owned by the wet-lab session and grant the user session
    ADMIN on it, which is what the create-field route requires.
    """
    study_idx = await _seed_study(ctx, owner_idx=ctx["wet_session"]["principal_idx"], suffix=suffix)
    await _grant_study_access(
        ctx,
        study_idx=study_idx,
        principal_idx=ctx["user_session"]["principal_idx"],
        tier="admin",
        granted_by_idx=ctx["wet_session"]["principal_idx"],
    )
    return study_idx


@pytest.mark.parametrize("surface", SAMPLE_FIELD_SURFACES, ids=_surface_id)
async def test_create_study_field_accepts_unique_in_study(ctx, surface):
    """Tests the case where a purely-local field of an eligible type is created
    with study-local uniqueness: the flag comes back on the 201 body.
    """
    study_idx = await _study_with_admin_grant(ctx, "uis-ok")
    display_name = unique_field_name("Local")

    resp = await post_study_field(
        ctx,
        surface=surface,
        client=ctx["user"],
        study_idx=study_idx,
        display_name=display_name,
        data_type="text",
        unique_in_study=True,
    )

    assert resp.status_code == 201, resp.text
    assert resp.json()["unique_in_study"] is True


@pytest.mark.parametrize("surface", SAMPLE_FIELD_SURFACES, ids=_surface_id)
async def test_create_study_field_defaults_unique_in_study_false(ctx, surface):
    """Tests the case where a field is created without asking for uniqueness:
    the stored policy is off, and the response says so.
    """
    study_idx = await _study_with_admin_grant(ctx, "uis-def")

    resp = await post_study_field(
        ctx,
        surface=surface,
        client=ctx["user"],
        study_idx=study_idx,
        display_name=unique_field_name("Local"),
        data_type="text",
    )

    assert resp.status_code == 201, resp.text
    assert resp.json()["unique_in_study"] is False


@pytest.mark.parametrize("surface", SAMPLE_FIELD_SURFACES, ids=_surface_id)
@pytest.mark.parametrize("data_type", ["boolean", "terminology"])
async def test_create_study_field_rejects_unique_on_closed_value_set(ctx, surface, data_type):
    """Tests the case where a field over a closed value set asks for study-local
    uniqueness: the wire model refuses it before the database is reached.
    """
    study_idx = await _study_with_admin_grant(ctx, "uis-closed")

    resp = await post_study_field(
        ctx,
        surface=surface,
        client=ctx["user"],
        study_idx=study_idx,
        display_name=unique_field_name("Local"),
        data_type=data_type,
        unique_in_study=True,
    )

    assert resp.status_code == 422, resp.text


@pytest.mark.parametrize("surface", SAMPLE_FIELD_SURFACES, ids=_surface_id)
async def test_create_study_field_rejects_unique_on_linked_field(ctx, surface):
    """Tests the case where a globally-linked field asks for study-local
    uniqueness: the linked mode refuses it alongside the inherited columns.
    """
    study_idx = await _study_with_admin_grant(ctx, "uis-linked")

    resp = await post_study_field(
        ctx,
        surface=surface,
        client=ctx["user"],
        study_idx=study_idx,
        display_name=unique_field_name("Linked"),
        **{surface.global_fk_key: 1},
        unique_in_study=True,
    )

    assert resp.status_code == 422, resp.text


@pytest.mark.parametrize("surface", SAMPLE_FIELD_SURFACES, ids=_surface_id)
async def test_list_study_fields_reports_unique_in_study(ctx, surface):
    """Tests the case where a flagged field is read back through the list
    route: the stored policy travels on the read, not just the create.
    """
    study_idx = await _study_with_admin_grant(ctx, "uis-list")
    display_name = unique_field_name("Local")
    await post_study_field(
        ctx,
        surface=surface,
        client=ctx["user"],
        study_idx=study_idx,
        display_name=display_name,
        data_type="text",
        unique_in_study=True,
    )

    resp = await ctx["user"].get(surface.url_template.format(study_idx=study_idx))

    assert resp.status_code == 200, resp.text
    listed = {row["display_name"]: row["unique_in_study"] for row in resp.json()}
    assert listed[display_name] is True


# ===========================================================================
# PATCH /api/v1/study/{study_idx}/{entity}-field/{study_field_idx}
# ===========================================================================


async def _seed_editable_field(ctx, surface, *, study_idx, data_type="text", **body):
    """Create one purely-local field on `study_idx` and return its idx."""
    resp = await post_study_field(
        ctx,
        surface=surface,
        client=ctx["user"],
        study_idx=study_idx,
        display_name=unique_field_name("Editable"),
        data_type=data_type,
        **body,
    )
    assert resp.status_code == 201, resp.text
    return resp.json()[surface.idx_key]


async def _etag(ctx, surface, study_field_idx):
    """The ETag the edit route will compare an If-Match against."""
    table = surface.created_key
    return await etag_for_row(ctx["pool"], table=table, row_idx=study_field_idx)


@pytest.mark.parametrize("surface", SAMPLE_FIELD_SURFACES, ids=_surface_id)
@pytest.mark.parametrize("case", STUDY_FIELD_CREATE_AUTHZ_CASES)
async def test_patch_study_field_authz(
    ctx, surface, case, no_biosample_write_client, no_prep_sample_write_client
):
    """Tests the case where each row of the access matrix reaches the edit
    route: the gate matches the create route's, at the same ADMIN floor.
    """
    # Both no-scope clients are async fixtures, so neither can be materialized
    # on demand from inside the test; each is requested and the surface's own
    # binding picks which one this entity's route should be denied by.
    no_scope_client = {
        "no_biosample_write_client": no_biosample_write_client,
        "no_prep_sample_write_client": no_prep_sample_write_client,
    }[surface.no_write_scope_fixture]

    async def send(ctx_, surface_, client, study_idx):
        # The field is seeded by the wet client, which clears the gate on every
        # seeded study; the case's own client is the one being judged. The
        # nonexistent-study row has no field to seed, and the study gate
        # answers before the idx in the path is resolved.
        if case == "nonexistent_study":
            return await patch_study_field(
                ctx_,
                surface=surface_,
                client=client,
                study_idx=study_idx,
                study_field_idx=1,
                if_match='"unused"',
                description="edited",
            )
        resp = await post_study_field(
            ctx_,
            surface=surface_,
            client=ctx_["wet"],
            study_idx=study_idx,
            display_name=unique_field_name("Authz"),
            data_type="text",
        )
        assert resp.status_code == 201, resp.text
        field_idx = resp.json()[surface_.idx_key]
        return await patch_study_field(
            ctx_,
            surface=surface_,
            client=client,
            study_idx=study_idx,
            study_field_idx=field_idx,
            if_match=await _etag(ctx_, surface_, field_idx),
            description="edited",
        )

    await assert_study_field_authz(
        ctx,
        case=case,
        cases=_STUDY_FIELD_ADMIN_FLOOR_AUTHZ,
        surface=surface,
        no_scope_client=no_scope_client,
        send=send,
        success_status=200,
    )


@pytest.mark.parametrize("surface", SAMPLE_FIELD_SURFACES, ids=_surface_id)
async def test_patch_study_field_edits_and_returns_new_etag(ctx, surface):
    """Tests the case where a cosmetic edit succeeds: the body reflects the
    change and the ETag moves, so a caller's next If-Match is the new one.
    """
    study_idx = await _study_with_admin_grant(ctx, "pat-ok")
    field_idx = await _seed_editable_field(ctx, surface, study_idx=study_idx)
    before = await _etag(ctx, surface, field_idx)

    resp = await patch_study_field(
        ctx,
        surface=surface,
        client=ctx["user"],
        study_idx=study_idx,
        study_field_idx=field_idx,
        if_match=before,
        description="edited",
    )

    assert resp.status_code == 200, resp.text
    assert resp.json()["description"] == "edited"
    assert resp.headers["ETag"] != before
    assert resp.headers["ETag"] == await _etag(ctx, surface, field_idx)


@pytest.mark.parametrize("surface", SAMPLE_FIELD_SURFACES, ids=_surface_id)
async def test_patch_study_field_without_if_match_428(ctx, surface):
    """Tests the case where a caller omits If-Match: the edit is refused
    rather than applied without concurrency control.
    """
    study_idx = await _study_with_admin_grant(ctx, "pat-428")
    field_idx = await _seed_editable_field(ctx, surface, study_idx=study_idx)

    resp = await patch_study_field(
        ctx,
        surface=surface,
        client=ctx["user"],
        study_idx=study_idx,
        study_field_idx=field_idx,
        if_match=None,
        description="edited",
    )

    assert resp.status_code == 428, resp.text


@pytest.mark.parametrize("surface", SAMPLE_FIELD_SURFACES, ids=_surface_id)
async def test_patch_study_field_stale_if_match_412(ctx, surface):
    """Tests the case where a caller's ETag predates someone else's edit: the
    second write is refused instead of silently overwriting the first.
    """
    study_idx = await _study_with_admin_grant(ctx, "pat-412")
    field_idx = await _seed_editable_field(ctx, surface, study_idx=study_idx)
    stale = await _etag(ctx, surface, field_idx)
    first = await patch_study_field(
        ctx,
        surface=surface,
        client=ctx["user"],
        study_idx=study_idx,
        study_field_idx=field_idx,
        if_match=stale,
        description="first",
    )
    assert first.status_code == 200, first.text

    resp = await patch_study_field(
        ctx,
        surface=surface,
        client=ctx["user"],
        study_idx=study_idx,
        study_field_idx=field_idx,
        if_match=stale,
        description="second",
    )

    assert resp.status_code == 412, resp.text


@pytest.mark.parametrize("surface", SAMPLE_FIELD_SURFACES, ids=_surface_id)
async def test_patch_study_field_enables_unique_in_study(ctx, surface):
    """Tests the case where a study decides after the fact that a field
    identifies its samples: the policy is switched on and reads back.
    """
    study_idx = await _study_with_admin_grant(ctx, "pat-uniq")
    field_idx = await _seed_editable_field(ctx, surface, study_idx=study_idx)

    resp = await patch_study_field(
        ctx,
        surface=surface,
        client=ctx["user"],
        study_idx=study_idx,
        study_field_idx=field_idx,
        if_match=await _etag(ctx, surface, field_idx),
        unique_in_study=True,
    )

    assert resp.status_code == 200, resp.text
    assert resp.json()["unique_in_study"] is True


@pytest.mark.parametrize("surface", SAMPLE_FIELD_SURFACES, ids=_surface_id)
async def test_patch_study_field_unique_on_closed_value_set_422(ctx, surface):
    """Tests the case where uniqueness is asked for on a boolean field: the
    stored type decides, since the body carries no type of its own.
    """
    study_idx = await _study_with_admin_grant(ctx, "pat-bool")
    field_idx = await _seed_editable_field(ctx, surface, study_idx=study_idx, data_type="boolean")

    resp = await patch_study_field(
        ctx,
        surface=surface,
        client=ctx["user"],
        study_idx=study_idx,
        study_field_idx=field_idx,
        if_match=await _etag(ctx, surface, field_idx),
        unique_in_study=True,
    )

    assert resp.status_code == 422, resp.text
    assert "requires data_type to be one of" in resp.json()["detail"]


@pytest.mark.parametrize("surface", SAMPLE_FIELD_SURFACES, ids=_surface_id)
@pytest.mark.parametrize("attribute", ["unique_in_study", "required"])
async def test_patch_study_field_inherited_attribute_on_linked_422(ctx, surface, attribute):
    """Tests the case where an attribute the global field owns is set on a
    linked row: it is refused, since the linked row stores none of them.
    """
    study_idx = await _study_with_admin_grant(ctx, "pat-linked")
    global_idx = await _seed_field_global(ctx, surface=surface, label="pat")
    created = await post_study_field(
        ctx,
        surface=surface,
        client=ctx["user"],
        study_idx=study_idx,
        display_name=unique_field_name("Linked"),
        **{surface.global_fk_key: global_idx},
    )
    assert created.status_code == 201, created.text
    field_idx = created.json()[surface.idx_key]

    resp = await patch_study_field(
        ctx,
        surface=surface,
        client=ctx["user"],
        study_idx=study_idx,
        study_field_idx=field_idx,
        if_match=await _etag(ctx, surface, field_idx),
        **{attribute: True},
    )

    assert resp.status_code == 422, resp.text
    assert "linked to a global field" in resp.json()["detail"]


@pytest.mark.parametrize("surface", SAMPLE_FIELD_SURFACES, ids=_surface_id)
async def test_patch_study_field_display_name_collision_409(ctx, surface):
    """Tests the case where a rename takes a name another field on the study
    already holds: the study's field names stay distinct.
    """
    study_idx = await _study_with_admin_grant(ctx, "pat-dup")
    taken = unique_field_name("Taken")
    first = await post_study_field(
        ctx,
        surface=surface,
        client=ctx["user"],
        study_idx=study_idx,
        display_name=taken,
        data_type="text",
    )
    assert first.status_code == 201, first.text
    field_idx = await _seed_editable_field(ctx, surface, study_idx=study_idx)

    resp = await patch_study_field(
        ctx,
        surface=surface,
        client=ctx["user"],
        study_idx=study_idx,
        study_field_idx=field_idx,
        if_match=await _etag(ctx, surface, field_idx),
        display_name=taken,
    )

    assert resp.status_code == 409, resp.text
    assert "already exists on this study" in resp.json()["detail"]


@pytest.mark.parametrize("surface", SAMPLE_FIELD_SURFACES, ids=_surface_id)
async def test_patch_study_field_from_another_study_404(ctx, surface):
    """Tests the case where the field exists but belongs to a different study:
    the answer is 404, not 403, so it does not confirm the field elsewhere.
    """
    owning_study_idx = await _study_with_admin_grant(ctx, "pat-own")
    other_study_idx = await _study_with_admin_grant(ctx, "pat-other")
    field_idx = await _seed_editable_field(ctx, surface, study_idx=owning_study_idx)

    resp = await patch_study_field(
        ctx,
        surface=surface,
        client=ctx["user"],
        study_idx=other_study_idx,
        study_field_idx=field_idx,
        if_match=await _etag(ctx, surface, field_idx),
        description="edited",
    )

    assert resp.status_code == 404, resp.text


@pytest.mark.parametrize("surface", SAMPLE_FIELD_SURFACES, ids=_surface_id)
async def test_patch_study_field_absent_404(ctx, surface):
    """Tests the case where the field idx names no row at all."""
    study_idx = await _study_with_admin_grant(ctx, "pat-absent")

    resp = await patch_study_field(
        ctx,
        surface=surface,
        client=ctx["user"],
        study_idx=study_idx,
        study_field_idx=2_000_000_000,
        if_match='"unused"',
        description="edited",
    )

    assert resp.status_code == 404, resp.text


# ===========================================================================
# GET /api/v1/study/{study_idx}/{entity}-field/{study_field_idx}
# ===========================================================================


@pytest.mark.parametrize("surface", SAMPLE_FIELD_SURFACES, ids=_surface_id)
async def test_get_study_field_returns_the_row_and_an_etag(ctx, surface):
    """Tests the case where one field is read by idx: the body is the same
    shape the list route returns for that row, and the header carries the tag.
    """
    study_idx = await _study_with_admin_grant(ctx, "get-ok")
    field_idx = await _seed_editable_field(ctx, surface, study_idx=study_idx)

    resp = await get_study_field(
        ctx, surface=surface, client=ctx["user"], study_idx=study_idx, study_field_idx=field_idx
    )

    assert resp.status_code == 200, resp.text
    listed = await ctx["user"].get(surface.url_template.format(study_idx=study_idx))
    assert resp.json() in listed.json()
    assert resp.headers["ETag"] == await _etag(ctx, surface, field_idx)


@pytest.mark.parametrize("surface", SAMPLE_FIELD_SURFACES, ids=_surface_id)
async def test_get_study_field_etag_is_accepted_as_if_match(ctx, surface):
    """Tests the case where a caller edits a field using only what HTTP gave
    it: the read's ETag is the If-Match the edit wants, with no out-of-band
    knowledge of how the tag is built.
    """
    study_idx = await _study_with_admin_grant(ctx, "get-rt")
    field_idx = await _seed_editable_field(ctx, surface, study_idx=study_idx)

    read = await get_study_field(
        ctx, surface=surface, client=ctx["user"], study_idx=study_idx, study_field_idx=field_idx
    )
    assert read.status_code == 200, read.text

    resp = await patch_study_field(
        ctx,
        surface=surface,
        client=ctx["user"],
        study_idx=study_idx,
        study_field_idx=field_idx,
        if_match=read.headers["ETag"],
        description="edited",
    )

    assert resp.status_code == 200, resp.text


@pytest.mark.parametrize("surface", SAMPLE_FIELD_SURFACES, ids=_surface_id)
async def test_create_study_field_etag_is_accepted_as_if_match(ctx, surface):
    """Tests the case where a caller edits the field it just minted: the
    create response's ETag is enough, so minting and editing need no read
    between them.
    """
    study_idx = await _study_with_admin_grant(ctx, "post-rt")
    created = await post_study_field(
        ctx,
        surface=surface,
        client=ctx["user"],
        study_idx=study_idx,
        display_name=unique_field_name("Minted"),
        data_type="text",
    )
    assert created.status_code == 201, created.text

    resp = await patch_study_field(
        ctx,
        surface=surface,
        client=ctx["user"],
        study_idx=study_idx,
        study_field_idx=created.json()[surface.idx_key],
        if_match=created.headers["ETag"],
        description="edited",
    )

    assert resp.status_code == 200, resp.text


@pytest.mark.parametrize("surface", SAMPLE_FIELD_SURFACES, ids=_surface_id)
async def test_get_study_field_from_another_study_404(ctx, surface):
    """Tests the case where a field is addressed under a study that does not
    hold it: the read answers 404 rather than confirming it exists elsewhere.
    """
    holding_study_idx = await _study_with_admin_grant(ctx, "get-holder")
    other_study_idx = await _study_with_admin_grant(ctx, "get-other")
    field_idx = await _seed_editable_field(ctx, surface, study_idx=holding_study_idx)

    resp = await get_study_field(
        ctx,
        surface=surface,
        client=ctx["user"],
        study_idx=other_study_idx,
        study_field_idx=field_idx,
    )

    assert resp.status_code == 404, resp.text


@pytest.mark.parametrize("surface", SAMPLE_FIELD_SURFACES, ids=_surface_id)
async def test_get_study_field_absent_404(ctx, surface):
    """Tests the case where no field carries the path's idx: the read answers
    404 with the same wording an out-of-study field gets.
    """
    study_idx = await _study_with_admin_grant(ctx, "get-absent")

    resp = await get_study_field(
        ctx,
        surface=surface,
        client=ctx["user"],
        study_idx=study_idx,
        study_field_idx=2_000_000_000,
    )

    assert resp.status_code == 404, resp.text
