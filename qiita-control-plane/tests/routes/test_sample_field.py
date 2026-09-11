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
    SAMPLE_FIELD_SURFACES,
    _grant_study_access,
    _seed_study,
    delete_idxs,
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
