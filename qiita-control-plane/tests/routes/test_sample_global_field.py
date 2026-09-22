"""Integration tests for the global sample-field registry routes.

Both entities' registries are read by the same handler shape over the same
generic repository read, so every behaviour is asserted once here and
parametrised over the two entity surfaces rather than duplicated per entity.
"""

import secrets

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from qiita_common.auth_constants import SYSTEM_PRINCIPAL_IDX, Scope
from qiita_common.models import FieldDataType

from qiita_control_plane.main import app
from qiita_control_plane.testing.db_seeds import seed_terminology

from .conftest import (
    BIOSAMPLE_FIELD_SURFACE,
    PREP_SAMPLE_FIELD_SURFACE,
    SampleFieldSurface,
    _seed_field_global,
    delete_idxs,
    sibling_field_surface,
)

pytestmark = pytest.mark.db

SURFACES = [
    pytest.param(BIOSAMPLE_FIELD_SURFACE, id="biosample"),
    pytest.param(PREP_SAMPLE_FIELD_SURFACE, id="prep_sample"),
]

# The columns the route returns, so a whole-response comparison can be built
# straight from the table.
_REGISTRY_COLUMNS = (
    "idx",
    "internal_name",
    "display_name",
    "description",
    "data_type",
    "default_tier",
    "required",
    "terminology_idx",
    "created_by_idx",
    "created_at",
)


@pytest_asyncio.fixture
async def ctx(role_keyed_clients):
    """Per-test fixture: role-keyed clients plus a `created` tracker for the
    rows the seeds add. A global field's terminology FK is ON DELETE RESTRICT,
    so the fields drop before the terminology they point at."""
    created: dict = {
        "biosample_global_field": [],
        "prep_sample_global_field": [],
        "terminology": [],
    }
    yield {**role_keyed_clients, "created": created}
    pool = role_keyed_clients["pool"]
    await delete_idxs(pool, "biosample_global_field", created["biosample_global_field"])
    await delete_idxs(pool, "prep_sample_global_field", created["prep_sample_global_field"])
    await delete_idxs(pool, "terminology", created["terminology"])


async def _registry_rows(pool, *, surface: SampleFieldSurface, actual: list[dict]) -> list[dict]:
    """Read the registry straight from its table in the order the route sorts
    it, shaped as the route's response bodies.

    Each row's created_at is the server-generated timestamp, so it is copied
    from the paired body in `actual` rather than re-serialized here; a row-count
    mismatch fails on the strict zip before the comparison."""
    columns = ", ".join(_REGISTRY_COLUMNS)
    rows = await pool.fetch(
        f"SELECT {columns} FROM {surface.global_field_table} ORDER BY internal_name"
    )

    # Every column carries its own name on the wire but the row's own idx, which
    # takes the entity-qualified spelling; created_at comes from the body.
    expected = []
    for row, actual_row in zip(rows, actual, strict=True):
        body = {column: row[column] for column in _REGISTRY_COLUMNS}
        body[surface.global_idx_key] = body.pop("idx")
        body["created_at"] = actual_row["created_at"]
        expected.append(body)
    return expected


@pytest.mark.parametrize("surface", SURFACES)
async def test_list_global_fields_returns_every_row(ctx, surface):
    """Tests the case where a caller with the entity read scope lists the
    registry: every stored row comes back, ordered by internal_name, including
    one seeded after the migrations so the response is not purely migration
    content."""
    await _seed_field_global(ctx, surface=surface, label="reg")

    resp = await ctx["user"].get(surface.global_field_url)

    assert resp.status_code == 200, resp.text
    actual = resp.json()
    expected = await _registry_rows(ctx["pool"], surface=surface, actual=actual)
    assert actual == expected


@pytest.mark.parametrize("surface", SURFACES)
async def test_list_global_fields_carries_description_and_terminology(ctx, surface):
    """Tests the case where a registry row fills the two columns the schema
    leaves nullable — a description and a terminology binding: both round-trip,
    so a caller can tell a terminology-typed field from a free-text one and read
    which terminology it draws on."""
    suffix = secrets.token_hex(4)
    terminology_idx = await seed_terminology(ctx["pool"], name=f"gf-term-{suffix}")
    ctx["created"]["terminology"].append(terminology_idx)
    global_idx = await surface.seed_global_field(
        ctx["pool"],
        internal_name=f"term_{suffix}",
        display_name=f"Term {suffix}",
        data_type=FieldDataType.TERMINOLOGY,
        created_by_idx=SYSTEM_PRINCIPAL_IDX,
        terminology_idx=terminology_idx,
    )
    ctx["created"][surface.global_created_key].append(global_idx)
    # The seeder deliberately has no description parameter, so set it here.
    description = f"Seeded description {suffix}"
    await ctx["pool"].execute(
        f"UPDATE {surface.global_field_table} SET description = $1 WHERE idx = $2",
        description,
        global_idx,
    )

    resp = await ctx["user"].get(surface.global_field_url)

    assert resp.status_code == 200, resp.text
    actual = {row[surface.global_idx_key]: row for row in resp.json()}[global_idx]
    expected = {
        surface.global_idx_key: global_idx,
        "internal_name": f"term_{suffix}",
        "display_name": f"Term {suffix}",
        "description": description,
        "data_type": "terminology",
        "default_tier": "public",
        "required": False,
        "terminology_idx": terminology_idx,
        "created_by_idx": SYSTEM_PRINCIPAL_IDX,
        "created_at": actual["created_at"],
    }
    assert actual == expected


@pytest.mark.parametrize("surface", SURFACES)
async def test_list_global_fields_anonymous_401(ctx, surface):
    """Tests the case where an unauthenticated caller hits the route: the
    require_human gate rejects with 401 before any registry read."""
    app.state.pool = ctx["pool"]
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as anon:
        resp = await anon.get(surface.global_field_url)
    assert resp.status_code == 401


@pytest.mark.parametrize("surface", SURFACES)
async def test_list_global_fields_scope_is_the_whole_gate(ctx, surface, make_pat_client):
    """Tests the case where the caller holds exactly one entity read scope and
    no study grants: the entity's own scope admits it with no study check, the
    sibling entity's scope does not satisfy it, and neither scope is a 403 —
    which is what pins each registry to its own scope rather than to either."""
    own_scope_client = await make_pat_client(label="gf-own-scope", scopes=[surface.read_scope])
    other_scope_client = await make_pat_client(
        label="gf-other-scope", scopes=[sibling_field_surface(surface).read_scope]
    )
    no_scope_client = await make_pat_client(label="gf-no-scope", scopes=[Scope.SELF_PROFILE])

    own = await own_scope_client.get(surface.global_field_url)
    other = await other_scope_client.get(surface.global_field_url)
    none = await no_scope_client.get(surface.global_field_url)

    assert (own.status_code, other.status_code, none.status_code) == (200, 403, 403), (
        f"own={own.text} other={other.text} none={none.text}"
    )
