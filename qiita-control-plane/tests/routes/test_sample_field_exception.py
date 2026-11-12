"""DB-bound route tests: a study admin sets, reads, and clears the exception
that restricts one sample's value of one field, on a biosample and on a
sequenced sample (whose metadata is its prep_sample's).

An exception only ever narrows who may read the value, so one at or below the
field's tier is refused as restricting nothing. On a field linked to a global
field it is keyed on the global field and follows the value into every study;
a value another study wrote is that study's to restrict. An exception and a
unique_in_study field exclude each other. Replacing or clearing an exception
needs the If-Match its read returned.
"""

import asyncio
import secrets
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

import pytest
import pytest_asyncio
from qiita_common.api_paths import (
    URL_BIOSAMPLE_BY_STUDY_AND_IDX,
    URL_BIOSAMPLE_FIELD_EXCEPTION,
    URL_SEQUENCED_SAMPLE_BY_STUDY_AND_IDX,
    URL_SEQUENCED_SAMPLE_FIELD_EXCEPTION,
)
from qiita_common.auth_constants import MAX_REASON_LENGTH, Scope
from qiita_common.models import FieldDataType, Tier

from qiita_control_plane.repositories._sample_helpers import EntityMetadataSpec
from qiita_control_plane.repositories.biosample_metadata import BIOSAMPLE_METADATA_SPEC
from qiita_control_plane.repositories.prep_sample_metadata import PREP_SAMPLE_METADATA_SPEC
from qiita_control_plane.routes._helpers import METADATA_LOCK_TIMEOUT_MS, etag_for_updated_at
from qiita_control_plane.testing.db_seeds import (
    seed_biosample,
    seed_biosample_to_study_link,
    seed_biosample_with_sequenced_prep_sample,
    seed_field_exception,
    seed_global_field_for_spec,
    seed_globally_linked_study_field,
    seed_local_study_field,
    seed_metadata_text_value,
    seed_prep_sample_to_study_link,
    seed_sequenced_sample_subtype,
)
from qiita_control_plane.testing.db_teardown import delete_idxs, teardown_entity_graph
from qiita_control_plane.testing.unique_names import unique_field_name

from .conftest import (
    BIOSAMPLE_FIELD_SURFACE,
    PREP_SAMPLE_FIELD_SURFACE,
    STUDY_SCOPED_SAMPLE_ADMIN_AUTHZ_CASES,
    SampleFieldSurface,
    _finish,
    _grant_study_access,
    _seed_authz_case_study,
    _seed_study,
    _wait_until_blocked_by,
    assert_study_scoped_sample_admin_authz,
    get_study_field,
    patch_study_field,
    post_study_field,
)

pytestmark = pytest.mark.db


@pytest_asyncio.fixture
async def ctx(role_keyed_clients):
    created: dict[str, list[int]] = {
        "study": [],
        "biosample": [],
        "prep_sample": [],
        "biosample_global_field": [],
        "prep_sample_global_field": [],
        "sequenced_pool": [],
        "sequencing_run": [],
    }
    yield {**role_keyed_clients, "created": created}
    pool = role_keyed_clients["pool"]
    await teardown_entity_graph(
        pool,
        study_idxs=created["study"],
        biosample_idxs=created["biosample"],
        prep_sample_idxs=created["prep_sample"],
    )
    await delete_idxs(pool, "sequenced_pool", created["sequenced_pool"])
    await delete_idxs(pool, "sequencing_run", created["sequencing_run"])
    await delete_idxs(pool, "biosample_global_field", created["biosample_global_field"])
    await delete_idxs(pool, "prep_sample_global_field", created["prep_sample_global_field"])


def _wet_idx(ctx) -> int:
    return ctx["wet_session"]["principal_idx"]


def _user_idx(ctx) -> int:
    return ctx["user_session"]["principal_idx"]


@dataclass(frozen=True)
class _Entity:
    # The idx in the route path, and the row that holds the metadata: the same
    # biosample, or a sequenced_sample and its supertype prep_sample.
    path_idx: int
    metadata_idx: int
    biosample_idx: int


async def _seed_biosample_on(ctx, study_idx: int) -> _Entity:
    pool = ctx["pool"]
    biosample_idx = await seed_biosample(
        pool, owner_idx=_wet_idx(ctx), created_by_idx=_wet_idx(ctx)
    )
    ctx["created"]["biosample"].append(biosample_idx)
    await seed_biosample_to_study_link(
        pool, biosample_idx=biosample_idx, study_idx=study_idx, created_by_idx=_wet_idx(ctx)
    )
    return _Entity(path_idx=biosample_idx, metadata_idx=biosample_idx, biosample_idx=biosample_idx)


async def _seed_sequenced_sample_on(ctx, study_idx: int) -> _Entity:
    pool = ctx["pool"]
    biosample_idx, prep_sample_idx = await seed_biosample_with_sequenced_prep_sample(
        pool, owner_idx=_wet_idx(ctx)
    )
    ctx["created"]["biosample"].append(biosample_idx)
    ctx["created"]["prep_sample"].append(prep_sample_idx)
    entity = _Entity(path_idx=0, metadata_idx=prep_sample_idx, biosample_idx=biosample_idx)
    await _link_prep_sample(ctx, entity, study_idx)
    run_idx, pool_idx, sequenced_sample_idx = await seed_sequenced_sample_subtype(
        pool,
        prep_sample_idx=prep_sample_idx,
        owner_idx=_wet_idx(ctx),
        sequenced_pool_item_id=f"exc-{secrets.token_hex(3)}",
    )
    ctx["created"]["sequenced_pool"].append(pool_idx)
    ctx["created"]["sequencing_run"].append(run_idx)
    return _Entity(
        path_idx=sequenced_sample_idx, metadata_idx=prep_sample_idx, biosample_idx=biosample_idx
    )


async def _link_biosample(ctx, entity: _Entity, study_idx: int) -> None:
    await seed_biosample_to_study_link(
        ctx["pool"],
        biosample_idx=entity.biosample_idx,
        study_idx=study_idx,
        created_by_idx=_wet_idx(ctx),
    )


async def _link_prep_sample(ctx, entity: _Entity, study_idx: int) -> None:
    # A prep_sample joins a study only beside its biosample.
    await _link_biosample(ctx, entity, study_idx)
    await seed_prep_sample_to_study_link(
        ctx["pool"],
        prep_sample_idx=entity.metadata_idx,
        study_idx=study_idx,
        created_by_idx=_wet_idx(ctx),
    )


@dataclass(frozen=True)
class _Surface:
    spec: EntityMetadataSpec
    fields: SampleFieldSurface
    seed_entity: Callable[..., Awaitable[_Entity]]
    # Links an existing entity into one more study.
    link_entity: Callable[..., Awaitable[None]]
    exception_url_template: str
    read_url_template: str
    path_key: str
    write_scope: Scope
    read_scope: Scope
    no_write_scope_fixture: str
    no_read_scope_fixture: str

    def exception_url(self, study_idx: int, entity: _Entity | int, study_field_idx: int) -> str:
        path_idx = entity if isinstance(entity, int) else entity.path_idx
        return self.exception_url_template.format(
            study_idx=study_idx, **{self.path_key: path_idx}, study_field_idx=study_field_idx
        )

    def read_url(self, study_idx: int, entity: _Entity) -> str:
        return self.read_url_template.format(
            study_idx=study_idx, **{self.path_key: entity.path_idx}
        )


_SURFACES = {
    "biosample": _Surface(
        spec=BIOSAMPLE_METADATA_SPEC,
        fields=BIOSAMPLE_FIELD_SURFACE,
        seed_entity=_seed_biosample_on,
        link_entity=_link_biosample,
        exception_url_template=URL_BIOSAMPLE_FIELD_EXCEPTION,
        read_url_template=URL_BIOSAMPLE_BY_STUDY_AND_IDX,
        path_key="biosample_idx",
        write_scope=Scope.BIOSAMPLE_WRITE,
        read_scope=Scope.BIOSAMPLE_READ,
        no_write_scope_fixture="no_biosample_write_client",
        no_read_scope_fixture="no_biosample_read_client",
    ),
    "sequenced_sample": _Surface(
        spec=PREP_SAMPLE_METADATA_SPEC,
        fields=PREP_SAMPLE_FIELD_SURFACE,
        seed_entity=_seed_sequenced_sample_on,
        link_entity=_link_prep_sample,
        exception_url_template=URL_SEQUENCED_SAMPLE_FIELD_EXCEPTION,
        read_url_template=URL_SEQUENCED_SAMPLE_BY_STUDY_AND_IDX,
        path_key="sequenced_sample_idx",
        write_scope=Scope.PREP_SAMPLE_WRITE,
        read_scope=Scope.PREP_SAMPLE_READ,
        no_write_scope_fixture="no_prep_sample_write_client",
        no_read_scope_fixture="no_prep_sample_read_client",
    ),
}
_SURFACE_IDS = tuple(_SURFACES)


@dataclass(frozen=True)
class _GlobalValue:
    global_field_idx: int
    internal_name: str
    # The study field through which the value was written.
    study_field_idx: int


async def _seed_global_field(ctx, spec, *, default_tier: Tier) -> tuple[int, str]:
    suffix = secrets.token_hex(4)
    internal_name = f"exc_{suffix}"
    global_field_idx = await seed_global_field_for_spec(
        ctx["pool"],
        spec=spec,
        internal_name=internal_name,
        display_name=f"Exc {suffix}",
        data_type=FieldDataType.TEXT,
        created_by_idx=_wet_idx(ctx),
        default_tier=default_tier,
    )
    ctx["created"][f"{spec.entity_kind}_global_field"].append(global_field_idx)
    return global_field_idx, internal_name


async def _alias_global_field(ctx, spec, *, study_idx: int, global_field_idx: int) -> int:
    return await seed_globally_linked_study_field(
        ctx["pool"],
        spec=spec,
        study_idx=study_idx,
        global_field_idx=global_field_idx,
        display_name=unique_field_name("exc_alias"),
        created_by_idx=_wet_idx(ctx),
    )


async def _seed_global_value(
    ctx, spec, *, study_idx: int, entity: _Entity, default_tier: Tier = Tier.PUBLIC
) -> _GlobalValue:
    """A global field at default_tier, aliased on study_idx, holding a value
    on entity written through that alias."""
    global_field_idx, internal_name = await _seed_global_field(ctx, spec, default_tier=default_tier)
    alias_idx = await _alias_global_field(
        ctx, spec, study_idx=study_idx, global_field_idx=global_field_idx
    )
    await seed_metadata_text_value(
        ctx["pool"],
        spec=spec,
        entity_idx=entity.metadata_idx,
        study_field_idx=alias_idx,
        created_by_idx=_wet_idx(ctx),
    )
    return _GlobalValue(global_field_idx, internal_name, alias_idx)


async def _seed_local_value(
    ctx, spec, *, study_idx: int, entity: _Entity, tier_override: Tier | None
) -> tuple[int, str]:
    """A study-local field holding a value on entity; returns (idx, display_name)."""
    display_name = unique_field_name("exc_local")
    field_idx = await seed_local_study_field(
        ctx["pool"],
        spec=spec,
        study_idx=study_idx,
        display_name=display_name,
        created_by_idx=_wet_idx(ctx),
        tier_override=tier_override,
    )
    await seed_metadata_text_value(
        ctx["pool"],
        spec=spec,
        entity_idx=entity.metadata_idx,
        study_field_idx=field_idx,
        created_by_idx=_wet_idx(ctx),
    )
    return field_idx, display_name


async def _member_study(ctx) -> int:
    """A study at the default (member) tier, owned by the wet-lab admin, on
    which the regular user holds member."""
    return await _seed_authz_case_study(ctx, owner_key="wet", grant_tier=Tier.MEMBER, case="exc")


def _body(tier: Tier, reason: str = "accidental PII") -> dict:
    return {"tier_override": tier, "reason": reason}


async def _put(client, url: str, tier: Tier, *, if_match: str | None = None, reason=None):
    headers = {} if if_match is None else {"If-Match": if_match}
    body = _body(tier) if reason is None else _body(tier, reason)
    return await client.put(url, json=body, headers=headers)


async def _etag(client, url: str) -> str:
    resp = await client.get(url)
    assert resp.status_code == 200, resp.text
    return resp.headers["ETag"]


async def _visible_keys(client, url: str) -> set[str]:
    resp = await client.get(url)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    return set(body["global_metadata"]) | set(body["local_metadata"])


async def _exception_rows(ctx, spec, entity: _Entity) -> list:
    return await ctx["pool"].fetch(
        f"SELECT {spec.study_field_idx_column} AS study_field_idx, global_field_idx,"
        f"       tier_override, reason"
        f" FROM {spec.field_exception_table} WHERE {spec.entity_key_column} = $1",
        entity.metadata_idx,
    )


# ---------------------------------------------------------------------------
# Setting and clearing
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("surface", _SURFACE_IDS)
async def test_put_exception_hides_the_value_below_its_tier(ctx, surface):
    s = _SURFACES[surface]
    study_idx = await _member_study(ctx)
    entity = await s.seed_entity(ctx, study_idx)
    value = await _seed_global_value(ctx, s.spec, study_idx=study_idx, entity=entity)
    read_url = s.read_url(study_idx, entity)
    assert value.internal_name in await _visible_keys(ctx["user"], read_url)

    resp = await _put(
        ctx["wet"], s.exception_url(study_idx, entity, value.study_field_idx), Tier.ADMIN
    )

    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["tier_override"] == Tier.ADMIN
    assert body["reason"] == "accidental PII"
    assert body["created_by_idx"] == _wet_idx(ctx)
    assert body["updated_by_idx"] == _wet_idx(ctx)
    assert resp.headers["ETag"]
    assert value.internal_name not in await _visible_keys(ctx["user"], read_url)
    assert value.internal_name in await _visible_keys(ctx["wet"], read_url)


@pytest.mark.parametrize("surface", _SURFACE_IDS)
async def test_delete_exception_restores_visibility(ctx, surface):
    s = _SURFACES[surface]
    study_idx = await _member_study(ctx)
    entity = await s.seed_entity(ctx, study_idx)
    value = await _seed_global_value(ctx, s.spec, study_idx=study_idx, entity=entity)
    url = s.exception_url(study_idx, entity, value.study_field_idx)
    put = await _put(ctx["wet"], url, Tier.ADMIN)
    assert put.status_code == 201, put.text

    resp = await ctx["wet"].delete(url, headers={"If-Match": put.headers["ETag"]})

    assert resp.status_code == 204, resp.text
    assert await _exception_rows(ctx, s.spec, entity) == []
    assert value.internal_name in await _visible_keys(ctx["user"], s.read_url(study_idx, entity))
    assert (await ctx["wet"].get(url)).status_code == 404


@pytest.mark.parametrize("surface", _SURFACE_IDS)
async def test_put_on_a_linked_field_keys_on_the_global_field(ctx, surface):
    """The exception follows the value: a second study that links the same
    global field reads the value through it, and the exception hides it there
    too."""
    s = _SURFACES[surface]
    study_idx = await _member_study(ctx)
    entity = await s.seed_entity(ctx, study_idx)
    value = await _seed_global_value(ctx, s.spec, study_idx=study_idx, entity=entity)
    second_study_idx = await _member_study(ctx)
    await s.link_entity(ctx, entity, second_study_idx)
    await _alias_global_field(
        ctx, s.spec, study_idx=second_study_idx, global_field_idx=value.global_field_idx
    )

    resp = await _put(
        ctx["wet"], s.exception_url(study_idx, entity, value.study_field_idx), Tier.ADMIN
    )

    assert resp.status_code == 201, resp.text
    assert resp.json()["keyed_on"] == "global"
    (row,) = await _exception_rows(ctx, s.spec, entity)
    assert row["global_field_idx"] == value.global_field_idx
    assert row["study_field_idx"] is None
    assert value.internal_name not in await _visible_keys(
        ctx["user"], s.read_url(second_study_idx, entity)
    )


@pytest.mark.parametrize("surface", _SURFACE_IDS)
async def test_put_on_a_local_field_keys_on_the_study_field(ctx, surface):
    s = _SURFACES[surface]
    study_idx = await _member_study(ctx)
    entity = await s.seed_entity(ctx, study_idx)
    field_idx, display_name = await _seed_local_value(
        ctx, s.spec, study_idx=study_idx, entity=entity, tier_override=Tier.VIEWER
    )

    resp = await _put(ctx["wet"], s.exception_url(study_idx, entity, field_idx), Tier.ADMIN)

    assert resp.status_code == 201, resp.text
    assert resp.json()["keyed_on"] == "local"
    (row,) = await _exception_rows(ctx, s.spec, entity)
    assert row["study_field_idx"] == field_idx
    assert row["global_field_idx"] is None
    assert display_name not in await _visible_keys(ctx["user"], s.read_url(study_idx, entity))


@pytest.mark.parametrize("surface", _SURFACE_IDS)
async def test_exception_on_an_empty_linked_slot_is_409(ctx, surface):
    """An exception on a linked field restricts the value whichever study
    writes it. Set before any study has, it would bind the first one that
    does, so it waits for a value."""
    s = _SURFACES[surface]
    study_idx = await _member_study(ctx)
    entity = await s.seed_entity(ctx, study_idx)
    global_field_idx, _ = await _seed_global_field(ctx, s.spec, default_tier=Tier.PUBLIC)
    alias_idx = await _alias_global_field(
        ctx, s.spec, study_idx=study_idx, global_field_idx=global_field_idx
    )

    resp = await _put(ctx["wet"], s.exception_url(study_idx, entity, alias_idx), Tier.ADMIN)

    assert resp.status_code == 409, resp.text
    assert "holds no value" in resp.json()["detail"]
    assert await _exception_rows(ctx, s.spec, entity) == []


@pytest.mark.parametrize("surface", _SURFACE_IDS)
async def test_exception_on_an_empty_local_slot_is_set(ctx, surface):
    """A purely-local field's slot is this study's alone, so an exception may
    wait there for its value."""
    s = _SURFACES[surface]
    study_idx = await _member_study(ctx)
    entity = await s.seed_entity(ctx, study_idx)
    field_idx = await seed_local_study_field(
        ctx["pool"],
        spec=s.spec,
        study_idx=study_idx,
        display_name=unique_field_name("exc_empty"),
        created_by_idx=_wet_idx(ctx),
        tier_override=Tier.VIEWER,
    )

    resp = await _put(ctx["wet"], s.exception_url(study_idx, entity, field_idx), Tier.ADMIN)

    assert resp.status_code == 201, resp.text
    assert resp.json()["keyed_on"] == "local"


@pytest.mark.parametrize("surface", _SURFACE_IDS)
async def test_put_replaces_an_exception_under_its_if_match(ctx, surface):
    s = _SURFACES[surface]
    study_idx = await _member_study(ctx)
    entity = await s.seed_entity(ctx, study_idx)
    value = await _seed_global_value(ctx, s.spec, study_idx=study_idx, entity=entity)
    url = s.exception_url(study_idx, entity, value.study_field_idx)
    first = await _put(ctx["wet"], url, Tier.MEMBER)
    assert first.status_code == 201, first.text

    resp = await _put(
        ctx["wet"], url, Tier.ADMIN, if_match=first.headers["ETag"], reason="named a participant"
    )

    assert resp.status_code == 200, resp.text
    assert resp.json()["tier_override"] == Tier.ADMIN
    assert resp.json()["reason"] == "named a participant"
    assert resp.headers["ETag"] != first.headers["ETag"]
    (row,) = await _exception_rows(ctx, s.spec, entity)
    assert (row["tier_override"], row["reason"]) == ("admin", "named a participant")


@pytest.mark.parametrize("surface", _SURFACE_IDS)
async def test_put_over_an_existing_exception_without_if_match_is_428(ctx, surface):
    """A PUT with no If-Match only creates, so it cannot silently undo an
    exception that another admin set in the meantime."""
    s = _SURFACES[surface]
    study_idx = await _member_study(ctx)
    entity = await s.seed_entity(ctx, study_idx)
    value = await _seed_global_value(ctx, s.spec, study_idx=study_idx, entity=entity)
    url = s.exception_url(study_idx, entity, value.study_field_idx)
    assert (await _put(ctx["wet"], url, Tier.ADMIN)).status_code == 201

    resp = await _put(ctx["wet"], url, Tier.MEMBER)

    assert resp.status_code == 428, resp.text
    (row,) = await _exception_rows(ctx, s.spec, entity)
    assert row["tier_override"] == "admin"


@pytest.mark.parametrize("surface", _SURFACE_IDS)
@pytest.mark.parametrize("verb", ("put", "delete"))
async def test_stale_if_match_is_412(ctx, surface, verb):
    s = _SURFACES[surface]
    study_idx = await _member_study(ctx)
    entity = await s.seed_entity(ctx, study_idx)
    value = await _seed_global_value(ctx, s.spec, study_idx=study_idx, entity=entity)
    url = s.exception_url(study_idx, entity, value.study_field_idx)
    first = await _put(ctx["wet"], url, Tier.MEMBER)
    stale = first.headers["ETag"]
    second = await _put(ctx["wet"], url, Tier.ADMIN, if_match=stale)
    assert second.status_code == 200, second.text

    if verb == "put":
        resp = await _put(ctx["wet"], url, Tier.VIEWER, if_match=stale)
    else:
        resp = await ctx["wet"].delete(url, headers={"If-Match": stale})

    assert resp.status_code == 412, resp.text
    (row,) = await _exception_rows(ctx, s.spec, entity)
    assert row["tier_override"] == "admin"


@pytest.mark.parametrize("surface", _SURFACE_IDS)
async def test_delete_without_if_match_is_428(ctx, surface):
    s = _SURFACES[surface]
    study_idx = await _member_study(ctx)
    entity = await s.seed_entity(ctx, study_idx)
    value = await _seed_global_value(ctx, s.spec, study_idx=study_idx, entity=entity)
    url = s.exception_url(study_idx, entity, value.study_field_idx)
    assert (await _put(ctx["wet"], url, Tier.ADMIN)).status_code == 201

    resp = await ctx["wet"].delete(url)

    assert resp.status_code == 428, resp.text
    assert len(await _exception_rows(ctx, s.spec, entity)) == 1


@pytest.mark.parametrize("surface", _SURFACE_IDS)
@pytest.mark.parametrize(
    ("field_shape", "field_tier"),
    (("global", Tier.PUBLIC), ("local", Tier.MEMBER)),
)
async def test_exception_at_or_below_the_field_tier_is_422(ctx, surface, field_shape, field_tier):
    """An exception can only increase the tier that a value needs; one at the
    field's own tier, or below it, would restrict nothing, and one a tier above
    it is set."""
    s = _SURFACES[surface]
    study_idx = await _member_study(ctx)
    entity = await s.seed_entity(ctx, study_idx)
    if field_shape == "global":
        value = await _seed_global_value(
            ctx, s.spec, study_idx=study_idx, entity=entity, default_tier=field_tier
        )
        field_idx = value.study_field_idx
    else:
        field_idx, _ = await _seed_local_value(
            ctx, s.spec, study_idx=study_idx, entity=entity, tier_override=field_tier
        )
    url = s.exception_url(study_idx, entity, field_idx)
    below = Tier.PUBLIC if field_tier is Tier.PUBLIC else Tier.VIEWER
    above = Tier.VIEWER if field_tier is Tier.PUBLIC else Tier.ADMIN

    for tier in {field_tier, below}:
        resp = await _put(ctx["wet"], url, tier)

        assert resp.status_code == 422, resp.text
        assert "restricts nothing" in resp.json()["detail"]
    assert await _exception_rows(ctx, s.spec, entity) == []
    assert (await _put(ctx["wet"], url, above)).status_code == 201


@pytest.mark.parametrize("surface", _SURFACE_IDS)
async def test_reason_longer_than_its_cap_is_422(ctx, surface):
    s = _SURFACES[surface]
    study_idx = await _member_study(ctx)
    entity = await s.seed_entity(ctx, study_idx)
    value = await _seed_global_value(ctx, s.spec, study_idx=study_idx, entity=entity)
    url = s.exception_url(study_idx, entity, value.study_field_idx)

    too_long = await _put(ctx["wet"], url, Tier.ADMIN, reason="x" * (MAX_REASON_LENGTH + 1))
    at_cap = await _put(ctx["wet"], url, Tier.ADMIN, reason="x" * MAX_REASON_LENGTH)

    assert too_long.status_code == 422, too_long.text
    assert at_cap.status_code == 201, at_cap.text


@pytest.mark.parametrize("surface", _SURFACE_IDS)
async def test_value_another_study_wrote_needs_admin_there(ctx, surface):
    """An admin of this study may not read, set, or clear the exception on a
    value that another study wrote, without admin on that study. The refusal is
    the same on every verb and whether or not an exception is set, and names
    neither the other study, nor the value, nor the exception's reason."""
    s = _SURFACES[surface]
    study_idx = await _seed_authz_case_study(
        ctx, owner_key="wet", grant_tier=Tier.ADMIN, case="exc-here"
    )
    entity = await s.seed_entity(ctx, study_idx)
    other_study_idx = await _seed_study(ctx, owner_idx=_wet_idx(ctx), suffix="exc-other")
    await s.link_entity(ctx, entity, other_study_idx)
    value = await _seed_global_value(ctx, s.spec, study_idx=other_study_idx, entity=entity)
    stored = await ctx["pool"].fetchval(
        f"SELECT value_text FROM {s.spec.metadata_table}"
        f" WHERE {s.spec.entity_key_column} = $1 AND global_field_idx = $2",
        entity.metadata_idx,
        value.global_field_idx,
    )
    here_alias_idx = await _alias_global_field(
        ctx, s.spec, study_idx=study_idx, global_field_idx=value.global_field_idx
    )
    url = s.exception_url(study_idx, entity, here_alias_idx)

    async def _every_verb(etag: str) -> list:
        return [
            await ctx["user"].get(url),
            await _put(ctx["user"], url, Tier.ADMIN),
            await _put(ctx["user"], url, Tier.ADMIN, if_match=etag),
            await ctx["user"].delete(url, headers={"If-Match": etag}),
        ]

    unset = await _every_verb('"2026-01-01T00:00:00+00:00"')
    await seed_field_exception(
        ctx["pool"],
        spec=s.spec,
        entity_idx=entity.metadata_idx,
        global_field_idx=value.global_field_idx,
        tier_override=Tier.MEMBER,
        created_by_idx=_wet_idx(ctx),
        reason="names the participant",
    )
    etag = await _etag(ctx["wet"], url)
    set_ = await _every_verb(etag)

    for resp in (*unset, *set_):
        assert resp.status_code == 403, resp.text
        assert stored not in resp.text
        assert "names the participant" not in resp.text
        assert f"study {other_study_idx}" not in resp.text
        assert f"study_idx={other_study_idx}" not in resp.text
    assert len({resp.json()["detail"] for resp in (*unset, *set_)}) == 1

    await _grant_study_access(
        ctx,
        study_idx=other_study_idx,
        principal_idx=_user_idx(ctx),
        tier=Tier.ADMIN,
        granted_by_idx=_wet_idx(ctx),
    )
    resp = await _put(ctx["user"], url, Tier.ADMIN, if_match=etag)

    assert resp.status_code == 200, resp.text
    assert resp.json()["created_by_idx"] == _wet_idx(ctx)
    assert resp.json()["updated_by_idx"] == _user_idx(ctx)


# ---------------------------------------------------------------------------
# What the path names
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("surface", _SURFACE_IDS)
async def test_get_missing_exception_is_404(ctx, surface):
    s = _SURFACES[surface]
    study_idx = await _member_study(ctx)
    entity = await s.seed_entity(ctx, study_idx)
    value = await _seed_global_value(ctx, s.spec, study_idx=study_idx, entity=entity)

    resp = await ctx["wet"].get(s.exception_url(study_idx, entity, value.study_field_idx))

    assert resp.status_code == 404, resp.text


@pytest.mark.parametrize("surface", _SURFACE_IDS)
async def test_delete_missing_exception_is_404(ctx, surface):
    s = _SURFACES[surface]
    study_idx = await _member_study(ctx)
    entity = await s.seed_entity(ctx, study_idx)
    value = await _seed_global_value(ctx, s.spec, study_idx=study_idx, entity=entity)

    resp = await ctx["wet"].delete(
        s.exception_url(study_idx, entity, value.study_field_idx),
        headers={"If-Match": '"2026-01-01T00:00:00+00:00"'},
    )

    assert resp.status_code == 404, resp.text


@pytest.mark.parametrize("surface", _SURFACE_IDS)
async def test_field_of_another_study_is_404(ctx, surface):
    s = _SURFACES[surface]
    study_idx = await _member_study(ctx)
    entity = await s.seed_entity(ctx, study_idx)
    other_study_idx = await _member_study(ctx)
    await s.link_entity(ctx, entity, other_study_idx)
    other_field_idx, _ = await _seed_local_value(
        ctx, s.spec, study_idx=other_study_idx, entity=entity, tier_override=Tier.VIEWER
    )

    resp = await _put(ctx["wet"], s.exception_url(study_idx, entity, other_field_idx), Tier.ADMIN)

    assert resp.status_code == 404, resp.text
    assert await _exception_rows(ctx, s.spec, entity) == []


@pytest.mark.parametrize("surface", _SURFACE_IDS)
async def test_entity_not_linked_to_the_study_is_404(ctx, surface):
    s = _SURFACES[surface]
    study_idx = await _member_study(ctx)
    other_study_idx = await _member_study(ctx)
    entity = await s.seed_entity(ctx, other_study_idx)
    field_idx, _ = await _seed_local_value(
        ctx, s.spec, study_idx=other_study_idx, entity=entity, tier_override=Tier.VIEWER
    )
    here_field_idx = await seed_local_study_field(
        ctx["pool"],
        spec=s.spec,
        study_idx=study_idx,
        display_name=unique_field_name("exc_here"),
        created_by_idx=_wet_idx(ctx),
    )

    resp = await _put(ctx["wet"], s.exception_url(study_idx, entity, here_field_idx), Tier.ADMIN)

    assert resp.status_code == 404, resp.text
    assert await _exception_rows(ctx, s.spec, entity) == []


# ---------------------------------------------------------------------------
# unique_in_study
# ---------------------------------------------------------------------------


async def _seed_unique_local_value(ctx, s: _Surface, *, study_idx: int, entity: _Entity) -> int:
    declared = await post_study_field(
        surface=s.fields,
        client=ctx["wet"],
        study_idx=study_idx,
        display_name=unique_field_name("exc_unique"),
        data_type=FieldDataType.TEXT,
        unique_in_study=True,
    )
    assert declared.status_code == 201, declared.text
    field_idx = declared.json()[s.fields.idx_key]
    await seed_metadata_text_value(
        ctx["pool"],
        spec=s.spec,
        entity_idx=entity.metadata_idx,
        study_field_idx=field_idx,
        created_by_idx=_wet_idx(ctx),
    )
    return field_idx


@pytest.mark.parametrize("surface", _SURFACE_IDS)
async def test_exception_on_a_unique_in_study_field_is_409(ctx, surface):
    """A field exception cannot be set on a unique_in_study field."""
    s = _SURFACES[surface]
    study_idx = await _member_study(ctx)
    entity = await s.seed_entity(ctx, study_idx)
    field_idx = await _seed_unique_local_value(ctx, s, study_idx=study_idx, entity=entity)

    resp = await _put(ctx["wet"], s.exception_url(study_idx, entity, field_idx), Tier.ADMIN)

    assert resp.status_code == 409, resp.text
    assert "unique_in_study" in resp.json()["detail"]
    assert await _exception_rows(ctx, s.spec, entity) == []


@pytest.mark.parametrize("surface", _SURFACE_IDS)
async def test_declaring_unique_in_study_over_an_exception_is_409(ctx, surface):
    s = _SURFACES[surface]
    study_idx = await _member_study(ctx)
    entity = await s.seed_entity(ctx, study_idx)
    field_idx, _ = await _seed_local_value(
        ctx, s.spec, study_idx=study_idx, entity=entity, tier_override=Tier.VIEWER
    )
    put = await _put(ctx["wet"], s.exception_url(study_idx, entity, field_idx), Tier.ADMIN)
    assert put.status_code == 201, put.text
    field = await get_study_field(
        surface=s.fields, client=ctx["wet"], study_idx=study_idx, study_field_idx=field_idx
    )

    resp = await patch_study_field(
        surface=s.fields,
        client=ctx["wet"],
        study_idx=study_idx,
        study_field_idx=field_idx,
        if_match=field.headers["ETag"],
        unique_in_study=True,
    )

    assert resp.status_code == 409, resp.text
    assert "exception" in resp.json()["detail"]
    after = await get_study_field(
        surface=s.fields, client=ctx["wet"], study_idx=study_idx, study_field_idx=field_idx
    )
    assert after.json()["unique_in_study"] is False


@pytest.mark.parametrize("surface", _SURFACE_IDS)
async def test_an_exception_on_another_field_does_not_block_unique_in_study(ctx, surface):
    s = _SURFACES[surface]
    study_idx = await _member_study(ctx)
    entity = await s.seed_entity(ctx, study_idx)
    excepted_field_idx, _ = await _seed_local_value(
        ctx, s.spec, study_idx=study_idx, entity=entity, tier_override=Tier.VIEWER
    )
    put = await _put(ctx["wet"], s.exception_url(study_idx, entity, excepted_field_idx), Tier.ADMIN)
    assert put.status_code == 201, put.text
    field_idx = await seed_local_study_field(
        ctx["pool"],
        spec=s.spec,
        study_idx=study_idx,
        display_name=unique_field_name("exc_unique_beside"),
        created_by_idx=_wet_idx(ctx),
    )
    field = await get_study_field(
        surface=s.fields, client=ctx["wet"], study_idx=study_idx, study_field_idx=field_idx
    )

    resp = await patch_study_field(
        surface=s.fields,
        client=ctx["wet"],
        study_idx=study_idx,
        study_field_idx=field_idx,
        if_match=field.headers["ETag"],
        unique_in_study=True,
    )

    assert resp.status_code == 200, resp.text
    assert resp.json()["unique_in_study"] is True


@pytest.mark.parametrize("surface", _SURFACE_IDS)
async def test_put_racing_a_unique_declaration_is_409(ctx, surface):
    """A PUT that read the field before an uncommitted unique_in_study
    declaration waits for it in the database, and is then refused with the same
    409 as one that read it after."""
    s = _SURFACES[surface]
    pool = ctx["pool"]
    study_idx = await _member_study(ctx)
    entity = await s.seed_entity(ctx, study_idx)
    field_idx, _ = await _seed_local_value(
        ctx, s.spec, study_idx=study_idx, entity=entity, tier_override=Tier.VIEWER
    )
    url = s.exception_url(study_idx, entity, field_idx)

    async with pool.acquire() as declarer:
        tx = declarer.transaction()
        await tx.start()
        try:
            # The unique_in_study propagation refuses to run without a bound.
            await declarer.execute(f"SET LOCAL lock_timeout = '{METADATA_LOCK_TIMEOUT_MS}ms'")
            await declarer.execute(
                f"UPDATE {s.spec.study_field_table} SET unique_in_study = true WHERE idx = $1",
                field_idx,
            )
            task = asyncio.create_task(_put(ctx["wet"], url, Tier.ADMIN))
            await _wait_until_blocked_by(pool, declarer, task)
        finally:
            await tx.commit()
    resp = await _finish(task)

    assert resp.status_code == 409, resp.text
    assert "unique_in_study" in resp.json()["detail"]
    assert await _exception_rows(ctx, s.spec, entity) == []


@pytest.mark.parametrize("surface", _SURFACE_IDS)
async def test_unique_declaration_racing_an_exception_is_409(ctx, surface):
    """A field PATCH declaring unique_in_study while an exception on the field
    is uncommitted waits for it at the field's row lock, and is then refused
    with the same 409 as one that read the field after it committed."""
    s = _SURFACES[surface]
    pool = ctx["pool"]
    study_idx = await _member_study(ctx)
    entity = await s.seed_entity(ctx, study_idx)
    field_idx, _ = await _seed_local_value(
        ctx, s.spec, study_idx=study_idx, entity=entity, tier_override=Tier.VIEWER
    )
    field = await get_study_field(
        surface=s.fields, client=ctx["wet"], study_idx=study_idx, study_field_idx=field_idx
    )

    async with pool.acquire() as setter:
        tx = setter.transaction()
        await tx.start()
        try:
            await seed_field_exception(
                setter,
                spec=s.spec,
                entity_idx=entity.metadata_idx,
                study_field_idx=field_idx,
                tier_override=Tier.ADMIN,
                created_by_idx=_wet_idx(ctx),
            )
            task = asyncio.create_task(
                patch_study_field(
                    surface=s.fields,
                    client=ctx["wet"],
                    study_idx=study_idx,
                    study_field_idx=field_idx,
                    if_match=field.headers["ETag"],
                    unique_in_study=True,
                )
            )
            await _wait_until_blocked_by(pool, setter, task)
        finally:
            await tx.commit()
    resp = await _finish(task)

    assert resp.status_code == 409, resp.text
    assert "field exceptions are set" in resp.json()["detail"]
    after = await get_study_field(
        surface=s.fields, client=ctx["wet"], study_idx=study_idx, study_field_idx=field_idx
    )
    assert after.json()["unique_in_study"] is False


# ---------------------------------------------------------------------------
# Access
# ---------------------------------------------------------------------------


def _authz_driver(ctx, s: _Surface, *, with_exception: bool):
    """(seed_sample, url, current_etag) for the shared access matrix: the
    sample carries a public global value, and an exception on it when
    with_exception. The field that the path names is recorded as the seed
    runs."""
    seeded: dict[str, int] = {}

    async def seed_sample(ctx, study_idx: int) -> int:
        entity = await s.seed_entity(ctx, study_idx)
        value = await _seed_global_value(ctx, s.spec, study_idx=study_idx, entity=entity)
        if with_exception:
            await seed_field_exception(
                ctx["pool"],
                spec=s.spec,
                entity_idx=entity.metadata_idx,
                global_field_idx=value.global_field_idx,
                tier_override=Tier.ADMIN,
                created_by_idx=_wet_idx(ctx),
            )
        seeded["field_idx"] = value.study_field_idx
        seeded["metadata_idx"] = entity.metadata_idx
        return entity.path_idx

    def url(study_idx: int, sample_idx: int) -> str:
        # A case with no study seeds nothing; the study gate answers first.
        return s.exception_url(study_idx, sample_idx, seeded.get("field_idx", 1))

    async def current_etag() -> str:
        updated_at = await ctx["pool"].fetchval(
            f"SELECT updated_at FROM {s.spec.field_exception_table}"
            f" WHERE {s.spec.entity_key_column} = $1",
            seeded.get("metadata_idx", 0),
        )
        return '"none"' if updated_at is None else etag_for_updated_at(updated_at)

    return seed_sample, url, current_etag


@pytest.mark.parametrize("surface", _SURFACE_IDS)
@pytest.mark.parametrize("verb", ("put", "delete"))
@pytest.mark.parametrize("case", STUDY_SCOPED_SAMPLE_ADMIN_AUTHZ_CASES)
async def test_field_exception_write_authz(
    ctx, no_biosample_write_client, no_prep_sample_write_client, surface, verb, case
):
    s = _SURFACES[surface]
    # Both entities' fixtures are requested and one selected by name: an async
    # fixture cannot be materialized on demand from inside a test body.
    no_scope_clients = {
        "no_biosample_write_client": no_biosample_write_client,
        "no_prep_sample_write_client": no_prep_sample_write_client,
    }
    seed_sample, url, current_etag = _authz_driver(ctx, s, with_exception=verb == "delete")

    async def send(client, study_idx, sample_idx):
        if verb == "put":
            return await _put(client, url(study_idx, sample_idx), Tier.ADMIN)
        return await client.delete(
            url(study_idx, sample_idx), headers={"If-Match": await current_etag()}
        )

    await assert_study_scoped_sample_admin_authz(
        ctx,
        case=case,
        no_scope_client=no_scope_clients[s.no_write_scope_fixture],
        seed_sample=seed_sample,
        send=send,
        success_status=201 if verb == "put" else 204,
        required_scope=s.write_scope,
    )


@pytest.mark.parametrize("surface", _SURFACE_IDS)
@pytest.mark.parametrize("case", STUDY_SCOPED_SAMPLE_ADMIN_AUTHZ_CASES)
async def test_field_exception_read_authz(
    ctx, no_biosample_read_client, no_prep_sample_read_client, surface, case
):
    s = _SURFACES[surface]
    no_scope_clients = {
        "no_biosample_read_client": no_biosample_read_client,
        "no_prep_sample_read_client": no_prep_sample_read_client,
    }
    seed_sample, url, _ = _authz_driver(ctx, s, with_exception=True)

    async def send(client, study_idx, sample_idx):
        return await client.get(url(study_idx, sample_idx))

    await assert_study_scoped_sample_admin_authz(
        ctx,
        case=case,
        no_scope_client=no_scope_clients[s.no_read_scope_fixture],
        seed_sample=seed_sample,
        send=send,
        success_status=200,
        required_scope=s.read_scope,
    )
