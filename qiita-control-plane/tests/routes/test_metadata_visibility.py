"""DB-bound route tests: each metadata read returns exactly the values the
caller's tier reaches.

One corpus per test: a study at the default `member` tier, and on it a sample
carrying one value per tier a value can require -- a global field at each tier
that matters, study-local fields inheriting the study default or overriding it,
and a public global value narrowed by an admin-tier exception. The biosample
corpus is imported through the route, so its owner-biosample-id field carries
the tier the import pins on it. Each test asserts the exact set of keys a
caller sees; the rule itself is pinned at the SQL level, over every
combination of field tier, exception, and caller tier.
"""

import secrets
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

import pytest
import pytest_asyncio
from qiita_common.api_paths import (
    URL_BIOSAMPLE_BY_IDX,
    URL_BIOSAMPLE_BY_STUDY,
    URL_BIOSAMPLE_BY_STUDY_AND_IDX,
    URL_BIOSAMPLE_BY_STUDY_UNIQUE_FIELD,
    URL_BIOSAMPLE_STUDY_FIELD_BY_STUDY,
    URL_SEQUENCED_SAMPLE_BY_IDX,
    URL_SEQUENCED_SAMPLE_BY_STUDY_AND_IDX,
)
from qiita_common.models import (
    BIOSAMPLE_DISPLAY_HOST_TAXON_ID,
    BIOSAMPLE_FIELD_HOST_TAXON_ID,
    FieldDataType,
    Tier,
)

from qiita_control_plane.repositories._sample_helpers import EntityMetadataSpec
from qiita_control_plane.repositories.biosample_metadata import BIOSAMPLE_METADATA_SPEC
from qiita_control_plane.repositories.prep_sample_metadata import PREP_SAMPLE_METADATA_SPEC
from qiita_control_plane.routes._helpers import detail_for_unique_field_miss
from qiita_control_plane.testing.db_seeds import (
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
from qiita_control_plane.testing.tiers import TIER_RANK
from qiita_control_plane.testing.unique_names import unique_field_name

from .conftest import _grant_study_access, _seed_authz_case_study, _seed_study

pytestmark = pytest.mark.db

# case -> (study owner, tier granted to the regular user, tier the caller reads
# at). The owner and grant are as _seed_authz_case_study takes them. Every case
# but the bypass calls as the regular user; what differs is how the user
# relates to the study.
_CASES: dict[str, tuple[str, Tier | None, Tier]] = {
    "viewer": ("wet", Tier.VIEWER, Tier.VIEWER),
    "member": ("wet", Tier.MEMBER, Tier.MEMBER),
    "admin": ("wet", Tier.ADMIN, Tier.ADMIN),
    "owner": ("user", None, Tier.ADMIN),
    "wet_lab_admin": ("wet", None, Tier.ADMIN),
}


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


@dataclass(frozen=True)
class _Corpus:
    study_idx: int
    # The idx the route addresses, and the entity the metadata hangs off: the
    # same biosample, or a sequenced_sample and its supertype prep_sample.
    entity_idx: int
    metadata_entity_idx: int
    # key -> the tier a caller needs to see it. Global values key on
    # internal_name, local ones on display_name, as the response does.
    global_keys: dict[str, Tier]
    local_keys: dict[str, Tier]

    def visible(self, keys: dict[str, Tier], read_tier: Tier) -> set[str]:
        return {k for k, needed in keys.items() if TIER_RANK[read_tier] >= TIER_RANK[needed]}


async def _seed_study_for_case(ctx, case: str) -> int:
    """Seed the study a case reads, owned and granted as the case says."""
    owner_key, grant_tier, _ = _CASES[case]
    return await _seed_authz_case_study(
        ctx, owner_key=owner_key, grant_tier=grant_tier, case=f"vis-{case}"
    )


def _client_for_case(ctx, case: str):
    return ctx["wet"] if case == "wet_lab_admin" else ctx["user"]


async def _seed_global_value(
    ctx, spec: EntityMetadataSpec, *, study_idx, entity_idx, default_tier, exception_tier=None
) -> str:
    """Seed a global field at default_tier, write a value through study_idx's
    alias of it, and return its internal_name."""
    pool = ctx["pool"]
    wet_idx = ctx["wet_session"]["principal_idx"]
    suffix = secrets.token_hex(4)
    internal_name = f"vis_{suffix}"
    global_field_idx = await seed_global_field_for_spec(
        pool,
        spec=spec,
        internal_name=internal_name,
        display_name=f"Vis {suffix}",
        data_type=FieldDataType.TEXT,
        created_by_idx=wet_idx,
        default_tier=default_tier,
    )
    ctx["created"][f"{spec.entity_kind}_global_field"].append(global_field_idx)
    alias_idx = await seed_globally_linked_study_field(
        pool,
        spec=spec,
        study_idx=study_idx,
        global_field_idx=global_field_idx,
        display_name=unique_field_name("vis_alias"),
        created_by_idx=wet_idx,
    )
    await seed_metadata_text_value(
        pool, spec=spec, entity_idx=entity_idx, study_field_idx=alias_idx, created_by_idx=wet_idx
    )
    if exception_tier is not None:
        await seed_field_exception(
            pool,
            spec=spec,
            entity_idx=entity_idx,
            global_field_idx=global_field_idx,
            tier_override=exception_tier,
            created_by_idx=wet_idx,
        )
    return internal_name


async def _seed_local_value(
    ctx, spec: EntityMetadataSpec, *, study_idx, entity_idx, tier_override
) -> str:
    """Seed a study-local field (tier_override None inherits the study's
    default), write a value through it, and return its display_name."""
    wet_idx = ctx["wet_session"]["principal_idx"]
    display_name = unique_field_name("vis_local")
    field_idx = await seed_local_study_field(
        ctx["pool"],
        spec=spec,
        study_idx=study_idx,
        display_name=display_name,
        created_by_idx=wet_idx,
        tier_override=tier_override,
    )
    await seed_metadata_text_value(
        ctx["pool"],
        spec=spec,
        entity_idx=entity_idx,
        study_field_idx=field_idx,
        created_by_idx=wet_idx,
    )
    return display_name


async def _seed_tiered_values(ctx, spec, *, study_idx, entity_idx):
    """The corpus's tiered values; returns (global_keys, local_keys)."""
    global_keys = {
        await _seed_global_value(
            ctx, spec, study_idx=study_idx, entity_idx=entity_idx, default_tier=Tier.PUBLIC
        ): Tier.PUBLIC,
        await _seed_global_value(
            ctx, spec, study_idx=study_idx, entity_idx=entity_idx, default_tier=Tier.MEMBER
        ): Tier.MEMBER,
        await _seed_global_value(
            ctx,
            spec,
            study_idx=study_idx,
            entity_idx=entity_idx,
            default_tier=Tier.PUBLIC,
            exception_tier=Tier.ADMIN,
        ): Tier.ADMIN,
    }
    local_keys = {
        # The study's default (member) applies to a field with no override.
        await _seed_local_value(
            ctx, spec, study_idx=study_idx, entity_idx=entity_idx, tier_override=None
        ): Tier.MEMBER,
        await _seed_local_value(
            ctx, spec, study_idx=study_idx, entity_idx=entity_idx, tier_override=Tier.VIEWER
        ): Tier.VIEWER,
        await _seed_local_value(
            ctx, spec, study_idx=study_idx, entity_idx=entity_idx, tier_override=Tier.ADMIN
        ): Tier.ADMIN,
    }
    return global_keys, local_keys


async def _import_biosample(
    ctx, *, study_idx: int, owner_idx: int, owner_id_field: str, owner_id_value: str
):
    # The import requires host taxon id; a missing-value marker needs no
    # seeded term. Its global field is public, so every caller sees it.
    resp = await ctx["wet"].post(
        URL_BIOSAMPLE_BY_STUDY.format(study_idx=study_idx),
        json={
            "owner_idx": owner_idx,
            "owner_biosample_id_field_name": owner_id_field,
            "owner_biosample_id_value": owner_id_value,
            "metadata": {BIOSAMPLE_DISPLAY_HOST_TAXON_ID: "not applicable"},
        },
    )
    assert resp.status_code == 201, resp.text
    biosample_idx = resp.json()["biosample_idx"]
    ctx["created"]["biosample"].append(biosample_idx)
    return biosample_idx


@dataclass(frozen=True)
class _BiosampleCorpus(_Corpus):
    owner_id_field: str
    owner_id_value: str


async def _seed_biosample_corpus(
    ctx, study_idx: int, *, owner_idx: int | None = None
) -> _BiosampleCorpus:
    """The biosample corpus; the biosample is the wet_lab_admin's unless
    owner_idx names another owner."""
    owner_id_field = unique_field_name("vis_owner_id")
    owner_id_value = f"VIS-{secrets.token_hex(3)}"
    biosample_idx = await _import_biosample(
        ctx,
        study_idx=study_idx,
        owner_idx=ctx["wet_session"]["principal_idx"] if owner_idx is None else owner_idx,
        owner_id_field=owner_id_field,
        owner_id_value=owner_id_value,
    )
    global_keys, local_keys = await _seed_tiered_values(
        ctx, BIOSAMPLE_METADATA_SPEC, study_idx=study_idx, entity_idx=biosample_idx
    )
    return _BiosampleCorpus(
        study_idx=study_idx,
        entity_idx=biosample_idx,
        metadata_entity_idx=biosample_idx,
        global_keys={BIOSAMPLE_FIELD_HOST_TAXON_ID: Tier.PUBLIC, **global_keys},
        # The import pins the owner-biosample-id field at member.
        local_keys={owner_id_field: Tier.MEMBER, **local_keys},
        owner_id_field=owner_id_field,
        owner_id_value=owner_id_value,
    )


async def _seed_sequenced_sample_corpus(ctx, study_idx: int) -> _Corpus:
    pool = ctx["pool"]
    wet_idx = ctx["wet_session"]["principal_idx"]
    biosample_idx, prep_sample_idx = await seed_biosample_with_sequenced_prep_sample(
        pool, owner_idx=wet_idx
    )
    ctx["created"]["biosample"].append(biosample_idx)
    ctx["created"]["prep_sample"].append(prep_sample_idx)
    await seed_biosample_to_study_link(
        pool, biosample_idx=biosample_idx, study_idx=study_idx, created_by_idx=wet_idx
    )
    await seed_prep_sample_to_study_link(
        pool, prep_sample_idx=prep_sample_idx, study_idx=study_idx, created_by_idx=wet_idx
    )
    run_idx, pool_idx, sequenced_sample_idx = await seed_sequenced_sample_subtype(
        pool,
        prep_sample_idx=prep_sample_idx,
        owner_idx=wet_idx,
        sequenced_pool_item_id=f"vis-{secrets.token_hex(3)}",
    )
    ctx["created"]["sequenced_pool"].append(pool_idx)
    ctx["created"]["sequencing_run"].append(run_idx)
    global_keys, local_keys = await _seed_tiered_values(
        ctx, PREP_SAMPLE_METADATA_SPEC, study_idx=study_idx, entity_idx=prep_sample_idx
    )
    return _Corpus(
        study_idx=study_idx,
        entity_idx=sequenced_sample_idx,
        metadata_entity_idx=prep_sample_idx,
        global_keys=global_keys,
        local_keys=local_keys,
    )


@pytest.mark.parametrize("case", tuple(_CASES))
async def test_study_scoped_biosample_read_shows_only_visible_values(ctx, case):
    study_idx = await _seed_study_for_case(ctx, case)
    corpus = await _seed_biosample_corpus(ctx, study_idx)

    resp = await _client_for_case(ctx, case).get(
        URL_BIOSAMPLE_BY_STUDY_AND_IDX.format(study_idx=study_idx, biosample_idx=corpus.entity_idx)
    )

    assert resp.status_code == 200, resp.text
    body = resp.json()
    read_tier = _CASES[case][2]
    assert set(body["global_metadata"]) == corpus.visible(corpus.global_keys, read_tier)
    assert set(body["local_metadata"]) == corpus.visible(corpus.local_keys, read_tier)


# The study's owner is absent: this read admits by a grant on a linked study or
# by owning the biosample, and the corpus's biosample is the wet_lab_admin's.
@pytest.mark.parametrize("case", ("viewer", "member", "admin", "wet_lab_admin"))
async def test_biosample_read_shows_only_visible_global_values(ctx, case):
    study_idx = await _seed_study_for_case(ctx, case)
    corpus = await _seed_biosample_corpus(ctx, study_idx)

    resp = await _client_for_case(ctx, case).get(
        URL_BIOSAMPLE_BY_IDX.format(biosample_idx=corpus.entity_idx)
    )

    assert resp.status_code == 200, resp.text
    assert set(resp.json()["global_metadata"]) == corpus.visible(
        corpus.global_keys, _CASES[case][2]
    )


async def test_biosample_owner_without_a_grant_reads_global_values_at_public(ctx):
    """Owning a biosample admits a caller to its read but confers no tier: with
    no grant on the study that wrote the values, the owner sees only the public
    ones."""
    # A study the wet_lab_admin owns, where the regular user holds no grant.
    study_idx = await _seed_authz_case_study(
        ctx, owner_key="wet", grant_tier=None, case="vis-biosample-owner"
    )
    corpus = await _seed_biosample_corpus(
        ctx, study_idx, owner_idx=ctx["user_session"]["principal_idx"]
    )

    resp = await ctx["user"].get(URL_BIOSAMPLE_BY_IDX.format(biosample_idx=corpus.entity_idx))

    assert resp.status_code == 200, resp.text
    assert set(resp.json()["global_metadata"]) == corpus.visible(corpus.global_keys, Tier.PUBLIC)


async def test_lookup_by_unique_field_hidden_value_is_a_miss(ctx):
    """A viewer cannot see the owner-biosample-id, so a lookup by it answers
    exactly as a lookup by a value no biosample carries does; a member can."""
    study_idx = await _seed_study_for_case(ctx, "viewer")
    corpus = await _seed_biosample_corpus(ctx, study_idx)
    url = URL_BIOSAMPLE_BY_STUDY_UNIQUE_FIELD.format(study_idx=study_idx)
    body = {
        "unique_field_display_name": corpus.owner_id_field,
        "unique_field_value": corpus.owner_id_value,
    }

    hidden = await ctx["user"].post(url, json=body)

    assert hidden.status_code == 404, hidden.text
    assert hidden.json()["detail"] == detail_for_unique_field_miss(
        noun="biosample",
        study_idx=study_idx,
        display_name=corpus.owner_id_field,
        value=corpus.owner_id_value,
    )

    await ctx["pool"].execute(
        "UPDATE qiita.study_access SET access_tier = $3::qiita.tier"
        " WHERE study_idx = $1 AND principal_idx = $2",
        study_idx,
        ctx["user_session"]["principal_idx"],
        Tier.MEMBER,
    )
    shown = await ctx["user"].post(url, json=body)

    assert shown.status_code == 200, shown.text
    assert shown.json()["biosample_idx"] == corpus.entity_idx


async def test_global_value_from_another_study_judged_there(ctx):
    """A global value another study wrote is judged on that study: a member of
    this study alone does not see a member-tier value the other study holds,
    and a grant on the other study brings it into view."""
    study_idx = await _seed_study_for_case(ctx, "member")
    corpus = await _seed_biosample_corpus(ctx, study_idx)
    other_study_idx = await _seed_study(
        ctx, owner_idx=ctx["wet_session"]["principal_idx"], suffix="vis-other"
    )
    await seed_biosample_to_study_link(
        ctx["pool"],
        biosample_idx=corpus.entity_idx,
        study_idx=other_study_idx,
        created_by_idx=ctx["wet_session"]["principal_idx"],
    )
    foreign_key = await _seed_global_value(
        ctx,
        BIOSAMPLE_METADATA_SPEC,
        study_idx=other_study_idx,
        entity_idx=corpus.entity_idx,
        default_tier=Tier.MEMBER,
    )
    url = URL_BIOSAMPLE_BY_STUDY_AND_IDX.format(
        study_idx=study_idx, biosample_idx=corpus.entity_idx
    )

    before = await ctx["user"].get(url)

    assert before.status_code == 200, before.text
    assert foreign_key not in before.json()["global_metadata"]

    await _grant_study_access(
        ctx,
        study_idx=other_study_idx,
        principal_idx=ctx["user_session"]["principal_idx"],
        tier=Tier.MEMBER,
        granted_by_idx=ctx["wet_session"]["principal_idx"],
    )
    after = await ctx["user"].get(url)

    assert after.status_code == 200, after.text
    assert foreign_key in after.json()["global_metadata"]


@pytest.mark.parametrize("case", tuple(_CASES))
async def test_study_scoped_sequenced_sample_read_shows_only_visible_values(ctx, case):
    study_idx = await _seed_study_for_case(ctx, case)
    corpus = await _seed_sequenced_sample_corpus(ctx, study_idx)

    resp = await _client_for_case(ctx, case).get(
        URL_SEQUENCED_SAMPLE_BY_STUDY_AND_IDX.format(
            study_idx=study_idx, sequenced_sample_idx=corpus.entity_idx
        )
    )

    assert resp.status_code == 200, resp.text
    body = resp.json()
    read_tier = _CASES[case][2]
    assert set(body["global_metadata"]) == corpus.visible(corpus.global_keys, read_tier)
    assert set(body["local_metadata"]) == corpus.visible(corpus.local_keys, read_tier)


async def test_owner_id_field_declared_below_its_tier_stays_hidden(ctx):
    """A study that declares its owner-biosample-id field at public before the
    first import does not expose the owner's names to a viewer; a member still
    reads them."""
    study_idx = await _seed_study_for_case(ctx, "viewer")
    owner_id_field = unique_field_name("vis_owner_id")
    declared = await ctx["wet"].post(
        URL_BIOSAMPLE_STUDY_FIELD_BY_STUDY.format(study_idx=study_idx),
        json={
            "display_name": owner_id_field,
            "data_type": FieldDataType.TEXT,
            "unique_in_study": True,
            "tier_override": Tier.PUBLIC,
        },
    )
    assert declared.status_code == 201, declared.text
    biosample_idx = await _import_biosample(
        ctx,
        study_idx=study_idx,
        owner_idx=ctx["wet_session"]["principal_idx"],
        owner_id_field=owner_id_field,
        owner_id_value=f"VIS-{secrets.token_hex(3)}",
    )
    url = URL_BIOSAMPLE_BY_STUDY_AND_IDX.format(study_idx=study_idx, biosample_idx=biosample_idx)

    as_viewer = await ctx["user"].get(url)

    assert as_viewer.status_code == 200, as_viewer.text
    assert owner_id_field not in as_viewer.json()["local_metadata"]

    await ctx["pool"].execute(
        "UPDATE qiita.study_access SET access_tier = $3::qiita.tier"
        " WHERE study_idx = $1 AND principal_idx = $2",
        study_idx,
        ctx["user_session"]["principal_idx"],
        Tier.MEMBER,
    )
    as_member = await ctx["user"].get(url)

    assert as_member.status_code == 200, as_member.text
    assert owner_id_field in as_member.json()["local_metadata"]


@dataclass(frozen=True)
class _TimestampSurface:
    """One read whose timestamps and ETag must describe only what it returns."""

    seed: Callable[..., Awaitable[_Corpus]]
    url: Callable[[_Corpus], str]
    # Writes one value on the corpus's entity that a viewer of the study can
    # (hidden=False) or cannot (hidden=True) read through this surface.
    write: Callable[..., Awaitable[str]]
    # The response's own-record timestamp.
    record_stamp: str


async def _write_local(ctx, spec, corpus: _Corpus, *, hidden: bool) -> str:
    return await _seed_local_value(
        ctx,
        spec,
        study_idx=corpus.study_idx,
        entity_idx=corpus.metadata_entity_idx,
        tier_override=Tier.ADMIN if hidden else Tier.VIEWER,
    )


async def _write_global(ctx, spec, corpus: _Corpus, *, hidden: bool) -> str:
    return await _seed_global_value(
        ctx,
        spec,
        study_idx=corpus.study_idx,
        entity_idx=corpus.metadata_entity_idx,
        default_tier=Tier.MEMBER if hidden else Tier.PUBLIC,
    )


_TIMESTAMP_SURFACES = {
    "study_scoped_biosample": _TimestampSurface(
        seed=_seed_biosample_corpus,
        url=lambda c: URL_BIOSAMPLE_BY_STUDY_AND_IDX.format(
            study_idx=c.study_idx, biosample_idx=c.entity_idx
        ),
        write=lambda ctx, c, *, hidden: _write_local(
            ctx, BIOSAMPLE_METADATA_SPEC, c, hidden=hidden
        ),
        record_stamp="updated_at",
    ),
    "biosample": _TimestampSurface(
        seed=_seed_biosample_corpus,
        url=lambda c: URL_BIOSAMPLE_BY_IDX.format(biosample_idx=c.entity_idx),
        write=lambda ctx, c, *, hidden: _write_global(
            ctx, BIOSAMPLE_METADATA_SPEC, c, hidden=hidden
        ),
        record_stamp="updated_at",
    ),
    "study_scoped_sequenced_sample": _TimestampSurface(
        seed=_seed_sequenced_sample_corpus,
        url=lambda c: URL_SEQUENCED_SAMPLE_BY_STUDY_AND_IDX.format(
            study_idx=c.study_idx, sequenced_sample_idx=c.entity_idx
        ),
        write=lambda ctx, c, *, hidden: _write_local(
            ctx, PREP_SAMPLE_METADATA_SPEC, c, hidden=hidden
        ),
        record_stamp="effective_updated_at",
    ),
}


@pytest.mark.parametrize("surface", tuple(_TIMESTAMP_SURFACES))
async def test_hidden_write_moves_nothing_the_viewer_reads(ctx, surface):
    """A write to a value the viewer cannot read leaves the viewer's
    last_metadata_change_at, record timestamp, and ETag where they were, so
    the read discloses neither that hidden values exist nor when they change;
    a caller who reads the value sees all of it move. A write the viewer can
    read moves their last_metadata_change_at and ETag, and never the record's
    own timestamp."""
    s = _TIMESTAMP_SURFACES[surface]
    study_idx = await _seed_study_for_case(ctx, "viewer")
    corpus = await s.seed(ctx, study_idx)
    url = s.url(corpus)

    async def stamps(client) -> tuple[str, str, str | None]:
        resp = await client.get(url)
        assert resp.status_code == 200, resp.text
        body = resp.json()
        return resp.headers["ETag"], body[s.record_stamp], body["last_metadata_change_at"]

    viewer_before = await stamps(ctx["user"])
    wet_before = await stamps(ctx["wet"])

    await s.write(ctx, corpus, hidden=True)

    assert await stamps(ctx["user"]) == viewer_before
    wet_etag, _, wet_last_change = await stamps(ctx["wet"])
    assert wet_etag != wet_before[0]
    assert wet_last_change != wet_before[2]

    await s.write(ctx, corpus, hidden=False)

    viewer_etag, viewer_record, viewer_last_change = await stamps(ctx["user"])
    assert viewer_etag != viewer_before[0]
    assert viewer_last_change != viewer_before[2]
    assert viewer_record == viewer_before[1]


async def test_patch_biosample_refuses_an_etag_a_metadata_write_made_stale(ctx):
    """The ETag a read hands out covers its metadata as well as the record:
    a value written after the read makes that ETag stale for the PATCH."""
    study_idx = await _seed_study_for_case(ctx, "wet_lab_admin")
    corpus = await _seed_biosample_corpus(ctx, study_idx)
    url = URL_BIOSAMPLE_BY_IDX.format(biosample_idx=corpus.entity_idx)
    stale = (await ctx["wet"].get(url)).headers["ETag"]

    await _write_global(ctx, BIOSAMPLE_METADATA_SPEC, corpus, hidden=False)

    refused = await ctx["wet"].patch(
        url, json={"submission_error": "transient"}, headers={"If-Match": stale}
    )
    assert refused.status_code == 412, refused.text
    fresh = (await ctx["wet"].get(url)).headers["ETag"]
    accepted = await ctx["wet"].patch(
        url, json={"submission_error": "transient"}, headers={"If-Match": fresh}
    )
    assert accepted.status_code == 200, accepted.text


async def test_patch_sequenced_sample_refuses_an_etag_a_metadata_write_made_stale(ctx):
    """The sequenced-sample twin: a prep_sample value written after the read
    makes its ETag stale for the PATCH."""
    study_idx = await _seed_study_for_case(ctx, "wet_lab_admin")
    corpus = await _seed_sequenced_sample_corpus(ctx, study_idx)
    url = URL_SEQUENCED_SAMPLE_BY_IDX.format(sequenced_sample_idx=corpus.entity_idx)
    stale = (await ctx["wet"].get(url)).headers["ETag"]

    await _write_global(ctx, PREP_SAMPLE_METADATA_SPEC, corpus, hidden=False)

    refused = await ctx["wet"].patch(
        url, json={"ena_experiment_accession": "ERX9000002"}, headers={"If-Match": stale}
    )
    assert refused.status_code == 412, refused.text
    fresh = (await ctx["wet"].get(url)).headers["ETag"]
    accepted = await ctx["wet"].patch(
        url, json={"ena_experiment_accession": "ERX9000002"}, headers={"If-Match": fresh}
    )
    assert accepted.status_code == 200, accepted.text
