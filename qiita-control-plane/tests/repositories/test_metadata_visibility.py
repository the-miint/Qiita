"""DB-bound tests for the metadata visibility rule: `qiita.metadata_field_tier`
and the per-entity `qiita.*_metadata_visibility` functions.

The functions are called directly with synthesized caller tier maps, so these
tests pin the SQL rule on its own; how a principal becomes a map is pinned with
the guards. The expected answer comes from an oracle written here,
independently of the SQL, so a drift in either one fails.
"""

import json
import secrets
from dataclasses import dataclass
from decimal import Decimal

import pytest
import pytest_asyncio
from qiita_common.models import FieldDataType, Tier

from qiita_control_plane.repositories._sample_helpers import (
    EntityMetadataSpec,
    SampleEntityKind,
    fetch_visible_metadata_last_change,
    insert_entity_to_study,
    visible_metadata_source,
)
from qiita_control_plane.repositories.biosample import OWNER_BIOSAMPLE_ID_TIER_OVERRIDE
from qiita_control_plane.repositories.biosample_metadata import (
    BIOSAMPLE_METADATA_SPEC,
    insert_owner_biosample_id_metadata,
)
from qiita_control_plane.repositories.study_access import CallerMetadataTiers
from qiita_control_plane.testing.db_seeds import (
    fetch_missing_value_reason_idx,
    fetch_seeded_metagenome_term,
    seed_biosample,
    seed_field_exception,
    seed_global_field_for_spec,
    seed_globally_linked_study_field,
    seed_local_study_field,
    seed_metadata_text_value,
    seed_sequenced_prep_sample,
    seed_study,
    seed_user_principal,
)
from qiita_control_plane.testing.db_teardown import (
    delete_idxs,
    delete_principal,
    teardown_entity_graph,
)
from qiita_control_plane.testing.tiers import TIER_RANK
from qiita_control_plane.testing.unique_names import unique_field_name

from .conftest import (
    SPECS,
    _create_linked_entity_for_spec,
    _seed_global_field_for_spec,
    _seed_secondary_studies_for_entity,
    _set_unique_in_study,
)

pytestmark = pytest.mark.db

_ALL_TIERS = (Tier.PUBLIC, Tier.VIEWER, Tier.MEMBER, Tier.ADMIN)
# An exception can only raise a value's required tier, so a public one would
# never restrict anything; the corpus seeds the three that can.
_EXCEPTION_TIERS = (None, Tier.VIEWER, Tier.MEMBER, Tier.ADMIN)

_VALUE_COLUMNS = (
    "value_text",
    "value_numeric",
    "value_boolean",
    "value_date",
    "value_terminology_term_idx",
    "value_missing_reason_idx",
)
# Every column the functions mask on a hidden row: the value, and when it last
# changed.
_MASKED_COLUMNS = (*_VALUE_COLUMNS, "updated_at")


def _most_restrictive(*tiers: Tier | None) -> Tier:
    return max((t for t in tiers if t is not None), key=TIER_RANK.__getitem__)


def _spec_id(spec: EntityMetadataSpec) -> str:
    return str(spec.entity_kind)


@dataclass(frozen=True)
class _SeededValue:
    """One corpus value and what the oracle needs to judge it."""

    metadata_idx: int
    study_idx: int
    field_tier: Tier
    exception_tier: Tier | None

    @property
    def required_tier(self) -> Tier:
        return _most_restrictive(self.field_tier, self.exception_tier)

    def visible_to(self, tiers: CallerMetadataTiers) -> bool:
        held = tiers.study_tiers.get(self.study_idx, tiers.default)
        return TIER_RANK[held] >= TIER_RANK[self.required_tier]


@dataclass(frozen=True)
class _Corpus:
    entity_idx: int
    study_idxs: tuple[int, ...]
    values: tuple[_SeededValue, ...]


async def _seed_global_field(pool, spec, *, default_tier, created_by_idx) -> int:
    suffix = secrets.token_hex(4)
    return await seed_global_field_for_spec(
        pool,
        spec=spec,
        internal_name=f"vis_{suffix}",
        display_name=f"Vis {suffix}",
        data_type=FieldDataType.TEXT,
        created_by_idx=created_by_idx,
        default_tier=default_tier,
    )


async def _seed_entity_on_studies(pool, spec, *, study_idxs, owner_idx) -> tuple[int, int]:
    """Seed one entity linked to every study; return (entity_idx, biosample_idx).

    A prep_sample needs its biosample linked to the same study first, so the
    biosample is linked either way and the prep_sample only on the prep spec.
    """
    biosample_idx = await seed_biosample(pool, owner_idx=owner_idx, created_by_idx=owner_idx)
    async with pool.acquire() as conn:
        for study_idx in study_idxs:
            await insert_entity_to_study(
                conn,
                spec=BIOSAMPLE_METADATA_SPEC,
                entity_idx=biosample_idx,
                study_idx=study_idx,
                created_by_idx=owner_idx,
            )
    if spec.entity_kind is SampleEntityKind.BIOSAMPLE:
        return biosample_idx, biosample_idx
    prep_sample_idx = await seed_sequenced_prep_sample(
        pool, biosample_idx=biosample_idx, owner_idx=owner_idx
    )
    async with pool.acquire() as conn:
        for study_idx in study_idxs:
            await insert_entity_to_study(
                conn,
                spec=spec,
                entity_idx=prep_sample_idx,
                study_idx=study_idx,
                created_by_idx=owner_idx,
            )
    return prep_sample_idx, biosample_idx


async def _build_corpus(pool, spec, *, owner_idx, created) -> _Corpus:
    """Seed one entity carrying a value for every (field shape, field tier,
    exception) combination the rule distinguishes.

    Four studies, one per default_tier, so a local field's fall-through to its
    study's default is exercised at every tier. On each: a local field per
    (tier_override, exception). Then a global field per (default_tier,
    exception), each written through one study's alias, round-robin, so global
    values sit on studies whose defaults differ from their field's tier.
    """
    study_idxs = []
    for default_tier in _ALL_TIERS:
        study_idx = await seed_study(
            pool,
            owner_idx=owner_idx,
            title=f"vis-{default_tier}-{secrets.token_hex(4)}",
            default_tier=default_tier,
        )
        study_idxs.append(study_idx)
        created["study"].append(study_idx)
    entity_idx, biosample_idx = await _seed_entity_on_studies(
        pool, spec, study_idxs=study_idxs, owner_idx=owner_idx
    )
    created["biosample"].append(biosample_idx)
    if entity_idx != biosample_idx:
        created["prep_sample"].append(entity_idx)

    values: list[_SeededValue] = []
    for study_idx, study_default in zip(study_idxs, _ALL_TIERS, strict=True):
        for tier_override in (None, *_ALL_TIERS):
            for exception_tier in _EXCEPTION_TIERS:
                field_idx = await seed_local_study_field(
                    pool,
                    spec=spec,
                    study_idx=study_idx,
                    display_name=unique_field_name("vis"),
                    created_by_idx=owner_idx,
                    tier_override=tier_override,
                )
                metadata_idx = await seed_metadata_text_value(
                    pool,
                    spec=spec,
                    entity_idx=entity_idx,
                    study_field_idx=field_idx,
                    created_by_idx=owner_idx,
                )
                if exception_tier is not None:
                    await seed_field_exception(
                        pool,
                        spec=spec,
                        entity_idx=entity_idx,
                        study_field_idx=field_idx,
                        tier_override=exception_tier,
                        created_by_idx=owner_idx,
                    )
                values.append(
                    _SeededValue(
                        metadata_idx=metadata_idx,
                        study_idx=study_idx,
                        field_tier=tier_override if tier_override is not None else study_default,
                        exception_tier=exception_tier,
                    )
                )

    position = 0
    for default_tier in _ALL_TIERS:
        for exception_tier in _EXCEPTION_TIERS:
            study_idx = study_idxs[position % len(study_idxs)]
            position += 1
            global_field_idx = await _seed_global_field(
                pool, spec, default_tier=default_tier, created_by_idx=owner_idx
            )
            created["global_field"].append(global_field_idx)
            alias_idx = await seed_globally_linked_study_field(
                pool,
                spec=spec,
                study_idx=study_idx,
                global_field_idx=global_field_idx,
                display_name=unique_field_name("vis_alias"),
                created_by_idx=owner_idx,
            )
            metadata_idx = await seed_metadata_text_value(
                pool,
                spec=spec,
                entity_idx=entity_idx,
                study_field_idx=alias_idx,
                created_by_idx=owner_idx,
            )
            if exception_tier is not None:
                await seed_field_exception(
                    pool,
                    spec=spec,
                    entity_idx=entity_idx,
                    global_field_idx=global_field_idx,
                    tier_override=exception_tier,
                    created_by_idx=owner_idx,
                )
            values.append(
                _SeededValue(
                    metadata_idx=metadata_idx,
                    study_idx=study_idx,
                    field_tier=default_tier,
                    exception_tier=exception_tier,
                )
            )

    return _Corpus(entity_idx=entity_idx, study_idxs=tuple(study_idxs), values=tuple(values))


@pytest_asyncio.fixture(scope="module")
async def corpora(postgres_pool):
    """One corpus per spec, built once for the module's matrix."""
    owner_idx = await seed_user_principal(postgres_pool, prefix="vis", suffix="owner")
    created: dict[str, list[int]] = {
        "study": [],
        "biosample": [],
        "prep_sample": [],
        "global_field": [],
    }
    built: dict[SampleEntityKind, _Corpus] = {}
    global_fields: dict[SampleEntityKind, list[int]] = {}
    try:
        for spec in SPECS:
            # Each spec's global fields live in their own table, so each gets
            # its own list; bound before building so a failure part-way still
            # tears down what was seeded.
            global_fields[spec.entity_kind] = created["global_field"] = []
            built[spec.entity_kind] = await _build_corpus(
                postgres_pool, spec, owner_idx=owner_idx, created=created
            )
        yield built
    finally:
        await teardown_entity_graph(
            postgres_pool,
            study_idxs=created["study"],
            biosample_idxs=created["biosample"],
            prep_sample_idxs=created["prep_sample"],
        )
        await delete_idxs(
            postgres_pool,
            "biosample_global_field",
            global_fields.get(SampleEntityKind.BIOSAMPLE, []),
        )
        await delete_idxs(
            postgres_pool,
            "prep_sample_global_field",
            global_fields.get(SampleEntityKind.PREP_SAMPLE, []),
        )
        await delete_principal(postgres_pool, owner_idx)


# Caller maps keyed by name; each is built from the corpus's study positions so
# one map definition serves both specs.
_CALLER_MAPS = {
    "public_everywhere": (dict.fromkeys(range(4), Tier.PUBLIC), Tier.PUBLIC),
    "viewer_everywhere": (dict.fromkeys(range(4), Tier.VIEWER), Tier.PUBLIC),
    "member_everywhere": (dict.fromkeys(range(4), Tier.MEMBER), Tier.PUBLIC),
    "admin_everywhere": (dict.fromkeys(range(4), Tier.ADMIN), Tier.PUBLIC),
    "no_grants": ({}, Tier.PUBLIC),
    "mixed": ({0: Tier.VIEWER, 1: Tier.ADMIN, 2: Tier.MEMBER}, Tier.PUBLIC),
    "bypass": ({}, Tier.ADMIN),
}


def _tiers_for(corpus: _Corpus, map_name: str) -> CallerMetadataTiers:
    by_position, default = _CALLER_MAPS[map_name]
    return CallerMetadataTiers(
        study_tiers={corpus.study_idxs[pos]: tier for pos, tier in by_position.items()},
        default=default,
    )


async def _visibility_rows(pool, spec, tiers: CallerMetadataTiers, entity_idx: int):
    return await pool.fetch(
        f"SELECT m.idx, m.study_idx, m.required_tier, m.caller_tier, m.visible,"
        f" {', '.join(f'm.{c}' for c in _MASKED_COLUMNS)}"
        f" FROM {visible_metadata_source(spec)}"
        f" WHERE m.{spec.entity_key_column} = $4",
        *tiers.sql_args(),
        entity_idx,
    )


@pytest.mark.parametrize(
    ("global_default", "local_override", "study_default", "expected"),
    [
        # A global field's tier is its own, whatever the study's default.
        (Tier.PUBLIC, None, Tier.ADMIN, Tier.PUBLIC),
        (Tier.MEMBER, None, Tier.PUBLIC, Tier.MEMBER),
        # A purely-local field's override, when it has one.
        (None, Tier.VIEWER, Tier.ADMIN, Tier.VIEWER),
        (None, Tier.ADMIN, Tier.PUBLIC, Tier.ADMIN),
        # No override: the study's default.
        (None, None, Tier.MEMBER, Tier.MEMBER),
        (None, None, Tier.PUBLIC, Tier.PUBLIC),
    ],
)
async def test_metadata_field_tier_chain(
    postgres_pool, global_default, local_override, study_default, expected
):
    got = await postgres_pool.fetchval(
        "SELECT qiita.metadata_field_tier($1::qiita.tier, $2::qiita.tier, $3::qiita.tier)",
        global_default,
        local_override,
        study_default,
    )
    assert Tier(got) is expected


@pytest.mark.parametrize("map_name", tuple(_CALLER_MAPS))
@pytest.mark.parametrize("spec", SPECS, ids=_spec_id)
async def test_visibility_matches_rule(postgres_pool, corpora, spec, map_name):
    corpus = corpora[spec.entity_kind]
    tiers = _tiers_for(corpus, map_name)

    rows = await _visibility_rows(postgres_pool, spec, tiers, corpus.entity_idx)

    by_idx = {r["idx"]: r for r in rows}
    assert set(by_idx) == {v.metadata_idx for v in corpus.values}
    for value in corpus.values:
        row = by_idx[value.metadata_idx]
        expected = value.visible_to(tiers)
        context = f"{value} under {map_name}"
        assert Tier(row["required_tier"]) is value.required_tier, context
        assert row["visible"] is expected, context
        assert (row["value_text"] is not None) is expected, context


@pytest.mark.parametrize("spec", SPECS, ids=_spec_id)
async def test_hidden_rows_null_every_value_column(ctx, spec):
    """A hidden typed value, a hidden missing-reason marker, and a hidden
    terminology term all come back with every value column NULL, so a
    predicate over any of them sees UNKNOWN rather than the value."""
    pool = ctx["pool"]
    entity_idx = await _create_linked_entity_for_spec(ctx, spec)
    term = await fetch_seeded_metagenome_term(pool)
    reason_idx = await fetch_missing_value_reason_idx(pool, "not collected")
    assert reason_idx is not None

    async def admin_field(data_type, terminology_idx=None):
        return await pool.fetchval(
            f"INSERT INTO {spec.study_field_table}"
            f" (study_idx, display_name, data_type, required, terminology_idx,"
            f"  tier_override, created_by_idx)"
            f" VALUES ($1, $2, $3, false, $4, $5::qiita.tier, $6) RETURNING idx",
            ctx["study_idx"],
            unique_field_name("hidden"),
            data_type,
            terminology_idx,
            Tier.ADMIN,
            ctx["principal_idx"],
        )

    seeded = [
        (await admin_field(FieldDataType.NUMERIC), "value_numeric", Decimal(7)),
        (await admin_field(FieldDataType.TEXT), "value_missing_reason_idx", reason_idx),
        (
            await admin_field(FieldDataType.TERMINOLOGY, term["terminology_idx"]),
            "value_terminology_term_idx",
            term["idx"],
        ),
    ]
    for field_idx, column, value in seeded:
        await pool.execute(
            f"INSERT INTO {spec.metadata_table}"
            f" ({spec.entity_key_column}, {spec.study_field_idx_column}, {column}, created_by_idx)"
            f" VALUES ($1, $2, $3, $4)",
            entity_idx,
            field_idx,
            value,
            ctx["principal_idx"],
        )

    member = CallerMetadataTiers(study_tiers={ctx["study_idx"]: Tier.MEMBER}, default=Tier.PUBLIC)
    hidden = await _visibility_rows(pool, spec, member, entity_idx)
    assert len(hidden) == len(seeded)
    for row in hidden:
        assert row["visible"] is False
        assert all(row[c] is None for c in _MASKED_COLUMNS), dict(row)

    # The mask is the caller's, not the row's: at admin the same rows carry
    # their values.
    admin = CallerMetadataTiers(study_tiers={ctx["study_idx"]: Tier.ADMIN}, default=Tier.PUBLIC)
    shown = {r["idx"]: r for r in await _visibility_rows(pool, spec, admin, entity_idx)}
    populated = {column for _, column, _ in seeded}
    for row in shown.values():
        assert row["visible"] is True
        assert any(row[c] is not None for c in populated), dict(row)
        assert row["updated_at"] is not None, dict(row)


@pytest.mark.parametrize("spec", SPECS, ids=_spec_id)
async def test_misfiled_local_exception_on_linked_value_still_restricts(ctx, spec):
    """An exception keyed on a linked field's study_field_idx, rather than on
    its global_field_idx where it belongs, still narrows the value: the rule
    applies both keys, so a misfiled row fails closed."""
    pool = ctx["pool"]
    entity_idx = await _create_linked_entity_for_spec(ctx, spec)
    global_field = await _seed_global_field_for_spec(ctx, spec)
    alias_idx = await seed_globally_linked_study_field(
        pool,
        spec=spec,
        study_idx=ctx["study_idx"],
        global_field_idx=global_field.idx,
        display_name=unique_field_name("misfiled"),
        created_by_idx=ctx["principal_idx"],
    )
    metadata_idx = await seed_metadata_text_value(
        pool,
        spec=spec,
        entity_idx=entity_idx,
        study_field_idx=alias_idx,
        created_by_idx=ctx["principal_idx"],
    )
    await seed_field_exception(
        pool,
        spec=spec,
        entity_idx=entity_idx,
        study_field_idx=alias_idx,
        tier_override=Tier.ADMIN,
        created_by_idx=ctx["principal_idx"],
    )

    member = CallerMetadataTiers(study_tiers={ctx["study_idx"]: Tier.MEMBER}, default=Tier.PUBLIC)
    rows = {r["idx"]: r for r in await _visibility_rows(pool, spec, member, entity_idx)}

    assert rows[metadata_idx]["visible"] is False
    assert Tier(rows[metadata_idx]["required_tier"]) is Tier.ADMIN


@pytest.mark.parametrize("spec", SPECS, ids=_spec_id)
async def test_global_value_judged_on_contributing_study(ctx, spec):
    """A global value belongs to the study whose alias wrote it: a grant on
    another study the entity is linked to does not reach it."""
    pool = ctx["pool"]
    entity_idx = await _create_linked_entity_for_spec(ctx, spec)
    (other_study_idx,) = await _seed_secondary_studies_for_entity(ctx, spec, entity_idx, 1)
    async with pool.acquire() as conn:
        await insert_entity_to_study(
            conn,
            spec=spec,
            entity_idx=entity_idx,
            study_idx=other_study_idx,
            created_by_idx=ctx["principal_idx"],
        )
    global_field_idx = await _seed_global_field(
        pool, spec, default_tier=Tier.MEMBER, created_by_idx=ctx["principal_idx"]
    )
    table_key = (
        "biosample_global_field"
        if spec.entity_kind is SampleEntityKind.BIOSAMPLE
        else "prep_sample_global_field"
    )
    ctx["created"][table_key].append(global_field_idx)
    alias_idx = await seed_globally_linked_study_field(
        pool,
        spec=spec,
        study_idx=ctx["study_idx"],
        global_field_idx=global_field_idx,
        display_name=unique_field_name("contrib"),
        created_by_idx=ctx["principal_idx"],
    )
    metadata_idx = await seed_metadata_text_value(
        pool,
        spec=spec,
        entity_idx=entity_idx,
        study_field_idx=alias_idx,
        created_by_idx=ctx["principal_idx"],
    )

    elsewhere = CallerMetadataTiers(study_tiers={other_study_idx: Tier.MEMBER}, default=Tier.PUBLIC)
    on_writer = CallerMetadataTiers(
        study_tiers={ctx["study_idx"]: Tier.MEMBER}, default=Tier.PUBLIC
    )

    hidden = {r["idx"]: r for r in await _visibility_rows(pool, spec, elsewhere, entity_idx)}
    shown = {r["idx"]: r for r in await _visibility_rows(pool, spec, on_writer, entity_idx)}

    assert hidden[metadata_idx]["study_idx"] == ctx["study_idx"]
    assert hidden[metadata_idx]["visible"] is False
    assert shown[metadata_idx]["visible"] is True


def _plan_node_types(plan: dict) -> list[tuple[str, str | None]]:
    """Every (Node Type, Function Name) in an EXPLAIN (FORMAT JSON) plan tree."""
    found = [(plan["Node Type"], plan.get("Function Name"))]
    for child in plan.get("Plans", []):
        found.extend(_plan_node_types(child))
    return found


@pytest.mark.parametrize("spec", SPECS, ids=_spec_id)
async def test_visibility_function_is_inlined(postgres_pool, spec):
    """The planner must inline the function into the caller's query; a
    Function Scan would materialize every metadata row before the caller's
    predicate could narrow it, and no index on the base table could serve it."""
    tiers = CallerMetadataTiers(study_tiers={}, default=Tier.PUBLIC)
    raw = await postgres_pool.fetchval(
        f"EXPLAIN (FORMAT JSON) SELECT m.idx"
        f" FROM {visible_metadata_source(spec)}"
        f" WHERE m.{spec.entity_key_column} = $4",
        *tiers.sql_args(),
        1,
    )
    plan = (json.loads(raw) if isinstance(raw, str) else raw)[0]["Plan"]
    function_scans = [name for node, name in _plan_node_types(plan) if node == "Function Scan"]
    assert spec.visibility_function.removeprefix("qiita.") not in function_scans, plan


async def test_owner_biosample_id_value_never_reads_below_its_pinned_tier(ctx):
    """An owner-biosample-id value requires OWNER_BIOSAMPLE_ID_TIER_OVERRIDE
    even through a field declared at public: a study that declared the field
    before its first import must not expose the owner's names below that
    tier."""
    spec = BIOSAMPLE_METADATA_SPEC
    pool = ctx["pool"]
    entity_idx = await _create_linked_entity_for_spec(ctx, spec)
    field_idx = await seed_local_study_field(
        pool,
        spec=spec,
        study_idx=ctx["study_idx"],
        display_name=unique_field_name("owner_id"),
        created_by_idx=ctx["principal_idx"],
        tier_override=Tier.PUBLIC,
    )
    await _set_unique_in_study(ctx, spec, field_idx, True)
    async with pool.acquire() as conn:
        metadata_idx = await insert_owner_biosample_id_metadata(
            conn,
            biosample_idx=entity_idx,
            biosample_study_field_idx=field_idx,
            value_text=f"OWNER-{secrets.token_hex(3)}",
            created_by_idx=ctx["principal_idx"],
        )
    pinned = OWNER_BIOSAMPLE_ID_TIER_OVERRIDE
    below = max((t for t in Tier if TIER_RANK[t] < TIER_RANK[pinned]), key=TIER_RANK.__getitem__)

    for held, visible in ((below, False), (pinned, True)):
        tiers = CallerMetadataTiers(study_tiers={ctx["study_idx"]: held}, default=Tier.PUBLIC)
        (row,) = [
            r
            for r in await _visibility_rows(pool, spec, tiers, entity_idx)
            if r["idx"] == metadata_idx
        ]
        assert row["required_tier"] == pinned, dict(row)
        assert row["visible"] is visible, dict(row)


async def _parent_timestamps(pool, spec, entity_idx):
    return await pool.fetchrow(
        f"SELECT updated_at, last_metadata_change_at FROM qiita.{spec.entity_kind} WHERE idx = $1",
        entity_idx,
    )


@pytest.mark.parametrize("spec", SPECS, ids=_spec_id)
async def test_metadata_write_moves_only_the_parents_metadata_timestamp(ctx, spec):
    """A metadata write records itself in the parent's
    last_metadata_change_at, not in its updated_at: updated_at describes the
    entity's own record, which a caller sees whatever their tier, so a write
    to a value they may not read must not move it."""
    pool = ctx["pool"]
    entity_idx = await _create_linked_entity_for_spec(ctx, spec)
    field_idx = await seed_local_study_field(
        pool,
        spec=spec,
        study_idx=ctx["study_idx"],
        display_name=unique_field_name("touch"),
        created_by_idx=ctx["principal_idx"],
    )
    before = await _parent_timestamps(pool, spec, entity_idx)

    await seed_metadata_text_value(
        pool,
        spec=spec,
        entity_idx=entity_idx,
        study_field_idx=field_idx,
        created_by_idx=ctx["principal_idx"],
    )

    after = await _parent_timestamps(pool, spec, entity_idx)
    assert after["updated_at"] == before["updated_at"]
    assert after["last_metadata_change_at"] is not None
    assert after["last_metadata_change_at"] != before["last_metadata_change_at"]


@pytest.mark.parametrize("spec", SPECS, ids=_spec_id)
async def test_visible_metadata_last_change_counts_only_the_read_values(ctx, spec):
    """The last change a read reports is the latest updated_at among the
    values that read returns: the entity's global values, plus the named
    study's local ones, each only when visible. A hidden write, or a local
    write on another study, does not move it."""
    pool = ctx["pool"]
    entity_idx = await _create_linked_entity_for_spec(ctx, spec)
    (other_study_idx,) = await _seed_secondary_studies_for_entity(ctx, spec, entity_idx, 1)
    async with pool.acquire() as conn:
        await insert_entity_to_study(
            conn,
            spec=spec,
            entity_idx=entity_idx,
            study_idx=other_study_idx,
            created_by_idx=ctx["principal_idx"],
        )
    viewer = CallerMetadataTiers(
        study_tiers={ctx["study_idx"]: Tier.VIEWER, other_study_idx: Tier.ADMIN},
        default=Tier.PUBLIC,
    )

    async def last_change(study_idx=None):
        return await fetch_visible_metadata_last_change(
            pool, spec=spec, entity_idx=entity_idx, tiers=viewer, study_idx=study_idx
        )

    async def write_local(study_idx, tier_override):
        field_idx = await seed_local_study_field(
            pool,
            spec=spec,
            study_idx=study_idx,
            display_name=unique_field_name("last_change"),
            created_by_idx=ctx["principal_idx"],
            tier_override=tier_override,
        )
        return await seed_metadata_text_value(
            pool,
            spec=spec,
            entity_idx=entity_idx,
            study_field_idx=field_idx,
            created_by_idx=ctx["principal_idx"],
        )

    assert await last_change(ctx["study_idx"]) is None

    shown_idx = await write_local(ctx["study_idx"], Tier.VIEWER)
    shown_at = await pool.fetchval(
        f"SELECT updated_at FROM {spec.metadata_table} WHERE idx = $1", shown_idx
    )
    assert await last_change(ctx["study_idx"]) == shown_at
    # A study-local value belongs to its own study's read, not the
    # entity-wide one.
    assert await last_change() is None

    await write_local(ctx["study_idx"], Tier.ADMIN)
    await write_local(other_study_idx, Tier.VIEWER)
    assert await last_change(ctx["study_idx"]) == shown_at
