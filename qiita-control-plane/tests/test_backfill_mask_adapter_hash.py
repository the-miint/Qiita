"""Tests for the mask adapter-hash re-key backfill.

The mint converts a mask_definition row when something re-mints its config; this
backfill converts the rest, so the contract phase that deletes the legacy lookup
has a column free of NULLs to read. It attributes rows by grouping them on their
stored hash rather than recomputing the legacy digest, which would need the
adapter Parquet bytes.

Every plan here is scoped with `mask_idxs` to the rows its own fixture created.
`plan_rekey` with no scope reads every row in qiita.mask_definition, and the DB
tier runs under xdist against one database — an unscoped plan would see another
worker's rows and, worse, write to them.
"""

import json
import secrets

import pytest
import pytest_asyncio
from qiita_common.hashing import canonical_params_hash

from qiita_control_plane import runner
from qiita_control_plane.backfill.mask_adapter_hash import apply_rekey, plan_rekey
from qiita_control_plane.repositories.mask_definition import (
    ADAPTER_HASH_SCHEME_SEQUENCE_HASH,
    fetch_mask_definition_by_idx,
    mint_mask_definition,
)
from qiita_control_plane.repositories.reference_membership import (
    reference_sequence_set_hash,
)
from qiita_control_plane.testing.db_seeds import (
    delete_reference_with_sequences,
    seed_legacy_mask_definition,
    seed_reference_with_sequences,
    seed_user_principal,
)

pytestmark = pytest.mark.db

# Distinct canonical hashes: none is another's reverse complement, so each seeds
# its own feature. (TTTTGGGG/CCCCAAAA would collapse to one.)
_ADAPTERS = ["ACGTACGT", "TTTTGGGG"]


def _params(adapter_set_hash, *, version: str) -> dict:
    """A mask config carrying `adapter_set_hash`, distinct per `version` so
    sibling rows in one test do not collapse onto one params_hash."""
    return runner._build_mask_params(
        action_id="read-mask",
        action_version=version,
        prep_protocol_idx=None,
        instrument_model="NextSeq 550",
        adapter_set_hash=adapter_set_hash,
        host_rype_reference_idx=None,
        host_minimap2_reference_idx=None,
        resolved_lima=None,
        resolved_syndna=None,
    )


@pytest_asyncio.fixture
async def seeded(postgres_pool):
    """A principal, an adapter reference, and two row-makers: `legacy` inserts an
    unstamped pre-migration row, `mint` goes through the current mint path (which
    stamps the scheme). Both track what they created, which is also what `plan`
    scopes to."""
    principal_idx = await seed_user_principal(postgres_pool, prefix="rekey", suffix="owner")
    references: list[int] = []

    async def adapter_reference(sequences: list[str]) -> int:
        reference_idx = await seed_reference_with_sequences(
            postgres_pool,
            name=f"rekey-adapters-{secrets.token_hex(6)}",
            created_by_idx=principal_idx,
            sequences=sequences,
        )
        references.append(reference_idx)
        return reference_idx

    reference_idx = await adapter_reference(_ADAPTERS)
    created: list[int] = []

    async def legacy(params: dict) -> int:
        mask_idx = await seed_legacy_mask_definition(
            postgres_pool, params=params, created_by_idx=principal_idx
        )
        created.append(mask_idx)
        return mask_idx

    async def mint(params: dict) -> int:
        """The derivation-aware mint path: it states the scheme, as the runner
        and the block planner do. (The public POST /mask-definition route does
        not — it mints caller-supplied params, so its rows stay unstamped.)"""
        async with postgres_pool.acquire() as conn:
            row = await mint_mask_definition(
                conn,
                filter_workflow=params["filter_workflow"],
                filter_version=params["filter_version"],
                params=params,
                principal_idx=principal_idx,
                adapter_hash_scheme=ADAPTER_HASH_SCHEME_SEQUENCE_HASH,
            )
        created.append(row["mask_idx"])
        return row["mask_idx"]

    async def plan(**kwargs):
        """plan_rekey scoped to this test's own rows unless told otherwise."""
        kwargs.setdefault("reference_idx", reference_idx)
        kwargs.setdefault("mask_idxs", created)
        return await plan_rekey(postgres_pool, **kwargs)

    yield {
        "pool": postgres_pool,
        "principal_idx": principal_idx,
        "reference_idx": reference_idx,
        "adapter_reference": adapter_reference,
        "legacy": legacy,
        "mint": mint,
        "plan": plan,
        "created": created,
    }
    await postgres_pool.execute(
        "DELETE FROM qiita.mask_definition WHERE mask_idx = ANY($1::bigint[])", created
    )
    for seeded_reference_idx in references:
        await delete_reference_with_sequences(postgres_pool, seeded_reference_idx)
    await postgres_pool.execute("DELETE FROM qiita.user WHERE principal_idx = $1", principal_idx)
    await postgres_pool.execute("DELETE FROM qiita.principal WHERE idx = $1", principal_idx)


async def _current_hash(seeded) -> str:
    return await reference_sequence_set_hash(seeded["pool"], seeded["reference_idx"])


async def test_plan_converts_the_single_legacy_hash(seeded):
    """Rows all carrying ONE legacy hash are attributable to the one canonical
    adapter set, so they are convertible."""
    tag = secrets.token_hex(4)
    a = await seeded["legacy"](_params("legacy-bytes-digest", version=f"{tag}-a"))
    b = await seeded["legacy"](_params("legacy-bytes-digest", version=f"{tag}-b"))

    plan = await seeded["plan"]()

    assert [r.mask_idx for r in plan.convertible] == [a, b]
    assert plan.unattributable == {}
    assert plan.collided == []
    assert not plan.blocked()
    assert plan.current_hash == await _current_hash(seeded)


async def test_plan_reports_rather_than_guesses_when_hashes_disagree(seeded):
    """Two distinct stored hashes cannot both be the canonical adapter set, and
    telling them apart needs the adapter bytes. Nothing is convertible; both are
    reported and the plan is blocked."""
    tag = secrets.token_hex(4)
    a = await seeded["legacy"](_params(f"digest-one-{tag}", version=f"{tag}-a"))
    b = await seeded["legacy"](_params(f"digest-two-{tag}", version=f"{tag}-b"))

    plan = await seeded["plan"]()

    assert plan.convertible == []
    assert plan.unattributable[f"digest-one-{tag}"] == [a]
    assert plan.unattributable[f"digest-two-{tag}"] == [b]
    assert plan.blocked()


async def test_apply_refuses_a_blocked_plan(seeded):
    """A blocked plan writes nothing at all — not even the rows it is sure about.
    An unattributable hash means more than one adapter set is stored, which puts
    the whole attribution in doubt."""
    tag = secrets.token_hex(4)
    a = await seeded["legacy"](_params(f"digest-one-{tag}", version=f"{tag}-a"))
    await seeded["legacy"](_params(f"digest-two-{tag}", version=f"{tag}-b"))

    plan = await seeded["plan"]()
    with pytest.raises(RuntimeError, match="refusing to write"):
        await apply_rekey(seeded["pool"], plan)

    assert (await fetch_mask_definition_by_idx(seeded["pool"], a))["adapter_hash_scheme"] is None


async def test_explicit_mask_idxs_convert_a_reported_residue(seeded):
    """Naming the rows is how an operator resolves an unattributable report: the
    attribution becomes a stated decision, and rows carrying different hashes all
    convert. Without this the residue is unconvertible and the contract phase is
    unreachable."""
    tag = secrets.token_hex(4)
    a = await seeded["legacy"](_params(f"digest-one-{tag}", version=f"{tag}-a"))
    b = await seeded["legacy"](_params(f"digest-two-{tag}", version=f"{tag}-b"))

    plan = await seeded["plan"](mask_idxs=[a, b], attribute_all=True)

    assert [r.mask_idx for r in plan.convertible] == [a, b]
    assert plan.unattributable == {}
    assert await apply_rekey(seeded["pool"], plan) == 2
    current_hash = await _current_hash(seeded)
    for mask_idx in (a, b):
        row = await fetch_mask_definition_by_idx(seeded["pool"], mask_idx)
        assert row["adapter_hash_scheme"] == ADAPTER_HASH_SCHEME_SEQUENCE_HASH
        assert json.loads(row["params"])["resolved_qc"]["adapter_set_hash"] == current_hash


async def test_plan_skips_rows_with_no_adapter_set(seeded):
    """A config using no adapter set stores a NULL adapter_set_hash. The two
    derivations agree on None, so the row is not part of this migration and keeps
    a NULL scheme permanently."""
    tag = secrets.token_hex(4)
    maskless = await seeded["legacy"](_params(None, version=f"{tag}-none"))

    plan = await seeded["plan"]()

    assert maskless not in [r.mask_idx for r in plan.writable()]
    assert all(maskless not in idxs for idxs in plan.unattributable.values())


async def test_stamp_only_row_gets_the_scheme_and_keeps_its_params_hash(seeded):
    """A row already storing the current hash but carrying no scheme needs the
    stamp and nothing else — its params and params_hash come out byte-identical.
    The mint's fast path leaves rows in this state (it returns an existing row
    without stamping it), as does a migrate:down/up round trip."""
    tag = secrets.token_hex(4)
    current_hash = await _current_hash(seeded)
    params = _params(current_hash, version=f"{tag}-a")
    mask_idx = await seeded["legacy"](params)
    before = await fetch_mask_definition_by_idx(seeded["pool"], mask_idx)

    plan = await seeded["plan"]()

    assert [r.mask_idx for r in plan.stamp_only] == [mask_idx]
    assert plan.convertible == []
    assert await apply_rekey(seeded["pool"], plan) == 1

    after = await fetch_mask_definition_by_idx(seeded["pool"], mask_idx)
    assert after["adapter_hash_scheme"] == ADAPTER_HASH_SCHEME_SEQUENCE_HASH
    assert bytes(after["params_hash"]) == bytes(before["params_hash"])
    assert json.loads(after["params"]) == json.loads(before["params"])


async def test_apply_rekeys_in_place_and_is_idempotent(seeded):
    """Apply rewrites params, params_hash and the scheme while keeping mask_idx —
    so mask_sample / work_ticket references stay valid. A second run finds nothing
    left to write."""
    tag = secrets.token_hex(4)
    mask_idx = await seeded["legacy"](_params("legacy-bytes-digest", version=f"{tag}-a"))

    plan = await seeded["plan"]()
    assert await apply_rekey(seeded["pool"], plan) == 1

    row = await fetch_mask_definition_by_idx(seeded["pool"], mask_idx)
    assert row["mask_idx"] == mask_idx
    assert row["adapter_hash_scheme"] == ADAPTER_HASH_SCHEME_SEQUENCE_HASH
    current_hash = await _current_hash(seeded)
    assert json.loads(row["params"])["resolved_qc"]["adapter_set_hash"] == current_hash
    assert bytes(row["params_hash"]) == canonical_params_hash(
        _params(current_hash, version=f"{tag}-a")
    )

    again = await seeded["plan"]()
    assert again.writable() == []


async def test_plan_reports_a_collision_rather_than_planning_a_write(seeded):
    """When the same config already exists under the current derivation, the
    legacy row's re-keyed params_hash is taken. Merging means repointing
    mask_sample / work_ticket rows, so the row is reported at PLAN time — a dry
    run has to show it — and left out of the writable set."""
    tag = secrets.token_hex(4)
    current_hash = await _current_hash(seeded)
    legacy_idx = await seeded["legacy"](_params("legacy-bytes-digest", version=f"{tag}-a"))
    current_idx = await seeded["mint"](_params(current_hash, version=f"{tag}-a"))

    plan = await seeded["plan"]()

    assert plan.collided == [legacy_idx]
    assert legacy_idx not in [r.mask_idx for r in plan.writable()]
    assert plan.blocked()
    assert (await fetch_mask_definition_by_idx(seeded["pool"], legacy_idx))[
        "adapter_hash_scheme"
    ] is None
    assert (await fetch_mask_definition_by_idx(seeded["pool"], current_idx))[
        "adapter_hash_scheme"
    ] == ADAPTER_HASH_SCHEME_SEQUENCE_HASH


async def test_plan_refuses_a_memberless_reference(seeded):
    """A reference naming no sequences has no identity to re-key onto."""
    empty = await seeded["adapter_reference"]([])
    with pytest.raises(RuntimeError, match="no rows in qiita.reference_membership"):
        await seeded["plan"](reference_idx=empty)


async def test_two_rows_that_merge_onto_one_params_hash_are_reported(seeded):
    """Two rows whose configs are identical apart from their stored adapter hash
    collapse onto ONE params_hash the moment both are re-keyed. Nothing else has
    taken that hash, so the collision is between the two plan rows themselves.

    This is the pyarrow-bump shape the migration describes: the same config
    minted before and after a writer bump carries two byte digests. It reaches
    `--attribute-all`, which is the documented way out of an unattributable
    report."""
    tag = secrets.token_hex(4)
    a = await seeded["legacy"](_params(f"digest-one-{tag}", version=f"{tag}-same"))
    b = await seeded["legacy"](_params(f"digest-two-{tag}", version=f"{tag}-same"))

    plan = await seeded["plan"](mask_idxs=[a, b], attribute_all=True)

    assert plan.collided == sorted([a, b])
    assert plan.writable() == []
    assert plan.blocked()
    with pytest.raises(RuntimeError, match="refusing to write"):
        await apply_rekey(seeded["pool"], plan)
