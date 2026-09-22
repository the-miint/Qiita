"""Repository-layer tests for the batched prep_sample → study lookup.

`fetch_active_study_idxs_for_prep_samples` is the many-sample sibling of
`fetch_active_study_idxs_for_prep_sample`. It exists so the alignment mint's
per-study access check costs one query plus one lookup per DISTINCT study,
rather than one round trip per sample — a cohort can carry thousands.

The property that matters is that it agrees with the single-sample function on
every input, including the two edge shapes the access gate keys off: a sample
whose links are all retired (which the gate must be able to distinguish from a
sample with links), and a sample linked to more than one study (which must
contribute BOTH studies to the check, since the caller needs access to each).
"""

import secrets

import pytest
import pytest_asyncio

from qiita_control_plane.repositories.prep_sample import (
    fetch_active_study_idxs_for_prep_sample,
    fetch_active_study_idxs_for_prep_samples,
)
from qiita_control_plane.testing.db_seeds import (
    retire_prep_sample_to_study_link,
    seed_biosample_to_study_link,
    seed_biosample_with_sequenced_prep_sample,
    seed_user_principal,
)

pytestmark = pytest.mark.db


async def _seed_study(pool, *, owner_idx: int) -> int:
    return await pool.fetchval(
        "INSERT INTO qiita.study (owner_idx, title, created_by_idx)"
        " VALUES ($1, $2, $1) RETURNING idx",
        owner_idx,
        f"batched-links-{secrets.token_hex(4)}",
    )


@pytest_asyncio.fixture
async def links(postgres_pool):
    """One principal, two studies, and three prep_samples wired as:

    ps_a → study_1              (the ordinary case)
    ps_b → study_1 + study_2    (spans studies — the pool-spans-studies shape)
    ps_c → study_1, retired     (orphaned by retirement)
    """
    suffix = secrets.token_hex(4)
    principal_idx = await seed_user_principal(postgres_pool, prefix="batch-links", suffix=suffix)
    study_1 = await _seed_study(postgres_pool, owner_idx=principal_idx)
    study_2 = await _seed_study(postgres_pool, owner_idx=principal_idx)

    samples = []
    for _ in range(3):
        bs, ps = await seed_biosample_with_sequenced_prep_sample(
            postgres_pool, owner_idx=principal_idx
        )
        samples.append((bs, ps))
    (bs_a, ps_a), (bs_b, ps_b), (bs_c, ps_c) = samples

    async def link(biosample_idx: int, prep_sample_idx: int, study_idx: int) -> None:
        # biosample_to_study first — the prep_sample_to_study
        # reject_without_biosample_link trigger requires it.
        await seed_biosample_to_study_link(
            postgres_pool,
            biosample_idx=biosample_idx,
            study_idx=study_idx,
            created_by_idx=principal_idx,
        )
        await postgres_pool.execute(
            "INSERT INTO qiita.prep_sample_to_study"
            " (prep_sample_idx, study_idx, created_by_idx) VALUES ($1, $2, $3)",
            prep_sample_idx,
            study_idx,
            principal_idx,
        )

    await link(bs_a, ps_a, study_1)
    await link(bs_b, ps_b, study_1)
    await link(bs_b, ps_b, study_2)
    await link(bs_c, ps_c, study_1)
    await retire_prep_sample_to_study_link(
        postgres_pool,
        prep_sample_idx=ps_c,
        study_idx=study_1,
        retired_by_idx=principal_idx,
    )

    yield {
        "pool": postgres_pool,
        "study_1": study_1,
        "study_2": study_2,
        "ps_a": ps_a,
        "ps_b": ps_b,
        "ps_c": ps_c,
    }

    prep_idxs = [ps_a, ps_b, ps_c]
    bio_idxs = [bs_a, bs_b, bs_c]
    studies = [study_1, study_2]
    await postgres_pool.execute(
        "DELETE FROM qiita.prep_sample_to_study WHERE prep_sample_idx = ANY($1::bigint[])",
        prep_idxs,
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.biosample_to_study WHERE biosample_idx = ANY($1::bigint[])",
        bio_idxs,
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.sequenced_sample WHERE prep_sample_idx = ANY($1::bigint[])", prep_idxs
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.prep_sample WHERE idx = ANY($1::bigint[])", prep_idxs
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.biosample WHERE idx = ANY($1::bigint[])", bio_idxs
    )
    await postgres_pool.execute("DELETE FROM qiita.study WHERE idx = ANY($1::bigint[])", studies)
    await postgres_pool.execute("DELETE FROM qiita.user WHERE principal_idx = $1", principal_idx)
    await postgres_pool.execute("DELETE FROM qiita.principal WHERE idx = $1", principal_idx)


async def test_batched_lookup_matches_the_per_sample_one(links):
    """The batched result must equal the single-sample function, sample for
    sample. That equivalence is the entire licence to use the fast path in the
    access gate; if it ever drifts, the gate authorizes against a different set
    of studies than the one a reader would check by hand."""
    pool = links["pool"]
    prep_idxs = [links["ps_a"], links["ps_b"], links["ps_c"]]

    batched = await fetch_active_study_idxs_for_prep_samples(pool, prep_idxs)
    for ps in prep_idxs:
        expected = await fetch_active_study_idxs_for_prep_sample(pool, ps)
        assert sorted(batched.get(ps, [])) == sorted(expected), f"drifted on prep_sample {ps}"


async def test_batched_lookup_returns_every_study_a_sample_spans(links):
    """A prep_sample linked to two studies contributes both.

    This is the pool-spans-studies case the whole access model exists for: the
    caller must hold the tier on EVERY study a sample touches, so dropping one
    would silently authorize a read the caller has no right to.
    """
    batched = await fetch_active_study_idxs_for_prep_samples(links["pool"], [links["ps_b"]])
    assert sorted(batched[links["ps_b"]]) == sorted([links["study_1"], links["study_2"]])


async def test_batched_lookup_omits_retired_links(links):
    """A retired link is not a link. `ps_c`'s only link is retired, so it comes
    back with no studies at all — which the access gate must be able to see, to
    tell 'no studies' apart from 'studies you can read'."""
    batched = await fetch_active_study_idxs_for_prep_samples(
        links["pool"], [links["ps_a"], links["ps_c"]]
    )
    assert batched.get(links["ps_c"], []) == []
    assert batched[links["ps_a"]] == [links["study_1"]]


async def test_batched_lookup_empty_input_returns_empty(links):
    """No query, no round trip — mirrors list_incomplete_alignment_samples."""
    assert await fetch_active_study_idxs_for_prep_samples(links["pool"], []) == {}
