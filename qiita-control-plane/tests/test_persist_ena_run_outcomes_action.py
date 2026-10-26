"""DB tests for the persist-ena-run-outcomes library primitive."""

import json
import secrets

import asyncpg
import pytest
import pytest_asyncio
from qiita_common.models import FailureType

from qiita_control_plane.actions.library import persist_ena_run_outcomes
from qiita_control_plane.testing.db_seeds import (
    seed_biosample_with_sequenced_prep_sample,
    seed_sequenced_sample_subtype,
    seed_user_principal,
)
from qiita_control_plane.testing.db_teardown import (
    delete_idxs,
    delete_principal,
    teardown_entity_graph,
)

pytestmark = pytest.mark.db


@pytest_asyncio.fixture
async def pools(postgres_pool):
    """Two pools of two runs each. Yields `(pool, members, pool_idxs)`, where
    `members[p]` lists pool p's `(prep_sample_idx, run_accession)` pairs."""
    owner = await seed_user_principal(postgres_pool, prefix="perO-test", suffix="owner")
    biosample_idxs, prep_sample_idxs, run_idxs, pool_idxs = [], [], [], []
    members: list[list[tuple[int, str]]] = []
    for _ in range(2):
        run_idx = pool_idx = None
        pool_members = []
        for _ in range(2):
            biosample_idx, prep_sample_idx = await seed_biosample_with_sequenced_prep_sample(
                postgres_pool, owner_idx=owner
            )
            run_idx, pool_idx, ss_idx = await seed_sequenced_sample_subtype(
                postgres_pool,
                prep_sample_idx=prep_sample_idx,
                owner_idx=owner,
                sequenced_pool_item_id=f"item-{secrets.token_hex(4)}",
                sequencing_run_idx=run_idx,
                sequenced_pool_idx=pool_idx,
            )
            accession = f"ERR{secrets.token_hex(5)}"
            await postgres_pool.execute(
                "UPDATE qiita.sequenced_sample SET ena_run_accession = $2 WHERE idx = $1",
                ss_idx,
                accession,
            )
            biosample_idxs.append(biosample_idx)
            prep_sample_idxs.append(prep_sample_idx)
            pool_members.append((prep_sample_idx, accession))
        run_idxs.append(run_idx)
        pool_idxs.append(pool_idx)
        members.append(pool_members)

    yield postgres_pool, members, pool_idxs

    await teardown_entity_graph(
        postgres_pool,
        study_idxs=[],
        biosample_idxs=biosample_idxs,
        prep_sample_idxs=prep_sample_idxs,
    )
    for pool_idx in pool_idxs:
        await delete_idxs(postgres_pool, "sequenced_pool", pool_idx)
    for run_idx in run_idxs:
        await delete_idxs(postgres_pool, "sequencing_run", run_idx)
    await delete_principal(postgres_pool, [owner])


def _outcomes_file(tmp_path, entries: list[tuple[int, str, str]]):
    path = tmp_path / "run_outcomes.json"
    path.write_text(
        json.dumps(
            [
                {
                    "prep_sample_idx": psi,
                    "ena_run_accession": acc,
                    "failure_reason": reason,
                    "failure_type": FailureType.PERMANENT.value,
                }
                for psi, acc, reason in entries
            ]
        )
    )
    return path


async def _failure(pool, prep_sample_idx):
    row = await pool.fetchrow(
        "SELECT download_failure_reason, download_failure_type"
        " FROM qiita.sequenced_sample WHERE prep_sample_idx = $1",
        prep_sample_idx,
    )
    return row["download_failure_reason"], row["download_failure_type"]


async def test_records_reason_and_failure_type_on_each_listed_run(pools, tmp_path):
    pool, members, pool_idxs = pools
    (bad_psi, bad_acc), (ok_psi, _) = members[0]
    path = _outcomes_file(tmp_path, [(bad_psi, bad_acc, "no sequence data found")])

    await persist_ena_run_outcomes(pool, pool_idxs[0], path)

    assert await _failure(pool, bad_psi) == ("no sequence data found", FailureType.PERMANENT)
    assert await _failure(pool, ok_psi) == (None, None)


async def test_empty_list_writes_nothing(pools, tmp_path):
    pool, members, pool_idxs = pools
    path = _outcomes_file(tmp_path, [])

    await persist_ena_run_outcomes(pool, pool_idxs[0], path)

    for psi, _ in members[0]:
        assert await _failure(pool, psi) == (None, None)


async def test_run_outside_the_pool_fails_loud_and_writes_nothing(pools, tmp_path):
    pool, members, pool_idxs = pools
    (own_psi, own_acc), _ = members[0]
    stranger_psi, stranger_acc = members[1][0]
    path = _outcomes_file(
        tmp_path, [(own_psi, own_acc, "boom"), (stranger_psi, stranger_acc, "boom")]
    )

    with pytest.raises(RuntimeError, match=str(stranger_psi)):
        await persist_ena_run_outcomes(pool, pool_idxs[0], path)

    assert await _failure(pool, own_psi) == (None, None)
    assert await _failure(pool, stranger_psi) == (None, None)


async def test_accession_that_does_not_match_the_prep_sample_fails_loud(pools, tmp_path):
    pool, members, pool_idxs = pools
    (psi, _), (_, other_acc) = members[0]
    path = _outcomes_file(tmp_path, [(psi, other_acc, "boom")])

    with pytest.raises(RuntimeError, match=other_acc):
        await persist_ena_run_outcomes(pool, pool_idxs[0], path)

    assert await _failure(pool, psi) == (None, None)


async def test_missing_outcomes_file_fails_loud(pools, tmp_path):
    pool, _, pool_idxs = pools
    with pytest.raises(FileNotFoundError):
        await persist_ena_run_outcomes(pool, pool_idxs[0], tmp_path / "absent.json")


@pytest.mark.parametrize(
    ("column", "value"),
    [("download_failure_reason", "x"), ("download_failure_type", FailureType.PERMANENT.value)],
)
async def test_check_rejects_a_row_with_only_one_failure_column(pools, column, value):
    pool, members, _ = pools
    psi, _ = members[0][0]
    with pytest.raises(asyncpg.CheckViolationError, match="sequenced_sample_download_failure_pair"):
        await pool.execute(
            f"UPDATE qiita.sequenced_sample SET {column} = $2 WHERE prep_sample_idx = $1",
            psi,
            value,
        )
