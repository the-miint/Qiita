"""Tests for the persist-syndna-read-count library primitive.

`syndna_read_counts` reduces the read-mask `syndna` step's alignment output to reads
per insert; `persist_syndna_read_count` writes one row per insert of the mask's SynDNA
reference, zeros included. The alignment fixture is written in the column layout
`jobs/syndna.py` emits.
"""

import json
import uuid
from pathlib import Path

import duckdb
import pytest
import pytest_asyncio

from qiita_control_plane.actions.library import persist_syndna_read_count, syndna_read_counts
from qiita_control_plane.testing.db_seeds import (
    cleanup_reference_graph,
    seed_bare_feature,
    seed_bare_reference,
    seed_biosample_with_sequenced_prep_sample,
    seed_reference_membership,
    seed_user_principal,
)


def _write_alignment(path: Path, rows: list[tuple[int, int, int]]) -> Path:
    """`rows` are `(prep_sample_idx, sequence_idx, parent_feature_idx)`."""
    with duckdb.connect(":memory:") as conn:
        conn.execute(
            "CREATE TABLE a (prep_sample_idx BIGINT, sequence_idx BIGINT,"
            " parent_feature_idx BIGINT, flags USMALLINT, position BIGINT,"
            " stop_position BIGINT, cigar VARCHAR)"
        )
        for ps, seq, feature in rows:
            conn.execute("INSERT INTO a VALUES (?, ?, ?, 0, 1, 100, '99M')", [ps, seq, feature])
        conn.execute(f"COPY a TO '{path}' (FORMAT PARQUET)")
    return path


def test_counts_distinct_reads_per_insert(tmp_path):
    path = _write_alignment(
        tmp_path / "a.parquet",
        [(9, 1, 100), (9, 2, 100), (9, 2, 100), (9, 3, 200)],
    )
    assert syndna_read_counts(path, prep_sample_idx=9) == {100: 2, 200: 1}


def test_an_empty_alignment_counts_nothing(tmp_path):
    path = _write_alignment(tmp_path / "a.parquet", [])
    assert syndna_read_counts(path, prep_sample_idx=9) == {}


def test_a_row_for_another_sample_is_refused(tmp_path):
    path = _write_alignment(tmp_path / "a.parquet", [(9, 1, 100), (8, 2, 100)])
    with pytest.raises(ValueError, match=r"prep_sample_idx \[8\]"):
        syndna_read_counts(path, prep_sample_idx=9)


@pytest_asyncio.fixture
async def seeded(postgres_pool):
    principal_idx = await seed_user_principal(postgres_pool, prefix="syndna", suffix="owner")
    biosample_idx, prep_sample_idx = await seed_biosample_with_sequenced_prep_sample(
        postgres_pool, owner_idx=principal_idx
    )
    reference_idx = await seed_bare_reference(postgres_pool, label="syndna-persist")
    inserts = [await seed_bare_feature(postgres_pool) for _ in range(3)]
    for feature_idx in inserts:
        await seed_reference_membership(
            postgres_pool, reference_idx=reference_idx, feature_idx=feature_idx
        )
    masks = {}
    for name, syndna in (("syndna", {"reference_idx": reference_idx}), ("plain", None)):
        masks[name] = await postgres_pool.fetchval(
            "INSERT INTO qiita.mask_definition"
            " (params_hash, filter_workflow, filter_version, params, created_by_idx)"
            " VALUES ($1, 'read-mask', '1.0.0', $2::jsonb, $3) RETURNING mask_idx",
            uuid.uuid4().bytes + uuid.uuid4().bytes,
            json.dumps({"resolved_syndna": syndna}),
            principal_idx,
        )
    yield {
        "prep_sample_idx": prep_sample_idx,
        "inserts": inserts,
        "masks": masks,
    }
    await postgres_pool.execute(
        "DELETE FROM qiita.mask_definition WHERE mask_idx = ANY($1::bigint[])",
        list(masks.values()),
    )
    await cleanup_reference_graph(postgres_pool, reference_idx=reference_idx, feature_idxs=inserts)
    await postgres_pool.execute("DELETE FROM qiita.prep_sample WHERE idx = $1", prep_sample_idx)
    await postgres_pool.execute("DELETE FROM qiita.biosample WHERE idx = $1", biosample_idx)


async def _stored(pool, mask_idx, prep_sample_idx) -> dict[int, int]:
    rows = await pool.fetch(
        "SELECT feature_idx, read_count FROM qiita.syndna_read_count"
        " WHERE mask_idx = $1 AND prep_sample_idx = $2",
        mask_idx,
        prep_sample_idx,
    )
    return {r["feature_idx"]: r["read_count"] for r in rows}


@pytest.mark.db
async def test_writes_every_insert_zero_filled_and_replaces_on_rerun(
    postgres_pool, seeded, tmp_path
):
    ps, (f1, f2, f3) = seeded["prep_sample_idx"], seeded["inserts"]
    mask_idx = seeded["masks"]["syndna"]
    first = _write_alignment(tmp_path / "1.parquet", [(ps, 1, f1), (ps, 2, f1), (ps, 3, f3)])

    written = await persist_syndna_read_count(
        postgres_pool, mask_idx=mask_idx, prep_sample_idx=ps, alignment_path=first
    )

    assert written == 3
    assert await _stored(postgres_pool, mask_idx, ps) == {f1: 2, f2: 0, f3: 1}

    second = _write_alignment(tmp_path / "2.parquet", [(ps, 1, f2)])
    await persist_syndna_read_count(
        postgres_pool, mask_idx=mask_idx, prep_sample_idx=ps, alignment_path=second
    )
    assert await _stored(postgres_pool, mask_idx, ps) == {f1: 0, f2: 1, f3: 0}


@pytest.mark.db
async def test_a_mask_without_syndna_is_refused(postgres_pool, seeded, tmp_path):
    ps = seeded["prep_sample_idx"]
    path = _write_alignment(tmp_path / "a.parquet", [(ps, 1, seeded["inserts"][0])])
    with pytest.raises(ValueError, match="no SynDNA reference"):
        await persist_syndna_read_count(
            postgres_pool,
            mask_idx=seeded["masks"]["plain"],
            prep_sample_idx=ps,
            alignment_path=path,
        )


@pytest.mark.db
async def test_a_feature_outside_the_reference_writes_nothing(postgres_pool, seeded, tmp_path):
    ps, mask_idx = seeded["prep_sample_idx"], seeded["masks"]["syndna"]
    stray = seeded["inserts"][-1] + 10**9
    path = _write_alignment(tmp_path / "a.parquet", [(ps, 1, seeded["inserts"][0]), (ps, 2, stray)])
    with pytest.raises(ValueError, match="not members"):
        await persist_syndna_read_count(
            postgres_pool, mask_idx=mask_idx, prep_sample_idx=ps, alignment_path=path
        )
    assert await _stored(postgres_pool, mask_idx, ps) == {}


@pytest.mark.db
async def test_a_missing_file_is_refused(postgres_pool, seeded, tmp_path):
    with pytest.raises(FileNotFoundError):
        await persist_syndna_read_count(
            postgres_pool,
            mask_idx=seeded["masks"]["syndna"],
            prep_sample_idx=seeded["prep_sample_idx"],
            alignment_path=tmp_path / "absent.parquet",
        )
