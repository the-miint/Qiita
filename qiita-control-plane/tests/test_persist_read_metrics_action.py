"""DB tests for the persist-read-metrics library primitive.

`persist_read_metrics` derives the three per-stage read counts (raw / biological
/ quality-filtered, both-mates r1r2) from the `read_mask` Parquet and writes them
onto the 1:1 sequenced_sample for a prep_sample. It is the in-process action
fastq-to-parquet runs after host_filter, reading the mask host_filter emitted.

Each test seeds its own principal -> biosample -> prep_sample chain (and, where
needed, the sequenced_sample subtype) so cleanup is FK-reverse and order-stable
on the shared postgres_pool fixture.
"""

import secrets
from pathlib import Path

import duckdb
import pytest
import pytest_asyncio
from qiita_common.models import ReadMaskReason

from qiita_control_plane.actions.library import persist_read_metrics
from qiita_control_plane.testing.db_seeds import (
    seed_biosample_with_sequenced_prep_sample,
    seed_sequenced_sample_subtype,
    seed_user_principal,
)

pytestmark = pytest.mark.db


def _write_read_mask(
    path: Path,
    *,
    n_pass: int,
    n_host: int,
    n_qc_fail: int,
    paired: bool = False,
) -> Path:
    """Write a read_mask.parquet with the given per-reason row counts. Single-end
    rows (paired=False) leave the mate trims NULL so each row's r1r2 weight is 1;
    paired rows set them to 0 so each weighs 2 (the COUNT(right_trim2) term)."""
    mate = "0, 0" if paired else "NULL, NULL"
    sidx = 0
    with duckdb.connect(":memory:") as conn:
        conn.execute(
            "CREATE TABLE m(mask_idx BIGINT, prep_sample_idx BIGINT, sequence_idx BIGINT, "
            "reason VARCHAR, left_trim1 UINTEGER, right_trim1 UINTEGER, "
            "left_trim2 UINTEGER, right_trim2 UINTEGER)"
        )
        for reason, n in (
            (ReadMaskReason.PASS.value, n_pass),
            (ReadMaskReason.HOST_RYPE.value, n_host),
            (ReadMaskReason.QC_TOO_SHORT.value, n_qc_fail),
        ):
            for _ in range(n):
                conn.execute(f"INSERT INTO m VALUES (1, 1, ?, '{reason}', 0, 0, {mate})", [sidx])
                sidx += 1
        conn.execute(f"COPY m TO '{path}' (FORMAT PARQUET)")
    return path


@pytest_asyncio.fixture
async def chain(postgres_pool):
    """Seed one principal + biosample + sequenced prep_sample; yield a context
    with a `seed_subtype()` helper that attaches the run -> pool ->
    sequenced_sample subtype on demand and records idxs for FK-reverse cleanup."""
    principal_idx = await seed_user_principal(postgres_pool, prefix="prm-test", suffix="owner")
    biosample_idx, prep_sample_idx = await seed_biosample_with_sequenced_prep_sample(
        postgres_pool, owner_idx=principal_idx
    )
    subtypes: list[tuple[int, int, int]] = []

    async def seed_subtype() -> int:
        run_idx, pool_idx, ss_idx = await seed_sequenced_sample_subtype(
            postgres_pool,
            prep_sample_idx=prep_sample_idx,
            owner_idx=principal_idx,
            sequenced_pool_item_id=f"item-{secrets.token_hex(4)}",
        )
        subtypes.append((run_idx, pool_idx, ss_idx))
        return ss_idx

    yield {
        "pool": postgres_pool,
        "principal_idx": principal_idx,
        "prep_sample_idx": prep_sample_idx,
        "seed_subtype": seed_subtype,
    }

    # FK-reverse cleanup: sequenced_sample -> sequenced_pool -> sequencing_run,
    # then prep_sample (cascades sequence_range), biosample, user, principal.
    for run_idx, pool_idx, ss_idx in subtypes:
        await postgres_pool.execute("DELETE FROM qiita.sequenced_sample WHERE idx = $1", ss_idx)
        await postgres_pool.execute("DELETE FROM qiita.sequenced_pool WHERE idx = $1", pool_idx)
        await postgres_pool.execute("DELETE FROM qiita.sequencing_run WHERE idx = $1", run_idx)
    await postgres_pool.execute("DELETE FROM qiita.prep_sample WHERE idx = $1", prep_sample_idx)
    await postgres_pool.execute("DELETE FROM qiita.biosample WHERE idx = $1", biosample_idx)
    await postgres_pool.execute("DELETE FROM qiita.user WHERE principal_idx = $1", principal_idx)
    await postgres_pool.execute("DELETE FROM qiita.principal WHERE idx = $1", principal_idx)


async def _read_counts(pool, ss_idx):
    return await pool.fetchrow(
        "SELECT raw_read_count_r1r2, biological_read_count_r1r2,"
        " quality_filtered_read_count_r1r2 FROM qiita.sequenced_sample WHERE idx = $1",
        ss_idx,
    )


async def test_persist_writes_three_counts(chain, tmp_path):
    """SE mask: 850 pass + 50 host + 100 qc_too_short -> raw 1000, biological 900
    (pass + host), quality_filtered 850 (pass). r1r2 == row count (single-end)."""
    ss_idx = await chain["seed_subtype"]()
    mask = _write_read_mask(tmp_path / "read_mask.parquet", n_pass=850, n_host=50, n_qc_fail=100)
    returned = await persist_read_metrics(chain["pool"], chain["prep_sample_idx"], mask)
    assert returned == ss_idx
    row = await _read_counts(chain["pool"], ss_idx)
    assert row["raw_read_count_r1r2"] == 1000
    assert row["biological_read_count_r1r2"] == 900
    assert row["quality_filtered_read_count_r1r2"] == 850


async def test_persist_paired_counts_double(chain, tmp_path):
    """Paired-end mask: each row is a pair, so r1r2 = 2 * rows. 400 pass + 50 host
    + 50 qc_fail rows -> raw 1000, biological 900, quality_filtered 800 — the
    COUNT(*) + COUNT(right_trim2) doubling, not a bare COUNT(*)."""
    ss_idx = await chain["seed_subtype"]()
    mask = _write_read_mask(
        tmp_path / "read_mask.parquet", n_pass=400, n_host=50, n_qc_fail=50, paired=True
    )
    await persist_read_metrics(chain["pool"], chain["prep_sample_idx"], mask)
    row = await _read_counts(chain["pool"], ss_idx)
    assert row["raw_read_count_r1r2"] == 1000
    assert row["biological_read_count_r1r2"] == 900
    assert row["quality_filtered_read_count_r1r2"] == 800


def _write_mixed_read_mask(
    path: Path,
    *,
    se_rows: dict[str, int],
    pe_rows: dict[str, int],
) -> Path:
    """Write a read_mask.parquet that MIXES single-end and paired-end rows in one
    file. SE rows leave the mate trims NULL (r1r2 weight 1); PE rows set them to 0
    (r1r2 weight 2). `se_rows`/`pe_rows` map a ReadMaskReason value -> row count.
    The mask writer in production emits one layout per sample, but the
    `_read_mask_counts` SQL is layout-agnostic (COUNT(*) + COUNT(right_trim2)), so
    this asserts the mix is counted correctly without SE/PE branching."""
    sidx = 0
    with duckdb.connect(":memory:") as conn:
        conn.execute(
            "CREATE TABLE m(mask_idx BIGINT, prep_sample_idx BIGINT, sequence_idx BIGINT, "
            "reason VARCHAR, left_trim1 UINTEGER, right_trim1 UINTEGER, "
            "left_trim2 UINTEGER, right_trim2 UINTEGER)"
        )
        for mate, rows in (("NULL, NULL", se_rows), ("0, 0", pe_rows)):
            for reason, n in rows.items():
                for _ in range(n):
                    conn.execute(
                        f"INSERT INTO m VALUES (1, 1, ?, '{reason}', 0, 0, {mate})", [sidx]
                    )
                    sidx += 1
        conn.execute(f"COPY m TO '{path}' (FORMAT PARQUET)")
    return path


async def test_persist_mixed_se_pe_counts(chain, tmp_path):
    """A read_mask mixing SE and PE rows is counted per bucket as
    COUNT(*) + COUNT(right_trim2): SE rows weigh 1, PE rows weigh 2, no branching.

    SE: 100 pass + 20 host + 30 qc_fail (weight 1 each).
    PE: 200 pass + 10 host + 40 qc_fail (weight 2 each).
      raw          = (100+20+30)*1 + (200+10+40)*2 = 150 + 500 = 650
      biological   = (100+20)*1   + (200+10)*2     = 120 + 420 = 540  (pass + host)
      quality_filt = 100*1        + 200*2          = 100 + 400 = 500  (pass only)
    """
    ss_idx = await chain["seed_subtype"]()
    mask = _write_mixed_read_mask(
        tmp_path / "read_mask.parquet",
        se_rows={
            ReadMaskReason.PASS.value: 100,
            ReadMaskReason.HOST_RYPE.value: 20,
            ReadMaskReason.QC_TOO_SHORT.value: 30,
        },
        pe_rows={
            ReadMaskReason.PASS.value: 200,
            ReadMaskReason.HOST_RYPE.value: 10,
            ReadMaskReason.QC_TOO_SHORT.value: 40,
        },
    )
    await persist_read_metrics(chain["pool"], chain["prep_sample_idx"], mask)
    row = await _read_counts(chain["pool"], ss_idx)
    assert row["raw_read_count_r1r2"] == 650
    assert row["biological_read_count_r1r2"] == 540
    assert row["quality_filtered_read_count_r1r2"] == 500


async def test_persist_is_idempotent(chain, tmp_path):
    """A workflow retried from the start re-runs the primitive; the second write
    overwrites with the same counts and returns the same idx."""
    ss_idx = await chain["seed_subtype"]()
    mask = _write_read_mask(tmp_path / "read_mask.parquet", n_pass=850, n_host=50, n_qc_fail=100)
    first = await persist_read_metrics(chain["pool"], chain["prep_sample_idx"], mask)
    second = await persist_read_metrics(chain["pool"], chain["prep_sample_idx"], mask)
    assert first == second == ss_idx
    row = await _read_counts(chain["pool"], ss_idx)
    assert row["quality_filtered_read_count_r1r2"] == 850


async def test_persist_no_host_filter_biological_equals_quality(chain, tmp_path):
    """Host filtering disabled (no host_* rows) -> quality_filtered == biological;
    the monotonic CHECK allows equality."""
    ss_idx = await chain["seed_subtype"]()
    mask = _write_read_mask(tmp_path / "read_mask.parquet", n_pass=800, n_host=0, n_qc_fail=200)
    await persist_read_metrics(chain["pool"], chain["prep_sample_idx"], mask)
    row = await _read_counts(chain["pool"], ss_idx)
    assert row["raw_read_count_r1r2"] == 1000
    assert row["biological_read_count_r1r2"] == row["quality_filtered_read_count_r1r2"] == 800


async def test_persist_fails_fast_without_sequenced_sample(chain, tmp_path):
    """A sequenced prep_sample with no sequenced_sample subtype is an ordering
    bug, not a benign skip — the primitive raises rather than silently no-op'ing."""
    mask = _write_read_mask(tmp_path / "read_mask.parquet", n_pass=850, n_host=50, n_qc_fail=100)
    with pytest.raises(RuntimeError, match="no sequenced_sample row"):
        await persist_read_metrics(chain["pool"], chain["prep_sample_idx"], mask)


async def test_persist_missing_mask_raises(chain, tmp_path):
    """A missing read_mask Parquet is fail-fast."""
    await chain["seed_subtype"]()
    with pytest.raises(FileNotFoundError):
        await persist_read_metrics(
            chain["pool"], chain["prep_sample_idx"], tmp_path / "nope.parquet"
        )
