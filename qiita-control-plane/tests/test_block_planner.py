"""Unit tests for the pure block tiler (qiita_control_plane.block_planner).

These are pure-unit (no DB): they exercise the tiling arithmetic that turns a
mask-partition's per-sample sequence_idx ranges into ≤target-read blocks. The
server-side orchestration (plan_and_submit_blocks) is covered by the DB-tier
tests in tests/test_block_plan_*.py.
"""

import pytest

from qiita_control_plane.block_planner import (
    BlockMember,
    SampleRange,
    tile_partition,
)


def _block_count(block: list[BlockMember]) -> int:
    """Total reads a block covers = sum of its members' inclusive ranges."""
    return sum(m.max_sequence_idx - m.min_sequence_idx + 1 for m in block)


def _assert_sample_fully_covered(blocks, sample: SampleRange):
    """Every read in the sample's [start, stop] is covered exactly once across
    all blocks — no gap, no overlap — and the sub-ranges are contiguous in
    order (the reconcile count-assertion + exact export selector rely on this)."""
    pieces = sorted(
        (m.min_sequence_idx, m.max_sequence_idx)
        for block in blocks
        for m in block
        if m.prep_sample_idx == sample.prep_sample_idx
    )
    assert pieces, f"sample {sample.prep_sample_idx} has no members"
    assert pieces[0][0] == sample.sequence_idx_start
    assert pieces[-1][1] == sample.sequence_idx_stop
    # Contiguous, non-overlapping: each piece starts one past the previous end.
    for (lo, hi), (nlo, _) in zip(pieces, pieces[1:]):
        assert lo <= hi
        assert nlo == hi + 1, f"gap/overlap in {sample.prep_sample_idx}: {pieces}"


# ---------------------------------------------------------------------------
# degenerate inputs
# ---------------------------------------------------------------------------


def test_empty_partition_yields_no_blocks():
    assert tile_partition([], target_reads=100) == []


def test_target_must_be_positive():
    with pytest.raises(ValueError):
        tile_partition([SampleRange(1, 0, 9)], target_reads=0)
    with pytest.raises(ValueError):
        tile_partition([SampleRange(1, 0, 9)], target_reads=-5)


def test_inverted_range_raises():
    with pytest.raises(ValueError):
        tile_partition([SampleRange(1, 10, 5)], target_reads=100)


# ---------------------------------------------------------------------------
# single sample
# ---------------------------------------------------------------------------


def test_single_small_sample_one_block_one_member():
    # 10 reads, target 100 → one block, one member covering the whole range.
    blocks = tile_partition([SampleRange(7, 0, 9)], target_reads=100)
    assert blocks == [[BlockMember(7, 0, 9)]]


def test_single_sample_exactly_target_one_block():
    blocks = tile_partition([SampleRange(7, 0, 99)], target_reads=100)
    assert blocks == [[BlockMember(7, 0, 99)]]
    assert _block_count(blocks[0]) == 100


def test_single_oversized_sample_splits_on_boundaries():
    # 250 reads, target 100 → 3 blocks: [0..99], [100..199], [200..249].
    sample = SampleRange(7, 0, 249)
    blocks = tile_partition([sample], target_reads=100)
    assert len(blocks) == 3
    assert blocks[0] == [BlockMember(7, 0, 99)]
    assert blocks[1] == [BlockMember(7, 100, 199)]
    assert blocks[2] == [BlockMember(7, 200, 249)]
    assert [_block_count(b) for b in blocks] == [100, 100, 50]
    _assert_sample_fully_covered(blocks, sample)


def test_single_sample_nonzero_start_split():
    # Start offset must be preserved in the sub-range bounds.
    sample = SampleRange(7, 1000, 1249)
    blocks = tile_partition([sample], target_reads=100)
    assert blocks[0] == [BlockMember(7, 1000, 1099)]
    assert blocks[-1] == [BlockMember(7, 1200, 1249)]
    _assert_sample_fully_covered(blocks, sample)


# ---------------------------------------------------------------------------
# multiple samples: packing + straddling split
# ---------------------------------------------------------------------------


def test_small_samples_pack_into_one_block():
    # Three 20-read samples, target 100 → all in one block, three members.
    samples = [SampleRange(1, 0, 19), SampleRange(2, 20, 39), SampleRange(3, 40, 59)]
    blocks = tile_partition(samples, target_reads=100)
    assert len(blocks) == 1
    assert blocks[0] == [BlockMember(1, 0, 19), BlockMember(2, 20, 39), BlockMember(3, 40, 59)]
    assert _block_count(blocks[0]) == 60


def test_sample_straddling_boundary_is_split_across_blocks():
    # Sample 1: 60 reads fills to 60; sample 2: 80 reads — 40 finish block 1
    # (to target 100), remaining 40 open block 2. Sample 2 is split.
    s1 = SampleRange(1, 0, 59)
    s2 = SampleRange(2, 60, 139)
    blocks = tile_partition([s1, s2], target_reads=100)
    assert len(blocks) == 2
    assert blocks[0] == [BlockMember(1, 0, 59), BlockMember(2, 60, 99)]
    assert blocks[1] == [BlockMember(2, 100, 139)]
    assert _block_count(blocks[0]) == 100
    assert _block_count(blocks[1]) == 40
    _assert_sample_fully_covered(blocks, s2)


def test_every_block_but_last_is_exactly_target():
    # A mix of sizes summing to 350, target 100 → blocks of 100,100,100,50.
    samples = [
        SampleRange(1, 0, 149),  # 150
        SampleRange(2, 150, 249),  # 100
        SampleRange(3, 250, 349),  # 100
    ]
    blocks = tile_partition(samples, target_reads=100)
    counts = [_block_count(b) for b in blocks]
    assert counts == [100, 100, 100, 50]
    for s in samples:
        _assert_sample_fully_covered(blocks, s)


def test_no_block_exceeds_target():
    samples = [SampleRange(i, i * 37, i * 37 + 36) for i in range(1, 20)]  # 19×37 reads
    blocks = tile_partition(samples, target_reads=100)
    assert all(_block_count(b) <= 100 for b in blocks)


# ---------------------------------------------------------------------------
# determinism
# ---------------------------------------------------------------------------


def test_tiling_is_order_independent():
    ordered = [SampleRange(1, 0, 59), SampleRange(2, 60, 139), SampleRange(3, 140, 199)]
    shuffled = [ordered[2], ordered[0], ordered[1]]
    assert tile_partition(shuffled, target_reads=100) == tile_partition(ordered, target_reads=100)
