"""Unit tests for the pure shard tiler (qiita_control_plane.shard_planner).

These are pure-unit (no DB): they exercise the lineage-sorted, fixed-N even
partition that turns a reference's sharding units (genomes for a genome
reference) into shards. The ingest-time wiring — feeding the tiler real
lineages, expanding genome→feature, and persisting via write_shard_assignment —
is a later milestone and is covered elsewhere.
"""

import pytest

from qiita_control_plane.shard_planner import (
    LineageItem,
    tile_by_lineage,
)


def _sizes(shards: list[list[int]]) -> list[int]:
    return [len(s) for s in shards]


# ---------------------------------------------------------------------------
# degenerate inputs
# ---------------------------------------------------------------------------


def test_empty_yields_no_shards():
    assert tile_by_lineage([], num_shards=1000) == []


def test_num_shards_must_be_positive():
    with pytest.raises(ValueError):
        tile_by_lineage([LineageItem(1, "d__Bacteria")], num_shards=0)
    with pytest.raises(ValueError):
        tile_by_lineage([LineageItem(1, "d__Bacteria")], num_shards=-5)


def test_single_item_one_shard():
    assert tile_by_lineage([LineageItem(7, "d__Bacteria;p__Bacillota")], num_shards=1000) == [[7]]


def test_fewer_items_than_shards_no_empty_shards():
    # 3 items, N=1000 → effective shard count is min(N, F) = 3, each a single
    # item; NO empty shards are produced.
    items = [LineageItem(3, "c"), LineageItem(1, "a"), LineageItem(2, "b")]
    shards = tile_by_lineage(items, num_shards=1000)
    assert shards == [[1], [2], [3]]
    assert all(shard for shard in shards)


# ---------------------------------------------------------------------------
# even partition
# ---------------------------------------------------------------------------


def test_even_partition_sizes_differ_by_at_most_one():
    # 10 items into 3 shards → contiguous groups whose sizes differ by ≤1, no
    # shard empty, every item present exactly once.
    items = [LineageItem(i, f"lin-{i:02d}") for i in range(10)]
    shards = tile_by_lineage(items, num_shards=3)
    assert len(shards) == 3
    sizes = _sizes(shards)
    assert sum(sizes) == 10
    assert max(sizes) - min(sizes) <= 1
    assert all(shard for shard in shards)
    flat = [item_id for shard in shards for item_id in shard]
    assert sorted(flat) == list(range(10))
    assert len(set(flat)) == 10  # no overlap


def test_shards_are_contiguous_in_lineage_order():
    # Items given out of lineage order → flattened output is the
    # (lineage, item_id)-sorted id sequence (lineage-adjacent units co-locate).
    items = [
        LineageItem(30, "d__Bacteria;p__Pseudomonadota"),
        LineageItem(10, "d__Archaea;p__Euryarchaeota"),
        LineageItem(20, "d__Bacteria;p__Bacillota"),
        LineageItem(40, "d__Bacteria;p__Pseudomonadota"),
    ]
    shards = tile_by_lineage(items, num_shards=2)
    flat = [item_id for shard in shards for item_id in shard]
    # sorted by (lineage, item_id): Archaea(10), Bacillota(20), Pseudomonadota 30 then 40
    assert flat == [10, 20, 30, 40]


def test_stable_tiebreak_within_shared_lineage():
    # Several items sharing one lineage string are ordered by item_id — a
    # deterministic tiebreak so the sort is stable.
    items = [
        LineageItem(9, "d__Bacteria;s__Escherichia coli"),
        LineageItem(2, "d__Bacteria;s__Escherichia coli"),
        LineageItem(5, "d__Bacteria;s__Escherichia coli"),
    ]
    shards = tile_by_lineage(items, num_shards=1)
    assert shards == [[2, 5, 9]]


# ---------------------------------------------------------------------------
# determinism
# ---------------------------------------------------------------------------


def test_tiling_is_order_independent():
    ordered = [LineageItem(i, f"lin-{i:02d}") for i in range(12)]
    shuffled = list(reversed(ordered))
    assert tile_by_lineage(shuffled, num_shards=5) == tile_by_lineage(ordered, num_shards=5)
