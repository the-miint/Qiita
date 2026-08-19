"""Shard planner / tiler for per-shard reference indexes.

An analysis reference's features are partitioned into shards (one `.ryxdi` per
shard) so reads can later be routed to — and aligned against — only the shard(s)
they classify into. This module holds the PURE tiler (`tile_by_lineage`,
unit-testable with no DB). The ingest-time wiring — building lineage strings
from taxonomy, choosing the sharding unit, expanding it back to features, and
persisting via `write_shard_assignment` — is a later milestone.

Sharding strategy: sort the sharding units lexicographically by their taxonomy
**lineage string**, then cut the sorted list into a fixed `_SHARD_COUNT` shards
of approximately even size. This is the mirror image of the read-block planner
(`block_planner`, fixed target *size* / variable *count*): here the *count* is
fixed and the size varies. Sorting by lineage keeps taxonomically-adjacent units
in the same or neighbouring shards, which is what makes classification-based
routing tractable.

The tiler is deliberately GENERIC over what a "unit" is — it tiles opaque
`(item_id, lineage)` records. The caller chooses `item_id = genome_idx` for a
genome reference (so a genome's contigs all inherit one shard and stay together,
and shards balance by *genome* count); whether 16S / no-genome references are
sharded at all, and how a unit expands back to `feature_idx` rows, is the
caller's concern, not the tiler's.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import NamedTuple

# Number of shards per analysis reference — one `.ryxdi` index each. Unlike the
# read-block planner this is a fixed COUNT, not a target size: the lineage-sorted
# units are cut into this many approximately-even groups. Tunable; the tiler is
# exact for any positive value, and callers may override it for tests.
_SHARD_COUNT = 1000


class LineageItem(NamedTuple):
    """One sharding unit and its taxonomy lineage sort key. `item_id` is a
    `genome_idx` for a genome reference (all of a genome's features inherit its
    shard); the tiler is agnostic to that choice. `lineage` is the string the
    units are sorted by (e.g. the semicolon-joined taxonomy ranks)."""

    item_id: int
    lineage: str


def tile_by_lineage(
    items: Sequence[LineageItem],
    num_shards: int = _SHARD_COUNT,
) -> list[list[int]]:
    """Partition `items` into at most `num_shards` shards, lineage-sorted.

    The units are sorted by `(lineage, item_id)` and the sorted sequence is cut
    into `num_shards` contiguous groups of approximately equal size (sizes differ
    by at most one). Because the cut is positional over the lineage-sorted tape,
    taxonomically-adjacent units land in the same or neighbouring shards.

    Deterministic and re-derivable from the same `items` + `num_shards`: the
    tiler sorts internally (with `item_id` as a stable tiebreak for units sharing
    a lineage), so the caller's input order does not matter — the tiler OWNS the
    tiling determinism.

    Returns a list of shards, each a non-empty list of `item_id`; `shard_id` is
    the shard's index in the returned list. An empty `items` yields `[]`. A
    reference with fewer units than `num_shards` yields exactly that many
    single-unit shards (`min(num_shards, len(items))`) — never an empty shard.
    Raises ValueError on a non-positive `num_shards`.
    """
    if num_shards <= 0:
        raise ValueError(f"num_shards must be positive, got {num_shards}")

    ordered = sorted(items, key=lambda it: (it.lineage, it.item_id))
    total = len(ordered)
    if total == 0:
        return []

    effective = min(num_shards, total)
    # Even positional cut: shard k spans [k*total//effective, (k+1)*total//effective).
    # Consecutive shards differ in size by at most one, and every unit lands in
    # exactly one shard with no gap or overlap.
    return [
        [it.item_id for it in ordered[k * total // effective : (k + 1) * total // effective]]
        for k in range(effective)
    ]
