"""Pure-unit tests for `align_planner`'s per-platform sizing tables.

The planner's HTTP wiring, mask selection, and 4xx behaviour are covered by the
DB-tier route tests (`tests/routes/test_align_plan.py`). What lives here needs no
database: the two platform→(aligner, block size) maps and their resolvers, which are
plain dicts and would otherwise only be exercised incidentally.

Both maps are keyed on `qiita.platform` values and must stay in step — a platform
that can be aligned needs a block size, and the *reason* it needs one is that the
short-read default is wrong for long reads by more than an order of magnitude.
"""

import pytest

from qiita_control_plane.align_planner import (
    _ALIGNER_BY_PLATFORM,
    _BLOCK_TARGET_READS_BY_PLATFORM,
    _LONG_READ_BLOCK_TARGET_READS,
    AlignUnsupportedPlatform,
    _aligner_for_platform,
    _block_target_for_platform,
)
from qiita_control_plane.block_planner import _BLOCK_TARGET_READS


def test_block_target_covers_every_aligned_platform():
    """Every alignable platform must declare a block size.

    If the two maps drift, `_block_target_for_platform` raises rather than falling
    back to the short-read 10M — but that is a runtime failure on a real plan
    request. This is the check that makes adding a platform force the decision at
    edit time instead."""
    assert set(_BLOCK_TARGET_READS_BY_PLATFORM) == set(_ALIGNER_BY_PLATFORM)


def test_long_read_platforms_get_the_smaller_block_target():
    """The long-read platforms tile at 1M reads, short-read Illumina at 10M.

    Not a cosmetic difference: the sharded aligner re-reads a block's reads once per
    touched shard, so the per-job cost follows BYTES. At ~15 kb/read a 10M-read HiFi
    block is ~150 GB and its re-scan alone exceeds the align step's PT4H baseline, so
    the ticket cannot finish; ~1M reads (~15 GB) brings it to ~27 min. Illumina reads
    are ~100x shorter, so 10M stays right there."""
    assert _block_target_for_platform("pacbio_smrt") == _LONG_READ_BLOCK_TARGET_READS
    assert _block_target_for_platform("oxford_nanopore") == _LONG_READ_BLOCK_TARGET_READS
    assert _block_target_for_platform("illumina") == _BLOCK_TARGET_READS
    # The whole point is that long-read blocks are SMALLER; an equal or larger value
    # would make the two-map split pointless while still looking deliberate.
    assert _LONG_READ_BLOCK_TARGET_READS < _BLOCK_TARGET_READS


def test_long_read_platforms_are_exactly_the_minimap2_ones():
    """The platforms taking the smaller block are exactly the minimap2 ones.

    These are two independent axes (aligner choice vs block size) and the code keeps
    them as separate maps deliberately, so this is an assertion about today's
    platform set rather than a derivation: both distinctions currently track long-vs-
    short reads. If a future platform breaks the correspondence, this test should be
    updated to name it, not deleted — the point is that the divergence gets noticed."""
    small_block = {p for p, t in _BLOCK_TARGET_READS_BY_PLATFORM.items() if t < _BLOCK_TARGET_READS}
    minimap2 = {p for p, a in _ALIGNER_BY_PLATFORM.items() if a == "minimap2"}
    assert small_block == minimap2


def test_block_target_for_unknown_platform_raises():
    """An unmapped platform fails loud. Callers reach `_block_target_for_platform`
    only after `_aligner_for_platform` accepted the platform, so this is the
    map-drift guard — never a silent default to the short-read size."""
    with pytest.raises(AlignUnsupportedPlatform, match="no align block-read target"):
        _block_target_for_platform("ls454")


def test_unsupported_platform_has_no_aligner():
    """The precondition the above relies on: an unalignable platform is rejected at
    the aligner lookup, before any sizing question is asked."""
    with pytest.raises(AlignUnsupportedPlatform, match="no sharded aligner"):
        _aligner_for_platform("ls454")
