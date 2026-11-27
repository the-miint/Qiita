"""Pure-unit guard for ena_import's concurrency constant."""

from __future__ import annotations

from qiita_control_plane.db import PRODUCTION_POOL_FLOOR
from qiita_control_plane.ena_import.batch import _STUDY_CONCURRENCY


def test_study_concurrency_stays_well_below_pool_max_size():
    """Well below means at most half of `PRODUCTION_POOL_FLOOR`, the smallest pool
    main.py's `get_pool` call builds (`dispatch.production_pool_max_size` only grows
    it)."""
    assert _STUDY_CONCURRENCY * 2 <= PRODUCTION_POOL_FLOOR
