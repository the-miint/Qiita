"""Unit test (no DB, no HTTP) guarding the process-wide ENA import concurrency bound."""

from __future__ import annotations

import inspect

from qiita_control_plane import db
from qiita_control_plane.ena_import.batch import _STUDY_CONCURRENCY


def test_study_concurrency_stays_below_pool_max_size():
    """Each permit holds a pool connection; must stay below db.get_pool's default max_size."""
    pool_max_size = inspect.signature(db.get_pool).parameters["max_size"].default
    assert _STUDY_CONCURRENCY < pool_max_size
