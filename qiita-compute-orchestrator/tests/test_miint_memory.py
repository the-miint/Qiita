"""Unit tests for the SLURM-allocation-aware DuckDB memory sizing helpers.

These are pure-Python (no DuckDB / no staged extension), so they run in the
fast ``make test`` tier. They pin the behaviour that makes the per-run
``--mem-gb`` override (#102) actually reach a job's in-process memory caps:
``SLURM_MEM_PER_NODE`` (the real cgroup) wins over the YAML-baseline literal
under SLURM, while off SLURM the literal fallback keeps the local backend /
tests unchanged.
"""

from __future__ import annotations

import pytest

from qiita_compute_orchestrator.miint import (
    duckdb_headroom_gb,
    resolve_duckdb_memory_gb,
    slurm_alloc_gb,
)


@pytest.fixture(autouse=True)
def _clear_slurm_mem(monkeypatch):
    """Default every test to the off-SLURM state; tests opt in explicitly."""
    monkeypatch.delenv("SLURM_MEM_PER_NODE", raising=False)


class TestSlurmAllocGb:
    def test_absent_is_none(self):
        assert slurm_alloc_gb() is None

    def test_empty_is_none(self, monkeypatch):
        monkeypatch.setenv("SLURM_MEM_PER_NODE", "")
        assert slurm_alloc_gb() is None

    def test_malformed_fails_soft_to_none(self, monkeypatch):
        monkeypatch.setenv("SLURM_MEM_PER_NODE", "48G")  # MB are bare ints; suffix is bad
        assert slurm_alloc_gb() is None

    def test_mb_converted_to_gb(self, monkeypatch):
        monkeypatch.setenv("SLURM_MEM_PER_NODE", str(48 * 1024))
        assert slurm_alloc_gb() == 48

    def test_floor_division(self, monkeypatch):
        # 8703 MB → 8 GB (floor), never rounds up past the real allocation.
        monkeypatch.setenv("SLURM_MEM_PER_NODE", "8703")
        assert slurm_alloc_gb() == 8


class TestDuckdbHeadroomGb:
    def test_base_plus_per_thread(self):
        # 2 base + ceil(0.5 * threads): 4-thread step → 4, 8-thread `load` → 6.
        assert duckdb_headroom_gb(4) == 4
        assert duckdb_headroom_gb(8) == 6

    def test_per_thread_rounds_up(self):
        # 1 thread → 2 + ceil(0.5) = 3; never under-reserves the fractional term.
        assert duckdb_headroom_gb(1) == 3


class TestResolveDuckdbMemoryGb:
    def test_off_slurm_uses_fallback(self):
        assert resolve_duckdb_memory_gb(7, threads=4) == 7

    def test_off_slurm_fallback_respects_cap(self):
        # A co-consumer job's fallback bounded by its cap (e.g. minimap2 box).
        assert resolve_duckdb_memory_gb(8, threads=4, cap_gb=8) == 8

    def test_under_slurm_tracks_cgroup_minus_headroom(self, monkeypatch):
        monkeypatch.setenv("SLURM_MEM_PER_NODE", str(48 * 1024))
        # 48 - 4 (4-thread headroom) = 44; this is the --mem-gb 48 → DuckDB path
        # that the old fixed 7 GB literal blocked.
        assert resolve_duckdb_memory_gb(7, threads=4) == 44

    def test_headroom_scales_with_threads(self, monkeypatch):
        # The 8-thread `load` step reserves more headroom than a 4-thread step.
        monkeypatch.setenv("SLURM_MEM_PER_NODE", str(48 * 1024))
        assert resolve_duckdb_memory_gb(31, threads=8) == 42  # 48 - 6

    def test_override_beats_smaller_literal(self, monkeypatch):
        # stage_local_fasta's 7 GB literal must NOT cap a 48 GB allocation.
        monkeypatch.setenv("SLURM_MEM_PER_NODE", str(48 * 1024))
        assert resolve_duckdb_memory_gb(7, threads=4) == 44

    def test_cap_bounds_a_co_consumer_share(self, monkeypatch):
        # build_rype_index holds DuckDB at its small fallback even on a big box.
        monkeypatch.setenv("SLURM_MEM_PER_NODE", str(48 * 1024))
        assert resolve_duckdb_memory_gb(4, threads=4, cap_gb=4) == 4

    def test_reserve_carves_out_co_consumer(self, monkeypatch):
        # build_minimap2_index: 48 - 4 headroom - 16 minimap2 reserve = 28.
        monkeypatch.setenv("SLURM_MEM_PER_NODE", str(48 * 1024))
        assert resolve_duckdb_memory_gb(8, threads=4, reserve_gb=16) == 28

    def test_never_below_one(self, monkeypatch):
        # A tiny cgroup minus headroom/reserve must still yield a usable ≥1 GB.
        monkeypatch.setenv("SLURM_MEM_PER_NODE", str(2 * 1024))
        assert resolve_duckdb_memory_gb(8, threads=4, reserve_gb=16) == 1
