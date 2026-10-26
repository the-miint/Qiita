"""Unit tests for the SLURM-allocation-aware DuckDB memory sizing helpers.

These are pure-Python apart from the last one, which opens an in-memory DuckDB
to pin its out-of-memory message; none needs the staged extension, so they all
run in the fast ``make test`` tier. They pin the behaviour that makes the per-run
``--mem-gb`` override actually reach a job's in-process memory caps:
``SLURM_MEM_PER_NODE`` (the real cgroup) wins over the YAML-baseline literal
under SLURM, while off SLURM the literal is a ceiling bounded by detected
host RAM so DuckDB is never handed more than the machine has.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from qiita_compute_orchestrator.miint import (
    detected_ram_gb,
    duckdb_headroom_gb,
    resolve_duckdb_memory_gb,
    slurm_alloc_gb,
)

_MIINT = "qiita_compute_orchestrator.miint"


@pytest.fixture(autouse=True)
def _clear_slurm_mem(monkeypatch):
    """Default every test to the off-SLURM state on a host big enough to back
    any literal; tests opt in explicitly (the SLURM var, or a smaller
    detected RAM) — otherwise results would track the test machine."""
    monkeypatch.delenv("SLURM_MEM_PER_NODE", raising=False)
    monkeypatch.setattr(f"{_MIINT}.detected_ram_gb", lambda: 128)


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


class TestDetectedRamGb:
    """Detection walks THIS process's cgroup (from /proc/self/cgroup) plus its
    ancestors and takes the tightest limit; sysconf is the fallback."""

    @pytest.fixture
    def cgroup(self, monkeypatch, tmp_path):
        """Fake the cgroup mounts under tmp_path. Returns the /proc/self/cgroup
        stand-in for the test to write; sysconf is pinned to a 64 GB host."""
        proc = tmp_path / "proc_self_cgroup"
        monkeypatch.setattr(f"{_MIINT}._PROC_SELF_CGROUP", proc)
        monkeypatch.setattr(f"{_MIINT}._CGROUP_V2_MOUNT", tmp_path / "sysfs")
        monkeypatch.setattr(f"{_MIINT}._CGROUP_V1_MOUNT", tmp_path / "sysfs_memory")
        monkeypatch.setattr(f"{_MIINT}._sysconf_ram_gb", lambda: 64)
        return proc

    @staticmethod
    def _write(path: Path, text: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)

    def test_root_cgroup_limit_below_host_wins(self, cgroup, tmp_path):
        # A container capped at 4 GB on a 64 GB host: the cgroup is the ceiling.
        cgroup.write_text("0::/\n")
        self._write(tmp_path / "sysfs" / "memory.max", str(4 * 1024**3))
        assert detected_ram_gb() == 4

    def test_own_cgroup_below_the_namespace_root_is_found(self, cgroup, tmp_path):
        # Docker on the HOST cgroup namespace: /proc/self/cgroup is
        # 0::/docker/<id> and no limit file exists at the mount root — reading
        # only the root missed this 4 GB limit and reported the 64 GB host.
        cgroup.write_text("0::/docker/abc\n")
        self._write(tmp_path / "sysfs" / "docker" / "abc" / "memory.max", str(4 * 1024**3))
        assert detected_ram_gb() == 4

    def test_tightest_ancestor_wins_not_the_first_read(self, cgroup, tmp_path):
        # The effective limit is the MINIMUM along the path: an ancestor
        # (4 GB) can be tighter than the process's own cgroup (8 GB).
        cgroup.write_text("0::/docker/abc\n")
        self._write(tmp_path / "sysfs" / "docker" / "abc" / "memory.max", str(8 * 1024**3))
        self._write(tmp_path / "sysfs" / "docker" / "memory.max", str(4 * 1024**3))
        assert detected_ram_gb() == 4

    def test_unlimited_own_cgroup_falls_to_ancestor(self, cgroup, tmp_path):
        # cgroup v2's "max" is not a number — the next-tightest ancestor counts.
        cgroup.write_text("0::/docker/abc\n")
        self._write(tmp_path / "sysfs" / "docker" / "abc" / "memory.max", "max")
        self._write(tmp_path / "sysfs" / "docker" / "memory.max", str(6 * 1024**3))
        assert detected_ram_gb() == 6

    def test_unlimited_everywhere_falls_through_to_host(self, cgroup, tmp_path):
        cgroup.write_text("0::/\n")
        self._write(tmp_path / "sysfs" / "memory.max", "max")
        assert detected_ram_gb() == 64

    def test_v1_unlimited_sentinel_ignored(self, cgroup, tmp_path):
        # cgroup v1 reports ~2^63 for "no limit"; treating it as a ceiling
        # would hand DuckDB an absurd number instead of the host total.
        cgroup.write_text("3:memory:/\n")
        self._write(tmp_path / "sysfs_memory" / "memory.limit_in_bytes", "9223372036854771712")
        assert detected_ram_gb() == 64

    def test_v1_limit_below_host_wins(self, cgroup, tmp_path):
        cgroup.write_text("3:memory:/\n")
        self._write(tmp_path / "sysfs_memory" / "memory.limit_in_bytes", str(4 * 1024**3))
        assert detected_ram_gb() == 4

    def test_proc_unreadable_uses_host(self, cgroup, tmp_path):
        # /proc stand-in never written → no candidates → sysconf.
        assert detected_ram_gb() == 64

    def test_undetectable_is_none(self, monkeypatch, tmp_path):
        # Nothing readable anywhere → None, so callers keep their literal.
        monkeypatch.setattr(f"{_MIINT}._PROC_SELF_CGROUP", tmp_path / "absent")
        monkeypatch.setattr(f"{_MIINT}._sysconf_ram_gb", lambda: None)
        assert detected_ram_gb() is None

    def test_sub_gb_cgroup_floors_to_one(self, cgroup, tmp_path):
        # Integral GB with the same ≥1 floor resolve applies.
        cgroup.write_text("0::/\n")
        self._write(tmp_path / "sysfs" / "memory.max", str(512 * 1024**2))
        assert detected_ram_gb() == 1


class TestResolveDuckdbMemoryGb:
    def test_off_slurm_uses_fallback(self):
        assert resolve_duckdb_memory_gb(7, threads=4) == 7

    def test_off_slurm_fallback_respects_cap(self):
        # A co-consumer job's fallback bounded by its cap (e.g. minimap2 box).
        assert resolve_duckdb_memory_gb(8, threads=4, cap_gb=8) == 8

    def test_off_slurm_caps_at_detected_ram_minus_headroom(self, monkeypatch):
        # The issue's shape: 32 GB host, `load`'s 31 GB literal, 8 threads →
        # min(31, 32 - 6) = 26, leaving the rest of the machine its room.
        monkeypatch.setattr(f"{_MIINT}.detected_ram_gb", lambda: 32)
        assert resolve_duckdb_memory_gb(31, threads=8) == 26

    def test_off_slurm_roomy_host_keeps_fallback(self, monkeypatch):
        # At or above fallback + headroom the literal is the intended size.
        monkeypatch.setattr(f"{_MIINT}.detected_ram_gb", lambda: 128)
        assert resolve_duckdb_memory_gb(31, threads=8) == 31

    def test_off_slurm_detection_failure_keeps_fallback(self, monkeypatch):
        monkeypatch.setattr(f"{_MIINT}.detected_ram_gb", lambda: None)
        assert resolve_duckdb_memory_gb(31, threads=8) == 31

    def test_off_slurm_never_below_one(self, monkeypatch):
        # 4 GB host minus an 8-thread headroom still floors to a usable 1.
        monkeypatch.setattr(f"{_MIINT}.detected_ram_gb", lambda: 4)
        assert resolve_duckdb_memory_gb(31, threads=8) == 1

    def test_off_slurm_reserve_carved_out_of_ram_bound(self, monkeypatch):
        # syndna's shape off SLURM: minimap2's reserve is subtracted from the
        # host bound too. Without it DuckDB gets min(31, 32 - 4) = 28 and 28 +
        # minimap2's 8 over-commits the 32 GB box.
        monkeypatch.setattr(f"{_MIINT}.detected_ram_gb", lambda: 32)
        assert resolve_duckdb_memory_gb(31, threads=4, reserve_gb=8) == 20

    def test_off_slurm_reserve_still_floors_at_one(self, monkeypatch):
        monkeypatch.setattr(f"{_MIINT}.detected_ram_gb", lambda: 8)
        assert resolve_duckdb_memory_gb(8, threads=4, reserve_gb=8) == 1

    def test_under_slurm_detected_ram_is_irrelevant(self, monkeypatch):
        # Detected RAM must not reach the SLURM path: a 4 GB reading must not
        # shrink a real 48 GB allocation (48 - 6 = 42).
        monkeypatch.setattr(f"{_MIINT}.detected_ram_gb", lambda: 4)
        monkeypatch.setenv("SLURM_MEM_PER_NODE", str(48 * 1024))
        assert resolve_duckdb_memory_gb(31, threads=8) == 42

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


def test_duckdb_out_of_memory_text_is_classified_as_oom():
    """DuckDB's `OutOfMemoryException` message must match `_OOM_SIGNATURES`, because
    that is the whole of the retry path for a native job that exhausts its DuckDB
    `memory_limit`.

    Such a job is not cgroup-killed — SLURM records it FAILED, and the classifier's
    only route to `OOM_KILLED` (and so to the escalated retry) is finding an OOM
    signature in the stderr tail. `assembly_load` relies on this: three first
    attempts at 16 GiB raised here and escalated to 32 without an operator.

    Probed rather than assumed: the match comes from DuckDB prefixing the message
    with "Out of Memory Error:", which is third-party formatting. A release that
    reworded it would silently turn these into permanent failures, so this pins it.

    The exception's CLASS matters as much as its text. ``run_native_job`` maps
    ``ValueError`` to ``BAD_INPUT``, which is permanent and never escalates, so an
    OOM class inheriting ``ValueError`` would bypass the OOM path entirely.
    """
    import duckdb
    from qiita_common.log_tail import contains_oom_signature

    conn = duckdb.connect(":memory:")
    conn.execute("SET memory_limit='20MB'")
    conn.execute("SET threads=1")
    with pytest.raises(duckdb.OutOfMemoryException) as exc:
        conn.execute(
            "SELECT count(*) FROM ("
            "  SELECT range AS a, repeat(md5(range::VARCHAR), 300) AS b"
            "  FROM range(400000) ORDER BY b)"
        ).fetchall()
    assert not isinstance(exc.value, (ValueError, FileNotFoundError)), (
        f"DuckDB's OOM class now inherits a type the native-job dispatcher maps to a "
        f"permanent failure, so these would stop escalating: "
        f"{type(exc.value).__mro__}"
    )
    assert contains_oom_signature(str(exc.value)), (
        f"DuckDB's OOM message no longer matches an OOM signature, so a native job "
        f"that exhausts memory_limit would fail permanently instead of escalating: "
        f"{str(exc.value)[:200]!r}"
    )
