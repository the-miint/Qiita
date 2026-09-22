"""Tests for `_common.commit_partials`, the all-or-nothing multi-file commit.

Two CLIs write their exports through this: the admin masked-read export (privacy-
masked sequence data, chmod 0600 before each rename) and the client feature-table
bundle (a table that is useless without the identifier map beside it). Both rely on
the same property — after a failure, the filesystem looks as it did before — and the
only code path that delivers it for a *partially committed* set is the rollback of
finals that already landed. That path is unreachable through either caller's own
tests, since they fail inside the write rather than between two renames, so it is
provoked directly here.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from qiita_control_plane.cli import _common


def _pairs(tmp_path: Path, *names: str) -> list[tuple[Path, Path]]:
    return [(tmp_path / f"{n}.partial", tmp_path / n) for n in names]


def _write_all(pairs) -> None:
    for partial, _ in pairs:
        partial.write_text(partial.name)


def test_a_clean_run_commits_every_pair_and_leaves_no_partial(tmp_path):
    pairs = _pairs(tmp_path, "table.parquet", "map.json")
    _common.commit_partials(lambda: _write_all(pairs), pairs)

    assert sorted(p.name for p in tmp_path.iterdir()) == ["map.json", "table.parquet"]


def test_a_failure_INSIDE_the_write_removes_the_partials_it_had_made(tmp_path):
    """The write may have created some partials before raising, which is why the paths
    are passed in rather than discovered afterwards."""
    pairs = _pairs(tmp_path, "table.parquet", "map.json")

    def write() -> None:
        pairs[0][0].write_text("first partial landed")
        raise RuntimeError("the second write failed")

    with pytest.raises(RuntimeError):
        _common.commit_partials(write, pairs)

    assert list(tmp_path.iterdir()) == []


def test_a_failure_BETWEEN_renames_takes_the_already_committed_file_back(monkeypatch, tmp_path):
    """The property the whole helper exists for, and the one no caller's own tests
    reach: the first rename succeeds, the second fails, and the first file must not
    survive. Without the rollback a masked-read export would leave a lone R1 without
    its R2, and a feature-table bundle a table nobody can join.
    """
    pairs = _pairs(tmp_path, "first", "second")
    real_replace = os.replace
    calls: list[int] = []

    def flaky_replace(src, dst):
        calls.append(1)
        if len(calls) == 2:
            raise OSError("the second rename failed")
        real_replace(src, dst)

    monkeypatch.setattr(_common.os, "replace", flaky_replace)
    with pytest.raises(OSError):
        _common.commit_partials(lambda: _write_all(pairs), pairs)

    assert list(tmp_path.iterdir()) == []


def test_a_keyboard_interrupt_rolls_back_too(monkeypatch, tmp_path):
    """`except BaseException`, not `except Exception`: a Ctrl-C mid-commit is the most
    likely way a human produces a partially-committed set."""
    pairs = _pairs(tmp_path, "first", "second")
    real_replace = os.replace
    calls: list[int] = []

    def interrupt_on_second(src, dst):
        calls.append(1)
        if len(calls) == 2:
            raise KeyboardInterrupt
        real_replace(src, dst)

    monkeypatch.setattr(_common.os, "replace", interrupt_on_second)
    with pytest.raises(KeyboardInterrupt):
        _common.commit_partials(lambda: _write_all(pairs), pairs)

    assert list(tmp_path.iterdir()) == []


def test_the_rollback_leaves_a_file_it_did_not_write_alone(tmp_path):
    """Only this commit's own paths are cleaned up. A file already at one of the final
    names is the caller's to refuse — see `_write_bundle` — not this helper's to
    delete, and an unrelated neighbour is nobody's business.
    """
    (tmp_path / "unrelated.txt").write_text("keep me")
    pairs = _pairs(tmp_path, "table.parquet")

    with pytest.raises(RuntimeError):
        _common.commit_partials(lambda: (_ for _ in ()).throw(RuntimeError("no write")), pairs)

    assert [p.name for p in tmp_path.iterdir()] == ["unrelated.txt"]


def test_the_mode_is_applied_BEFORE_the_rename(monkeypatch, tmp_path):
    """The masked-read export's whole reason for a mode: the file must never be visible
    at its final name under a looser umask, even for an instant. Observed by recording
    the partial's permissions at the moment of the rename rather than after it.
    """
    pairs = _pairs(tmp_path, "masked.fastq.gz")
    real_replace = os.replace
    seen: list[int] = []

    def record_then_replace(src, dst):
        seen.append(os.stat(src).st_mode & 0o777)
        real_replace(src, dst)

    monkeypatch.setattr(_common.os, "replace", record_then_replace)
    _common.commit_partials(lambda: _write_all(pairs), pairs, mode=0o600)

    assert seen == [0o600]
    assert (tmp_path / "masked.fastq.gz").stat().st_mode & 0o777 == 0o600


def test_no_mode_leaves_the_permissions_alone(tmp_path):
    """The bundle's case: one of its two files is meant to be published, so a
    restrictive mode would be wrong for it. Passing no mode must therefore change
    nothing rather than pick a default."""
    pairs = _pairs(tmp_path, "table.parquet")
    pairs[0][0].write_text("x")
    pairs[0][0].chmod(0o640)
    _common.commit_partials(lambda: None, pairs)

    assert (tmp_path / "table.parquet").stat().st_mode & 0o777 == 0o640
