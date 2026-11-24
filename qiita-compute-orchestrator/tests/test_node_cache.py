"""node_cache: sealing a folder, the cheap check of a copy, and the node cache jobs read
sealed folders through. A plain folder stands in for any sealed content."""

from __future__ import annotations

import errno
import hashlib
import shutil
import threading
import time
import types
from pathlib import Path

import pytest

from qiita_compute_orchestrator import node_cache as nc

REL = "references/18/thing/v1"


def _same_size_edit(path: Path) -> None:
    data = bytearray(path.read_bytes())
    data[-2] = ord("x") if data[-2] != ord("x") else ord("y")
    path.write_bytes(bytes(data))


@pytest.fixture
def world(tmp_path, monkeypatch):
    derived = tmp_path / "derived"
    master = derived / REL
    (master / "sub").mkdir(parents=True)
    (master / "a.bin").write_bytes(b"a" * 5000)
    (master / "sub" / "b.txt").write_text("bee\n")
    sums, manifest = nc.seal(master, {"label": "v1"}, threads=2)
    node = tmp_path / "node"
    node.mkdir()
    settings = types.SimpleNamespace(path_derived=derived, path_node_local=node)
    monkeypatch.setattr(nc, "get_settings", lambda: settings)
    return types.SimpleNamespace(
        master=master, sums=sums, manifest=manifest, node=node, copy=node / REL, settings=settings
    )


# --- seal / verify_sealed -----------------------------------------------------------


def test_seal_is_deterministic_and_lists_every_file(world):
    again, manifest = nc.seal(world.master, {"label": "v1"})
    sums_file = (world.master / nc.SHA256SUMS).read_bytes()
    assert again == world.sums == hashlib.sha256(sums_file).hexdigest()
    assert manifest == world.manifest
    assert manifest["files"] == {"a.bin": 5000, "sub/b.txt": 4}
    listed = [ln.split("  ", 1)[1] for ln in sums_file.decode().splitlines()]
    assert listed == ["a.bin", nc.MANIFEST, "sub/b.txt"]


def test_seal_refuses_a_folder_changed_since_it_was_sealed(world):
    (world.master / "sub" / "b.txt").write_text("changed\n")
    with pytest.raises(nc.SealedFolderInvalid, match="changed since it was sealed"):
        nc.seal(world.master, {"label": "v1"})


def test_seal_keeps_files_for_itself(world):
    with pytest.raises(ValueError, match="`files`"):
        nc.seal(world.master, {"files": 1})


def test_verify_accepts_the_sealed_folder(world):
    assert nc.verify_sealed(world.master, world.sums) == world.manifest


@pytest.mark.parametrize(
    ("edit", "check"),
    [
        (lambda f: (f / "extra").write_text("x"), "files"),
        (lambda f: (f / "a.bin").unlink(), "files"),
        (lambda f: (f / "a.bin").write_bytes(b"x"), "size"),
        (lambda f: _same_size_edit(f / nc.MANIFEST), "manifest"),
        (lambda f: (f / "sub" / "link").symlink_to(f / "a.bin"), "link"),
    ],
)
def test_verify_refuses_a_changed_copy(world, edit, check):
    edit(world.master)
    with pytest.raises(nc.SealedFolderInvalid) as exc:
        nc.verify_sealed(world.master, world.sums)
    assert exc.value.check == check


def test_verify_refuses_another_folder(world):
    with pytest.raises(nc.SealedFolderInvalid) as exc:
        nc.verify_sealed(world.master, "0" * 64)
    assert exc.value.check == "sha256sums"


def test_verify_tells_missing_from_unreadable(world, tmp_path):
    with pytest.raises(nc.SealedFolderInvalid) as exc:
        nc.verify_sealed(tmp_path / "absent", world.sums)
    assert exc.value.check == "missing"
    (world.master / nc.SHA256SUMS).chmod(0)
    try:
        with pytest.raises(nc.SealedFolderInvalid) as exc:
            nc.verify_sealed(world.master, world.sums)
        assert exc.value.check == "unreadable"
    finally:
        (world.master / nc.SHA256SUMS).chmod(0o644)


def test_an_unreadable_subdirectory_is_refused_not_skipped(world):
    """A directory that cannot be listed would otherwise drop out of the file list."""
    (world.master / "sub").chmod(0)
    try:
        with pytest.raises(nc.SealedFolderInvalid) as exc:
            nc.verify_sealed(world.master, world.sums)
        assert exc.value.check == "unreadable"
    finally:
        (world.master / "sub").chmod(0o755)


# --- materialize ----------------------------------------------------------------------


def test_without_a_node_cache_the_master_is_checked_and_returned(world):
    world.settings.path_node_local = None
    assert nc.materialize(REL, world.sums) == world.master
    (world.master / "extra").write_text("x")
    with pytest.raises(nc.SealedFolderInvalid):
        nc.materialize(REL, world.sums)


def test_the_first_use_copies_and_a_later_one_reuses(world, monkeypatch):
    assert nc.materialize(REL, world.sums) == world.copy
    assert nc.verify_sealed(world.copy, world.sums) == world.manifest
    assert (world.copy / "a.bin").read_bytes() == b"a" * 5000

    def _no_copy(*a, **k):
        raise AssertionError("copied again")

    monkeypatch.setattr(nc, "_copy", _no_copy)
    assert nc.materialize(REL, world.sums) == world.copy


def test_a_changed_node_copy_is_copied_again(world):
    nc.materialize(REL, world.sums)
    (world.copy / "a.bin").write_bytes(b"x")
    nc.materialize(REL, world.sums)
    assert (world.copy / "a.bin").read_bytes() == b"a" * 5000


def test_a_partial_copy_left_by_a_stopped_job_is_replaced(world):
    partial = world.copy.parent / f".{world.copy.name}.partial"
    partial.mkdir(parents=True)
    (partial / "a.bin").write_bytes(b"half")
    nc.materialize(REL, world.sums)
    assert not partial.exists() and nc.verify_sealed(world.copy, world.sums)


def test_a_master_file_that_does_not_match_fails_and_leaves_nothing(world):
    _same_size_edit(world.master / "a.bin")
    with pytest.raises(nc.SealedFolderInvalid) as exc:
        nc.materialize(REL, world.sums)
    assert exc.value.check == "hash"
    assert sorted(p.name for p in world.copy.parent.iterdir()) == []


def test_not_enough_space_fails_before_copying(world, monkeypatch):
    monkeypatch.setattr(nc.shutil, "disk_usage", lambda p: types.SimpleNamespace(free=10))
    with pytest.raises(nc.NodeCacheError) as exc:
        nc.materialize(REL, world.sums)
    assert exc.value.check == "space" and "has 10 free" in exc.value.detail
    assert not world.copy.exists()


def test_a_disk_that_fills_during_the_copy_fails_and_leaves_nothing(world, monkeypatch):
    real = nc._copy_hashed

    def _fill(src, dst, want):
        if src.name == "b.txt":
            raise OSError(errno.ENOSPC, "No space left on device")
        real(src, dst, want)

    monkeypatch.setattr(nc, "_copy_hashed", _fill)
    with pytest.raises(nc.NodeCacheError) as exc:
        nc.materialize(REL, world.sums)
    assert exc.value.check == "space"
    assert sorted(p.name for p in world.copy.parent.iterdir()) == []


def test_jobs_on_one_node_copy_once(world, monkeypatch):
    """Two jobs at once: the second waits on the lock, then checks and uses the copy."""
    copies = []
    real = nc._copy

    def _slow_copy(*a, **k):
        copies.append(1)
        time.sleep(0.3)
        real(*a, **k)

    monkeypatch.setattr(nc, "_copy", _slow_copy)
    results: list[Path] = []
    threads = [
        threading.Thread(target=lambda: results.append(nc.materialize(REL, world.sums)))
        for _ in range(2)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert copies == [1] and results == [world.copy, world.copy]


def test_evict_runs_before_the_copy_so_its_space_counts(world, monkeypatch):
    """A removed folder's copy (and any partial of it) goes before this one is copied:
    the free space is enough only once the evicted copy is gone."""
    other = "references/18/thing/v2"
    shutil.copytree(world.master, world.settings.path_derived / other)
    nc.materialize(other, world.sums)
    (world.node / "references/18/thing/.v2.partial").mkdir()
    real = nc.shutil.disk_usage
    evicted = world.node / other

    def _free(path):
        free = real(path).free if not evicted.exists() else 0
        return types.SimpleNamespace(free=free)

    monkeypatch.setattr(nc.shutil, "disk_usage", _free)
    nc.materialize(REL, world.sums, evict_first=[other])
    assert sorted(p.name for p in world.copy.parent.iterdir()) == ["v1"]


def test_evict_raises_on_a_copy_it_cannot_look_up(world):
    """A copy behind a directory without search permission is not taken as absent."""
    nc.materialize(REL, world.sums)
    hidden = world.node / "references"
    hidden.chmod(0o600)
    try:
        with pytest.raises(PermissionError):
            nc.evict([REL])
    finally:
        hidden.chmod(0o755)
    assert world.copy.exists()


def test_a_master_with_an_unlisted_file_is_refused_with_the_cache_too(world):
    """The two read modes accept the same masters."""
    (world.master / "extra").write_text("x")
    with pytest.raises(nc.SealedFolderInvalid) as exc:
        nc.materialize(REL, world.sums)
    assert exc.value.check == "files" and not world.copy.exists()


def _wrong_manifest_size(world) -> str:
    """Make the master's manifest give a.bin a wrong size, with every hash matching;
    return the new SHA256SUMS hash."""
    manifest_path = world.master / nc.MANIFEST
    manifest_path.write_text(manifest_path.read_text().replace('"a.bin": 5000', '"a.bin": 4999'))
    sums_path = world.master / nc.SHA256SUMS
    lines = [
        f"{nc.sha256_file(manifest_path)}  {nc.MANIFEST}" if ln.endswith(nc.MANIFEST) else ln
        for ln in sums_path.read_text().splitlines()
    ]
    sums_path.write_text("\n".join(lines) + "\n")
    return hashlib.sha256(sums_path.read_bytes()).hexdigest()


@pytest.mark.parametrize("cached", [True, False])
def test_a_master_whose_manifest_sizes_are_wrong_is_refused_in_both_modes(world, cached):
    sums = _wrong_manifest_size(world)
    if not cached:
        world.settings.path_node_local = None
    with pytest.raises(nc.SealedFolderInvalid) as exc:
        nc.materialize(REL, sums)
    assert exc.value.check == "size" and not world.copy.exists()


def test_an_unwritable_root_fails_by_name(world):
    world.node.chmod(0o555)
    try:
        with pytest.raises(nc.NodeCacheError) as exc:
            nc.materialize(REL, world.sums)
        assert exc.value.check == "root"
    finally:
        world.node.chmod(0o755)


# --- probe ----------------------------------------------------------------------------


def test_probe_statuses(world):
    code, detail = nc.probe()
    assert code == 0 and "writable" in detail
    world.node.chmod(0o555)
    try:
        assert nc.probe()[0] == 4
    finally:
        world.node.chmod(0o755)
    world.settings.path_node_local = None
    assert nc.probe()[0] == 3
