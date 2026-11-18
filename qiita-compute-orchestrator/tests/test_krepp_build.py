"""krepp_build: the inventory, the files registration writes, and the per-job check,
against a small index the staged miint builds.

The index is built once per session in a subprocess with a timeout:
`krepp_index_create` can hang (duckdb-miint#318), and a hang must fail the session's
fixture rather than the whole run. Building it also pins what krepp writes — seven
files per partition named `<stem>-m<m>r<r>-no_frac`, and a `key: value` metadata
text carrying SHARED_METADATA_KEYS — because `inventory_index` succeeding on it is
that check.
"""

from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
from pathlib import Path

import pytest
from helpers import krepp_recorded_version

from qiita_compute_orchestrator.krepp_build import (
    CHECK_KREPP_VERSION,
    JEST_LABELLED_TREE,
    JEST_NULL,
    MANIFEST,
    SHA256SUMS,
    SHARED_METADATA_KEYS,
    VERIFY_SCRIPT,
    KreppBuildInvalid,
    inventory_index,
    linked_krepp_version,
    verify_build,
    write_build_files,
)
from qiita_compute_orchestrator.miint import open_miint_conn


@pytest.fixture
def build(krepp_built_index, tmp_path) -> Path:
    """A build folder: the index plus the two JEST files, not yet registered."""
    folder = tmp_path / "w43_test"
    shutil.copytree(krepp_built_index, folder / "index")
    (folder / "jest").mkdir()
    (folder / JEST_LABELLED_TREE).write_text(
        "((g0:1{0},g1:1{1}):1{2},(g2:1{3},g3:1{4}):1{5}){6};\n"
    )
    (folder / JEST_NULL).write_text("0.1\n0.2\n")
    return folder


def _register(folder: Path) -> tuple[str, dict]:
    return write_build_files(folder, reference_idx=18, build_label=folder.name, threads=2)


def test_inventory_reads_what_krepp_writes(krepp_built_index):
    inv = inventory_index(krepp_built_index)
    assert inv.m == 4
    assert inv.partitions == (0, 1)
    assert len(inv.files) == 14
    assert set(inv.settings) == set(SHARED_METADATA_KEYS)
    assert (inv.settings["k"], inv.settings["w"], inv.settings["h"]) == ("19", "25", "9")
    assert inv.settings["krepp version"].startswith("v")
    # krepp writes the backbone tree as Newick carrying the edge numbers it was given.
    assert "{6}" in (krepp_built_index / "tree-m4r0-no_frac").read_text()


def test_registration_is_deterministic(build):
    first, manifest = _register(build)
    again, _ = _register(build)
    assert first == again
    assert first == hashlib.sha256((build / SHA256SUMS).read_bytes()).hexdigest()
    assert manifest["partitions"] == [0, 1]
    listed = [line.split("  ", 1)[1] for line in (build / SHA256SUMS).read_text().splitlines()]
    assert SHA256SUMS not in listed and VERIFY_SCRIPT in listed and MANIFEST in listed


def test_registration_refuses_a_folder_edited_since(build):
    _register(build)
    (build / JEST_NULL).write_text("0.3\n")
    with pytest.raises(KreppBuildInvalid, match="changed since it was registered"):
        _register(build)


@pytest.mark.parametrize(
    ("edit", "check"),
    [
        (lambda f: (f / "index" / "reflist-m4r1-no_frac").unlink(), "inventory"),
        (lambda f: (f / "index" / "notes.txt").write_text("x"), "inventory"),
        (lambda f: (f / "extra.txt").write_text("x"), "inventory"),
        (lambda f: (f / JEST_NULL).unlink(), "inventory"),
    ],
)
def test_registration_refuses_a_folder_that_is_not_a_build(build, edit, check):
    edit(build)
    with pytest.raises(KreppBuildInvalid) as exc:
        _register(build)
    assert exc.value.check == check


def test_registration_refuses_partitions_that_disagree(build):
    txt = build / "index" / "metadata-m4r1-no_frac.txt"
    txt.write_text(txt.read_text().replace("seed: 0", "seed: 7"))
    with pytest.raises(KreppBuildInvalid, match=r"differs from r0 on \['seed'\]"):
        _register(build)


def test_registration_refuses_differing_trees(build):
    tree = build / "index" / "tree-m4r1-no_frac"
    tree.write_text(tree.read_text().replace("{6}", "{9}"))
    with pytest.raises(KreppBuildInvalid) as exc:
        _register(build)
    assert exc.value.check == "tree"


def test_verify_accepts_the_registered_copy(build):
    sums, _ = _register(build)
    copy = build.parent / "node-copy"
    shutil.copytree(build, copy)
    manifest = verify_build(copy, sums, linked_krepp_version=None)
    assert manifest["build_label"] == build.name


@pytest.mark.parametrize(
    ("edit", "check"),
    [
        (lambda f: (f / "index" / "extra").write_text("x"), "files"),
        (lambda f: (f / "index" / "cmer-m4r0-no_frac").unlink(), "files"),
        (lambda f: (f / "index" / "cmer-m4r0-no_frac").write_bytes(b"x"), "size"),
        (lambda f: _same_size_edit(f / "index" / "metadata-m4r0-no_frac.txt"), "hash"),
        (lambda f: _same_size_edit(f / MANIFEST), "manifest"),
    ],
)
def test_verify_refuses_a_changed_copy(build, edit, check):
    sums, _ = _register(build)
    edit(build)
    with pytest.raises(KreppBuildInvalid) as exc:
        verify_build(build, sums, linked_krepp_version=None)
    assert exc.value.check == check


def _same_size_edit(path: Path) -> None:
    data = bytearray(path.read_bytes())
    data[-2] = ord("x") if data[-2] != ord("x") else ord("y")
    path.write_bytes(bytes(data))


def test_verify_refuses_another_build(build):
    _register(build)
    with pytest.raises(KreppBuildInvalid) as exc:
        verify_build(build, "0" * 64, linked_krepp_version=None)
    assert exc.value.check == "sha256sums"


def test_verify_tells_missing_from_unreadable(build, tmp_path):
    sums, _ = _register(build)
    with pytest.raises(KreppBuildInvalid) as exc:
        verify_build(tmp_path / "absent", sums, linked_krepp_version=None)
    assert exc.value.check == "missing"
    (build / SHA256SUMS).chmod(0)
    try:
        with pytest.raises(KreppBuildInvalid) as exc:
            verify_build(build, sums, linked_krepp_version=None)
        assert exc.value.check == "unreadable"
    finally:
        (build / SHA256SUMS).chmod(0o644)


def test_an_unreadable_subdirectory_is_refused_not_skipped(build):
    """A directory that cannot be listed would otherwise drop out of the stray-file
    check and SHA256SUMS."""
    sums, _ = _register(build)
    stray = build / "stray"
    stray.mkdir()
    (stray / "f").write_text("x")
    stray.chmod(0)
    try:
        for attempt in (
            lambda: _register(build),
            lambda: verify_build(build, sums, linked_krepp_version=None),
        ):
            with pytest.raises(KreppBuildInvalid) as exc:
                attempt()
            assert exc.value.check == "unreadable"
    finally:
        stray.chmod(0o755)


def test_verify_refuses_a_build_from_another_krepp(build):
    sums, manifest = _register(build)
    built = manifest["settings"]["krepp version"]
    assert verify_build(build, sums, linked_krepp_version=built)
    with pytest.raises(KreppBuildInvalid) as exc:
        verify_build(build, sums, linked_krepp_version=built + "-other")
    assert exc.value.check == CHECK_KREPP_VERSION


def test_linked_version_is_the_one_an_index_records(krepp_built_index):
    """The version the per-job check compares against is the string the same miint
    writes into an index it builds."""
    conn = open_miint_conn()
    try:
        linked = linked_krepp_version(conn)
    finally:
        conn.close()
    assert linked == krepp_recorded_version(krepp_built_index)


def _gnu_sha256sum() -> bool:
    try:
        out = subprocess.run(["sha256sum", "--version"], capture_output=True, text=True)
    except FileNotFoundError:
        return False
    return "GNU coreutils" in out.stdout


@pytest.mark.skipif(not _gnu_sha256sum(), reason="verify.sh targets GNU coreutils on the nodes")
def test_verify_script(build):
    sums, _ = _register(build)
    script = build / VERIFY_SCRIPT

    def run() -> subprocess.CompletedProcess:
        return subprocess.run([str(script), sums], capture_output=True, text=True)

    assert run().returncode == 0
    (build / "index" / "stray").write_text("x")
    result = run()
    assert result.returncode != 0 and "not in SHA256SUMS: index/stray" in result.stderr
    (build / "index" / "stray").unlink()
    _same_size_edit(build / JEST_NULL)
    assert run().returncode != 0
    assert json.loads((build / MANIFEST).read_text())["build_label"] == build.name
