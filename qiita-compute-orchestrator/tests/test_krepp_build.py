"""krepp_build: the inventory and the sealing registration does, against a small
index the staged miint builds. The sealed-folder checks themselves are in
test_node_cache.

The index is built once per session in a subprocess with a timeout:
`krepp_index_create` can hang (duckdb-miint#318), and a hang must fail the session's
fixture rather than the whole run. Building it also pins the two things Qiita reads
of what krepp writes — `metadata-*.txt` texts carrying RECORDED_METADATA_KEYS and
`tree-*` files — because `inventory_index` succeeding on it is that check.
"""

from __future__ import annotations

import hashlib
import shutil
from pathlib import Path

import pytest

from qiita_compute_orchestrator.krepp_build import (
    JEST_LABELLED_TREE,
    JEST_NULL,
    RECORDED_METADATA_KEYS,
    KreppBuildInvalid,
    inventory_index,
    write_build_files,
)
from qiita_compute_orchestrator.node_cache import (
    MANIFEST,
    SHA256SUMS,
    SealedFolderInvalid,
    verify_sealed,
)


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
    assert len(inv.files) == 14
    assert set(inv.settings) == set(RECORDED_METADATA_KEYS)
    assert (inv.settings["m"], inv.settings["frac"]) == ("4", "false")
    assert (inv.settings["k"], inv.settings["w"], inv.settings["h"]) == ("19", "25", "9")
    assert inv.settings["krepp version"].startswith("v")
    # krepp writes the backbone tree as Newick carrying the edge numbers it was given.
    assert "{6}" in (krepp_built_index / "tree-m4r0-no_frac").read_text()


def test_registration_is_deterministic(build):
    first, manifest = _register(build)
    again, _ = _register(build)
    assert first == again
    assert first == hashlib.sha256((build / SHA256SUMS).read_bytes()).hexdigest()
    listed = [line.split("  ", 1)[1] for line in (build / SHA256SUMS).read_text().splitlines()]
    assert SHA256SUMS not in listed and MANIFEST in listed
    assert manifest["build_label"] == build.name and manifest["settings"]["m"] == "4"
    assert verify_sealed(build, first) == manifest


def test_registration_refuses_a_folder_edited_since(build):
    _register(build)
    (build / JEST_NULL).write_text("0.3\n")
    with pytest.raises(SealedFolderInvalid, match="changed since it was sealed"):
        _register(build)


@pytest.mark.parametrize(
    ("edit", "check"),
    [
        (lambda f: (f / "extra.txt").write_text("x"), "inventory"),
        (lambda f: (f / JEST_NULL).unlink(), "inventory"),
        (lambda f: (f / "index" / "extra").symlink_to(f / JEST_NULL), "link"),
        (lambda f: (f / "index" / "sub").symlink_to(f / "jest"), "link"),
    ],
)
def test_registration_refuses_a_folder_that_is_not_a_build(build, edit, check):
    edit(build)
    with pytest.raises(SealedFolderInvalid) as exc:
        _register(build)
    assert exc.value.check == check


def test_registration_refuses_partitions_that_disagree_on_a_recorded_setting(build):
    txt = build / "index" / "metadata-m4r1-no_frac.txt"
    txt.write_text(txt.read_text().replace("w: 25", "w: 31"))
    with pytest.raises(
        KreppBuildInvalid,
        match=r"disagree on 'w': '25' in \['metadata-m4r0-no_frac.txt'\]; '31' in \['metadata-m4r1",
    ):
        _register(build)


def test_registration_refuses_an_index_without_metadata_text(build):
    for txt in (build / "index").glob("metadata-*.txt"):
        txt.unlink()
    with pytest.raises(KreppBuildInvalid, match=r"holds no metadata-\*\.txt"):
        _register(build)


def test_registration_refuses_metadata_without_a_recorded_setting(build):
    txt = build / "index" / "metadata-m4r1-no_frac.txt"
    txt.write_text(
        "".join(ln for ln in txt.read_text().splitlines(True) if not ln.startswith("h: "))
    )
    with pytest.raises(KreppBuildInvalid, match=r"\['metadata-m4r1-no_frac.txt'\] lack 'h'"):
        _register(build)


def test_registration_refuses_differing_trees(build):
    tree = build / "index" / "tree-m4r1-no_frac"
    tree.write_text(tree.read_text().replace("{6}", "{9}"))
    with pytest.raises(KreppBuildInvalid) as exc:
        _register(build)
    assert exc.value.check == "tree"
