"""krepp_node_check.check: the krepp-index probe row's status for this node's copies."""

from __future__ import annotations

import shutil

import pytest
from qiita_common.krepp import KreppBuildRecord

from qiita_compute_orchestrator import krepp_node_check as kc
from qiita_compute_orchestrator.derived_store import krepp_build_relpath
from qiita_compute_orchestrator.krepp_build import JEST_LABELLED_TREE, JEST_NULL, write_build_files


@pytest.fixture
def node(krepp_built_index, tmp_path):
    rel = krepp_build_relpath(18, "t1")
    copy = tmp_path / "node" / rel
    shutil.copytree(krepp_built_index, copy / "index")
    (copy / "jest").mkdir()
    (copy / JEST_LABELLED_TREE).write_text("(a{0});\n")
    (copy / JEST_NULL).write_text("0.1\n")
    sums, manifest = write_build_files(copy, reference_idx=18, build_label="t1")
    build = KreppBuildRecord(
        reference_index_idx=5, reference_idx=18, fs_path=rel, sha256sums_sha256=sums
    )
    return tmp_path / "node", copy, build, manifest["settings"]["krepp version"]


def test_ok_when_every_copy_passes(node):
    root, _copy, build, version = node
    assert kc.check([build], str(root), version)[0] == kc.EXIT_OK


def test_skip_without_an_active_build(node):
    root, _copy, _build, version = node
    assert kc.check([], str(root), version)[0] == kc.EXIT_SKIP


def test_warn_on_a_bad_copy_naming_the_build(node):
    root, copy, build, version = node
    (copy / "index" / "stray").write_text("x")
    code, detail = kc.check([build], str(root), version)
    assert code == kc.EXIT_WARN
    assert "build 5" in detail and "files" in detail


def test_warn_on_a_missing_copy(node, tmp_path):
    _root, _copy, build, version = node
    code, detail = kc.check([build], str(tmp_path / "empty"), version)
    assert code == kc.EXIT_WARN and "missing" in detail


def test_warn_on_another_krepp_naming_both_versions(node):
    root, _copy, build, version = node
    code, detail = kc.check([build], str(root), "v0.0.0")
    assert code == kc.EXIT_WARN
    assert f"written by krepp {version}" in detail and "links v0.0.0" in detail


def test_warn_when_miint_reports_no_krepp_version(node):
    root, _copy, build, _version = node
    code, detail = kc.check([build], str(root), None)
    assert code == kc.EXIT_WARN and "no krepp-version row" in detail


def test_fail_without_the_tier(node):
    _root, _copy, build, version = node
    assert kc.check([build], None, version)[0] == kc.EXIT_FAIL
