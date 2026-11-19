"""The krepp build path helpers in derived_store."""

from pathlib import Path

import pytest

from qiita_compute_orchestrator.derived_store import krepp_build_relpath, node_relative_path


def test_build_relpath_and_node_path():
    rel = krepp_build_relpath(18, "w43_r0-25")
    assert rel == "references/18/krepp/w43_r0-25"
    assert node_relative_path("/scratch/qiita-miint", rel) == Path(
        "/scratch/qiita-miint/references/18/krepp/w43_r0-25"
    )


@pytest.mark.parametrize("label", ["", ".hidden", "a/b", "..", "x y", "a" * 129])
def test_build_label_must_be_one_plain_segment(label):
    with pytest.raises(ValueError, match="build label"):
        krepp_build_relpath(18, label)


@pytest.mark.parametrize("rel", ["", "/abs/path", "a/../b", "./a", "a//b"])
def test_node_relative_path_refuses_escapes(rel):
    with pytest.raises(ValueError, match="must be relative"):
        node_relative_path("/root", rel)
