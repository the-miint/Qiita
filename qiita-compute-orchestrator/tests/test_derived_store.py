"""Unit tests for the derived-storage path layout (`derived_store`).

These pin the `{PATH_DERIVED}/references/{idx}/...` convention the orchestrator
owns, so the three consumers (build_rype_index, build_minimap2_index, the
reference-artifact purge endpoint) stay in lockstep through one source.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from qiita_compute_orchestrator.derived_store import (
    bowtie2_index_path,
    minimap2_index_path,
    reference_derived_dir,
    reference_shard_dir,
    rype_index_path,
    shard_bowtie2_index_prefix,
    shard_minimap2_index_path,
    shard_rype_index_path,
)


def test_reference_derived_dir_layout():
    root = Path("/derived")
    assert reference_derived_dir(root, 7) == Path("/derived/references/7")


def test_rype_index_path_layout():
    assert rype_index_path("/derived", 7) == Path("/derived/references/7/rype/index.ryxdi")


def test_minimap2_index_path_layout():
    assert minimap2_index_path("/derived", 7) == Path("/derived/references/7/minimap2/index.mmi")


@pytest.mark.parametrize("builder", [rype_index_path, minimap2_index_path])
def test_index_paths_live_under_the_reference_dir(builder):
    """Both index kinds must sit inside the per-reference subtree, so the purge
    endpoint's single `reference_derived_dir` rmtree removes them both."""
    root = Path("/derived")
    index = builder(root, 42)
    assert index.is_relative_to(reference_derived_dir(root, 42))


def test_accepts_str_or_path_root():
    """`Settings.path_derived` is a str; a caller holding a Path shouldn't have
    to round-trip through str. Both produce the same absolute layout."""
    assert rype_index_path("/derived", 1) == rype_index_path(Path("/derived"), 1)


def test_reference_shard_dir_layout():
    """A sharded analysis index's per-shard directory sits under the
    per-reference subtree: `{PATH_DERIVED}/references/{idx}/shards/{shard_id}`."""
    assert reference_shard_dir("/derived", 7, 0) == Path("/derived/references/7/shards/0")


def test_reference_shard_dir_lives_under_the_reference_dir():
    """Each shard directory must sit inside the per-reference subtree, so the
    purge endpoint's single `reference_derived_dir` rmtree removes it too."""
    root = Path("/derived")
    assert reference_shard_dir(root, 42, 3).is_relative_to(reference_derived_dir(root, 42))


def test_reference_shard_dir_accepts_str_or_path_root():
    assert reference_shard_dir("/derived", 1, 2) == reference_shard_dir(Path("/derived"), 1, 2)


def test_shard_rype_index_path_layout():
    """The per-shard rype `.ryxdi` routing index sits at
    `{PATH_DERIVED}/references/{idx}/shards/{shard_id}/index.ryxdi`."""
    assert shard_rype_index_path("/derived", 7, 0) == Path(
        "/derived/references/7/shards/0/index.ryxdi"
    )


def test_shard_rype_index_path_under_reference_dir():
    """The shard `.ryxdi` must sit inside the per-reference subtree, so the purge
    endpoint's single `reference_derived_dir` rmtree removes it too."""
    root = Path("/derived")
    assert shard_rype_index_path(root, 42, 3).is_relative_to(reference_derived_dir(root, 42))


def test_shard_rype_index_path_accepts_str_or_path_root():
    assert shard_rype_index_path("/derived", 1, 2) == shard_rype_index_path(Path("/derived"), 1, 2)


def test_shard_minimap2_index_path_layout():
    """The per-shard minimap2 `.mmi` sits at
    `{PATH_DERIVED}/references/{idx}/shards/{shard_id}/minimap2/index.mmi`."""
    assert shard_minimap2_index_path("/derived", 7, 2) == Path(
        "/derived/references/7/shards/2/minimap2/index.mmi"
    )


def test_bowtie2_index_path_layout():
    """The host bowtie2 index PREFIX (bowtie2 writes multiple `.bt2` files under
    it) sits at `{PATH_DERIVED}/references/{idx}/bowtie2/index`."""
    assert bowtie2_index_path("/derived", 7) == Path("/derived/references/7/bowtie2/index")


def test_shard_bowtie2_index_prefix_layout():
    """The per-shard bowtie2 index PREFIX sits at
    `{PATH_DERIVED}/references/{idx}/shards/{shard_id}/bowtie2/index`."""
    assert shard_bowtie2_index_prefix("/derived", 7, 2) == Path(
        "/derived/references/7/shards/2/bowtie2/index"
    )


@pytest.mark.parametrize(
    "builder",
    [
        lambda root: shard_minimap2_index_path(root, 42, 3),
        lambda root: bowtie2_index_path(root, 42),
        lambda root: shard_bowtie2_index_prefix(root, 42, 3),
    ],
)
def test_new_index_paths_live_under_the_reference_dir(builder):
    """Every new index path/prefix must sit inside the per-reference subtree, so
    the purge endpoint's single `reference_derived_dir` rmtree removes it too."""
    root = Path("/derived")
    assert builder(root).is_relative_to(reference_derived_dir(root, 42))


@pytest.mark.parametrize(
    "builder",
    [shard_minimap2_index_path, shard_bowtie2_index_prefix],
)
def test_new_shard_paths_accept_str_or_path_root(builder):
    assert builder("/derived", 1, 2) == builder(Path("/derived"), 1, 2)


def test_bowtie2_index_path_accepts_str_or_path_root():
    assert bowtie2_index_path("/derived", 1) == bowtie2_index_path(Path("/derived"), 1)
