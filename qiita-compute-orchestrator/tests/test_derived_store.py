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
    rype_index_path,
    rype_router_index_path,
    shard_bowtie2_dir,
    shard_bowtie2_index_prefix,
    shard_minimap2_dir,
    shard_minimap2_index_path,
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


def test_shard_minimap2_dir_layout():
    """The minimap2 shard-directory root (the `align_minimap2_sharded`
    `shard_directory`) sits at `{PATH_DERIVED}/references/{idx}/minimap2-shards`."""
    assert shard_minimap2_dir("/derived", 7) == Path("/derived/references/7/minimap2-shards")


def test_shard_minimap2_index_path_layout():
    """The per-shard minimap2 `.mmi` is a FLAT `{shard_id}.mmi` under the shard
    directory: `{PATH_DERIVED}/references/{idx}/minimap2-shards/{shard_id}.mmi` —
    the `{shard_directory}/{shard_name}.mmi` shape miint binds."""
    assert shard_minimap2_index_path("/derived", 7, 2) == Path(
        "/derived/references/7/minimap2-shards/2.mmi"
    )


def test_shard_minimap2_index_path_lives_under_its_shard_dir():
    """Every per-shard `.mmi` must sit directly in the one shard directory
    `align_minimap2_sharded` scans (miint globs `{shard_directory}/*.mmi`)."""
    assert shard_minimap2_index_path("/derived", 7, 2).parent == shard_minimap2_dir("/derived", 7)


def test_bowtie2_index_path_layout():
    """The host bowtie2 index PREFIX (bowtie2 writes multiple `.bt2` files under
    it) sits at `{PATH_DERIVED}/references/{idx}/bowtie2/index`."""
    assert bowtie2_index_path("/derived", 7) == Path("/derived/references/7/bowtie2/index")


def test_shard_bowtie2_dir_layout():
    """The bowtie2 shard-directory root (the `align_bowtie2_sharded`
    `shard_directory`) sits at `{PATH_DERIVED}/references/{idx}/bowtie2-shards`."""
    assert shard_bowtie2_dir("/derived", 7) == Path("/derived/references/7/bowtie2-shards")


def test_shard_bowtie2_index_prefix_layout():
    """The per-shard bowtie2 index PREFIX sits inside a per-shard subdir named
    `{shard_id}`: `{PATH_DERIVED}/references/{idx}/bowtie2-shards/{shard_id}/index`
    — the `{shard_directory}/{shard_name}/index.*` shape miint binds."""
    assert shard_bowtie2_index_prefix("/derived", 7, 2) == Path(
        "/derived/references/7/bowtie2-shards/2/index"
    )


def test_shard_bowtie2_index_prefix_lives_under_its_shard_subdir():
    """Each shard's `.bt2` set sits in its own `{shard_id}` subdir under the one
    shard directory `align_bowtie2_sharded` scans."""
    prefix = shard_bowtie2_index_prefix("/derived", 7, 2)
    assert prefix.parent == shard_bowtie2_dir("/derived", 7) / "2"


def test_rype_router_index_path_layout():
    """The whole-reference rype router `.ryxdi` sits directly under the
    per-reference subtree: `{PATH_DERIVED}/references/{idx}/rype-router.ryxdi`."""
    assert rype_router_index_path("/derived", 7) == Path("/derived/references/7/rype-router.ryxdi")


def test_rype_router_index_path_under_reference_dir():
    """The router `.ryxdi` must sit inside the per-reference subtree, so the purge
    endpoint's single `reference_derived_dir` rmtree removes it too."""
    root = Path("/derived")
    assert rype_router_index_path(root, 42).is_relative_to(reference_derived_dir(root, 42))


def test_rype_router_index_path_accepts_str_or_path_root():
    assert rype_router_index_path("/derived", 1) == rype_router_index_path(Path("/derived"), 1)


@pytest.mark.parametrize(
    "builder",
    [
        lambda root: shard_minimap2_dir(root, 42),
        lambda root: shard_minimap2_index_path(root, 42, 3),
        lambda root: bowtie2_index_path(root, 42),
        lambda root: shard_bowtie2_dir(root, 42),
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


@pytest.mark.parametrize(
    "builder",
    [shard_minimap2_dir, shard_bowtie2_dir],
)
def test_new_shard_dir_roots_accept_str_or_path_root(builder):
    assert builder("/derived", 1) == builder(Path("/derived"), 1)


def test_bowtie2_index_path_accepts_str_or_path_root():
    assert bowtie2_index_path("/derived", 1) == bowtie2_index_path(Path("/derived"), 1)
