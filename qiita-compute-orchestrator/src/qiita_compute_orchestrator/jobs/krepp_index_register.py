"""Register a krepp build that was made outside Qiita and placed at its master path.

The build is a folder at ``PATH_DERIVED/references/{reference_idx}/krepp/{build_label}/``
(``derived_store.krepp_build_relpath``) holding ``index/`` and ``jest/``
(``krepp_build``). This step checks it and writes Qiita's files into it; the
``register-index`` action that follows records it `registered`. Nothing serves a
registered build: the cluster admins copy the folder to every node, and the
``reference-index-activate`` workflow checks a node copy and makes it the
reference's active build.

Checks, cheapest first, before any file is hashed:

- its settings and tree can be read (``krepp_build.inventory_index``);
- it was written by the krepp the staged miint links (``linked_krepp_version``);
- its tree is the reference's tree as a placement sees it
  (``_phylogeny.tree_difference``): every edge number, parent edge number, branch
  length and tip name, for the reason docs/architecture/reference-data.md "Edge
  numbering" gives. The reference's tree must be fully numbered
  (``POST /reference/{idx}/phylogeny/mint-edge-id``);
- JEST's tree (``labelled.nwk``) carries the same edges (branch lengths not compared).

Then every file is hashed once and SHA256SUMS written (``write_build_files``).
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import duckdb
from pydantic import BaseModel
from qiita_common.krepp import SHA256SUMS_SHA256_PARAM
from qiita_common.models import INDEX_TYPE_KREPP, IndexPathRoot, ReferenceIndexState

from ..config import get_settings
from ..data_plane_client import open_reference_phylogeny_stream
from ..derived_store import KREPP_INDEX_SUBDIR, krepp_build_relpath, node_relative_path
from ..krepp_build import (
    JEST_LABELLED_TREE,
    KreppBuildInvalid,
    check_krepp_version,
    inventory_index,
    linked_krepp_version,
    write_build_files,
)
from ..miint import open_miint_conn
from ._phylogeny import tree_difference

_TREE_COLUMNS = "node_index, parent_index, edge_id, branch_length, is_tip, name"


class Inputs(BaseModel):
    """`reference_idx` and `work_ticket_idx` are injected scope scalars;
    `build_label` names the folder (one path segment)."""

    reference_idx: int
    work_ticket_idx: int
    build_label: str


def _hash_threads() -> int:
    return max(1, int(os.environ.get("SLURM_CPUS_PER_TASK", "0")) or (os.cpu_count() or 1))


async def check_trees(
    conn: duckdb.DuckDBPyConnection, reference_idx: int, folder: Path, tree_file: str
) -> None:
    """Raise KreppBuildInvalid unless the build's tree, and JEST's tree by its
    edges, are the reference's tree in the lake."""
    conn.execute(
        f"CREATE OR REPLACE TEMP TABLE krepp_tree AS SELECT {_TREE_COLUMNS} FROM read_newick(?)",
        [str(folder / tree_file)],
    )
    conn.execute(
        f"CREATE OR REPLACE TEMP TABLE jest_tree AS SELECT {_TREE_COLUMNS} FROM read_newick(?)",
        [str(folder / JEST_LABELLED_TREE)],
    )
    async with open_reference_phylogeny_stream(conn, reference_idx=reference_idx) as rel:
        conn.execute(f"CREATE OR REPLACE TEMP TABLE lake_tree AS SELECT {_TREE_COLUMNS} FROM {rel}")
    nodes, unnumbered = conn.execute(
        "SELECT count(*), count(*) FILTER (edge_id IS NULL) FROM lake_tree"
    ).fetchone()
    if nodes == 0:
        raise KreppBuildInvalid("tree", f"reference {reference_idx} has no phylogeny in the lake")
    if unnumbered:
        raise KreppBuildInvalid(
            "tree",
            f"{unnumbered} of reference {reference_idx}'s {nodes} tree nodes have no edge_id;"
            f" number them first (POST /reference/{reference_idx}/phylogeny/mint-edge-id)",
        )
    for table, what, lengths in (
        ("krepp_tree", f"the build's tree ({tree_file})", True),
        ("jest_tree", f"JEST's labelled tree ({JEST_LABELLED_TREE})", False),
    ):
        diff = tree_difference(conn, "lake_tree", table, compare_lengths=lengths)
        if not diff.same:
            raise KreppBuildInvalid(
                "tree",
                f"{what} is not reference {reference_idx}'s tree: {diff.actual_nodes} nodes to"
                f" the reference's {diff.expected_nodes}, and {diff.missing} of the reference's"
                f" nodes have no match (e.g. {diff.examples})",
            )


async def execute(inputs: Inputs, workspace: Path) -> dict[str, Path]:
    relpath = krepp_build_relpath(inputs.reference_idx, inputs.build_label)
    folder = node_relative_path(get_settings().path_derived, relpath)
    inventory = inventory_index(folder / KREPP_INDEX_SUBDIR)
    conn = open_miint_conn()
    try:
        check_krepp_version(inventory.settings["krepp version"], linked_krepp_version(conn))
        await check_trees(conn, inputs.reference_idx, folder, inventory.tree_file)
    finally:
        conn.close()
    sums_sha256, manifest = write_build_files(
        folder,
        reference_idx=inputs.reference_idx,
        build_label=inputs.build_label,
        threads=_hash_threads(),
    )
    settings = manifest["settings"]
    meta = {
        "index_type": INDEX_TYPE_KREPP,
        "fs_path": relpath,
        "state": ReferenceIndexState.REGISTERED.value,
        "fs_path_root": IndexPathRoot.NODE_LOCAL.value,
        "params": {
            SHA256SUMS_SHA256_PARAM: sums_sha256,
            "build_label": inputs.build_label,
            "krepp_version": settings["krepp version"],
            "k": int(settings["k"]),
            "w": int(settings["w"]),
            "h": int(settings["h"]),
            "m": int(settings["m"]),
            "file_count": len(manifest["files"]),
            "bytes": sum(manifest["files"].values()),
        },
    }
    meta_path = workspace / "krepp_index_meta.json"
    meta_path.write_text(json.dumps(meta, sort_keys=True))
    return {"krepp_index_meta": meta_path}
