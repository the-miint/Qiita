"""A krepp build folder: what is in it, what registration writes into it, and its checks.

A build is made outside Qiita and placed, by hand, at its master path under
PATH_DERIVED (``derived_store.krepp_build_relpath``). Registration
(``jobs.krepp_index_register``) inventories it and seals it (``node_cache.seal``),
so a job can read it through ``node_cache.materialize``, which copies it to the
node's PATH_NODE_LOCAL on first use when that is set.

Layout::

    SHA256SUMS      `sha256sum -c` format, every file but itself
    manifest.json   the recorded index settings, the tree file, every file's size
    index/          the krepp files as krepp wrote them (`place_krepp`'s index directory)
    jest/           labelled.nwk (the backbone tree with JEST's clade labels), null.txt

Of krepp's files Qiita reads two kinds: the ``metadata-*.txt`` texts (``key: value``
lines carrying RECORDED_METADATA_KEYS) and the ``tree-*`` files; a test pins both
against an index built by the staged miint.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import duckdb

from .derived_store import KREPP_INDEX_SUBDIR
from .node_cache import MANIFEST, SHA256SUMS, SealedFolderInvalid, all_files, seal

JEST_SUBDIR = "jest"
JEST_LABELLED_TREE = f"{JEST_SUBDIR}/labelled.nwk"
JEST_NULL = f"{JEST_SUBDIR}/null.txt"
MANIFEST_FORMAT = 1

# The metadata keys Qiita reads from every `metadata-*.txt` in `index/`: all are
# recorded in the manifest, and all but `frac` in reference_index.params.
RECORDED_METADATA_KEYS = ("krepp version", "k", "w", "h", "m", "frac")


class KreppBuildInvalid(SealedFolderInvalid):
    """The folder is not the build it should be. `check` names the check that failed
    (stable, for logs); `detail` says what was found."""

    def __init__(self, check: str, detail: str):
        super().__init__(check, detail)
        self.args = (f"krepp build check {check!r} failed: {detail}",)


@dataclass(frozen=True)
class Inventory:
    """What registration needs from `index/`: the recorded settings, every file, and
    the tree file a placement uses."""

    settings: dict[str, str]
    files: tuple[str, ...]
    tree_file: str


def _recorded_settings(index_dir: Path) -> dict[str, str]:
    """RECORDED_METADATA_KEYS from the index's metadata texts, read with DuckDB's
    read_text as miint's krepp docs show. Each key must be present with one value
    across the texts, since Qiita records one."""
    with duckdb.connect() as conn:
        rows = conn.execute(
            "SELECT key, regexp_extract(content, '(?m)^' || key || ': ([^\\r\\n]*)', 1),"
            " parse_filename(filename)"
            " FROM read_text(?) CROSS JOIN unnest(?::VARCHAR[]) AS t(key)",
            [str(index_dir / "metadata-*.txt"), list(RECORDED_METADATA_KEYS)],
        ).fetchall()
    if not rows:
        raise KreppBuildInvalid("metadata", f"{index_dir} holds no metadata-*.txt")
    by_key: dict[str, dict[str, list[str]]] = {}
    for key, value, name in rows:
        by_key.setdefault(key, {}).setdefault(value, []).append(name)
    settings: dict[str, str] = {}
    for key in RECORDED_METADATA_KEYS:
        found = by_key[key]
        if "" in found:
            raise KreppBuildInvalid("metadata", f"{sorted(found[''])} lack {key!r}")
        if len(found) != 1:
            detail = "; ".join(f"{v!r} in {sorted(n)}" for v, n in sorted(found.items()))
            raise KreppBuildInvalid("metadata", f"the metadata texts disagree on {key!r}: {detail}")
        (settings[key],) = found
    return settings


def inventory_index(index_dir: Path) -> Inventory:
    """Read `index/` for what registration records. Whether the files make a complete
    krepp index is krepp's to say; Qiita reads the settings it records and picks the
    tree it compares with the reference's, and refuses `tree-*` files that differ,
    since it compares only one. Raises KreppBuildInvalid, or SealedFolderInvalid
    (`link`, `unreadable`) from listing the files."""
    if not index_dir.is_dir():
        raise KreppBuildInvalid("inventory", f"{index_dir} is not a directory")
    names = all_files(index_dir)
    if not names:
        raise KreppBuildInvalid("inventory", f"{index_dir} is empty")
    settings = _recorded_settings(index_dir)
    trees = [n for n in names if "/" not in n and n.startswith("tree-")]
    if not trees:
        raise KreppBuildInvalid("tree", f"{index_dir} holds no tree-* file")
    first = (index_dir / trees[0]).read_bytes()
    for name in trees[1:]:
        if (index_dir / name).read_bytes() != first:
            raise KreppBuildInvalid("tree", f"{name} differs from {trees[0]}")
    return Inventory(
        settings=settings,
        files=tuple(f"{KREPP_INDEX_SUBDIR}/{n}" for n in names),
        tree_file=f"{KREPP_INDEX_SUBDIR}/{trees[0]}",
    )


def write_build_files(
    folder: Path, *, reference_idx: int, build_label: str, threads: int = 1
) -> tuple[str, dict]:
    """Inventory the folder and seal it (``node_cache.seal``); return (sha256 of
    SHA256SUMS, manifest). Refuses a file that is not part of a build, and a folder
    sealed earlier with other content, which means it was edited after an earlier
    registration."""
    inventory = inventory_index(folder / KREPP_INDEX_SUBDIR)
    for rel in (JEST_LABELLED_TREE, JEST_NULL):
        if not (folder / rel).is_file():
            raise KreppBuildInvalid("inventory", f"{rel} is missing")
    content = [rel for rel in all_files(folder) if rel not in (SHA256SUMS, MANIFEST)]
    stray = sorted(set(content) - {*inventory.files, JEST_LABELLED_TREE, JEST_NULL})
    if stray:
        raise KreppBuildInvalid("inventory", f"files that are not part of a build: {stray}")
    fields = {
        "format": MANIFEST_FORMAT,
        "reference_idx": reference_idx,
        "build_label": build_label,
        "settings": inventory.settings,
        "tree_file": inventory.tree_file,
    }
    return seal(folder, fields, threads=threads)
