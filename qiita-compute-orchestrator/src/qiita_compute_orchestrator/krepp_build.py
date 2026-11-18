"""A krepp build folder: what is in it, the files Qiita writes into it, and the checks.

A build is made outside Qiita and placed, by hand, at its master path under
PATH_DERIVED (``derived_store.krepp_build_relpath``). Registration
(``jobs.krepp_index_register``) inventories it and writes four files beside the
krepp files; the cluster admins then copy the whole folder to every compute node's
PATH_NODE_LOCAL; every job that reads a node copy first runs ``verify_build`` on it.

Layout::

    README.md       for people: what the build is and how to check a copy
    SHA256SUMS      `sha256sum -c` format, every file but itself
    manifest.json   partitions, the shared index settings, every file's size
    verify.sh       the full check of one copy, run by the cluster admins
    index/          the krepp files, 7 per partition (`place_krepp`'s index directory)
    jest/           JEST's labelled tree and null distribution

The krepp file names and the ``metadata-*.txt`` format are krepp's
(``src/index.cpp`` ``save_index`` / ``save_info``); ``tests/test_krepp_build.py``
pins them against an index built by the staged miint. Everything Qiita writes is a
function of the folder's content only, so registering the same folder twice yields
the same ``SHA256SUMS`` and the same hash.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path

from .derived_store import KREPP_INDEX_SUBDIR

SHA256SUMS = "SHA256SUMS"
MANIFEST = "manifest.json"
README = "README.md"
VERIFY_SCRIPT = "verify.sh"
JEST_SUBDIR = "jest"
JEST_LABELLED_TREE = f"{JEST_SUBDIR}/labelled.nwk"
JEST_NULL = f"{JEST_SUBDIR}/null.txt"
MANIFEST_FORMAT = 1

# Each partition r of an index with m partitions is these seven files, `<stem>-m<m>r<r>-
# <frac tag>` plus the text twin of `metadata` (krepp `IndexMultiple::save_index`).
_PARTITION_STEMS = ("cmer", "crecord", "inc", "metadata", "reflist", "tree")
_PARTITION_FILE = re.compile(
    r"(?P<stem>cmer|crecord|inc|metadata|reflist|tree)-m(?P<m>\d+)r(?P<r>\d+)-"
    r"(?P<frac>no_frac|frac)(?P<txt>\.txt)?"
)
FILES_PER_PARTITION = len(_PARTITION_STEMS) + 1

# The metadata-*.txt keys every partition of one index must agree on. `date`,
# `nrows` and `total_num_kmers` differ per partition by construction.
SHARED_METADATA_KEYS = (
    "krepp version",
    "seed",
    "k",
    "w",
    "h",
    "m",
    "frac",
    "ppos_v",
    "npos_v",
    "sdust-t",
    "sdust-w",
)

_HASH_CHUNK = 8 << 20


class KreppBuildInvalid(ValueError):
    """The folder is not the build it should be. `check` names the check that failed
    (stable, for logs and the readiness probe); `detail` says what was found."""

    def __init__(self, check: str, detail: str):
        self.check = check
        self.detail = detail
        super().__init__(f"krepp build check {check!r} failed: {detail}")


@dataclass(frozen=True)
class Inventory:
    """What `index/` holds: the partition numbers, the shared settings from the
    metadata text, and the file names."""

    m: int
    partitions: tuple[int, ...]
    settings: dict[str, str]
    files: tuple[str, ...]
    tree_file: str


def parse_metadata_text(text: str) -> dict[str, str]:
    """`key: value` lines (krepp `IndexMultiple::save_info`) into a dict."""
    out: dict[str, str] = {}
    for line in text.splitlines():
        if not line.strip():
            continue
        key, sep, value = line.partition(": ")
        if not sep:
            raise KreppBuildInvalid("metadata", f"line {line!r} is not `key: value`")
        out[key] = value.strip()
    return out


def inventory_index(index_dir: Path) -> Inventory:
    """Read `index/`: every file is a krepp partition file, every partition has all
    seven, all of one `m` and frac tag, and every partition's metadata text agrees on
    SHARED_METADATA_KEYS. Raises KreppBuildInvalid naming the first problem."""
    if not index_dir.is_dir():
        raise KreppBuildInvalid("inventory", f"{index_dir} is not a directory")
    by_partition: dict[int, set[str]] = {}
    ms: set[int] = set()
    fracs: set[str] = set()
    names = sorted(p.name for p in index_dir.iterdir())
    for name in names:
        path = index_dir / name
        match = _PARTITION_FILE.fullmatch(name)
        if match is None or not path.is_file() or path.is_symlink():
            raise KreppBuildInvalid(
                "inventory", f"{KREPP_INDEX_SUBDIR}/{name} is not a krepp partition file"
            )
        if match["txt"] and match["stem"] != "metadata":
            raise KreppBuildInvalid(
                "inventory", f"{KREPP_INDEX_SUBDIR}/{name}: only metadata has a .txt"
            )
        ms.add(int(match["m"]))
        fracs.add(match["frac"])
        by_partition.setdefault(int(match["r"]), set()).add(match["stem"] + (match["txt"] or ""))
    if not by_partition:
        raise KreppBuildInvalid("inventory", f"{index_dir} holds no krepp partition")
    if len(ms) != 1 or len(fracs) != 1:
        raise KreppBuildInvalid(
            "inventory", f"partitions of more than one index: m {sorted(ms)}, {sorted(fracs)}"
        )
    expected = {*_PARTITION_STEMS, "metadata.txt"}
    for r, stems in sorted(by_partition.items()):
        if stems != expected:
            raise KreppBuildInvalid(
                "inventory", f"partition r{r} is missing {sorted(expected - stems)}"
            )
    (m,) = ms
    (frac,) = fracs
    partitions = tuple(sorted(by_partition))
    settings: dict[str, str] | None = None
    for r in partitions:
        text = (index_dir / f"metadata-m{m}r{r}-{frac}.txt").read_text()
        meta = parse_metadata_text(text)
        missing = [k for k in SHARED_METADATA_KEYS if k not in meta]
        if missing:
            raise KreppBuildInvalid("metadata", f"partition r{r} lacks {missing}")
        shared = {k: meta[k] for k in SHARED_METADATA_KEYS}
        if settings is None:
            settings = shared
        elif shared != settings:
            differ = sorted(k for k in SHARED_METADATA_KEYS if shared[k] != settings[k])
            raise KreppBuildInvalid(
                "metadata", f"partition r{r} differs from r{partitions[0]} on {differ}"
            )
    tree_files = [f"tree-m{m}r{r}-{frac}" for r in partitions]
    first_tree = (index_dir / tree_files[0]).read_bytes()
    for name in tree_files[1:]:
        if (index_dir / name).read_bytes() != first_tree:
            raise KreppBuildInvalid("tree", f"{name} differs from {tree_files[0]}")
    assert settings is not None
    return Inventory(
        m=m,
        partitions=partitions,
        settings=settings,
        files=tuple(f"{KREPP_INDEX_SUBDIR}/{n}" for n in names),
        tree_file=f"{KREPP_INDEX_SUBDIR}/{tree_files[0]}",
    )


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        while chunk := fh.read(_HASH_CHUNK):
            h.update(chunk)
    return h.hexdigest()


def _hash_all(folder: Path, relpaths: list[str], threads: int) -> dict[str, str]:
    # hashlib releases the GIL on large updates, so threads hash files in parallel.
    with ThreadPoolExecutor(max_workers=max(1, threads)) as pool:
        digests = pool.map(lambda rel: sha256_file(folder / rel), relpaths)
        return dict(zip(relpaths, digests, strict=True))


def _all_files(folder: Path) -> list[str]:
    out = []
    for root, dirs, files in os.walk(folder):
        dirs.sort()
        for name in sorted(files):
            out.append((Path(root) / name).relative_to(folder).as_posix())
    return sorted(out)


def _readme(manifest: dict) -> str:
    s = manifest["settings"]
    parts = manifest["partitions"]
    return (
        f"# krepp build {manifest['build_label']}\n\n"
        f"A krepp placement index for Qiita reference {manifest['reference_idx']}, registered by\n"
        "Qiita. Do not edit, add or remove files: a changed copy fails every job on the node\n"
        "it is on.\n\n"
        f"- krepp version: {s['krepp version']}\n"
        f"- k {s['k']}, w {s['w']}, h {s['h']}, m {s['m']}, frac {s['frac']}\n"
        f"- partitions: {len(parts)} ({', '.join(f'r{r}' for r in parts)})\n"
        f"- files: {len(manifest['files'])}, {sum(manifest['files'].values())} bytes\n\n"
        "## Checking a copy\n\n"
        "Run `./verify.sh <sha256>` in the folder, with the SHA256SUMS hash Qiita\n"
        "recorded for this build. It checks that SHA256SUMS is that file, that every\n"
        "file listed in it has its hash, and that no other file is present.\n"
    )


_VERIFY_SCRIPT = """#!/bin/sh
# Full check of one copy of a krepp build (see README.md). Usage: ./verify.sh <sha256>
# where <sha256> is the hash of SHA256SUMS that Qiita recorded for the build.
set -eu
cd "$(dirname "$0")"
want="${1:?usage: verify.sh <sha256 of SHA256SUMS>}"
got=$(sha256sum SHA256SUMS | cut -d' ' -f1)
[ "$got" = "$want" ] || { echo "verify.sh: SHA256SUMS is $got, expected $want" >&2; exit 1; }
sha256sum --check --quiet --strict SHA256SUMS
# sha256sum --check ignores files it is not told about; refuse them here.
listed=$(mktemp)
trap 'rm -f "$listed"' EXIT
cut -c67- SHA256SUMS | sort > "$listed"
extra=$(find . -type f ! -path ./SHA256SUMS | sed 's|^[.]/||' | sort | comm -23 - "$listed")
[ -z "$extra" ] || { echo "verify.sh: files not in SHA256SUMS: $extra" >&2; exit 1; }
echo "verify.sh: ok"
"""


def write_build_files(
    folder: Path, *, reference_idx: int, build_label: str, threads: int = 1
) -> tuple[str, dict]:
    """Inventory the folder, write manifest.json, README.md, verify.sh and then
    SHA256SUMS, and return (sha256 of SHA256SUMS, manifest). Reads every file once.
    Refuses a folder that already carries different Qiita files, which means it
    was edited after an earlier registration."""
    inventory = inventory_index(folder / KREPP_INDEX_SUBDIR)
    for rel in (JEST_LABELLED_TREE, JEST_NULL):
        if not (folder / rel).is_file():
            raise KreppBuildInvalid("inventory", f"{rel} is missing")
    ours = {SHA256SUMS, MANIFEST, README, VERIFY_SCRIPT}
    content = [rel for rel in _all_files(folder) if rel not in ours]
    allowed = {*inventory.files, JEST_LABELLED_TREE, JEST_NULL}
    stray = sorted(set(content) - allowed)
    if stray:
        raise KreppBuildInvalid("inventory", f"files that are not part of a build: {stray}")
    manifest = {
        "format": MANIFEST_FORMAT,
        "reference_idx": reference_idx,
        "build_label": build_label,
        "settings": inventory.settings,
        "m": inventory.m,
        "partitions": list(inventory.partitions),
        "tree_file": inventory.tree_file,
        "files": {rel: (folder / rel).stat().st_size for rel in content},
    }
    generated = {
        MANIFEST: json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        README: _readme(manifest),
        VERIFY_SCRIPT: _VERIFY_SCRIPT,
    }
    for name, text in generated.items():
        path = folder / name
        if path.exists() and path.read_text() != text:
            raise KreppBuildInvalid(
                "inventory",
                f"{name} exists with other content; the folder changed since it was registered",
            )
    for name, text in generated.items():
        (folder / name).write_text(text)
    (folder / VERIFY_SCRIPT).chmod(0o755)
    digests = _hash_all(folder, sorted([*content, *generated]), threads)
    sums = "".join(f"{digests[rel]}  {rel}\n" for rel in sorted(digests))
    sums_path = folder / SHA256SUMS
    if sums_path.exists() and sums_path.read_text() != sums:
        raise KreppBuildInvalid(
            "inventory",
            "SHA256SUMS exists with other content; the folder changed since it was registered",
        )
    sums_path.write_text(sums)
    return hashlib.sha256(sums.encode()).hexdigest(), manifest


def _parse_sums(text: str) -> dict[str, str]:
    out = {}
    for line in text.splitlines():
        digest, sep, rel = line.partition("  ")
        if not sep or len(digest) != 64:
            raise KreppBuildInvalid("sha256sums", f"line {line!r} is not `<sha256>  <path>`")
        out[rel] = digest
    return out


def verify_build(
    folder: Path,
    expected_sums_sha256: str,
    *,
    linked_krepp_version: str | None,
) -> dict:
    """The per-job check of one copy, cheap enough to run before every placement:
    SHA256SUMS is the registered one; the folder holds exactly the files it lists;
    every size matches the manifest; the small files (manifest, README, verify.sh,
    every metadata text and the tree) match their hashes; and the build's krepp
    version is the one miint links, when the caller passes it. The large index files
    are checked by size only; ``verify.sh`` hashes everything.

    Returns the manifest. Raises KreppBuildInvalid. A missing folder and one this
    process cannot read are reported apart."""
    try:
        sums_text = (folder / SHA256SUMS).read_text()
    except FileNotFoundError as exc:
        raise KreppBuildInvalid("missing", f"{folder / SHA256SUMS} does not exist") from exc
    except PermissionError as exc:
        raise KreppBuildInvalid("unreadable", f"{folder / SHA256SUMS}: {exc}") from exc
    got = hashlib.sha256(sums_text.encode()).hexdigest()
    if got != expected_sums_sha256:
        raise KreppBuildInvalid(
            "sha256sums", f"SHA256SUMS is {got}, the registered build's is {expected_sums_sha256}"
        )
    listed = _parse_sums(sums_text)
    present = set(_all_files(folder)) - {SHA256SUMS}
    if present != set(listed):
        raise KreppBuildInvalid(
            "files",
            f"missing {sorted(set(listed) - present)}, not listed {sorted(present - set(listed))}",
        )
    if sha256_file(folder / MANIFEST) != listed[MANIFEST]:
        raise KreppBuildInvalid("manifest", "manifest.json does not match SHA256SUMS")
    manifest = json.loads((folder / MANIFEST).read_text())
    for rel, size in manifest["files"].items():
        actual = (folder / rel).stat().st_size
        if actual != size:
            raise KreppBuildInvalid("size", f"{rel} is {actual} bytes, the manifest says {size}")
    small = [README, VERIFY_SCRIPT, manifest["tree_file"]] + [
        rel for rel in listed if rel.endswith(".txt") and rel.startswith(f"{KREPP_INDEX_SUBDIR}/")
    ]
    for rel in small:
        if sha256_file(folder / rel) != listed[rel]:
            raise KreppBuildInvalid("hash", f"{rel} does not match SHA256SUMS")
    if linked_krepp_version is not None:
        built = manifest["settings"]["krepp version"]
        if built != linked_krepp_version:
            raise KreppBuildInvalid(
                "krepp-version",
                f"the build was written by krepp {built};"
                f" the loaded miint links {linked_krepp_version}",
            )
    return manifest


def linked_krepp_version(conn) -> str:
    """krepp's own version string, as the loaded miint reports it: the `krepp-version`
    row of `miint_versions()`, the string an index records as `krepp version:`
    (https://the-miint.github.io/duckdb-miint/utilities/)."""
    row = conn.execute(
        "SELECT version FROM miint_versions() WHERE library = 'krepp-version'"
    ).fetchone()
    if row is None:
        raise RuntimeError(
            "the loaded miint reports no krepp-version row: built without krepp,"
            " or older than duckdb-miint#475"
        )
    return row[0]
