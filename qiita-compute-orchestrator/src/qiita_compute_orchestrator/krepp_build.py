"""A krepp build folder: what is in it, the files Qiita writes into it, and the checks.

A build is made outside Qiita and placed, by hand, at its master path under
PATH_DERIVED (``derived_store.krepp_build_relpath``). Registration
(``jobs.krepp_index_register``) inventories it and writes four files beside the
krepp files; the cluster admins then copy the whole folder to every compute node's
PATH_NODE_LOCAL; every job that reads a node copy first runs ``verify_build`` on it.

Layout::

    README.md       for people: what the build is and how to check a copy
    SHA256SUMS      `sha256sum -c` format, every file but itself
    manifest.json   the recorded index settings, every file's size
    verify.sh       the full check of one copy, run by the cluster admins
    index/          the krepp files as krepp wrote them (`place_krepp`'s index directory)
    jest/           labelled.nwk (the backbone tree with JEST's clade labels), null.txt

Of krepp's files Qiita reads two kinds: the ``metadata-*.txt`` texts (``key: value``
lines carrying RECORDED_METADATA_KEYS) and the ``tree-*`` files; a test pins both
against an index built by the staged miint. Everything Qiita writes is a
function of the folder's content only, so registering the same folder twice yields
the same ``SHA256SUMS`` and the same hash.
"""

from __future__ import annotations

import hashlib
import json
import os
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path

import duckdb

from .derived_store import KREPP_INDEX_SUBDIR

SHA256SUMS = "SHA256SUMS"
MANIFEST = "manifest.json"
README = "README.md"
VERIFY_SCRIPT = "verify.sh"
JEST_SUBDIR = "jest"
JEST_LABELLED_TREE = f"{JEST_SUBDIR}/labelled.nwk"
JEST_NULL = f"{JEST_SUBDIR}/null.txt"
MANIFEST_FORMAT = 1

# The metadata keys Qiita records for a build (manifest, README, reference_index.params)
# and checks (krepp version), read from every `metadata-*.txt` in `index/`.
RECORDED_METADATA_KEYS = ("krepp version", "k", "w", "h", "m", "frac")

_HASH_CHUNK = 8 << 20


# The `check` of a KreppBuildInvalid raised because the build's krepp is not the linked
# one. The readiness probe fails on it rather than warning, so it is compared by name.
CHECK_KREPP_VERSION = "krepp-version"


class KreppBuildInvalid(ValueError):
    """The folder is not the build it should be. `check` names the check that failed
    (stable, for logs and the readiness probe); `detail` says what was found."""

    def __init__(self, check: str, detail: str):
        self.check = check
        self.detail = detail
        super().__init__(f"krepp build check {check!r} failed: {detail}")


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
            "SELECT key, list(DISTINCT"
            "  regexp_extract(content, '(?m)^' || key || ': ([^\\r\\n]*)', 1))"
            " FROM read_text(?) CROSS JOIN unnest(?::VARCHAR[]) AS t(key)"
            " GROUP BY key",
            [str(index_dir / "metadata-*.txt"), list(RECORDED_METADATA_KEYS)],
        ).fetchall()
    if not rows:
        raise KreppBuildInvalid("metadata", f"{index_dir} holds no metadata-*.txt")
    values = dict(rows)
    settings: dict[str, str] = {}
    for key in RECORDED_METADATA_KEYS:
        found = sorted(values[key])
        if "" in found:
            raise KreppBuildInvalid("metadata", f"a metadata text lacks {key!r}")
        if len(found) != 1:
            raise KreppBuildInvalid("metadata", f"the metadata texts disagree on {key!r}: {found}")
        settings[key] = found[0]
    return settings


def inventory_index(index_dir: Path) -> Inventory:
    """Read `index/` for what registration records. Whether the files make a complete
    krepp index is krepp's to say; Qiita reads the settings it records and picks the
    tree it compares with the reference's, and refuses `tree-*` files that differ,
    since it compares only one. Raises KreppBuildInvalid."""
    if not index_dir.is_dir():
        raise KreppBuildInvalid("inventory", f"{index_dir} is not a directory")
    names = _all_files(index_dir)
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
    """Every file under `folder`, relative. A directory os.walk cannot list raises
    KreppBuildInvalid (`unreadable`); os.walk alone skips it, which would drop its
    files from both the stray-file check and SHA256SUMS."""

    def _unreadable(exc: OSError) -> None:
        raise KreppBuildInvalid("unreadable", f"{exc.filename}: {exc.strerror}") from exc

    out = []
    for root, dirs, files in os.walk(folder, onerror=_unreadable):
        dirs.sort()
        for name in sorted(files):
            out.append((Path(root) / name).relative_to(folder).as_posix())
    return sorted(out)


def _readme(manifest: dict) -> str:
    s = manifest["settings"]
    return (
        f"# krepp build {manifest['build_label']}\n\n"
        f"A krepp placement index for Qiita reference {manifest['reference_idx']}, registered by\n"
        "Qiita. Do not edit, add or remove files: a changed copy fails every krepp job on\n"
        "the node it is on.\n\n"
        f"- krepp version: {s['krepp version']}\n"
        f"- k {s['k']}, w {s['w']}, h {s['h']}, m {s['m']}, frac {s['frac']}\n"
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
        check_krepp_version(manifest["settings"]["krepp version"], linked_krepp_version)
    return manifest


def check_krepp_version(built: str, linked: str) -> None:
    """Raise KreppBuildInvalid (`CHECK_KREPP_VERSION`) when the build's krepp version is
    not the one the loaded miint links."""
    if built != linked:
        raise KreppBuildInvalid(
            CHECK_KREPP_VERSION,
            f"the build was written by krepp {built}; the loaded miint links {linked}",
        )


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
