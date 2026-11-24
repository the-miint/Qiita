"""Sealed folders under PATH_DERIVED, and the node-local cache jobs read them through.

A **sealed folder** is a folder Qiita has listed and hashed: ``manifest.json`` (the
caller's fields plus every file's size) and ``SHA256SUMS`` (every file but itself, in
``sha256sum -c`` format). The sha256 of ``SHA256SUMS`` is recorded in the database and
names the folder's exact content. ``seal`` writes the two files; ``verify_sealed`` is
the cheap check of a copy (the ``SHA256SUMS`` hash, the file list, every size).

The **node cache** is ``PATH_NODE_LOCAL``, a directory on each compute node's own disk.
This module is the only code that writes to it. ``materialize`` returns where a job
reads a sealed folder from:

- ``PATH_NODE_LOCAL`` unset: the master copy under ``PATH_DERIVED``, after
  ``verify_sealed``.
- set: the node's copy at the same relative path, made on first use. Jobs on one node
  take a per-folder ``flock`` in turn. The first runs ``verify_sealed`` on the
  master, checks the free space against the manifest's sizes, copies into a hidden
  ``.partial`` folder while hashing every file against ``SHA256SUMS``, copies
  ``manifest.json`` last and renames the folder into place, so a folder at
  the target path was always written whole. A job that waited finds the copy and
  runs ``verify_sealed`` on it. A disk that fills during the copy fails the job
  with the same ``space`` error as the check before the copy.

``evict`` deletes the node copies the caller names. The lock covers making a copy,
not reading it, so the caller names only folders no job can still read. The cluster admins only
create ``PATH_NODE_LOCAL`` on each node, writable by the job user; nothing is copied
by hand.
"""

from __future__ import annotations

import errno
import fcntl
import hashlib
import json
import os
import shutil
import sys
from collections.abc import Iterable, Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path

from .config import get_settings
from .derived_store import relative_path_under

SHA256SUMS = "SHA256SUMS"
MANIFEST = "manifest.json"
# Lock files, one per cached folder, under PATH_NODE_LOCAL.
LOCK_DIR = ".locks"
_PARTIAL_SUFFIX = ".partial"
_CHUNK = 8 << 20

# `probe`'s exit codes; the readiness probe script maps each to the node-cache row.
PROBE_OK = 0
PROBE_FAIL = 1
PROBE_SKIP = 3


class SealedFolderInvalid(ValueError):
    """A folder is not the sealed folder it should be. `check` names the check that
    failed (stable, for logs); `detail` says what was found."""

    def __init__(self, check: str, detail: str):
        self.check = check
        self.detail = detail
        super().__init__(f"sealed folder check {check!r} failed: {detail}")


class NodeCacheError(RuntimeError):
    """The node cache could not hold a copy: `check` is `space` (not enough free space,
    before or during the copy) or `root` (PATH_NODE_LOCAL missing or not writable)."""

    def __init__(self, check: str, detail: str):
        self.check = check
        self.detail = detail
        super().__init__(f"node cache {check!r}: {detail}")


def default_threads() -> int:
    """Threads for hashing and copying: the job's SLURM CPUs, else the host's."""
    return max(1, int(os.environ.get("SLURM_CPUS_PER_TASK", "0")) or (os.cpu_count() or 1))


def all_files(folder: Path) -> list[str]:
    """Every file under `folder`, relative and sorted. A directory os.walk cannot list
    raises SealedFolderInvalid (`unreadable`); os.walk alone skips it. A symlink
    raises (`link`): a copied link can point back at the shared filesystem or dangle
    on a node, and os.walk does not descend into a linked directory."""

    def _unreadable(exc: OSError) -> None:
        raise SealedFolderInvalid("unreadable", f"{exc.filename}: {exc.strerror}") from exc

    out = []
    for root, dirs, files in os.walk(folder, onerror=_unreadable):
        dirs.sort()
        links = [n for n in (*dirs, *files) if os.path.islink(os.path.join(root, n))]
        if links:
            rel = sorted((Path(root) / n).relative_to(folder).as_posix() for n in links)
            raise SealedFolderInvalid("link", f"symlinks are not part of a sealed folder: {rel}")
        for name in files:
            out.append((Path(root) / name).relative_to(folder).as_posix())
    return sorted(out)


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        while chunk := fh.read(_CHUNK):
            h.update(chunk)
    return h.hexdigest()


def _hash_all(folder: Path, relpaths: list[str], threads: int) -> dict[str, str]:
    # hashlib releases the GIL on large updates, so threads hash files in parallel.
    with ThreadPoolExecutor(max_workers=max(1, threads)) as pool:
        return dict(
            zip(relpaths, pool.map(lambda r: sha256_file(folder / r), relpaths), strict=True)
        )


def seal(folder: Path, fields: dict, *, threads: int = 1) -> tuple[str, dict]:
    """Write `manifest.json` (`fields` plus `files`: every file's size) and then
    `SHA256SUMS`; return (sha256 of SHA256SUMS, manifest). Reads every file once.
    Refuses a folder already sealed with other content, which means it changed after
    it was sealed. Everything written is a function of the folder's content and
    `fields`, so sealing the same folder twice gives the same hash."""
    if "files" in fields:
        raise ValueError("`files` is the manifest key seal() fills in")
    content = [rel for rel in all_files(folder) if rel not in (SHA256SUMS, MANIFEST)]
    manifest = {**fields, "files": {rel: (folder / rel).stat().st_size for rel in content}}
    text = json.dumps(manifest, indent=2, sort_keys=True) + "\n"
    path = folder / MANIFEST
    if path.exists() and path.read_text() != text:
        raise SealedFolderInvalid(
            "sealed",
            f"{MANIFEST} exists with other content; the folder changed since it was sealed",
        )
    path.write_text(text)
    digests = _hash_all(folder, sorted([*content, MANIFEST]), threads)
    sums = "".join(f"{digests[rel]}  {rel}\n" for rel in sorted(digests))
    sums_path = folder / SHA256SUMS
    if sums_path.exists() and sums_path.read_text() != sums:
        raise SealedFolderInvalid(
            "sealed",
            f"{SHA256SUMS} exists with other content; the folder changed since it was sealed",
        )
    sums_path.write_text(sums)
    return hashlib.sha256(sums.encode()).hexdigest(), manifest


def _parse_sums(text: str) -> dict[str, str]:
    out = {}
    for line in text.splitlines():
        digest, sep, rel = line.partition("  ")
        if not sep or len(digest) != 64:
            raise SealedFolderInvalid("sha256sums", f"line {line!r} is not `<sha256>  <path>`")
        out[rel] = digest
    return out


def _read_sums(folder: Path, expected_sums_sha256: str) -> dict[str, str]:
    try:
        text = (folder / SHA256SUMS).read_text()
    except FileNotFoundError as exc:
        raise SealedFolderInvalid("missing", f"{folder / SHA256SUMS} does not exist") from exc
    except PermissionError as exc:
        raise SealedFolderInvalid("unreadable", f"{folder / SHA256SUMS}: {exc}") from exc
    got = hashlib.sha256(text.encode()).hexdigest()
    if got != expected_sums_sha256:
        raise SealedFolderInvalid(
            "sha256sums", f"{SHA256SUMS} is {got}, the recorded one is {expected_sums_sha256}"
        )
    return _parse_sums(text)


def _check_file_list(folder: Path, listed: dict[str, str]) -> None:
    """The folder holds exactly the files SHA256SUMS lists, and no symlink."""
    present = set(all_files(folder)) - {SHA256SUMS}
    if present != set(listed):
        raise SealedFolderInvalid(
            "files",
            f"missing {sorted(set(listed) - present)}, not listed {sorted(present - set(listed))}",
        )


def _present(path: Path) -> bool:
    """Whether `path` exists. Unlike Path.exists, a path that cannot be looked up
    (a directory above it without search permission) raises instead of reading as
    absent."""
    try:
        path.lstat()
    except FileNotFoundError:
        return False
    return True


def verify_sealed(folder: Path, expected_sums_sha256: str) -> dict:
    """The cheap check of a copy: SHA256SUMS is the recorded one, the folder holds
    exactly the files it lists, manifest.json matches its hash, and every size matches
    the manifest. Large files are not re-hashed; `materialize` hashes every file when
    it copies. Returns the manifest. Raises SealedFolderInvalid."""
    listed = _read_sums(folder, expected_sums_sha256)
    _check_file_list(folder, listed)
    if sha256_file(folder / MANIFEST) != listed[MANIFEST]:
        raise SealedFolderInvalid("manifest", f"{MANIFEST} does not match {SHA256SUMS}")
    manifest = json.loads((folder / MANIFEST).read_text())
    for rel, size in manifest["files"].items():
        actual = (folder / rel).stat().st_size
        if actual != size:
            raise SealedFolderInvalid("size", f"{rel} is {actual} bytes, the manifest says {size}")
    return manifest


def _log(msg: str) -> None:
    print(f"node_cache: {msg}", file=sys.stderr, flush=True)


@contextmanager
def _locked(root: Path, relpath: str) -> Iterator[None]:
    """Hold the folder's lock: an exclusive flock on a file under ROOT/.locks, named
    by the sha256 of `relpath`. Blocks until any other job on this node releases it."""
    lock_dir = root / LOCK_DIR
    lock_dir.mkdir(exist_ok=True)
    path = lock_dir / f"{hashlib.sha256(relpath.encode()).hexdigest()}.lock"
    with path.open("a") as fh:
        try:
            fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            _log(f"waiting for another job on this node to finish with {relpath}")
            fcntl.flock(fh, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(fh, fcntl.LOCK_UN)


def _partial(target: Path) -> Path:
    return target.parent / f".{target.name}{_PARTIAL_SUFFIX}"


def _copy_hashed(src: Path, dst: Path, want: str) -> None:
    """Copy one file, hashing it as it is written; raise when the hash differs."""
    h = hashlib.sha256()
    with src.open("rb") as fin, dst.open("wb") as fout:
        while chunk := fin.read(_CHUNK):
            h.update(chunk)
            fout.write(chunk)
    if h.hexdigest() != want:
        raise SealedFolderInvalid("hash", f"{src} does not match {SHA256SUMS}")


def _copy(master: Path, target: Path, expected_sums_sha256: str, threads: int) -> None:
    manifest = verify_sealed(master, expected_sums_sha256)
    listed = _read_sums(master, expected_sums_sha256)
    files = [rel for rel in listed if rel != MANIFEST]
    need = sum(manifest["files"].values()) + sum(
        (master / n).stat().st_size for n in (MANIFEST, SHA256SUMS)
    )
    partial = _partial(target)
    if _present(partial):
        _log(f"deleting {partial}, left by a job that stopped while copying")
        shutil.rmtree(partial)
    target.parent.mkdir(parents=True, exist_ok=True)
    free = shutil.disk_usage(target.parent).free
    if free < need:
        raise NodeCacheError(
            "space", f"copying {master} needs {need} bytes; {target.parent} has {free} free"
        )
    _log(f"copying {master} ({need} bytes) to {target}")
    try:
        partial.mkdir()
        for d in sorted({str(Path(rel).parent) for rel in files} - {"."}):
            (partial / d).mkdir(parents=True, exist_ok=True)
        pool = ThreadPoolExecutor(max_workers=max(1, threads))
        try:
            futures = [pool.submit(_copy_hashed, master / r, partial / r, listed[r]) for r in files]
            for future in futures:
                future.result()
        finally:
            # On the first failure, files not yet started are not copied.
            pool.shutdown(cancel_futures=True)
        shutil.copyfile(master / SHA256SUMS, partial / SHA256SUMS)
        _copy_hashed(master / MANIFEST, partial / MANIFEST, listed[MANIFEST])
        # The folder appears at its path only here, after every file is written.
        partial.rename(target)
    except OSError as exc:
        shutil.rmtree(partial, ignore_errors=True)
        if exc.errno == errno.ENOSPC:
            raise NodeCacheError(
                "space", f"{target.parent} filled while copying {master} ({need} bytes)"
            ) from exc
        raise
    except BaseException:
        shutil.rmtree(partial, ignore_errors=True)
        raise
    _log(f"copied {master} to {target}")


def _node_root() -> Path | None:
    raw = get_settings().path_node_local
    if not raw:
        return None
    root = Path(raw)
    if not root.is_dir() or not os.access(root, os.W_OK | os.X_OK):
        raise NodeCacheError(
            "root",
            f"PATH_NODE_LOCAL {root} is not a writable directory on this node",
        )
    return root


def evict(relpaths: Iterable[str]) -> None:
    """Delete this node's copies of `relpaths` (folders whose build was removed),
    each under its lock. A no-op without PATH_NODE_LOCAL. The caller makes sure no job
    on any node still reads them; the lock does not."""
    root = _node_root()
    if root is None:
        return
    for relpath in relpaths:
        target = relative_path_under(root, relpath)
        with _locked(root, relpath):
            for path in (target, _partial(target)):
                if _present(path):
                    _log(f"deleting {path}")
                    shutil.rmtree(path)


def materialize(
    relpath: str,
    expected_sums_sha256: str,
    *,
    evict_first: Iterable[str] = (),
    threads: int | None = None,
) -> Path:
    """Where this job reads the sealed folder at `relpath` from (see the module
    docstring). `evict_first` is passed to `evict` before anything is copied, so a
    removed folder's space is free for this one. Raises SealedFolderInvalid when the
    master or a copy is not the recorded folder, NodeCacheError when the node cannot
    hold it."""
    master = relative_path_under(get_settings().path_derived, relpath)
    root = _node_root()
    if root is None:
        verify_sealed(master, expected_sums_sha256)
        return master
    evict(evict_first)
    target = relative_path_under(root, relpath)
    with _locked(root, relpath):
        if _present(target):
            try:
                verify_sealed(target, expected_sums_sha256)
                return target
            except SealedFolderInvalid as exc:
                _log(f"{target} is not the recorded copy ({exc}); copying it again")
                shutil.rmtree(target)
        _copy(master, target, expected_sums_sha256, threads or default_threads())
    return target


def probe() -> tuple[int, str]:
    """The compute-readiness `node-cache` row for this node: (exit code, detail).
    PROBE_OK, PROBE_SKIP (PATH_NODE_LOCAL unset: jobs read from PATH_DERIVED),
    PROBE_FAIL (set but not a writable directory here: every job that reads through
    the cache would fail on this node)."""
    raw = get_settings().path_node_local
    if not raw:
        return PROBE_SKIP, "PATH_NODE_LOCAL is not set; jobs read sealed folders from PATH_DERIVED"
    try:
        root = _node_root()
    except NodeCacheError as exc:
        return PROBE_FAIL, exc.detail
    return PROBE_OK, f"{root} writable, {shutil.disk_usage(root).free} bytes free"


if __name__ == "__main__":
    code, detail = probe()
    print(detail)
    sys.exit(code)
