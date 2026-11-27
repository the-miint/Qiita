"""Container-output verification gates.

Precondition (checked by the caller, not here): the SLURM job exited 0.
A non-zero exit jumps straight to a different failure path and never
reaches the verifier.

Per docs/architecture/processing.md "Container contract", every workflow container
must also produce on success:

  1. `$QIITA_OUTPUT_PATH/manifest.json`, written as the container's
     final act before chmod (its presence is the completion marker):
         {
           "files": [{"path": "output.parquet", "size_bytes": 12345}, ...],
           "outputs": {"manifest": "output.parquet", ...}
         }
     `files` lists every output file with its size in bytes.
     `outputs` maps the YAML step's declared `outputs:` names to
     relative paths under $QIITA_OUTPUT_PATH (use "." for the
     directory itself when an output IS the directory, e.g. a step
     whose YAML output is `staging_dir`).
  2. Every listed file exists with the declared size_bytes.
  3. All files under `$QIITA_OUTPUT_PATH` are mode `0o440`
     (owner-and-group read-only).

Gate failures are CONTRACT_VIOLATION (permanent) — the container
returned exit 0 but didn't honor the contract, so retry won't help.

`size_bytes` is a format-agnostic stat() check. Parquet self-validates
truncation via its trailing PAR1 magic, but non-parquet outputs
(.nwk, .jplace, .tsv, future HTML/JSON/logs) have no equivalent.
"""

from __future__ import annotations

import json
import os
import stat
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

from .contract import EXPECTED_FILE_MODE, MANIFEST_FILENAME

__all__ = [
    # Re-exported for backward compatibility with `from .verify import
    # EXPECTED_FILE_MODE`; the canonical home is now `slurm/contract.py`.
    "EXPECTED_FILE_MODE",
    "VerificationFailure",
    "parse_outputs_map",
    "verify_container_output",
]

# Size cap on the manifest itself. A pathologically large manifest
# (tens of MB) would slow ops dashboards and indicate a malformed
# container. Real manifests are <1 KB; 1 MiB is more than enough head
# room.
_MANIFEST_MAX_BYTES = 1 * 1024 * 1024


@dataclass(frozen=True, slots=True)
class VerificationFailure:
    """One container-contract violation. Multiple may be reported per
    job — a missing manifest blocks the rest, but with a manifest
    present a per-file walk can surface several wrong-size or
    wrong-mode files in one go."""

    reason: str  # human-readable, drives BackendFailure.reason
    detail: str | None = None


def verify_container_output(
    output_path: Path, *, recovered: bool = False
) -> list[VerificationFailure]:
    """Walk `output_path` and validate the container contract.

    Returns an empty list when all gates pass; otherwise a non-empty
    list with one VerificationFailure per problem found.

    `recovered` re-reads a step that passed every check when it finished, for a
    resume or a redrive: only the manifest and its `outputs` map are checked, with each
    declared output still present. The per-file checks (each listed file at its
    size; every file listed and read-only) are skipped, because a later step may
    have moved the files the manifest lists — `register-files` moves its staging
    files into the lake.

    Caller is responsible for ensuring the SLURM job exited 0 before
    calling this (gate 1 lives outside the file-system surface).
    """
    failures: list[VerificationFailure] = []

    if not output_path.exists():
        return [
            VerificationFailure(
                reason="$QIITA_OUTPUT_PATH does not exist",
                detail=str(output_path),
            )
        ]
    if not output_path.is_dir():
        return [
            VerificationFailure(
                reason="$QIITA_OUTPUT_PATH is not a directory",
                detail=str(output_path),
            )
        ]
    # Resolve once so per-iteration path-traversal checks don't re-stat
    # the directory (and chase symlinks) on every entry.
    output_path_resolved = output_path.resolve()

    manifest_path = output_path / MANIFEST_FILENAME
    if not manifest_path.exists():
        return [
            VerificationFailure(
                reason="manifest.json missing",
                detail=str(manifest_path),
            )
        ]

    try:
        size = manifest_path.stat().st_size
    except OSError as exc:
        return [
            VerificationFailure(
                reason="cannot stat manifest.json",
                detail=str(exc),
            )
        ]
    if size > _MANIFEST_MAX_BYTES:
        return [
            VerificationFailure(
                reason="manifest.json exceeds size cap",
                detail=f"{size} bytes > {_MANIFEST_MAX_BYTES} bytes",
            )
        ]

    try:
        manifest_data = json.loads(manifest_path.read_text())
    except json.JSONDecodeError as exc:
        return [
            VerificationFailure(
                reason="manifest.json is not valid JSON",
                detail=str(exc),
            )
        ]
    if not isinstance(manifest_data, dict) or "files" not in manifest_data:
        if isinstance(manifest_data, dict):
            shape = f"got top-level keys: {sorted(manifest_data)}"
        else:
            shape = f"got top-level type: {type(manifest_data).__name__}"
        return [
            VerificationFailure(
                reason="manifest.json must be an object with a `files` key",
                detail=shape,
            )
        ]
    files = manifest_data["files"]
    if not isinstance(files, list):
        return [
            VerificationFailure(
                reason="manifest.json `files` must be an array",
                detail=f"got {type(files).__name__}",
            )
        ]
    outputs = manifest_data.get("outputs")
    if not isinstance(outputs, dict):
        return [
            VerificationFailure(
                reason="manifest.json `outputs` must be an object {output_name: relative path}",
                detail=f"got {type(outputs).__name__}",
            )
        ]
    for name, value in outputs.items():
        if not isinstance(name, str) or not name:
            failures.append(
                VerificationFailure(
                    reason="manifest.json `outputs` has an empty or non-string key",
                    detail=repr(name),
                )
            )
            continue
        if not isinstance(value, str):
            failures.append(
                VerificationFailure(
                    reason=f"manifest.json `outputs.{name}` must be a string path",
                    detail=f"got {type(value).__name__}",
                )
            )
            continue
        # Resolve relative to output_path; reject path traversal.
        full = (output_path / value).resolve()
        try:
            full.relative_to(output_path_resolved)
        except ValueError:
            failures.append(
                VerificationFailure(
                    reason=f"manifest.json `outputs.{name}` escapes $QIITA_OUTPUT_PATH",
                    detail=value,
                )
            )
            continue
        looked = _stat_or_error(full)
        if looked is None:
            failures.append(
                VerificationFailure(
                    reason=f"manifest.json `outputs.{name}` points at missing path",
                    detail=value,
                )
            )
        elif isinstance(looked, OSError):
            failures.append(
                VerificationFailure(
                    reason=f"manifest.json `outputs.{name}` cannot be read",
                    detail=f"{value}: [Errno {looked.errno}] {looked.strerror}",
                )
            )

    if recovered:
        return failures

    tree, unreadable = _walk_output_tree(output_path)
    # A directory or entry the walk cannot read hides whatever files are in or
    # behind it from gate 3, so it fails the output rather than being skipped.
    for relative, exc in unreadable:
        failures.append(
            VerificationFailure(
                reason="output entry cannot be read",
                detail=f"{relative}: [Errno {exc.errno}] {exc.strerror}",
            )
        )

    def plain_entry(relative: str) -> tuple[tuple[str, ...], _WalkedEntry] | None:
        """The walked entry `relative` names when the walk reached it as a
        non-symlink, with its parts; None when the path needs a real `resolve()`."""
        parts = _plain_relative_parts(relative)
        walked = tree.get(parts) if parts is not None else None
        if walked is None or walked.is_symlink:
            return None
        return parts, walked

    def canonical(relative: str) -> Path:
        """`(output_path / relative).resolve()`, without the syscalls when the
        walk already shows the path is a non-symlink entry under the output dir."""
        plain = plain_entry(relative)
        if plain is None:
            return (output_path / relative).resolve()
        return output_path_resolved.joinpath(*plain[0])

    # Gate 2: every listed file exists with declared size.
    for i, entry in enumerate(files):
        if not isinstance(entry, dict):
            failures.append(
                VerificationFailure(
                    reason=f"manifest entry {i} is not an object",
                    detail=str(entry)[:200],
                )
            )
            continue
        relative = entry.get("path")
        declared_size = entry.get("size_bytes")
        if not isinstance(relative, str) or not relative:
            failures.append(
                VerificationFailure(
                    reason=f"manifest entry {i} missing or invalid `path`",
                    detail=str(entry)[:200],
                )
            )
            continue
        if not isinstance(declared_size, int) or declared_size < 0:
            failures.append(
                VerificationFailure(
                    reason=f"manifest entry {i} missing or invalid `size_bytes`",
                    detail=str(entry)[:200],
                )
            )
            continue
        # Resolve relative to output_path; reject path traversal that
        # would let the container declare a file outside its scope.
        full = canonical(relative)
        try:
            full.relative_to(output_path_resolved)
        except ValueError:
            failures.append(
                VerificationFailure(
                    reason=f"manifest entry {i} path escapes $QIITA_OUTPUT_PATH",
                    detail=relative,
                )
            )
            continue
        plain = plain_entry(relative)
        if plain is not None and plain[1].file_stat is not None:
            actual_size = plain[1].file_stat.st_size
        else:
            looked = _stat_or_error(full)
            if looked is None:
                failures.append(
                    VerificationFailure(
                        reason="declared output file missing",
                        detail=relative,
                    )
                )
                continue
            if isinstance(looked, OSError):
                failures.append(
                    VerificationFailure(
                        reason="declared output file cannot be read",
                        detail=f"{relative}: [Errno {looked.errno}] {looked.strerror}",
                    )
                )
                continue
            actual_size = looked.st_size
        if actual_size != declared_size:
            failures.append(
                VerificationFailure(
                    reason="declared size_bytes mismatches actual file size",
                    detail=f"{relative}: declared {declared_size}, actual {actual_size}",
                )
            )

    # Gate 3: every file under output_path is mode 0o440. We walk the
    # directory rather than only the manifest list — a container that
    # writes extra files (debug logs, partial outputs) is also a
    # contract violation; the manifest must list everything.
    declared_paths = {
        canonical(entry["path"])
        for entry in files
        if isinstance(entry, dict) and isinstance(entry.get("path"), str)
    }
    declared_paths.add(canonical(MANIFEST_FILENAME))
    for parts, walked in sorted(tree.items()):
        if walked.file_stat is None:
            continue
        relative = "/".join(parts)
        if canonical(relative) not in declared_paths:
            failures.append(
                VerificationFailure(
                    reason="output file not listed in manifest",
                    detail=relative,
                )
            )
            continue
        mode = stat.S_IMODE(walked.file_stat.st_mode)
        if mode != EXPECTED_FILE_MODE:
            failures.append(
                VerificationFailure(
                    reason="output file has wrong mode",
                    detail=f"{relative}: expected 0o440, got {mode:#o}",
                )
            )

    return failures


@dataclass(frozen=True, slots=True)
class _WalkedEntry:
    is_symlink: bool
    # The followed stat when the entry is (or links to) a regular file, else None.
    file_stat: os.stat_result | None


def _walk_output_tree(
    output_path: Path,
) -> tuple[dict[tuple[str, ...], _WalkedEntry], list[tuple[str, OSError]]]:
    """Every non-directory entry under `output_path`, keyed by its relative parts,
    and each relative path (`.` for `output_path`) the walk could not read.

    Lists dotfiles, and lists a symlink without descending into it when it points
    at a directory. One `scandir` per directory and one `stat` per regular file, so
    a caller can resolve a path only where a symlink is involved. A dangling
    symlink is an entry with no file stat, not an unreadable one.
    """
    entries: dict[tuple[str, ...], _WalkedEntry] = {}
    unreadable: list[tuple[str, OSError]] = []
    pending: list[tuple[Path, tuple[str, ...]]] = [(output_path, ())]
    while pending:
        directory, prefix = pending.pop()
        try:
            with os.scandir(directory) as scan:
                listed = list(scan)
        except OSError as exc:
            unreadable.append(("/".join(prefix) or ".", exc))
            continue
        for dir_entry in listed:
            parts = (*prefix, dir_entry.name)
            try:
                if dir_entry.is_dir(follow_symlinks=False):
                    pending.append((Path(dir_entry.path), parts))
                    continue
                entries[parts] = _WalkedEntry(
                    is_symlink=dir_entry.is_symlink(),
                    file_stat=dir_entry.stat() if dir_entry.is_file() else None,
                )
            except OSError as exc:
                unreadable.append(("/".join(parts), exc))
    return entries, unreadable


def _stat_or_error(path: Path) -> os.stat_result | OSError | None:
    """`path.stat()`, following symlinks. None when the path or one of its parents
    does not exist; the `OSError` for any other failure, such as a parent directory
    that cannot be traversed."""
    try:
        return path.stat()
    except FileNotFoundError, NotADirectoryError:
        return None
    except OSError as exc:
        return exc


def _plain_relative_parts(relative: str) -> tuple[str, ...] | None:
    """`relative`'s parts when it names a path inside the directory without `..`
    or an absolute root, else None (that path needs a real `resolve()`)."""
    parts = PurePosixPath(relative).parts
    if not parts or parts[0] == "/" or ".." in parts:
        return None
    return parts


def parse_outputs_map(output_path: Path) -> dict[str, Path]:
    """Read the `outputs` map from `output_path/manifest.json` and
    return it as `{output_name: absolute_path}`. Caller must have run
    `verify_container_output` first and confirmed an empty failure
    list — this helper assumes the contract holds.

    Raises FileNotFoundError if manifest.json is missing or unreadable.
    Caller is the orchestrator's result_step, which catches and wraps as
    CONTRACT_VIOLATION (the verifier should already have caught this,
    but defense in depth)."""
    manifest = json.loads((output_path / MANIFEST_FILENAME).read_text())
    return {name: (output_path / value).resolve() for name, value in manifest["outputs"].items()}
