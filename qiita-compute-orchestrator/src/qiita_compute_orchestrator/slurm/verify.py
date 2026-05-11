"""Container-output verification gates.

Precondition (checked by the caller, not here): the SLURM job exited 0.
A non-zero exit jumps straight to a different failure path and never
reaches the verifier.

Per docs/architecture.md "Container contract", every workflow container
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
import stat
from dataclasses import dataclass
from pathlib import Path

# 0o440: owner read, group read, no other access. The data plane refuses
# to register files that aren't 440 (see qiita-data-plane file-mode
# check); the orchestrator enforces it earlier so a contract violation
# fails the step rather than failing later at registration time.
EXPECTED_FILE_MODE: int = 0o440

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


def verify_container_output(output_path: Path) -> list[VerificationFailure]:
    """Walk `output_path` and validate the container contract.

    Returns an empty list when all gates pass; otherwise a non-empty
    list with one VerificationFailure per problem found.

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

    manifest_path = output_path / "manifest.json"
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
        if not full.exists():
            failures.append(
                VerificationFailure(
                    reason=f"manifest.json `outputs.{name}` points at missing path",
                    detail=value,
                )
            )

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
        full = (output_path / relative).resolve()
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
        if not full.exists():
            failures.append(
                VerificationFailure(
                    reason="declared output file missing",
                    detail=relative,
                )
            )
            continue
        actual_size = full.stat().st_size
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
        (output_path / entry["path"]).resolve()
        for entry in files
        if isinstance(entry, dict) and isinstance(entry.get("path"), str)
    }
    declared_paths.add(manifest_path.resolve())
    for child in output_path.rglob("*"):
        if not child.is_file():
            continue
        resolved = child.resolve()
        if resolved not in declared_paths:
            failures.append(
                VerificationFailure(
                    reason="output file not listed in manifest",
                    detail=str(child.relative_to(output_path)),
                )
            )
            continue
        mode = stat.S_IMODE(child.stat().st_mode)
        if mode != EXPECTED_FILE_MODE:
            failures.append(
                VerificationFailure(
                    reason="output file has wrong mode",
                    detail=f"{child.relative_to(output_path)}: expected 0o440, got {mode:#o}",
                )
            )

    return failures


def parse_outputs_map(output_path: Path) -> dict[str, Path]:
    """Read the `outputs` map from `output_path/manifest.json` and
    return it as `{output_name: absolute_path}`. Caller must have run
    `verify_container_output` first and confirmed an empty failure
    list — this helper assumes the contract holds.

    Raises FileNotFoundError if manifest.json is missing or unreadable.
    Caller is the orchestrator's run_step, which catches and wraps as
    CONTRACT_VIOLATION (the verifier should already have caught this,
    but defense in depth)."""
    manifest = json.loads((output_path / "manifest.json").read_text())
    return {name: (output_path / value).resolve() for name, value in manifest["outputs"].items()}
