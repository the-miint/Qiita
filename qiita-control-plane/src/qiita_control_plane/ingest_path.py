"""Bound which host paths a submitted `action_context` may name.

A work_ticket's `action_context` can carry absolute host paths that a SLURM
step re-opens much later (`bcl_input_dir`, `bam_path`, `fastq_path`, the
`local-(host-)reference-add` `*_path` set). The path is recorded verbatim and
resolved on a compute node, so a path that resolves on the submitting machine
and nowhere else is accepted at submit and fails inside the job.

Two rules, applied by `resolve_ingest_path`:

  * the path resolves under one of `Settings.path_ingest_roots`
    (`PATH_INGEST_ROOTS`), and
  * it exists, when the control plane has enough traversal rights to tell.

The second rule is deliberately conditional. The control plane runs as
`qiita-api` and SLURM steps run as `qiita-job`, two accounts with different
group membership, so a directory the job can read may not be one the control
plane can stat. `_probe` distinguishes "the parent was traversable and the
entry is not there" (definitive — reject) from "permission denied" (unknown —
admit, and let the step report it). Reporting a permission error as a missing
path would refuse submissions that would have run.

The roots are also the reach bound: without them, any absolute path the
orchestrator can open is nameable through the API.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

from qiita_common.actions import HOST_PATH_KEY_SUFFIXES, UPLOAD_IDX_SUFFIX

# A `context_schema` property is a host path iff it is a string constrained to
# start with `/`. Every shipped host-path property carries the pair, and a
# `HOST_PATH_KEY_SUFFIXES`-named property that omits the pattern fails CI.
_ABSOLUTE_PATH_PATTERN = "^/"


class IngestPathError(ValueError):
    """A submitted host path violated one of the two rules above.

    Carries the offending value and the roots so the caller can render a 422
    body naming both, rather than a bare message.
    """

    def __init__(self, reason: str, *, path: str, roots: tuple[Path, ...]) -> None:
        super().__init__(reason)
        self.reason = reason
        self.path = path
        self.roots = roots


@dataclass(frozen=True)
class HostPathKeys:
    """The `action_context` keys an action's `context_schema` declares as host
    paths, and the ones it declares as upload handles.

    Both are derived from the schema rather than from a hardcoded list so a new
    workflow field is covered the day it ships.
    """

    paths: tuple[str, ...]
    uploads: tuple[str, ...]


def host_path_keys(context_schema: dict[str, Any]) -> HostPathKeys:
    """Read an action's `context_schema` for the keys that name a host path and
    the keys that name an upload handle.

    A property is a host path when it is `type: string` with
    `pattern: "^/"`; an upload handle when its name ends in `_upload_idx`.
    A schema with no `properties` (the permissive `{}` default) yields neither.
    """
    properties = context_schema.get("properties")
    if not isinstance(properties, dict):
        return HostPathKeys(paths=(), uploads=())
    paths = tuple(
        sorted(
            name
            for name, spec in properties.items()
            if isinstance(spec, dict)
            and spec.get("type") == "string"
            and spec.get("pattern") == _ABSOLUTE_PATH_PATTERN
        )
    )
    uploads = tuple(sorted(name for name in properties if name.endswith(UPLOAD_IDX_SUFFIX)))
    return HostPathKeys(paths=paths, uploads=uploads)


def named_host_paths(
    context_schema: dict[str, Any], action_context: dict[str, Any]
) -> dict[str, str]:
    """The `{key: value}` pairs in `action_context` the gate must check.

    Two sources, unioned:

      * what the schema declares — a `type: string` / `pattern: "^/"` property
        (`host_path_keys`), which is how every shipped workflow spells one; and
      * what the key is named — a string value under a key ending in `_path`
        or `_dir`.

    The second is not redundant. The schema rule can only see keys the schema
    declares, and an action whose `context_schema` is the permissive `{}`
    default declares none — its path inputs would pass through ungated. The
    naming rule is the same convention `test_actions_loader` enforces on
    workflow YAML, applied here so the route does not depend on the YAML being
    well-formed to bound what a submitter can name.

    Non-string values are skipped rather than rejected: a non-string is not a
    path, and `validate_context` has already run against whatever the schema
    does declare.
    """
    declared = set(host_path_keys(context_schema).paths)
    return {
        key: value
        for key, value in action_context.items()
        if isinstance(value, str) and (key in declared or key.endswith(HOST_PATH_KEY_SUFFIXES))
    }


def _probe(path: Path) -> None:
    """Raise FileNotFoundError when `path` is definitively absent.

    `os.stat` separates the two outcomes `os.path.exists` collapses into
    `False`: ENOENT means the parent was traversable and the entry is not
    there, EACCES means this process cannot tell. Only the first is a
    submitter error — see the module docstring on the account split.
    """
    try:
        os.stat(path)
    except FileNotFoundError:
        raise
    except NotADirectoryError:
        # A component of the path is a regular file, so the entry cannot
        # exist. Same standing as ENOENT.
        raise FileNotFoundError(path) from None
    except OSError:
        # PermissionError and anything else the mount reports: unknown, admit.
        return


def resolve_ingest_path(raw: str, *, roots: tuple[Path, ...]) -> Path:
    """Return `raw` as a normalized absolute path inside one of `roots`.

    Raises `IngestPathError` when the value is not absolute, escapes every
    root, or is definitively absent.

    Containment is checked twice, and both checks are load-bearing.

    The lexical one, on `os.path.normpath` output, holds even when the control
    plane cannot traverse far enough to resolve symlinks (see the module
    docstring). The second resolves the RAW value — not the normalized one —
    because the kernel resolves a symlink before it applies the `..` that
    follows it, while `normpath` cancels the pair textually. With a root of
    `/sequencing` and `/sequencing/link` pointing outside it,
    `/sequencing/link/../tail` normalizes to `/sequencing/tail` and passes the
    lexical rule, but opens `/tail` on the compute node. `os.path.realpath`
    degrades to the unresolved tail rather than raising, so this never fires on
    a path the lexical rule accepted for a reason the process cannot observe.
    """
    if not raw or not PurePosixPath(raw).is_absolute():
        raise IngestPathError(
            "host path must be absolute",
            path=raw,
            roots=roots,
        )
    lexical = Path(os.path.normpath(raw))
    if not any(lexical.is_relative_to(root) for root in roots):
        raise IngestPathError(
            "host path is outside every configured ingest root",
            path=raw,
            roots=roots,
        )
    resolved = Path(os.path.realpath(raw))
    if resolved != lexical:
        # Compare against the RESOLVED roots. A root can itself sit behind a
        # symlink (`/var` -> `/private/var` on macOS, a mount reached through a
        # link on Linux), and comparing a resolved path against an unresolved
        # root then rejects every path under that root.
        resolved_roots = [Path(os.path.realpath(root)) for root in roots]
        if not any(resolved.is_relative_to(root) for root in resolved_roots):
            raise IngestPathError(
                f"host path resolves to {resolved}, outside every configured ingest root",
                path=raw,
                roots=roots,
            )
    try:
        _probe(lexical)
    except FileNotFoundError:
        raise IngestPathError(
            "host path does not exist",
            path=raw,
            roots=roots,
        ) from None
    return lexical
