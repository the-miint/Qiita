"""Path layout for per-reference DERIVED artifacts under ``PATH_DERIVED``.

The compute orchestrator owns three distinct storage concerns, and this module
makes the third one explicit:

  * the **data plane** owns persistent data (DuckLake + permanent Parquet);
  * the orchestrator owns the **ephemeral per-attempt workspace**
    (``$QIITA_OUTPUT_PATH`` / scratch — disposable, one per step attempt);
  * the orchestrator *also* owns **derived storage** — the persistent
    host-filter indexes under ``PATH_DERIVED``. These are per-reference and
    durable (they outlive the work ticket and are consumed at host-filter
    time), but they are produced, read, and deleted entirely on the compute
    side.

Every touch point of derived storage is an orchestrator concern:

  * ``jobs/build_rype_index`` writes the rype ``.ryxdi``;
  * ``jobs/build_minimap2_index`` writes the minimap2 ``.mmi``;
  * ``jobs/host_filter`` reads them (via the DB-recorded ``reference_index.fs_path``);
  * the ``DELETE /reference-artifact/{idx}`` endpoint (``reference_artifact``)
    purges the whole per-reference subtree.

Before this module the ``{PATH_DERIVED}/references/{idx}/...`` layout was
reconstructed by hand in each of those places; centralizing it here removes the
duplication and gives the convention one owner.

These paths live **outside** ``$QIITA_OUTPUT_PATH`` on purpose. A native job
must therefore never declare a derived path as a step output: the launcher's
manifest write and the verifier both require every declared output to resolve
under ``$QIITA_OUTPUT_PATH``, so an out-of-tree output is a CONTRACT_VIOLATION.
Communicate a derived artifact's location via an in-tree meta JSON instead
(``register-index`` reads ``fs_path`` from it). The home for this rule is
``docs/architecture.md`` (the native-step note under the Container contract).

This module is a sibling of ``jobs/`` (not inside it) by design: the boot scan
validates every non-dunder module under ``jobs/`` as a native job, so shared
helpers must live outside that package.
"""

from __future__ import annotations

from pathlib import Path

__all__ = [
    "minimap2_index_path",
    "reference_derived_dir",
    "reference_shard_dir",
    "rype_index_path",
    "shard_rype_index_path",
]


def reference_derived_dir(derived_root: Path | str, reference_idx: int) -> Path:
    """``{PATH_DERIVED}/references/{reference_idx}`` — the entire per-reference
    derived subtree (both index kinds live below it). This is the purge target
    for ``DELETE /reference-artifact/{idx}``.

    ``derived_root`` is ``Settings.path_derived`` (a str on the model); accept
    ``Path`` too so callers that already hold one don't round-trip through str.
    """
    return Path(derived_root) / "references" / str(reference_idx)


def reference_shard_dir(derived_root: Path | str, reference_idx: int, shard_id: int) -> Path:
    """``{PATH_DERIVED}/references/{reference_idx}/shards/{shard_id}`` — the
    per-shard directory of a sharded *analysis* index (one ``shard_id`` per
    shard, 0..N-1). Composed off ``reference_derived_dir`` so it sits inside the
    per-reference subtree the ``DELETE /reference-artifact/{idx}`` rmtree purges
    — no separate cleanup path.

    B1 lays down only the directory *layout*; what a shard directory contains
    (a ``.mmi`` vs vendored FASTA vs a ``.ryxdi``) is decided by the later
    sharded-index build milestone."""
    return reference_derived_dir(derived_root, reference_idx) / "shards" / str(shard_id)


def rype_index_path(derived_root: Path | str, reference_idx: int) -> Path:
    """The rype ``.ryxdi`` index directory for a reference (a DIRECTORY:
    ``manifest.toml`` + Parquet shards), written by ``build_rype_index``."""
    return reference_derived_dir(derived_root, reference_idx) / "rype" / "index.ryxdi"


def shard_rype_index_path(derived_root: Path | str, reference_idx: int, shard_id: int) -> Path:
    """The per-shard rype ``.ryxdi`` ROUTING index directory of a sharded
    *analysis* reference:
    ``{PATH_DERIVED}/references/{reference_idx}/shards/{shard_id}/index.ryxdi``.
    Composed off ``reference_shard_dir`` so it sits inside the per-reference
    subtree the ``DELETE /reference-artifact/{idx}`` rmtree purges. Written by
    ``build_rype_index`` in shard mode (one ``.ryxdi`` per shard, over just that
    shard's features)."""
    return reference_shard_dir(derived_root, reference_idx, shard_id) / "index.ryxdi"


def minimap2_index_path(derived_root: Path | str, reference_idx: int) -> Path:
    """The minimap2 ``.mmi`` index file for a reference (a single FILE),
    written by ``build_minimap2_index``."""
    return reference_derived_dir(derived_root, reference_idx) / "minimap2" / "index.mmi"
