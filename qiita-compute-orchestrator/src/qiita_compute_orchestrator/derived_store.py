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
    "bowtie2_index_path",
    "minimap2_index_path",
    "reference_derived_dir",
    "rype_index_path",
    "rype_router_index_path",
    "shard_bowtie2_dir",
    "shard_bowtie2_index_prefix",
    "shard_minimap2_dir",
    "shard_minimap2_index_path",
]


def reference_derived_dir(derived_root: Path | str, reference_idx: int) -> Path:
    """``{PATH_DERIVED}/references/{reference_idx}`` — the entire per-reference
    derived subtree (both index kinds live below it). This is the purge target
    for ``DELETE /reference-artifact/{idx}``.

    ``derived_root`` is ``Settings.path_derived`` (a str on the model); accept
    ``Path`` too so callers that already hold one don't round-trip through str.
    """
    return Path(derived_root) / "references" / str(reference_idx)


def rype_index_path(derived_root: Path | str, reference_idx: int) -> Path:
    """The rype ``.ryxdi`` index directory for a reference (a DIRECTORY:
    ``manifest.toml`` + Parquet shards), written by ``build_rype_index``."""
    return reference_derived_dir(derived_root, reference_idx) / "rype" / "index.ryxdi"


def rype_router_index_path(derived_root: Path | str, reference_idx: int) -> Path:
    """The whole-reference rype ROUTER ``.ryxdi`` index directory:
    ``{PATH_DERIVED}/references/{reference_idx}/rype-router.ryxdi``.

    A single multi-bucket router over the ENTIRE reference — one bucket per shard
    (``bucket_name = str(shard_id)``) — written by ``build_routing_index``. One
    ``rype_classify`` pass against it yields the ``read_to_shard`` table
    ``align_*_sharded`` need (see C1). Distinct from the per-reference host-filter
    ``rype_index_path`` (a single POSITIVE bucket). Sits directly under the
    per-reference subtree so the ``DELETE /reference-artifact/{idx}`` rmtree purges
    it; ``shard_id`` is NULL on its ``reference_index`` row (whole-reference, not
    per-shard). It replaced the per-shard routing ``.ryxdi`` removed in C2."""
    return reference_derived_dir(derived_root, reference_idx) / "rype-router.ryxdi"


def minimap2_index_path(derived_root: Path | str, reference_idx: int) -> Path:
    """The minimap2 ``.mmi`` index file for a reference (a single FILE),
    written by ``build_minimap2_index`` in host/whole-reference mode."""
    return reference_derived_dir(derived_root, reference_idx) / "minimap2" / "index.mmi"


def shard_minimap2_dir(derived_root: Path | str, reference_idx: int) -> Path:
    """The per-reference minimap2 SHARD-DIRECTORY root:
    ``{PATH_DERIVED}/references/{reference_idx}/minimap2-shards``.

    This is the ``shard_directory`` ``align_minimap2_sharded`` is pointed at: a
    FLAT directory holding one ``{shard_id}.mmi`` per shard (see
    ``shard_minimap2_index_path``). miint resolves each shard by
    ``{shard_directory}/{shard_name}.mmi`` where ``shard_name = str(shard_id)``, so
    the directory must contain nothing but those files. Sits under the
    per-reference subtree the ``DELETE /reference-artifact/{idx}`` rmtree purges."""
    return reference_derived_dir(derived_root, reference_idx) / "minimap2-shards"


def shard_minimap2_index_path(derived_root: Path | str, reference_idx: int, shard_id: int) -> Path:
    """The per-shard minimap2 ``.mmi`` index FILE of a sharded *analysis*
    reference: ``{PATH_DERIVED}/references/{reference_idx}/minimap2-shards/{shard_id}.mmi``.

    A flat ``{shard_id}.mmi`` inside ``shard_minimap2_dir`` — the exact shape
    ``align_minimap2_sharded`` binds (``{shard_directory}/{shard_name}.mmi``,
    ``shard_name = str(shard_id)``). Written by ``build_minimap2_index`` in shard
    mode (one ``.mmi`` per shard, over just that shard's features)."""
    return shard_minimap2_dir(derived_root, reference_idx) / f"{shard_id}.mmi"


def bowtie2_index_path(derived_root: Path | str, reference_idx: int) -> Path:
    """The bowtie2 index PREFIX for a reference (host/whole-reference mode):
    ``{PATH_DERIVED}/references/{reference_idx}/bowtie2/index``. bowtie2 writes
    MULTIPLE files under this shared prefix (``index.1.bt2`` … ``index.rev.2.bt2``),
    so this is a prefix, not a single file — ``reference_index.fs_path`` for a
    bowtie2 row is this prefix. Written by ``build_bowtie2_index``."""
    return reference_derived_dir(derived_root, reference_idx) / "bowtie2" / "index"


def shard_bowtie2_dir(derived_root: Path | str, reference_idx: int) -> Path:
    """The per-reference bowtie2 SHARD-DIRECTORY root:
    ``{PATH_DERIVED}/references/{reference_idx}/bowtie2-shards``.

    This is the ``shard_directory`` ``align_bowtie2_sharded`` is pointed at: it
    holds one SUBDIR per shard, named ``{shard_id}``, each carrying the
    ``index.*.bt2`` set (see ``shard_bowtie2_index_prefix``). miint resolves each
    shard by ``{shard_directory}/{shard_name}/index.*`` where
    ``shard_name = str(shard_id)``. Sits under the per-reference subtree the
    ``DELETE /reference-artifact/{idx}`` rmtree purges."""
    return reference_derived_dir(derived_root, reference_idx) / "bowtie2-shards"


def shard_bowtie2_index_prefix(derived_root: Path | str, reference_idx: int, shard_id: int) -> Path:
    """The per-shard bowtie2 index PREFIX of a sharded *analysis* reference:
    ``{PATH_DERIVED}/references/{reference_idx}/bowtie2-shards/{shard_id}/index``.
    Like ``bowtie2_index_path`` this is a PREFIX (bowtie2 writes multiple ``.bt2``
    files under it) — the per-shard SUBDIR is ``{shard_id}`` under
    ``shard_bowtie2_dir`` and the prefix inside it is ``index``, the exact shape
    ``align_bowtie2_sharded`` binds (``{shard_directory}/{shard_name}/index.*``,
    ``shard_name = str(shard_id)``). Written by ``build_bowtie2_index`` in shard
    mode."""
    return shard_bowtie2_dir(derived_root, reference_idx) / str(shard_id) / "index"
