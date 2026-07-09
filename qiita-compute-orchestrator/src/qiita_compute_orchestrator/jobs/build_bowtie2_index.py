"""Native job: build a bowtie2 index (a `.bt2` set) for a reference.

The analysis-alignment sibling of `build_minimap2_index`. Where minimap2's `.mmi`
is the host-filter second-pass index, bowtie2's `.bt2` set is the subject index
C1's `align_bowtie2_sharded` reads. Two modes, identical in shape to
`build_minimap2_index`:

* **Host / whole-reference mode** (no `shard_id`) — reassembles the whole
  reference from its staging chunks and writes the `.bt2` set under
  `{path_derived}/references/{idx}/bowtie2/index` (a PREFIX).
* **Shard mode** (`shard_id` + `shard_features` roster) — one shard's subject
  index, built over just that shard's features, chunk bytes STREAMED from the
  data plane over Arrow Flight (B6s), written under
  `.../bowtie2-shards/{shard_id}/index` — the `{shard_directory}/{shard_name}/index.*`
  shape `align_bowtie2_sharded` binds (`shard_name = str(shard_id)`).

Both modes reassemble the per-feature contig into a `(read_id, sequence1)` subject
via the shared `subject.stage_subject` (the same subject minimap2 indexes).

miint signature (qiita-verified against the team-mirror build `ec2ef3e` /
miint 1.5.4 via probe + the host-mode real-miint smoke; see docs/duckdb-miint.md):
  save_bowtie2_index(subject_table, output_path, [threads])
Exactly TWO positional args (subject table NAME, output path PREFIX); returns one
row `(success BOOLEAN, index_path VARCHAR, num_subjects BIGINT)`. Unlike
`save_minimap2_index` it takes **no `preset`** — a bowtie2 index is
preset-independent (presets apply at align time via `align_bowtie2_sharded`), so
this builder has no preset knob. The subject `read_id` may be BIGINT (it is here,
the feature_idx). The function needs no GPL boundary (`install_gpl_boundary` is
NOT required — verified by the smoke; bowtie2 is statically linked in this build).

**Multi-file output.** bowtie2 writes SIX files under the shared prefix
(`index.1.bt2 … index.4.bt2`, `index.rev.1.bt2`, `index.rev.2.bt2`), so
`output_path` is a PREFIX and `reference_index.fs_path` for a bowtie2 row is that
prefix. A rerun clears the whole `bowtie2/` directory first (rmtree, mirroring
rype's `.ryxdi` dir cleanup) so a partial prior build leaves no stale `.bt2`.
"""

from __future__ import annotations

import json
import math
import shutil
from pathlib import Path

import duckdb
from pydantic import BaseModel, model_validator
from qiita_common.models import INDEX_TYPE_BOWTIE2
from qiita_common.parquet import validate_parquet_path

from ..config import get_settings
from ..data_plane_client import open_reference_chunk_stream
from ..derived_store import bowtie2_index_path, shard_bowtie2_index_prefix
from ..miint import (
    apply_duckdb_settings,
    duckdb_headroom_gb,
    duckdb_tmp_dir,
    open_miint_conn,
    resolve_duckdb_memory_gb,
)
from ..subject import stage_subject
from . import JobPlan, JobResourcePlan

YAML_STEP_NAME = "build_bowtie2_index"

# Co-consumer step: DuckDB reassembles the per-feature subject and hands the TABLE
# to bowtie2-build, which does the heavy indexing in-process from the cgroup
# remainder. `_DUCKDB_MEMORY_GB` is the OFF-SLURM fallback; under SLURM the limit
# tracks the real cgroup via `resolve_duckdb_memory_gb()` with
# `_BOWTIE2_RESERVE_GB` carved out for bowtie2's in-process build. Mirrors
# build_minimap2_index's split.
_DUCKDB_MEMORY_GB = 8
_DUCKDB_THREADS = 4
# Cgroup reserved for bowtie2's in-process index build. bowtie2's BWT build is
# lighter than a genome-scale minimap2 index, so 16 GB (matching minimap2's
# reserve) is a conservative starting envelope — the tuning knob to refine against
# a real genome-scale build's MaxRSS (sacct).
_BOWTIE2_RESERVE_GB = 16

# In-DuckDB name handed to save_bowtie2_index (resolved by its separate
# connection — must be non-temp; a TABLE because the reassembly is a blocking agg).
_SUBJECT_RELATION = "bowtie2_subject"

# plan() memory sizing for SHARD mode (advisory, down-only), mirroring
# build_minimap2_index / build_rype_index. Floor = DuckDB's fallback share +
# bowtie2's reserve + the shared headroom; per-bp term over-provisions gently.
_SHARD_PLAN_FLOOR_GB = _DUCKDB_MEMORY_GB + _BOWTIE2_RESERVE_GB + duckdb_headroom_gb(_DUCKDB_THREADS)
_SHARD_PLAN_BP_PER_GB = 1_000_000_000


class Inputs(BaseModel):
    """Typed input contract for build_bowtie2_index.

    `reference_sequence_chunks` is the feature-keyed chunk output of the `load`
    step — REQUIRED in host mode, unused in shard mode (the shard streams), hence
    `Path | None`. `reference_idx` and `work_ticket_idx` are framework-injected
    scope scalars. There is deliberately no `preset` field: the bowtie2 index build
    (`save_bowtie2_index`) takes no preset (unlike minimap2); presets are an
    align-time concern.

    SHARD mode (both `shard_id` and `shard_features` set) builds one shard's
    subject `.bt2` set over just that shard's features: `shard_features` is a
    runner-staged Parquet roster `(feature_idx BIGINT, sequence_length_bp BIGINT)`
    whose `feature_idx` list scopes a B6s DoGet ticket, and the chunk bytes stream
    from the data plane. Left unset (both None) is HOST/whole-reference mode.
    """

    reference_sequence_chunks: Path | None = None
    reference_idx: int
    work_ticket_idx: int
    shard_id: int | None = None
    shard_features: Path | None = None

    @model_validator(mode="after")
    def _shard_fields_both_or_neither(self) -> Inputs:
        if (self.shard_id is None) != (self.shard_features is None):
            raise ValueError(
                "shard_id and shard_features must be supplied together (both for a"
                " sharded build, or neither for a whole-reference/host build)"
            )
        if self.shard_id is None and self.reference_sequence_chunks is None:
            raise ValueError(
                "reference_sequence_chunks is required in host/whole-reference mode"
                " (supply shard_id + shard_features for a sharded streaming build)"
            )
        return self


def _run_save_bowtie2_index(
    conn: duckdb.DuckDBPyConnection,
    subject_table: str,
    output_path: str,
) -> bool:
    """Seam around miint's `save_bowtie2_index`. Isolated so unit tests can stub
    the real build (which needs the extension + real sequence bytes). Returns the
    `success` flag from the function's single status row.

    Two positional args (`subject_table`, `output_path` — both VARCHAR, bound as
    `?`); no preset (the index is preset-independent). `output_path` is a PREFIX
    (bowtie2 writes multiple `.bt2` files under it). `subject_table` is a table
    NAME the function resolves on its own connection.

    The function always emits exactly one status row, so a missing row is a
    structural contract violation (not a clean `success=false`) — raise loudly.
    No `install_gpl_boundary()` is needed (verified by the host-mode smoke): the
    call is a straight invocation like the minimap2 seam."""
    row = conn.execute(
        "SELECT success FROM save_bowtie2_index(?, ?)",
        [subject_table, output_path],
    ).fetchone()
    if row is None:
        raise RuntimeError("save_bowtie2_index returned no status row (miint contract violation)")
    return bool(row[0])


async def execute(inputs: Inputs, workspace: Path) -> dict[str, Path]:
    # SHARD mode when shard_id/shard_features are set (validator guarantees
    # both-or-neither, and that host mode carries `reference_sequence_chunks`).
    sharded = inputs.shard_id is not None

    read_target: Path | None = None
    if not sharded:
        chunks = inputs.reference_sequence_chunks
        if not chunks.exists():
            raise FileNotFoundError(f"reference_sequence_chunks not found: {chunks}")
        # reference_load emits chunks as a directory of part_*.parquet; accept a
        # single file too. Same resolution as build_minimap2_index.
        read_target = chunks / "part_*.parquet" if chunks.is_dir() else chunks

    # Persistent index PREFIX under the derived-artifact root (PATH_DERIVED), NOT
    # the ephemeral per-attempt workspace. bowtie2 writes multiple `.bt2` files
    # under this prefix; the layout is owned by `derived_store` (shared with the
    # reference-artifact purge endpoint). A sharded build lands at
    # `.../bowtie2-shards/{shard_id}/index` (a per-shard subdir, the shape
    # `align_bowtie2_sharded` binds).
    path_derived = get_settings().path_derived
    index_prefix = (
        shard_bowtie2_index_prefix(path_derived, inputs.reference_idx, inputs.shard_id)
        if sharded
        else bowtie2_index_path(path_derived, inputs.reference_idx)
    )
    # On a workflow retry the build re-runs against the same persistent prefix.
    # bowtie2 is a MULTI-FILE artifact, so clear the whole containing `bowtie2/`
    # directory (rmtree, mirroring rype's `.ryxdi` dir cleanup) — a plain unlink
    # of one file would leave stale `.bt2` shards from a partial prior build.
    # Safe within scope: the reference is still `indexing` during the build.
    index_dir = index_prefix.parent
    if index_dir.exists():
        shutil.rmtree(index_dir)
    index_dir.mkdir(parents=True, exist_ok=True)

    feature_ids: list[int] = []
    with duckdb_tmp_dir(workspace) as duckdb_tmp, open_miint_conn() as conn:
        apply_duckdb_settings(
            conn,
            duckdb_tmp,
            memory_gb=resolve_duckdb_memory_gb(
                _DUCKDB_MEMORY_GB,
                threads=_DUCKDB_THREADS,
                reserve_gb=_BOWTIE2_RESERVE_GB,
            ),
            threads=_DUCKDB_THREADS,
        )
        if sharded:
            # Read the shard's feature roster (small — one row per feature) to
            # scope the DoGet ticket, then stream that roster's chunks. The
            # subject TABLE is materialised INSIDE the stream `with` (a blocking
            # agg that drains the stream) so the Flight client closes before the
            # long bowtie2 build.
            roster_sql = validate_parquet_path(inputs.shard_features)
            feature_ids = [
                r[0]
                for r in conn.execute(
                    f"SELECT feature_idx FROM read_parquet('{roster_sql}')"
                ).fetchall()
            ]
            if not feature_ids:
                raise ValueError(
                    f"shard {inputs.shard_id} roster ({inputs.shard_features}) is empty:"
                    " nothing to build a bowtie2 index from"
                )
            async with open_reference_chunk_stream(
                conn, reference_idx=inputs.reference_idx, feature_idx=feature_ids
            ) as rel:
                num_subjects = stage_subject(conn, rel, subject_table=_SUBJECT_RELATION)
        else:
            source = f"read_parquet('{validate_parquet_path(read_target)}')"
            num_subjects = stage_subject(conn, source, subject_table=_SUBJECT_RELATION)

        if num_subjects == 0:
            # No chunks to reassemble — nothing to index. This step only runs
            # where a bowtie2 index is required, so an empty subject is a
            # fail-fast, not a silent empty index.
            raise ValueError(
                f"reference {inputs.reference_idx} has no sequence chunks to index: "
                f"nothing to build a bowtie2 index from"
            )
        success = _run_save_bowtie2_index(conn, _SUBJECT_RELATION, str(index_prefix))
    if not success:
        raise RuntimeError(
            f"save_bowtie2_index reported failure for reference {inputs.reference_idx} "
            f"→ {index_prefix}"
        )
    # Self-check the multi-file write: at least one bowtie2 index file must exist.
    # A `success=true` with no files would be a silent miint contract break. The
    # glob matches both the normal `.bt2` set and the LARGE-index `.bt2l` set
    # bowtie2-build auto-emits when the reference exceeds ~4 Gbp (a big
    # whole-reference build) — `*.bt2*` covers both; the downstream aligner
    # auto-detects the format from the same prefix.
    produced = list(index_dir.glob(f"{index_prefix.name}*.bt2*"))
    if not produced:
        raise RuntimeError(
            f"save_bowtie2_index reported success but wrote no bowtie2 index files under "
            f"{index_prefix} for reference {inputs.reference_idx}"
        )

    # No preset for bowtie2 (the index is preset-independent). Host meta records
    # the staging source; shard meta records `source: "stream"` + the roster size.
    if sharded:
        params = {
            "source": "stream",
            "feature_count": len(feature_ids),
            "num_subjects": num_subjects,
        }
    else:
        params = {
            "source_chunks": str(inputs.reference_sequence_chunks),
            "num_subjects": num_subjects,
        }
    meta_path = workspace / "bowtie2_index_meta.json"
    # Only a sharded build adds `shard_id` to the meta JSON; host mode omits it.
    # The runner's register-index arm reads it via `meta.get("shard_id")`.
    meta: dict = {
        "index_type": INDEX_TYPE_BOWTIE2,
        "fs_path": str(index_prefix),
        "params": params,
    }
    if sharded:
        meta["shard_id"] = inputs.shard_id
    meta_path.write_text(json.dumps(meta))
    # Only the in-tree meta JSON is a step output. The `.bt2` set itself lives
    # under PATH_DERIVED (outside the per-attempt workspace) on purpose — it
    # outlives the work ticket — so it CANNOT be a declared output. register-index
    # reads its location from meta `fs_path` (the prefix).
    return {"bowtie2_index_meta": meta_path}


def plan(inputs: Inputs) -> JobPlan:
    """Size a SHARD build's memory down from the whole-reference baseline.

    Host/unsharded mode → no opinion (empty `JobPlan`). Shard mode → size `mem_gb`
    from the shard's total bp: the runtime-consistent floor (`_SHARD_PLAN_FLOOR_GB`)
    plus a gentle per-bp term. Advisory, down-only; an under-estimate is still
    caught by OOM escalation. Mirrors build_minimap2_index.plan()."""
    if inputs.shard_id is None or inputs.shard_features is None:
        return JobPlan()
    with duckdb.connect(":memory:") as conn:
        total_bp = conn.execute(
            "SELECT COALESCE(sum(sequence_length_bp), 0) FROM read_parquet(?)",
            [str(inputs.shard_features)],
        ).fetchone()[0]
    mem_gb = _SHARD_PLAN_FLOOR_GB + math.ceil(total_bp / _SHARD_PLAN_BP_PER_GB)
    return JobPlan(resources=JobResourcePlan(mem_gb=mem_gb))
