"""Native job: build a minimap2 `.mmi` index for a reference.

Two modes, mirroring `build_rype_index`:

* **Host / whole-reference mode** (no `shard_id`) — the second-pass aligner index
  `host_filter` consumes (`align_minimap2(index_path=<.mmi>, preset='sr')`),
  reassembled from the SAME feature-keyed staging chunks `build_rype_index` reads
  (`reference_sequence_chunks`: `feature_idx, chunk_index, chunk_data`), written to
  `{path_derived}/references/{idx}/minimap2/index.mmi`. Byte-identical to the
  shipped host builder — it reads the staging Parquet via `read_parquet`, so it
  runs AFTER the chunks are produced and BEFORE `register-files` moves them.
* **Shard mode** (`shard_id` + `shard_features` roster) — one shard's analysis
  subject index, built over just that shard's features. The roster's small
  `feature_idx` list rides the job input; the chunk bytes STREAM from the data
  plane over Arrow Flight (the B6s `open_reference_chunk_stream` seam), not staging
  Parquet. Written to `.../minimap2-shards/{shard_id}.mmi` — the flat
  `{shard_directory}/{shard_name}.mmi` shape `align_minimap2_sharded` binds
  (`shard_name = str(shard_id)`). This is the first builder to consume the B6s
  stream; C1's `align_minimap2_sharded` consumes the per-shard `.mmi` set.

Both modes reassemble the per-feature contig into a `(read_id, sequence1)` subject
via the shared `subject.stage_subject` (`string_agg(chunk_data ORDER BY
chunk_index)` GROUP BY feature_idx) — the index is built from exactly the bytes
stored in the data plane, no raw-FASTA side channel. The subject is a non-temp
TABLE because miint's `save_minimap2_index` opens a SEPARATE connection on the
same DuckDB instance during bind/execute, which resolves regular `view`/`table`
names but not TEMP tables / CTEs (see docs/duckdb-miint.md).

miint signature (qiita-verified against the team-mirror build; see
docs/duckdb-miint.md):
  save_minimap2_index(subject_table, output_path, [eqx], [w], [k], [preset])
returns a single row `(success BOOLEAN, index_path VARCHAR, num_subjects INTEGER)`.
Exactly TWO positional args (subject table NAME, output path); `preset` is named.
The control plane records only a small params copy via the meta JSON this job
writes — native step outputs are paths, so params can't ride a binding directly
(mirrors build_rype_index).
"""

from __future__ import annotations

import json
import math
from pathlib import Path

import duckdb
from pydantic import BaseModel, model_validator
from qiita_common.models import HOST_FILTER_INDEX_TYPE_MINIMAP2
from qiita_common.parquet import validate_parquet_path

from ..config import get_settings
from ..data_plane_client import open_reference_chunk_stream
from ..derived_store import minimap2_index_path, shard_minimap2_index_path
from ..miint import (
    apply_duckdb_settings,
    duckdb_headroom_gb,
    duckdb_tmp_dir,
    open_miint_conn,
    resolve_duckdb_memory_gb,
)
from ..subject import stage_subject
from . import JobPlan, JobResourcePlan

YAML_STEP_NAME = "build_minimap2_index"

# Co-consumer step: DuckDB reassembles the per-feature subject (string_agg over
# chunks) and hands the TABLE to minimap2, which does the heavy indexing
# in-process from the cgroup remainder. `_DUCKDB_MEMORY_GB` is the OFF-SLURM
# fallback (local backend / tests). Under SLURM the limit tracks the real cgroup
# via `resolve_duckdb_memory_gb()` with `_MINIMAP2_RESERVE_GB` carved out for
# minimap2's in-process index — so a `--mem-gb` override grows both the
# DuckDB reassembly headroom (genome-scale contigs are large) and minimap2's
# index budget, instead of OOMing against a fixed 8 GB.
_DUCKDB_MEMORY_GB = 8
_DUCKDB_THREADS = 4
# Cgroup reserved for minimap2's in-process index build — it is given no explicit
# memory limit and allocates from whatever DuckDB's limit leaves under the cgroup.
# Basis: a human-genome (~3.1 Gbp) `-x sr` minimap2 index is ~8 GB resident, and
# the 'sr' preset's denser minimizers run heavier than the default preset; 16 GB
# is a ~2x envelope over that. This is the number most likely to be wrong for the
# genome-scale case that motivated this change — refine it against the first real
# human-reference build's MaxRSS (sacct) and bump if the build still OOMs.
_MINIMAP2_RESERVE_GB = 16

# minimap2 preset default — 'sr' (short-read), the host-filter alignment mode
# `host_filter` mirrors on the query side. Overridable via Inputs.
_DEFAULT_PRESET = "sr"

# In-DuckDB name handed to save_minimap2_index (resolved by its separate
# connection — must be non-temp; a TABLE because the reassembly is a blocking agg).
_SUBJECT_RELATION = "minimap2_subject"

# plan() memory sizing for SHARD mode (advisory, down-only), mirroring
# build_rype_index. The FLOOR is the smallest allocation the runtime DuckDB +
# minimap2 split stays consistent at: DuckDB's fallback share + minimap2's reserve
# + the shared headroom. Above the floor, add a gentle per-bp term (the minimap2
# index scales with the shard's total sequence). The CP applies this ONLY below
# the step's YAML baseline (down-only composition), so a small shard runs in a
# smaller SLURM slot while an over-estimate harmlessly stays at baseline; an
# under-estimate is still caught by OOM escalation.
_SHARD_PLAN_FLOOR_GB = (
    _DUCKDB_MEMORY_GB + _MINIMAP2_RESERVE_GB + duckdb_headroom_gb(_DUCKDB_THREADS)
)
_SHARD_PLAN_BP_PER_GB = 1_000_000_000


class Inputs(BaseModel):
    """Typed input contract for build_minimap2_index.

    `reference_sequence_chunks` is the feature-keyed chunk output of the `load`
    step (a DIRECTORY of `part_*.parquet`, or a single Parquet file) — the SAME
    binding `build_rype_index` consumes. It is REQUIRED in host mode and unused in
    shard mode (the shard streams its chunks), hence `Path | None`. `reference_idx`
    and `work_ticket_idx` are framework-injected scope scalars. `preset` is the
    minimap2 preset baked into the index.

    SHARD mode (both `shard_id` and `shard_features` set) builds one shard's
    subject `.mmi` over just that shard's features: `shard_features` is a
    runner-staged Parquet roster `(feature_idx BIGINT, sequence_length_bp BIGINT)`
    whose `feature_idx` list scopes a B6s DoGet ticket, and the chunk bytes stream
    from the data plane. Left unset (both None) is HOST/whole-reference mode —
    today's behavior, byte-identical.
    """

    reference_sequence_chunks: Path | None = None
    reference_idx: int
    work_ticket_idx: int
    preset: str = _DEFAULT_PRESET
    shard_id: int | None = None
    shard_features: Path | None = None

    @model_validator(mode="after")
    def _shard_fields_both_or_neither(self) -> Inputs:
        if (self.shard_id is None) != (self.shard_features is None):
            raise ValueError(
                "shard_id and shard_features must be supplied together (both for a"
                " sharded build, or neither for a whole-reference/host build)"
            )
        # Host mode reads staging Parquet, so it needs the chunk binding; shard
        # mode streams and ignores it.
        if self.shard_id is None and self.reference_sequence_chunks is None:
            raise ValueError(
                "reference_sequence_chunks is required in host/whole-reference mode"
                " (supply shard_id + shard_features for a sharded streaming build)"
            )
        return self


def _run_save_minimap2_index(
    conn: duckdb.DuckDBPyConnection,
    subject_table: str,
    output_path: str,
    *,
    preset: str,
) -> bool:
    """Seam around miint's `save_minimap2_index`. Isolated so unit tests can stub
    the real build (which needs the extension + real sequence bytes). Returns the
    `success` flag from the function's single status row.

    Two positional args (`subject_table`, `output_path` — both VARCHAR, bound as
    `?`); `preset` is named and bound as `?` (a table-function call accepts
    prepared params for both positional and named args). `subject_table` is a
    table NAME the function resolves on its own connection (hence VARCHAR, not a
    SQL identifier).

    The function always emits exactly one status row, so a missing row is a
    structural contract violation (not a clean `success=false`) — raise loudly
    rather than coerce it into the ordinary failure path."""
    row = conn.execute(
        "SELECT success FROM save_minimap2_index(?, ?, preset := ?)",
        [subject_table, output_path, preset],
    ).fetchone()
    if row is None:
        raise RuntimeError("save_minimap2_index returned no status row (miint contract violation)")
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
        # single file too (tests / future producers). Same resolution as build_rype_index.
        read_target = chunks / "part_*.parquet" if chunks.is_dir() else chunks

    # Persistent index location under the derived-artifact root (PATH_DERIVED),
    # NOT the ephemeral per-attempt workspace. On SLURM the backend propagates
    # PATH_DERIVED into the job env so get_settings() resolves the real value.
    # The layout is owned by `derived_store` (shared with build_rype_index and
    # the reference-artifact purge endpoint). A sharded build lands at
    # `.../minimap2-shards/{shard_id}.mmi` (one flat `.mmi` per shard, the shape
    # `align_minimap2_sharded` binds).
    path_derived = get_settings().path_derived
    index_path = (
        shard_minimap2_index_path(path_derived, inputs.reference_idx, inputs.shard_id)
        if sharded
        else minimap2_index_path(path_derived, inputs.reference_idx)
    )
    index_path.parent.mkdir(parents=True, exist_ok=True)
    # On a workflow retry the build re-runs against the same persistent path;
    # clear any prior (possibly partial) `.mmi` so the rebuild is deterministic.
    # The minimap2 index is a single FILE, so unlink (not rmtree). Safe within
    # scope: the reference is still `indexing` (not yet `active`) during the build.
    index_path.unlink(missing_ok=True)

    feature_ids: list[int] = []
    with duckdb_tmp_dir(workspace) as duckdb_tmp, open_miint_conn() as conn:
        apply_duckdb_settings(
            conn,
            duckdb_tmp,
            memory_gb=resolve_duckdb_memory_gb(
                _DUCKDB_MEMORY_GB,
                threads=_DUCKDB_THREADS,
                reserve_gb=_MINIMAP2_RESERVE_GB,
            ),
            threads=_DUCKDB_THREADS,
        )
        if sharded:
            # Read the shard's feature roster (small — one row per feature) to
            # scope the DoGet ticket, then stream that roster's chunks. The
            # subject TABLE is materialised INSIDE the stream `with` (a blocking
            # agg that drains the stream) so the Flight client closes before the
            # long minimap2 build.
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
                    " nothing to build a minimap2 index from"
                )
            async with open_reference_chunk_stream(
                conn, reference_idx=inputs.reference_idx, feature_idx=feature_ids
            ) as rel:
                num_subjects = stage_subject(conn, rel, subject_table=_SUBJECT_RELATION)
        else:
            source = f"read_parquet('{validate_parquet_path(read_target)}')"
            num_subjects = stage_subject(conn, source, subject_table=_SUBJECT_RELATION)

        if num_subjects == 0:
            # No chunks to reassemble — there is nothing to index. This step only
            # runs where a minimap2 index is required, so an empty subject is a
            # fail-fast, not a silent empty index. (Shard mode also fails on an
            # empty roster above; this catches a roster whose features have no
            # chunks in the data plane yet.)
            raise ValueError(
                f"reference {inputs.reference_idx} has no sequence chunks to index: "
                f"nothing to build a minimap2 index from"
            )
        success = _run_save_minimap2_index(
            conn, _SUBJECT_RELATION, str(index_path), preset=inputs.preset
        )
    if not success:
        raise RuntimeError(
            f"save_minimap2_index reported failure for reference {inputs.reference_idx} "
            f"→ {index_path}"
        )

    # Host meta records the staging source (`source_chunks`); shard meta records
    # `source: "stream"` + the roster size instead (there is no staging path).
    if sharded:
        params = {
            "preset": inputs.preset,
            "source": "stream",
            "feature_count": len(feature_ids),
            "num_subjects": num_subjects,
        }
    else:
        params = {
            "preset": inputs.preset,
            "source_chunks": str(inputs.reference_sequence_chunks),
            "num_subjects": num_subjects,
        }
    meta_path = workspace / "minimap2_index_meta.json"
    # Only a sharded build adds `shard_id` to the meta JSON; host mode omits it
    # (keeping the host meta byte-identical). The runner's register-index arm
    # reads it via `meta.get("shard_id")` — absent → None → a whole-reference row.
    meta: dict = {
        "index_type": HOST_FILTER_INDEX_TYPE_MINIMAP2,
        "fs_path": str(index_path),
        "params": params,
    }
    if sharded:
        meta["shard_id"] = inputs.shard_id
    meta_path.write_text(json.dumps(meta))
    # Only the in-tree meta JSON is a step output. The `.mmi` itself lives under
    # PATH_DERIVED (outside the per-attempt workspace) on purpose — it outlives
    # the work ticket — so it CANNOT be a declared output: the launcher manifest
    # write and the verifier both require every output to resolve under
    # $QIITA_OUTPUT_PATH. register-index reads its location from meta `fs_path`.
    return {"minimap2_index_meta": meta_path}


def plan(inputs: Inputs) -> JobPlan:
    """Size a SHARD build's memory down from the whole-reference baseline.

    Host/unsharded mode → no opinion (empty `JobPlan` → keep the step's YAML
    baseline). Shard mode → size `mem_gb` from the shard's total bp: the
    runtime-consistent floor (`_SHARD_PLAN_FLOOR_GB`) plus a gentle per-bp term.
    Advisory, down-only (the CP applies it only below baseline); an under-estimate
    is still caught by OOM escalation. `plan()` runs at submit time in the
    orchestrator process and reads only the small roster (bp sum), not the chunk
    data. Mirrors build_rype_index.plan()."""
    if inputs.shard_id is None or inputs.shard_features is None:
        return JobPlan()
    with duckdb.connect(":memory:") as conn:
        total_bp = conn.execute(
            "SELECT COALESCE(sum(sequence_length_bp), 0) FROM read_parquet(?)",
            [str(inputs.shard_features)],
        ).fetchone()[0]
    mem_gb = _SHARD_PLAN_FLOOR_GB + math.ceil(total_bp / _SHARD_PLAN_BP_PER_GB)
    return JobPlan(resources=JobResourcePlan(mem_gb=mem_gb))
