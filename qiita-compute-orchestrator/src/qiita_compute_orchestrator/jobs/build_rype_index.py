"""Native job: build a rype index for a reference (host-read filtering).

Reads the feature-keyed chunked sequences `reference_load` produced
(`reference_sequence_chunks`: `feature_idx, chunk_index, chunk_data`), builds
a single-bucket `(feature_idx, bucket_name)` mapping, and calls miint's
`rype_index_create` to write a `.ryxdi` index to a PERSISTENT location under
the shared filesystem (NOT the ephemeral workspace) — the index outlives the
work ticket and is consumed at host-filter time.

For host filtering every feature goes to one bucket: the `.ryxdi` is a POSITIVE
host index — at filter time `rype_classify` emits any read matching it (host =
any emitted row), and the `host_filter` step removes those reads (then minimap2
re-checks the survivors). This is NOT rype's `-N` / `negative_index` mode. We
still pass a named single-bucket mapping (default `reference_{reference_idx}`)
rather than omitting the optional mapping table: it keeps the index
self-describing and exercises the same mapping path future multi-bucket
(microbial) uses will reuse.

rype build parameters default to k=64, w=20 (the function's own w default is
50, so we pass 20 explicitly); `w` is overridable per build via the `rype_w`
action_context key. The authoritative build manifest lives inside
the `.ryxdi` itself; the control plane records only a small params copy
(see `register_index`), threaded forward via the meta JSON this job writes —
native step outputs are paths (`dict[str, Path]`), so params can't ride a
binding directly.

miint signature (see `docs/duckdb-miint.md`, which carries the qiita-verified
signature as the single source — it tracks upstream drift so this comment
doesn't rot against a version tag):
  rype_index_create(chunk_table, output_path, [mapping_table],
                    [k=64], [w=50], [salt=...], [orient=true], [max_memory=0])
chunk_table needs columns feature_idx/chunk_index/chunk_data; mapping_table
needs feature_idx/bucket_name. Both are referenced by NAME — miint's
bind/execute opens a separate connection on the same DuckDB instance, which
resolves regular (non-temp) tables/views but not TEMP tables / CTEs, so we
create them as plain VIEW/TABLE (see docs/duckdb-miint.md).
"""

from __future__ import annotations

import json
import math
import shutil
from pathlib import Path

import duckdb
from pydantic import BaseModel, model_validator
from qiita_common.models import HOST_FILTER_INDEX_TYPE_RYPE
from qiita_common.parquet import validate_parquet_path

from ..config import get_settings
from ..data_plane_client import open_reference_chunk_stream
from ..derived_store import rype_index_path, shard_rype_index_path
from ..miint import (
    apply_duckdb_settings,
    duckdb_headroom_gb,
    duckdb_tmp_dir,
    open_miint_conn,
    resolve_duckdb_memory_gb,
    slurm_alloc_gb,
)
from . import JobPlan, JobResourcePlan

YAML_STEP_NAME = "build_rype_index"

# Co-consumer step: DuckDB feeds the chunk stream to rype + computes the small
# DISTINCT mapping, while rype does the heavy index build in-process and gets the
# bulk of the cgroup via `max_memory`. rype stays the ELASTIC consumer (its share
# grows with the allocation); DuckDB takes a bounded share via a HARD cap, so a
# larger `--mem-gb` — including the bigger allocation an OOM retry escalates to —
# buys a bigger rype build, not a ballooning DuckDB.
#
# The literals here are the OFF-SLURM fallbacks (local backend / tests). Under
# SLURM the split tracks the real cgroup: DuckDB = min(alloc − headroom,
# `_DUCKDB_MEMORY_CAP_GB`), rype = (allocation − DuckDB − headroom) floored at the
# `_RYPE_MAX_MEMORY_GB` fallback.
#
# DuckDB's cap is small because `rype_index_create` now WINDOWS its chunk feed:
# instead of one corpus-wide `ORDER BY` over the 64 KB BLOBs — which DuckDB cannot
# spill, and which OOMed DuckDB at ~3.7 GB on a human host reference (T2T-CHM13)
# under the old 4 GB cap, before rype's `max_memory` was ever exercised — the feed
# is read in bounded ~256 MiB windows, each sorted independently. DuckDB's working
# set is now bounded by WINDOW size, not corpus size: even one oversized feature
# (a human chromosome ≈ 256 MB of chunk data, its sort inflating several-fold
# across the 8 threads) sits comfortably under a few GB. 8 GB is well clear of
# that real per-window peak — bigger than the 4 GB off-SLURM fallback only to keep
# scan/decompress headroom. This relies on the windowed-feed miint build being
# live on the mirror; a pre-windowing build still does the corpus-wide sort and
# would need the old ~30 GB cap.
#
# Sizing for a large host set (many human genomes): the build_rype_index step
# starts at 64 GB (YAML baseline) and an OOM retry doubles it to the 128 GB action
# ceiling. DuckDB is hard-capped at 8 GB at BOTH sizes (a bigger allocation must
# not grow DuckDB), so the elastic rype share is 64−8−6 = 50 GB at the start and
# 128−8−6 = 114 GB on the escalated retry — i.e. rype's `max_memory` starts at
# 50 GB and grows with each OOM retry. Tune the cap against a real genome-scale
# DuckDB MaxRSS — the DuckDB/rype split here is the one knob this step exposes.
_DUCKDB_MEMORY_GB = 4
# Under-SLURM HARD ceiling for DuckDB's share (see the split note above): DuckDB
# stays bounded at this size even as an OOM retry grows the cgroup, so the extra
# memory flows to rype. Safe at this size only because the rype feed is windowed
# (see above). The 4 GB fallback only applies off-SLURM.
_DUCKDB_MEMORY_CAP_GB = 8
_DUCKDB_THREADS = 8
# rype's `max_memory` floor (GB) — also the off-SLURM fallback. Under SLURM rype
# gets max(this, alloc − DuckDB − headroom), so this is the STARTING budget and it
# grows elastically as an OOM retry escalates the allocation.
_RYPE_MAX_MEMORY_GB = 30

# plan() memory sizing for SHARD mode (advisory, down-only). A shard is ~1/1000
# of the reference, so it needn't request the whole-reference 64 GB YAML baseline;
# plan() sizes mem_gb from the shard's total bp and lets the CP's down-only
# composition lower the SLURM allocation. The FLOOR is the smallest allocation the
# runtime DuckDB/rype split stays consistent at: rype's max_memory is floored at
# `_RYPE_MAX_MEMORY_GB`, so the cgroup must hold rype's floor + DuckDB's cap + the
# shared headroom, else the split would hand rype more than the cgroup has. Above
# the floor, add a gentle per-bp term (the rype build is memory-bounded/windowed,
# so this over-provisions safely rather than tracking a hard requirement). Tune
# against a real shard build; an under-estimate is still caught by OOM escalation.
_SHARD_PLAN_FLOOR_GB = (
    _RYPE_MAX_MEMORY_GB + _DUCKDB_MEMORY_CAP_GB + duckdb_headroom_gb(_DUCKDB_THREADS)
)
_SHARD_PLAN_BP_PER_GB = 1_000_000_000

# rype build defaults. w=20 is passed explicitly (the function default is 50).
# Override per-build with the `rype_w` action_context key (host-reference-add).
_DEFAULT_K = 64
_DEFAULT_W = 20
# rype's default hash salt; pinned here so the build is reproducible and the
# call stays all-positional (see _run_rype_index_create).
_DEFAULT_SALT = 6148914691236517205

# In-DuckDB names handed to rype_index_create (resolved by its separate
# connection — must be non-temp).
_CHUNK_VIEW = "rype_chunk_input"
_MAPPING_TABLE = "rype_bucket_map"


class Inputs(BaseModel):
    """Typed input contract for build_rype_index.

    `reference_sequence_chunks` is the feature-keyed chunk output of the `load`
    step (a DIRECTORY of `part_*.parquet`, or a single Parquet file). It is
    REQUIRED in host mode and unused in shard mode (the shard streams its chunks
    from the data plane), hence `Path | None`. `reference_idx` and
    `work_ticket_idx` are framework-injected scope scalars. `k` / `w` are the
    rype build parameters (host-filter defaults); `bucket_name` overrides the
    default single-bucket name.

    SHARD mode (both `shard_id` and `shard_features` set) builds one shard's
    routing `.ryxdi` over just that shard's features: `shard_features` is a
    runner-staged Parquet roster `(feature_idx BIGINT, sequence_length_bp BIGINT)`
    (the shard's members, from `reference_membership.shard_id`) whose
    `feature_idx` list scopes a B6s DoGet ticket, and the chunk bytes STREAM from
    the data plane over Arrow Flight — a shard build runs AFTER the ingest
    ticket's register-files has moved the staging chunks into DuckLake, so there
    is no staging Parquet to read. Left unset (both None) is HOST/unsharded
    mode — today's whole-reference behavior, byte-identical (staging read).
    """

    reference_sequence_chunks: Path | None = None
    reference_idx: int
    work_ticket_idx: int
    k: int = _DEFAULT_K
    w: int = _DEFAULT_W
    bucket_name: str | None = None
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


def _run_rype_index_create(
    conn: duckdb.DuckDBPyConnection,
    chunk_table: str,
    output_path: str,
    mapping_table: str,
    *,
    k: int,
    w: int,
    max_memory: int,
) -> str:
    """Seam around miint's `rype_index_create`. Isolated so unit tests can
    stub the real build (which needs the extension + real sequence bytes).
    Returns the status string from the function's single status row.

    The real function takes exactly TWO positional args — `chunk_table`,
    `output_path` (both VARCHAR table-name / path values, bound as `?`) — and
    everything else is NAMED: `mapping_table` (VARCHAR), `k`/`w` (INTEGER),
    `salt` (UBIGINT — explicit cast), `orient` (BOOLEAN), `max_memory` (BIGINT).
    `chunk_table` / `mapping_table` are table NAMES the function resolves on its
    own connection (not SQL identifiers, hence VARCHAR). Named values are
    inlined (int()/pinned constant — no injection surface), `mapping_table`
    quote-escaped. orient=TRUE keeps both-strand matching (right for host
    classification — a read can match the host on either strand)."""
    mapping_sql = mapping_table.replace("'", "''")
    row = conn.execute(
        "SELECT status FROM rype_index_create(?, ?, "
        f"mapping_table := '{mapping_sql}', "
        f"k := {int(k)}, w := {int(w)}, salt := {_DEFAULT_SALT}::UBIGINT, "
        f"orient := TRUE, max_memory := {int(max_memory)})",
        [chunk_table, output_path],
    ).fetchone()
    return row[0] if row else None


async def execute(inputs: Inputs, workspace: Path) -> dict[str, Path]:
    # SHARD mode when shard_id/shard_features are set (the validator guarantees
    # both-or-neither, and that host mode carries `reference_sequence_chunks`).
    # A sharded build streams only the shard's features and writes a per-shard
    # `.ryxdi`; host/unsharded mode reads staging Parquet, byte-identical to before.
    sharded = inputs.shard_id is not None

    read_target: Path | None = None
    if not sharded:
        chunks = inputs.reference_sequence_chunks
        if not chunks.exists():
            raise FileNotFoundError(f"reference_sequence_chunks not found: {chunks}")
        # reference_load emits chunks as a directory of part_*.parquet; accept a
        # single file too (tests / future producers).
        read_target = chunks / "part_*.parquet" if chunks.is_dir() else chunks
    if inputs.bucket_name is not None:
        bucket = inputs.bucket_name
    elif sharded:
        bucket = f"reference_{inputs.reference_idx}_shard_{inputs.shard_id}"
    else:
        bucket = f"reference_{inputs.reference_idx}"

    # Persistent index location under the derived-artifact root (PATH_DERIVED),
    # NOT the ephemeral per-attempt workspace. On SLURM the backend propagates
    # PATH_DERIVED into the job env so get_settings() resolves the real value
    # here instead of the $TMPDIR/qiita/derived default. The layout is owned by
    # `derived_store` (the orchestrator's derived-storage convention, shared with
    # build_minimap2_index and the reference-artifact purge endpoint). A sharded
    # build lands at `.../shards/{shard_id}/index.ryxdi` (one `.ryxdi` per shard).
    path_derived = get_settings().path_derived
    index_dir = (
        shard_rype_index_path(path_derived, inputs.reference_idx, inputs.shard_id)
        if sharded
        else rype_index_path(path_derived, inputs.reference_idx)
    )
    index_dir.parent.mkdir(parents=True, exist_ok=True)
    # On a workflow retry the build re-runs against the same persistent path;
    # clear any prior (possibly partial) `.ryxdi` so the rebuild is
    # deterministic regardless of rype's overwrite behavior. Safe within scope:
    # the reference is still in `indexing` (not yet `active`) during the build,
    # and re-indexing a grown/active reference is out of scope.
    if index_dir.exists():
        shutil.rmtree(index_dir)

    # DuckDB share stays bounded (cap at `_DUCKDB_MEMORY_CAP_GB`); rype gets the
    # rest of the cgroup. The cap is small because `rype_index_create` windows its
    # chunk feed (see the split note above), so DuckDB's working set is bounded by
    # window size, not corpus size. Off SLURM both fall back to their literals
    # (4 + 30). The headroom subtracted from rype's share is the same margin DuckDB
    # reserves under the cgroup, so the two stay in lockstep from one source.
    duckdb_memory_gb = resolve_duckdb_memory_gb(
        _DUCKDB_MEMORY_GB, threads=_DUCKDB_THREADS, cap_gb=_DUCKDB_MEMORY_CAP_GB
    )
    alloc_gb = slurm_alloc_gb()
    rype_max_memory_gb = (
        _RYPE_MAX_MEMORY_GB
        if alloc_gb is None
        else max(
            _RYPE_MAX_MEMORY_GB,
            alloc_gb - duckdb_memory_gb - duckdb_headroom_gb(_DUCKDB_THREADS),
        )
    )

    with duckdb_tmp_dir(workspace) as duckdb_tmp, open_miint_conn() as conn:
        apply_duckdb_settings(conn, duckdb_tmp, memory_gb=duckdb_memory_gb, threads=_DUCKDB_THREADS)
        # Non-temp view/table (`_CHUNK_VIEW`) so rype's separate bind/execute
        # connection can resolve it by name. Host mode reads the staging Parquet
        # lazily (a VIEW rype windows over); shard mode streams the roster's
        # chunks from the data plane and MATERIALIZES them into a table inside the
        # stream `with` (draining the Flight stream so the client closes before
        # the long rype build — a shard is ~1/1000 of the reference, so the
        # materialized set is small).
        if sharded:
            # Read the shard's feature roster (small — one row per feature) to
            # scope the DoGet ticket, then stream that roster's chunks. Mirrors
            # build_minimap2_index's shard path.
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
                    " nothing to build a rype index from"
                )
            async with open_reference_chunk_stream(
                conn, reference_idx=inputs.reference_idx, feature_idx=feature_ids
            ) as rel:
                conn.execute(
                    f"CREATE OR REPLACE TABLE {_CHUNK_VIEW} AS "
                    f"SELECT feature_idx, chunk_index, chunk_data FROM {rel}"
                )
        else:
            # DuckDB rejects prepared parameters inside CREATE VIEW, so the path
            # is inlined; validate_parquet_path rejects quote/backslash/control
            # chars (the repo's fail-fast escaping contract).
            read_target_sql = validate_parquet_path(read_target)
            conn.execute(
                f"CREATE OR REPLACE VIEW {_CHUNK_VIEW} AS "
                "SELECT feature_idx, chunk_index, chunk_data "
                f"FROM read_parquet('{read_target_sql}')"
            )
        # Single-bucket mapping over every distinct feature. bucket is a
        # controlled string; escape quotes for the inlined literal.
        bucket_sql = bucket.replace("'", "''")
        conn.execute(
            f"CREATE OR REPLACE TABLE {_MAPPING_TABLE} AS "
            f"SELECT DISTINCT feature_idx, CAST('{bucket_sql}' AS VARCHAR) AS bucket_name "
            f"FROM {_CHUNK_VIEW}"
        )
        status = _run_rype_index_create(
            conn,
            _CHUNK_VIEW,
            str(index_dir),
            _MAPPING_TABLE,
            k=inputs.k,
            w=inputs.w,
            max_memory=rype_max_memory_gb * 1024**3,
        )
    if status != "ok":
        raise RuntimeError(
            f"rype_index_create returned status {status!r} (expected 'ok') for "
            f"reference {inputs.reference_idx} → {index_dir}"
        )

    params = {"k": inputs.k, "w": inputs.w, "bucket_name": bucket}
    meta_path = workspace / "rype_index_meta.json"
    # Only a sharded build adds `shard_id` to the meta JSON; host mode omits it
    # (keeping the host meta byte-identical). The runner's register-index arm
    # reads it via `meta.get("shard_id")` — absent → None → a whole-reference row.
    meta: dict = {
        "index_type": HOST_FILTER_INDEX_TYPE_RYPE,
        "fs_path": str(index_dir),
        "params": params,
    }
    if sharded:
        meta["shard_id"] = inputs.shard_id
    meta_path.write_text(json.dumps(meta))
    # Only the in-tree meta JSON is a step output. The `.ryxdi` itself lives
    # under PATH_DERIVED (outside the per-attempt workspace) on purpose — it
    # outlives the work ticket — so it CANNOT be a declared output: the launcher
    # manifest write and the verifier both require every output to resolve under
    # $QIITA_OUTPUT_PATH. register-index reads its location from meta `fs_path`.
    return {"rype_index_meta": meta_path}


def plan(inputs: Inputs) -> JobPlan:
    """Size a SHARD build's memory down from the whole-reference baseline.

    Host/unsharded mode → no opinion (empty `JobPlan` → keep the step's YAML
    baseline; the whole-reference build still gets its 64 GB). Shard mode → size
    `mem_gb` from the shard's total bp: the runtime-consistent floor
    (`_SHARD_PLAN_FLOOR_GB` — rype's `max_memory` floor + DuckDB's cap + shared
    headroom) plus a gentle per-bp term. The control plane applies this ONLY when
    it is below the step's baseline (down-only composition), so a small shard runs
    in a smaller SLURM slot (1000 shards don't each grab 64 GB) while an
    over-estimate harmlessly stays at baseline. Advisory — an under-estimate is
    still caught by the existing OOM-retry escalation. `plan()` runs at submit
    time in the orchestrator process and reads only the small roster (bp sum), not
    the chunk data."""
    if inputs.shard_id is None or inputs.shard_features is None:
        return JobPlan()
    with duckdb.connect(":memory:") as conn:
        total_bp = conn.execute(
            "SELECT COALESCE(sum(sequence_length_bp), 0) FROM read_parquet(?)",
            [str(inputs.shard_features)],
        ).fetchone()[0]
    mem_gb = _SHARD_PLAN_FLOOR_GB + math.ceil(total_bp / _SHARD_PLAN_BP_PER_GB)
    return JobPlan(resources=JobResourcePlan(mem_gb=mem_gb))
