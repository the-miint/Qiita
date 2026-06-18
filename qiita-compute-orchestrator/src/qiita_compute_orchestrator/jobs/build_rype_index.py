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
import shutil
from pathlib import Path

import duckdb
from pydantic import BaseModel

from ..config import get_settings
from ..derived_store import rype_index_path
from ..miint import (
    apply_duckdb_settings,
    duckdb_headroom_gb,
    open_miint_conn,
    resolve_duckdb_memory_gb,
    slurm_alloc_gb,
)

YAML_STEP_NAME = "build_rype_index"

# Co-consumer step: DuckDB feeds the chunk stream to rype + computes the small
# DISTINCT mapping, while rype does the heavy index build in-process and gets the
# bulk of the cgroup via `max_memory`. rype stays the ELASTIC consumer (its share
# grows with the allocation); DuckDB takes a bigger-but-BOUNDED share via a cap,
# so a larger `--mem-gb` (#102) buys a bigger rype build, not a ballooning DuckDB.
#
# The three literals are the OFF-SLURM fallbacks (local backend / tests). Under
# SLURM the split tracks the real cgroup: DuckDB = min(alloc − headroom,
# `_DUCKDB_MEMORY_CAP_GB`), rype = (allocation − DuckDB − headroom) floored at the
# 24 GB fallback. The DuckDB cap is NOT the 4 GB fallback: feeding a genome-scale
# chunk scan (the full sequence bytes, streamed to rype's read) needs well more
# than 4 GB — a human host reference (T2T-CHM13) OOMed DuckDB at ~3.7 GB under
# the 4 GB cap while reading `rype_chunk_input`, before rype's `max_memory` was
# ever exercised.
# `_DUCKDB_MEMORY_CAP_GB` is a heuristic (~5× the raw single-genome chunk bytes);
# tune it against a real genome-scale MaxRSS — the DuckDB/rype split here is the
# one knob this step exposes.
_DUCKDB_MEMORY_GB = 4
# Under-SLURM ceiling for DuckDB's share (see the split note above); the 4 GB
# fallback only applies off-SLURM.
_DUCKDB_MEMORY_CAP_GB = 16
_DUCKDB_THREADS = 4
_RYPE_MAX_MEMORY_GB = 24

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

    `reference_sequence_chunks` is the feature-keyed chunk output of the
    `load` step (a DIRECTORY of `part_*.parquet`, or a single Parquet file).
    `reference_idx` and `work_ticket_idx` are framework-injected scope scalars.
    `k` / `w` are the rype build parameters (host-filter defaults); `bucket_name`
    overrides the default single-bucket name.
    """

    reference_sequence_chunks: Path
    reference_idx: int
    work_ticket_idx: int
    k: int = _DEFAULT_K
    w: int = _DEFAULT_W
    bucket_name: str | None = None


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
    chunks = inputs.reference_sequence_chunks
    if not chunks.exists():
        raise FileNotFoundError(f"reference_sequence_chunks not found: {chunks}")
    # reference_load emits chunks as a directory of part_*.parquet; accept a
    # single file too (tests / future producers).
    read_target = str(chunks / "part_*.parquet") if chunks.is_dir() else str(chunks)

    bucket = inputs.bucket_name or f"reference_{inputs.reference_idx}"

    # Persistent index location under the derived-artifact root (PATH_DERIVED),
    # NOT the ephemeral per-attempt workspace. On SLURM the backend propagates
    # PATH_DERIVED into the job env so get_settings() resolves the real value
    # here instead of the $TMPDIR/qiita/derived default. The layout is owned by
    # `derived_store` (the orchestrator's derived-storage convention, shared with
    # build_minimap2_index and the reference-artifact purge endpoint).
    index_dir = rype_index_path(get_settings().path_derived, inputs.reference_idx)
    index_dir.parent.mkdir(parents=True, exist_ok=True)
    # On a workflow retry the build re-runs against the same persistent path;
    # clear any prior (possibly partial) `.ryxdi` so the rebuild is
    # deterministic regardless of rype's overwrite behavior. Safe within scope:
    # the reference is still in `indexing` (not yet `active`) during the build,
    # and re-indexing a grown/active reference is out of scope.
    if index_dir.exists():
        shutil.rmtree(index_dir)

    duckdb_tmp = workspace / ".duckdb_tmp"
    duckdb_tmp.mkdir(parents=True, exist_ok=True)

    # DuckDB share stays bounded (cap at `_DUCKDB_MEMORY_CAP_GB`, big enough to
    # feed a genome-scale chunk scan — NOT the 4 GB fallback, which OOMed on
    # T2T-CHM13); rype gets the rest of the cgroup. Off SLURM both fall back to
    # their literals (4 + 24). The headroom subtracted from rype's share is the
    # same margin DuckDB reserves under the cgroup, so the two stay in lockstep
    # from one source.
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

    with open_miint_conn() as conn:
        apply_duckdb_settings(conn, duckdb_tmp, memory_gb=duckdb_memory_gb, threads=_DUCKDB_THREADS)
        # Non-temp view/table so rype's separate bind/execute connection can
        # resolve them by name. DuckDB rejects prepared parameters inside
        # CREATE VIEW, so the path is inlined (quote-escaped — it's a
        # filesystem path, no other injection surface).
        read_target_sql = read_target.replace("'", "''")
        conn.execute(
            f"CREATE OR REPLACE VIEW {_CHUNK_VIEW} AS "
            f"SELECT feature_idx, chunk_index, chunk_data FROM read_parquet('{read_target_sql}')"
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
    meta_path.write_text(
        json.dumps({"index_type": "rype", "fs_path": str(index_dir), "params": params})
    )
    # Only the in-tree meta JSON is a step output. The `.ryxdi` itself lives
    # under PATH_DERIVED (outside the per-attempt workspace) on purpose — it
    # outlives the work ticket — so it CANNOT be a declared output: the launcher
    # manifest write and the verifier both require every output to resolve under
    # $QIITA_OUTPUT_PATH. register-index reads its location from meta `fs_path`.
    return {"rype_index_meta": meta_path}
