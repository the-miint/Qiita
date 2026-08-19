"""Native job: build the whole-reference rype ROUTER index (sharded routing).

Where `build_rype_index` builds a POSITIVE single-bucket host index (host = any
emitted row) and its shard-mode twin builds one per-shard `.ryxdi`, this job
builds ONE whole-reference MULTI-bucket router: a single `.ryxdi` over every
sharded feature, with one bucket per shard (`bucket_name = str(shard_id)`). One
`rype_classify` pass against it emits `(read_id, bucket_name)` rows = the
`read_to_shard` table the sharded aligners (`align_{minimap2,bowtie2}_sharded`)
need — in O(1) classify passes rather than N per-shard passes (impractical
at `_SHARD_COUNT=1000`).

The router STREAMS the whole reference's chunks from the data plane (the
`open_reference_chunk_stream` seam, whole-reference `feature_idx=None`) — it runs
after ingest's register-files has moved staging into DuckLake, so there is no
staging Parquet — persists them to a workspace Parquet, then builds a VIEW over
that Parquet. That two-step (stream → disk → lazy VIEW) is deliberate: a Flight
stream is single-pass and visible only on the connection it is registered on, but
`rype_index_create` windows its chunk feed on a SEPARATE connection (it opens its
own connection on the same DuckDB instance during bind/execute), so the streamed
chunks must first land on disk where that separate connection can re-scan them.
This is exactly the lazy-VIEW-over-Parquet shape `build_rype_index` uses in host
mode; the only difference is the source (a stream persisted to the workspace
rather than the ingest staging Parquet), so the whole-reference build stays
memory-bounded (rype windows over the on-disk Parquet; DuckDB never materialises
the corpus into an in-memory table).

The shard->bucket mapping rides a runner-staged `shard_mapping` Parquet
`(feature_idx BIGINT, bucket_name VARCHAR = str(shard_id))` (built from
`reference_membership.shard_id`; that runner staging + the workflow wiring are
external to this job — the smoke feeds the mapping directly) and is passed as
`rype_index_create(mapping_table:=…)`.

Writes `rype_router_index_path(idx)`
(`{PATH_DERIVED}/references/{idx}/rype-router.ryxdi`); meta
`index_type="rype_router"`, `shard_id` omitted (whole-reference → NULL). This job
does NOT register a `reference_index` row for the router (its smoke passes the path
directly to the align job), so no row ever carries the `rype_router` type yet;
the sharded reference-add path registers it and adds the one-line
`reference_index.index_type` CHECK migration for the value in the same PR.

miint signature (see docs/duckdb-miint.md — the single qiita-verified source):
  rype_index_create(chunk_table, output_path, [mapping_table], [k=64], [w=50],
                    [salt=6148914691236517205], [orient=true], [max_memory=0])
Identical to the call `build_rype_index` makes; the only difference is a
MULTI-bucket `mapping_table` (one bucket per shard) instead of a single bucket.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import duckdb
from pydantic import BaseModel
from qiita_common.models import INDEX_TYPE_RYPE_ROUTER
from qiita_common.parquet import validate_parquet_path

from ..config import get_settings
from ..data_plane_client import open_reference_chunk_stream
from ..derived_store import rype_router_index_path
from ..miint import (
    PARQUET_OPTS_CHUNKED,
    apply_duckdb_settings,
    duckdb_headroom_gb,
    duckdb_tmp_dir,
    open_miint_conn,
    resolve_duckdb_memory_gb,
    slurm_alloc_gb,
)

YAML_STEP_NAME = "build_routing_index"

# DuckDB/rype memory split, identical in shape to build_rype_index: DuckDB feeds
# rype the chunk stream and stays bounded via a HARD cap (it only streams/persists
# chunks and computes nothing heavy), while rype does the in-process index build
# and gets the rest of the cgroup via `max_memory`. The literals are the OFF-SLURM
# fallbacks (local backend / tests); under SLURM the split tracks the real cgroup.
# rype's feed is windowed (not a whole-corpus sort), so DuckDB's cap is small; see
# build_rype_index for the full split rationale (this job mirrors it).
_DUCKDB_MEMORY_GB = 4
_DUCKDB_MEMORY_CAP_GB = 8
_DUCKDB_THREADS = 8
_RYPE_MAX_MEMORY_GB = 30

# rype build defaults. w=20 is passed explicitly (the function default is 50) so
# the router's minimizer scheme matches the reference's own rype indexing. Pinned
# salt keeps the build reproducible and the call all-positional-or-inlined.
_DEFAULT_K = 64
_DEFAULT_W = 20
_DEFAULT_SALT = 6148914691236517205

# In-DuckDB names handed to rype_index_create (resolved by its separate
# connection — must be non-temp).
_CHUNK_VIEW = "router_chunk_input"
_MAPPING_TABLE = "router_bucket_map"


class Inputs(BaseModel):
    """Typed input contract for build_routing_index.

    `reference_idx` and `work_ticket_idx` are framework-injected scope scalars.
    `shard_mapping` is a runner-staged Parquet roster
    `(feature_idx BIGINT, bucket_name VARCHAR)` where `bucket_name = str(shard_id)`
    — one row per sharded feature, built from `reference_membership.shard_id` (the
    runner stages it, and the smoke stages it directly). It is BOTH the
    `rype_index_create` mapping table AND the authoritative shard-membership set:
    the router is built over exactly these features (streamed whole-reference),
    with each routed to its shard's bucket.

    `k`/`w` are the rype build parameters (default to the same values
    `build_rype_index` uses so routing and reference indexing share a minimizer
    scheme). There is no shard mode — the router is always whole-reference.
    """

    reference_idx: int
    work_ticket_idx: int
    shard_mapping: Path
    k: int = _DEFAULT_K
    w: int = _DEFAULT_W


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
    """Seam around miint's `rype_index_create`. Isolated so unit tests can stub
    the real build (which needs the extension + real sequence bytes). Returns the
    status string from the function's single status row.

    A deliberate twin of `build_rype_index._run_rype_index_create` (each builder
    owns its own stub seam, like the minimap2/bowtie2 save seams) — the call is
    identical: two positional args (`chunk_table`, `output_path`, both VARCHAR
    table-name / path values bound as `?`); everything else NAMED — `mapping_table`
    (VARCHAR, quote-escaped), `k`/`w` (INTEGER, inlined int()), `salt` (UBIGINT,
    pinned constant), `orient` (BOOLEAN), `max_memory` (BIGINT). `chunk_table` /
    `mapping_table` are table NAMES the function resolves on its own connection.
    orient=TRUE keeps both-strand matching — a read can minimise into a shard on
    either strand, so it must route on both."""
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
    if not inputs.shard_mapping.exists():
        raise FileNotFoundError(f"shard_mapping not found: {inputs.shard_mapping}")

    # Persistent router location under the derived-artifact root (PATH_DERIVED),
    # NOT the ephemeral per-attempt workspace. On SLURM the backend propagates
    # PATH_DERIVED into the job env so get_settings() resolves the real value. The
    # layout is owned by `derived_store` (shared with the reference-artifact purge
    # endpoint). One `.ryxdi` directory per reference (whole-reference router).
    path_derived = get_settings().path_derived
    router_dir = rype_router_index_path(path_derived, inputs.reference_idx)
    router_dir.parent.mkdir(parents=True, exist_ok=True)
    # On a workflow retry the build re-runs against the same persistent path; clear
    # any prior (possibly partial) `.ryxdi` so the rebuild is deterministic. Safe
    # within scope: the reference is still `indexing` (not yet `active`).
    if router_dir.exists():
        shutil.rmtree(router_dir)

    # DuckDB share stays bounded (cap at `_DUCKDB_MEMORY_CAP_GB`); rype gets the
    # rest of the cgroup. Off SLURM both fall back to their literals (4 + 30). The
    # headroom subtracted from rype's share is the same margin DuckDB reserves.
    # Mirrors build_rype_index.
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

    chunks_parquet = workspace / "router_chunks.parquet"
    # Validate the COPY target ONCE and reuse it for both the write and the re-scan
    # (DuckDB rejects bound params inside COPY / CREATE VIEW, so the path is inlined;
    # validate_parquet_path enforces the fail-fast escaping contract on both).
    chunks_sql = validate_parquet_path(chunks_parquet)
    try:
        with duckdb_tmp_dir(workspace) as duckdb_tmp, open_miint_conn() as conn:
            apply_duckdb_settings(
                conn, duckdb_tmp, memory_gb=duckdb_memory_gb, threads=_DUCKDB_THREADS
            )
            # Stream the WHOLE reference (feature_idx=None) and persist the chunks to
            # a workspace Parquet — the COPY drains the Flight stream (so the client
            # closes before the long rype build) and lands the chunks where rype's
            # separate connection can re-scan them. Chunked-Parquet write settings
            # keep the row layout narrow on genome-scale contigs (same as ingest).
            async with open_reference_chunk_stream(
                conn, reference_idx=inputs.reference_idx, feature_idx=None
            ) as rel:
                conn.execute(
                    f"COPY (SELECT feature_idx, chunk_index, chunk_data FROM {rel}) "
                    f"TO '{chunks_sql}' ({PARQUET_OPTS_CHUNKED})"
                )

            # Multi-bucket mapping straight from the staged Parquet: one bucket per
            # shard (`bucket_name = str(shard_id)`), each feature at most once. Cast
            # to the exact rype mapping_table types. Built BEFORE the chunk corpus
            # view so the corpus can be scoped to exactly the mapped feature set.
            mapping_sql = validate_parquet_path(inputs.shard_mapping)
            conn.execute(
                f"CREATE OR REPLACE TABLE {_MAPPING_TABLE} AS "
                "SELECT CAST(feature_idx AS BIGINT) AS feature_idx, "
                "CAST(bucket_name AS VARCHAR) AS bucket_name "
                f"FROM read_parquet('{mapping_sql}')"
            )
            # Mapping integrity (fail-fast, per the repo's "silent failures are bugs"
            # ethos): this job takes the mapping on faith from an external staging
            # step (the runner), unlike build_rype_index which derives its own from
            # the chunks — so validate what that roster is contractually required to
            # be.
            num_mapped, non_null_features, num_features_mapped, num_buckets, non_null_buckets = (
                conn.execute(
                    f"SELECT count(*), count(feature_idx), count(DISTINCT feature_idx), "
                    f"count(DISTINCT bucket_name), count(bucket_name) FROM {_MAPPING_TABLE}"
                ).fetchone()
            )
            if num_mapped == 0:
                raise ValueError(
                    f"shard_mapping ({inputs.shard_mapping}) is empty: "
                    "nothing maps features to shards, so there is no router to build"
                )
            if non_null_features != num_mapped or non_null_buckets != num_mapped:
                raise ValueError(
                    f"shard_mapping ({inputs.shard_mapping}) has NULL feature_idx or "
                    "bucket_name rows — every row must map a feature to a shard bucket"
                )
            if num_features_mapped != num_mapped:
                raise ValueError(
                    f"shard_mapping ({inputs.shard_mapping}) maps a feature_idx to more "
                    "than one shard: each feature belongs to exactly one shard "
                    f"({num_mapped} rows, {num_features_mapped} distinct features)"
                )

            # The rype corpus is the MAPPED feature set only. The stream is
            # whole-reference (every member), but a reference may legitimately have
            # no-genome members (16S / deferred / a genome map that covers only a
            # subset of contigs) that plan_shards leaves with shard_id NULL and thus
            # OUT of shard_mapping. Those features live in no shard, so routing a read
            # to them is meaningless — scope the corpus to the mapped set (a semi-join
            # against the mapping) rather than rejecting the whole build. This is what
            # makes a partial genome map a SUPPORTED input instead of a hard failure
            # that only surfaces AFTER the per-shard fan-out has committed hours of
            # index builds.
            conn.execute(
                f"CREATE OR REPLACE VIEW {_CHUNK_VIEW} AS "
                "SELECT feature_idx, chunk_index, chunk_data "
                f"FROM read_parquet('{chunks_sql}') "
                f"WHERE feature_idx IN (SELECT feature_idx FROM {_MAPPING_TABLE})"
            )
            num_features = conn.execute(
                f"SELECT count(DISTINCT feature_idx) FROM {_CHUNK_VIEW}"
            ).fetchone()[0]
            if num_features == 0:
                # No MAPPED feature streamed any chunks. This job only runs where a
                # router is required, so an empty corpus is a fail-fast, not a
                # silent empty router.
                raise ValueError(
                    f"reference {inputs.reference_idx}: no shard-mapped feature "
                    "streamed any sequence chunks — nothing to build a routing "
                    "index from"
                )
            # A mapped feature with NO streamed chunks is still a real error: a
            # bucket over a feature that is not in the reference. (The reverse — a
            # streamed feature absent from the mapping — is the legitimate no-genome
            # case the scoping above absorbs, no longer an error.) Every mapped
            # feature is distinct (checked above), so the count of mapped features
            # that DID stream chunks (num_features) subtracted from the distinct
            # mapped roster is exactly the unchunked-mapping count.
            unchunked_mappings = num_features_mapped - num_features
            if unchunked_mappings:
                raise ValueError(
                    f"reference {inputs.reference_idx}: shard_mapping "
                    f"({inputs.shard_mapping}) maps {unchunked_mappings} feature(s) "
                    "with no streamed chunks — a bucket over features that are not "
                    "in the reference"
                )

            status = _run_rype_index_create(
                conn,
                _CHUNK_VIEW,
                str(router_dir),
                _MAPPING_TABLE,
                k=inputs.k,
                w=inputs.w,
                max_memory=rype_max_memory_gb * 1024**3,
            )
        if status != "ok":
            raise RuntimeError(
                f"rype_index_create returned status {status!r} (expected 'ok') for the "
                f"router of reference {inputs.reference_idx} → {router_dir}"
            )

        params = {
            "k": inputs.k,
            "w": inputs.w,
            "source": "stream",
            # feature_count is the ROUTED feature set (mapped features with chunks),
            # not the whole reference — no-genome members are not in the router.
            "feature_count": num_features,
            "shard_count": num_buckets,
        }
        meta_path = workspace / "routing_index_meta.json"
        # No `shard_id` key — the router is whole-reference, so register-index reads
        # meta.get("shard_id") -> None -> a NULL shard_id row (matching the host
        # rype/minimap2 rows). Only the in-tree meta JSON is a step output; the
        # `.ryxdi` lives under PATH_DERIVED (outside the workspace) so it CANNOT be a
        # declared output — register-index reads its location from meta `fs_path`.
        meta = {
            "index_type": INDEX_TYPE_RYPE_ROUTER,
            "fs_path": str(router_dir),
            "params": params,
        }
        meta_path.write_text(json.dumps(meta))
        return {"routing_index_meta": meta_path}
    finally:
        # router_chunks.parquet is a whole-reference dump (tens of GB at GG2 scale),
        # NOT a declared output and consumed only within this call (rype re-scans it
        # via _CHUNK_VIEW above). Remove it unconditionally so it does not leak into
        # shared PATH_SCRATCH per build AND per retry attempt — the same "no space in
        # /tmp" hazard the duckdb_tmp_dir guard exists for. missing_ok covers a
        # failure before the COPY created it.
        chunks_parquet.unlink(missing_ok=True)
