"""Native job: build a minimap2 `.mmi` index for a host reference.

A sidecar to `build_rype_index`: where rype is the first-pass host filter
(minimizer classify), the minimap2 `.mmi` is the second-pass aligner index
`host_filter` consumes (`align_minimap2(index_path=<.mmi>, preset='sr')`). It is
a reusable, precomputed artifact written to a PERSISTENT location under the
derived-artifact root (`{path_derived}/references/{idx}/minimap2/index.mmi`), NOT
the ephemeral per-attempt workspace — it outlives the work ticket.

Input is the SAME feature-keyed chunked Parquet `build_rype_index` consumes
(`reference_sequence_chunks`: `feature_idx, chunk_index, chunk_data`, the chunks
`reference_load` re-emits and `register-files` moves into the data plane). minimap2
needs whole reference contigs, so the chunks are reassembled per feature via
`string_agg(chunk_data ORDER BY chunk_index)` into a `(read_id, sequence1)`
subject — the index is built from exactly the bytes stored in the data plane, no
special raw-FASTA side channel. Because it reads the chunks (the same as rype),
this step must run AFTER the chunks are produced and BEFORE `register-files`
moves them — identical ordering to `build_rype_index`.

The subject is staged as a non-temp TABLE because miint's `save_minimap2_index`
opens a SEPARATE connection on the same DuckDB instance during bind/execute,
which resolves regular `view`/`table` names but not TEMP tables / CTEs (see
docs/duckdb-miint.md). A TABLE (not a VIEW): the `string_agg ... GROUP BY`
reassembly is a blocking aggregation that can't stream, so we materialise it once
(mirroring build_rype_index's `_MAPPING_TABLE`) instead of recomputing it on every
scan minimap2 issues against the subject.

miint signature (qiita-verified against the team-mirror build; see
docs/duckdb-miint.md):
  save_minimap2_index(subject_table, output_path, [eqx], [w], [k], [preset])
returns a single row `(success BOOLEAN, index_path VARCHAR, num_subjects INTEGER)`.
Exactly TWO positional args (subject table NAME, output path); `preset` is named.
The control plane records only a small params copy (preset + source) via the meta
JSON this job writes — native step outputs are paths, so params can't ride a
binding directly (mirrors build_rype_index).
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import duckdb
from pydantic import BaseModel

from ..config import get_settings
from ..miint import apply_duckdb_settings, open_miint_conn, resolve_duckdb_memory_gb

YAML_STEP_NAME = "build_minimap2_index"

# Co-consumer step: DuckDB reassembles the per-feature subject (string_agg over
# chunks) and hands the TABLE to minimap2, which does the heavy indexing
# in-process from the cgroup remainder. `_DUCKDB_MEMORY_GB` is the OFF-SLURM
# fallback (local backend / tests). Under SLURM the limit tracks the real cgroup
# via `resolve_duckdb_memory_gb()` with `_MINIMAP2_RESERVE_GB` carved out for
# minimap2's in-process index — so a `--mem-gb` override (#102) grows both the
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


class Inputs(BaseModel):
    """Typed input contract for build_minimap2_index.

    `reference_sequence_chunks` is the feature-keyed chunk output of the `load`
    step (a DIRECTORY of `part_*.parquet`, or a single Parquet file) — the SAME
    binding `build_rype_index` consumes. `reference_idx` and `work_ticket_idx`
    are framework-injected scope scalars (REFERENCE kind plus the always-on
    work_ticket_idx); declaring them keeps the contract explicit, mirroring
    build_rype_index / stage_local_fasta. `preset` is the minimap2 preset baked
    into the index.
    """

    reference_sequence_chunks: Path
    reference_idx: int
    work_ticket_idx: int
    preset: str = _DEFAULT_PRESET


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


def _stage_subject(conn: duckdb.DuckDBPyConnection, read_target: str) -> int:
    """Reassemble the feature-keyed chunked Parquet into a `(read_id, sequence1)`
    subject TABLE via `string_agg(chunk_data ORDER BY chunk_index)` GROUP BY
    feature_idx. The ORDER BY makes the reassembly independent of scan order
    (preserve_insertion_order=false); `feature_idx` is the subject identifier (it
    surfaces as the alignment `reference` column, which host_filter ignores — it
    only checks for any hit). Returns the subject row count so the caller can
    fail fast on an empty reference. A TABLE (not a VIEW) because the GROUP BY is
    a blocking aggregation: materialise once rather than recompute on every scan
    minimap2's separate connection issues against the subject.

    The path is inlined (quote-escaped — a filesystem path, no other injection
    surface): DuckDB rejects prepared parameters inside CREATE TABLE AS."""
    target_sql = read_target.replace("'", "''")
    conn.execute(
        f"CREATE OR REPLACE TABLE {_SUBJECT_RELATION} AS "
        "SELECT feature_idx AS read_id, "
        "string_agg(chunk_data, '' ORDER BY chunk_index) AS sequence1 "
        f"FROM read_parquet('{target_sql}') GROUP BY feature_idx"
    )
    return conn.execute(f"SELECT count(*) FROM {_SUBJECT_RELATION}").fetchone()[0]


async def execute(inputs: Inputs, workspace: Path) -> dict[str, Path]:
    chunks = inputs.reference_sequence_chunks
    if not chunks.exists():
        raise FileNotFoundError(f"reference_sequence_chunks not found: {chunks}")
    # reference_load emits chunks as a directory of part_*.parquet; accept a
    # single file too (tests / future producers). Same resolution as build_rype_index.
    read_target = str(chunks / "part_*.parquet") if chunks.is_dir() else str(chunks)

    # Persistent index location under the derived-artifact root (PATH_DERIVED),
    # NOT the ephemeral per-attempt workspace. On SLURM the backend propagates
    # PATH_DERIVED into the job env so get_settings() resolves the real value.
    derived_root = Path(get_settings().path_derived)
    index_path = derived_root / "references" / str(inputs.reference_idx) / "minimap2" / "index.mmi"
    index_path.parent.mkdir(parents=True, exist_ok=True)
    # On a workflow retry the build re-runs against the same persistent path;
    # clear any prior (possibly partial) `.mmi` so the rebuild is deterministic.
    # The minimap2 index is a single FILE, so unlink (not rmtree). Safe within
    # scope: the reference is still `indexing` (not yet `active`) during the build.
    index_path.unlink(missing_ok=True)

    duckdb_tmp = workspace / ".duckdb_tmp"
    duckdb_tmp.mkdir(parents=True, exist_ok=True)

    try:
        with open_miint_conn() as conn:
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
            num_subjects = _stage_subject(conn, read_target)
            if num_subjects == 0:
                # The chunked reference is empty — there is nothing to index. This
                # step only runs in the host-reference workflows where a minimap2
                # index is required, so an empty reference is a fail-fast, not a
                # silent empty index.
                raise ValueError(
                    f"reference {inputs.reference_idx} has no sequence chunks "
                    f"({read_target}): nothing to build a minimap2 index from"
                )
            success = _run_save_minimap2_index(
                conn, _SUBJECT_RELATION, str(index_path), preset=inputs.preset
            )
        if not success:
            raise RuntimeError(
                f"save_minimap2_index reported failure for reference {inputs.reference_idx} "
                f"→ {index_path}"
            )
    finally:
        # Drop the DuckDB spill dir (possibly large) before returning so it
        # doesn't accumulate in the shared work-ticket workspace — mirrors the
        # sibling native DuckDB jobs (host_filter, stage_local_fasta, …).
        shutil.rmtree(duckdb_tmp, ignore_errors=True)

    params = {"preset": inputs.preset, "source_chunks": str(chunks), "num_subjects": num_subjects}
    meta_path = workspace / "minimap2_index_meta.json"
    meta_path.write_text(
        json.dumps({"index_type": "minimap2", "fs_path": str(index_path), "params": params})
    )
    # The persistent .mmi lives under PATH_DERIVED, OUTSIDE the ephemeral
    # workspace, so it is NOT a step output: the manifest/verify contract requires
    # every declared output to resolve under $QIITA_OUTPUT_PATH (see
    # jobs/__main__._write_manifest and slurm/verify.py). Its location travels in
    # the meta JSON's `fs_path`, which the register-index step consumes.
    return {"minimap2_index_meta": meta_path}
