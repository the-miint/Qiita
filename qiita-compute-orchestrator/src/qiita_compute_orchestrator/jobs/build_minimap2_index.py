"""Native job: build a minimap2 `.mmi` index for a host reference.

A sidecar to `build_rype_index`: where rype is the first-pass host filter
(minimizer classify), the minimap2 `.mmi` is the second-pass aligner index
`host_filter` consumes (`align_minimap2(index_path=<.mmi>, preset='sr')`). It is
a reusable, precomputed artifact written to a PERSISTENT location under the
derived-artifact root (`{path_derived}/references/{idx}/minimap2/index.mmi`), NOT
the ephemeral per-attempt workspace — it outlives the work ticket.

Unlike rype (which indexes the feature-keyed chunks `reference_load` re-emits),
this job builds the index from the **raw** host FASTA, so it carries NO ordering
dependency on `register-files` (which MOVES the staging chunks). Two input modes,
exactly one supplied:

  - LOCAL (`minimap2_fasta_manifest`): a manifest of absolute FASTA paths (the
    minimap2-tagged subset `stage_local_fasta` emits). Each is parsed with
    miint's `read_fastx` and UNION-ALL'd into a `(read_id, sequence1)` subject
    VIEW — no Python sequence handling, no double materialisation.
  - UPLOAD (`fasta_path`): the chunked `upload.parquet`
    `(read_id, chunk_index, chunk_data)` `_resolve_upload_handles` resolves from
    `fasta_upload_idx` (the same binding `hash_sequences` consumes). Reassembled
    to `(read_id, sequence1)` via `string_agg(chunk_data ORDER BY chunk_index)`.

The subject is staged as a non-temp relation because miint's `save_minimap2_index`
opens a SEPARATE connection on the same DuckDB instance during bind/execute,
which resolves regular `view`/`table` names but not TEMP tables / CTEs (see
docs/duckdb-miint.md). LOCAL stages a VIEW over `read_fastx` so the raw genome
streams from disk (bounded by `max_batch_bytes`) rather than landing in DuckDB's
heap. UPLOAD stages a TABLE: the `string_agg ... GROUP BY` reassembly is a
blocking aggregation that can't stream, so we materialise it once (mirroring
build_rype_index's `_MAPPING_TABLE`) instead of recomputing it on every scan
minimap2 issues against the subject.

miint signature (qiita-verified against the team-mirror build; see
docs/duckdb-miint.md):
  save_minimap2_index(subject_table, output_path, [eqx], [w], [k], [preset])
returns a single row `(success BOOLEAN, index_path VARCHAR, num_subjects INTEGER)`.
Exactly TWO positional args (subject table NAME, output path); `preset` is named.
The control plane records only a small params copy (preset + source_files) via
the meta JSON this job writes — native step outputs are paths, so params can't
ride a binding directly (mirrors build_rype_index).
"""

from __future__ import annotations

import json
from pathlib import Path

import duckdb
from pydantic import BaseModel, model_validator
from qiita_common.duckdb_miint import is_empty_sequence_file

from ..config import get_settings
from ..miint import apply_duckdb_settings, open_miint_conn

YAML_STEP_NAME = "build_minimap2_index"

# DuckDB only stages the subject sequences (read_fastx parse, or string_agg
# reassembly) and hands the VIEW to minimap2, which does the heavy indexing
# in-process — so DuckDB's own cap is modest and most of the YAML allocation is
# left for minimap2 + Python + OS headroom. Literals mirror the host-reference-add
# YAML's baseline_resources for this step (a mismatch is visible at review).
# Genome-scale sizing (a real host reference) is a deliberate follow-up: bump the
# YAML mem_gb and this cap together when sized against real data.
_DUCKDB_MEMORY_GB = 8
_DUCKDB_THREADS = 4

# Byte budget per `read_fastx` batch — caps the read-side vector so a run of
# multi-MB genome records can't materialise a giant chunk before minimap2 reads
# the subject. Same lever stage_local_fasta uses.
_READ_FASTX_MAX_BATCH_BYTES = "64MB"

# minimap2 preset default — 'sr' (short-read), the host-filter alignment mode
# `host_filter` mirrors on the query side. Overridable via Inputs.
_DEFAULT_PRESET = "sr"

# In-DuckDB name handed to save_minimap2_index (resolved by its separate
# connection — must be non-temp; a VIEW for LOCAL, a TABLE for UPLOAD).
_SUBJECT_RELATION = "minimap2_subject"


class Inputs(BaseModel):
    """Typed input contract for build_minimap2_index.

    Exactly one of `minimap2_fasta_manifest` (LOCAL) or `fasta_path` (UPLOAD) is
    supplied — the workflow YAML binds one or the other. `reference_idx` and
    `work_ticket_idx` are framework-injected scope scalars (REFERENCE kind plus
    the always-on work_ticket_idx); declaring them keeps the contract explicit,
    mirroring build_rype_index / stage_local_fasta. `preset` is the minimap2
    preset baked into the index.
    """

    minimap2_fasta_manifest: Path | None = None
    fasta_path: Path | None = None
    reference_idx: int
    work_ticket_idx: int
    preset: str = _DEFAULT_PRESET

    @model_validator(mode="after")
    def _exactly_one_source(self) -> Inputs:
        supplied = [self.minimap2_fasta_manifest, self.fasta_path]
        n = sum(s is not None for s in supplied)
        if n != 1:
            raise ValueError(
                "build_minimap2_index requires exactly one of "
                "minimap2_fasta_manifest (local) or fasta_path (upload); "
                f"got {n}"
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


def _read_manifest(manifest_path: Path) -> list[Path]:
    """Parse the minimap2-tagged FASTA manifest into validated absolute paths.

    One path per line; blank lines and `#` comments skipped. Every entry must be
    absolute and an existing file (fail fast). Raises ValueError on the first bad
    entry or if no real path lines remain. Mirrors `stage_local_fasta._read_manifest`
    (Phase 3 consolidates if it stays clean)."""
    if not manifest_path.is_absolute():
        raise ValueError(f"minimap2_fasta_manifest must be absolute, got {manifest_path!r}")
    if not manifest_path.exists() or not manifest_path.is_file():
        raise FileNotFoundError(f"minimap2 FASTA manifest not found or not a file: {manifest_path}")

    fasta_paths: list[Path] = []
    for raw in manifest_path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        entry = Path(line)
        if not entry.is_absolute():
            raise ValueError(f"manifest entry must be absolute, got {line!r}")
        if not entry.exists() or not entry.is_file():
            raise ValueError(f"FASTA file in manifest not found or not a file: {entry}")
        fasta_paths.append(entry)

    if not fasta_paths:
        # This step only runs in the host-reference workflows, where a minimap2
        # index is required — an empty subset manifest means the operator didn't
        # designate any sequences for it. stage_local_fasta builds this file from
        # the `\tminimap2`-tagged lines of the source manifest, so the actionable
        # cause is "no FASTA was tagged", not "the file is malformed".
        raise ValueError(
            f"minimap2 FASTA manifest is empty ({manifest_path}): a host reference "
            "needs at least one FASTA designated for the minimap2 index — add a "
            "trailing '\\tminimap2' flag to the relevant line(s) in the source manifest"
        )
    return fasta_paths


def _stage_subject_from_manifest(conn: duckdb.DuckDBPyConnection, fasta_paths: list[Path]) -> None:
    """LOCAL mode: stage every tagged raw FASTA into a `(read_id, sequence1)`
    subject VIEW via `read_fastx`, UNION-ALL'd across files. Skips empty files
    (read_fastx throws on a zero-record input). Raises ValueError if every file
    is empty — there is nothing to index.

    Paths are inlined (quote-escaped — a filesystem path, no other injection
    surface): DuckDB rejects prepared parameters inside CREATE VIEW."""
    selects: list[str] = []
    for fasta_path in fasta_paths:
        if is_empty_sequence_file(fasta_path):
            continue
        path_sql = str(fasta_path).replace("'", "''")
        selects.append(
            "SELECT read_id, sequence1 FROM "
            f"read_fastx('{path_sql}', max_batch_bytes := '{_READ_FASTX_MAX_BATCH_BYTES}')"
        )
    if not selects:
        raise ValueError("every FASTA file in the minimap2 manifest is empty — nothing to index")
    conn.execute(f"CREATE OR REPLACE VIEW {_SUBJECT_RELATION} AS {' UNION ALL '.join(selects)}")


def _stage_subject_from_parquet(conn: duckdb.DuckDBPyConnection, fasta_parquet: Path) -> None:
    """UPLOAD mode: reassemble the chunked `(read_id, chunk_index, chunk_data)`
    upload.parquet into a `(read_id, sequence1)` subject TABLE via
    `string_agg(chunk_data ORDER BY chunk_index)`. The ORDER BY makes the
    reassembly independent of scan order (preserve_insertion_order=false).

    A TABLE (not a VIEW): the reassembly is a blocking aggregation that can't
    stream, so materialise it once rather than recompute it on each scan
    minimap2's separate connection issues against the subject."""
    parquet_sql = str(fasta_parquet).replace("'", "''")
    conn.execute(
        f"CREATE OR REPLACE TABLE {_SUBJECT_RELATION} AS "
        "SELECT read_id, string_agg(chunk_data, '' ORDER BY chunk_index) AS sequence1 "
        f"FROM read_parquet('{parquet_sql}') GROUP BY read_id"
    )


async def execute(inputs: Inputs, workspace: Path) -> dict[str, Path]:
    # Resolve the subject source (LOCAL manifest vs UPLOAD parquet); the Inputs
    # validator guarantees exactly one is set. `fasta_paths` is the LOCAL marker
    # (None in UPLOAD mode) — staging below branches on it, so the binding it
    # consumes is always set in the same path. source_files records the declared
    # provenance for the index meta.
    if inputs.minimap2_fasta_manifest is not None:
        fasta_paths: list[Path] | None = _read_manifest(inputs.minimap2_fasta_manifest)
        source_files = [str(p) for p in fasta_paths]
    else:
        fasta_paths = None
        if not inputs.fasta_path.exists():
            raise FileNotFoundError(f"fasta_path parquet not found: {inputs.fasta_path}")
        source_files = [str(inputs.fasta_path)]

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

    with open_miint_conn() as conn:
        apply_duckdb_settings(
            conn, duckdb_tmp, memory_gb=_DUCKDB_MEMORY_GB, threads=_DUCKDB_THREADS
        )
        if fasta_paths is not None:
            _stage_subject_from_manifest(conn, fasta_paths)
        else:
            _stage_subject_from_parquet(conn, inputs.fasta_path)
        success = _run_save_minimap2_index(
            conn, _SUBJECT_RELATION, str(index_path), preset=inputs.preset
        )
    if not success:
        raise RuntimeError(
            f"save_minimap2_index reported failure for reference {inputs.reference_idx} "
            f"→ {index_path}"
        )

    params = {"preset": inputs.preset, "source_files": source_files}
    meta_path = workspace / "minimap2_index_meta.json"
    meta_path.write_text(
        json.dumps({"index_type": "minimap2", "fs_path": str(index_path), "params": params})
    )
    return {"minimap2_index_path": index_path, "minimap2_index_meta": meta_path}
