"""Native job: estimate a metagenomic OGU feature table from alignment data.

Compute-on-demand, never persisted: given one alignment run and an explicit
`prep_sample_idx` cohort (both carried on the work ticket's `action_context` and
resolved CP-side into the alignment DoGet ticket), build a genome-keyed OGU
feature table via duckdb-miint `woltka_ogu`, filtered to genomes meeting a
breadth-of-coverage threshold POOLED over the whole cohort.

**The analytic itself — its SQL, its relation names, and every rule that makes it
correct — is `qiita_common.ogu_table`**, shared with the client-side feature-table
recipe so the two cannot disagree about it. This module is the server-side half:
where the three inputs come from, and where the result goes.

Three inputs, three sources:

* the **alignment slice** streams from the data plane over Arrow Flight
  (`open_alignment_stream`, minted by `work_ticket_idx`) — no disk;
* the **per-feature lengths** stream from the data plane's `reference_sequences`
  (`open_reference_sequences_stream`, whole-reference so unaligned contigs are in
  the coverage denominator) — no disk;
* the **feature -> genome map** is the one Postgres-only input, staged as a small
  workspace Parquet by the CP runner resolver (`runner/_feature_table.py`) and
  read here via `read_parquet`.

Each stream is drained inside its own `with`, by the CREATE that stages it, so the
Flight client closes before the compute starts.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from pydantic import BaseModel, Field
from qiita_common import ogu_table

# The projection this job asks the data plane for. Owned by the shared analytic,
# alongside the SQL that binds it, so the requested columns and the bound SELECT
# stay one list.
from qiita_common.ogu_table import ALIGNMENT_COLUMNS as _ALIGNMENT_COLUMNS
from qiita_common.parquet import validate_parquet_path

from ..data_plane_client import open_alignment_stream, open_reference_sequences_stream
from ..miint import (
    PARQUET_OPTS,
    apply_duckdb_settings,
    duckdb_tmp_dir,
    open_miint_conn,
    resolve_duckdb_memory_gb,
)

if TYPE_CHECKING:
    import duckdb

YAML_STEP_NAME = "estimate_feature_table"

# The single step output: the coverage-filtered OGU feature table. Emitted as a
# Parquet under the per-attempt workspace (it is NOT registered into DuckLake —
# the table is computed on demand, never persisted).
OGU_TABLE_OUTPUT_KEY = "ogu_table"
OGU_TABLE_FILENAME = "ogu_table.parquet"

# DuckDB resource caps. This job's heavy work is entirely in-DuckDB (the coverage
# interval merge + the woltka aggregation over the streamed alignment slice) — no
# in-process co-consumer, so no `reserve_gb`. `_DUCKDB_MEMORY_GB` is the OFF-SLURM
# fallback (local backend / tests); under SLURM the limit tracks the real cgroup
# via `resolve_duckdb_memory_gb()`, so a `--mem-gb` override reaches DuckDB. The
# cohort/alignment size is not known at submit time, so there is deliberately no
# `plan()` — the workflow YAML baseline governs, and OOM escalation backstops an
# under-estimate.
_DUCKDB_MEMORY_GB = 8
_DUCKDB_THREADS = 4


class Inputs(BaseModel):
    """Typed input contract for estimate_feature_table.

    `reference_idx` and `work_ticket_idx` are framework-injected scope scalars (a
    REFERENCE-scoped ticket). `coverage_threshold` rides the workflow `params:`
    (a proportion in [0, 1] — e.g. 0.01 for 1% breadth). `genome_map_path` is the
    resolver-staged `(feature_idx, genome_idx)` Parquet. There is deliberately no
    `alignment_idx`: the alignment DoGet ticket is minted by `work_ticket_idx`, and
    the CP route derives `alignment_idx` + the cohort from the ticket's
    `action_context`.
    """

    reference_idx: int
    work_ticket_idx: int
    coverage_threshold: float = Field(ge=0.0, le=1.0)
    genome_map_path: Path


def _write_ogu_table(
    conn: duckdb.DuckDBPyConnection,
    *,
    coverage_threshold: float,
    out_path: Path,
) -> None:
    """Run `qiita_common.ogu_table`'s analytic over the already-staged working tables
    and COPY the result to `out_path` as Parquet (v2 + zstd). That module documents
    every rule the SQL encodes; what this function decides is the two things a
    caller must:

    * **at `coverage_threshold == 0` the coverage calc is skipped entirely** — every
      genome with any alignment trivially qualifies, so there is no survivor set to
      build or join (and `execute` does not even stream the lengths);
    * **an empty `ogu_input` short-circuits** to a valid 0-row Parquet, because
      `woltka_ogu` rejects an all-NULL `sample_id` source and an empty result is a
      legitimate compute-on-demand answer, not a failure.
    """
    out_sql = validate_parquet_path(out_path)

    filtered = coverage_threshold > 0.0
    if filtered:
        conn.execute(ogu_table.coverage_alignments_view_sql())
        conn.execute(ogu_table.pooled_survivor_table_sql(), [coverage_threshold])
    conn.execute(ogu_table.ogu_input_table_sql(filter_to_survivors=filtered))

    empty = conn.execute(f"SELECT count(*) FROM {ogu_table.OGU_INPUT_TABLE}").fetchone()[0] == 0
    select_sql = ogu_table.empty_ogu_select_sql() if empty else ogu_table.woltka_ogu_select_sql()
    conn.execute(f"COPY ({select_sql}) TO '{out_sql}' ({PARQUET_OPTS})")


async def execute(inputs: Inputs, workspace: Path) -> dict[str, Path]:
    workspace.mkdir(parents=True, exist_ok=True)
    out_path = workspace / OGU_TABLE_FILENAME

    success = False
    try:
        with duckdb_tmp_dir(workspace) as duckdb_tmp, open_miint_conn() as conn:
            apply_duckdb_settings(
                conn,
                duckdb_tmp,
                memory_gb=resolve_duckdb_memory_gb(_DUCKDB_MEMORY_GB, threads=_DUCKDB_THREADS),
                threads=_DUCKDB_THREADS,
            )

            # The feature -> genome map: the one Postgres-only input, read from the
            # resolver-staged Parquet. Inner-consistent BIGINT ids (int64 Parquet).
            map_sql = validate_parquet_path(inputs.genome_map_path)
            conn.execute(ogu_table.map_table_sql(f"read_parquet('{map_sql}')"))

            # The lengths feed ONLY the coverage calc, so at coverage_threshold == 0
            # (where the calc is skipped) the stream is skipped too — the point is to
            # avoid the coverage calculation entirely, not just its filter.
            if inputs.coverage_threshold > 0.0:
                async with open_reference_sequences_stream(
                    conn, reference_idx=inputs.reference_idx
                ) as lengths_rel:
                    conn.execute(ogu_table.genome_lengths_table_sql(lengths_rel))

            # The alignment slice, all cohort samples pooled.
            async with open_alignment_stream(
                conn, work_ticket_idx=inputs.work_ticket_idx, columns=_ALIGNMENT_COLUMNS
            ) as alignment_rel:
                conn.execute(ogu_table.alignment_table_sql(alignment_rel))

            _write_ogu_table(conn, coverage_threshold=inputs.coverage_threshold, out_path=out_path)
        success = True
    finally:
        # On failure remove a partial COPY output so the SLURM launcher's manifest
        # walker (which runs after execute()) can't promote it as the result.
        if not success:
            out_path.unlink(missing_ok=True)

    return {OGU_TABLE_OUTPUT_KEY: out_path}
