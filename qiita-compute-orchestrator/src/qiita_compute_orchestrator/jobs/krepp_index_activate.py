"""Check a node's copy of a registered krepp build before it is made active.

Runs on whichever compute node SLURM picks. It runs the per-job check
(``krepp_build.verify_build``) on that node's copy under PATH_NODE_LOCAL, then a
known-answer check: one piece of each of up to ``KNOWN_ANSWER_TIPS`` backbone tips is
placed with the copy, and the share whose best edge is the tip's own edge is
reported. A build over another edge numbering than the reference's tree places
almost none on their own edge. The `activate-krepp-build` action that follows
activates the build only when the share reaches ``min_own_edge_share``; with
``activate: false`` in the workflow context the report is the only result, which is
how the share of a correctly numbered build is first measured.
"""

from __future__ import annotations

import os
import socket
from pathlib import Path

from pydantic import BaseModel, Field
from qiita_common.chunking import reassemble_chunks_expr
from qiita_common.krepp import (
    KREPP_ACTIVATION_REPORT_BASENAME,
    KreppActivationReport,
    KreppBuildRecord,
)

from ..config import get_settings
from ..data_plane_client import open_reference_chunk_stream, open_reference_phylogeny_stream
from ..derived_store import KREPP_INDEX_SUBDIR, node_relative_path
from ..krepp_build import linked_krepp_version, verify_build
from ..miint import (
    apply_duckdb_settings,
    duckdb_tmp_dir,
    open_miint_conn,
    resolve_duckdb_memory_gb,
)

# Tips placed, chosen by md5 of their feature_idx so a re-run checks the same tips.
KNOWN_ANSWER_TIPS = 100

# The step's `cpu:` (pinned to it by a test). DuckDB's limit covers the tips' sequences
# only, not the index: a 258 MB index placed under memory_limit=16MB in the same peak
# RSS as with none (372 vs 371 MB, miint a88f3d33, 2026-11-18). 8 GB is a starting
# figure, not a measured one; the rest of the allocation is left to the index.
_DUCKDB_THREADS = 4
_DUCKDB_MEMORY_GB = 8
# Bases placed per tip, from the middle of its sequence (all of a shorter one): about
# the length of an assembled contig, which is what placement is used for.
KNOWN_ANSWER_PIECE_BP = 30_000


class Inputs(BaseModel):
    reference_idx: int
    work_ticket_idx: int
    krepp_build_record: Path
    min_own_edge_share: float = Field(ge=0, le=1)


def _node_copy(record: KreppBuildRecord) -> Path:
    root = get_settings().path_node_local
    if not root:
        raise RuntimeError(
            "PATH_NODE_LOCAL is not set in this job's environment; the orchestrator"
            " forwards it on SLURM (config.node_local_job_env)"
        )
    return node_relative_path(root, record.fs_path)


async def execute(inputs: Inputs, workspace: Path) -> dict[str, Path]:
    record = KreppBuildRecord.model_validate_json(inputs.krepp_build_record.read_text())
    folder = _node_copy(record)
    with duckdb_tmp_dir(workspace) as duckdb_tmp, open_miint_conn() as conn:
        apply_duckdb_settings(
            conn,
            duckdb_tmp,
            memory_gb=resolve_duckdb_memory_gb(
                _DUCKDB_MEMORY_GB, threads=_DUCKDB_THREADS, cap_gb=_DUCKDB_MEMORY_GB
            ),
            threads=_DUCKDB_THREADS,
        )
        verify_build(
            folder, record.sha256sums_sha256, linked_krepp_version=linked_krepp_version(conn)
        )
        async with open_reference_phylogeny_stream(conn, reference_idx=inputs.reference_idx) as rel:
            conn.execute(
                "CREATE TEMP TABLE tip AS"
                f" SELECT feature_idx, edge_id FROM {rel}"
                " WHERE is_tip AND feature_idx IS NOT NULL AND edge_id IS NOT NULL"
                " ORDER BY md5(CAST(feature_idx AS VARCHAR)) LIMIT ?",
                [KNOWN_ANSWER_TIPS],
            )
        features = [r[0] for r in conn.execute("SELECT feature_idx FROM tip").fetchall()]
        if not features:
            raise RuntimeError(f"reference {inputs.reference_idx}'s tree has no numbered tips")
        async with open_reference_chunk_stream(
            conn, reference_idx=inputs.reference_idx, feature_idx=features
        ) as rel:
            conn.execute(
                "CREATE TEMP TABLE query AS"
                f" WITH whole AS (SELECT feature_idx, {reassemble_chunks_expr()} AS seq"
                f"               FROM {rel} GROUP BY feature_idx)"
                " SELECT feature_idx AS read_id,"
                " substr(seq, greatest(1, (length(seq) - ?) // 2 + 1), ?) AS sequence1 FROM whole",
                [KNOWN_ANSWER_PIECE_BP, KNOWN_ANSWER_PIECE_BP],
            )
        index_dir = str(folder / KREPP_INDEX_SUBDIR)
        tips, placed, own = conn.execute(
            "WITH p AS (SELECT fragment, edge_num, like_weight_ratio"
            "           FROM place_krepp(query_table := 'query', index_path := ?)),"
            " best AS (SELECT fragment, edge_num FROM p"
            # Ties on like_weight_ratio go to the lowest edge_num, so the
            # tip's best edge is the same on every run.
            "          QUALIFY row_number() OVER (PARTITION BY fragment"
            "                  ORDER BY like_weight_ratio DESC, edge_num) = 1)"
            " SELECT (SELECT count(*) FROM query), count(*),"
            "        count(*) FILTER (WHERE best.edge_num = tip.edge_id)"
            " FROM best JOIN tip ON tip.feature_idx = best.fragment",
            [index_dir],
        ).fetchone()
    share = own / tips if tips else 0.0
    report = KreppActivationReport(
        reference_index_idx=record.reference_index_idx,
        node=os.environ.get("SLURMD_NODENAME") or socket.gethostname(),
        tips=tips,
        placed=placed,
        own_edge=own,
        own_edge_share=share,
        min_own_edge_share=inputs.min_own_edge_share,
        passed=share >= inputs.min_own_edge_share,
    )
    print(f"krepp_index_activate: {report.model_dump_json()}", flush=True)
    out = workspace / KREPP_ACTIVATION_REPORT_BASENAME
    out.write_text(report.model_dump_json())
    return {"krepp_activation_report": out}
