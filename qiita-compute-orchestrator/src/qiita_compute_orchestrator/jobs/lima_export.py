"""Native job: stage a sample's raw reads as FASTQ for the `lima` container.

`read.parquet -> lima_in.fastq` + `lima_config.json`. First entry of the long-read
adapter chain (`lima_export -> lima -> lima_mask`), which runs BEFORE `qc` so the
Twist adaptor is stripped before QC's length/quality filter sees the insert.

**Why the reads are re-serialized rather than streamed.** lima is a container
binary; it reads files. The runner's `_resolve_staged_masked_reads` cannot serve
here — it streams the `read_masked` view, which requires an already-COMPLETED mask,
and this chain is what builds that mask. (The raw `read` table is not
Flight-reachable at all.) So the already-bound `reads` parquet is written out as
FASTQ. Per CLAUDE.local.md this on-disk hand-off to a container is called out, not
silently entrenched: both hops stay inside one ticket's workspace and nothing
crosses a workflow boundary as a filepath.

**The FASTQ record name is `sequence_idx`, not `read_id`.** miint's `infer_trim`
joins the original and trimmed relations on `sequence_index`, and `read_fastx`
assigns its own `sequence_index` POSITIONALLY, resetting per file — so the key
cannot be recovered by re-parsing lima's output. Carrying `sequence_idx` as the
record name is what makes the round-trip work: lima preserves the name verbatim
and appends its BAM tags after a single space, which `read_fastx` parses into a
separate `comment` column.

**`lima_config.json` carries the argument string.** A scalar cannot ride a
container step's `inputs` — the runner treats every container input as a
bind-mount path and rejects a non-absolute one as CONTRACT_VIOLATION — so the
control-plane-resolved `lima_args` is written to a file the container reads. Same
trick `long-read-assembly`'s `assembly_run_config` uses for its `assembler`.
"""

from __future__ import annotations

import json
from pathlib import Path

from pydantic import BaseModel
from qiita_common.models import ReadMaskReason
from qiita_common.parquet import validate_parquet_path

from ..miint import (
    apply_duckdb_settings,
    duckdb_tmp_dir,
    open_miint_conn,
    resolve_duckdb_memory_gb,
)
from ._partial_mask import assert_covers_reads, assert_single_end

YAML_STEP_NAME = "lima_export"

# Off-SLURM fallback cap; under SLURM the real cap is sized to the cgroup. This
# step streams a projection of the reads parquet straight to FASTQ, so its peak
# footprint is ~flat in read count.
_DUCKDB_MEMORY_GB = 8
_DUCKDB_THREADS = 4


class Inputs(BaseModel):
    """Typed input contract for lima_export.

    `reads` is the raw `read.parquet` (binding `reads`):
    `(prep_sample_idx, sequence_idx, read_id, sequence1, qual1, sequence2, qual2)`.
    `lima_args` is the control-plane-resolved lima argument string — the CP maps
    the client's `lima_preset` to it, so it is never client-supplied. Long reads
    are single-end; a paired-end read set here is a contract error.
    """

    reads: Path
    lima_args: str
    # OPTIONAL upstream partial mask (today: syndna's). When bound, only its
    # still-`pass` reads are exported to lima — the spike-ins it already marked
    # never reach lima, so lima cannot mis-drop them as `twist_no_adaptor`. Unbound
    # -> every raw read is exported (lima runs first).
    partial_mask: Path | None = None
    prep_sample_idx: int | None = None
    work_ticket_idx: int


_INCOMING = "lima_export_incoming"


async def execute(inputs: Inputs, workspace: Path) -> dict[str, Path]:
    if not inputs.reads.exists():
        raise FileNotFoundError(f"reads parquet not found: {inputs.reads}")
    if not inputs.lima_args.strip():
        raise ValueError("lima_args is empty; the control plane must resolve it from lima_preset")
    if inputs.partial_mask is not None and not inputs.partial_mask.exists():
        raise FileNotFoundError(f"partial_mask not found: {inputs.partial_mask}")

    workspace.mkdir(parents=True, exist_ok=True)
    lima_in_fastq = workspace / "lima_in.fastq"
    lima_config = workspace / "lima_config.json"

    reads_sql = validate_parquet_path(inputs.reads)
    out_sql = validate_parquet_path(lima_in_fastq)

    success = False
    try:
        with duckdb_tmp_dir(workspace) as duckdb_tmp, open_miint_conn() as conn:
            apply_duckdb_settings(
                conn,
                duckdb_tmp,
                memory_gb=resolve_duckdb_memory_gb(_DUCKDB_MEMORY_GB, threads=_DUCKDB_THREADS),
                threads=_DUCKDB_THREADS,
            )
            assert_single_end(conn, reads_sql, "reads", inputs.reads)
            # The source of reads to export: all of them, or — when an upstream
            # mask is bound — only its still-`pass` reads (spike-ins excluded).
            if inputs.partial_mask is None:
                source = f"read_parquet('{reads_sql}')"
            else:
                mask_sql = validate_parquet_path(inputs.partial_mask)
                conn.execute(f"CREATE VIEW {_INCOMING} AS SELECT * FROM read_parquet('{mask_sql}')")
                assert_covers_reads(conn, reads_sql, _INCOMING, "partial_mask", inputs.partial_mask)
                conn.execute(
                    f"CREATE VIEW lima_export_pass AS "
                    f"SELECT r.* FROM read_parquet('{reads_sql}') r JOIN {_INCOMING} m "
                    f"USING (sequence_idx) WHERE m.reason = '{ReadMaskReason.PASS.value}'"
                )
                source = "lima_export_pass"
            # `sequence_idx` becomes the FASTQ record name. The CAST is REQUIRED:
            # miint's FASTQ writer takes the record name from a VARCHAR `read_id`
            # column and raises an INTERNAL error (invalidating the connection) on
            # a BIGINT. ORDER BY keeps the output deterministic.
            conn.execute(
                "COPY (SELECT CAST(sequence_idx AS VARCHAR) AS read_id, sequence1, qual1 "
                f"      FROM {source} ORDER BY sequence_idx) "
                f"TO '{out_sql}' (FORMAT FASTQ)"
            )
        lima_config.write_text(json.dumps({"args": inputs.lima_args}) + "\n")
        success = True
    finally:
        # On failure remove partial outputs so the SLURM launcher's manifest walker
        # (which runs after execute()) cannot promote them as the result.
        if not success:
            lima_in_fastq.unlink(missing_ok=True)
            lima_config.unlink(missing_ok=True)

    return {"lima_in_fastq": lima_in_fastq, "lima_config": lima_config}
