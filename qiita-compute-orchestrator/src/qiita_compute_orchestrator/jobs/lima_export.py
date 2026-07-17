"""Native job: stage a sample's raw reads as a CCS uBAM for the `lima` container.

`read.parquet -> lima_in.bam + lima_zmw_map.parquet` + `lima_config.json`. First
entry of the long-read adapter chain (`lima_export -> lima -> lima_mask`), which
runs BEFORE `qc` so the Twist adaptor is stripped before QC's length/quality
filter sees the insert.

**Why a BAM and not a FASTQ.** lima decides CCS-vs-CLR from the input FORMAT, not
from `--hifi-preset`: handed a FASTQ it declares the reads non-CCS ("CLR
demultiplexing is only supported with BAM/XML input") and demultiplexes each
sequence individually. That path does not merely run slow — it does not finish.
Probed at lima 2.13.0 on the vendored Twist adapter set: the FASTQ run produced
zero bytes and had to be killed at a timeout, while the BYTE-IDENTICAL reads as a
CCS BAM completed in ~2 s; dropping preset flags did not change it. So there is
nothing to parallelize or scale here. The lake stores reads as plain sequences —
the instrument's CCS BAM and its ZMW tags are long gone — so the BAM lima needs is
synthesized here from `sequence1` / `qual1`, which is sufficient: lima needs an
`@RG` carrying `DS:READTYPE=CCS` and nothing about the original instrument BAM.

**Why pysam and not miint.** miint's `COPY … TO (FORMAT BAM)` is an ALIGNMENT
writer, not a reads writer: it never emits SEQ/QUAL (every record lands `… * *`,
confirmed against a mapped-record control), it requires a non-empty
`REFERENCE_LENGTHS` @SQ header a uBAM does not have, and it exposes no read-group
option — so it cannot express `@RG DS:READTYPE=CCS`. This is the one place in the
repo that writes sequences without a miint writer; it is not a miint-first
oversight. See `docs/duckdb-miint.md`.

**The ZMW is a dense counter, NOT the `sequence_idx`.** The record name must be
`<movie>/<zmw>/ccs` — a bare integer name sends lima back into the hang. lima
rewrites the name of every read it emits from the per-read `zm` tag (with no `zm`
the name returns as `<movie>/?/ccs`, destroying the key), and `zm` is an int32 BAM
tag while `sequence_idx` is a lake-wide-unique BIGINT. An out-of-range
`sequence_idx` does not error — it comes back TRUNCATED (5000000000 -> 705032704),
i.e. a mask silently attributed to the wrong read. So the ZMW is a per-file
counter that cannot overflow, and `lima_zmw_map.parquet` carries `zmw ->
sequence_idx` for `lima_mask` to join back. The counter is assigned as the rows
stream past, so no ORDER BY / window function is needed: a blocking sort over
`sequence1` + `qual1` (~20 kB x millions of HiFi rows) is exactly what this job
must not do.

**`lima_config.json` carries the argument string.** A scalar cannot ride a
container step's `inputs` — the runner treats every container input as a
bind-mount path and rejects a non-absolute one as CONTRACT_VIOLATION — so the
control-plane-resolved `lima_args` is written to a file the container reads. Same
trick `long-read-assembly`'s `assembly_run_config` uses for its `assembler`.
"""

from __future__ import annotations

import array
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pysam
from pydantic import BaseModel
from qiita_common.models import ReadMaskReason
from qiita_common.parquet import validate_parquet_path

from ..miint import (
    apply_duckdb_settings,
    duckdb_tmp_dir,
    open_miint_conn,
    resolve_duckdb_memory_gb,
)
from ._partial_mask import assert_single_end

YAML_STEP_NAME = "lima_export"

# Off-SLURM fallback cap; under SLURM the real cap is sized to the cgroup. This step
# STREAMS a projection of the reads parquet straight out — no sort, no accumulator
# — so its peak footprint is flat in READ COUNT: it is bounded by one batch, not by
# the sample.
_DUCKDB_MEMORY_GB = 8
_DUCKDB_THREADS = 4

# Carved out of the cgroup for the BAM writer, which — unlike the `COPY ... FORMAT
# FASTQ` this step used to be — is an in-process co-consumer DuckDB's `memory_limit`
# does not account for: one batch of Arrow reads, their per-read phred arrays, and
# pysam/htslib's own buffers all live outside it (the sibling pattern — see syndna's
# `_MM2_RESERVE_GB`).
#
# BE EXPLICIT ABOUT THE ARITHMETIC, because this SHRINKS DuckDB rather than growing
# the step: `resolve_duckdb_memory_gb` computes `alloc - headroom(threads) - reserve`,
# so at the YAML's unchanged `mem_gb: 8` DuckDB goes 8 - 4 - 2 = **2 GB**, down from
# 4 GB before this reserve existed. That is deliberate and measured, not incidental:
# peak RSS for the WHOLE step is ~0.6 GB on 15 kB HiFi reads, and the query is a
# streaming scan whose only blocking build is the small `partial_mask` hash side, so
# 2 GB is ample and a bigger SLURM slot would just idle. A `plan()` cannot express
# this — plan composition is down-only, so it can lower the allocation but never
# raise it. If a future read length ever does need more, raise `mem_gb` in the
# workflow YAML; do not quietly drop the reserve.
_WRITER_RESERVE_GB = 2

# Synthetic movie name. lima requires the PacBio `<movie>/<zmw>/ccs` record-name
# convention (a bare integer name hangs), but nothing downstream reads the movie
# field — `lima_mask` keys on the ZMW alone. It is a fixed, obviously-synthetic
# constant rather than a per-run value precisely so it cannot be mistaken for the
# instrument's real movie name: these reads come from the lake, not from a movie.
_MOVIE = "qiita_synthetic_ccs"

# Read-group ID for the synthesized @RG, carried per-read as an `RG` tag (pbbam
# errors out on a record without one: "tag RG was requested but is missing").
# `DS:READTYPE=CCS` is the load-bearing FIELD: probed, an @RG whose DS says
# READTYPE=UNKNOWN is accepted but demoted ("Unknown read type ... will generate
# use SubreadSets"). `PL`/`PU`/`SM` follow PacBio convention and were not varied
# independently — do not read them as established requirements.
_READ_GROUP_ID = "qiita"

# Rows per streamed batch — the step's memory peak, since nothing accumulates
# across batches. A batch holds whole HiFi reads (~15-20 kB of bases plus one phred
# byte per base), so 4096 of them is a few hundred MB of Arrow + Python: measured
# ~0.6 GB peak RSS for the whole step at 15 kB reads, inside `_WRITER_RESERVE_GB`.
# Lower this before raising the step's `mem_gb` if a longer-read sample ever pushes
# it — the batch size is the knob, the sample size is not.
_BATCH_ROWS = 4096

_INCOMING = "lima_export_incoming"

# `lima_zmw_map.parquet`'s schema. The ZMW rides in an int32 `zm` BAM tag, so
# `_MAX_ZMW` — not this type's ceiling — is the real bound; UINT32 is simply the
# narrowest Arrow type that holds it. `sequence_idx` stays INT64, the lake-wide key
# it maps back to.
_ZMW_MAP_SCHEMA = pa.schema([("zmw", pa.uint32()), ("sequence_idx", pa.int64())])

# The BAM `zm` tag is int32. The dense counter cannot realistically reach this (it
# would need >2^31 reads in ONE sample), but the whole point of the counter is that
# an over-range ZMW corrupts silently rather than failing — so it is asserted, not
# assumed.
_MAX_ZMW = 2**31 - 1


def _bam_header() -> dict:
    """The minimal CCS-uBAM header. `DS:READTYPE=CCS` is the field lima keys on
    (see `_READ_GROUP_ID`). The instrument metadata a real CCS header carries
    (BINDINGKIT, SEQUENCINGKIT, BASECALLERVERSION, FRAMERATEHZ) is deliberately
    absent: probed, lima does not need it, and we do not have it — these reads come
    from the lake, not from a movie. Per-read `np`/`rq` are likewise not needed."""
    return {
        "HD": {"VN": "1.6", "SO": "unknown", "pb": "5.0.0"},
        "RG": [
            {
                "ID": _READ_GROUP_ID,
                "PL": "PACBIO",
                "PU": _MOVIE,
                "SM": _MOVIE,
                "DS": "READTYPE=CCS",
            }
        ],
    }


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


def _phred_arrays(qual_column: pa.Array) -> list[array.array]:
    """One `array.array('B')` of phred scores per read, read straight off Arrow's
    values buffer.

    pysam takes the phred ints directly, so there is no encode step here — and the
    obvious `qual_column.to_pylist()` must be avoided: it turns every base's score
    into a Python object, which on HiFi reads (~15-20 kB each) is both the step's
    memory peak and the bulk of its runtime. Measured on 15 kB reads it is ~9x the
    cost of this slicing, which is the difference between this step fitting its PT2H
    walltime on a real Revio sample and not (the FASTQ path it replaces was a pure
    DuckDB COPY, i.e. all of this ran in C++). `array.array` copies from the
    memoryview at C speed, and only the one read's worth.

    Slice-safe: `flatten()` / `value_lengths()` are taken from the column itself, so
    a batch DuckDB hands us as a slice of a larger buffer walks the right bytes.
    """
    if qual_column.null_count:
        # The running offset below assumes one contiguous run of scores per read; a
        # NULL list would silently shift every subsequent read's qualities.
        raise ValueError("qual1 contains NULL; every exported read must carry qualities")
    flat = qual_column.flatten()
    buf = memoryview(flat.buffers()[1])[flat.offset : flat.offset + len(flat)]
    out: list[array.array] = []
    pos = 0
    for length in qual_column.value_lengths().to_pylist():
        out.append(array.array("B", buf[pos : pos + length]))
        pos += length
    return out


def _write_bam_and_map(conn, source: str, lima_in_bam: Path, zmw_map: Path) -> None:
    """Stream `source`'s reads into the CCS uBAM, emitting the `zmw -> sequence_idx`
    map alongside.

    Both files are written from ONE pass, batch by batch: the ZMW is the row's
    position in that stream, so the map is just the batch's `sequence_idx` column
    against a counter range — no sort, no window function, and nothing accumulated
    across batches.
    """
    reader = conn.execute(f"SELECT sequence_idx, sequence1, qual1 FROM {source}").to_arrow_reader(
        _BATCH_ROWS
    )

    zmw = 0
    with (
        pysam.AlignmentFile(str(lima_in_bam), "wb", header=_bam_header()) as bam,
        pq.ParquetWriter(zmw_map, _ZMW_MAP_SCHEMA, compression="zstd") as map_writer,
    ):
        for batch in reader:
            sequences = batch.column("sequence1").to_pylist()
            quals = _phred_arrays(batch.column("qual1"))
            n = len(sequences)
            if zmw + n - 1 > _MAX_ZMW:
                raise ValueError(
                    f"read count exceeds the {_MAX_ZMW} ZMWs addressable by the BAM "
                    "`zm` tag; lima would truncate it and mask the wrong reads"
                )
            for i in range(n):
                record = pysam.AlignedSegment()
                # `<movie>/<zmw>/ccs`: lima hangs on a bare-integer name, and
                # rewrites this name from the `zm` tag below on the way out.
                record.query_name = f"{_MOVIE}/{zmw + i}/ccs"
                record.flag = 4  # unmapped: these reads were never aligned
                # ORDER MATTERS: pysam resets query_qualities when query_sequence
                # is assigned, so the qualities must be set after the bases.
                record.query_sequence = sequences[i]
                record.query_qualities = quals[i]
                record.set_tag("RG", _READ_GROUP_ID, "Z")
                # The one tag lima reads back out: it rebuilds the emitted record's
                # name from `zm`. `np`/`rq` are not required (probed).
                record.set_tag("zm", zmw + i, "i")
                bam.write(record)
            map_writer.write_batch(
                pa.record_batch(
                    [
                        pa.array(range(zmw, zmw + n), type=pa.uint32()),
                        batch.column("sequence_idx").cast(pa.int64()),
                    ],
                    schema=_ZMW_MAP_SCHEMA,
                )
            )
            zmw += n


async def execute(inputs: Inputs, workspace: Path) -> dict[str, Path]:
    if not inputs.reads.exists():
        raise FileNotFoundError(f"reads parquet not found: {inputs.reads}")
    if not inputs.lima_args.strip():
        raise ValueError("lima_args is empty; the control plane must resolve it from lima_preset")
    if inputs.partial_mask is not None and not inputs.partial_mask.exists():
        raise FileNotFoundError(f"partial_mask not found: {inputs.partial_mask}")

    workspace.mkdir(parents=True, exist_ok=True)
    lima_in_bam = workspace / "lima_in.bam"
    zmw_map = workspace / "lima_zmw_map.parquet"
    lima_config = workspace / "lima_config.json"

    reads_sql = validate_parquet_path(inputs.reads)

    success = False
    try:
        with duckdb_tmp_dir(workspace) as duckdb_tmp, open_miint_conn() as conn:
            apply_duckdb_settings(
                conn,
                duckdb_tmp,
                memory_gb=resolve_duckdb_memory_gb(
                    _DUCKDB_MEMORY_GB, threads=_DUCKDB_THREADS, reserve_gb=_WRITER_RESERVE_GB
                ),
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
                conn.execute(
                    f"CREATE VIEW lima_export_pass AS "
                    f"SELECT r.* FROM read_parquet('{reads_sql}') r JOIN {_INCOMING} m "
                    f"USING (sequence_idx) WHERE m.reason = '{ReadMaskReason.PASS.value}'"
                )
                source = "lima_export_pass"
            _write_bam_and_map(conn, source, lima_in_bam, zmw_map)
        lima_config.write_text(json.dumps({"args": inputs.lima_args}) + "\n")
        success = True
    finally:
        # On failure remove partial outputs so the SLURM launcher's manifest walker
        # (which runs after execute()) cannot promote them as the result.
        if not success:
            lima_in_bam.unlink(missing_ok=True)
            zmw_map.unlink(missing_ok=True)
            lima_config.unlink(missing_ok=True)

    return {
        "lima_in_bam": lima_in_bam,
        "lima_zmw_map": zmw_map,
        "lima_config": lima_config,
    }
