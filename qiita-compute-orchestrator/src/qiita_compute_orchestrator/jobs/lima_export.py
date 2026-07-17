"""Native job: stage a sample's raw reads as a CCS uBAM for the `lima` container.

`read.parquet -> lima_in.bam` + `lima_config.json`. First entry of the long-read
adapter chain (`lima_export -> lima -> lima_mask`), which runs BEFORE `qc` so the
Twist adaptor is stripped before QC's length/quality filter sees the insert.

**Why a BAM and not a FASTQ.** lima decides CCS-vs-CLR from the input FORMAT, not
from `--hifi-preset`: handed a FASTQ it declares the reads non-CCS ("CLR
demultiplexing is only supported with BAM/XML input") and demultiplexes each
sequence individually. That path does not merely run slow — it does not finish.
Probed at lima 2.13.0 on the vendored Twist adapter set: the FASTQ run produced
zero bytes and had to be killed at a timeout, while the BYTE-IDENTICAL reads as a
CCS BAM completed in ~2 s; dropping preset flags did not change it. So there is
nothing to parallelize or scale here. The lake stores reads as plain sequences —
the instrument's CCS BAM is not retained — so the BAM lima needs is rebuilt here
from `read_id` / `sequence1` / `qual1`. That is sufficient: lima needs an `@RG`
carrying `DS:READTYPE=CCS` and nothing else about the original BAM. (Probed: a BAM
carrying the full real CCS tag set and one carrying only `@RG` + `zm` produce
byte-identical clipped output — same reads kept, same dropped, same clip
positions. The `np`/`rq` the lake discards do not change lima's answer.)

**The key is `read_id`, which is already PacBio's `<movie>/<zmw>/ccs`.** Nothing
is invented here. `bam_to_parquet` keeps the BAM's QNAME verbatim, so the lake
still holds the instrument's own name; this job writes it back as the record name,
sets `zm` to the hole number parsed out of it, and points the `@RG`'s `PU` at the
movie parsed out of it. lima then reconstructs the emitted name from `zm` + the
read group — byte-identically to `read_id` (probed on real production names) — so
`lima_mask` joins its output straight back on `read_id`.

Two properties come free from that, and both are why the key is `read_id` and NOT
`sequence_idx`: the hole number is int32 **by provenance** (it came out of a real
PacBio BAM, where `zm` is int32) whereas a lake-wide `sequence_idx` would silently
TRUNCATE in that tag (5000000000 -> 705032704, a mask attributed to the wrong
read); and `read_id` is unique **by an invariant already enforced upstream** —
`bam_to_parquet` rejects any input whose `read_id` is not unique.

**`lima_config.json` carries the argument string.** A scalar cannot ride a
container step's `inputs` — the runner treats every container input as a
bind-mount path and rejects a non-absolute one as CONTRACT_VIOLATION — so the
control-plane-resolved `lima_args` is written to a file the container reads. Same
trick `long-read-assembly`'s `assembly_run_config` uses for its `assembler`.
"""

from __future__ import annotations

import json
from pathlib import Path

import duckdb
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

# Off-SLURM fallback cap; under SLURM the real cap is sized to the cgroup. The
# write is a miint COPY — no per-row Python, no in-process co-consumer — so this
# step is a streaming scan and its footprint is flat in read count.
_DUCKDB_MEMORY_GB = 8
_DUCKDB_THREADS = 4

# Read-group ID for the rebuilt @RG, carried per-read as an `RG` tag (pbbam errors
# out on a record without one: "tag RG was requested but is missing").
# `DS:READTYPE=CCS` is the load-bearing FIELD: probed, an @RG whose DS says
# READTYPE=UNKNOWN is accepted but demoted ("Unknown read type ... will generate
# use SubreadSets"). `PL` follows PacBio convention and was not varied
# independently — do not read it as an established requirement. `PU`/`SM` carry the
# read set's real movie, parsed from `read_id`: lima names each emitted record from
# `zm` + its read group, so `PU` is what makes the output name match `read_id`.
_READ_GROUP_ID = "qiita"

_INCOMING = "lima_export_incoming"

# PacBio CCS read names are `<movie>/<zmw>/ccs`, which is exactly what the lake's
# `read_id` holds for a BAM-ingested sample. Both halves are needed: the movie for
# the @RG, the hole number for the `zm` tag lima rebuilds the name from.
_MOVIE_FROM_READ_ID = "split_part(read_id, '/', 1)"
_ZMW_FROM_READ_ID = "TRY_CAST(split_part(read_id, '/', 2) AS BIGINT)"

# The BAM `zm` tag is int32. A hole number from a real PacBio BAM cannot exceed it
# (it was an int32 there), so this should never fire — but an over-range value would
# be TRUNCATED into a valid-looking ZMW rather than rejected, and the mask would
# land on the wrong read. Asserted, not assumed.
_MAX_ZMW = 2**31 - 1

# THE ONE PLACE THE uBAM WRITER'S CALL SHAPE LIVES.
#
# miint's `COPY ... TO (FORMAT UBAM)` — requested in duckdb-miint#156 and pinned
# from the qiita side by `tests/jobs/test_sam_bam_writer_miint_contract.py`, which
# documents why the pre-existing `FORMAT SAM|BAM` cannot serve (it is an ALIGNMENT
# writer: it never emits SEQ/QUAL, demands a non-empty REFERENCE_LENGTHS @SQ header
# a uBAM has none of, and exposes no read-group option).
#
# `read_id` is the record name (the same column `FORMAT FASTQ` names records from);
# `zmw` is an ordinary column of this projection, which `TAGS` binds to the `zm`
# tag — it is not a lake column and miint is not reaching into a schema.
#
# Kept as one formattable statement so that if the shipped option syntax differs
# from the requested shape, the fix is this string and nothing else.
_UBAM_COPY_SQL = """
COPY (
    SELECT read_id,
           sequence1,
           qual1,
           {zmw_expr} AS zmw
    FROM {source}
) TO '{out}' (
    FORMAT UBAM,
    READ_GROUP {{ID: '{rg}', PL: 'PACBIO', PU: '{movie}', SM: '{movie}', DS: 'READTYPE=CCS'}},
    TAGS {{zm: zmw}}
)
"""


def miint_supports_ubam(conn: duckdb.DuckDBPyConnection, probe_path: Path) -> bool:
    """Whether the loaded miint build can write an unaligned reads BAM.

    False on every build up to and including the one this job was written against
    — the writer is requested in duckdb-miint#156. Exists so the test suite can
    skip the writer-dependent cases with a legible reason instead of failing
    opaquely, and so `execute` can name the missing capability rather than surface
    a bare BinderException. Delete it (and this branch) once the floor build ships
    the format.
    """
    # Exercise the REAL statement against one throwaway read, so the probe answers
    # "can this build run the call this job makes" rather than an approximation.
    conn.execute(
        "CREATE OR REPLACE TEMP TABLE lima_ubam_probe AS "
        "SELECT 'm/1/ccs' AS read_id, 'ACGT' AS sequence1, "
        "       [40,40,40,40]::UTINYINT[] AS qual1"
    )
    try:
        conn.execute(
            _UBAM_COPY_SQL.format(
                zmw_expr=_ZMW_FROM_READ_ID,
                source="lima_ubam_probe",
                out=probe_path,
                rg=_READ_GROUP_ID,
                movie="m",
            )
        )
        return True
    except duckdb.Error:
        return False


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


def _resolve_movie(conn: duckdb.DuckDBPyConnection, source: str) -> str:
    """The read set's movie, plus the three checks that make `read_id` a safe key.

    One scan answers all of it, because each failure is silent in a different way:

    - **A `read_id` that is not `<movie>/<zmw>/ccs`.** A FASTQ-ingested sample
      carries whatever the FASTQ said. lima would then be handed a bare-ish name
      and HANG (probed), so this is rejected here where the cause is legible.
    - **More than one movie.** A single `@RG` stamps ONE `PU` on every read, and
      lima names each record from `zm` + its read group — so a second movie's reads
      would come back under the first movie's name: a wrong-but-plausible `read_id`
      that mis-joins or vanishes. The lake is single-movie per prep_sample today
      (verified on real data); this fails loud rather than trusting that forever.
      Supporting it would need one `@RG` per movie plus a per-read `RG` column.
    - **A hole number over int32.** Cannot happen for a name that came out of a real
      PacBio BAM, but an over-range `zm` truncates into a valid-looking ZMW rather
      than erroring, so it is checked rather than trusted.
    """
    movies, bad_names, max_zmw = conn.execute(
        f"SELECT count(DISTINCT {_MOVIE_FROM_READ_ID}), "
        f"       count(*) FILTER (WHERE {_ZMW_FROM_READ_ID} IS NULL "
        f"                          OR split_part(read_id, '/', 3) <> 'ccs'), "
        f"       max({_ZMW_FROM_READ_ID}) "
        f"FROM {source}"
    ).fetchone()
    if bad_names:
        raise ValueError(
            f"{bad_names} read(s) have a read_id that is not PacBio's "
            "'<movie>/<zmw>/ccs'; lima needs that name shape and hangs without it. "
            "The lima chain expects a BAM-ingested CCS read set."
        )
    if movies and movies > 1:
        raise ValueError(
            f"reads span {movies} movies; a single @RG would stamp one movie on every "
            "record and lima would rebuild the wrong read_id for the rest. Supporting "
            "this needs one @RG per movie plus a per-read RG tag."
        )
    if max_zmw is not None and max_zmw > _MAX_ZMW:
        raise ValueError(
            f"read_id carries a ZMW ({max_zmw}) over the {_MAX_ZMW} addressable by the "
            "BAM `zm` tag; lima would truncate it and mask the wrong read"
        )
    (movie,) = conn.execute(f"SELECT {_MOVIE_FROM_READ_ID} FROM {source} LIMIT 1").fetchone()
    return movie


async def execute(inputs: Inputs, workspace: Path) -> dict[str, Path]:
    if not inputs.reads.exists():
        raise FileNotFoundError(f"reads parquet not found: {inputs.reads}")
    if not inputs.lima_args.strip():
        raise ValueError("lima_args is empty; the control plane must resolve it from lima_preset")
    if inputs.partial_mask is not None and not inputs.partial_mask.exists():
        raise FileNotFoundError(f"partial_mask not found: {inputs.partial_mask}")

    workspace.mkdir(parents=True, exist_ok=True)
    lima_in_bam = workspace / "lima_in.bam"
    lima_config = workspace / "lima_config.json"

    reads_sql = validate_parquet_path(inputs.reads)
    bam_sql = validate_parquet_path(lima_in_bam)

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
                conn.execute(
                    f"CREATE VIEW lima_export_pass AS "
                    f"SELECT r.* FROM read_parquet('{reads_sql}') r JOIN {_INCOMING} m "
                    f"USING (sequence_idx) WHERE m.reason = '{ReadMaskReason.PASS.value}'"
                )
                source = "lima_export_pass"
            movie = _resolve_movie(conn, source)
            try:
                conn.execute(
                    _UBAM_COPY_SQL.format(
                        zmw_expr=_ZMW_FROM_READ_ID,
                        source=source,
                        out=bam_sql,
                        rg=_READ_GROUP_ID,
                        movie=movie,
                    )
                )
            except duckdb.Error as exc:
                # Name the missing capability rather than surface a bare binder
                # error four layers down in a SLURM log.
                raise RuntimeError(
                    "the loaded miint build cannot write an unaligned reads BAM "
                    "(COPY ... TO (FORMAT UBAM)); lima requires a CCS BAM and does not "
                    "finish on a FASTQ. See duckdb-miint#156 and "
                    f"tests/jobs/test_sam_bam_writer_miint_contract.py. Underlying error: {exc}"
                ) from exc
        lima_config.write_text(json.dumps({"args": inputs.lima_args}) + "\n")
        success = True
    finally:
        # On failure remove partial outputs so the SLURM launcher's manifest walker
        # (which runs after execute()) cannot promote them as the result.
        if not success:
            lima_in_bam.unlink(missing_ok=True)
            lima_config.unlink(missing_ok=True)

    return {"lima_in_bam": lima_in_bam, "lima_config": lima_config}
