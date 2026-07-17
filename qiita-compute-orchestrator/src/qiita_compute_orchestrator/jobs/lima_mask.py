"""Native job: turn lima's clipped FASTQ into a partial read mask.

`(read.parquet, lima_out.fastq [, partial_mask]) -> partial_mask`. Last entry of the
long-read adapter chain (`lima_export -> lima -> lima_mask`); it emits the
`partial_mask` binding qc consumes (or host_filter, further down the chain),
which then extends the trims cumulatively (see jobs/qc.py).

**Trims come from miint's `infer_trim`, not from parsing lima.** The macro takes
the original and the clipped relations, joins them on `sequence_index`, locates
the clipped sequence inside the original, and returns `(sequence_index,
trimmed_5p, trimmed_3p)` — one row per ORIGINAL read, with `NULL/NULL` for a read
the tool omitted. It fails loud if a kept read is not a contiguous substring of its
original (i.e. the tool edited internal bases rather than end-trimming). lima is a
pure end-trimmer, so that contract holds; do not suppress the failure.

**The join key is the record name's ZMW, resolved through `lima_zmw_map`.**
`lima_export` cannot put `sequence_idx` in the record name: lima requires the
PacBio `<movie>/<zmw>/ccs` convention and rewrites the name of every read it emits
from the per-read `zm` tag, which is an int32 — a lake-wide `sequence_idx` over
2^31 would come back silently TRUNCATED. So the ZMW is a per-file dense counter
and `lima_zmw_map` carries `zmw -> sequence_idx`; the key is recovered by parsing
the ZMW out of the name and joining. (`read_fastx`'s own `sequence_index` is
POSITIONAL and resets per file, so it is NOT our `sequence_idx` either.)

**Reads lima dropped become `twist_no_adaptor`.** A HiFi read carrying no Twist
adaptor is not a library molecule from this run — it is artifactual. It is masked
out (excluded from `read_masked`, which serves only `pass`) and, per the mask's
bucket whitelist, counts toward `raw` only.

**An empty lima output does not arrive via the lima step — it is a GUARD, not a
path.** The comment this replaces claimed an adapter-free sample was "a legitimate
outcome, not an error" that yielded an all-`twist_no_adaptor` mask. Probed at lima
2.13.0 on a CCS BAM, that is false: handed reads of which NONE carries an adaptor,
lima does not emit an empty file and exit 0 — it exits 1 with `FATAL ... Could not
find matching barcodes!` and writes nothing, so `lima.sh` fails the step and this
job never runs. (Control: the byte-identical BAM plus ONE adaptered read exits 0
and clips it, so the trigger really is "no adapters anywhere", not the fixture.)
The branch below is kept because it is one cheap line and `lima_out_fastq` is an
external tool's file — but do not read it as the documented behavior of an
adapter-free sample. What that sample SHOULD do (fail the ticket loudly, as it does
now, versus mask every read out) is an assay decision, not a behavioral one, and is
not settled here.

`read_fastx` REJECTS an empty file (`Error: Empty file`), so the guard hands
`infer_trim` an empty typed relation rather than letting it crash the step.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
from pydantic import BaseModel
from qiita_common.duckdb_miint import is_empty_sequence_file
from qiita_common.models import ReadMaskReason
from qiita_common.parquet import validate_parquet_path

from ..miint import (
    PARQUET_OPTS,
    apply_duckdb_settings,
    duckdb_tmp_dir,
    open_miint_conn,
    resolve_duckdb_memory_gb,
)

YAML_STEP_NAME = "lima_mask"

# Off-SLURM fallback cap; under SLURM the real cap is sized to the cgroup.
# `infer_trim` materializes both the original and clipped sequence sets and runs a
# per-row substring search, so unlike qc this step's footprint scales with total
# SEQUENCE BYTES (millions of ~10 kb HiFi reads), not row count alone. Size the
# SLURM allocation accordingly; this constant is only the off-SLURM floor.
_DUCKDB_MEMORY_GB = 8
_DUCKDB_THREADS = 4

# In-DuckDB relation names.
_ORIG = "lima_orig"
_QCD = "lima_qcd"
_INCOMING = "lima_mask_incoming"
_ZMW_MAP = "lima_zmw_map"

# lima emits `<movie>/<zmw>/ccs`, then its own BAM tags after a space (`read_fastx`
# parses those into a separate `comment` column, so `read_id` is just the name).
# The ZMW is field 2 — the key into `lima_zmw_map`.
#
# TRY_CAST, not `::UINTEGER`: a name whose ZMW is not an integer is exactly the
# shape lima emits when a record reaches it without a `zm` tag (`<movie>/?/ccs`).
# A hard cast aborts the query with a raw DuckDB ConversionException; TRY_CAST
# yields NULL and routes it into `_assert_lima_reads_are_known`'s unresolved-ZMW
# error, which names the actual problem. Both fail loud — only one explains itself.
_ZMW_FROM_READ_ID = "TRY_CAST(split_part(f.read_id, '/', 2) AS UINTEGER)"


def _carry_select(incoming_view: str) -> str:
    """The incoming mask's non-`pass` rows, verbatim — the spike-ins syndna marked.
    They were never sent to lima, so infer_trim never sees them; carrying them here
    (with `ELSE reason` semantics) is what keeps a `spikein_syndna` verdict from
    being overwritten by `twist_no_adaptor`."""
    return (
        "SELECT sequence_idx, reason, "
        "left_trim1::UINTEGER AS left_trim1, right_trim1::UINTEGER AS right_trim1, "
        "left_trim2::UINTEGER AS left_trim2, right_trim2::UINTEGER AS right_trim2 "
        f"FROM {incoming_view} WHERE reason <> '{ReadMaskReason.PASS.value}'"
    )


class Inputs(BaseModel):
    """Typed input contract for lima_mask.

    `reads` is the raw `read.parquet` lima_export exported; `lima_out_fastq` is
    lima's clipped output; `lima_zmw_map` is lima_export's `zmw -> sequence_idx`
    map, without which lima's output cannot be keyed back to the lake (see the
    module docstring). `partial_mask` is the OPTIONAL upstream mask (today:
    syndna's) — when bound, only its `pass` reads went to lima, and its non-`pass`
    rows (the spike-ins) are carried through unchanged. `prep_sample_idx` is
    accepted but unused — the mask keys on the globally-unique `sequence_idx`, so a
    BLOCK-scoped ticket (which flows no such scalar) validates against the same shape.
    """

    reads: Path
    lima_out_fastq: Path
    lima_zmw_map: Path
    partial_mask: Path | None = None
    prep_sample_idx: int | None = None
    work_ticket_idx: int


def _assert_lima_reads_are_known(conn: duckdb.DuckDBPyConnection, lima_out_fastq: Path) -> None:
    """Every record lima emitted must correspond to exactly one input read.

    lima's output should be a SUBSET of what we sent it (it only drops reads and
    clips ends), so all three conditions checked here — a ZMW that is not in the
    map, a resolved key that is not an input read, or a duplicated one — would
    indeed be a lima bug, or a stale/mismatched output file. None has been
    observed; this is a boundary check, not a workaround.

    It is here, and not deleted as over-defensive, for one reason: lima is an EXTERNAL
    container binary, so its output is not an invariant our own code establishes (the
    shape of an incoming `partial_mask` IS — see `_partial_mask`, where the equivalent
    checks were dropped for exactly that reason). And if the contract does break, it
    breaks SILENTLY: `infer_trim` LEFT JOINs original→clipped, so an unknown clipped
    key is dropped rather than erroring, and a duplicate key fans the join out and
    emits two mask rows for one read. A wrong mask would ship looking like a right
    one. One aggregate scan is a cheap price for turning that into a loud failure.

    The unresolved-ZMW check is the sharpest of the three: lima writes the record
    name from the `zm` tag, so a name whose ZMW is not in `lima_zmw_map` means the
    key channel itself broke (a truncated ZMW, a mismatched map from another run).
    That must never be papered over — the join would drop the read and the mask
    would call it `twist_no_adaptor`.

    All three are counted in ONE pass: `_QCD` is a view over `read_fastx`, so a
    scan per check would re-parse lima's whole output three times.
    """
    unresolved, n, distinct = conn.execute(
        "SELECT count(*) FILTER (WHERE q.sequence_index IS NULL), count(*), "
        "       count(DISTINCT q.sequence_index) "
        f"FROM {_QCD} q"
    ).fetchone()
    if unresolved:
        raise ValueError(
            f"lima output ({lima_out_fastq}) has {unresolved} record(s) whose ZMW is not "
            "in lima_zmw_map; the sequence_idx round-trip is broken"
        )
    (unknown,) = conn.execute(
        f"SELECT count(*) FROM {_QCD} q ANTI JOIN {_ORIG} o USING (sequence_index)"
    ).fetchone()
    if unknown:
        raise ValueError(
            f"lima output ({lima_out_fastq}) has {unknown} record(s) whose ZMW resolves to "
            "a sequence_idx that is not an input read; the map does not match these reads"
        )
    if n != distinct:
        raise ValueError(
            f"lima output ({lima_out_fastq}) has {n - distinct} duplicate record name(s); "
            "infer_trim would fan the join out and emit two mask rows for one read"
        )


async def execute(inputs: Inputs, workspace: Path) -> dict[str, Path]:
    if not inputs.reads.exists():
        raise FileNotFoundError(f"reads parquet not found: {inputs.reads}")
    if not inputs.lima_out_fastq.exists():
        raise FileNotFoundError(f"lima_out_fastq not found: {inputs.lima_out_fastq}")
    if not inputs.lima_zmw_map.exists():
        raise FileNotFoundError(f"lima_zmw_map not found: {inputs.lima_zmw_map}")
    if inputs.partial_mask is not None and not inputs.partial_mask.exists():
        raise FileNotFoundError(f"partial_mask not found: {inputs.partial_mask}")

    workspace.mkdir(parents=True, exist_ok=True)
    partial_mask = workspace / "partial_mask.parquet"

    reads_sql = validate_parquet_path(inputs.reads)
    lima_sql = validate_parquet_path(inputs.lima_out_fastq)
    zmw_map_sql = validate_parquet_path(inputs.lima_zmw_map)
    out_sql = validate_parquet_path(partial_mask)

    success = False
    try:
        with duckdb_tmp_dir(workspace) as duckdb_tmp, open_miint_conn() as conn:
            apply_duckdb_settings(
                conn,
                duckdb_tmp,
                memory_gb=resolve_duckdb_memory_gb(_DUCKDB_MEMORY_GB, threads=_DUCKDB_THREADS),
                threads=_DUCKDB_THREADS,
            )
            # infer_trim's ORIGINALS are exactly the reads lima_export sent to lima:
            # every raw read, or — when an upstream mask is bound — only its `pass`
            # reads (the spike-ins were never exported). A spike-in must NOT reach
            # infer_trim: absent from lima's output, it would come back NULL/NULL and
            # be mislabelled twist_no_adaptor. Its row is carried verbatim instead.
            carry_select: str | None = None
            if inputs.partial_mask is None:
                conn.execute(
                    f"CREATE VIEW {_ORIG} AS "
                    "SELECT sequence_idx AS sequence_index, sequence1 AS sequence "
                    f"FROM read_parquet('{reads_sql}')"
                )
            else:
                mask_sql = validate_parquet_path(inputs.partial_mask)
                conn.execute(f"CREATE VIEW {_INCOMING} AS SELECT * FROM read_parquet('{mask_sql}')")
                conn.execute(
                    f"CREATE VIEW {_ORIG} AS "
                    "SELECT r.sequence_idx AS sequence_index, r.sequence1 AS sequence "
                    f"FROM read_parquet('{reads_sql}') r JOIN {_INCOMING} m USING (sequence_idx) "
                    f"WHERE m.reason = '{ReadMaskReason.PASS.value}'"
                )
                carry_select = _carry_select(_INCOMING)

            # infer_trim requires both relations to expose `sequence_index` +
            # `sequence`. On the clipped side the key is the ZMW parsed out of
            # lima's record name and resolved through `lima_zmw_map` — never
            # read_fastx's positional `sequence_index`.
            conn.execute(f"CREATE VIEW {_ZMW_MAP} AS SELECT * FROM read_parquet('{zmw_map_sql}')")
            if is_empty_sequence_file(inputs.lima_out_fastq):
                # A GUARD, not the adapter-free path: probed, lima FATALs rather than
                # emitting an empty output, so the step fails before reaching here (see
                # the module docstring). Kept only because `read_fastx` raises `Empty
                # file` rather than returning zero rows, and this job must not crash on
                # an external tool's file — hand infer_trim an empty typed relation.
                conn.execute(
                    f"CREATE VIEW {_QCD} AS "
                    "SELECT NULL::BIGINT AS sequence_index, NULL::VARCHAR AS sequence "
                    "WHERE false"
                )
            else:
                # LEFT JOIN, not JOIN: an unresolvable ZMW must surface as a NULL
                # key for `_assert_lima_reads_are_known` to raise on, not vanish
                # from the relation and silently become `twist_no_adaptor`.
                conn.execute(
                    f"CREATE VIEW {_QCD} AS "
                    "SELECT m.sequence_idx AS sequence_index, f.sequence1 AS sequence "
                    f"FROM read_fastx('{lima_sql}') f "
                    f"LEFT JOIN {_ZMW_MAP} m ON m.zmw = {_ZMW_FROM_READ_ID}"
                )
            _assert_lima_reads_are_known(conn, inputs.lima_out_fastq)
            # One row per ORIGINAL (pass) read. `infer_trim` returns NULL/NULL for a
            # read lima omitted -> twist_no_adaptor with zero trims. Single-end, so
            # the mate trims are NULL, matching the partial-mask shape qc consumes.
            # The carry branch (when there is one) adds the incoming non-pass rows —
            # the spike-ins — unchanged.
            no_adaptor = ReadMaskReason.TWIST_NO_ADAPTOR.value
            infer_select = (
                "SELECT sequence_index AS sequence_idx, "
                f"        CASE WHEN trimmed_5p IS NULL THEN '{no_adaptor}' "
                f"             ELSE '{ReadMaskReason.PASS.value}' END AS reason, "
                "        coalesce(trimmed_5p, 0)::UINTEGER AS left_trim1, "
                "        coalesce(trimmed_3p, 0)::UINTEGER AS right_trim1, "
                "        NULL::UINTEGER AS left_trim2, "
                "        NULL::UINTEGER AS right_trim2 "
                f"FROM infer_trim({_ORIG}, {_QCD})"
            )
            branches = [infer_select] if carry_select is None else [infer_select, carry_select]
            union_sql = " UNION ALL ".join(f"({b})" for b in branches)
            conn.execute(
                f"COPY ({union_sql} ORDER BY sequence_idx) TO '{out_sql}' ({PARQUET_OPTS})"
            )
        success = True
    finally:
        if not success:
            partial_mask.unlink(missing_ok=True)

    # Same `partial_mask` binding syndna emits, so qc consumes whichever ran last
    # (last-writer-wins on the binding).
    return {"partial_mask": partial_mask}
