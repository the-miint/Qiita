"""qiita-admin CLI — masked-read-export subcommand.

Each prep_sample on the pool is resolved across the named mask's similarity group
by the roster route; a `processed` one is exported from the mask named in its
resolution, and the others are listed and skipped. The output directory carries a
manifest (`MANIFEST_NAME`) naming, per exported file, the prep_sample's and the
mask's public handles.
"""

import argparse
import base64
import itertools
import json
import math
import os
import re
import sys
import time
from pathlib import Path

import httpx
from pydantic import ValidationError
from qiita_common.api_paths import (
    PATH_ADMIN_MASKED_READ_EXPORT_TICKET,
    PATH_ADMIN_PREFIX,
    PATH_ADMIN_SEQUENCED_POOL_MASKED_READ_EXPORT,
    PATH_EXPORTED_IDENTIFIER_MASK,
    PATH_EXPORTED_IDENTIFIER_PREFIX,
    PATH_EXPORTED_PROCESSING_MASK,
    PATH_EXPORTED_PROCESSING_PREFIX,
)
from qiita_common.data_plane_errors import is_data_plane_transient
from qiita_common.flight_constants import ipc_compression_headers
from qiita_common.models import (
    MaskedReadExportSample,
    SampleResolutionState,
)
from qiita_common.models._base import MAX_COHORT_PREP_SAMPLE_IDX
from qiita_common.parquet import PARQUET_COMPRESSION, ROW_GROUP_SIZE_BYTES
from qiita_common.sql import sql_string_literal

from qiita_control_plane.miint import connect_with_miint

from .. import _common

# Conservative accession charset: the accession is the leading filename
# component AND (via the output path) is interpolated into the DuckDB COPY SQL,
# so reject anything outside [A-Za-z0-9._-] — that excludes '/' (path traversal)
# and "'" (SQL-string break). ENA/NCBI accessions are alphanumeric in practice.
_SAFE_ACCESSION = re.compile(r"^[A-Za-z0-9._-]+$")


# The read_masked macro's columns, in the verbatim order the miint FORMAT FASTQ
# writer requires (read_id, sequence1, qual1, sequence2, qual2). Projected by the
# fastq COPY; aliasing any of these away raises a BinderException (pinned by the
# orchestrator's masked-export fastq contract test).
_READ_MASKED_COLUMNS = "read_id, sequence1, qual1, sequence2, qual2"


# How long a prep_sample's export is retried after the data plane or the control
# plane became unreachable. A data plane draining for a restart refuses new requests
# for as long as its in-flight ones run, up to the unit's TimeoutStopSec
# (deploy/systemd/qiita-data-plane@.service), so the default matches that bound.
DEFAULT_EXPORT_RETRY_SECONDS = 3900.0


def _retry_seconds_arg(value: str) -> float:
    """argparse `type=` for --retry-seconds: a finite number of seconds, 0 or more."""
    try:
        seconds = float(value)
    except ValueError:
        seconds = math.nan
    if not math.isfinite(seconds) or seconds < 0:
        raise argparse.ArgumentTypeError(f"must be a finite number >= 0, got {value!r}")
    return seconds


# The longest wait between two attempts of one prep_sample.
_EXPORT_RETRY_MAX_DELAY_S = 30.0

# A control-plane answer that means a proxy found nothing behind it, as during a
# control-plane restart.
_TRANSIENT_HTTP_STATUSES = frozenset({502, 503, 504})


def _export_failure_is_transient(exc: BaseException) -> bool:
    """True if a prep_sample's export failed because a service was unreachable rather
    than because of the request or the output.

    A data plane that goes away mid-stream reaches here as whatever was reading the
    stream raised: a Flight error, an `OSError` from the parquet path's reader, or a
    `duckdb.Error` from the fastq COPY over it, each carrying the gRPC message the
    data-plane classifier matches. Gated on those types, as the runner gates on the
    pyarrow ones, so a bug's message cannot match by text."""
    import duckdb  # noqa: PLC0415
    import pyarrow as pa  # noqa: PLC0415
    import pyarrow.flight as flight  # noqa: PLC0415

    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code in _TRANSIENT_HTTP_STATUSES
    if isinstance(exc, httpx.TransportError):
        return True
    if isinstance(exc, (flight.FlightError, OSError, duckdb.Error, pa.ArrowException)):
        return is_data_plane_transient(exc)
    return False


def _with_retry(export_one, prep_sample_idx: int, retry_seconds: float):
    """Run `export_one()`, retrying on a transient failure until `retry_seconds` have
    passed since the first one. Each attempt starts the prep_sample over: its files are
    committed all-or-nothing (`_commit_partials`), so a failed attempt leaves none."""
    deadline = None
    attempt = 0
    while True:
        try:
            return export_one()
        except Exception as exc:
            if not _export_failure_is_transient(exc):
                raise
            now = time.monotonic()
            if deadline is None:
                deadline = now + retry_seconds
            if now >= deadline:
                raise
            delay = min(_EXPORT_RETRY_MAX_DELAY_S, 2.0**attempt, deadline - now)
            print(
                f"prep_sample {prep_sample_idx}: {type(exc).__name__}: {str(exc)[:200]}; "
                f"retrying in {delay:.0f}s",
                file=sys.stderr,
            )
            time.sleep(delay)
            attempt += 1


def _commit_partials(copy_fn, pairs: list[tuple[Path, Path]]) -> None:
    """`_common.commit_partials` at mode 0600: these reads are privacy-masked sequence
    data, so each partial is chmod'd *before* its rename and the file is never visible
    at its final name under a looser umask, even for an instant. The all-or-nothing
    commit itself is shared — see there."""
    _common.commit_partials(copy_fn, pairs, mode=0o600)


def _peek_paired(reader):
    """Decide single-end vs paired from the Arrow `reader` WITHOUT draining it.

    A prep_sample is uniformly single- or paired-end — the mask filter drops
    reads but never changes R1/R2 layout — so the first non-empty batch is
    representative. Read leading batches until one carries rows, read pairing off
    that batch's `sequence2` null-ness, then return `(paired, stream)` where
    `stream` re-prepends the peeked batches in front of the still-unconsumed tail.
    This lets the fastq COPY stream straight through (bounded to one batch) rather
    than materializing the whole sample just to choose its output target. An empty
    stream (no rows at all) reports single-end."""
    import pyarrow as pa  # noqa: PLC0415

    schema = reader.schema
    sequence2_idx = schema.get_field_index("sequence2")
    peeked: list = []
    paired = False
    for batch in reader:
        peeked.append(batch)
        if batch.num_rows:
            paired = batch.column(sequence2_idx).null_count < batch.num_rows
            break
    stream = pa.RecordBatchReader.from_batches(schema, itertools.chain(peeked, reader))
    return paired, stream


def _write_masked_sample(reader, stem: str, output_dir: Path, fmt: str, con) -> list[str]:
    """Write one sample's streamed masked reads under output_dir, atomically (via
    a `.partial` sibling renamed into place) and chmod 0600, and return the names
    of the files committed. Both formats stream the Arrow `reader` (bounded memory,
    no full materialization):

      parquet — stream straight to a `pyarrow.parquet.ParquetWriter` (zstd) into
                one `<stem>.parquet`. No DuckDB hop, so the bulk read bytes are
                never materialized into DuckDB vectors and the scan never touches
                Acero (which is why the parquet path needs no buffer realignment —
                see `_handle_masked_read_export`). `con` is unused (pass None). The
                writer is opened from `reader.schema`, so a zero-row stream still
                produces a valid empty `<stem>.parquet`.
      fastq   — stream through the caller's shared miint DuckDB `con` (the FORMAT
                FASTQ writer lives in DuckDB+miint; `con` is reused across all
                samples). Output is gzip-compressed (`<stem>.fastq.gz`). The
                manifest carries no paired flag, so pairing is read from the data
                (`sequence2` null-ness) by peeking the first batch (`_peek_paired`),
                without draining the single-pass reader. A single-end sample → one
                `<stem>.fastq.gz`; a paired sample → `<stem>.R1.fastq.gz` +
                `<stem>.R2.fastq.gz` via miint's `{ORIENTATION}` placeholder
                (paired rows into a single path are a hard error in the writer;
                should the per-sample SE/PE uniformity ever break, a misdetected
                single-end COPY hits that error and fails loudly)."""
    if fmt == "parquet":
        import pyarrow as pa  # noqa: PLC0415
        import pyarrow.parquet as pq  # noqa: PLC0415

        partial = output_dir / f"{stem}.parquet.partial"

        def _write_parquet() -> None:
            # The data plane streams ~2048-row DuckDB DataChunks, so writing each
            # incoming batch as its own row group would fragment the file into
            # hundreds of tiny row groups (worse compression + pruning). Buffer
            # batches up to one row group's worth and write them as a single row
            # group — reproducing the layout (and bounded peak memory) of the
            # DuckDB `COPY` this path replaced. Size the group by encoded bytes
            # (ROW_GROUP_SIZE_BYTES, the qiita-wide cap every Parquet option set carries) rather
            # than a fixed row count, so wide rows don't produce oversized groups;
            # batch.nbytes is the in-memory size DuckDB's byte cap also measures.
            writer = pq.ParquetWriter(partial, reader.schema, compression=PARQUET_COMPRESSION)
            try:
                buffer: list = []
                buffered_bytes = 0

                def flush() -> None:
                    nonlocal buffer, buffered_bytes
                    if buffer:
                        writer.write_table(pa.Table.from_batches(buffer, reader.schema))
                        buffer = []
                        buffered_bytes = 0

                for batch in reader:
                    buffer.append(batch)
                    buffered_bytes += batch.nbytes
                    if buffered_bytes >= ROW_GROUP_SIZE_BYTES:
                        flush()
                flush()
            finally:
                writer.close()

        _commit_partials(_write_parquet, [(partial, output_dir / f"{stem}.parquet")])
        return [f"{stem}.parquet"]
    if fmt == "fastq":
        paired, stream = _peek_paired(reader)
        con.register("masked", stream)
        pairs = [
            (output_dir / f"{name}.partial", output_dir / name)
            for name in _fastq_names(stem, paired=paired)
        ]
        if paired:
            # `{ORIENTATION}` expands to R1/R2, so the one COPY emits both
            # `<stem>.R1.fastq.gz.partial` and `<stem>.R2.fastq.gz.partial`.
            target = output_dir / f"{stem}.{{ORIENTATION}}.fastq.gz.partial"
        else:
            target = pairs[0][0]
        _commit_partials(
            lambda: con.execute(
                f"COPY (SELECT {_READ_MASKED_COLUMNS} FROM masked) "
                f"TO {sql_string_literal(target)} (FORMAT FASTQ, COMPRESSION 'gzip')"
            ),
            pairs,
        )
        return [final.name for _, final in pairs]
    raise ValueError(f"unsupported export format: {fmt!r}")


def _fastq_names(stem: str, *, paired: bool) -> list[str]:
    """The fastq files one prep_sample's export commits: `<stem>.fastq.gz`, or
    `<stem>.R1.fastq.gz` and `<stem>.R2.fastq.gz` when paired. The writer
    commits these and the never-overwrite pre-scan looks for them, so a rename
    cannot leave the pre-scan checking names the writer no longer uses."""
    if paired:
        return [f"{stem}.R1.fastq.gz", f"{stem}.R2.fastq.gz"]
    return [f"{stem}.fastq.gz"]


def _export_stem(sample: dict, run_idx, pool_idx) -> str:
    """Per-sample output filename stem, single-sourced so the export loop and the
    fastq overwrite pre-scan can't drift: ``<accession>.<run>.<pool>.<prep_sample>``.

    **This composite embeds our internal identifiers and predates the rule against
    letting them leave Qiita** (see CLAUDE.md). It survives because this command is
    system_admin-only and changing the stem would silently rename every existing
    export and route around the `_SAFE_ACCESSION` charset check above. Do not copy
    the shape to a new surface: a published artifact names its samples with
    `qiita.exported_identifier.export_id`.
    """
    return f"{sample['biosample_accession']}.{run_idx}.{pool_idx}.{sample['prep_sample_idx']}"


def _parquet_row_count(path: Path) -> int:
    """Record count of an existing parquet export, read from the footer metadata
    only — no row data is scanned. The export writes one row per masked-read record
    (a pair counts once), so this lines up with the data plane's read_masked row
    count directly."""
    import pyarrow.parquet as pq  # noqa: PLC0415

    return pq.ParquetFile(path).metadata.num_rows


def _count_masked(flight_client, ticket_bytes: bytes) -> int:
    """How many masked reads the signed ticket selects, via the data plane's
    ``count_masked`` DoAction — a cheap ``count(*)`` against the light ``read_mask``
    table that never streams or materializes the read sequences. Reuses the very
    ticket the export streams with: counting is strictly less than reading, so the
    ticket's ``(prep_sample_idx, mask_idx)`` authorization already covers it."""
    import pyarrow.flight as flight  # noqa: PLC0415

    results = list(flight_client.do_action(flight.Action("count_masked", ticket_bytes)))
    if not results:
        raise RuntimeError("count_masked DoAction returned no result")
    return json.loads(results[0].body.to_pybytes())["count"]


# The output directory's record of which prep_sample and which mask each exported file
# holds, by public handle: `{"files": {<file name>: {"export_id": "QM…",
# "biosample_accession": …, "export_processing_id": "QP…"}}}`. Rewritten after each
# prep_sample's files are committed. The overwrite check reads it: a file it names
# under another mask's handle is never overwritten.
MANIFEST_NAME = "masked-read-export.manifest.json"


def _read_export_manifest(output_dir: Path) -> dict[str, dict]:
    """The `files` map of `output_dir`'s manifest, or `{}` when there is none.
    Raises ValueError when the file is not a manifest this command wrote."""
    path = output_dir / MANIFEST_NAME
    if not path.exists():
        return {}
    try:
        document = json.loads(path.read_text())
    except json.JSONDecodeError as exc:
        raise ValueError(f"{path} is not valid JSON: {exc}") from exc
    files = document.get("files") if isinstance(document, dict) else None
    if not isinstance(files, dict) or not all(
        isinstance(entry, dict) and isinstance(entry.get("export_processing_id"), str)
        for entry in files.values()
    ):
        raise ValueError(
            f"{path} is not a masked-read-export manifest (expected"
            ' {"files": {<name>: {"export_processing_id": ...}}})'
        )
    return files


def _write_export_manifest(output_dir: Path, files: dict[str, dict]) -> None:
    """Replace `output_dir`'s manifest with `files`, through a `.partial` sibling so a
    reader never sees a half-written one."""
    path = output_dir / MANIFEST_NAME
    partial = path.with_name(path.name + ".partial")
    with partial.open("w") as handle:
        json.dump({"files": dict(sorted(files.items()))}, handle, indent=2, sort_keys=True)
        handle.write("\n")
    os.replace(partial, path)


def _candidate_names(stem: str, fmt: str) -> list[str]:
    """Every file name a prep_sample's export may commit in `fmt`."""
    if fmt == "parquet":
        return [f"{stem}.parquet"]
    return [name for paired in (False, True) for name in _fastq_names(stem, paired=paired)]


def _mint_mask_handles(
    base_url: str, token: str, samples_by_mask: dict[int, list[int]]
) -> tuple[dict[int, str], dict[int, str]]:
    """Mint (or recover) each mask's public handle and each prep_sample's under its mask
    (POST /exported-processing/mask, /exported-identifier/mask). Returns
    `({mask_idx: export_processing_id}, {prep_sample_idx: export_id})`."""
    processing_path = f"{PATH_EXPORTED_PROCESSING_PREFIX}{PATH_EXPORTED_PROCESSING_MASK}"
    identifier_path = f"{PATH_EXPORTED_IDENTIFIER_PREFIX}{PATH_EXPORTED_IDENTIFIER_MASK}"
    mask_handles: dict[int, str] = {}
    sample_handles: dict[int, str] = {}
    for mask_idx, preps in sorted(samples_by_mask.items()):
        body = _common.call("POST", base_url, token, processing_path, json={"mask_idx": mask_idx})
        mask_handles[mask_idx] = body["export_processing_id"]
        for start in range(0, len(preps), MAX_COHORT_PREP_SAMPLE_IDX):
            chunk = preps[start : start + MAX_COHORT_PREP_SAMPLE_IDX]
            body = _common.call(
                "POST",
                base_url,
                token,
                identifier_path,
                json={"mask_idx": mask_idx, "prep_sample_idx": chunk},
            )
            sample_handles.update(
                {i["prep_sample_idx"]: i["export_id"] for i in body["identifiers"]}
            )
    return mask_handles, sample_handles


def _left_out_lines(samples: list[MaskedReadExportSample]) -> list[str]:
    """One line per roster prep_sample that is not exported, and why."""
    return [s.resolution.describe() for s in samples]


def _handle_masked_read_export(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """Export the masked reads of every (non-retired) prep_sample on a sequenced_pool
    that is processed in `--mask-idx`'s similarity group, each from the mask it
    resolved to, to per-sample files. system_admin only (admin:masked_read_export).

    GETs the roster manifest, which resolves each prep_sample across the group (409 when
    `--mask-idx` is deprecated). A prep_sample that is `incomplete` or `not_processed`
    is not exported; the run lists them, with the reason, on stderr after the
    summary, and still exits 0 when every processed prep_sample was exported. For each
    processed prep_sample it mints a just-in-time DoGet ticket under the prep_sample's mask
    and streams its read_masked rows from the data plane straight to disk (parquet
    via a pyarrow ParquetWriter; fastq.gz via one shared miint DuckDB connection
    reused across every prep_sample) — so a large pool never buffers in memory or on an
    intermediate disk hop. Per-sample writes are atomic and 0600.

    Before any download it mints the public handle of each mask exported from and
    of each prep_sample under its mask, and after each prep_sample's files are
    committed it records them in the output directory's manifest (`MANIFEST_NAME`):
    file name → prep_sample handle, biosample accession, mask handle.

    Fails loudly (exit 1, nothing written) when:
      - a processed prep_sample lacks a usable biosample_accession (missing — not yet
        NCBI-submitted — or outside the safe charset), since the filename needs it;
      - an existing output file is recorded in the manifest under a different mask's
        handle: re-exporting it would replace one mask's reads with another's
        under the same name;
      - fastq output: any target file already exists (below).

    A file that the manifest records under the same mask, or does not record, is
    re-exported idempotently for parquet: skipped when its record count matches the
    data plane's (nothing changed), overwritten when the counts differ. fastq has no
    cheap on-disk count, so an existing fastq target is refused up front rather
    than re-exported or overwritten.

    A prep_sample whose export fails because the data plane or the control plane is
    unreachable is exported again from the start, for up to `--retry-seconds`
    (`_with_retry`).

    The output directory is created if missing (parents included).
    """
    import duckdb  # noqa: PLC0415
    import pyarrow as pa  # noqa: PLC0415
    import pyarrow.flight as flight  # noqa: PLC0415
    import pyarrow.ipc as ipc  # noqa: PLC0415

    output_dir: Path = args.output_dir
    try:
        output_dir.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        print(f"error: could not create output directory {output_dir}: {exc}", file=sys.stderr)
        return 2

    try:
        token = _common.read_token()
    except RuntimeError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    manifest_path = (
        f"{PATH_ADMIN_PREFIX}"
        f"{PATH_ADMIN_SEQUENCED_POOL_MASKED_READ_EXPORT.format(sequenced_pool_idx=args.sequenced_pool_idx)}"
    )
    ticket_path = f"{PATH_ADMIN_PREFIX}{PATH_ADMIN_MASKED_READ_EXPORT_TICKET}"
    try:
        roster = _common.call(
            "GET", args.base_url, token, manifest_path, params={"mask_idx": args.mask_idx}
        )
    except httpx.HTTPStatusError as exc:
        _common.print_http_status_error(exc.response)
        return 1
    except httpx.RequestError as exc:
        _common.print_request_error(exc)
        return 1

    try:
        samples = [MaskedReadExportSample.model_validate(s) for s in roster["samples"]]
    except ValidationError as exc:
        _common.print_validation_error(exc)
        return 1
    run_idx = roster["sequencing_run_idx"]
    pool_idx = roster["sequenced_pool_idx"]
    if not samples:
        print(
            f"no prep_samples on sequenced_pool {pool_idx} for mask_idx {args.mask_idx}; "
            "nothing to export"
        )
        return 0
    processed = [s for s in samples if s.resolution.state is SampleResolutionState.PROCESSED]
    left_out = [s for s in samples if s.resolution.state is not SampleResolutionState.PROCESSED]

    def print_left_out() -> None:
        if not left_out:
            return
        print(
            f"not exported: {len(left_out)} prep_sample(s) on sequenced_pool {pool_idx} are"
            f" not processed in mask_idx {args.mask_idx}'s group:",
            file=sys.stderr,
        )
        for line in _left_out_lines(left_out):
            print(f"  {line}", file=sys.stderr)

    # Validate every exported prep_sample's accession up front so an unsubmitted/odd
    # prep_sample fails the whole export before any download — never a partial output set.
    bad = sorted(
        s.prep_sample_idx
        for s in processed
        if not s.biosample_accession or not _SAFE_ACCESSION.match(s.biosample_accession)
    )
    if bad:
        print(
            f"error: {len(bad)} prep_sample(s) on sequenced_pool {pool_idx} have no usable "
            f"biosample_accession (missing or outside [A-Za-z0-9._-]): prep_sample_idx {bad}. "
            "The export filename requires the accession — submit/repair these prep_samples first.",
            file=sys.stderr,
        )
        return 1
    if not processed:
        print(f"exported 0 prep_sample(s) from sequenced_pool {pool_idx} to {output_dir}")
        print_left_out()
        return 0

    stems = {s.prep_sample_idx: _export_stem(s.model_dump(), run_idx, pool_idx) for s in processed}
    samples_by_mask: dict[int, list[int]] = {}
    for s in processed:
        samples_by_mask.setdefault(s.resolution.definition_idx, []).append(s.prep_sample_idx)
    try:
        recorded = _read_export_manifest(output_dir)
        mask_handles, sample_handles = _mint_mask_handles(args.base_url, token, samples_by_mask)
    except httpx.HTTPStatusError as exc:
        _common.print_http_status_error(exc.response)
        return 1
    except httpx.RequestError as exc:
        _common.print_request_error(exc)
        return 1
    except (ValueError, OSError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    # A file the manifest records under another mask's handle holds that mask's
    # reads; refuse the whole export before any download rather than replace them.
    conflicts = sorted(
        f"{name} (recorded under {recorded[name]['export_processing_id']},"
        f" now {mask_handles[s.resolution.definition_idx]})"
        for s in processed
        for name in _candidate_names(stems[s.prep_sample_idx], args.format)
        if name in recorded
        and (output_dir / name).exists()
        and recorded[name]["export_processing_id"] != mask_handles[s.resolution.definition_idx]
    )
    if conflicts:
        print(
            f"error: refusing to overwrite {len(conflicts)} file(s) in {output_dir} exported"
            f" from a different mask: {conflicts}. Export to another --output-dir, or move"
            " them aside first.",
            file=sys.stderr,
        )
        return 1

    # fastq output is never overwritten: a gzipped fastq has no cheap footer to
    # count (unlike parquet), and a paired sample's two files make a count-vs-disk
    # compare ambiguous. So for fastq, refuse up front if any target name already
    # exists — fail loudly before streaming rather than clobber a prior export.
    # (parquet re-export is decided per-sample by the count probe in the loop.)
    if args.format == "fastq":
        existing = [
            str(p)
            for s in processed
            for name in _candidate_names(stems[s.prep_sample_idx], "fastq")
            if (p := output_dir / name).exists()
        ]
        if existing:
            print(
                f"error: refusing to overwrite {len(existing)} existing fastq file(s) in "
                f"{output_dir}: {existing}. Delete them to re-export "
                "(fastq export has no incremental/count-based skip).",
                file=sys.stderr,
            )
            return 1

    # Only the fastq path feeds Flight batches into DuckDB (the miint FORMAT FASTQ
    # writer), which routes a registered pyarrow reader through pyarrow.dataset →
    # Acero. Flight hands us each RecordBatch by zero-copying the gRPC message
    # body, whose absolute base address carries no element-alignment guarantee, so
    # a uint64/int32 column buffer routinely lands off its natural alignment even
    # though the data plane writes 64-byte-aligned IPC (arrow-rs default), and
    # Acero then logs a "poorly aligned input buffer" warning per misaligned column
    # per batch (apache/arrow#37195). Ask the Flight reader to realign each buffer
    # to its type's required alignment on receive (DataTypeSpecific copies only the
    # small offset/validity/fixed-width buffers, leaving the bulk sequence/quality
    # byte buffers zero-copy). The parquet path streams straight to a ParquetWriter
    # (no Acero), so it needs no realignment and keeps those bulk buffers zero-copy.
    #
    # Built unconditionally so the two independent knobs — realignment (fastq
    # only) and compression (`--compress`) — cannot drop each other. The earlier
    # form returned `None` for parquet, which left no place to put a header.
    # Empty headers plus `read_options=None` is equivalent to passing no options.
    read_opts = flight.FlightCallOptions(
        headers=ipc_compression_headers(args.compress),
        read_options=(
            ipc.IpcReadOptions(ensure_alignment=ipc.Alignment.DataTypeSpecific)
            if args.format == "fastq"
            else None
        ),
    )
    con = None
    flight_client = None
    exported = 0
    skipped = 0
    try:
        # The fastq writer needs a miint DuckDB connection; open it once and reuse
        # it across all samples (each sample re-registers the `masked` view) rather
        # than paying a fresh connect + extension LOAD per sample. Parquet needs no
        # DuckDB.
        con = connect_with_miint() if args.format == "fastq" else None
        flight_client = flight.FlightClient(args.data_plane_url)
        for s in processed:
            prep = s.prep_sample_idx
            stem = stems[prep]
            mask_idx = s.resolution.definition_idx

            def export_one(prep=prep, stem=stem, mask_idx=mask_idx) -> list[str] | None:
                """Export one prep_sample and return the names written; None when it
                was already up to date."""
                # A fresh ticket per attempt: one minted before a long outage may
                # have expired by the time the data plane is back.
                ticket_resp = _common.call(
                    "POST",
                    args.base_url,
                    token,
                    ticket_path,
                    json={"prep_sample_idx": prep, "mask_idx": mask_idx},
                )
                ticket_bytes = base64.b64decode(ticket_resp["ticket"])
                # Idempotent parquet re-export: only probe the data plane's count
                # when the output file already exists (a first export pays
                # nothing), then skip on a match (nothing changed) and fall through
                # to overwrite on a mismatch. fastq took the all-or-nothing
                # existence check above, so it always exports here.
                if args.format == "parquet":
                    target = output_dir / f"{stem}.parquet"
                    if target.exists() and _parquet_row_count(target) == _count_masked(
                        flight_client, ticket_bytes
                    ):
                        return None
                reader = flight_client.do_get(flight.Ticket(ticket_bytes), read_opts).to_reader()
                return _write_masked_sample(reader, stem, output_dir, args.format, con)

            written = _with_retry(export_one, prep, args.retry_seconds)
            if written is None:
                skipped += 1
                continue
            exported += 1
            for name in written:
                recorded[name] = {
                    "export_id": sample_handles[prep],
                    "biosample_accession": s.biosample_accession,
                    "export_processing_id": mask_handles[mask_idx],
                }
            _write_export_manifest(output_dir, recorded)
    except httpx.HTTPStatusError as exc:
        _common.print_http_status_error(exc.response)
        return 1
    except httpx.RequestError as exc:
        _common.print_request_error(exc)
        return 1
    except flight.FlightError as exc:
        print(f"flight error: {exc}", file=sys.stderr)
        return 1
    except (duckdb.Error, OSError, pa.ArrowInvalid, pa.ArrowKeyError) as exc:
        # The types the environment raises at these sites; a plain ValueError or
        # RuntimeError from a bug still ends in a traceback. A data plane failure
        # after the first batch lands here rather than above: the parquet path's
        # reader raises OSError, the fastq path's COPY over it a duckdb.Error.
        # ArrowInvalid and ArrowKeyError are pyarrow refusing an input, among
        # them --data-plane-url (unparseable, or a scheme with no Flight
        # transport, such as http) and an existing parquet target it cannot read.
        print(f"error: {exc}", file=sys.stderr)
        return 1
    finally:
        if flight_client is not None:
            flight_client.close()
        if con is not None:
            con.close()

    summary = f"exported {exported} prep_sample(s)"
    if skipped:
        summary += f" (skipped {skipped} already up to date)"
    print(f"{summary} from sequenced_pool {pool_idx} to {output_dir}")
    print_left_out()
    return 0
