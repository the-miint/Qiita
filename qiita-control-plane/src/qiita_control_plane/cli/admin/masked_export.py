"""qiita-admin CLI — masked-read-export subcommand.

Split out of the former single-file ``cli.admin`` module; behavior unchanged.
"""

import argparse
import base64
import contextlib
import itertools
import json
import os
import re
import sys
from pathlib import Path

import httpx
from qiita_common.api_paths import (
    PATH_ADMIN_MASKED_READ_EXPORT_TICKET,
    PATH_ADMIN_PREFIX,
    PATH_ADMIN_SEQUENCED_POOL_MASKED_READ_EXPORT,
)
from qiita_common.parquet import ROW_GROUP_SIZE_BYTES

from qiita_control_plane.miint import connect_with_miint

from .. import _common

# Conservative accession charset: the accession is the leading filename
# component AND (via the output path) is interpolated into the DuckDB COPY SQL,
# so reject anything outside [A-Za-z0-9._-] — that excludes '/' (path traversal)
# and "'" (SQL-string break). ENA/NCBI accessions are alphanumeric in practice.
_SAFE_ACCESSION = re.compile(r"^[A-Za-z0-9._-]+$")


def _sql_str(path: Path) -> str:
    """Escape a filesystem path for inlining as a DuckDB SQL string literal."""
    return str(path).replace("'", "''")


# The read_masked view's columns, in the verbatim order the miint FORMAT FASTQ
# writer requires (read_id, sequence1, qual1, sequence2, qual2). Projected by the
# fastq COPY; aliasing any of these away raises a BinderException (pinned by the
# orchestrator's masked-export fastq contract test).
_READ_MASKED_COLUMNS = "read_id, sequence1, qual1, sequence2, qual2"


def _commit_partials(copy_fn, pairs: list[tuple[Path, Path]]) -> None:
    """Run `copy_fn` (which COPYs the masked rows into each pair's `.partial`),
    then move each partial into place. Each partial is chmod 0600 *before* the
    rename — the reads are privacy-masked sequence data, so the file is never
    visible at its final name under a looser umask, even for an instant.

    All-or-nothing across the pair: on any failure (COPY error, or a rename/chmod
    failing partway through a paired R1+R2 commit) every partial AND every
    already-committed final is removed, so a retry never finds a half-written
    file or a lone R1 without its R2. The partial paths are known up front so a
    failure *inside* the COPY (which may have already created some partials) is
    cleaned up too."""
    committed: list[Path] = []
    try:
        copy_fn()
        for partial, final in pairs:
            partial.chmod(0o600)
            os.replace(partial, final)
            committed.append(final)
    except BaseException:
        for partial, _ in pairs:
            with contextlib.suppress(FileNotFoundError):
                partial.unlink()
        for final in committed:
            with contextlib.suppress(FileNotFoundError):
                final.unlink()
        raise


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


def _write_masked_sample(reader, stem: str, output_dir: Path, fmt: str, con) -> None:
    """Write one sample's streamed masked reads under output_dir, atomically (via
    a `.partial` sibling renamed into place) and chmod 0600. Both formats stream
    the Arrow `reader` (bounded memory, no full materialization):

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
            # (ROW_GROUP_SIZE_BYTES, the qiita-wide cap from PARQUET_OPTS) rather
            # than a fixed row count, so wide rows don't produce oversized groups;
            # batch.nbytes is the in-memory size DuckDB's byte cap also measures.
            writer = pq.ParquetWriter(partial, reader.schema, compression="zstd")
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
    elif fmt == "fastq":
        paired, stream = _peek_paired(reader)
        con.register("masked", stream)
        if paired:
            # `{ORIENTATION}` expands to R1/R2, so the one COPY emits both
            # `<stem>.R1.fastq.gz.partial` and `<stem>.R2.fastq.gz.partial`.
            target = output_dir / f"{stem}.{{ORIENTATION}}.fastq.gz.partial"
            pairs = [
                (
                    output_dir / f"{stem}.{o}.fastq.gz.partial",
                    output_dir / f"{stem}.{o}.fastq.gz",
                )
                for o in ("R1", "R2")
            ]
        else:
            target = output_dir / f"{stem}.fastq.gz.partial"
            pairs = [(target, output_dir / f"{stem}.fastq.gz")]
        _commit_partials(
            lambda: con.execute(
                f"COPY (SELECT {_READ_MASKED_COLUMNS} FROM masked) "
                f"TO '{_sql_str(target)}' (FORMAT FASTQ, COMPRESSION 'gzip')"
            ),
            pairs,
        )
    else:
        raise ValueError(f"unsupported export format: {fmt!r}")


def _export_stem(sample: dict, run_idx, pool_idx) -> str:
    """Per-sample output filename stem, single-sourced so the export loop and the
    fastq overwrite pre-scan can't drift: ``<accession>.<run>.<pool>.<prep_sample>``."""
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


def _handle_masked_read_export(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """Export every (non-retired) sample on a sequenced_pool's masked reads to
    per-sample files. system_admin only (admin:masked_read_export).

    GETs the roster manifest, then for each sample mints a just-in-time DoGet
    ticket and streams its read_masked rows from the data plane straight to disk
    (parquet via a pyarrow ParquetWriter; fastq.gz via one shared miint DuckDB
    connection reused across every sample) — so a large pool never buffers in
    memory or on an intermediate disk hop. Per-sample writes are atomic and 0600.

    Fails loudly (exit 1, nothing written) if any sample lacks a usable
    biosample_accession (missing — not yet NCBI-submitted — or outside the safe
    charset), since the filename requires it; validated up front so one odd
    sample can't leave a partial export.

    Re-export is idempotent for parquet: a sample whose output file already exists
    is skipped when its record count matches the data plane's (nothing changed)
    and overwritten when the counts differ (reads added/removed since). fastq has
    no cheap on-disk count, so an existing fastq target is refused up front rather
    than re-exported or overwritten.

    The output directory is created if missing (parents included).
    """
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
        manifest = _common.call(
            "GET", args.base_url, token, manifest_path, params={"mask_idx": args.mask_idx}
        )
    except httpx.HTTPStatusError as exc:
        print(f"http error {exc.response.status_code}: {exc.response.text}", file=sys.stderr)
        return 1

    samples = manifest["samples"]
    run_idx = manifest["sequencing_run_idx"]
    pool_idx = manifest["sequenced_pool_idx"]
    if not samples:
        print(
            f"no samples on sequenced_pool {pool_idx} for mask_idx {args.mask_idx}; "
            "nothing to export"
        )
        return 0

    # Validate every accession up front so an unsubmitted/odd sample fails the
    # whole export before any download — never a partial output set.
    bad = sorted(
        s["prep_sample_idx"]
        for s in samples
        if not s["biosample_accession"] or not _SAFE_ACCESSION.match(s["biosample_accession"])
    )
    if bad:
        print(
            f"error: {len(bad)} sample(s) on sequenced_pool {pool_idx} have no usable "
            f"biosample_accession (missing or outside [A-Za-z0-9._-]): prep_sample_idx {bad}. "
            "The export filename requires the accession — submit/repair these samples first.",
            file=sys.stderr,
        )
        return 1

    # fastq output is never overwritten: a gzipped fastq has no cheap footer to
    # count (unlike parquet), and a paired sample's two files make a count-vs-disk
    # compare ambiguous. So for fastq, refuse up front if any target name already
    # exists — fail loudly before streaming rather than clobber a prior export.
    # (parquet re-export is decided per-sample by the count probe in the loop.)
    if args.format == "fastq":
        existing = []
        for s in samples:
            stem = _export_stem(s, run_idx, pool_idx)
            existing += [
                p
                for name in (f"{stem}.fastq.gz", f"{stem}.R1.fastq.gz", f"{stem}.R2.fastq.gz")
                if (p := output_dir / name).exists()
            ]
        if existing:
            print(
                f"error: refusing to overwrite {len(existing)} existing fastq file(s) in "
                f"{output_dir}: {[str(p) for p in existing]}. Delete them to re-export "
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
    read_opts = (
        flight.FlightCallOptions(
            read_options=ipc.IpcReadOptions(ensure_alignment=ipc.Alignment.DataTypeSpecific)
        )
        if args.format == "fastq"
        else None
    )
    # The fastq writer needs a miint DuckDB connection; open it once and reuse it
    # across all samples (each sample re-registers the `masked` view) rather than
    # paying a fresh connect + extension LOAD per sample. Parquet needs no DuckDB.
    con = connect_with_miint() if args.format == "fastq" else None
    flight_client = flight.FlightClient(args.data_plane_url)
    exported = 0
    skipped = 0
    try:
        for s in samples:
            prep = s["prep_sample_idx"]
            stem = _export_stem(s, run_idx, pool_idx)
            ticket_resp = _common.call(
                "POST",
                args.base_url,
                token,
                ticket_path,
                json={"prep_sample_idx": prep, "mask_idx": args.mask_idx},
            )
            ticket_bytes = base64.b64decode(ticket_resp["ticket"])
            # Idempotent parquet re-export: only probe the data plane's count when
            # the output file already exists (a first export pays nothing), then
            # skip on a match (nothing changed) and fall through to overwrite on a
            # mismatch. fastq took the all-or-nothing existence check above, so it
            # always exports here.
            if args.format == "parquet":
                target = output_dir / f"{stem}.parquet"
                if target.exists() and _parquet_row_count(target) == _count_masked(
                    flight_client, ticket_bytes
                ):
                    skipped += 1
                    continue
            reader = flight_client.do_get(flight.Ticket(ticket_bytes), read_opts).to_reader()
            _write_masked_sample(reader, stem, output_dir, args.format, con)
            exported += 1
    except httpx.HTTPStatusError as exc:
        print(f"http error {exc.response.status_code}: {exc.response.text}", file=sys.stderr)
        return 1
    finally:
        flight_client.close()
        if con is not None:
            con.close()

    summary = f"exported {exported} sample(s)"
    if skipped:
        summary += f" (skipped {skipped} already up to date)"
    print(f"{summary} from sequenced_pool {pool_idx} to {output_dir}")
    return 0
