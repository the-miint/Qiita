"""qiita user CLI — reference list / load subcommands.

Split out of the former single-file ``cli.user`` module; behavior unchanged.
"""

import argparse
import asyncio
import base64
import contextlib
import json
import os
import sys
from pathlib import Path
from typing import Any

from qiita_common.api_paths import (
    PATH_REFERENCE_DOGET,
    PATH_REFERENCE_GENOME_MEMBER,
    PATH_REFERENCE_INDEX,
    PATH_REFERENCE_PREFIX,
)
from qiita_common.models import (
    ReferenceStatus,
    WorkTicketState,
)

from .. import _common

# The reference_sequence_chunks columns the DoGet stream carries. The FASTA
# writer reassembles chunk_data ordered by chunk_index per feature_idx.
_CHUNKS_TABLE = "reference_sequence_chunks"


def _handle_reference_list(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """List references with their built index types, filtered by --host /
    --active / --index-type — discover the idx for --host-rype-reference-idx etc.

    Index types come from a separate endpoint (GET /reference/{idx}/index), so
    each listed reference costs one extra GET — fine for a human discovery
    command. With --index-type, references lacking that built index are dropped,
    so the result is exactly the set submit-host-filter-pool's readiness gate
    (`_assert_host_reference_ready`) accepts.

    That `/index` endpoint requires the `reference:read` scope (it exposes
    `fs_path`), so — unlike `prep-protocol list` — this command needs a token
    carrying that scope; a token without it fails on the first per-row call."""
    params: dict[str, str] = {}
    if args.host:
        params["is_host"] = "true"
    if args.active:
        params["status"] = ReferenceStatus.ACTIVE.value

    def _fetch(token: str) -> list:
        references = _common.call(
            "GET", args.base_url, token, PATH_REFERENCE_PREFIX, params=params or None
        )
        enriched: list = []
        for reference in references:
            idx = reference["reference_idx"]
            indexes = _common.call(
                "GET",
                args.base_url,
                token,
                f"{PATH_REFERENCE_PREFIX}{PATH_REFERENCE_INDEX.format(reference_idx=idx)}",
            )
            index_types = sorted({row["index_type"] for row in indexes})
            if args.index_type and args.index_type not in index_types:
                continue
            enriched.append({**reference, "index_types": index_types})
        return enriched

    return _common.run_http_subcommand(_fetch)


async def _run_reference_load(
    *,
    base_url: str,
    token: str,
    data_plane_url: str | None,
    args: argparse.Namespace,
) -> dict:
    """Construct real httpx + (for remote ingest) pyarrow.flight clients and
    drive `do_reference_load`. Lives next to the CLI handler so the handler
    stays a thin argparse → entry-point shim; the entry point itself
    (in cli.reference_load) takes injected clients so tests bypass this
    function entirely.

    Under `--local` no bytes cross the wire, so no Flight client is built and
    `--data-plane-url` is not needed; the by-path manifest + companions ride in
    action_context. The remote path requires `--data-plane-url`."""
    import httpx as _httpx

    from ..reference_load import do_reference_load

    # Shared keyword args for both ingest modes.
    common_kwargs: dict[str, Any] = dict(
        token=token,
        local=args.local,
        fasta_path=args.fasta,
        fasta_manifest_path=args.fasta_manifest,
        name=args.name,
        version=args.version,
        kind=args.kind,
        host=args.host,
        shard_index=args.shard_index,
        reference_idx=args.reference_idx,
        taxonomy_path=args.taxonomy,
        tree_path=args.tree,
        jplace_path=args.jplace,
        genome_map_path=args.genome_map,
        gff_path=args.gff,
        build_rype=not args.no_rype_index,
        build_minimap2=not args.no_minimap2_index,
        build_bowtie2=not args.no_bowtie2_index,
        rype_w=args.rype_w,
        minimap2_preset=args.minimap2_preset,
        watch=not args.no_watch,
        poll_interval_seconds=args.poll_interval,
        timeout_seconds=args.timeout,
        mem_gb=args.mem_gb,
    )

    if args.local:
        async with _httpx.AsyncClient(
            base_url=base_url, timeout=_common.CLI_HTTP_TIMEOUT_SECONDS
        ) as http:
            return await do_reference_load(http=http, flight_client=None, **common_kwargs)

    if not data_plane_url:
        raise ValueError("--data-plane-url is required for remote ingest (or use --local)")

    import pyarrow.flight as flight

    flight_client = flight.FlightClient(data_plane_url)
    try:
        async with _httpx.AsyncClient(
            base_url=base_url, timeout=_common.CLI_HTTP_TIMEOUT_SECONDS
        ) as http:
            return await do_reference_load(http=http, flight_client=flight_client, **common_kwargs)
    finally:
        flight_client.close()


def _handle_reference_load(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """Entry point for `qiita reference load`. Reads the PAT, builds
    a real httpx + flight client, and calls `do_reference_load`. Maps
    every known failure shape to exit 1 with a one-line stderr message —
    no silent retry, no buried traceback. Terminal work_ticket=failed
    also exits 1 so callers wrapping this in a Makefile / CI step get
    the build break."""
    import httpx as _httpx
    import pyarrow.flight as _flight

    try:
        token = _common.read_token()
    except RuntimeError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    try:
        result = asyncio.run(
            _run_reference_load(
                base_url=args.base_url,
                token=token,
                data_plane_url=args.data_plane_url,
                args=args,
            )
        )
    except _httpx.HTTPStatusError as exc:
        print(
            f"http error {exc.response.status_code}: {exc.response.text}",
            file=sys.stderr,
        )
        return 1
    except _flight.FlightError as exc:
        # Catch the gRPC-level error explicitly so the operator sees a
        # formatted error line instead of a raw traceback. FlightError is
        # NOT a RuntimeError subclass, so the catch-all below would miss
        # it. Common shapes: network refused, expired ticket, DP
        # rejected the stream mid-write.
        print(f"flight error: {exc}", file=sys.stderr)
        return 1
    except (RuntimeError, ValueError, TimeoutError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    # Under --watch, anything but a COMPLETED ticket surfaces as exit 1, not exit
    # 0 — a CI step wrapping this CLI must distinguish a successful reference
    # build from one that didn't produce a reference. The JSON body still goes to
    # stdout so the caller can see the failure_reason.
    #
    # Tested against COMPLETED rather than against a list of bad states: `no_data`
    # is a terminal outcome that builds nothing, and a positive list of failures
    # would let it — and every future state — exit 0 as a success.
    #
    # `state` is absent under --no-watch (we returned before the ticket reached an
    # outcome), and that is not a failure: the caller polls it themselves.
    work_ticket = result.get("work_ticket") or {}
    final_state = work_ticket.get("state")
    print(json.dumps(_serializable(result), indent=2))
    if final_state is not None and final_state != WorkTicketState.COMPLETED.value:
        return 1
    return 0


def _serializable(obj):
    """Recursively replace Pydantic / Path values with their JSON form so
    `json.dumps` succeeds on the result dict (which carries upload-idx
    metadata + the final work_ticket body)."""
    from pathlib import Path as _Path

    if isinstance(obj, dict):
        return {k: _serializable(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_serializable(v) for v in obj]
    if isinstance(obj, _Path):
        return str(obj)
    return obj


# ---------------------------------------------------------------------------
# `qiita reference export` — pull a genome's sequences to FASTA.gz or Parquet
# ---------------------------------------------------------------------------

_EXPORT_FORMATS = ("fasta", "parquet")


def _sql_str(path: Path) -> str:
    """Escape a filesystem path for inlining as a DuckDB SQL string literal."""
    return str(path).replace("'", "''")


def _resolve_genome_members(base_url: str, token: str, *, reference_idx: int, genome_idx: int):
    """GET a genome's member features (feature_idx + the reference's accession)
    within one reference, via the Phase-3 resolver route. Returns the list of
    `{feature_idx, accession}` (404 → HTTPStatusError, surfaced by the caller)."""
    path = (
        f"{PATH_REFERENCE_PREFIX}"
        f"{PATH_REFERENCE_GENOME_MEMBER.format(reference_idx=reference_idx, genome_idx=genome_idx)}"
    )
    return _common.call("GET", base_url, token, path)


def _create_chunks_doget_ticket(
    base_url: str, token: str, *, reference_idx: int, feature_idxs: list[int]
) -> bytes:
    """POST a DoGet ticket scoped to this reference + feature set for the
    reference_sequence_chunks table, returning the decoded signed ticket bytes."""
    path = f"{PATH_REFERENCE_PREFIX}{PATH_REFERENCE_DOGET.format(reference_idx=reference_idx)}"
    resp = _common.call(
        "POST",
        base_url,
        token,
        path,
        json={"table": _CHUNKS_TABLE, "feature_idx": feature_idxs},
    )
    return base64.b64decode(resp["ticket"])


def _atomic_write(target: Path, write_fn) -> None:
    """Run `write_fn(partial)` then atomically rename the partial into place; on
    any failure remove the partial so a retry never finds a half-written file. No
    restrictive chmod — reference sequences are public reference data, unlike the
    privacy-masked read export."""
    partial = target.parent / f"{target.name}.partial"
    try:
        write_fn(partial)
        os.replace(partial, target)
    except BaseException:
        with contextlib.suppress(FileNotFoundError):
            partial.unlink()
        raise


def _write_genome_parquet(reader, target: Path) -> None:
    """Stream the DoGet chunk reader straight to a zstd Parquet (the raw
    `reference_sequence_chunks` rows: feature_idx, chunk_index, chunk_data). No
    DuckDB hop, so the bulk sequence bytes are never materialized into DuckDB
    vectors. Batches are buffered up to one row group's worth of bytes so a
    genome-scale export doesn't fragment into tiny row groups (mirrors the
    masked-read export's parquet writer). A zero-row stream still writes a valid
    empty Parquet from the reader's schema."""
    import pyarrow as pa  # noqa: PLC0415
    import pyarrow.parquet as pq  # noqa: PLC0415
    from qiita_common.parquet import ROW_GROUP_SIZE_BYTES  # noqa: PLC0415

    def _write(partial: Path) -> None:
        writer = pq.ParquetWriter(partial, reader.schema, compression="zstd")
        try:
            buffer: list = []
            buffered_bytes = 0
            for batch in reader:
                buffer.append(batch)
                buffered_bytes += batch.nbytes
                if buffered_bytes >= ROW_GROUP_SIZE_BYTES:
                    writer.write_table(pa.Table.from_batches(buffer, reader.schema))
                    buffer = []
                    buffered_bytes = 0
            if buffer:
                writer.write_table(pa.Table.from_batches(buffer, reader.schema))
        finally:
            writer.close()

    _atomic_write(target, _write)


def _write_genome_fasta(reader, accession_map: dict[int, str | None], target: Path, con) -> None:
    """Reassemble each feature's sequence from the DoGet chunk stream and write a
    gzipped FASTA via miint's FORMAT FASTA writer, using the reference's accession
    as each record header. `con` is a shared miint DuckDB connection.

    The header (accession) is carried as DATA (a registered column), never inlined
    into the COPY SQL — so it presents no SQL-injection surface; only the output
    path is inlined (escaped via `_sql_str`). A feature with a NULL accession (a
    non-FASTA ingest, or a pre-accession-column row) falls back to a
    `feature_<idx>` header so the FASTA stays valid and traceable.

    Strand caveat: chunk bytes are stored as the ORIGINAL submitted strand, never
    normalized — but the feature_idx dedup is by CANONICAL hash (a sequence and
    its reverse complement collapse to one feature_idx), and only the
    representative record's chunks survive. So a reverse-complement-equal input
    exports the representative record's original strand, which may be the other
    orientation than the one a given accession was submitted with."""
    import pyarrow as pa  # noqa: PLC0415

    con.register("chunks", reader)
    acc_tbl = pa.table(
        {
            "feature_idx": pa.array(list(accession_map.keys()), pa.int64()),
            "accession": pa.array([accession_map[k] for k in accession_map], pa.string()),
        }
    )
    con.register("accession_map", acc_tbl)
    try:

        def _write(partial: Path) -> None:
            con.execute(
                "COPY (SELECT coalesce(m.accession, 'feature_' || c.feature_idx) AS read_id,"
                "        string_agg(c.chunk_data, '' ORDER BY c.chunk_index) AS sequence1"
                "   FROM chunks c JOIN accession_map m USING (feature_idx)"
                "  GROUP BY c.feature_idx, m.accession"
                "  ORDER BY c.feature_idx)"
                f" TO '{_sql_str(partial)}' (FORMAT FASTA, COMPRESSION 'gzip')"
            )

        _atomic_write(target, _write)
    finally:
        con.unregister("chunks")
        con.unregister("accession_map")


def _export_one_genome(
    *,
    base_url: str,
    token: str,
    flight_client,
    con,
    reference_idx: int,
    genome_idx: int,
    fmt: str,
    output_dir: Path,
) -> Path:
    """Resolve one genome's members, DoGet its sequence chunks, and write the
    requested format to `<reference_idx>.<genome_idx>.{fasta.gz|parquet}` under
    output_dir. Returns the written path. A feature shared across genomes (a
    plasmid) is included for each of its genomes — the many-to-many payoff."""
    import pyarrow.flight as flight  # noqa: PLC0415

    members = _resolve_genome_members(
        base_url, token, reference_idx=reference_idx, genome_idx=genome_idx
    )
    feature_idxs = [m["feature_idx"] for m in members]
    ticket = _create_chunks_doget_ticket(
        base_url, token, reference_idx=reference_idx, feature_idxs=feature_idxs
    )
    ticket_obj = flight.Ticket(ticket)
    if fmt == "parquet":
        # Parquet streams straight to a ParquetWriter (no Acero), keeping the bulk
        # chunk buffers zero-copy — so no buffer realignment is needed.
        target = output_dir / f"{reference_idx}.{genome_idx}.parquet"
        _write_genome_parquet(flight_client.do_get(ticket_obj).to_reader(), target)
    else:
        # The FASTA path registers the reader into DuckDB (miint FORMAT FASTA via
        # Acero), so ask the Flight reader to realign each buffer on receive —
        # otherwise Acero logs a "poorly aligned input buffer" warning per column
        # per batch (apache/arrow#37195), matching the masked-read export's fastq path.
        import pyarrow.ipc as ipc  # noqa: PLC0415

        read_opts = flight.FlightCallOptions(
            read_options=ipc.IpcReadOptions(ensure_alignment=ipc.Alignment.DataTypeSpecific)
        )
        target = output_dir / f"{reference_idx}.{genome_idx}.fasta.gz"
        accession_map = {m["feature_idx"]: m["accession"] for m in members}
        _write_genome_fasta(
            flight_client.do_get(ticket_obj, read_opts).to_reader(), accession_map, target, con
        )
    return target


def _handle_reference_genome_export(
    args: argparse.Namespace, parser: argparse.ArgumentParser
) -> int:
    """Entry point for `qiita reference export`. For each `--genome-idx`, resolves
    the genome's members, DoGets its sequence chunks from the data plane, and
    writes one file per genome (FASTA.gz or Parquet). Requires `reference:read`
    (the member route requires it and the reference DoGet ticket route accepts it —
    reference sequences are public reference data). Fails loudly (exit 1) on the
    first HTTP / Flight error, never a silent skip; genomes written before the
    failure remain on disk (each file is written atomically, so every one present
    is complete)."""
    import pyarrow.flight as flight  # noqa: PLC0415

    from ...miint import connect_with_miint

    try:
        token = _common.read_token()
    except RuntimeError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    output_dir: Path = args.output_dir
    try:
        output_dir.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        print(f"error: could not create output directory {output_dir}: {exc}", file=sys.stderr)
        return 2

    # The FASTA writer needs a miint DuckDB connection; open it once and reuse it
    # across every genome. Parquet streams straight to a ParquetWriter (no miint).
    con = connect_with_miint() if args.format == "fasta" else None
    flight_client = flight.FlightClient(args.data_plane_url)
    written: list[Path] = []
    try:
        for genome_idx in args.genome_idx:
            written.append(
                _export_one_genome(
                    base_url=args.base_url,
                    token=token,
                    flight_client=flight_client,
                    con=con,
                    reference_idx=args.reference_idx,
                    genome_idx=genome_idx,
                    fmt=args.format,
                    output_dir=output_dir,
                )
            )
    except _common.httpx.HTTPStatusError as exc:
        print(f"http error {exc.response.status_code}: {exc.response.text}", file=sys.stderr)
        return 1
    except flight.FlightError as exc:
        print(f"flight error: {exc}", file=sys.stderr)
        return 1
    finally:
        flight_client.close()
        if con is not None:
            con.close()

    print(f"exported {len(written)} genome(s) to {output_dir}: {[str(p) for p in written]}")
    return 0
