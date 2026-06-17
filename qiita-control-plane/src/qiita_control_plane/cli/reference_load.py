"""`qiita reference load` — drive a reference-add workflow end-to-end.

What the subcommand does, in order, against a running CP + DP:

  1. POST /reference (or skip if --reference-idx was supplied).
  2. For each input file (FASTA required, taxonomy/tree/jplace/genome_map
     optional):
       a. Open an Arrow RecordBatch stream over the source file (see
          "Arrow streaming" below). No intermediate Parquet is written
          to local disk; batches go straight to DoPut.
       b. POST /upload to mint an upload slot — returns upload_idx +
          signed DoPut Flight ticket.
       c. pyarrow.flight do_put — streams the Arrow batches to the data
          plane, which writes `{root}/uploads/{upload_idx}/upload.parquet`
          and returns a PutResult body with sha256 / row_count /
          bytes_received.
       d. POST /upload/{idx}/done — descriptive claim of the data plane's
          sha256 / row_count / bytes_received. Transitions pending → ready.
  3. POST /work-ticket with `action_context = {fasta_upload_idx: N, ...}`
     for the reference-add action. The CP fires the runner in the
     background; the runner resolves upload handles, walks the workflow,
     and transitions ready → consumed on success.
  4. (--watch, default) Poll GET /work-ticket/{idx} until terminal,
     printing state transitions; (--no-watch) print the work_ticket_idx
     and exit so the caller can poll externally.

**Local ingest (`--local`).** When the FASTA files already reside on the
compute host (e.g. ~100 human genomes ≈ 300 GB), streaming bytes over Flight is
wasteful — `--local --fasta-manifest PATH` ingests by path instead. Step 2's
upload loop is skipped entirely (no DoPut, no /upload slots); the manifest and
any companions ride in `action_context` as raw absolute `*_path` keys, and the
`local-(host-)reference-add` action is submitted. Its first step
(`stage_local_fasta`) reads the manifest on the host and stages the files into
the same chunked Parquet the remote path produces, so everything downstream is
identical. No `--data-plane-url` is needed. `--fasta` and `--fasta-manifest`
are mutually exclusive.

**Arrow streaming** (per role, remote path only):

  - FASTA: miint ``read_fastx`` + miint ``sequence_split`` (``UNNEST``)
           64 KB chunking, streamed via ``to_arrow_reader``. Upload
           schema: ``(read_id, chunk_index, chunk_data)``. Bounded memory
           regardless of record size — ``max_batch_bytes`` caps the read
           batch so a multi-MB GG2 genome record (~21 MB) streams as many
           small chunks instead of a single multi-MB Parquet cell. No
           sequence bytes pass through Python; see `_fasta_upload_stream`.
  - Newick / jplace: Python ``read(64 KB)`` loop over the source file.
           Upload schema: ``(chunk_index, chunk_data BLOB)``. Bounded
           memory regardless of file size — GG2 phylogeny (~407 MB) and
           jplace files (multi-GB) stream chunk-by-chunk.
  - Taxonomy / genome_map Parquet: ``pq.ParquetFile.iter_batches()``
           passthrough — these are already row-shaped data.

All chunked uploads batch at CHUNK_ROW_GROUP_SIZE = 16384 rows (shared
``qiita_common.chunking``): ~1 GB per batch at 64 KB chunks.

Client does NOT canonicalize sequences or hash anything — that happens
server-side inside hash_sequences. The client never reconstructs a full
sequence; chunks go on the wire as-uploaded.

**Failure UX.** Mid-upload network drops surface as `httpx.HTTPStatusError`
or `pyarrow.flight.FlightError` — no silent retry. The caller sees the
specific failure and decides whether to restart. Already-uploaded slots
that didn't complete /done stay at status='pending' and age out via a
future cleanup sweep; they cannot be reused because the slot's DoPut
ticket is one-shot — the data plane refuses a second write to the same
upload_idx.
"""

from __future__ import annotations

import base64
import contextlib
import json
import logging
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

import httpx
from qiita_common.api_paths import (
    URL_REFERENCE_BY_IDX,
    URL_REFERENCE_PREFIX,
    URL_UPLOAD_DONE,
    URL_UPLOAD_PREFIX,
    URL_WORK_TICKET_BY_IDX,
    URL_WORK_TICKET_PREFIX,
)
from qiita_common.auth_constants import BEARER_PREFIX
from qiita_common.chunking import CHUNK_ROW_GROUP_SIZE, CHUNK_SIZE, sequence_split_expr

if TYPE_CHECKING:
    import pyarrow.flight as flight

_log = logging.getLogger(__name__)

# Terminal work_ticket states the CLI's --watch loop stops on.
_TERMINAL_WORK_TICKET_STATES = frozenset({"completed", "failed"})

# Default poll cadence + ceiling for --watch. Two-second poll keeps the
# CLI feeling responsive without hammering /work-ticket/{idx} on a slow
# (multi-hour) reference build. 24 h ceiling matches the YAML's
# action_ceiling.walltime upper bound for reference-add.
DEFAULT_POLL_INTERVAL_SECONDS = 2.0
DEFAULT_POLL_TIMEOUT_SECONDS = 24 * 3600

# Action identifiers the CLI submits, by host-ness and ingest mode. Pinned
# against the on-disk workflow YAML in tests/test_actions_loader.py so an
# id/version drift fails at build time instead of 404ing at submit. All four
# workflows ship as version 1.0.0.
#
# Remote (DoPut upload) vs local (by-path, --local) are distinct actions
# because their first step and context_schema differ: remote resolves a
# `fasta_upload_idx` handle; local's stage_local_fasta reads a
# `fasta_manifest_path` and stages many host-resident FASTA files. Everything
# downstream of the first step is identical.
_REFERENCE_ADD_ACTION_ID = "reference-add"
_HOST_REFERENCE_ADD_ACTION_ID = "host-reference-add"
_LOCAL_REFERENCE_ADD_ACTION_ID = "local-reference-add"
_LOCAL_HOST_REFERENCE_ADD_ACTION_ID = "local-host-reference-add"
_REFERENCE_ADD_ACTION_VERSION = "1.0.0"


@dataclass(frozen=True, slots=True)
class UploadResult:
    """One row's worth of upload metadata, returned to the caller for
    audit / logging. The work-ticket dispatch step keys off `upload_idx`;
    `sha256` / `row_count` / `bytes_received` are descriptive."""

    upload_idx: int
    sha256: str
    row_count: int
    bytes_received: int


# =============================================================================
# Arrow streaming
# =============================================================================
#
# Each role exposes a context manager yielding an `UploadStream`: an
# Arrow `Schema` plus an iterator of `RecordBatch`. The caller pipes the
# batches straight into the DoPut writer — no intermediate Parquet hits
# the local filesystem. The DoPut handler is schema-agnostic, so each
# role picks the shape its server-side consumer expects to read.

# Chunked-upload tuning. The 64 KB chunk size is the shared
# qiita_common.chunking.CHUNK_SIZE: the FASTA streamer passes it to miint's
# native `sequence_split` over read_fastx, and the blob streamer (Newick /
# jplace) reuses it for its byte reads. The per-RecordBatch row count —
# CHUNK_ROW_GROUP_SIZE (16384) — keeps each batch ~1 GB on dense data and bounds
# CLI memory regardless of source-record size (GG2 genome records run up to
# ~21 MB; backbone 16S records are ~1.5 KB).
_CHUNK_SIZE = CHUNK_SIZE
_CHUNK_ROWS_PER_BATCH = CHUNK_ROW_GROUP_SIZE

# Byte budget per read_fastx batch. Caps the read-side vector so a run of
# multi-MB genome records can't materialise a giant batch before `sequence_split`
# runs — the read-side memory lever (the DuckDB result streams to the
# DoPut writer one bounded RecordBatch at a time via to_arrow_reader).
_READ_FASTX_MAX_BATCH_BYTES = "64MB"


@dataclass(frozen=True)
class UploadStream:
    """Schema + iterator of RecordBatches the DoPut writer consumes.
    The schema is needed up-front to open the DoPut writer; the
    iterator may pull from a still-open file or DuckDB connection,
    which is why this lives behind a context manager."""

    schema: Any  # pyarrow.Schema, but kept Any to avoid an eager import
    batches: Iterator[Any]  # Iterator[pyarrow.RecordBatch]


@contextlib.contextmanager
def _fasta_upload_stream(fasta_path: Path) -> Iterator[UploadStream]:
    """FASTA → `(read_id, chunk_index, chunk_data)` chunked stream via miint.

    miint's `read_fastx` parses FASTA natively (gz-transparent; `read_id` is
    the header's first token, description dropped); the 64 KB chunking is miint's
    native `sequence_split` (`UNNEST`ed); `to_arrow_reader` hands DuckDB's
    streaming result to the DoPut writer one bounded RecordBatch at a time. No
    sequence bytes pass through Python and no intermediate Parquet hits local
    disk.

    Bounded memory regardless of record size: `max_batch_bytes` caps each
    read_fastx batch by bytes so a multi-MB GG2 genome record (~21 MB) can't
    materialise a giant vector, and the result streams (proven: emits far more
    output than the DuckDB `memory_limit` without spilling). chunks within a
    read carry their own `chunk_index`, so `hash_sequences` reconstructs by
    ORDER BY regardless of arrival order.

    The DuckDB connection is held open for the life of the `with` block — the
    reader pulls from it lazily as the DoPut writer consumes batches — and
    closed on exit.

    An empty FASTA is rejected up front with a clear message: `read_fastx`
    raises a raw "Empty file" error on a zero-record input, so pre-check with
    the shared `is_empty_sequence_file` (matching `stage_local_fasta`)."""
    from qiita_common.duckdb_miint import is_empty_sequence_file

    from qiita_control_plane.miint import connect_with_miint

    if is_empty_sequence_file(fasta_path):
        raise ValueError(f"FASTA file contains no records: {fasta_path}")

    conn = connect_with_miint()
    try:
        # miint's native `sequence_split` is the shared chunker (one definition
        # for both the CLI and the orchestrator's stage_local_fasta; see
        # qiita_common.chunking).
        reader = conn.execute(
            "SELECT read_id, c.chunk_index, c.chunk_data FROM ("
            f"  SELECT read_id, UNNEST({sequence_split_expr('sequence1')}) AS c"
            f"  FROM read_fastx(?, max_batch_bytes:='{_READ_FASTX_MAX_BATCH_BYTES}')"
            ")",
            [str(fasta_path)],
        ).to_arrow_reader(_CHUNK_ROWS_PER_BATCH)
        yield UploadStream(schema=reader.schema, batches=reader)
    finally:
        conn.close()


@contextlib.contextmanager
def _blob_upload_stream(src: Path) -> Iterator[UploadStream]:
    """Opaque binary file → `(chunk_index INT, chunk_data BLOB)` chunked
    stream. Reads `src` in 64 KB blocks and emits one Arrow batch per
    `_CHUNK_ROWS_PER_BATCH` chunks. Bounded memory even on GG2-scale
    inputs (407 MB phylogeny, multi-GB jplace). Server side stitches
    chunks back into a temp file via `_unwrap_chunks_to_temp_file`.

    Reads gzipped (`.gz`) inputs transparently — chunk_data carries the
    decompressed bytes. The server's stitched temp file is then valid
    plaintext for miint's `read_newick` / `read_jplace`, which only
    accept on-disk text/JSON. Mirrors the FASTA streamer's treatment of
    `.gz` for the same reason."""
    import gzip

    import pyarrow as pa

    schema = pa.schema(
        [
            pa.field("chunk_index", pa.int32()),
            pa.field("chunk_data", pa.binary()),
        ]
    )

    opener = gzip.open if src.suffix == ".gz" else open

    def _iter_batches() -> Iterator[Any]:
        indices: list[int] = []
        datas: list[bytes] = []
        idx = 0
        with opener(src, "rb") as f:
            while True:
                data = f.read(_CHUNK_SIZE)
                if not data:
                    break
                indices.append(idx)
                datas.append(data)
                idx += 1
                if len(indices) >= _CHUNK_ROWS_PER_BATCH:
                    yield pa.RecordBatch.from_arrays(
                        [pa.array(indices, type=pa.int32()), pa.array(datas, type=pa.binary())],
                        schema=schema,
                    )
                    indices = []
                    datas = []
        if indices:
            yield pa.RecordBatch.from_arrays(
                [pa.array(indices, type=pa.int32()), pa.array(datas, type=pa.binary())],
                schema=schema,
            )

    yield UploadStream(schema=schema, batches=_iter_batches())


@contextlib.contextmanager
def _passthrough_parquet_stream(src: Path) -> Iterator[UploadStream]:
    """Taxonomy / genome_map already arrive as Parquet — stream their
    existing row batches through unchanged. `iter_batches` is bounded by
    the source's row groups; these inputs are small (tens of MB at
    most) so default batching is fine."""
    import pyarrow.parquet as pq

    pf = pq.ParquetFile(src)
    yield UploadStream(schema=pf.schema_arrow, batches=pf.iter_batches())


_ROLE_STREAMERS: dict[str, Callable[[Path], Any]] = {
    "fasta": _fasta_upload_stream,
    "taxonomy": _passthrough_parquet_stream,
    "tree": _blob_upload_stream,
    "jplace": _blob_upload_stream,
    "genome_map": _passthrough_parquet_stream,
}


def _open_upload_stream(file_path: Path, role: str):
    """Dispatch on role to the right streaming context manager."""
    streamer = _ROLE_STREAMERS.get(role)
    if streamer is None:
        raise ValueError(f"unknown upload role: {role!r}")
    return streamer(file_path)


# =============================================================================
# HTTP + Flight helpers
# =============================================================================


def _auth_headers(token: str) -> dict[str, str]:
    return {"Authorization": f"{BEARER_PREFIX}{token}"}


async def _post(
    http: httpx.AsyncClient,
    token: str,
    url: str,
    *,
    body: dict[str, Any] | None = None,
    expected_status: Iterable[int] = (200, 201, 202),
) -> dict[str, Any]:
    """Authenticated POST that asserts the response status and returns
    the decoded JSON body. `url` is the full path including API_PREFIX
    (e.g. `URL_UPLOAD_PREFIX`)."""
    resp = await http.post(url, headers=_auth_headers(token), json=body or {})
    if resp.status_code not in expected_status:
        raise httpx.HTTPStatusError(
            f"POST {url} expected {sorted(expected_status)}, got {resp.status_code}: {resp.text}",
            request=resp.request,
            response=resp,
        )
    return resp.json()


async def _get(
    http: httpx.AsyncClient,
    token: str,
    url: str,
) -> dict[str, Any]:
    resp = await http.get(url, headers=_auth_headers(token))
    resp.raise_for_status()
    return resp.json()


def _do_put_stream(
    flight_client: flight.FlightClient,
    ticket_bytes: bytes,
    stream: UploadStream,
) -> dict[str, Any]:
    """Stream `stream.batches` to the data plane via DoPut. Runs
    synchronously (pyarrow.flight is a sync API); the async caller wraps
    in `asyncio.to_thread` so the event loop stays free.

    Returns the PutResult body the data plane wrote on the metadata
    side — `{upload_idx, sha256, row_count, bytes_received}`."""
    import pyarrow.flight as flight

    descriptor = flight.FlightDescriptor.for_command(ticket_bytes)
    writer, reader = flight_client.do_put(descriptor, stream.schema)
    try:
        for batch in stream.batches:
            writer.write_batch(batch)
        writer.done_writing()
        put_metadata = reader.read()
    finally:
        writer.close()
    if put_metadata is None:
        raise RuntimeError("data plane returned no PutResult")
    return json.loads(bytes(put_metadata).decode())


# =============================================================================
# Upload orchestration
# =============================================================================


async def upload_file(
    *,
    http: httpx.AsyncClient,
    token: str,
    flight_client: flight.FlightClient,
    file_path: Path,
    role: str,
    description: str | None = None,
) -> UploadResult:
    """Stream + DoPut + /done a single input file. Returns the upload
    metadata the caller stitches into `action_context`."""
    import asyncio

    create_body = {"description": description or f"{role}: {file_path.name}"}
    create = await _post(http, token, URL_UPLOAD_PREFIX, body=create_body, expected_status=(201,))
    upload_idx = create["upload_idx"]
    ticket_bytes = base64.b64decode(create["doput_ticket"])

    def _do_put_with_stream() -> dict[str, Any]:
        with _open_upload_stream(file_path, role) as stream:
            return _do_put_stream(flight_client, ticket_bytes, stream)

    put_body = await asyncio.to_thread(_do_put_with_stream)
    if put_body.get("upload_idx") != upload_idx:
        raise RuntimeError(
            f"data plane PutResult upload_idx={put_body.get('upload_idx')!r} does not match "
            f"minted slot {upload_idx} — likely a ticket / DP misconfiguration"
        )

    await _post(
        http,
        token,
        URL_UPLOAD_DONE.format(upload_idx=upload_idx),
        body={
            "sha256": put_body["sha256"],
            "row_count": put_body["row_count"],
            "bytes_received": put_body["bytes_received"],
        },
        expected_status=(200,),
    )

    return UploadResult(
        upload_idx=upload_idx,
        sha256=put_body["sha256"],
        row_count=put_body["row_count"],
        bytes_received=put_body["bytes_received"],
    )


async def watch_work_ticket(
    http: httpx.AsyncClient,
    token: str,
    work_ticket_idx: int,
    *,
    poll_interval_seconds: float = DEFAULT_POLL_INTERVAL_SECONDS,
    timeout_seconds: float = DEFAULT_POLL_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    """Poll until terminal. Returns the final work_ticket body. Raises
    TimeoutError after `timeout_seconds`."""
    import asyncio
    import time

    by_idx_url = URL_WORK_TICKET_BY_IDX.format(work_ticket_idx=work_ticket_idx)
    deadline = time.monotonic() + timeout_seconds
    last_state: str | None = None
    while True:
        body = await _get(http, token, by_idx_url)
        state = body.get("state")
        if state != last_state:
            _log.info("work_ticket %d state=%s", work_ticket_idx, state)
            last_state = state
        if state in _TERMINAL_WORK_TICKET_STATES:
            return body
        if time.monotonic() >= deadline:
            raise TimeoutError(
                f"work_ticket {work_ticket_idx} did not reach a terminal state "
                f"within {timeout_seconds:.0f}s (last state: {state!r})"
            )
        await asyncio.sleep(poll_interval_seconds)


async def do_reference_load(
    *,
    http: httpx.AsyncClient,
    token: str,
    flight_client: flight.FlightClient | None = None,
    fasta_path: Path | None = None,
    fasta_manifest_path: Path | None = None,
    local: bool = False,
    name: str | None = None,
    version: str | None = None,
    kind: str | None = "sequence_reference",
    host: bool = False,
    reference_idx: int | None = None,
    taxonomy_path: Path | None = None,
    tree_path: Path | None = None,
    jplace_path: Path | None = None,
    genome_map_path: Path | None = None,
    watch: bool = True,
    poll_interval_seconds: float = DEFAULT_POLL_INTERVAL_SECONDS,
    timeout_seconds: float = DEFAULT_POLL_TIMEOUT_SECONDS,
    mem_gb: int | None = None,
) -> dict[str, Any]:
    """Programmatic entry point. Returns a dict with `reference_idx`,
    `work_ticket_idx`, `upload_idxs`, and (when watch=True) the final
    work_ticket body. Tests call this directly with injected clients.

    Exactly one of `reference_idx` or (`name` + `version`) must be set —
    the latter creates a new reference, the former binds to an existing
    one. `kind` is consumed only when creating.

    **Two ingest modes, selected by `local`:**

    - Remote (`local=False`, the default): a single `fasta_path` is streamed
      over Arrow Flight DoPut alongside any companions, and the
      `(host-)reference-add` action is submitted with `*_upload_idx` handles.
      `flight_client` is required. `fasta_manifest_path` must be unset.
    - Local (`local=True`): no bytes cross the wire. `fasta_manifest_path`
      (an absolute path to a manifest of absolute FASTA paths already resident
      on the compute host) and any companions ride in `action_context` as raw
      `*_path` keys, and the `local-(host-)reference-add` action is submitted —
      its first step (`stage_local_fasta`) reads the manifest and stages the
      files. `flight_client` is unused. `fasta_path` must be unset.

    `host=True` marks the new reference `is_host=true` and routes to the
    `(local-)host-reference-add` action (which additionally builds the rype +
    minimap2 host-filter indexes consumed at host-read-filtering time). A host
    reference requires `taxonomy_path` — it's the rype mapping authority's
    source — so it's a fail-fast precondition here."""
    has_idx = reference_idx is not None
    has_name_version = name is not None and version is not None
    if has_idx and (name is not None or version is not None):
        raise ValueError(
            "exactly one of (--reference-idx) or (--name + --version) must be supplied;"
            " --reference-idx and --name/--version cannot be combined"
        )
    if not has_idx and not has_name_version:
        raise ValueError(
            "exactly one of (--reference-idx) or (--name + --version) must be supplied"
        )
    if host and taxonomy_path is None:
        raise ValueError(
            "--host requires --taxonomy: a host reference must ship a taxonomy"
            " mapping authority for the rype index build"
        )

    # FASTA source mode. --fasta (remote DoPut) and --fasta-manifest (--local
    # by-path) are mutually exclusive; exactly one applies per ingest mode.
    if local:
        if fasta_path is not None:
            raise ValueError(
                "--local ingests FASTA by path via --fasta-manifest; --fasta"
                " (a DoPut upload) cannot be combined with --local"
            )
        if fasta_manifest_path is None:
            raise ValueError(
                "--local requires --fasta-manifest: an absolute path to a manifest"
                " listing one absolute FASTA path per line"
            )
        if not fasta_manifest_path.is_absolute():
            raise ValueError(f"--fasta-manifest must be absolute, got {str(fasta_manifest_path)!r}")
        if not fasta_manifest_path.exists() or not fasta_manifest_path.is_file():
            # Do NOT hard-fail on existence. The CLI may run on a host —
            # e.g. a login node — that doesn't share the compute node's
            # filesystem view, and the manifest is read by `stage_local_fasta`
            # on the compute node, never by the CLI. A missing path here may
            # simply be invisible from here, so warn (a real typo is still
            # flagged) and proceed — consistent with the companions below,
            # which aren't existence-checked at all. If the compute node can't
            # find it either, the workflow fails loudly there.
            _log.warning(
                "--fasta-manifest %s is not visible from this host (not found or"
                " not a file). If you're running --local from a login node"
                " without the compute node's shared-FS view this is expected;"
                " otherwise check for a typo. Proceeding — the manifest is read"
                " on the compute node.",
                fasta_manifest_path,
            )
        # Companions ride as raw `*_path` strings the compute host reads, so
        # they must be absolute too — the workflow context_schema enforces
        # `pattern:"^/"` on every path key, and a relative path would otherwise
        # 422 server-side with an opaque message. Validate here for a clear,
        # boundary-local error. Existence is NOT checked for the manifest or
        # the companions: under SLURM they may live on a shared FS the CLI host
        # can't see, and none of them are read by the CLI.
        for flag, companion in (
            ("--taxonomy", taxonomy_path),
            ("--tree", tree_path),
            ("--jplace", jplace_path),
            ("--genome-map", genome_map_path),
        ):
            if companion is not None and not companion.is_absolute():
                raise ValueError(f"{flag} must be absolute under --local, got {str(companion)!r}")
    else:
        if fasta_manifest_path is not None:
            raise ValueError(
                "--fasta-manifest requires --local; the remote path uploads a"
                " single --fasta over DoPut"
            )
        if fasta_path is None:
            raise ValueError("--fasta is required (or use --local with --fasta-manifest)")
        if flight_client is None:
            raise ValueError("a Flight client is required for the remote (DoPut) ingest path")

    if reference_idx is None:
        create = await _post(
            http,
            token,
            URL_REFERENCE_PREFIX,
            body={
                "name": name,
                "version": version,
                "kind": kind or "sequence_reference",
                "is_host": host,
            },
            expected_status=(201,),
        )
        reference_idx = create["reference_idx"]
        _log.info("created reference %d (%s, %s)", reference_idx, name, version)
    elif host:
        # Binding to an existing reference with --host: is_host is write-once at
        # creation (no PATCH path), so we can't set it here. Verify the existing
        # reference is actually a host reference — otherwise host-reference-add
        # would build a rype index for a reference whose is_host=false, a silent
        # metadata/behaviour mismatch. GET only on this path; a plain bind does
        # not pay the round-trip.
        existing = await _get(http, token, URL_REFERENCE_BY_IDX.format(reference_idx=reference_idx))
        if not existing.get("is_host"):
            raise ValueError(
                f"--host was given but reference {reference_idx} has is_host=false; "
                "is_host is fixed at creation, so host-reference-add cannot run against "
                "a non-host reference. Create a new host reference with --name/--version "
                "--host, or drop --host to run reference-add against this one."
            )

    # Heterogeneous by ingest mode: remote keys map `*_upload_idx` -> int,
    # local keys map `*_path` -> str. Hence dict[str, Any] rather than the
    # remote-only dict[str, int].
    action_context: dict[str, Any] = {}
    upload_idxs: dict[str, int] = {}

    if local:
        # By-path ingest: no bytes cross the wire. The manifest and every
        # companion ride in action_context as raw absolute `*_path` strings;
        # the runner leaves these untouched (it only resolves `*_upload_idx`
        # keys), and stage_local_fasta reads the manifest on the compute host.
        action_context["fasta_manifest_path"] = str(fasta_manifest_path)
        for role, src in [
            ("taxonomy", taxonomy_path),
            ("tree", tree_path),
            ("jplace", jplace_path),
            ("genome_map", genome_map_path),
        ]:
            if src is None:
                continue
            action_context[f"{role}_path"] = str(src)
            _log.info("local %s path=%s", role, src)
    else:
        # Remote ingest: upload sequentially. Concurrent DoPuts would be faster
        # on a fast link, but reference-add inputs are typically dominated by
        # the FASTA; parallelizing taxonomy/tree/jplace saves seconds at the
        # cost of a much harder-to-debug failure mode if one upload fails
        # mid-stream.
        for role, src in [
            ("fasta", fasta_path),
            ("taxonomy", taxonomy_path),
            ("tree", tree_path),
            ("jplace", jplace_path),
            ("genome_map", genome_map_path),
        ]:
            if src is None:
                continue
            res = await upload_file(
                http=http,
                token=token,
                flight_client=flight_client,
                file_path=src,
                role=role,
            )
            action_context[f"{role}_upload_idx"] = res.upload_idx
            upload_idxs[role] = res.upload_idx
            _log.info("uploaded %s as upload_idx=%d", role, res.upload_idx)

    # Select the action by ingest mode (local vs remote) and host-ness. Host
    # references run the *-host-reference-add workflow (the base steps plus the
    # trailing rype-index build + register-index); plain references run
    # *-reference-add. The local variants prepend stage_local_fasta and take
    # raw `*_path` keys instead of `*_upload_idx` handles.
    if local:
        action_id = _LOCAL_HOST_REFERENCE_ADD_ACTION_ID if host else _LOCAL_REFERENCE_ADD_ACTION_ID
    else:
        action_id = _HOST_REFERENCE_ADD_ACTION_ID if host else _REFERENCE_ADD_ACTION_ID
    submit_body: dict[str, Any] = {
        "action_id": action_id,
        "action_version": _REFERENCE_ADD_ACTION_VERSION,
        "scope_target": {"kind": "reference", "reference_idx": reference_idx},
        "action_context": action_context,
    }
    # Optional per-run memory floor for the index-build steps (a human genome
    # OOMs the conservative YAML default). Server-gated to wet_lab_admin+ and
    # bounded by the action ceiling.
    if mem_gb is not None:
        submit_body["resource_override"] = {"mem_gb": mem_gb}
    submit = await _post(
        http,
        token,
        URL_WORK_TICKET_PREFIX,
        body=submit_body,
        expected_status=(202,),
    )
    work_ticket_idx = submit["work_ticket_idx"]
    _log.info("submitted work_ticket %d for reference %d", work_ticket_idx, reference_idx)

    result: dict[str, Any] = {
        "reference_idx": reference_idx,
        "work_ticket_idx": work_ticket_idx,
        "upload_idxs": upload_idxs,
    }

    if watch:
        final = await watch_work_ticket(
            http,
            token,
            work_ticket_idx,
            poll_interval_seconds=poll_interval_seconds,
            timeout_seconds=timeout_seconds,
        )
        result["work_ticket"] = final
    return result
