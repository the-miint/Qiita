"""`qiita-admin reference load` — drive a reference-add workflow end-to-end.

What the subcommand does, in order, against a running CP + DP:

  1. POST /reference (or skip if --reference-idx was supplied).
  2. For each input file (FASTA required, taxonomy/tree/jplace/genome_map
     optional):
       a. Convert the file to an upload-shape Parquet (see Arrow conversion
          below). The Parquet lands in a tmpdir under workspace.
       b. POST /upload to mint an upload slot — returns upload_idx +
          signed DoPut Flight ticket.
       c. pyarrow.flight do_put — streams the Parquet to the data plane,
          which writes `{root}/uploads/{upload_idx}/upload.parquet` and
          returns a PutResult body with sha256 / row_count / bytes_received.
       d. POST /upload/{idx}/done — descriptive claim of the data plane's
          sha256 / row_count / bytes_received. Transitions pending → ready.
  3. POST /work-ticket with `action_context = {fasta_upload_idx: N, ...}`
     for the reference-add action. The CP fires the runner in the
     background; the runner resolves upload handles, walks the workflow,
     and transitions ready → consumed on success.
  4. (--watch, default) Poll GET /work-ticket/{idx} until terminal,
     printing state transitions; (--no-watch) print the work_ticket_idx
     and exit so the caller can poll externally.

**Arrow conversion** matches what `hash_sequences` and `reference_load`
expect to read on disk:

  - FASTA   → ``SELECT read_id, sequence1 AS sequence FROM read_fastx(?)``
              via duckdb-miint, streamed to a Parquet under tmp.
  - Taxonomy Parquet (read_id+taxonomy schema)   → passthrough copy.
  - Newick tree → single-row ``(newick_bytes BLOB)``.
  - jplace JSON → single-row ``(jplace_bytes BLOB)``.
  - genome_map Parquet → passthrough copy.

Client does NOT canonicalize sequences — that happens server-side inside
hash_sequences. Hashes the client forwards on /done are descriptive only.

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
import json
import logging
import os
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, Any

import httpx
from qiita_common.api_paths import (
    URL_REFERENCE_PREFIX,
    URL_UPLOAD_DONE,
    URL_UPLOAD_PREFIX,
    URL_WORK_TICKET_BY_IDX,
    URL_WORK_TICKET_PREFIX,
)
from qiita_common.auth_constants import BEARER_PREFIX

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
# Arrow conversion
# =============================================================================
#
# Each helper writes an `upload.parquet`-shaped file under `workspace`
# matching what the corresponding native job will read. The DoPut handler
# is schema-agnostic, so any Arrow stream works as long as the consumer
# step's read_parquet call sees the columns it expects.


def _ensure_workspace(workspace: Path) -> None:
    workspace.mkdir(parents=True, exist_ok=True)


def _fasta_to_upload_parquet(fasta_path: Path, workspace: Path) -> Path:
    """FASTA → `(read_id VARCHAR, sequence VARCHAR)` Parquet via miint's
    `read_fastx`. miint emits `sequence1` natively (the FASTQ paired-read
    convention); the SELECT aliases it to `sequence` so hash_sequences'
    `SELECT upper(sequence) FROM read_parquet(?)` picks up the right
    column. preserve_insertion_order is left at default — read order
    isn't load-bearing here; hash_sequences computes its own sort.

    Reuses `MIINT_EXTENSION_REPO` (the orchestrator's flag) when set so
    the team mirror's unsigned binaries are picked up here too."""
    import duckdb

    _ensure_workspace(workspace)
    out = workspace / "fasta_upload.parquet"
    mirror = os.environ.get("MIINT_EXTENSION_REPO")
    config = {"allow_unsigned_extensions": "true"} if mirror else {}
    with duckdb.connect(":memory:", config=config) as conn:
        if mirror:
            conn.execute(f"FORCE INSTALL miint FROM '{mirror}';")
        else:
            conn.execute("INSTALL miint FROM community;")
        conn.execute("LOAD miint;")
        conn.execute(
            "COPY (SELECT read_id, sequence1 AS sequence "
            "FROM read_fastx(?)) "
            f"TO '{out}' (FORMAT PARQUET)",
            [str(fasta_path)],
        )
    return out


def _passthrough_parquet_copy(src: Path, workspace: Path, label: str) -> Path:
    """Re-emit a source Parquet under a fixed name. We re-write rather
    than shipping the original bytes to (a) sanity-check the file parses
    as Parquet on the CLI side before paying for a DoPut round-trip, and
    (b) normalize Parquet version / compression to the canonical form
    upload consumers expect (snappy intermediates are fine here — the
    server-side `register-files` pass writes the final zstd-compressed
    DuckLake-side Parquet)."""
    import duckdb

    _ensure_workspace(workspace)
    out = workspace / f"{label}_upload.parquet"
    with duckdb.connect(":memory:") as conn:
        conn.execute(
            f"COPY (SELECT * FROM read_parquet(?)) TO '{out}' (FORMAT PARQUET)", [str(src)]
        )
    return out


def _blob_to_single_row_parquet(
    src: Path, workspace: Path, *, label: str, column_name: str
) -> Path:
    """Newick / jplace / any opaque text file → single-row Parquet with
    one BLOB column. The server-side native job reads the column via
    `read_*` (read_newick / read_jplace) which expects an on-disk
    filesystem path, not a BLOB — so this representation is provisional:
    the orchestrator step writes the BLOB back out to its workspace
    before invoking the parser (see `_write_phylogeny` and
    `_write_placements` in `reference_load`). A follow-up will switch to
    reading the BLOB inline once miint's `read_newick` accepts an
    in-memory string."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    _ensure_workspace(workspace)
    out = workspace / f"{label}_upload.parquet"
    blob = src.read_bytes()
    table = pa.table({column_name: [blob]}, schema=pa.schema([(column_name, pa.binary())]))
    pq.write_table(table, out)
    return out


_ROLE_CONVERTERS: dict[str, Callable[[Path, Path], Path]] = {
    "fasta": _fasta_to_upload_parquet,
    "taxonomy": lambda src, ws: _passthrough_parquet_copy(src, ws, "taxonomy"),
    "tree": lambda src, ws: _blob_to_single_row_parquet(
        src, ws, label="tree", column_name="newick_bytes"
    ),
    "jplace": lambda src, ws: _blob_to_single_row_parquet(
        src, ws, label="jplace", column_name="jplace_bytes"
    ),
    "genome_map": lambda src, ws: _passthrough_parquet_copy(src, ws, "genome_map"),
}


def _convert_for_role(file_path: Path, role: str, workspace: Path) -> Path:
    """Dispatch on role to the right Arrow-conversion helper. Ensures
    `workspace` exists so helpers can assume it; the top-level entry
    point also creates it but standalone callers (tests, future custom
    pipelines) may not."""
    workspace.mkdir(parents=True, exist_ok=True)
    try:
        return _ROLE_CONVERTERS[role](file_path, workspace)
    except KeyError as exc:
        raise ValueError(f"unknown upload role: {role!r}") from exc


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


def _do_put_sync(
    flight_client: flight.FlightClient,
    ticket_bytes: bytes,
    upload_parquet_path: Path,
) -> dict[str, Any]:
    """Stream `upload_parquet_path` to the data plane via DoPut. Runs
    synchronously (pyarrow.flight is a sync API); the async caller wraps
    in `asyncio.to_thread` so the event loop stays free.

    Returns the PutResult body the data plane wrote on the metadata
    side — `{upload_idx, sha256, row_count, bytes_received}`."""
    import pyarrow.flight as flight
    import pyarrow.parquet as pq

    descriptor = flight.FlightDescriptor.for_command(ticket_bytes)
    # iter_batches keeps memory bounded — a multi-GB FASTA Parquet never
    # materialises in the CLI process. The pyarrow.flight writer
    # back-pressures on the wire side.
    pf = pq.ParquetFile(upload_parquet_path)
    writer, reader = flight_client.do_put(descriptor, pf.schema_arrow)
    try:
        for batch in pf.iter_batches():
            writer.write_batch(batch)
        writer.done_writing()
        put_metadata = reader.read()
    finally:
        writer.close()
    if put_metadata is None:
        raise RuntimeError(f"data plane returned no PutResult for upload at {upload_parquet_path}")
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
    workspace: Path,
    description: str | None = None,
) -> UploadResult:
    """Convert + DoPut + /done a single input file. Returns the upload
    metadata the caller stitches into `action_context`."""
    import asyncio

    upload_parquet = _convert_for_role(file_path, role, workspace)

    create_body = {"description": description or f"{role}: {file_path.name}"}
    create = await _post(http, token, URL_UPLOAD_PREFIX, body=create_body, expected_status=(201,))
    upload_idx = create["upload_idx"]
    ticket_bytes = base64.b64decode(create["doput_ticket"])

    put_body = await asyncio.to_thread(_do_put_sync, flight_client, ticket_bytes, upload_parquet)
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
    flight_client: flight.FlightClient,
    fasta_path: Path,
    name: str | None = None,
    version: str | None = None,
    kind: str | None = "sequence_reference",
    reference_idx: int | None = None,
    taxonomy_path: Path | None = None,
    tree_path: Path | None = None,
    jplace_path: Path | None = None,
    genome_map_path: Path | None = None,
    workspace: Path | None = None,
    watch: bool = True,
    poll_interval_seconds: float = DEFAULT_POLL_INTERVAL_SECONDS,
    timeout_seconds: float = DEFAULT_POLL_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    """Programmatic entry point. Returns a dict with `reference_idx`,
    `work_ticket_idx`, `upload_idxs`, and (when watch=True) the final
    work_ticket body. Tests call this directly with injected clients.

    Exactly one of `reference_idx` or (`name` + `version`) must be set —
    the latter creates a new reference, the former binds to an existing
    one. `kind` is consumed only when creating."""
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

    if reference_idx is None:
        create = await _post(
            http,
            token,
            URL_REFERENCE_PREFIX,
            body={"name": name, "version": version, "kind": kind or "sequence_reference"},
            expected_status=(201,),
        )
        reference_idx = create["reference_idx"]
        _log.info("created reference %d (%s, %s)", reference_idx, name, version)

    if workspace is None:
        # Tests pin workspace via tmp_path; production runs use a tmpdir
        # the system reclaims on exit. The Parquets we write here are
        # transient — only the server-side bytes (under upload_staging_root)
        # persist past this call.
        ctx = TemporaryDirectory(prefix="qiita-ref-load-")
        workspace = Path(ctx.name)
    else:
        ctx = None
    try:
        workspace.mkdir(parents=True, exist_ok=True)

        action_context: dict[str, int] = {}
        upload_idxs: dict[str, int] = {}

        # Upload sequentially. Concurrent DoPuts would be faster on a
        # fast link, but reference-add inputs are typically dominated by
        # the FASTA; parallelizing taxonomy/tree/jplace saves seconds at
        # the cost of a much harder-to-debug failure mode if one upload
        # fails mid-stream.
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
                workspace=workspace,
            )
            action_context[f"{role}_upload_idx"] = res.upload_idx
            upload_idxs[role] = res.upload_idx
            _log.info("uploaded %s as upload_idx=%d", role, res.upload_idx)

        submit_body = {
            "action_id": "reference-add",
            "action_version": "1.0.0",
            "scope_target": {"kind": "reference", "reference_idx": reference_idx},
            "action_context": action_context,
        }
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
    finally:
        if ctx is not None:
            ctx.cleanup()
