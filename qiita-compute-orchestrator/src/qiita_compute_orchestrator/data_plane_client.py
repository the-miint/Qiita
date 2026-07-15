"""Data-plane DoGet stream helpers for native jobs.

The two-step path a native job uses to pull rows from the data plane over Arrow
Flight instead of reading staging Parquet — a reference build job pulling a
shard's sequences, or the feature-table job pulling an alignment slice / the
per-feature lengths:

1. a `fetch_*_doget_ticket` call — a CO→CP call (compute service-account PAT) to a
   mint route, returning a signed, scoped DoGet ticket. Runs at job RUNTIME (not
   delivered at submit) because tickets have a short TTL and a job can run long
   after submit.
2. `open_doget_stream` — a CO→DP Arrow Flight DoGet against `data_plane_url`,
   streaming the ticket's rows into a DuckDB relation the caller reads from.

`_open_ticket_stream` composes the two (mint then stream) for the public
`open_*_stream` seams below; those differ only in their mint call.

Lives outside `jobs/` deliberately: the boot scan validates every `jobs/` module
as a native-job contract (exactly `Inputs` + `execute`), and these are shared
helpers, not a job. `pyarrow.flight` is imported inline (matches every other
Flight caller — the CP runner and the admin CLI — and keeps it off the module
import path for jobs that never stream).
"""

from __future__ import annotations

import base64
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from contextlib import asynccontextmanager, contextmanager
from typing import TYPE_CHECKING

from qiita_common.api_paths import URL_ALIGNMENT_DOGET, URL_REFERENCE_DOGET

from .config import get_settings
from .cp_client import make_cp_client

if TYPE_CHECKING:
    import duckdb
    import httpx


async def fetch_reference_doget_ticket(
    *,
    http: httpx.AsyncClient,
    reference_idx: int,
    table: str,
    feature_idx: list[int] | None = None,
) -> bytes:
    """POST /reference/{idx}/ticket/doget and return the raw signed ticket bytes.

    `http` is the authed httpx client (Bearer with the compute SA PAT,
    base_url = the CP) from `cp_client.make_cp_client()`. Mirrors
    `sequence_range.mint_sequence_range`'s transport shape.

    `feature_idx` scopes the ticket to a subset (a shard's roster); omit it
    (None) for a whole-reference ticket. An empty list is rejected by the CP
    (422) — pass None for whole-reference, never `[]`.

    The CP returns the ticket base64-encoded; this decodes it to the raw bytes
    `open_doget_stream` wraps in a `flight.Ticket`. Raises
    `httpx.HTTPStatusError` on any non-2xx (404 missing reference, 409 wrong
    status, 403 missing scope, 5xx) — the caller maps it to a BackendFailure.
    """
    body: dict[str, object] = {"table": table}
    if feature_idx is not None:
        body["feature_idx"] = feature_idx
    resp = await http.post(
        URL_REFERENCE_DOGET.format(reference_idx=reference_idx),
        json=body,
    )
    resp.raise_for_status()
    return base64.b64decode(resp.json()["ticket"])


@contextmanager
def open_doget_stream(
    conn: duckdb.DuckDBPyConnection,
    *,
    data_plane_url: str,
    ticket_bytes: bytes,
    relation: str = "reference_chunks",
) -> Iterator[str]:
    """Stream a signed DoGet ticket into `conn` as `relation`.

    Ticket-generic: it wraps whatever table the signed ticket authorizes into an
    Arrow reader, so it backs the chunk stream (`open_reference_chunk_stream`), the
    whole-reference metadata stream (`open_reference_sequences_stream`), and the
    alignment-slice stream (`open_alignment_stream`) alike. Yields the registered
    relation name; the caller runs its reassembly/materialization query inside the
    `with` block (`SELECT feature_idx, string_agg(chunk_data, '' ORDER BY chunk_index)
    ...` for chunks; a plain `CREATE TABLE ... AS SELECT` for the flat
    alignment/metadata tables). The Arrow stream is pulled lazily by DuckDB as the
    query scans `relation`, so rows are never buffered in Python — the whole point of
    streaming rather than the CP runner's `read_all()` buffer form.

    The FlightClient and stream stay open for the body's duration (they back the
    lazily-consumed reader); both are torn down and the relation unregistered on
    exit. `ticket_bytes` is the raw signed ticket from a `fetch_*_doget_ticket` call
    (already base64-decoded).
    """
    import pyarrow.flight as flight  # noqa: PLC0415

    client = flight.FlightClient(data_plane_url)
    try:
        reader = client.do_get(flight.Ticket(ticket_bytes)).to_reader()
        conn.register(relation, reader)
        try:
            yield relation
        finally:
            conn.unregister(relation)
    finally:
        client.close()


@asynccontextmanager
async def _open_ticket_stream(
    conn: duckdb.DuckDBPyConnection,
    *,
    mint: Callable[[httpx.AsyncClient], Awaitable[bytes]],
    relation: str,
) -> AsyncIterator[str]:
    """Compose the two seams every public opener shares: mint a signed DoGet ticket
    (CO→CP) then stream it (CO→DP Flight) into `conn` as `relation`.

    `mint` is called with an open, authed CP client and returns the raw signed ticket
    bytes — the only thing the openers differ in. The CP client is closed as soon as
    the ticket is minted (nothing in the body calls the CP); only the Flight
    client/stream stays open for the body's duration (pulled lazily by DuckDB as the
    body's query scans `relation`), torn down on exit. `data_plane_url` resolves from
    `get_settings()` (the lifespan-installed value on the service, the propagated
    `DATA_PLANE_URL` on a compute node).
    """
    async with make_cp_client() as http:
        ticket = await mint(http)
    with open_doget_stream(
        conn,
        data_plane_url=get_settings().data_plane_url,
        ticket_bytes=ticket,
        relation=relation,
    ) as rel:
        yield rel


@asynccontextmanager
async def open_reference_chunk_stream(
    conn: duckdb.DuckDBPyConnection,
    *,
    reference_idx: int,
    feature_idx: list[int] | None,
    relation: str = "reference_chunks",
) -> AsyncIterator[str]:
    """Compose the two seams into one: mint a `feature_idx`-scoped DoGet
    ticket (CO→CP) and stream that roster's `reference_sequence_chunks` rows
    (CO→DP Flight) into `conn` as `relation`, yielding the registered relation
    name for the caller to reassemble from inside the `async with` body.

    This is the seam a shard builder imports and monkeypatches in tests. The
    underlying seams (`fetch_reference_doget_ticket`, `open_doget_stream`) stay
    separate — and the composition rides the shared `_open_ticket_stream` — so the
    integration test can bypass the CP hop and sign a ticket directly against the
    fixture data plane's HMAC secret, calling `open_doget_stream` itself.

    `feature_idx` scopes the ticket to a shard's roster; pass None only for a
    whole-reference stream (never `[]` — the CP rejects it).
    """

    async def _mint(http: httpx.AsyncClient) -> bytes:
        return await fetch_reference_doget_ticket(
            http=http,
            reference_idx=reference_idx,
            table="reference_sequence_chunks",
            feature_idx=feature_idx,
        )

    async with _open_ticket_stream(conn, mint=_mint, relation=relation) as rel:
        yield rel


async def fetch_alignment_doget_ticket(
    *,
    http: httpx.AsyncClient,
    work_ticket_idx: int,
) -> bytes:
    """POST /alignment/ticket/doget and return the raw signed ticket bytes.

    Unlike `fetch_reference_doget_ticket` (which takes `table` + `feature_idx`),
    the alignment mint route takes ONLY `work_ticket_idx`: the CP reads the
    `alignment_idx` and the `prep_sample_idx` cohort from that work ticket's
    `action_context` (set at plan time) and signs the scoped `alignment` ticket
    itself, keeping the potentially large cohort CP-side rather than on the wire.
    Called at job RUNTIME (short-TTL ticket; a SLURM queue can outlive a
    submit-time ticket), same rationale as the reference-chunk mint.

    `http` is the authed httpx client (Bearer with the compute SA PAT, base_url =
    the CP) from `cp_client.make_cp_client()`. The CP returns the ticket
    base64-encoded; this decodes it to the raw bytes `open_doget_stream` wraps in a
    `flight.Ticket`. Raises `httpx.HTTPStatusError` on any non-2xx (404 missing
    ticket, 422 absent/invalid feature-table scope, 403 missing scope, 5xx) — the
    caller maps it to a BackendFailure.
    """
    resp = await http.post(URL_ALIGNMENT_DOGET, json={"work_ticket_idx": work_ticket_idx})
    resp.raise_for_status()
    return base64.b64decode(resp.json()["ticket"])


@asynccontextmanager
async def open_alignment_stream(
    conn: duckdb.DuckDBPyConnection,
    *,
    work_ticket_idx: int,
    relation: str = "alignment",
) -> AsyncIterator[str]:
    """Mint a work-ticket-scoped `alignment` DoGet ticket (CO→CP) and stream that
    alignment run's rows (CO→DP Flight) into `conn` as `relation`, yielding the
    registered relation name for the caller to materialize from inside the
    `async with` body.

    The alignment DoGet is projected DP-side to the six columns the feature-table
    recipe needs — `prep_sample_idx, sequence_idx, feature_idx, flags, position,
    stop_position` — so the caller sees exactly those. The caller MATERIALIZES the
    stream to a real non-temp TABLE (`woltka_ogu` resolves its source on a separate
    connection → a registered view is invisible there; see docs/duckdb-miint.md),
    which also drains the stream so the Flight client can close before the compute.

    Rides the shared `_open_ticket_stream`, so (like `open_reference_chunk_stream`)
    the CP client is closed as soon as the ticket is minted; only the Flight
    client/stream stays open for the body's duration. `data_plane_url` resolves from
    `get_settings()`.
    """

    async def _mint(http: httpx.AsyncClient) -> bytes:
        return await fetch_alignment_doget_ticket(http=http, work_ticket_idx=work_ticket_idx)

    async with _open_ticket_stream(conn, mint=_mint, relation=relation) as rel:
        yield rel


@asynccontextmanager
async def open_reference_sequences_stream(
    conn: duckdb.DuckDBPyConnection,
    *,
    reference_idx: int,
    relation: str = "reference_lengths",
) -> AsyncIterator[str]:
    """Mint a WHOLE-reference `reference_sequences` DoGet ticket (CO→CP) and stream
    its per-feature metadata rows (`feature_idx, sequence_hash,
    sequence_length_bp`) into `conn` as `relation`, yielding the registered
    relation name.

    The feature-table job reads `(feature_idx, sequence_length_bp)` from it to
    build per-genome length denominators for `genome_coverage`. Whole-reference
    (`feature_idx=None`) on purpose: the coverage denominator is the FULL genome
    length, so every contig's length is needed — including contigs with no
    alignment in the cohort. Rides the shared `_open_ticket_stream` (CP client
    closed once the ticket is minted; only the Flight stream stays open for the
    body).
    """

    async def _mint(http: httpx.AsyncClient) -> bytes:
        return await fetch_reference_doget_ticket(
            http=http,
            reference_idx=reference_idx,
            table="reference_sequences",
            feature_idx=None,
        )

    async with _open_ticket_stream(conn, mint=_mint, relation=relation) as rel:
        yield rel
