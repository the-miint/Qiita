"""Reference-chunk retrieval for native jobs.

The two-step path a native build job (aligner-subject builders, and the
eventual rype migration) uses to pull a shard's reference sequences from
the data plane instead of reading staging Parquet:

1. `fetch_reference_doget_ticket` — a CO→CP call (compute service-account PAT)
   to `POST /reference/{idx}/ticket/doget`, returning a signed, `feature_idx`-
   scoped DoGet ticket. Runs at job RUNTIME (not delivered at submit) because
   tickets have a short TTL and a build can run long after submit.
2. `stream_reference_chunks` — a CO→DP Arrow Flight DoGet against
   `data_plane_url`, streaming the `(feature_idx, chunk_index, chunk_data)`
   rows of `reference_sequence_chunks` into a DuckDB relation the caller
   reassembles from.

Lives outside `jobs/` deliberately: the boot scan validates every `jobs/`
module as a native-job contract (exactly `Inputs` + `execute`), and this is a
shared helper, not a job. `pyarrow.flight` is imported inline (matches every
other Flight caller — the CP runner and the admin CLI — and keeps it off the
module import path for jobs that never stream).
"""

from __future__ import annotations

import base64
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager, contextmanager
from typing import TYPE_CHECKING

from qiita_common.api_paths import URL_REFERENCE_DOGET

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
    `stream_reference_chunks` wraps in a `flight.Ticket`. Raises
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
def stream_reference_chunks(
    conn: duckdb.DuckDBPyConnection,
    *,
    data_plane_url: str,
    ticket_bytes: bytes,
    relation: str = "reference_chunks",
) -> Iterator[str]:
    """Stream a DoGet of `reference_sequence_chunks` into `conn` as `relation`.

    Yields the registered relation name; the caller runs its reassembly query
    (`SELECT feature_idx, string_agg(chunk_data, '' ORDER BY chunk_index) ...`)
    inside the `with` block. The Arrow stream is pulled lazily by DuckDB as the
    query scans `relation`, so rows are never buffered in Python — the whole
    point of streaming rather than the CP runner's `read_all()` buffer form.

    The FlightClient and stream stay open for the body's duration (they back the
    lazily-consumed reader); both are torn down and the relation unregistered on
    exit. `ticket_bytes` is the raw signed ticket from
    `fetch_reference_doget_ticket` (already base64-decoded).
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
async def open_reference_table_stream(
    conn: duckdb.DuckDBPyConnection,
    *,
    reference_idx: int,
    table: str,
    feature_idx: list[int] | None = None,
    relation: str,
) -> AsyncIterator[str]:
    """Mint a DoGet ticket for one reference table (CO→CP) and stream its rows
    (CO→DP Flight) into `conn` as `relation`, yielding the registered relation name.

    The generic form. `table` must be in the CP's DoGet allowlist
    (`routes/reference._DOGET_ALLOWED_TABLES`, which is pinned to the data plane's
    `ALLOWED_TABLES`) — `reference_sequence_chunks` for a shard builder's sequences,
    `reference_annotation` for the coverage job's feature windows, and so on.

    The CP client is closed as soon as the ticket is minted — nothing in the body calls
    the CP. Only the Flight client/stream stays open for the body (DuckDB pulls it lazily
    as the query scans `relation`); it is torn down and the relation unregistered on exit.

    `feature_idx` scopes the ticket to a subset (a shard's roster); pass None for the whole
    reference — never `[]`, which the CP rejects. The ticket always carries `reference_idx`,
    so a table with its own `reference_idx` column (like `reference_annotation`) is scoped
    for free, with no membership join.
    """
    async with make_cp_client() as http:
        ticket = await fetch_reference_doget_ticket(
            http=http,
            reference_idx=reference_idx,
            table=table,
            feature_idx=feature_idx,
        )
    with stream_reference_chunks(
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
    """`open_reference_table_stream` fixed to `reference_sequence_chunks`.

    Kept as its own name because it is the seam the shard builders import and
    monkeypatch in their tests; the generic form underneath is what a new consumer
    (the coverage job's annotation windows) should use.
    """
    async with open_reference_table_stream(
        conn,
        reference_idx=reference_idx,
        table="reference_sequence_chunks",
        feature_idx=feature_idx,
        relation=relation,
    ) as rel:
        yield rel
