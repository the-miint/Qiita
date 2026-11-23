"""A data-plane DoGet stream checked against the data plane's count of its rows.

A stream that ends early without an error looks to its consumer like a complete one.
Every DoGet a service, a job or a CLI opens therefore asks the data plane's
`count_doget` DoAction first, which counts the rows a DoGet of the same signed ticket
streams, and fails with `StreamRowCountMismatch` when the stream delivered a different
number.

`counted_doget` is the opener; `counted_reader` wraps a reader already open;
`registered_counted` registers the wrapped reader on a DuckDB connection; and
`read_all_counted` is the counted form of `do_get(...).read_all()`.
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING

from .duckdb_relation import registered

if TYPE_CHECKING:
    import duckdb
    import pyarrow as pa
    import pyarrow.flight as flight

_LOG = logging.getLogger(__name__)


class StreamRowCountMismatch(RuntimeError):
    """A DoGet stream delivered a different number of rows than the data plane
    counted for the same ticket."""


def count_doget(client: flight.FlightClient, ticket_bytes: bytes) -> int:
    """How many rows a DoGet of `ticket_bytes` streams, from the data plane's
    `count_doget` DoAction, which takes the same signed ticket."""
    import pyarrow.flight as flight  # noqa: PLC0415

    results = list(client.do_action(flight.Action("count_doget", ticket_bytes)))
    if len(results) != 1:
        raise RuntimeError(f"count_doget returned {len(results)} results, expected 1")
    return int(json.loads(results[0].body.to_pybytes())["count"])


@contextmanager
def counted_reader(
    reader: pa.RecordBatchReader,
    *,
    expected_rows: int,
    name: str,
    require_drained: bool = False,
) -> Iterator[pa.RecordBatchReader]:
    """Yield `reader` wrapped so the rows taken from it are counted, and raise
    `StreamRowCountMismatch` when the body returns and the stream did not deliver
    `expected_rows` rows. `name` identifies the stream in the error and the log.

    The rows counted are those the body took plus any it left, which are drained after
    the body returns: a consumer need not read every row (a join whose other side is
    empty may stop scanning this one), and what is checked is that the stream was
    whole. `require_drained` also refuses a body that left rows unread, for a consumer
    whose output depends on having seen every one.

    The check runs only when the body returns: an exception from the body propagates
    unchanged. The timing line is logged either way.
    """
    import pyarrow as pa  # noqa: PLC0415

    delivered = 0
    # How long the body left the stream unread: from opening to its first ask, and
    # from each batch handed over to its next ask. Waiting on the data plane is not
    # counted; see `send_timeout` in deploy/nginx/qiita.conf for why it matters.
    opened = time.monotonic()
    first_ask: float | None = None
    longest_hold = 0.0

    def _batches() -> Iterator[pa.RecordBatch]:
        nonlocal delivered, first_ask, longest_hold
        first_ask = time.monotonic() - opened
        for batch in reader:
            delivered += batch.num_rows
            handed = time.monotonic()
            yield batch
            longest_hold = max(longest_hold, time.monotonic() - handed)

    try:
        yield pa.RecordBatchReader.from_batches(reader.schema, _batches())
    finally:
        _LOG.info(
            "stream %s: %d rows read; first batch asked for %s; "
            "longest hold of a batch before asking for the next %.1f s",
            name,
            delivered,
            "never" if first_ask is None else f"{first_ask:.1f} s after the stream opened",
            longest_hold,
        )
    unread = sum(batch.num_rows for batch in reader)
    if delivered + unread != expected_rows:
        raise StreamRowCountMismatch(
            f"the stream {name!r} delivered {delivered + unread} rows, but the data "
            f"plane counted {expected_rows} for the same ticket: the stream ended early, "
            "or the rows changed between the count and the stream"
        )
    if require_drained and unread:
        raise StreamRowCountMismatch(
            f"the job read {delivered} of the {expected_rows} rows streamed as "
            f"{name!r}; it must read every row"
        )


@contextmanager
def counted_doget(
    client: flight.FlightClient,
    ticket_bytes: bytes,
    *,
    name: str,
    options: flight.FlightCallOptions | None = None,
    require_drained: bool = False,
) -> Iterator[pa.RecordBatchReader]:
    """Ask `count_doget` for the ticket's rows, open its DoGet, and yield the
    stream's reader under `counted_reader`'s check."""
    import pyarrow.flight as flight  # noqa: PLC0415

    expected_rows = count_doget(client, ticket_bytes)
    ticket = flight.Ticket(ticket_bytes)
    stream = client.do_get(ticket) if options is None else client.do_get(ticket, options)
    with counted_reader(
        stream.to_reader(),
        expected_rows=expected_rows,
        name=name,
        require_drained=require_drained,
    ) as reader:
        yield reader


@contextmanager
def registered_counted(
    conn: duckdb.DuckDBPyConnection,
    relation: str,
    reader: pa.RecordBatchReader,
    *,
    expected_rows: int,
    require_drained: bool = False,
) -> Iterator[str]:
    """`registered`, over `reader` under `counted_reader`'s check."""
    with (
        counted_reader(
            reader, expected_rows=expected_rows, name=relation, require_drained=require_drained
        ) as counted,
        registered(conn, relation, counted),
    ):
        yield relation


def read_all_counted(client: flight.FlightClient, ticket_bytes: bytes, *, name: str) -> pa.Table:
    """`client.do_get(ticket).read_all()`, under `counted_doget`'s check."""
    with counted_doget(client, ticket_bytes, name=name) as reader:
        return reader.read_all()
