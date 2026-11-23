"""A data-plane DoGet stream checked against the data plane's count of its rows.

A stream that ends early without an error looks to its consumer like a complete one.
Every DoGet a service, a job or a CLI opens therefore asks the data plane's
`count_doget` DoAction first, which counts the rows a DoGet of the same signed ticket
streams, and fails with `StreamRowCountMismatch` when the stream delivered a different
number.

`open_counted_stream` / `counted_doget` open a counted DoGet; `counted_stream` /
`counted_reader` wrap a reader already open; and `read_all_counted` is the counted form of
`do_get(...).read_all()`. The `*_stream` forms yield a `CountedStream`, whose
`verify()` a consumer that commits a file calls before the commit; the others yield
the reader and check when the block returns.
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING

if TYPE_CHECKING:
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


class CountedStream:
    """A stream's reader, wrapped so the rows taken from it are counted, and the
    check of that count against `expected_rows`. `name` identifies the stream in the
    error and the log.

    `verify()` raises `StreamRowCountMismatch` when the stream did not deliver
    `expected_rows` rows. The rows counted are those the consumer took plus any it
    left, which `verify()` drains: a consumer need not read every row (a join whose
    other side is empty may stop scanning this one), and what is checked is that the
    stream was whole. `require_drained` also refuses a consumer that left rows unread,
    for one whose output depends on having seen every one. A consumer that commits a
    file calls `verify()` before the commit, so a short stream leaves no file in place.
    """

    def __init__(
        self,
        reader: pa.RecordBatchReader,
        *,
        expected_rows: int,
        name: str,
        require_drained: bool = False,
    ):
        import pyarrow as pa  # noqa: PLC0415

        self._source = reader
        self._expected_rows = expected_rows
        self._name = name
        self._require_drained = require_drained
        self._verified = False
        self._delivered = 0
        # How long the consumer left the stream unread: from opening to its first
        # ask, and from each batch handed over to its next ask. Waiting on the data
        # plane is not counted; see `send_timeout` in deploy/nginx/qiita.conf for why
        # it matters.
        self._opened = time.monotonic()
        self._first_ask: float | None = None
        self._longest_hold = 0.0
        self.reader = pa.RecordBatchReader.from_batches(reader.schema, self._batches())

    def _batches(self) -> Iterator[pa.RecordBatch]:
        self._first_ask = time.monotonic() - self._opened
        for batch in self._source:
            self._delivered += batch.num_rows
            handed = time.monotonic()
            yield batch
            self._longest_hold = max(self._longest_hold, time.monotonic() - handed)

    def log_timing(self) -> None:
        _LOG.info(
            "stream %s: %d rows read; first batch asked for %s; "
            "longest hold of a batch before asking for the next %.1f s",
            self._name,
            self._delivered,
            "never"
            if self._first_ask is None
            else f"{self._first_ask:.1f} s after the stream opened",
            self._longest_hold,
        )

    def verify(self) -> None:
        if self._verified:
            return
        self._verified = True
        unread = sum(batch.num_rows for batch in self._source)
        delivered = self._delivered
        if delivered + unread != self._expected_rows:
            raise StreamRowCountMismatch(
                f"the stream {self._name!r} delivered {delivered + unread} rows, but the "
                f"data plane counted {self._expected_rows} for the same ticket: the "
                "stream ended early, or the rows changed between the count and the stream"
            )
        if self._require_drained and unread:
            raise StreamRowCountMismatch(
                f"the consumer read {delivered} of the {self._expected_rows} rows "
                f"streamed as {self._name!r}; it must read every row"
            )


@contextmanager
def counted_stream(
    reader: pa.RecordBatchReader,
    *,
    expected_rows: int,
    name: str,
    require_drained: bool = False,
) -> Iterator[CountedStream]:
    """Yield a `CountedStream` over `reader`, and verify it when the body returns,
    if the body did not. An exception from the body propagates unchanged and is not
    followed by a check. The timing line is logged either way."""
    stream = CountedStream(
        reader, expected_rows=expected_rows, name=name, require_drained=require_drained
    )
    try:
        yield stream
    finally:
        stream.log_timing()
    stream.verify()


@contextmanager
def counted_reader(
    reader: pa.RecordBatchReader,
    *,
    expected_rows: int,
    name: str,
    require_drained: bool = False,
) -> Iterator[pa.RecordBatchReader]:
    """`counted_stream`, yielding only the wrapped reader: for a consumer whose
    output is complete only when the body returns."""
    with counted_stream(
        reader, expected_rows=expected_rows, name=name, require_drained=require_drained
    ) as stream:
        yield stream.reader


@contextmanager
def open_counted_stream(
    client: flight.FlightClient,
    ticket_bytes: bytes,
    *,
    name: str,
    options: flight.FlightCallOptions | None = None,
    require_drained: bool = False,
) -> Iterator[CountedStream]:
    """Ask `count_doget` for the ticket's rows, open its DoGet, and yield the
    stream as a `counted_stream`."""
    import pyarrow.flight as flight  # noqa: PLC0415

    expected_rows = count_doget(client, ticket_bytes)
    ticket = flight.Ticket(ticket_bytes)
    response = client.do_get(ticket) if options is None else client.do_get(ticket, options)
    with counted_stream(
        response.to_reader(),
        expected_rows=expected_rows,
        name=name,
        require_drained=require_drained,
    ) as stream:
        yield stream


@contextmanager
def counted_doget(
    client: flight.FlightClient,
    ticket_bytes: bytes,
    *,
    name: str,
    options: flight.FlightCallOptions | None = None,
    require_drained: bool = False,
) -> Iterator[pa.RecordBatchReader]:
    """`open_counted_stream`, yielding only the wrapped reader."""
    with open_counted_stream(
        client, ticket_bytes, name=name, options=options, require_drained=require_drained
    ) as stream:
        yield stream.reader


def read_all_counted(client: flight.FlightClient, ticket_bytes: bytes, *, name: str) -> pa.Table:
    """`client.do_get(ticket).read_all()`, under `counted_doget`'s check."""
    with counted_doget(client, ticket_bytes, name=name) as reader:
        return reader.read_all()
