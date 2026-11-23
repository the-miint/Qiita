"""Tests for qiita_common.flight_stream: the row-count check on a DoGet stream.

Run on a plain DuckDB connection over an in-memory reader: what is under test is the
count of rows the consumer pulls, not the transport. The Flight half (`count_doget`,
`counted_doget`) runs against an in-process Flight server below.
"""

from __future__ import annotations

import json

import pytest

import qiita_common.flight_stream as fs

# `registered_counted` — `counted_reader`'s check, registered on DuckDB. Run on
# a plain DuckDB connection over an in-memory reader: what is under test is the count
# of rows DuckDB pulls, not the transport.


def _int_reader(n: int, *, batch: int = 3):
    import pyarrow as pa  # noqa: PLC0415

    table = pa.table({"sequence_idx": pa.array(range(n), pa.int64())})
    return pa.RecordBatchReader.from_batches(table.schema, table.to_batches(max_chunksize=batch))


def test_registered_counted_passes_a_stream_drained_in_full():
    import duckdb  # noqa: PLC0415

    with duckdb.connect() as conn:
        with fs.registered_counted(conn, "r", _int_reader(7), expected_rows=7) as rel:
            conn.execute(f"CREATE TABLE t AS SELECT * FROM {rel}")
        assert conn.execute("SELECT count(*) FROM t").fetchone() == (7,)


def test_registered_counted_refuses_a_stream_that_ends_short():
    """Seven rows counted, five on the wire, and the stream ends without an error."""
    import duckdb  # noqa: PLC0415

    with duckdb.connect() as conn:
        with pytest.raises(
            fs.StreamRowCountMismatch, match="delivered 5 rows, but the data plane counted 7"
        ):
            with fs.registered_counted(conn, "r", _int_reader(5), expected_rows=7) as rel:
                conn.execute(f"CREATE TABLE t AS SELECT * FROM {rel}")


def test_registered_counted_refuses_a_stream_longer_than_counted():
    import duckdb  # noqa: PLC0415

    with duckdb.connect() as conn:
        with pytest.raises(
            fs.StreamRowCountMismatch, match="delivered 8 rows, but the data plane counted 7"
        ):
            with fs.registered_counted(conn, "r", _int_reader(8), expected_rows=7) as rel:
                conn.execute(f"CREATE TABLE t AS SELECT * FROM {rel}")


def test_registered_counted_counts_the_rows_a_body_left_unread():
    """A body need not read every row (a join whose other side is empty may stop
    scanning this one); the rest is drained and counted, so a whole stream passes."""
    import duckdb  # noqa: PLC0415

    with duckdb.connect() as conn:
        with fs.registered_counted(conn, "r", _int_reader(30_000), expected_rows=30_000) as rel:
            conn.execute(f"SELECT * FROM {rel} LIMIT 1").fetchall()


def test_registered_counted_still_refuses_a_short_stream_a_body_left_unread():
    import duckdb  # noqa: PLC0415

    with duckdb.connect() as conn:
        with pytest.raises(fs.StreamRowCountMismatch, match="delivered 29999 rows"):
            with fs.registered_counted(conn, "r", _int_reader(29_999), expected_rows=30_000) as rel:
                conn.execute(f"SELECT * FROM {rel} LIMIT 1").fetchall()


def test_registered_counted_require_drained_refuses_a_body_that_left_rows():
    import duckdb  # noqa: PLC0415

    with duckdb.connect() as conn:
        with pytest.raises(fs.StreamRowCountMismatch, match="must read every row"):
            with fs.registered_counted(
                conn, "r", _int_reader(30_000), expected_rows=30_000, require_drained=True
            ) as rel:
                conn.execute(f"SELECT * FROM {rel} LIMIT 1").fetchall()


def test_registered_counted_lets_the_body_exception_through():
    """A body that raises is not checked: its own exception reaches the caller, not a
    count mismatch over the rows it never read."""
    import duckdb  # noqa: PLC0415

    with duckdb.connect() as conn:
        with pytest.raises(ValueError, match="from the body"):
            with fs.registered_counted(conn, "r", _int_reader(7), expected_rows=7):
                raise ValueError("from the body")


def test_registered_counted_logs_how_long_the_body_left_the_stream_unread(caplog):
    """The numbers nginx's `send_timeout` has to cover, in the job's step log. The
    body here holds the stream 0.3 s before its first ask."""
    import time  # noqa: PLC0415

    import duckdb  # noqa: PLC0415

    with caplog.at_level("INFO", logger=fs.__name__), duckdb.connect() as conn:
        with fs.registered_counted(conn, "r", _int_reader(7), expected_rows=7) as rel:
            time.sleep(0.3)
            conn.execute(f"CREATE TABLE t AS SELECT * FROM {rel}")
    [line] = [r.getMessage() for r in caplog.records if r.name == fs.__name__]
    assert line.startswith("stream r: 7 rows read; first batch asked for 0.")
    assert float(line.split("asked for ")[1].split(" s")[0]) >= 0.3


def test_registered_counted_logs_the_timing_when_the_body_raises(caplog):
    """A cut stream raises in the body, which is the case the timing line is for."""
    import duckdb  # noqa: PLC0415

    with caplog.at_level("INFO", logger=fs.__name__), duckdb.connect() as conn:
        with pytest.raises(ValueError, match="cut"):
            with fs.registered_counted(conn, "r", _int_reader(7), expected_rows=7) as rel:
                conn.execute(f"CREATE TABLE t AS SELECT * FROM {rel}")
                raise ValueError("cut")
    [line] = [r.getMessage() for r in caplog.records if r.name == fs.__name__]
    assert line.startswith("stream r: 7 rows read; first batch asked for ")


def test_registered_counted_says_never_when_no_batch_was_asked_for(caplog):
    import duckdb  # noqa: PLC0415

    with caplog.at_level("INFO", logger=fs.__name__), duckdb.connect() as conn:
        with pytest.raises(ValueError):
            with fs.registered_counted(conn, "r", _int_reader(7), expected_rows=7):
                raise ValueError("before any read")
    [line] = [r.getMessage() for r in caplog.records if r.name == fs.__name__]
    assert "first batch asked for never;" in line


class _CountingFlightServer:
    """An in-process Flight server whose DoGet streams `rows` rows and whose
    `count_doget` answers `counted`."""

    def __init__(self, *, rows: int, counted: int):
        import pyarrow as pa  # noqa: PLC0415
        import pyarrow.flight as flight  # noqa: PLC0415

        table = pa.table({"sequence_idx": pa.array(range(rows), pa.int64())})
        self.actions: list[tuple[str, bytes]] = []
        outer = self

        class _Server(flight.FlightServerBase):
            def do_get(self, context, ticket):
                return flight.RecordBatchStream(table)

            def do_action(self, context, action):
                outer.actions.append((action.type, action.body.to_pybytes()))
                yield flight.Result(json.dumps({"count": counted}).encode())

        self.server = _Server("grpc://127.0.0.1:0")
        self.url = f"grpc://127.0.0.1:{self.server.port}"

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.server.shutdown()


def test_read_all_counted_returns_a_whole_stream_and_counts_under_the_same_ticket():
    import pyarrow.flight as flight  # noqa: PLC0415

    with _CountingFlightServer(rows=5000, counted=5000) as server:
        with flight.FlightClient(server.url) as client:
            table = fs.read_all_counted(client, b"signed", name="t")
        assert table.num_rows == 5000
        assert server.actions == [("count_doget", b"signed")]


def test_read_all_counted_refuses_a_short_stream():
    import pyarrow.flight as flight  # noqa: PLC0415

    with _CountingFlightServer(rows=4000, counted=5000) as server:
        with flight.FlightClient(server.url) as client:
            with pytest.raises(fs.StreamRowCountMismatch, match="delivered 4000 rows"):
                fs.read_all_counted(client, b"signed", name="t")


def test_counted_doget_checks_a_reader_a_python_writer_drains():
    import pyarrow.flight as flight  # noqa: PLC0415

    with _CountingFlightServer(rows=4000, counted=5000) as server:
        with flight.FlightClient(server.url) as client:
            with pytest.raises(fs.StreamRowCountMismatch, match="delivered 4000 rows"):
                with fs.counted_doget(client, b"signed", name="t") as reader:
                    for _ in reader:
                        pass
