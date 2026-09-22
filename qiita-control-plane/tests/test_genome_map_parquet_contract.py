"""Contract tests for the genome map's Parquet form — the properties that make
it safe to serve uncapped, pinned so they cannot quietly stop holding.

No database: these are the format's own properties. The route behaviour (the cap,
the 404, the agreement with `export_member_genome`) lives beside the JSON route's
tests, which are `db`; `tests/cli/test_fetch_binary.py` holds the transport
helper's.
"""

import pyarrow as pa
import pyarrow.ipc as ipc
import pyarrow.parquet as pq
import pytest
from qiita_common.models import GenomeMapEntry
from qiita_common.parquet import PARQUET_COMPRESSION

from qiita_control_plane.actions.library import GENOME_MAP_PARQUET_SCHEMA


def test_parquet_schema_matches_the_json_entry():
    """ACCEPTANCE: the Parquet columns are exactly `GenomeMapEntry`'s fields.

    The two forms answer the same question, so a column added to one and not the
    other gives a caller a different map depending on which they asked for.
    """
    assert tuple(GENOME_MAP_PARQUET_SCHEMA.names) == tuple(GenomeMapEntry.model_fields)


def test_a_truncated_parquet_body_is_fatal():
    """The property that lets these routes drop the cap: a short body cannot read
    back as a short map.

    Parquet keeps its footer and magic at the tail, so a transfer cut anywhere
    fails to parse.
    """
    table = pa.table(
        {
            "feature_idx": [1, 2, 3],
            "genome_idx": [1, 1, 2],
            "source": ["a"] * 3,
            "source_id": ["x", "y", "z"],
        },
        schema=GENOME_MAP_PARQUET_SCHEMA,
    )
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, compression=PARQUET_COMPRESSION)
    body = sink.getvalue().to_pybytes()

    assert pq.read_table(pa.BufferReader(pa.py_buffer(body))).num_rows == 3
    for cut in (len(body) // 2, len(body) - 8):
        with pytest.raises(pa.ArrowInvalid):
            pq.read_table(pa.BufferReader(pa.py_buffer(body[:cut])))


def test_a_truncated_arrow_ipc_stream_reads_back_short_and_silent():
    """The measurement behind serving Parquet rather than an Arrow IPC stream.

    IPC is what Flight already carries and what the client already stages through
    `_registered`, so it is where a reader would start. Three batches cut to one
    come back as a third of the rows with nothing raised, so a partial map would
    stage as complete. A cut landing mid-message does raise — the silent case
    needs the cut on a boundary, which is narrower than "any truncation" and still
    undetectable by a caller.

    A failure here means upstream changed that, which reopens the format choice.
    """

    def batch(value):
        return pa.record_batch(
            {
                "feature_idx": [value] * 10,
                "genome_idx": [value] * 10,
                "source": ["a"] * 10,
                "source_id": ["x"] * 10,
            },
            schema=GENOME_MAP_PARQUET_SCHEMA,
        )

    def stream(n):
        sink = pa.BufferOutputStream()
        writer = ipc.new_stream(sink, GENOME_MAP_PARQUET_SCHEMA)
        for i in range(n):
            writer.write_batch(batch(i))
        writer.close()
        return sink.getvalue().to_pybytes()

    full = stream(3)
    # Bytes of schema + exactly one batch: where a boundary cut falls.
    boundary = len(stream(1)) - len(_IPC_END_OF_STREAM)

    assert ipc.open_stream(pa.BufferReader(pa.py_buffer(full))).read_all().num_rows == 30
    short = ipc.open_stream(pa.BufferReader(pa.py_buffer(full[:boundary]))).read_all()
    assert short.num_rows == 10, "10 of 30 rows came back, with nothing raised"

    with pytest.raises(pa.ArrowInvalid):
        ipc.open_stream(pa.BufferReader(pa.py_buffer(full[: boundary + 5]))).read_all()


# The end-of-stream marker an IPC writer appends on close: continuation bytes plus
# a zero length. Subtracted to find where a batch boundary actually falls.
_IPC_END_OF_STREAM = b"\xff\xff\xff\xff\x00\x00\x00\x00"
