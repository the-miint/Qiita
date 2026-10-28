//! DoGet streaming: the IPC body codec a client asks for, and the bounded
//! producer that streams a DuckLake query's batches.

use arrow_array::RecordBatch;
use arrow_flight::error::FlightError;
use arrow_ipc::CompressionType;
use duckdb::Connection;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Status;

use crate::pool::PooledConnection;

/// gRPC metadata key by which a DoGet client asks for a compressed IPC body.
///
/// Lowercase because HTTP/2 requires it of header names. **Python twin:**
/// `qiita_common.flight_constants.IPC_COMPRESSION_HEADER` — the two are a wire
/// contract and must change together.
const IPC_COMPRESSION_HEADER: &str = "qiita-ipc-compression";

/// The only codec this server will apply, and the value clients send to get it.
const IPC_COMPRESSION_ZSTD: &str = "zstd";
/// Explicitly asking for no compression — the same as sending no header, but
/// lets a client be unambiguous.
const IPC_COMPRESSION_NONE: &str = "none";

/// The IPC body codec this DoGet should use, from the client's request metadata.
///
/// **The client chooses, not the server.** Whether compression pays depends on
/// the client's bandwidth, which the server cannot know — behind nginx it cannot
/// even see the client's address. So the default is off and the client opts in
/// per call. The break-even arithmetic is in `docs/architecture/flight.md`.
///
/// An unrecognised value is an **error, not a fallback**. A client that asked
/// for compression, silently did not get it, and measured the result would draw
/// the wrong conclusion about its own transfer. `lz4` is rejected along with
/// everything else rather than served as a quietly worse stream.
pub(super) fn requested_ipc_codec(
    metadata: &tonic::metadata::MetadataMap,
) -> Result<Option<CompressionType>, Status> {
    // `get_all`, not `get`: HTTP/2 headers may repeat, and `get` returns only the
    // FIRST value — so `zstd` followed by `lz4` would apply zstd and silently
    // drop the value it does not support, which is exactly the quiet downgrade
    // this function exists to prevent. A repeated header is ambiguous about what
    // the client wanted, and ambiguity here is refused rather than resolved.
    let mut values = metadata.get_all(IPC_COMPRESSION_HEADER).iter();
    let Some(raw) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(Status::invalid_argument(format!(
            "{IPC_COMPRESSION_HEADER} was sent more than once; send exactly one \
             value ({IPC_COMPRESSION_ZSTD:?} or {IPC_COMPRESSION_NONE:?})"
        )));
    }
    let value = raw.to_str().map_err(|_| {
        Status::invalid_argument(format!(
            "{IPC_COMPRESSION_HEADER} must be valid UTF-8; accepted values are \
             {IPC_COMPRESSION_ZSTD:?} and {IPC_COMPRESSION_NONE:?}"
        ))
    })?;
    match value {
        IPC_COMPRESSION_ZSTD => Ok(Some(CompressionType::ZSTD)),
        IPC_COMPRESSION_NONE => Ok(None),
        other => Err(Status::invalid_argument(format!(
            "unsupported {IPC_COMPRESSION_HEADER}: {other:?}; accepted values are \
             {IPC_COMPRESSION_ZSTD:?} and {IPC_COMPRESSION_NONE:?}"
        ))),
    }
}

/// Bounded depth of the DoGet batch channel. Backpressure: the blocking producer
/// parks on `blocking_send` when the channel is full until the async consumer
/// drains it, so peak memory is ~this many RecordBatches in flight rather than
/// the whole result set.
const DOGET_BATCH_CHANNEL_DEPTH: usize = 4;

/// Stream a DuckLake query's RecordBatches over a bounded channel.
///
/// Memory is bounded on BOTH sides of the handoff:
///
///   * DuckDB executes in STREAMING mode (`stream_arrow` →
///     `duckdb_execute_prepared_streaming`), fetching one data chunk at a time
///     rather than computing the whole result set into memory first. This is the
///     load-bearing half: the earlier materialized `query_arrow` buffered the
///     ENTIRE result inside DuckDB before the first batch was drainable, so a
///     whole-reference DoGet (the rype router streaming every genome's
///     `chunk_data`) OOM-killed the data plane at ~374 GB. A bounded channel
///     alone could never have caught that — the blow-up was upstream of it.
///   * The `Statement` borrows the `Connection`, and the `DoGetStream` we return
///     must be `'static`, so neither can be handed out directly. A blocking task
///     owns the borrowed instance for the query's lifetime and pushes each
///     `RecordBatch` into a bounded channel (`DOGET_BATCH_CHANNEL_DEPTH`); the
///     returned stream drains the receiver, applying backpressure to the
///     streaming producer. The instance goes back to the pool when that task
///     ends — including when it ends early because the client dropped the
///     stream, which closes the channel the producer sends into.
///
/// A zero-row result still emits one empty `RecordBatch` carrying the schema, so
/// the client always receives a valid (possibly empty) Arrow table. A DuckDB
/// error — prepare, execute, or a chunk fetch after batches have already gone
/// out — surfaces as a trailing `Err` item, so a client never reads a truncated
/// result as a whole one; so does a panic (see `run_producer`). The connection is
/// already borrowed when this is called, so a pool with nothing left to lend
/// never reaches here.
pub(super) fn stream_ducklake_batches(
    conn: PooledConnection,
    sql: String,
    table: String,
) -> ReceiverStream<Result<RecordBatch, FlightError>> {
    let (tx, rx) =
        tokio::sync::mpsc::channel::<Result<RecordBatch, FlightError>>(DOGET_BATCH_CHANNEL_DEPTH);
    // JoinHandle dropped intentionally: the blocking task runs to completion
    // independently, draining into `tx`. Don't `.await`/`.join()` it — the
    // result is delivered through the channel, not the handle.
    tokio::task::spawn_blocking(move || {
        run_producer(&tx, || send_query_batches(&conn, &sql, &table, &tx))
    });
    ReceiverStream::new(rx)
}

/// Run `produce`, and send its failure to `tx` as the stream's trailing `Err` item
/// (a failed send means the consumer is already gone, so it is ignored). A panic is
/// a failure too: left to unwind, it would drop `tx` and end the stream as though
/// the result were complete.
fn run_producer(
    tx: &tokio::sync::mpsc::Sender<Result<RecordBatch, FlightError>>,
    produce: impl FnOnce() -> Result<(), Status>,
) {
    let message = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(produce)) {
        Ok(Ok(())) => return,
        Ok(Err(status)) => status.message().to_string(),
        Err(payload) => {
            let what = payload
                .downcast_ref::<&str>()
                .copied()
                .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
                .unwrap_or("a non-string payload");
            format!("data plane producer panicked: {what}")
        }
    };
    eprintln!("error: DoGet stream failed: {message}");
    let _ = tx.blocking_send(Err(FlightError::ExternalError(Box::new(
        std::io::Error::other(message),
    ))));
}

/// Execute `sql` on `conn` in streaming mode and push each `RecordBatch` into
/// `tx` — the producer half of `stream_ducklake_batches`. Returns early, without
/// error, once the receiver is gone (the client hung up).
fn send_query_batches(
    conn: &Connection,
    sql: &str,
    table: &str,
    tx: &tokio::sync::mpsc::Sender<Result<RecordBatch, FlightError>>,
) -> Result<(), Status> {
    let mut stmt = conn
        .prepare(sql)
        .map_err(|e| Status::internal(format!("query preparation failed for {table}: {e}")))?;
    // Streaming execution, never the materializing `query_arrow`; see
    // `stream_ducklake_batches` for why.
    let schema = stmt
        .stream_arrow([])
        .map_err(|e| Status::internal(format!("query execution failed for {table}: {e}")))?
        .get_schema();
    let mut produced = false;
    // Fetch with the fallible `step`, not the `ArrowStream` iterator, which
    // panics on a failed fetch: a DuckDB error is an outcome to report, not a bug.
    while let Some(chunk) = stmt.step().map_err(|e| {
        let when = if produced {
            "mid-stream"
        } else {
            "before its first batch"
        };
        Status::internal(format!("query failed {when} for {table}: {e}"))
    })? {
        produced = true;
        // Receiver dropped (client hung up) — stop early, don't error.
        if tx.blocking_send(Ok(RecordBatch::from(chunk))).is_err() {
            return Ok(());
        }
    }
    if !produced {
        // Preserve the schema for a zero-row result.
        let _ = tx.blocking_send(Ok(RecordBatch::new_empty(schema)));
    }
    Ok(())
}

#[cfg(test)]
#[path = "doget_tests.rs"]
mod tests;
