//! Measuring instrument for the M0 DoGet compression evaluation.
//!
//! For a given Arrow input and a given (codec, representation, transport)
//! setting: how many bytes cross the wire, and what does encode and decode
//! cost? It encodes through the same `FlightDataEncoderBuilder` the DoGet uses,
//! so a number here is a number about production, not about a model of it.
//!
//! It lives under `tests/` because it is not production code and must never be
//! reachable from the service binary. `tests/compression.rs` calibrates it
//! against synthetic inputs; the real-fixture runs consume the same functions.
//!
//! Peak memory is roughly three copies of the input (input + encoded messages +
//! decoded batches), so size fixtures accordingly.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::builder::StringDictionaryBuilder;
use arrow_array::types::{Int32Type, UInt32Type};
use arrow_array::{
    Array, ArrayRef, Int32Array, Int64Array, RecordBatch, RunArray, StringArray, StringViewArray,
};
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::encode::{DictionaryHandling, FlightDataEncoderBuilder};
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use arrow_ipc::writer::IpcWriteOptions;
use arrow_ipc::{root_as_message, CompressionType};
use arrow_schema::{Field, Schema};
use futures::{stream, Stream, StreamExt, TryStreamExt};
use prost::Message as _;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tonic::codec::CompressionEncoding;
use tonic::transport::{Endpoint, Server};
use tonic::{Request, Response, Status, Streaming};

/// The IPC body codec. Arrow's `Message.fbs` defines exactly these two, so this
/// is the complete axis — there is no third option to evaluate.
// Each test target constructs a different subset of these; cargo compiles
// the module once per target, so an unused variant is expected, not dead.
#[allow(dead_code)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IpcCodec {
    None,
    Lz4Frame,
    Zstd,
}

impl IpcCodec {
    fn compression_type(self) -> Option<CompressionType> {
        match self {
            IpcCodec::None => None,
            IpcCodec::Lz4Frame => Some(CompressionType::LZ4_FRAME),
            IpcCodec::Zstd => Some(CompressionType::ZSTD),
        }
    }
}

/// Mirrors `arrow_flight::encode::DictionaryHandling`, which is neither `Copy`
/// nor `Clone` and so cannot live in a settings struct.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Dictionaries {
    Hydrate,
    Resend,
}

impl From<Dictionaries> for DictionaryHandling {
    fn from(value: Dictionaries) -> Self {
        match value {
            Dictionaries::Hydrate => DictionaryHandling::Hydrate,
            Dictionaries::Resend => DictionaryHandling::Resend,
        }
    }
}

/// gRPC per-message compression — the transport alternative to compressing the
/// IPC body. It also covers the flatbuffer metadata that IPC body compression
/// leaves untouched.
// Each test target constructs a different subset of these; cargo compiles
// the module once per target, so an unused variant is expected, not dead.
#[allow(dead_code)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Transport {
    None,
    Gzip,
    Zstd,
}

impl Transport {
    fn encoding(self) -> Option<CompressionEncoding> {
        match self {
            Transport::None => None,
            Transport::Gzip => Some(CompressionEncoding::Gzip),
            Transport::Zstd => Some(CompressionEncoding::Zstd),
        }
    }
}

/// Everything the encoder can be told to do differently.
#[derive(Clone, Copy, Debug)]
pub struct IpcSetting {
    pub codec: IpcCodec,
    pub dictionaries: Dictionaries,
    /// `FlightDataEncoderBuilder::with_max_flight_data_size` — the batch
    /// geometry axis. Larger amortises dictionaries and compresses better;
    /// smaller starts streaming sooner.
    pub max_flight_data_size: usize,
}

impl IpcSetting {
    /// The default setting with one codec swapped in — the common case, since
    /// every measurement is relative to `IpcSetting::default()`.
    pub fn with_codec(codec: IpcCodec) -> Self {
        Self {
            codec,
            ..Self::default()
        }
    }
}

/// Bytes as MiB, for the markdown the measurement targets print.
#[allow(dead_code)]
pub fn mib(bytes: u64) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

impl Default for IpcSetting {
    /// Exactly what the data plane does today: `FlightDataEncoderBuilder::new()`
    /// with no options set. Every measurement is relative to this.
    fn default() -> Self {
        Self {
            codec: IpcCodec::None,
            dictionaries: Dictionaries::Hydrate,
            max_flight_data_size: arrow_flight::encode::GRPC_TARGET_MAX_FLIGHT_SIZE_BYTES,
        }
    }
}

/// One encode plus one decode of one input under one setting.
// Which fields a target reads depends on what it measures, and cargo compiles
// this module once per target — an unread field here is expected, not dead.
#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct Measurement {
    /// Summed protobuf-encoded length of every `FlightData` produced — schema,
    /// dictionary, and batch messages alike. This is the gRPC message payload;
    /// the per-message length prefix and the HTTP/2 framing around it belong to
    /// [`TransportMeasurement`].
    pub encoded_bytes: u64,
    pub messages: usize,
    /// Rows recovered by the decode, which [`measure_ipc`] has already checked
    /// against the input.
    pub rows: usize,
    pub encode: Duration,
    pub decode: Duration,
}

impl Measurement {
    /// Compression ratio against an uncompressed run of the same input:
    /// `baseline / self`, so above 1.0 is a win.
    pub fn ratio_over(&self, baseline: &Measurement) -> f64 {
        baseline.encoded_bytes as f64 / self.encoded_bytes as f64
    }
}

/// Where the Phase 2 fixtures live: `$QIITA_M0_FIXTURES`, else
/// `localdocs/ducklake-sampled-data/` beside the workspace.
// Cargo compiles this module once per test target, so anything only the
// `fixtures` target calls reads as dead code in the `compression` target.
#[allow(dead_code)]
pub fn fixture_dir() -> PathBuf {
    std::env::var_os("QIITA_M0_FIXTURES")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .expect("qiita-data-plane has a parent directory")
                .join("localdocs/ducklake-sampled-data")
        })
}

/// Load a Parquet fixture into `RecordBatch`es the way a DoGet would.
///
/// Through `stream_arrow`, not `query_arrow`, because that is what
/// `stream_ducklake_batches` does in production. The materialising form can
/// return the whole file as one batch; the streaming form yields DuckDB's
/// natural chunk geometry. Batch geometry changes the compression ratio, so
/// reading through the materialising path would measure a shape production
/// never emits.
///
/// A missing file is an error, never an empty result: a silently-skipped
/// fixture would report a shape as measured when nothing was measured.
pub fn load_fixture(path: &Path) -> Result<Vec<RecordBatch>, BoxError> {
    if !path.is_file() {
        return Err(format!("fixture not found: {}", path.display()).into());
    }
    let literal = path
        .to_str()
        .ok_or_else(|| format!("fixture path is not UTF-8: {}", path.display()))?;
    if literal.contains('\'') {
        return Err(format!("fixture path contains a quote: {literal}").into());
    }
    let conn = duckdb::Connection::open_in_memory()?;
    let sql = format!("SELECT * FROM read_parquet('{literal}')");

    // Same zero-row schema probe as production: streaming execution does not
    // surface the schema until a chunk is fetched, and `stream_arrow` needs it
    // up front.
    let schema = conn
        .prepare(&format!("SELECT * FROM ({sql}) AS _schema_probe LIMIT 0"))?
        .query_arrow([])?
        .get_schema();
    let mut stmt = conn.prepare(&sql)?;
    Ok(stmt.stream_arrow([], schema)?.collect())
}

/// One column of every batch, keeping the batch boundaries.
///
/// This is how a per-column measurement is taken. IPC compresses each buffer
/// independently, so a column measured alone compresses as it does in the whole
/// table — `per_column_encoded_bytes_sum_to_the_whole_table` is what holds that
/// claim to account.
pub fn project(batches: &[RecordBatch], column: &str) -> Result<Vec<RecordBatch>, BoxError> {
    let Some(first) = batches.first() else {
        return Ok(Vec::new());
    };
    let index = first
        .schema()
        .index_of(column)
        .map_err(|e| format!("no column {column:?} in fixture: {e}"))?;
    Ok(batches
        .iter()
        .map(|b| b.project(&[index]))
        .collect::<Result<_, _>>()?)
}

// --- Representation transforms (Phase 4) --------------------------------
//
// Arrow IPC has no encoding layer, so the only way to change what goes on the
// wire is to change the array type handed to the encoder. Each of these rebuilds
// one column as a different Arrow type; the calibration tests hold them to
// preserving every value, because a transform that silently dropped rows would
// make every byte count derived from it meaningless.

/// Rebuild an `Int64` column as run-end encoded (`RunArray<Int32Type>`).
///
/// The candidate for identifier columns that are constant or long-running
/// within a stream. Returns `None` for a column that is not `Int64`.
#[allow(dead_code)]
pub fn to_run_end_encoded(batch: &RecordBatch, index: usize) -> Option<RecordBatch> {
    let column = batch.column(index).as_any().downcast_ref::<Int64Array>()?;

    let mut run_ends: Vec<i32> = Vec::new();
    let mut values: Vec<Option<i64>> = Vec::new();
    for row in 0..column.len() {
        let value = column.is_valid(row).then(|| column.value(row));
        if values.last().is_none_or(|last| *last != value) {
            values.push(value);
            run_ends.push(0);
        }
        *run_ends.last_mut().expect("a run was just pushed") = row as i32 + 1;
    }

    let array = RunArray::<Int32Type>::try_new(
        &Int32Array::from(run_ends),
        &Int64Array::from(values) as &dyn Array,
    )
    .ok()?;
    rebuild(batch, index, Arc::new(array))
}

/// Rebuild a `Utf8` column as `Utf8View` — buffer sharing instead of the
/// dictionary's index indirection. Returns `None` for a non-`Utf8` column.
///
/// `gc()` compacts the builder's block slack so the measurement is about the
/// format rather than the builder. It is a small effect (~2% of capacity, and
/// IPC writes buffer *length* anyway) — deliberately not the interesting part.
///
/// **The interesting part is that a view array must not be sliced.** Views
/// reference shared data buffers, so a slice keeps *every* buffer of the
/// original; `FlightDataEncoderBuilder` splits batches to honour
/// `max_flight_data_size`, and each slice then re-serializes the whole buffer.
/// A compact 8.2 MB column measures 8.2 MB unsplit and **32.7 MB split five
/// ways**. See `string_view_conversion_does_not_inflate_the_layout` and
/// `slicing_a_string_view_array_duplicates_its_data_buffers`.
#[allow(dead_code)]
pub fn to_string_view(batch: &RecordBatch, index: usize) -> Option<RecordBatch> {
    let column = batch.column(index).as_any().downcast_ref::<StringArray>()?;
    let array: StringViewArray = column.iter().collect();
    rebuild(batch, index, Arc::new(array.gc()))
}

/// Rebuild a `Utf8` column as a dictionary-encoded one. Returns `None` for a
/// non-`Utf8` column.
#[allow(dead_code)]
pub fn to_dictionary(batch: &RecordBatch, index: usize) -> Option<RecordBatch> {
    let column = batch.column(index).as_any().downcast_ref::<StringArray>()?;
    let mut builder = StringDictionaryBuilder::<UInt32Type>::new();
    for value in column.iter() {
        builder.append_option(value);
    }
    rebuild(batch, index, Arc::new(builder.finish()))
}

/// Rebuild an `Int64` column as `Int32`.
///
/// **Errors rather than truncating.** Item 5's whole hazard is that a narrowed
/// identifier which later exceeds its range is a silent corruption, so the
/// instrument refuses out-of-range input instead of wrapping it — the same
/// principle as [`verify_codec`]. `Ok(None)` means "not an Int64 column".
#[allow(dead_code)]
pub fn to_narrowed_i32(batch: &RecordBatch, index: usize) -> Result<Option<RecordBatch>, BoxError> {
    let Some(column) = batch.column(index).as_any().downcast_ref::<Int64Array>() else {
        return Ok(None);
    };
    let mut narrowed: Vec<Option<i32>> = Vec::with_capacity(column.len());
    for row in 0..column.len() {
        if !column.is_valid(row) {
            narrowed.push(None);
            continue;
        }
        let value = column.value(row);
        narrowed.push(Some(i32::try_from(value).map_err(|_| {
            format!("{value} does not fit in i32 — narrowing would silently truncate")
        })?));
    }
    Ok(rebuild(batch, index, Arc::new(Int32Array::from(narrowed))))
}

/// Replace one column, deriving the new schema from the array's own type.
fn rebuild(batch: &RecordBatch, index: usize, array: ArrayRef) -> Option<RecordBatch> {
    let mut fields: Vec<Field> = batch
        .schema()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    fields[index] = Field::new(
        fields[index].name(),
        array.data_type().clone(),
        fields[index].is_nullable(),
    );
    let mut columns = batch.columns().to_vec();
    columns[index] = array;
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).ok()
}

/// The codec stamped into each record-batch message, in stream order. `None`
/// means the message declares no `BodyCompression`, i.e. raw buffers.
///
/// This inspects, it does not validate: schema and dictionary messages are
/// skipped because they are not record batches, and a message whose flatbuffer
/// header will not parse is skipped too. [`verify_codec`] is the guard.
pub fn record_batch_codecs(messages: &[FlightData]) -> Vec<Option<CompressionType>> {
    messages
        .iter()
        .filter_map(|m| {
            let batch = root_as_message(&m.data_header)
                .ok()?
                .header_as_record_batch()?;
            Some(batch.compression().map(|c| c.codec()))
        })
        .collect()
}

/// Fail unless every record-batch message actually used `expected`.
///
/// Arrow returns a runtime error when a codec's crate feature is missing, but
/// nothing checks that the encoder honoured the request — and a stream that
/// quietly shipped raw buffers would make every number derived from it a lie.
/// A stream carrying no record batches is not a measurement either.
pub fn verify_codec(messages: &[FlightData], expected: IpcCodec) -> Result<(), FlightError> {
    let want = expected.compression_type();
    let stamped = record_batch_codecs(messages);
    if stamped.is_empty() {
        return Err(FlightError::ProtocolError(
            "no record-batch messages in the encoded stream".into(),
        ));
    }
    if let Some(found) = stamped.iter().find(|c| **c != want) {
        return Err(FlightError::ProtocolError(format!(
            "requested {expected:?} but a batch message carries {found:?} — \
             the codec did not take effect"
        )));
    }
    Ok(())
}

/// Encode `batches` the way a DoGet would, returning every `FlightData` the
/// encoder produced and how long producing it took.
pub async fn encode(
    batches: &[RecordBatch],
    setting: IpcSetting,
) -> Result<(Vec<FlightData>, Duration), FlightError> {
    let options =
        IpcWriteOptions::default().try_with_compression(setting.codec.compression_type())?;
    // `build` wants a `'static` stream, so it gets clones — a `RecordBatch`
    // clone bumps Arcs rather than copying buffers, and it happens before the
    // clock starts, so it does not enter the measurement.
    let owned: Vec<Result<RecordBatch, FlightError>> = batches.iter().cloned().map(Ok).collect();
    let mut encoder = FlightDataEncoderBuilder::new()
        .with_options(options)
        .with_dictionary_handling(setting.dictionaries.into())
        .with_max_flight_data_size(setting.max_flight_data_size)
        .build(stream::iter(owned));

    let start = Instant::now();
    let mut produced = Vec::new();
    while let Some(message) = encoder.next().await {
        produced.push(message?);
    }
    let elapsed = start.elapsed();

    verify_codec(&produced, setting.codec)?;
    Ok((produced, elapsed))
}

/// Decode a captured stream back to record batches.
pub async fn decode(
    messages: Vec<FlightData>,
) -> Result<(Vec<RecordBatch>, Duration), FlightError> {
    let start = Instant::now();
    let batches =
        FlightRecordBatchStream::new_from_flight_data(stream::iter(messages.into_iter().map(Ok)))
            .try_collect::<Vec<_>>()
            .await?;
    Ok((batches, start.elapsed()))
}

/// Encode, then decode, reporting bytes and both timings.
pub async fn measure_ipc(
    batches: &[RecordBatch],
    setting: IpcSetting,
) -> Result<Measurement, FlightError> {
    let (messages, encode_time) = encode(batches, setting).await?;
    let encoded_bytes = messages.iter().map(|m| m.encoded_len() as u64).sum();
    let message_count = messages.len();

    let (decoded, decode_time) = decode(messages).await?;
    // The same principle as `verify_codec`: a byte count is only meaningful
    // against a stream that carried the whole input, so a round trip that lost
    // rows is refused rather than reported.
    let rows: usize = decoded.iter().map(|b| b.num_rows()).sum();
    let expected: usize = batches.iter().map(|b| b.num_rows()).sum();
    if rows != expected {
        return Err(FlightError::ProtocolError(format!(
            "round trip lost rows: encoded {expected}, decoded {rows}"
        )));
    }

    Ok(Measurement {
        encoded_bytes,
        messages: message_count,
        rows,
        encode: encode_time,
        decode: decode_time,
    })
}

/// What a real gRPC round trip of a captured stream cost.
///
/// To compare the two layers against each other, put both through here: an
/// IPC-compressed stream at `Transport::None` against an uncompressed stream at
/// `Transport::Zstd`. Comparing `wire_bytes` to [`Measurement::encoded_bytes`]
/// directly would compare different things.
// Which fields a target reads depends on what it measures, and cargo compiles
// this module once per target — an unread field here is expected, not dead.
#[allow(dead_code)]
pub struct TransportMeasurement {
    /// Server→client bytes over the socket, HTTP/2 framing and headers
    /// included — the whole point of measuring at this layer rather than
    /// summing message payloads. It covers the whole connection, so it also
    /// carries the handshake (SETTINGS and friends): a fixed overhead of order
    /// 100 bytes that only distorts a very small fixture. Counting stops when
    /// the client finishes reading, so a trailing GOAWAY may not be included.
    pub wire_bytes: u64,
    pub rows: usize,
    /// Client-side wall time, covering transport compression and decode but
    /// **not** the IPC encode — the messages arrive already encoded. Not
    /// comparable to [`Measurement::encode`], and inflated by the counting
    /// relay in the path; use it to compare transport settings to each other.
    pub elapsed: Duration,
}

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Serve `messages` over a real socket and fetch them back, counting the bytes
/// that actually crossed it.
pub async fn measure_transport(
    messages: &[FlightData],
    transport: Transport,
) -> Result<TransportMeasurement, BoxError> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let server_addr = listener.local_addr()?;
    let mut service = FlightServiceServer::new(ReplayService {
        messages: messages.to_vec(),
    });
    if let Some(encoding) = transport.encoding() {
        service = service.send_compressed(encoding);
    }
    let server: JoinHandle<()> = tokio::spawn(async move {
        let incoming = stream::unfold(listener, |listener| async move {
            let accepted = listener.accept().await.map(|(socket, _)| socket);
            Some((accepted, listener))
        });
        let _ = Server::builder()
            .add_service(service)
            .serve_with_incoming(incoming)
            .await;
    });

    let (relay_addr, counter, relay) = counting_relay(server_addr).await?;
    let result = fetch_through(relay_addr, transport).await;

    relay.abort();
    server.abort();
    let (rows, elapsed) = result?;
    Ok(TransportMeasurement {
        wire_bytes: counter.load(Ordering::Relaxed),
        rows,
        elapsed,
    })
}

async fn fetch_through(
    addr: SocketAddr,
    transport: Transport,
) -> Result<(usize, Duration), BoxError> {
    let channel = Endpoint::from_shared(format!("http://{addr}"))?
        .connect()
        .await?;
    let mut client = FlightServiceClient::new(channel);
    if let Some(encoding) = transport.encoding() {
        client = client.accept_compressed(encoding);
    }
    let start = Instant::now();
    let response = client.do_get(Ticket::new("m0")).await?.into_inner();
    let rows = FlightRecordBatchStream::new_from_flight_data(
        response.map_err(|status| FlightError::Tonic(Box::new(status))),
    )
    .try_fold(
        0usize,
        |acc, batch| async move { Ok(acc + batch.num_rows()) },
    )
    .await?;
    Ok((rows, start.elapsed()))
}

/// A byte-counting TCP relay in front of `upstream`.
///
/// tonic exposes no "bytes actually sent" hook, so the client talks to the
/// relay and the relay counts the server→client direction. Returns the address
/// to dial, the counter, and the task to abort when done.
async fn counting_relay(
    upstream: SocketAddr,
) -> Result<(SocketAddr, Arc<AtomicU64>, JoinHandle<()>), BoxError> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let counter = Arc::new(AtomicU64::new(0));

    let task_counter = counter.clone();
    let task = tokio::spawn(async move {
        while let Ok((client, _)) = listener.accept().await {
            let Ok(server) = TcpStream::connect(upstream).await else {
                return;
            };
            let (mut client_rx, mut client_tx) = client.into_split();
            let (mut server_rx, mut server_tx) = server.into_split();
            tokio::spawn(async move {
                let _ = tokio::io::copy(&mut client_rx, &mut server_tx).await;
                let _ = server_tx.shutdown().await;
            });
            let counter = task_counter.clone();
            tokio::spawn(async move {
                let mut buf = vec![0u8; 64 * 1024];
                while let Ok(n) = server_rx.read(&mut buf).await {
                    if n == 0 || client_tx.write_all(&buf[..n]).await.is_err() {
                        break;
                    }
                    counter.fetch_add(n as u64, Ordering::Relaxed);
                }
                let _ = client_tx.shutdown().await;
            });
        }
    });
    Ok((addr, counter, task))
}

/// A Flight service whose DoGet replays a captured stream. Every other method
/// is unreachable in this harness.
#[derive(Clone)]
struct ReplayService {
    messages: Vec<FlightData>,
}

type BoxStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;

#[tonic::async_trait]
impl FlightService for ReplayService {
    type HandshakeStream = BoxStream<HandshakeResponse>;
    type ListFlightsStream = BoxStream<FlightInfo>;
    type DoGetStream = BoxStream<FlightData>;
    type DoPutStream = BoxStream<PutResult>;
    type DoExchangeStream = BoxStream<FlightData>;
    type DoActionStream = BoxStream<arrow_flight::Result>;
    type ListActionsStream = BoxStream<ActionType>;

    async fn do_get(
        &self,
        _request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let messages = self.messages.clone();
        Ok(Response::new(Box::pin(stream::iter(
            messages.into_iter().map(Ok),
        ))))
    }

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("harness serves do_get only"))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("harness serves do_get only"))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("harness serves do_get only"))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("harness serves do_get only"))
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("harness serves do_get only"))
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("harness serves do_get only"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("harness serves do_get only"))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("harness serves do_get only"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("harness serves do_get only"))
    }
}
