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
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::RecordBatch;
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
