//! Arrow Flight service implementation for the qiita data plane.
//!
//! Handles DoGet requests by verifying HMAC-signed tickets, querying DuckLake,
//! and streaming results as Arrow RecordBatches.
//!
//! Each request opens its own DuckDB connection and attaches DuckLake. This
//! avoids shared mutable state and allows concurrent requests — DuckLake's
//! snapshot isolation in the shared Postgres catalog handles concurrency.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use arrow_flight::decode::{DecodedPayload, FlightDataDecoder};
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::{
    Action, ActionType, Criteria, FlightData, FlightDescriptor, FlightInfo, HandshakeRequest,
    HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use duckdb::Connection;
use futures::stream::{self, Stream, StreamExt};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::{WriterProperties, WriterVersion};
use sha2::{Digest, Sha256};
use tonic::{Request, Response, Status, Streaming};

use crate::auth;
use crate::ducklake;

/// The qiita data plane Flight service.
pub struct QiitaFlightService {
    /// HMAC secret key for ticket verification.
    hmac_secret: Vec<u8>,
    /// DuckLake catalog connection string (libpq format).
    catalog_connstr: String,
    /// Directory where DuckLake stores Parquet data files.
    data_path: String,
    /// Root for DoPut staging — uploads land at
    /// `{root}/uploads/{upload_idx}/upload.parquet`. CP and DP must agree
    /// on the layout convention (CP derives the same path on the read
    /// side); both derive it as `PATH_SCRATCH/staging`.
    upload_staging_root: PathBuf,
}

impl QiitaFlightService {
    pub fn new(
        hmac_secret: Vec<u8>,
        catalog_connstr: String,
        data_path: String,
        upload_staging_root: PathBuf,
    ) -> Self {
        Self {
            hmac_secret,
            catalog_connstr,
            data_path,
            upload_staging_root,
        }
    }

    /// Open a fresh DuckDB connection with DuckLake attached.
    /// Each request gets its own connection — no shared state.
    fn open_ducklake_conn(&self) -> Result<Connection, Status> {
        let conn = Connection::open_in_memory()
            .map_err(|e| Status::internal(format!("failed to open DuckDB: {e}")))?;
        ducklake::connect_ducklake(&conn, &self.catalog_connstr, &self.data_path)
            .map_err(|e| Status::internal(format!("failed to attach DuckLake: {e}")))?;
        Ok(conn)
    }
}

/// Canonical staging path for an upload — single source of truth shared by
/// the DoPut handler (writes here) and the control plane (reads here).
pub fn staging_path_for(root: &Path, upload_idx: i64) -> PathBuf {
    root.join("uploads")
        .join(upload_idx.to_string())
        .join("upload.parquet")
}

/// Allowed table names for DoGet queries. Reject anything else.
///
/// PRIVACY: `read` and `read_mask` are deliberately absent. Reads are only
/// reachable through the `read_masked` view, which excludes host/human and
/// QC-failed rows by construction (`WHERE m.reason = 'pass'`). Raw read access
/// is via direct DB tooling on the host, never Flight. Do not add `read` or
/// `read_mask` here.
const ALLOWED_TABLES: &[&str] = &[
    "reference_sequences",
    "reference_sequence_chunks",
    "reference_taxonomy",
    "reference_phylogeny",
    "reference_placements",
    "read_masked",
];

/// Allowed column names for filter clauses. All identifier columns that can
/// appear in a signed ticket's filter. Whitelist prevents information leakage
/// via error messages for non-existent columns.
const ALLOWED_FILTER_COLUMNS: &[&str] = &[
    "feature_idx",
    "reference_idx",
    "node_index",
    "mask_idx",
    "prep_sample_idx",
];

#[tonic::async_trait]
impl FlightService for QiitaFlightService {
    type HandshakeStream =
        Pin<Box<dyn futures::Stream<Item = Result<HandshakeResponse, Status>> + Send>>;
    type ListFlightsStream =
        Pin<Box<dyn futures::Stream<Item = Result<FlightInfo, Status>> + Send>>;
    type DoGetStream = Pin<Box<dyn futures::Stream<Item = Result<FlightData, Status>> + Send>>;
    type DoPutStream = Pin<Box<dyn futures::Stream<Item = Result<PutResult, Status>> + Send>>;
    type DoExchangeStream = Pin<Box<dyn futures::Stream<Item = Result<FlightData, Status>> + Send>>;
    type DoActionStream =
        Pin<Box<dyn futures::Stream<Item = Result<arrow_flight::Result, Status>> + Send>>;
    type ListActionsStream =
        Pin<Box<dyn futures::Stream<Item = Result<ActionType, Status>> + Send>>;

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket_bytes = &request.into_inner().ticket;

        // Verify HMAC signature, expiry, and parse payload
        let payload = auth::verify_ticket(ticket_bytes, &self.hmac_secret)
            .map_err(|e| Status::unauthenticated(e.to_string()))?;

        // Validate table name
        if !ALLOWED_TABLES.contains(&payload.table.as_str()) {
            return Err(Status::invalid_argument(format!(
                "unknown table: {:?}",
                payload.table
            )));
        }

        // Build query from filter
        let (sql, table) = build_query(&payload.table, &payload.filter)?;

        // Open a per-request DuckDB connection with DuckLake attached.
        // Each request gets its own snapshot — no shared state, no mutex.
        let conn = self.open_ducklake_conn()?;
        let mut stmt = conn
            .prepare(&sql)
            .map_err(|e| Status::internal(format!("query preparation failed for {table}: {e}")))?;
        let arrow_result = stmt
            .query_arrow([])
            .map_err(|e| Status::internal(format!("query execution failed for {table}: {e}")))?;
        let schema = arrow_result.get_schema();
        let batches: Vec<_> = arrow_result.collect();
        // Connection is dropped here — DuckDB cleans up.

        // If no batches, send an empty RecordBatch with the schema so the
        // client receives a valid (but empty) Arrow table.
        let batches = if batches.is_empty() {
            vec![arrow_array::RecordBatch::new_empty(schema)]
        } else {
            batches
        };

        // Stream RecordBatches as FlightData
        let batch_stream = stream::iter(
            batches
                .into_iter()
                .map(|b| Ok(b) as Result<_, arrow_flight::error::FlightError>),
        );
        let flight_stream = FlightDataEncoderBuilder::new().build(batch_stream);
        let mapped = flight_stream.map(|result| {
            result.map_err(|e| Status::internal(format!("flight encoding error: {e}")))
        });

        Ok(Response::new(Box::pin(mapped)))
    }

    // --- Unimplemented methods return Unimplemented status ---

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("handshake not supported"))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("list_flights not supported"))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("get_flight_info not supported"))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info not supported"))
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("get_schema not supported"))
    }

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        let result = self.do_put_inner(request.into_inner()).await?;
        let out = stream::once(futures::future::ready(Ok(result)));
        Ok(Response::new(Box::pin(out)))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange not supported"))
    }

    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        let action = request.into_inner();

        match action.r#type.as_str() {
            "register_files" => {
                let payload = auth::verify_action(&action.body, &self.hmac_secret)
                    .map_err(|e| Status::unauthenticated(e.to_string()))?;

                if payload.action != "register_files" {
                    return Err(Status::invalid_argument(format!(
                        "action type mismatch: header says 'register_files', payload says {:?}",
                        payload.action
                    )));
                }

                let registered = register_files(&self.catalog_connstr, &self.data_path, &payload)?;

                let result_body = serde_json::to_vec(&serde_json::json!({
                    "registered": registered,
                }))
                .map_err(|e| Status::internal(format!("json serialization failed: {e}")))?;

                let result = arrow_flight::Result {
                    body: result_body.into(),
                };
                let output = stream::once(futures::future::ready(Ok(result)));
                Ok(Response::new(Box::pin(output)))
            }
            "delete_reference" => {
                let payload = auth::verify_delete_reference(&action.body, &self.hmac_secret)
                    .map_err(|e| Status::unauthenticated(e.to_string()))?;

                if payload.action != "delete_reference" {
                    return Err(Status::invalid_argument(format!(
                        "action type mismatch: header says 'delete_reference', payload says {:?}",
                        payload.action
                    )));
                }

                let deleted = delete_reference(
                    &self.catalog_connstr,
                    &self.data_path,
                    payload.reference_idx,
                )?;

                let result_body = serde_json::to_vec(&deleted)
                    .map_err(|e| Status::internal(format!("json serialization failed: {e}")))?;

                let result = arrow_flight::Result {
                    body: result_body.into(),
                };
                let output = stream::once(futures::future::ready(Ok(result)));
                Ok(Response::new(Box::pin(output)))
            }
            "delete_mask" => {
                let payload = auth::verify_delete_mask(&action.body, &self.hmac_secret)
                    .map_err(|e| Status::unauthenticated(e.to_string()))?;

                if payload.action != "delete_mask" {
                    return Err(Status::invalid_argument(format!(
                        "action type mismatch: header says 'delete_mask', payload says {:?}",
                        payload.action
                    )));
                }

                let deleted =
                    delete_mask(&self.catalog_connstr, &self.data_path, payload.mask_idx)?;

                let result_body = serde_json::to_vec(&deleted)
                    .map_err(|e| Status::internal(format!("json serialization failed: {e}")))?;

                let result = arrow_flight::Result {
                    body: result_body.into(),
                };
                let output = stream::once(futures::future::ready(Ok(result)));
                Ok(Response::new(Box::pin(output)))
            }
            other => Err(Status::invalid_argument(format!(
                "unknown action type: {other:?}"
            ))),
        }
    }

    async fn list_actions(
        &self,
        _request: Request<arrow_flight::Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("list_actions not supported"))
    }
}

// ---------------------------------------------------------------------------
// DoPut — generic Arrow-data staging
// ---------------------------------------------------------------------------
//
// Receives an Arrow Flight stream with a signed DoPut ticket on the first
// message's FlightDescriptor.cmd. The ticket payload is exactly
// `{"action": "doput", "upload_idx": N}` — no consumer-specific fields. The
// handler is content-agnostic: whatever schema the client streams is what
// lands on disk as Parquet, set mode 440 on close. The consuming workflow
// (an orchestrator native module) reads `upload.parquet` and interprets it.
//
// Failure policy: any error mid-stream deletes the partial file and returns
// a Status to the client. The upload row in `qiita.upload` stays at
// `pending`; the client mints a fresh slot to retry. Partial-write
// failures aren't resumable. Post-write failures (the chmod 440 or the
// PutResult JSON encode) DO clean up the just-written file, which means
// a retry against the same upload_idx would re-trigger `create_new`
// successfully — but the client never learns the upload_idx is reusable
// in that window, so in practice retries always mint a fresh slot.

impl QiitaFlightService {
    /// Generic over the input stream so unit tests can drive it with an
    /// in-memory `stream::iter([...])` instead of needing a real
    /// `Streaming<FlightData>` (which only the tonic transport can build).
    pub(crate) async fn do_put_inner<S>(&self, mut stream: S) -> Result<PutResult, Status>
    where
        S: Stream<Item = Result<FlightData, Status>> + Send + Unpin + 'static,
    {
        // Peel the first message, extract + verify the ticket.
        let first = stream
            .next()
            .await
            .ok_or_else(|| Status::invalid_argument("empty DoPut stream"))?
            .map_err(|e| Status::internal(format!("recv error: {e}")))?;
        let descriptor = first.flight_descriptor.as_ref().ok_or_else(|| {
            Status::invalid_argument("first DoPut message lacks FlightDescriptor")
        })?;
        if descriptor.cmd.is_empty() {
            return Err(Status::invalid_argument(
                "FlightDescriptor.cmd is empty (expected signed DoPut ticket)",
            ));
        }
        let payload = auth::verify_doput(&descriptor.cmd, &self.hmac_secret)
            .map_err(|e| Status::unauthenticated(e.to_string()))?;
        if payload.action != "doput" {
            return Err(Status::invalid_argument(format!(
                "action mismatch: ticket says {:?}, expected \"doput\"",
                payload.action
            )));
        }

        // Resolve staging path, create parent dir.
        let staging_path = staging_path_for(&self.upload_staging_root, payload.upload_idx);
        let parent = staging_path
            .parent()
            .ok_or_else(|| Status::internal("staging path has no parent"))?;
        std::fs::create_dir_all(parent)
            .map_err(|e| Status::internal(format!("mkdir {}: {e}", parent.display())))?;

        // Write the parquet + chmod + body-encode under a single
        // error-guarded scope. Any Err return below cleans up the partial
        // staging file via the trailing `if result.is_err()` block — this is
        // the single cleanup site so a new fallible operation in this scope
        // can't accidentally bypass it.
        let path_for_cleanup = staging_path.clone();
        let result: Result<PutResult, Status> = async {
            let (sha256, row_count, bytes_received) =
                write_doput_parquet(staging_path.clone(), first, stream).await?;

            // Lock the file 440 — owner+group read, no write, no world.
            // After this the data plane itself can't modify it; matches
            // the immutability assumption the consuming workflow makes.
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&staging_path, std::fs::Permissions::from_mode(0o440))
                .map_err(|e| Status::internal(format!("chmod 440: {e}")))?;

            let body = serde_json::to_vec(&serde_json::json!({
                "sha256": sha256,
                "row_count": row_count,
                "bytes_received": bytes_received,
                "upload_idx": payload.upload_idx,
            }))
            .map_err(|e| Status::internal(format!("json: {e}")))?;
            Ok(PutResult {
                app_metadata: body.into(),
            })
        }
        .await;

        // Cleanup the partial / fully-written-but-unblessed Parquet on any
        // error path EXCEPT AlreadyExists. AlreadyExists means we never
        // opened the file (a prior successful DoPut owns it via
        // create_new's atomic guard); deleting it would wipe a legitimate
        // upload owned by a different call.
        if let Err(ref e) = result {
            if e.code() != tonic::Code::AlreadyExists {
                if let Err(cleanup_err) = std::fs::remove_file(&path_for_cleanup) {
                    if cleanup_err.kind() != std::io::ErrorKind::NotFound {
                        eprintln!(
                            "warning: failed to clean up partial DoPut at {}: {cleanup_err}",
                            path_for_cleanup.display()
                        );
                    }
                }
            }
        }
        result
    }
}

/// `std::io::Write` adapter that incrementally feeds every byte the inner
/// writer accepts into a shared Sha256 + byte counter. Wrapping the staging
/// `File` in this lets ArrowWriter's normal write path also drive the digest,
/// removing the second full-file read `sha256_and_size` used to do.
///
/// State lives in an `Arc<Mutex<...>>` so the outer scope can extract the
/// final hash + byte count after `ArrowWriter::close()` consumes (and drops)
/// the wrapped writer. Mutex is uncontended in practice — parquet-rs writes
/// from the single async task that owns this writer.
struct HashingWriter<W: Write> {
    inner: W,
    state: Arc<Mutex<(Sha256, u64)>>,
}

impl<W: Write> Write for HashingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.inner.write(buf)?;
        let mut state = self
            .state
            .lock()
            .expect("HashingWriter mutex never poisoned");
        state.0.update(&buf[..n]);
        state.1 += n as u64;
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

/// Drive the Flight stream through a Parquet writer; return
/// `(sha256_hex, row_count, bytes_received)`. The caller owns staging-path
/// cleanup on Err.
async fn write_doput_parquet<S>(
    staging_path: PathBuf,
    first: FlightData,
    stream: S,
) -> Result<(String, u64, u64), Status>
where
    S: Stream<Item = Result<FlightData, Status>> + Send + Unpin + 'static,
{
    // Re-prepend the first message and map Status → FlightError so the
    // arrow-flight decoder can consume it.
    let combined = stream::once(async move { Ok::<_, Status>(first) })
        .chain(stream)
        .map(|r| {
            r.map_err(|s| {
                arrow_flight::error::FlightError::ExternalError(Box::new(std::io::Error::other(
                    s.to_string(),
                )))
            })
        });
    let mut decoder = FlightDataDecoder::new(combined);

    // sync_handle is a dup of the same file ArrowWriter owns. After
    // ArrowWriter::close() (which only flushes the writer's user-space
    // buffer plus the OS write buffer), sync_all() on the dup forces a
    // disk-level flush. Without it, a power loss / OOM kill between close
    // and /done can leave the client thinking the upload succeeded while
    // the bytes were never durable.
    let mut writer: Option<ArrowWriter<HashingWriter<std::fs::File>>> = None;
    let mut sync_handle: Option<std::fs::File> = None;
    let mut row_count: u64 = 0;
    // Outer half of the shared state. The HashingWriter held inside
    // ArrowWriter holds a clone; on ArrowWriter::close() that clone is
    // dropped and we can `try_unwrap` to extract the final digest and
    // byte count without a second read of the file.
    let hash_state: Arc<Mutex<(Sha256, u64)>> = Arc::new(Mutex::new((Sha256::new(), 0)));

    while let Some(item) = decoder.next().await {
        let decoded = item.map_err(|e| Status::internal(format!("flight decode: {e}")))?;
        match decoded.payload {
            DecodedPayload::Schema(schema) => {
                if writer.is_some() {
                    return Err(Status::invalid_argument(
                        "DoPut stream carried multiple schemas",
                    ));
                }
                // `create_new` fails atomically (EEXIST) if the file already
                // exists. Guards against two concurrent DoPuts with the same
                // upload_idx silently clobbering each other's bytes via
                // separate write() calls on the same path. The CP doesn't
                // reissue tickets for a given slot, so this is the
                // contract-violation surface.
                let file = std::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&staging_path)
                    .map_err(|e| match e.kind() {
                        std::io::ErrorKind::AlreadyExists => Status::already_exists(format!(
                            "staging file already exists — concurrent DoPut?: {}",
                            staging_path.display()
                        )),
                        _ => Status::internal(format!("create {}: {e}", staging_path.display())),
                    })?;
                sync_handle = Some(
                    file.try_clone()
                        .map_err(|e| Status::internal(format!("dup file handle: {e}")))?,
                );
                // Parquet v2 + zstd. The orchestrator's miint.py defines
                // two conventions: PARQUET_OPTS (zstd, for DuckLake-bound
                // durable artifacts) and PARQUET_OPTS_INTERMEDIATE (snappy,
                // for transient files read once then deleted in the same
                // job). DoPut uploads are intermediate in the consumed-once
                // sense — but their disk-residency is "from /done to
                // (eventual) cleanup," not "until the next phase in the
                // same job." That can be minutes to indefinitely with the
                // current no-sweep follow-up open. The disk-footprint
                // tradeoff outweighs the snappy fast-decode win at GG2
                // scale: backbone FASTA blew up to ~3.5× the source on
                // disk under uncompressed v1 (DNA-chunk VARCHAR columns
                // don't dictionary-encode), and zstd-default level 3
                // gives ~4× compression at parquet-rs's default cost.
                let props = WriterProperties::builder()
                    .set_writer_version(WriterVersion::PARQUET_2_0)
                    .set_compression(Compression::ZSTD(ZstdLevel::default()))
                    .build();
                let hashing_writer = HashingWriter {
                    inner: file,
                    state: hash_state.clone(),
                };
                writer = Some(
                    ArrowWriter::try_new(hashing_writer, schema, Some(props))
                        .map_err(|e| Status::internal(format!("parquet writer init: {e}")))?,
                );
            }
            DecodedPayload::RecordBatch(batch) => {
                let w = writer
                    .as_mut()
                    .ok_or_else(|| Status::invalid_argument("RecordBatch arrived before Schema"))?;
                row_count += batch.num_rows() as u64;
                w.write(&batch)
                    .map_err(|e| Status::internal(format!("parquet write: {e}")))?;
            }
            DecodedPayload::None => {}
        }
    }
    let w = writer.ok_or_else(|| Status::invalid_argument("DoPut stream had no Schema"))?;
    w.close()
        .map_err(|e| Status::internal(format!("parquet close: {e}")))?;

    // Force disk-level flush via the dup'd handle. Unwrap is safe —
    // sync_handle is set in lockstep with `writer`, which we just confirmed
    // resolved Some via the line above.
    sync_handle
        .expect("sync_handle set in lockstep with writer")
        .sync_all()
        .map_err(|e| Status::internal(format!("fsync: {e}")))?;

    // ArrowWriter::close() above dropped the HashingWriter (and with it
    // the inner Arc clone of hash_state); the outer Arc is now the sole
    // owner, so try_unwrap succeeds.
    let (hasher, bytes_received) = Arc::try_unwrap(hash_state)
        .expect("HashingWriter dropped its Arc clone via ArrowWriter::close")
        .into_inner()
        .expect("hash_state mutex never poisoned");
    let digest = hasher.finalize();
    let mut sha256 = String::with_capacity(64);
    for b in digest {
        use std::fmt::Write;
        write!(&mut sha256, "{b:02x}").expect("write to String never fails");
    }
    Ok((sha256, row_count, bytes_received))
}

/// Move Parquet files from staging to permanent storage and register in DuckLake.
///
/// Validates all requested files exist in staging, moves them to permanent
/// locations under `data_path/{table_name}/`, then attaches DuckLake and
/// registers the moved files.
///
/// Uses `std::fs::rename` with a copy+delete fallback for cross-filesystem moves
/// (e.g., SLURM local scratch → shared NFS).
///
/// Note: the action token is scoped to staging_dir + files, not to a specific
/// reference_idx. The control plane is responsible for issuing tokens only for
/// valid references in the correct state.
fn register_files(
    catalog_connstr: &str,
    data_path: &str,
    payload: &auth::ActionPayload,
) -> Result<Vec<String>, Status> {
    let staging = std::path::Path::new(&payload.staging_dir);
    let perm_root = std::path::Path::new(data_path);

    // Validate every filename is a safe relative path under
    // staging_dir. Filenames may carry a subdir prefix
    // (e.g. "reference_sequence_chunks/part_00000.parquet") to register
    // multiple parts under one DuckLake table, but must not contain
    // `..` or absolute components. Although `payload.files` is
    // HMAC-signed by the control plane and so already trusted, this
    // defense-in-depth check keeps the data plane's filesystem
    // contract independent of CP correctness.
    for filename in payload.files.keys() {
        let candidate = std::path::Path::new(filename);
        if candidate.components().any(|c| {
            matches!(
                c,
                std::path::Component::ParentDir
                    | std::path::Component::RootDir
                    | std::path::Component::Prefix(_)
            )
        }) {
            return Err(Status::invalid_argument(format!(
                "filename must be a relative path with no '..' components: {filename}"
            )));
        }
    }

    // Validate all requested files exist.
    for filename in payload.files.keys() {
        let src = staging.join(filename);
        if !src.exists() {
            return Err(Status::not_found(format!(
                "staging file not found: {}",
                src.display()
            )));
        }
    }

    // Move all files to permanent storage.
    let mut moved: Vec<(String, std::path::PathBuf)> = Vec::new();
    for (filename, table) in &payload.files {
        let src = staging.join(filename);
        let dest_dir = perm_root.join(table);
        std::fs::create_dir_all(&dest_dir).map_err(|e| {
            Status::internal(format!(
                "failed to create directory {}: {e}",
                dest_dir.display()
            ))
        })?;
        // Multi-file tables carry a subdir prefix in `filename`
        // (e.g. "reference_sequence_chunks/part_00000.parquet"). Use
        // only the basename when placing into `dest_dir` — otherwise
        // we'd nest the staging subdir inside the per-table
        // destination dir.
        let basename = std::path::Path::new(filename)
            .file_name()
            .and_then(|b| b.to_str())
            .ok_or_else(|| {
                Status::invalid_argument(format!("filename has no UTF-8 basename: {filename}"))
            })?;
        // Mint a unique, ticket-traceable destination name — the data plane
        // owns lake-storage layout, and the producer reuses fixed basenames
        // across loads, so placing the bare basename would collide with an
        // already-registered file in the same per-table dir. `move_file`
        // refuses to overwrite besides, as a hard safety net.
        let dest = dest_dir.join(lake_dest_filename(payload.work_ticket_idx, basename));
        move_file(&src, &dest)?;
        moved.push((table.clone(), dest));
    }

    // Register in DuckLake. Tables are ensured at startup in main.rs.
    let conn = duckdb::Connection::open_in_memory()
        .map_err(|e| Status::internal(format!("failed to open DuckDB: {e}")))?;
    ducklake::connect_ducklake(&conn, catalog_connstr, data_path)
        .map_err(|e| Status::internal(format!("failed to attach DuckLake: {e}")))?;

    let mut registered = Vec::new();
    for (table, dest) in &moved {
        let dest_str = dest
            .to_str()
            .ok_or_else(|| Status::internal(format!("non-UTF-8 path: {}", dest.display())))?;
        conn.execute(
            "CALL ducklake_add_data_files('qiita_lake', ?, ?)",
            duckdb::params![table, dest_str],
        )
        .map_err(|e| {
            // Log which files were already registered for recovery.
            let already: Vec<_> = registered.iter().collect();
            Status::internal(format!(
                "ducklake_add_data_files failed for {table}/{}: {e}. \
                 Already registered: {already:?}. \
                 Already moved: {:?}. \
                 Manual recovery may be needed.",
                dest.display(),
                moved
                    .iter()
                    .map(|(_, p)| p.display().to_string())
                    .collect::<Vec<_>>()
            ))
        })?;
        registered.push(dest_str.to_string());
    }

    Ok(registered)
}

/// Delete every DuckLake row belonging to a reference.
///
/// Scoping rules mirror the identifier hierarchy:
/// - `reference_taxonomy`, `reference_phylogeny`, `reference_placements`, and
///   `reference_membership` carry `reference_idx` directly → deleted by a plain
///   `WHERE reference_idx = ?`.
/// - `reference_sequences` / `reference_sequence_chunks` are keyed by
///   `feature_idx` and **shared across references** (a feature deduplicates by
///   sequence hash). Only *orphan* features — owned by this reference and no
///   other — are removed; a feature another reference still claims keeps its
///   sequence. Orphans are computed from this data plane's own
///   `reference_membership`, so the action ticket needs only `reference_idx`.
///
/// Order matters: the sequence deletes run *before* the membership delete so
/// the orphan subquery can still see this reference's rows. Idempotent — a
/// reference with no loaded data deletes zero rows and still succeeds.
fn delete_reference(
    catalog_connstr: &str,
    data_path: &str,
    reference_idx: i64,
) -> Result<serde_json::Value, Status> {
    let conn = duckdb::Connection::open_in_memory()
        .map_err(|e| Status::internal(format!("failed to open DuckDB: {e}")))?;
    ducklake::connect_ducklake(&conn, catalog_connstr, data_path)
        .map_err(|e| Status::internal(format!("failed to attach DuckLake: {e}")))?;

    let exec = |sql: &str, params: &[i64]| -> Result<usize, Status> {
        // duckdb's params! wants &dyn ToSql; i64 implements it. Build the
        // slice explicitly so the same helper serves the 1- and 2-param calls.
        let boxed: Vec<&dyn duckdb::ToSql> =
            params.iter().map(|p| p as &dyn duckdb::ToSql).collect();
        conn.execute(sql, boxed.as_slice())
            .map_err(|e| Status::internal(format!("delete failed ({sql}): {e}")))
    };

    // All six deletes are one DuckLake transaction so the action is
    // all-or-nothing: a mid-delete failure rolls every table back rather than
    // leaving a half-purged reference. That atomicity is what lets the control
    // plane safely retry — a failed call leaves DuckLake membership fully
    // intact, so the orphan recomputation on the next attempt is unchanged.
    conn.execute_batch("BEGIN TRANSACTION")
        .map_err(|e| Status::internal(format!("failed to begin transaction: {e}")))?;

    // Orphan features: this reference's features minus every other reference's.
    // This set MUST match the Postgres-side orphan computation in
    // qiita_control_plane.actions.reference.delete_reference_cascade — the two
    // stores GC the same features independently, so a change to one query must
    // change the other or sequences/features desync across stores.
    let orphan_filter = "feature_idx IN (
            SELECT feature_idx FROM qiita_lake.reference_membership WHERE reference_idx = ?
            EXCEPT
            SELECT feature_idx FROM qiita_lake.reference_membership WHERE reference_idx <> ?
        )";

    // Sequence/chunk deletes run BEFORE the membership delete: the orphan
    // subquery needs this reference's membership rows still present.
    let deletes = (|| -> Result<serde_json::Value, Status> {
        let sequences_deleted = exec(
            &format!("DELETE FROM qiita_lake.reference_sequences WHERE {orphan_filter}"),
            &[reference_idx, reference_idx],
        )?;
        let chunks_deleted = exec(
            &format!("DELETE FROM qiita_lake.reference_sequence_chunks WHERE {orphan_filter}"),
            &[reference_idx, reference_idx],
        )?;
        let membership_deleted = exec(
            "DELETE FROM qiita_lake.reference_membership WHERE reference_idx = ?",
            &[reference_idx],
        )?;
        let taxonomy_deleted = exec(
            "DELETE FROM qiita_lake.reference_taxonomy WHERE reference_idx = ?",
            &[reference_idx],
        )?;
        let phylogeny_deleted = exec(
            "DELETE FROM qiita_lake.reference_phylogeny WHERE reference_idx = ?",
            &[reference_idx],
        )?;
        let placements_deleted = exec(
            "DELETE FROM qiita_lake.reference_placements WHERE reference_idx = ?",
            &[reference_idx],
        )?;
        Ok(serde_json::json!({
            "sequences_deleted": sequences_deleted,
            "chunks_deleted": chunks_deleted,
            "membership_deleted": membership_deleted,
            "taxonomy_deleted": taxonomy_deleted,
            "phylogeny_deleted": phylogeny_deleted,
            "placements_deleted": placements_deleted,
        }))
    })();

    let counts = match deletes {
        Ok(counts) => counts,
        Err(e) => {
            // Best-effort rollback; surface the original delete error.
            let _ = conn.execute_batch("ROLLBACK");
            return Err(e);
        }
    };
    conn.execute_batch("COMMIT")
        .map_err(|e| Status::internal(format!("failed to commit delete transaction: {e}")))?;

    let mut out = counts;
    out["reference_idx"] = serde_json::json!(reference_idx);
    Ok(out)
}

/// Logically delete every row a mask owns from the DuckLake `read_mask` table.
///
/// Mirrors `delete_reference`: one DuckLake transaction, logical `DELETE` only.
/// No raw parquet `unlink` — DuckLake owns file lifecycle and a manual unlink
/// would corrupt the catalog; orphan parquets are tolerated until a future
/// maintenance pass (matches `delete_reference`, which also reclaims nothing
/// from disk). Idempotent: deleting a `mask_idx` with zero rows is success and
/// returns `rows_deleted: 0`, so the control plane can safely retry.
fn delete_mask(
    catalog_connstr: &str,
    data_path: &str,
    mask_idx: i64,
) -> Result<serde_json::Value, Status> {
    let conn = duckdb::Connection::open_in_memory()
        .map_err(|e| Status::internal(format!("failed to open DuckDB: {e}")))?;
    ducklake::connect_ducklake(&conn, catalog_connstr, data_path)
        .map_err(|e| Status::internal(format!("failed to attach DuckLake: {e}")))?;

    // Single-statement delete wrapped in an explicit transaction so the action
    // is all-or-nothing and the control plane can safely retry: a failed call
    // leaves the mask's rows fully intact, so a retry sees the same row set.
    conn.execute_batch("BEGIN TRANSACTION")
        .map_err(|e| Status::internal(format!("failed to begin transaction: {e}")))?;

    let deleted = conn.execute(
        "DELETE FROM qiita_lake.read_mask WHERE mask_idx = ?",
        [&mask_idx as &dyn duckdb::ToSql],
    );

    let rows_deleted = match deleted {
        Ok(n) => n,
        Err(e) => {
            // Best-effort rollback; surface the original delete error.
            let _ = conn.execute_batch("ROLLBACK");
            return Err(Status::internal(format!(
                "delete failed (DELETE FROM qiita_lake.read_mask WHERE mask_idx = ?): {e}"
            )));
        }
    };
    conn.execute_batch("COMMIT")
        .map_err(|e| Status::internal(format!("failed to commit delete transaction: {e}")))?;

    Ok(serde_json::json!({
        "mask_idx": mask_idx,
        "rows_deleted": rows_deleted,
    }))
}

/// Mint a unique, ticket-traceable lake-storage filename for a registered
/// Parquet.
///
/// The producer (the reference-load job) reuses fixed basenames
/// (`part_00000.parquet`, `reference_<table>.parquet`) on every load, so the
/// bare basename is NOT unique within a per-table lake dir: two registrations
/// into the same table would target the same path and the second would clobber
/// the first's live, catalog-registered file. Prefixing with the originating
/// work ticket makes the name unique across loads (every load is a distinct
/// ticket) while staying unique within a load (the basename — part index or
/// table name — still distinguishes files under one ticket), and lets an
/// operator trace any lake file back to the ticket that wrote it. DuckLake
/// names its own INSERT-written data files uniquely for the same reason; this
/// is the equivalent for our "register an existing file" path.
fn lake_dest_filename(work_ticket_idx: i64, basename: &str) -> String {
    format!("wt{work_ticket_idx}-{basename}")
}

/// Move a file, falling back to copy+delete for cross-filesystem moves.
///
/// Refuses to overwrite an existing destination. Lake data files are
/// registered in the DuckLake catalog by absolute path and written read-only
/// (mode 0440); clobbering one corrupts the lake (or, because of the read-only
/// bit, fails mid-copy with a cryptic EACCES). Callers mint unique destination
/// names ([`lake_dest_filename`]), so a pre-existing dest signals a genuine
/// double-registration — surface it loudly as `AlreadyExists` rather than
/// touching the file.
///
/// If the copy succeeds but delete fails, the dest file is kept (it's the
/// correct data) and the error message includes the orphaned source path
/// for cleanup.
fn move_file(src: &std::path::Path, dest: &std::path::Path) -> Result<(), Status> {
    if dest.exists() {
        return Err(Status::already_exists(format!(
            "refusing to overwrite existing lake file {}",
            dest.display()
        )));
    }
    match std::fs::rename(src, dest) {
        Ok(()) => Ok(()),
        Err(e) if e.raw_os_error() == Some(18) => {
            // EXDEV: cross-device link — fall back to copy + delete
            std::fs::copy(src, dest).map_err(|e| {
                Status::internal(format!(
                    "cross-fs copy failed {} → {}: {e}",
                    src.display(),
                    dest.display()
                ))
            })?;
            if let Err(e) = std::fs::remove_file(src) {
                // Copy succeeded — dest has the data. Log the orphan but
                // don't fail the operation. The staging file is stale.
                eprintln!(
                    "warning: cross-fs cleanup failed for {} (dest {} is valid): {e}",
                    src.display(),
                    dest.display()
                );
            }
            Ok(())
        }
        Err(e) => Err(Status::internal(format!(
            "rename failed {} → {}: {e}",
            src.display(),
            dest.display()
        ))),
    }
}

/// Build a SQL query for the given table and filter.
///
/// SQL injection defense model:
/// - Table name: whitelist (`ALLOWED_TABLES`) — only known-safe values
/// - Column names: whitelist (`ALLOWED_FILTER_COLUMNS`) — only known identifier columns
/// - Values: parsed as i64 then stringified — no string data reaches SQL
/// - All inputs are also HMAC-verified (set by the control plane, not the client)
///
/// DuckDB does not support parameterized identifiers (table/column names), so
/// whitelisting is the correct defense. Values could be parameterized but are
/// already safe as parsed integers.
fn build_query(table: &str, filter: &auth::TicketFilter) -> Result<(String, String), Status> {
    let full_table = format!("qiita_lake.{table}");

    if filter.is_empty() {
        return Ok((format!("SELECT * FROM {full_table}"), full_table));
    }

    // reference_sequences and reference_sequence_chunks have no reference_idx
    // column. When the filter includes reference_idx, resolve via a JOIN with
    // the membership table.
    let needs_membership_join = (table == "reference_sequences"
        || table == "reference_sequence_chunks")
        && filter.contains_key("reference_idx");

    let mut where_clauses = Vec::new();
    for (col, values) in filter {
        // Whitelist column names — all SQL is constructed from known-safe identifiers.
        // Input is HMAC-verified (set by control plane), but we validate anyway for
        // defense-in-depth.
        if !ALLOWED_FILTER_COLUMNS.contains(&col.as_str()) {
            return Err(Status::invalid_argument(format!(
                "unknown filter column: {col:?}"
            )));
        }
        if values.is_empty() {
            return Err(Status::invalid_argument(format!(
                "filter column {col:?} has empty values list"
            )));
        }
        // Build IN clause with integer values only
        let int_values: Vec<i64> = values
            .iter()
            .map(|v| {
                v.as_i64().ok_or_else(|| {
                    Status::invalid_argument(format!(
                        "filter values for {col:?} must be integers, got {v}"
                    ))
                })
            })
            .collect::<Result<_, _>>()?;
        let csv = int_values
            .iter()
            .map(|v| v.to_string())
            .collect::<Vec<_>>()
            .join(",");

        if needs_membership_join && col == "reference_idx" {
            // Applied as a WHERE on the joined membership table alias.
            where_clauses.push(format!("m.reference_idx IN ({csv})"));
        } else {
            where_clauses.push(format!("{col} IN ({csv})"));
        }
    }

    let where_str = where_clauses.join(" AND ");
    let sql = if needs_membership_join {
        format!(
            "SELECT t.* FROM {full_table} t \
             JOIN qiita_lake.reference_membership m ON t.feature_idx = m.feature_idx \
             WHERE {where_str}"
        )
    } else {
        format!("SELECT * FROM {full_table} WHERE {where_str}")
    };
    Ok((sql, full_table))
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- delete_reference integration harness (mirrors ducklake.rs::tests) ---

    #[cfg(feature = "integration")]
    fn delete_test_catalog_connstr() -> String {
        std::env::var("DUCKLAKE_CATALOG_CONNSTR").unwrap_or_else(|_| {
            "dbname=qiita_ducklake host=localhost port=5433 user=qiita password=qiita".to_string()
        })
    }

    #[cfg(feature = "integration")]
    fn delete_test_data_path() -> String {
        let data_path = std::env::var("PATH_PERSISTENT")
            .map(|base| format!("{base}/ducklake"))
            .unwrap_or_else(|_| "/tmp/qiita-integration-ducklake-data".to_string());
        std::fs::create_dir_all(&data_path).unwrap();
        data_path
    }

    /// Orphan-only sequence deletion: a feature owned by another reference
    /// keeps its sequence; a feature owned only by the deleted reference loses
    /// it. Reference-scoped tables (membership, taxonomy) drop fully.
    #[test]
    #[serial_test::serial]
    #[cfg(feature = "integration")]
    fn delete_reference_drops_orphans_keeps_shared() {
        let connstr = delete_test_catalog_connstr();
        let data_path = delete_test_data_path();
        let conn = Connection::open_in_memory().unwrap();
        ducklake::connect_ducklake(&conn, &connstr, &data_path).unwrap();
        ducklake::ensure_reference_tables(&conn).unwrap();

        // Unique ids so leftover rows never collide with other tests.
        let ref_a: i64 = 910_000;
        let ref_b: i64 = 910_001;
        let shared: i64 = 910_010; // claimed by ref_a AND ref_b
        let orphan: i64 = 910_011; // claimed by ref_a only

        conn.execute_batch(&format!(
            "INSERT INTO qiita_lake.reference_membership VALUES \
                 ({ref_a}, {shared}), ({ref_a}, {orphan}), ({ref_b}, {shared});
             INSERT INTO qiita_lake.reference_sequences VALUES \
                 ({shared}, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::UUID, 4), \
                 ({orphan}, 'b0eebc99-9c0b-4ef8-bb6d-6bb9bd380a22'::UUID, 5);
             INSERT INTO qiita_lake.reference_taxonomy (reference_idx, feature_idx, domain) VALUES \
                 ({ref_a}, {shared}, 'd__Bacteria'), ({ref_a}, {orphan}, 'd__Bacteria');"
        ))
        .unwrap();

        let counts =
            delete_reference(&connstr, &data_path, ref_a).expect("delete_reference failed");
        assert_eq!(counts["sequences_deleted"], 1, "only the orphan sequence");
        assert_eq!(counts["membership_deleted"], 2, "both ref_a memberships");
        assert_eq!(counts["taxonomy_deleted"], 2);

        let remaining_seq = |feature: i64| -> i64 {
            conn.query_row(
                &format!(
                    "SELECT count(*) FROM qiita_lake.reference_sequences WHERE feature_idx = {feature}"
                ),
                [],
                |r| r.get(0),
            )
            .unwrap()
        };
        assert_eq!(
            remaining_seq(shared),
            1,
            "shared feature keeps its sequence"
        );
        assert_eq!(remaining_seq(orphan), 0, "orphan feature sequence deleted");

        let ref_a_membership: i64 = conn
            .query_row(
                &format!(
                    "SELECT count(*) FROM qiita_lake.reference_membership WHERE reference_idx = {ref_a}"
                ),
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(ref_a_membership, 0);
        let ref_b_membership: i64 = conn
            .query_row(
                &format!(
                    "SELECT count(*) FROM qiita_lake.reference_membership WHERE reference_idx = {ref_b}"
                ),
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(ref_b_membership, 1, "ref_b membership untouched");

        // Best-effort cleanup of the surviving shared rows.
        let _ = conn.execute_batch(&format!(
            "DELETE FROM qiita_lake.reference_membership WHERE reference_idx = {ref_b};
             DELETE FROM qiita_lake.reference_sequences WHERE feature_idx = {shared};"
        ));
    }

    /// `delete_mask` drops exactly the target mask's `read_mask` rows, leaves a
    /// different mask untouched, and is idempotent: a second delete of the same
    /// mask_idx succeeds and reports `rows_deleted: 0`.
    #[test]
    #[serial_test::serial]
    #[cfg(feature = "integration")]
    fn delete_mask_drops_target_idempotently() {
        let connstr = delete_test_catalog_connstr();
        let data_path = delete_test_data_path();
        let conn = Connection::open_in_memory().unwrap();
        ducklake::connect_ducklake(&conn, &connstr, &data_path).unwrap();
        ducklake::ensure_read_tables(&conn).unwrap();

        // Unique ids so leftover rows never collide with other serial tests.
        let mask_a: i64 = 930_000;
        let mask_b: i64 = 930_001;
        let prep: i64 = 930_010;
        let seq1: i64 = 930_020;
        let seq2: i64 = 930_021;

        conn.execute_batch(&format!(
            "DELETE FROM qiita_lake.read_mask WHERE mask_idx IN ({mask_a}, {mask_b});
             INSERT INTO qiita_lake.read_mask \
                 (mask_idx, prep_sample_idx, sequence_idx, reason) VALUES \
                 ({mask_a}, {prep}, {seq1}, 'pass'), \
                 ({mask_a}, {prep}, {seq2}, 'pass'), \
                 ({mask_b}, {prep}, {seq1}, 'pass');"
        ))
        .unwrap();

        let first = delete_mask(&connstr, &data_path, mask_a).expect("delete_mask failed");
        assert_eq!(first["rows_deleted"], 2, "both mask_a rows deleted");
        assert_eq!(first["mask_idx"], mask_a);

        let count = |mask: i64| -> i64 {
            conn.query_row(
                &format!("SELECT count(*) FROM qiita_lake.read_mask WHERE mask_idx = {mask}"),
                [],
                |r| r.get(0),
            )
            .unwrap()
        };
        assert_eq!(count(mask_a), 0, "mask_a rows gone");
        assert_eq!(count(mask_b), 1, "mask_b untouched");

        // Idempotency: re-deleting the now-empty mask is success with 0 rows.
        let second =
            delete_mask(&connstr, &data_path, mask_a).expect("idempotent re-delete failed");
        assert_eq!(second["rows_deleted"], 0, "second delete removes nothing");

        // Best-effort cleanup of the surviving mask_b row.
        let _ = conn.execute_batch(&format!(
            "DELETE FROM qiita_lake.read_mask WHERE mask_idx = {mask_b};"
        ));
    }

    // DoGet round-trip for read_masked: drive the exact query path do_get uses
    // (build_query → prepare → query_arrow → get_schema → collect, plus the
    // empty-result RecordBatch::new_empty branch) against fixture data, and
    // assert the UTINYINT[] qual column survives as an Arrow List of UInt8.
    // This pins the one read-path behavior the reference tables don't cover
    // (they have no list columns): a UTINYINT[] column round-trips through
    // query_arrow → Arrow.
    #[test]
    #[serial_test::serial]
    #[cfg(feature = "integration")]
    fn read_masked_doget_roundtrips_utinyint_array() {
        use arrow_schema::DataType;

        let connstr = delete_test_catalog_connstr();
        let data_path = delete_test_data_path();
        let conn = Connection::open_in_memory().unwrap();
        ducklake::connect_ducklake(&conn, &connstr, &data_path).unwrap();
        ducklake::ensure_read_tables(&conn).unwrap();

        // Unique ids so leftover rows never collide with other serial tests.
        let prep: i64 = 920_000;
        let mask: i64 = 920_001;
        let seq: i64 = 920_010;

        conn.execute_batch(&format!(
            "DELETE FROM qiita_lake.read WHERE prep_sample_idx = {prep};
             DELETE FROM qiita_lake.read_mask WHERE prep_sample_idx = {prep};
             INSERT INTO qiita_lake.read \
                 (prep_sample_idx, sequence_idx, read_id, sequence1, qual1, sequence2, qual2) VALUES \
                 ({prep}, {seq}, 'r', 'ACGTAC', [5,6,7,8,9,10]::UTINYINT[], NULL, NULL);
             INSERT INTO qiita_lake.read_mask \
                 (mask_idx, prep_sample_idx, sequence_idx, reason, left_trim1, right_trim1) VALUES \
                 ({mask}, {prep}, {seq}, 'pass', 1, 1);"
        ))
        .unwrap();

        // Helper that mirrors do_get's query body for read_masked.
        let run = |filter: &auth::TicketFilter| -> Vec<arrow_array::RecordBatch> {
            let (sql, _) = build_query("read_masked", filter).unwrap();
            let mut stmt = conn.prepare(&sql).unwrap();
            let arrow_result = stmt.query_arrow([]).unwrap();
            let schema = arrow_result.get_schema();
            let batches: Vec<_> = arrow_result.collect();
            if batches.is_empty() {
                vec![arrow_array::RecordBatch::new_empty(schema)]
            } else {
                batches
            }
        };

        // Non-empty: the qual1 column is an Arrow List whose items are UInt8.
        let mut filter = auth::TicketFilter::new();
        filter.insert("mask_idx".to_string(), vec![serde_json::Value::from(mask)]);
        filter.insert(
            "prep_sample_idx".to_string(),
            vec![serde_json::Value::from(prep)],
        );
        let batches = run(&filter);
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 1, "one pass read should round-trip");

        let schema = batches[0].schema();
        let qual1 = schema.field_with_name("qual1").unwrap();
        let item_type = match qual1.data_type() {
            DataType::List(item) | DataType::LargeList(item) => item.data_type().clone(),
            other => panic!("qual1 should be an Arrow List, got: {other:?}"),
        };
        assert_eq!(
            item_type,
            DataType::UInt8,
            "UTINYINT[] must round-trip as a List of UInt8"
        );

        // Empty-result branch: a mask_idx with no rows yields exactly one empty
        // batch carrying the schema (do_get's RecordBatch::new_empty path).
        let mut empty_filter = auth::TicketFilter::new();
        empty_filter.insert(
            "mask_idx".to_string(),
            vec![serde_json::Value::from(mask + 999_999)],
        );
        empty_filter.insert(
            "prep_sample_idx".to_string(),
            vec![serde_json::Value::from(prep)],
        );
        let empty = run(&empty_filter);
        assert_eq!(empty.len(), 1, "empty result still yields one schema batch");
        assert_eq!(empty[0].num_rows(), 0, "the schema batch has no rows");
        assert!(
            empty[0].schema().field_with_name("qual1").is_ok(),
            "empty batch carries the full read_masked schema"
        );

        // Cleanup.
        let _ = conn.execute_batch(&format!(
            "DELETE FROM qiita_lake.read WHERE prep_sample_idx = {prep};
             DELETE FROM qiita_lake.read_mask WHERE prep_sample_idx = {prep};"
        ));
    }

    // Regression (data-plane lake-file placement): when `register_files`
    // moves an externally-produced Parquet into managed lake storage, it must
    // NEVER overwrite a file already present there. The reference-load job
    // emits fixed basenames (`part_00000.parquet`, `reference_<table>.parquet`),
    // so a second registration into the same table targeted the exact path of
    // the first load's live, catalog-registered data file. Registered files are
    // mode 0440, so on the live host the clobber surfaced as a cryptic EACCES
    // ("cross-fs copy failed … Permission denied"); this pins the intended
    // behavior independent of the dest's mode: refuse with AlreadyExists and
    // leave the existing file byte-for-byte intact. The copy is the data
    // plane's responsibility, so the guard lives at the copy primitive.
    #[test]
    fn move_file_refuses_to_overwrite_existing_dest() {
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("src.parquet");
        let dest = tmp.path().join("dest.parquet");
        std::fs::write(&src, b"new load output").unwrap();
        std::fs::write(&dest, b"REGISTERED LAKE DATA").unwrap();

        let err = move_file(&src, &dest)
            .expect_err("move_file must refuse to overwrite an existing destination");
        assert_eq!(
            err.code(),
            tonic::Code::AlreadyExists,
            "clobber must surface as AlreadyExists, not a cryptic permission error"
        );

        // The existing (registered) lake file is untouched ...
        assert_eq!(
            std::fs::read(&dest).unwrap(),
            b"REGISTERED LAKE DATA",
            "existing lake file must not be modified"
        );
        // ... and the source is preserved for diagnosis (the move is refused,
        // not half-applied).
        assert!(
            src.exists(),
            "source must be preserved when the move is refused"
        );
    }

    // The minted lake filename carries the work ticket (traceability) and is
    // unique across loads: the same producer basename registered under two
    // different tickets must land at distinct paths, so neither clobbers the
    // other in the shared per-table lake dir.
    #[test]
    fn lake_dest_filename_is_traceable_and_unique_across_tickets() {
        let a = lake_dest_filename(27, "part_00000.parquet");
        let b = lake_dest_filename(31, "part_00000.parquet");
        assert_eq!(a, "wt27-part_00000.parquet", "name embeds the work ticket");
        assert_ne!(
            a, b,
            "same basename under different tickets must not collide"
        );
        // Deterministic — no randomness, so a resume/retry recomputes the same
        // name and the move_file guard can detect a true double-registration.
        assert_eq!(a, lake_dest_filename(27, "part_00000.parquet"));
        // Distinct basenames within one ticket stay distinct (multiple parts
        // and the flat per-table files share a ticket).
        assert_ne!(
            lake_dest_filename(27, "part_00001.parquet"),
            lake_dest_filename(27, "reference_membership.parquet")
        );
    }

    #[test]
    fn build_query_no_filter() {
        let (sql, _) = build_query("reference_sequences", &auth::TicketFilter::new()).unwrap();
        assert_eq!(sql, "SELECT * FROM qiita_lake.reference_sequences");
    }

    #[test]
    fn build_query_with_filter() {
        let mut filter = auth::TicketFilter::new();
        filter.insert(
            "feature_idx".to_string(),
            vec![
                serde_json::Value::from(1),
                serde_json::Value::from(2),
                serde_json::Value::from(3),
            ],
        );
        let (sql, _) = build_query("reference_sequences", &filter).unwrap();
        assert!(sql.contains("feature_idx IN (1,2,3)"));
    }

    #[test]
    fn build_query_rejects_bad_column() {
        let mut filter = auth::TicketFilter::new();
        filter.insert(
            "'; DROP TABLE".to_string(),
            vec![serde_json::Value::from(1)],
        );
        let result = build_query("reference_sequences", &filter);
        assert!(result.is_err());
    }

    #[test]
    fn build_query_rejects_non_integer_values() {
        let mut filter = auth::TicketFilter::new();
        filter.insert(
            "feature_idx".to_string(),
            vec![serde_json::Value::from("not_an_int")],
        );
        let result = build_query("reference_sequences", &filter);
        assert!(result.is_err());
    }

    #[test]
    fn build_query_rejects_empty_values() {
        let mut filter = auth::TicketFilter::new();
        filter.insert("feature_idx".to_string(), vec![]);
        let result = build_query("reference_sequences", &filter);
        assert!(result.is_err());
    }

    #[test]
    fn build_query_sequences_reference_idx_uses_join() {
        let mut filter = auth::TicketFilter::new();
        filter.insert(
            "reference_idx".to_string(),
            vec![serde_json::Value::from(42)],
        );
        let (sql, _) = build_query("reference_sequences", &filter).unwrap();
        assert!(
            sql.contains("JOIN qiita_lake.reference_membership m ON t.feature_idx = m.feature_idx"),
            "expected JOIN for reference_sequences + reference_idx, got: {sql}"
        );
        assert!(sql.contains("m.reference_idx IN (42)"));
        assert!(sql.starts_with("SELECT t.* FROM"));
    }

    #[test]
    fn build_query_taxonomy_reference_idx_direct() {
        let mut filter = auth::TicketFilter::new();
        filter.insert(
            "reference_idx".to_string(),
            vec![serde_json::Value::from(42)],
        );
        let (sql, _) = build_query("reference_taxonomy", &filter).unwrap();
        assert!(
            sql.contains("reference_idx IN (42)"),
            "expected direct filter, got: {sql}"
        );
        assert!(
            !sql.contains("JOIN"),
            "taxonomy should not use JOIN, got: {sql}"
        );
    }

    #[test]
    fn build_query_read_masked_both_filters() {
        // read_masked is a plain view: both mask_idx and prep_sample_idx are
        // integer columns filtered directly via IN clauses (no membership join).
        let mut filter = auth::TicketFilter::new();
        filter.insert("mask_idx".to_string(), vec![serde_json::Value::from(7)]);
        filter.insert(
            "prep_sample_idx".to_string(),
            vec![serde_json::Value::from(11), serde_json::Value::from(12)],
        );
        let (sql, table) = build_query("read_masked", &filter).unwrap();
        assert_eq!(table, "qiita_lake.read_masked");
        assert!(
            sql.starts_with("SELECT * FROM qiita_lake.read_masked WHERE"),
            "expected a plain view select, got: {sql}"
        );
        assert!(sql.contains("mask_idx IN (7)"), "got: {sql}");
        assert!(sql.contains("prep_sample_idx IN (11,12)"), "got: {sql}");
        assert!(
            !sql.contains("JOIN"),
            "read_masked is a plain view, no membership JOIN, got: {sql}"
        );
    }

    #[test]
    fn build_query_read_masked_rejects_bad_column() {
        // sequence_idx is a column of the view but is NOT an allowed filter
        // column, so a ticket filtering on it must be rejected.
        let mut filter = auth::TicketFilter::new();
        filter.insert("sequence_idx".to_string(), vec![serde_json::Value::from(1)]);
        let result = build_query("read_masked", &filter);
        assert!(
            result.is_err(),
            "sequence_idx is not an allowed filter column"
        );
    }

    // ------------------------------------------------------------------
    // DoPut handler tests
    // ------------------------------------------------------------------

    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    use std::os::unix::fs::PermissionsExt;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    type HmacSha256 = Hmac<Sha256>;

    fn sign_doput_for_test(upload_idx: i64, secret: &[u8], expiry: u64) -> Vec<u8> {
        let payload = format!(r#"{{"action":"doput","upload_idx":{upload_idx}}}"#);
        sign_raw(payload.as_bytes(), secret, expiry)
    }

    fn sign_raw(payload: &[u8], secret: &[u8], expiry: u64) -> Vec<u8> {
        let version: u8 = 1;
        let payload_len = (payload.len() as u32).to_be_bytes();
        let expiry_bytes = expiry.to_be_bytes();
        let mac_input = [&[version][..], &payload_len[..], payload, &expiry_bytes[..]].concat();
        let mut mac = HmacSha256::new_from_slice(secret).unwrap();
        mac.update(&mac_input);
        let hmac_result = mac.finalize().into_bytes();
        let mut ticket = Vec::new();
        ticket.push(version);
        ticket.extend_from_slice(&payload_len);
        ticket.extend_from_slice(payload);
        ticket.extend_from_slice(&hmac_result);
        ticket.extend_from_slice(&expiry_bytes);
        ticket
    }

    fn future_expiry_secs(secs: u64) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + secs
    }

    /// Build a tiny test RecordBatch — schema is arbitrary, DoPut is content-agnostic.
    fn sample_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("read_id", DataType::Utf8, false),
            Field::new("seq_length", DataType::Int64, false),
        ]));
        let read_ids = Arc::new(StringArray::from(vec!["r1", "r2", "r3"]));
        let lengths = Arc::new(Int64Array::from(vec![12i64, 34, 56]));
        RecordBatch::try_new(schema, vec![read_ids, lengths]).unwrap()
    }

    /// Convert one or more RecordBatches into a Flight stream stamped with
    /// the supplied ticket on the first message's FlightDescriptor.cmd.
    async fn flight_stream_with_ticket(
        batches: Vec<RecordBatch>,
        ticket: Vec<u8>,
    ) -> Vec<Result<FlightData, Status>> {
        let batch_stream = stream::iter(
            batches
                .into_iter()
                .map(Ok::<_, arrow_flight::error::FlightError>),
        );
        let mut flight_data: Vec<FlightData> = FlightDataEncoderBuilder::new()
            .build(batch_stream)
            .filter_map(|r| async move { r.ok() })
            .collect()
            .await;
        // Stamp the ticket onto the first message's descriptor — pyarrow's
        // client does the equivalent via FlightDescriptor.for_command.
        let mut first = flight_data.remove(0);
        first.flight_descriptor = Some(FlightDescriptor::new_cmd(ticket));
        let mut out = vec![Ok(first)];
        out.extend(flight_data.into_iter().map(Ok));
        out
    }

    fn make_service(staging_root: PathBuf) -> QiitaFlightService {
        QiitaFlightService::new(
            b"dev-secret".to_vec(),
            // catalog + data_path unused by DoPut path
            "dbname=unused host=localhost".to_string(),
            "/tmp/unused".to_string(),
            staging_root,
        )
    }

    #[tokio::test]
    async fn do_put_writes_arrow_stream_to_parquet() {
        let tmp = tempfile::tempdir().unwrap();
        let service = make_service(tmp.path().to_path_buf());

        let ticket = sign_doput_for_test(42, b"dev-secret", future_expiry_secs(300));
        let messages = flight_stream_with_ticket(vec![sample_batch()], ticket).await;

        let result = service
            .do_put_inner(stream::iter(messages))
            .await
            .expect("do_put should succeed on a well-formed stream");

        let staged = tmp.path().join("uploads/42/upload.parquet");
        assert!(staged.exists(), "staging file not written");

        // File mode is 440 (owner+group read, no write, no world)
        let perms = std::fs::metadata(&staged).unwrap().permissions();
        assert_eq!(perms.mode() & 0o777, 0o440);

        // PutResult body carries sha256/row_count/bytes/upload_idx — and
        // deliberately NOT staging_path. Clients are not allowed to learn
        // server-side paths (the architecture commitment); the layout is
        // derivable from root + upload_idx by parties that legitimately
        // need it (CP, DP), but the client is not one of those.
        let body: serde_json::Value = serde_json::from_slice(&result.app_metadata).unwrap();
        assert_eq!(body["upload_idx"], 42);
        assert_eq!(body["row_count"], 3);
        assert!(
            body.get("staging_path").is_none(),
            "staging_path must not leak to the client"
        );
        let claimed_sha = body["sha256"].as_str().unwrap();
        let claimed_bytes = body["bytes_received"].as_u64().unwrap();

        // Recompute sha256 + size of the actual file, verify the PutResult
        // claim matches byte-for-byte.
        let actual_bytes = std::fs::metadata(&staged).unwrap().len();
        assert_eq!(claimed_bytes, actual_bytes);
        let file_bytes = std::fs::read(&staged).unwrap();
        let mut hasher = Sha256::new();
        hasher.update(&file_bytes);
        let actual_sha: String = hasher
            .finalize()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        assert_eq!(claimed_sha, actual_sha);
    }

    #[tokio::test]
    async fn do_put_rejects_expired_ticket() {
        let tmp = tempfile::tempdir().unwrap();
        let service = make_service(tmp.path().to_path_buf());
        let expired = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            - 1000;
        let ticket = sign_doput_for_test(1, b"dev-secret", expired);
        let messages = flight_stream_with_ticket(vec![sample_batch()], ticket).await;

        let err = service
            .do_put_inner(stream::iter(messages))
            .await
            .expect_err("expired ticket must be rejected");
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    #[tokio::test]
    async fn do_put_rejects_bad_hmac() {
        let tmp = tempfile::tempdir().unwrap();
        let service = make_service(tmp.path().to_path_buf());
        // Sign with a different secret than the service holds.
        let ticket = sign_doput_for_test(1, b"wrong-secret", future_expiry_secs(300));
        let messages = flight_stream_with_ticket(vec![sample_batch()], ticket).await;

        let err = service
            .do_put_inner(stream::iter(messages))
            .await
            .expect_err("bad HMAC must be rejected");
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    #[tokio::test]
    async fn do_put_rejects_missing_descriptor() {
        let tmp = tempfile::tempdir().unwrap();
        let service = make_service(tmp.path().to_path_buf());
        // A stream whose first message has no descriptor at all.
        let messages: Vec<Result<FlightData, Status>> = vec![Ok(FlightData::default())];

        let err = service
            .do_put_inner(stream::iter(messages))
            .await
            .expect_err("missing descriptor must be rejected");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn do_put_rejects_empty_cmd() {
        let tmp = tempfile::tempdir().unwrap();
        let service = make_service(tmp.path().to_path_buf());
        let fd = FlightData {
            flight_descriptor: Some(FlightDescriptor::new_cmd(Vec::<u8>::new())),
            ..Default::default()
        };
        let messages: Vec<Result<FlightData, Status>> = vec![Ok(fd)];

        let err = service
            .do_put_inner(stream::iter(messages))
            .await
            .expect_err("empty cmd must be rejected");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn do_put_interrupted_stream_leaves_no_parquet() {
        let tmp = tempfile::tempdir().unwrap();
        let service = make_service(tmp.path().to_path_buf());
        let ticket = sign_doput_for_test(99, b"dev-secret", future_expiry_secs(300));

        // Build a valid first message (descriptor + schema), then yield an
        // Err mid-stream before any batch lands. The handler should
        // surface the error AND leave nothing in the staging directory.
        let mut messages = flight_stream_with_ticket(vec![sample_batch()], ticket).await;
        // Truncate to schema only, then inject an error.
        messages.truncate(1);
        messages.push(Err(Status::internal("simulated mid-stream drop")));

        let err = service
            .do_put_inner(stream::iter(messages))
            .await
            .expect_err("interrupted stream must surface an error");
        assert_eq!(err.code(), tonic::Code::Internal);

        let staged = tmp.path().join("uploads/99/upload.parquet");
        assert!(
            !staged.exists(),
            "partial parquet must be deleted on interrupt; found {}",
            staged.display()
        );
    }

    #[tokio::test]
    async fn do_put_same_upload_idx_second_attempt_rejected() {
        // After a successful DoPut to upload_idx=N, a second DoPut to the
        // same N must fail with AlreadyExists rather than silently
        // clobbering the staged file. The CP doesn't reissue tickets, but
        // a malicious / buggy client could replay a still-valid one.
        let tmp = tempfile::tempdir().unwrap();
        let service = make_service(tmp.path().to_path_buf());

        let ticket = sign_doput_for_test(7, b"dev-secret", future_expiry_secs(300));
        let m1 = flight_stream_with_ticket(vec![sample_batch()], ticket.clone()).await;
        service
            .do_put_inner(stream::iter(m1))
            .await
            .expect("first DoPut should succeed");

        let m2 = flight_stream_with_ticket(vec![sample_batch()], ticket).await;
        let err = service
            .do_put_inner(stream::iter(m2))
            .await
            .expect_err("second DoPut to the same upload_idx must be rejected");
        assert_eq!(err.code(), tonic::Code::AlreadyExists);

        // The first DoPut's file survives (still mode 440); it was not
        // clobbered by the failed second attempt.
        let staged = tmp.path().join("uploads/7/upload.parquet");
        let perms = std::fs::metadata(&staged).unwrap().permissions();
        assert_eq!(perms.mode() & 0o777, 0o440);
    }

    #[tokio::test]
    async fn do_put_concurrent_uploads_are_isolated() {
        // Two uploads to different upload_idx values land at different
        // staging paths and don't trample each other. Smoke test that the
        // QiitaFlightService has no shared mutable state.
        let tmp = tempfile::tempdir().unwrap();
        let service = make_service(tmp.path().to_path_buf());

        let t1 = sign_doput_for_test(1, b"dev-secret", future_expiry_secs(300));
        let t2 = sign_doput_for_test(2, b"dev-secret", future_expiry_secs(300));
        let m1 = flight_stream_with_ticket(vec![sample_batch()], t1).await;
        let m2 = flight_stream_with_ticket(vec![sample_batch()], t2).await;

        let (r1, r2) = futures::join!(
            service.do_put_inner(stream::iter(m1)),
            service.do_put_inner(stream::iter(m2)),
        );
        r1.unwrap();
        r2.unwrap();

        assert!(tmp.path().join("uploads/1/upload.parquet").exists());
        assert!(tmp.path().join("uploads/2/upload.parquet").exists());
    }

    #[test]
    fn staging_path_for_layout() {
        let root = Path::new("/scratch/ephemeral/staging");
        assert_eq!(
            staging_path_for(root, 42),
            Path::new("/scratch/ephemeral/staging/uploads/42/upload.parquet")
        );
    }
}
