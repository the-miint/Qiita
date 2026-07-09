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

use arrow_array::RecordBatch;
use arrow_flight::decode::{DecodedPayload, FlightDataDecoder};
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
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
use tokio_stream::wrappers::ReceiverStream;
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
    /// The `PATH_SCRATCH` base root (parent of `upload_staging_root`). The
    /// `export_read` DoAction validates that its requested destination — a
    /// control-plane ticket workspace path under `{PATH_SCRATCH}/ticket/...` —
    /// resolves under this root before writing.
    scratch_root: PathBuf,
}

impl QiitaFlightService {
    pub fn new(
        hmac_secret: Vec<u8>,
        catalog_connstr: String,
        data_path: String,
        upload_staging_root: PathBuf,
        scratch_root: PathBuf,
    ) -> Self {
        Self {
            hmac_secret,
            catalog_connstr,
            data_path,
            upload_staging_root,
            scratch_root,
        }
    }
}

/// Open a fresh in-memory DuckDB connection with DuckLake attached. Each
/// request gets its own connection — no shared state. A free function (not a
/// method) so the DoGet streaming task can open the connection on its own
/// blocking thread from owned connstr/data_path, without borrowing `self`.
fn open_ducklake(catalog_connstr: &str, data_path: &str) -> Result<Connection, Status> {
    let conn = Connection::open_in_memory()
        .map_err(|e| Status::internal(format!("failed to open DuckDB: {e}")))?;
    ducklake::connect_ducklake(&conn, catalog_connstr, data_path)
        .map_err(|e| Status::internal(format!("failed to attach DuckLake: {e}")))?;
    Ok(conn)
}

/// Bounded depth of the DoGet batch channel. Backpressure: the blocking producer
/// parks on `blocking_send` when the channel is full until the async consumer
/// drains it, so peak memory is ~this many RecordBatches in flight rather than
/// the whole result set.
const DOGET_BATCH_CHANNEL_DEPTH: usize = 4;

/// Stream a DuckLake query's RecordBatches over a bounded channel.
///
/// `query_arrow` yields an iterator that borrows the `Statement`, which borrows
/// the `Connection`; the `DoGetStream` we return must be `'static`, so the
/// DuckDB iterator cannot be streamed directly. Instead a blocking task owns the
/// connection for the query's lifetime and pushes each `RecordBatch` into a
/// bounded channel, and the returned stream drains the receiver. This replaces
/// the old `.collect()` that buffered the entire result set in memory; peak
/// memory is now bounded by `DOGET_BATCH_CHANNEL_DEPTH` (see above).
///
/// A zero-row result still emits one empty `RecordBatch` carrying the schema, so
/// the client always receives a valid (possibly empty) Arrow table — the same
/// contract the buffered path had. A connect/prepare/execute error surfaces as a
/// single `Err` item, never a silently-truncated empty stream.
///
/// Caveat — mid-stream truncation is indistinguishable from completion
/// (pre-existing, shared with the old `.collect()` path, and bounded by the
/// DuckDB API): the DuckDB Arrow iterator's `Item` is a bare `RecordBatch`, not
/// a `Result`, so a failure that occurs *mid-iteration* (after at least one
/// batch has been sent) cannot be surfaced as an error. The iterator simply
/// terminates early, the channel closes, and the consumer sees a clean EOF —
/// byte-for-byte identical to a successful, complete stream. A DoGet client
/// therefore CANNOT tell a truncated result from a whole one on the wire; only
/// connect/prepare/execute errors, which occur *before* the first batch, become
/// an `Err` item the client can see.
///
/// We accept this rather than work around it: the fix would require the upstream
/// `duckdb` crate to yield `Result<RecordBatch>` from its Arrow iterator (it does
/// not today), and there is no in-crate seam to inject a trailing sentinel that
/// survives the `FlightDataEncoder`. Mid-iteration failures are also rare in
/// practice — the query is already prepared and executing, and the batches are
/// read from local/attached storage. Callers that need end-to-end integrity
/// verify it out-of-band (row counts, digests) rather than trusting stream
/// termination. If the `duckdb` API ever exposes a fallible per-batch iterator,
/// revisit this to surface mid-stream errors.
fn stream_ducklake_batches(
    catalog_connstr: String,
    data_path: String,
    sql: String,
    table: String,
) -> ReceiverStream<Result<RecordBatch, FlightError>> {
    let (tx, rx) =
        tokio::sync::mpsc::channel::<Result<RecordBatch, FlightError>>(DOGET_BATCH_CHANNEL_DEPTH);
    // JoinHandle dropped intentionally: the blocking task runs to completion
    // independently, draining into `tx`. Don't `.await`/`.join()` it — the
    // result is delivered through the channel, not the handle.
    tokio::task::spawn_blocking(move || {
        let produce = || -> Result<(), Status> {
            let conn = open_ducklake(&catalog_connstr, &data_path)?;
            let mut stmt = conn.prepare(&sql).map_err(|e| {
                Status::internal(format!("query preparation failed for {table}: {e}"))
            })?;
            let arrow_result = stmt.query_arrow([]).map_err(|e| {
                Status::internal(format!("query execution failed for {table}: {e}"))
            })?;
            let schema = arrow_result.get_schema();
            let mut produced = false;
            // `arrow_result`'s Item is a bare `RecordBatch`, not a `Result` — a
            // failure once iteration has begun cannot be observed here; the loop
            // just ends and the consumer sees a clean EOF (see the fn-level
            // caveat). Nothing to do about it until the duckdb API is fallible.
            for batch in arrow_result {
                produced = true;
                // Receiver dropped (client hung up) — stop early, don't error.
                if tx.blocking_send(Ok(batch)).is_err() {
                    return Ok(());
                }
            }
            if !produced {
                // Preserve the schema for a zero-row result.
                let _ = tx.blocking_send(Ok(RecordBatch::new_empty(schema)));
            }
            Ok(())
        };
        if let Err(status) = produce() {
            // Surface the producer error as a stream item (ignore send failure —
            // the consumer is already gone).
            let _ = tx.blocking_send(Err(FlightError::ExternalError(Box::new(
                std::io::Error::other(status.message().to_string()),
            ))));
        }
    });
    ReceiverStream::new(rx)
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

/// DoAction variants that are safe to replay — the accepted-risk registry.
///
/// Flight action tokens are HMAC-authenticated but carry **no single-use
/// ledger**: within a token's lifetime (bounded by `MAX_TICKET_LIFETIME`, ~1h)
/// a captured, still-valid token can be replayed. We deliberately do NOT add a
/// server-side nonce/consumed-token store — the operational cost of one is not
/// justified because every action the data plane dispatches is idempotent or
/// otherwise replay-safe (see `docs/auth.md#ticket-replay`):
///
/// - `register_files` — dest names are ticket-unique and `move_file` refuses to
///   overwrite, so a replay after success fails closed (AlreadyExists), never a
///   double-registration.
/// - `delete_reference` / `delete_mask` / `delete_pool_reads` /
///   `delete_read_mask_block` — logical DELETEs; re-running deletes zero rows.
/// - `export_read` / `export_read_block` — re-materialize the same sample/block
///   bytes to the same ticket path via atomic publish; a replay reproduces
///   identical output.
/// - `count_masked` / `mask_metrics` — read-only aggregates.
///
/// The `do_action` dispatcher rejects any action not in this set, so a **new**
/// action is refused until it is added here — forcing whoever adds it to
/// consciously classify it idempotent/replay-safe or give it replay protection
/// first. `replay_safe_actions_matches_dispatcher` in the tests pins the set to
/// the dispatcher's handled arms so the two can't drift.
const REPLAY_SAFE_ACTIONS: &[&str] = &[
    "register_files",
    "delete_reference",
    "delete_mask",
    "delete_pool_reads",
    "delete_read_mask_block",
    "export_read",
    "export_read_block",
    "count_masked",
    "mask_metrics",
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

        // Stream the result incrementally. Each request gets its own DuckDB
        // connection + DuckLake snapshot, opened on a blocking task that feeds
        // RecordBatches through a bounded channel — so the data plane never
        // buffers the whole result set (the non-blocking, memory-bounded path).
        // Ticket/table/query-shape errors above are returned synchronously;
        // per-request DB errors (connect/prepare/execute) surface as the first
        // stream item (see stream_ducklake_batches).
        let batch_stream = stream_ducklake_batches(
            self.catalog_connstr.clone(),
            self.data_path.clone(),
            sql,
            table,
        );
        let flight_stream = FlightDataEncoderBuilder::new().build(batch_stream);
        let mapped = flight_stream.map(|result| {
            result.map_err(|e| Status::internal(format!("data plane stream error: {e}")))
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

        // replay: Flight action tokens are HMAC-authenticated but have NO
        // single-use ledger — a captured, still-valid token can be replayed
        // within its lifetime. We accept that risk (see docs/auth.md and the
        // REPLAY_SAFE_ACTIONS registry) because every arm below is idempotent or
        // otherwise replay-safe. The registry is the gate: an action absent from
        // it is rejected here, so a newly-added arm stays unreachable until it
        // is consciously classified replay-safe (or given replay protection),
        // and the match's `other =>` arm below is a defensive, unreachable
        // fail-closed fallback. Keep this set and the match arms in lockstep —
        // the test `replay_safe_actions_matches_dispatcher` fails otherwise.
        if !REPLAY_SAFE_ACTIONS.contains(&action.r#type.as_str()) {
            return Err(Status::invalid_argument(format!(
                "unknown action type: {:?}",
                action.r#type
            )));
        }

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

                // register_files moves files and runs a blocking DuckLake
                // transaction; run it on the blocking pool so it never starves a
                // tonic async worker (mirrors export_read / count_masked). The
                // closure opens and drops its own connection, so it is Send and
                // crosses no await.
                let catalog = self.catalog_connstr.clone();
                let data_path = self.data_path.clone();
                let registered = tokio::task::spawn_blocking(move || {
                    register_files(&catalog, &data_path, &payload)
                })
                .await
                .map_err(|e| Status::internal(format!("register_files task join failed: {e}")))??;

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

                // Blocking DuckLake delete transaction — run it on the blocking
                // pool so it never starves a tonic async worker (mirrors
                // export_read / count_masked). The closure opens and drops its
                // own connection, so it is Send and crosses no await.
                let catalog = self.catalog_connstr.clone();
                let data_path = self.data_path.clone();
                let reference_idx = payload.reference_idx;
                let deleted = tokio::task::spawn_blocking(move || {
                    delete_reference(&catalog, &data_path, reference_idx)
                })
                .await
                .map_err(|e| {
                    Status::internal(format!("delete_reference task join failed: {e}"))
                })??;

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

                // Blocking DuckLake delete transaction — run it on the blocking
                // pool so it never starves a tonic async worker (mirrors
                // export_read / count_masked). The closure opens and drops its
                // own connection, so it is Send and crosses no await.
                let catalog = self.catalog_connstr.clone();
                let data_path = self.data_path.clone();
                let mask_idx = payload.mask_idx;
                let deleted = tokio::task::spawn_blocking(move || {
                    delete_mask(&catalog, &data_path, mask_idx)
                })
                .await
                .map_err(|e| Status::internal(format!("delete_mask task join failed: {e}")))??;

                let result_body = serde_json::to_vec(&deleted)
                    .map_err(|e| Status::internal(format!("json serialization failed: {e}")))?;

                let result = arrow_flight::Result {
                    body: result_body.into(),
                };
                let output = stream::once(futures::future::ready(Ok(result)));
                Ok(Response::new(Box::pin(output)))
            }
            "delete_pool_reads" => {
                let payload = auth::verify_delete_pool_reads(&action.body, &self.hmac_secret)
                    .map_err(|e| Status::unauthenticated(e.to_string()))?;

                if payload.action != "delete_pool_reads" {
                    return Err(Status::invalid_argument(format!(
                        "action type mismatch: header says 'delete_pool_reads', payload says {:?}",
                        payload.action
                    )));
                }

                // Blocking DuckLake delete transaction — run it on the blocking
                // pool so it never starves a tonic async worker (mirrors
                // export_read / count_masked). The closure opens and drops its
                // own connection, so it is Send and crosses no await.
                let catalog = self.catalog_connstr.clone();
                let data_path = self.data_path.clone();
                let prep_sample_idxs = payload.prep_sample_idxs;
                let deleted = tokio::task::spawn_blocking(move || {
                    delete_pool_reads(&catalog, &data_path, &prep_sample_idxs)
                })
                .await
                .map_err(|e| {
                    Status::internal(format!("delete_pool_reads task join failed: {e}"))
                })??;

                let result_body = serde_json::to_vec(&deleted)
                    .map_err(|e| Status::internal(format!("json serialization failed: {e}")))?;

                let result = arrow_flight::Result {
                    body: result_body.into(),
                };
                let output = stream::once(futures::future::ready(Ok(result)));
                Ok(Response::new(Box::pin(output)))
            }
            "delete_read_mask_block" => {
                let payload = auth::verify_delete_read_mask_block(&action.body, &self.hmac_secret)
                    .map_err(|e| Status::unauthenticated(e.to_string()))?;

                if payload.action != "delete_read_mask_block" {
                    return Err(Status::invalid_argument(format!(
                        "action type mismatch: header says 'delete_read_mask_block', payload says {:?}",
                        payload.action
                    )));
                }
                // An empty block is a control-plane bug, not a valid ask —
                // reject it loudly rather than deleting nothing silently.
                if payload.members.is_empty() {
                    return Err(Status::invalid_argument(
                        "delete_read_mask_block requires a non-empty members list",
                    ));
                }

                let deleted = delete_read_mask_block(
                    &self.catalog_connstr,
                    &self.data_path,
                    payload.mask_idx,
                    &payload.members,
                )?;

                let result_body = serde_json::to_vec(&deleted)
                    .map_err(|e| Status::internal(format!("json serialization failed: {e}")))?;

                let result = arrow_flight::Result {
                    body: result_body.into(),
                };
                let output = stream::once(futures::future::ready(Ok(result)));
                Ok(Response::new(Box::pin(output)))
            }
            "export_read" => {
                let payload = auth::verify_export_read(&action.body, &self.hmac_secret)
                    .map_err(|e| Status::unauthenticated(e.to_string()))?;

                if payload.action != "export_read" {
                    return Err(Status::invalid_argument(format!(
                        "action type mismatch: header says 'export_read', payload says {:?}",
                        payload.action
                    )));
                }

                // Defense in depth on the HMAC-trusted destination before it is
                // inlined into a DuckDB `COPY ... TO` literal and written to.
                let dest = validate_export_dest(&payload.dest, &self.scratch_root)?;

                // `COPY` is synchronous and, for a whole sample, long-lived —
                // run it on the blocking pool so it never starves a tonic async
                // worker. The closure opens and drops its own connection, so it
                // is Send and crosses no await (mirrors `register_files`).
                let catalog = self.catalog_connstr.clone();
                let data_path = self.data_path.clone();
                let scratch_root = self.scratch_root.clone();
                let prep = payload.prep_sample_idx;
                let count = tokio::task::spawn_blocking(move || {
                    export_read_to_parquet(&catalog, &data_path, prep, &dest, &scratch_root)
                })
                .await
                .map_err(|e| Status::internal(format!("export_read task join failed: {e}")))??;

                let result_body = serde_json::to_vec(&serde_json::json!({
                    "count": count,
                    "dest": payload.dest,
                }))
                .map_err(|e| Status::internal(format!("json serialization failed: {e}")))?;

                let result = arrow_flight::Result {
                    body: result_body.into(),
                };
                let output = stream::once(futures::future::ready(Ok(result)));
                Ok(Response::new(Box::pin(output)))
            }
            "export_read_block" => {
                let payload = auth::verify_export_read_block(&action.body, &self.hmac_secret)
                    .map_err(|e| Status::unauthenticated(e.to_string()))?;

                if payload.action != "export_read_block" {
                    return Err(Status::invalid_argument(format!(
                        "action type mismatch: header says 'export_read_block', payload says {:?}",
                        payload.action
                    )));
                }
                // An empty block is a control-plane bug, not a valid ask —
                // reject it loudly rather than silently writing an empty file.
                if payload.members.is_empty() {
                    return Err(Status::invalid_argument(
                        "export_read_block requires a non-empty members list",
                    ));
                }

                // Defense in depth on the HMAC-trusted destination before it is
                // inlined into a DuckDB `COPY ... TO` literal and written to.
                let dest = validate_export_dest(&payload.dest, &self.scratch_root)?;

                // `COPY` is synchronous and, for a ~10M-read block, long-lived —
                // run it on the blocking pool so it never starves a tonic async
                // worker. The closure opens and drops its own connection, so it
                // is Send and crosses no await (mirrors `export_read`).
                let catalog = self.catalog_connstr.clone();
                let data_path = self.data_path.clone();
                let scratch_root = self.scratch_root.clone();
                let members = payload.members;
                let count = tokio::task::spawn_blocking(move || {
                    export_read_block_to_parquet(
                        &catalog,
                        &data_path,
                        &members,
                        &dest,
                        &scratch_root,
                    )
                })
                .await
                .map_err(|e| {
                    Status::internal(format!("export_read_block task join failed: {e}"))
                })??;

                let result_body = serde_json::to_vec(&serde_json::json!({
                    "count": count,
                    "dest": payload.dest,
                }))
                .map_err(|e| Status::internal(format!("json serialization failed: {e}")))?;

                let result = arrow_flight::Result {
                    body: result_body.into(),
                };
                let output = stream::once(futures::future::ready(Ok(result)));
                Ok(Response::new(Box::pin(output)))
            }
            "count_masked" => {
                // Reuse the *DoGet* read_masked ticket rather than minting a
                // bespoke action token: counting the rows a ticket selects is
                // strictly less than streaming them, so the ticket's
                // (prep_sample_idx, mask_idx) authorization already covers it —
                // no new ticket type or control-plane route is needed, the CLI
                // sends the same signed bytes it would otherwise stream with.
                let payload = auth::verify_ticket(&action.body, &self.hmac_secret)
                    .map_err(|e| Status::unauthenticated(e.to_string()))?;
                if payload.table != "read_masked" {
                    return Err(Status::invalid_argument(format!(
                        "count_masked requires a read_masked ticket, got table {:?}",
                        payload.table
                    )));
                }
                let prep_sample_idx = single_i64_filter(&payload.filter, "prep_sample_idx")?;
                let mask_idx = single_i64_filter(&payload.filter, "mask_idx")?;

                // count(*) is synchronous DuckDB work; run it on the blocking
                // pool so it never starves a tonic async worker (mirrors
                // export_read). The closure opens and drops its own connection.
                let catalog = self.catalog_connstr.clone();
                let data_path = self.data_path.clone();
                let count = tokio::task::spawn_blocking(move || {
                    count_masked_reads(&catalog, &data_path, prep_sample_idx, mask_idx)
                })
                .await
                .map_err(|e| Status::internal(format!("count_masked task join failed: {e}")))??;

                let result_body = serde_json::to_vec(&serde_json::json!({ "count": count }))
                    .map_err(|e| Status::internal(format!("json serialization failed: {e}")))?;
                let result = arrow_flight::Result {
                    body: result_body.into(),
                };
                let output = stream::once(futures::future::ready(Ok(result)));
                Ok(Response::new(Box::pin(output)))
            }
            "mask_metrics" => {
                // Unlike `count_masked` (which reuses a `read_masked` DoGet
                // ticket the CLI already holds), the block reconcile primitive
                // runs control-plane-side and signs a first-class action token —
                // so this arm verifies a `mask_metrics` payload, not a ticket.
                let payload = auth::verify_mask_metrics(&action.body, &self.hmac_secret)
                    .map_err(|e| Status::unauthenticated(e.to_string()))?;
                if payload.action != "mask_metrics" {
                    return Err(Status::invalid_argument(format!(
                        "action type mismatch: header says 'mask_metrics', payload says {:?}",
                        payload.action
                    )));
                }
                let mask_idx = payload.mask_idx;
                let prep_sample_idx = payload.prep_sample_idx;

                // The aggregate is a synchronous DuckDB count over the light
                // `read_mask` table; run it on the blocking pool so it never
                // starves a tonic async worker (mirrors count_masked). The
                // closure opens and drops its own connection.
                let catalog = self.catalog_connstr.clone();
                let data_path = self.data_path.clone();
                let counts = tokio::task::spawn_blocking(move || {
                    mask_metrics_counts(&catalog, &data_path, mask_idx, prep_sample_idx)
                })
                .await
                .map_err(|e| Status::internal(format!("mask_metrics task join failed: {e}")))??;

                let result_body = serde_json::to_vec(&counts)
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

/// Bounded depth of the DoPut decoder→writer channel. The blocking writer task
/// parks the async decoder on `send` when the channel is full, so peak memory is
/// ~this many decoded payloads in flight rather than the whole upload. A payload
/// is one client RecordBatch, which the chunked-upload path sizes up to ~1 GiB
/// (see `FLIGHT_MAX_DECODING_BYTES` in main.rs), so this is deliberately small —
/// enough to overlap decode and write without buffering many large batches.
/// Mirrors `DOGET_BATCH_CHANNEL_DEPTH`.
const DOPUT_WRITER_CHANNEL_DEPTH: usize = 4;

/// One decoded payload forwarded from the async decoder to the blocking writer.
enum DoPutWriterMsg {
    Schema(arrow_schema::SchemaRef),
    Batch(RecordBatch),
}

/// Drive the Flight stream through a Parquet writer; return
/// `(sha256_hex, row_count, bytes_received)`. The caller owns staging-path
/// cleanup on Err.
///
/// The Parquet write, `fsync`, and hashing are all blocking, but they must be
/// interleaved with `decoder.next().await` on the live tonic stream — so this
/// can't be a single `spawn_blocking`. Instead it bridges the two: an async loop
/// pulls decoded payloads off the stream and forwards each to a `spawn_blocking`
/// writer task over a bounded mpsc channel; the writer task owns the file and
/// does all blocking I/O off the async runtime. The bounded channel
/// backpressures the decoder (and thus the network) when the writer falls
/// behind, so peak memory stays bounded (`DOPUT_WRITER_CHANNEL_DEPTH`) — the
/// same posture as the DoGet streaming path.
async fn write_doput_parquet<S>(
    staging_path: PathBuf,
    first: FlightData,
    stream: S,
) -> Result<(String, u64, u64), Status>
where
    S: Stream<Item = Result<FlightData, Status>> + Send + Unpin + 'static,
{
    let (tx, mut rx) = tokio::sync::mpsc::channel::<DoPutWriterMsg>(DOPUT_WRITER_CHANNEL_DEPTH);

    // Blocking writer task: owns the file + ArrowWriter, consumes payloads until
    // the channel closes, then closes + fsyncs and returns the digest. All file
    // I/O lives here, off the tonic async worker.
    let writer_task = tokio::task::spawn_blocking(move || -> Result<(String, u64, u64), Status> {
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

        while let Some(msg) = rx.blocking_recv() {
            match msg {
                DoPutWriterMsg::Schema(schema) => {
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
                            _ => {
                                Status::internal(format!("create {}: {e}", staging_path.display()))
                            }
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
                DoPutWriterMsg::Batch(batch) => {
                    let w = writer.as_mut().ok_or_else(|| {
                        Status::invalid_argument("RecordBatch arrived before Schema")
                    })?;
                    row_count += batch.num_rows() as u64;
                    w.write(&batch)
                        .map_err(|e| Status::internal(format!("parquet write: {e}")))?;
                }
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
    });

    // Async side: re-prepend the first message and map Status → FlightError so
    // the arrow-flight decoder can consume it, then forward each decoded payload
    // to the writer task.
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

    // A mid-stream decode error must win over whatever the writer task reports:
    // once we drop `tx`, the task finishes "normally" on a partial file. Capture
    // the decode error and return it after joining the task (the caller then
    // cleans up the partial file).
    let mut decode_err: Option<Status> = None;
    while let Some(item) = decoder.next().await {
        let decoded = match item {
            Ok(d) => d,
            Err(e) => {
                decode_err = Some(Status::internal(format!("flight decode: {e}")));
                break;
            }
        };
        let msg = match decoded.payload {
            DecodedPayload::Schema(schema) => DoPutWriterMsg::Schema(schema),
            DecodedPayload::RecordBatch(batch) => DoPutWriterMsg::Batch(batch),
            DecodedPayload::None => continue,
        };
        // A send error means the writer task already returned (an internal
        // error, e.g. AlreadyExists on create_new). Stop forwarding; the task's
        // Result carries the real cause.
        if tx.send(msg).await.is_err() {
            break;
        }
    }
    // Close the channel so the writer task can finish (drain buffered payloads,
    // then close + fsync).
    drop(tx);

    // Fold a task-panic join error into the same Result shape as the task body.
    let task_result: Result<(String, u64, u64), Status> = writer_task.await.unwrap_or_else(|e| {
        Err(Status::internal(format!(
            "doput writer task join failed: {e}"
        )))
    });

    // Error precedence. An `AlreadyExists` from the writer (it lost the
    // create_new race for this upload_idx) MUST win over a mid-stream decode
    // error: do_put_inner skips its partial-file cleanup ONLY for AlreadyExists,
    // and that staged file belongs to the concurrent, legitimate DoPut —
    // surfacing the decode error instead would let the cleanup unlink their
    // file. For every other outcome the decode error wins, because the writer
    // may have "succeeded" on a truncated file that the caller must clean up.
    if let Err(ref e) = task_result {
        if e.code() == tonic::Code::AlreadyExists {
            return task_result;
        }
    }
    if let Some(e) = decode_err {
        return Err(e);
    }
    task_result
}

/// Canonical Parquet write options for an exported reads file — Parquet v2 +
/// zstd, matching `qiita_common.parquet.PARQUET_OPTS` so the materialized file
/// is shape-identical to the durable copy `ingest_reads` first wrote.
const EXPORT_READ_PARQUET_OPTS: &str =
    "FORMAT PARQUET, PARQUET_VERSION 'v2', COMPRESSION 'zstd', ROW_GROUP_SIZE_BYTES '64MB'";

/// Validate a control-plane-signed `export_read` destination before the data
/// plane writes to it. The token is HMAC-trusted, so this is defense in depth:
/// the dest must be absolute, contain no single quote (it is inlined into a
/// DuckDB `COPY ... TO '<dest>'` literal), carry no `..`/prefix component, and
/// resolve under the data plane's scratch root (the shared tree the control
/// plane's ticket workspaces live under). Returns the validated path.
fn validate_export_dest(dest: &str, scratch_root: &Path) -> Result<PathBuf, Status> {
    let path = Path::new(dest);
    if !path.is_absolute() {
        return Err(Status::invalid_argument(format!(
            "export dest must be an absolute path: {dest:?}"
        )));
    }
    if dest.contains('\'') {
        return Err(Status::invalid_argument(format!(
            "export dest must not contain a single quote: {dest:?}"
        )));
    }
    if path.components().any(|c| {
        matches!(
            c,
            std::path::Component::ParentDir | std::path::Component::Prefix(_)
        )
    }) {
        return Err(Status::invalid_argument(format!(
            "export dest must not contain '..' or a prefix component: {dest:?}"
        )));
    }
    if !path.starts_with(scratch_root) {
        return Err(Status::invalid_argument(format!(
            "export dest {dest:?} is not under the data plane scratch root {}",
            scratch_root.display()
        )));
    }
    Ok(path.to_path_buf())
}

/// Re-materialize a selection of the DuckLake `read` table into a Parquet at
/// `dest`, filtered by the caller-supplied `where_clause` (an already-safe SQL
/// predicate — HMAC-verified inlined integers only). Returns the row count.
///
/// Shared machinery for the read-export DoActions: `export_read` (one whole
/// sample) and `export_read_block` (the union of a block's `(prep_sample,
/// sub-range)` members). An empty selection writes NO file and returns 0 — the
/// control plane turns that into a clean submission failure. The `COPY` streams
/// row groups to disk, so memory stays bounded regardless of selection size.
/// Opens and drops its own connection so the caller can run it on the blocking
/// pool (mirrors `register_files`).
///
/// `dest` arrives already lexically validated (`validate_export_dest`), but this
/// is human-read data, so we ALSO resolve symlinks: another job on the shared
/// scratch tree could plant a symlink to redirect the write outside the
/// controlled workspace. We `canonicalize` the (created) parent and re-assert it
/// resolves under `scratch_root` before writing. The file is then published
/// atomically (write a sibling `.partial`, then rename) so `dest` only ever
/// appears complete — a failed or partial `COPY` never leaves a half-written
/// `reads.parquet` a retry could read. The row count is read back from the
/// written file, so it always matches the bytes on disk (no separate catalog
/// scan that could race the `COPY`).
fn export_read_where_to_parquet(
    catalog_connstr: &str,
    data_path: &str,
    where_clause: &str,
    dest: &Path,
    scratch_root: &Path,
) -> Result<i64, Status> {
    let conn = duckdb::Connection::open_in_memory()
        .map_err(|e| Status::internal(format!("failed to open DuckDB: {e}")))?;
    ducklake::connect_ducklake(&conn, catalog_connstr, data_path)
        .map_err(|e| Status::internal(format!("failed to attach DuckLake: {e}")))?;
    // ROW_GROUP_SIZE_BYTES (in EXPORT_READ_PARQUET_OPTS) requires insertion
    // order NOT be preserved — and we don't need it: the file is unordered
    // because every consumer (qc is per-row, host_filter collapses with
    // DISTINCT) is order-independent.
    conn.execute_batch("SET preserve_insertion_order = false;")
        .map_err(|e| Status::internal(format!("failed to set preserve_insertion_order: {e}")))?;

    let parent = dest.parent().ok_or_else(|| {
        Status::internal(format!("export dest has no parent: {}", dest.display()))
    })?;
    std::fs::create_dir_all(parent)
        .map_err(|e| Status::internal(format!("failed to create {}: {e}", parent.display())))?;

    // Symlink-safe containment: the lexical `validate_export_dest` check is not
    // enough on a shared scratch tree, where another job could plant a symlink
    // that redirects these (human) reads outside the controlled workspace.
    // Canonicalize the now-existing parent and re-assert it resolves under the
    // scratch root before we write anything.
    let real_parent = std::fs::canonicalize(parent)
        .map_err(|e| Status::internal(format!("failed to resolve {}: {e}", parent.display())))?;
    let real_root = std::fs::canonicalize(scratch_root).map_err(|e| {
        Status::internal(format!(
            "failed to resolve scratch root {}: {e}",
            scratch_root.display()
        ))
    })?;
    if !real_parent.starts_with(&real_root) {
        return Err(Status::permission_denied(format!(
            "export dest parent {} resolves outside the scratch root {}",
            real_parent.display(),
            real_root.display()
        )));
    }

    // Write to a sibling temp, then publish atomically. `dest` is validated
    // (absolute, under the scratch root, no `..`, no single quote) and the
    // `.partial` suffix preserves all of that; the `where_clause` carries only
    // HMAC-verified inlined integers — all safe to inline. The column list is
    // the full `read` schema in table order, so the file is a drop-in for the
    // durable staging copy (modulo row order, which does not matter).
    let tmp = {
        let mut s = dest.as_os_str().to_os_string();
        s.push(".partial");
        PathBuf::from(s)
    };
    let tmp_sql = tmp
        .to_str()
        .ok_or_else(|| Status::internal(format!("non-UTF-8 dest path: {}", tmp.display())))?;
    let copy_sql = format!(
        "COPY (SELECT prep_sample_idx, sequence_idx, read_id, sequence1, qual1, sequence2, qual2 \
         FROM qiita_lake.read WHERE {where_clause}) \
         TO '{tmp_sql}' ({EXPORT_READ_PARQUET_OPTS})"
    );

    // The fallible sequence is isolated so the temp file is cleaned up on the
    // empty path (count 0) and on any error; on success it is renamed away.
    let published = (|| -> Result<i64, Status> {
        conn.execute_batch(&copy_sql)
            .map_err(|e| Status::internal(format!("read export COPY failed: {e}")))?;
        // Count from the file we just wrote, so it matches the bytes exactly.
        let count: i64 = conn
            .query_row(
                &format!("SELECT count(*) FROM read_parquet('{tmp_sql}')"),
                [],
                |row| row.get(0),
            )
            .map_err(|e| Status::internal(format!("read export count failed: {e}")))?;
        if count == 0 {
            // Nothing selected — publish nothing; the CP raises.
            return Ok(0);
        }
        // Match the read result-file convention: owner/group read-only.
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o440))
            .map_err(|e| Status::internal(format!("failed to chmod {}: {e}", tmp.display())))?;
        std::fs::rename(&tmp, dest).map_err(|e| {
            Status::internal(format!(
                "failed to publish {} -> {}: {e}",
                tmp.display(),
                dest.display()
            ))
        })?;
        Ok(count)
    })();

    // On the empty path (Ok(0)) or any failure the temp still exists; remove it.
    // On success (Ok(n>0)) it was renamed to `dest`, so this is a no-op.
    if !matches!(published, Ok(n) if n > 0) {
        let _ = std::fs::remove_file(&tmp);
    }
    published
}

/// Re-materialize one prep_sample's reads into a per-ticket `reads.parquet` a
/// read-mask job consumes (the per-sample export). A sample with no stored reads
/// writes NO file and returns 0. `prep_sample_idx` is an HMAC-verified i64, safe
/// to inline. See `export_read_where_to_parquet` for the shared write/publish.
fn export_read_to_parquet(
    catalog_connstr: &str,
    data_path: &str,
    prep_sample_idx: i64,
    dest: &Path,
    scratch_root: &Path,
) -> Result<i64, Status> {
    export_read_where_to_parquet(
        catalog_connstr,
        data_path,
        &format!("prep_sample_idx = {prep_sample_idx}"),
        dest,
        scratch_root,
    )
}

/// Re-materialize a block's reads — the union of its `(prep_sample_idx,
/// sequence_idx sub-range)` members — into a per-ticket `reads.parquet` a
/// read-mask *block* job consumes. Returns the row count (0 ⇒ no file written).
///
/// The WHERE clause is **exact by construction** yet still pushes down. Two
/// coarse conjuncts do the pruning — each DuckLake `read` data file holds exactly
/// one sample sorted by `sequence_idx`, so `prep_sample_idx IN (...)` prunes to
/// the block's files at the catalog level and `sequence_idx BETWEEN block_min AND
/// block_max` prunes row-groups (both are top-level conjuncts DuckDB pushes to
/// the scan). A third conjunct — the per-member OR of `(prep_sample_idx = p AND
/// sequence_idx BETWEEN start AND stop)` — is an exact residual on the already-
/// pruned rows: it guarantees a split member contributes ONLY its own sub-range,
/// so the part of that sample living in a sibling block never leaks, independent
/// of any tiling order or boundary-alignment invariant. The coarse pair is a
/// superset of the OR, so `coarse AND exact == exact`. All member integers are
/// HMAC-verified i64s, safe to inline. An empty `members` list writes no file and
/// returns 0 (the DoAction arm rejects it earlier too).
fn export_read_block_to_parquet(
    catalog_connstr: &str,
    data_path: &str,
    members: &[auth::ExportReadBlockMember],
    dest: &Path,
    scratch_root: &Path,
) -> Result<i64, Status> {
    if members.is_empty() {
        return Ok(0);
    }
    let where_clause = block_read_where_clause(members);
    export_read_where_to_parquet(
        catalog_connstr,
        data_path,
        &where_clause,
        dest,
        scratch_root,
    )
}

/// Build the block export's `read` WHERE clause from its members. Factored out of
/// `export_read_block_to_parquet` so the pushdown performance-assessment test can
/// exercise the EXACT predicate the export emits, not a hand-written copy that
/// could silently drift.
///
/// Two coarse conjuncts drive pruning (both are top-level, so DuckDB pushes them
/// to the scan): `prep_sample_idx IN (...)` prunes DuckLake data files by their
/// per-file `prep_sample_idx` stats, and `sequence_idx BETWEEN block_min AND
/// block_max` bounds the row-group span. A third conjunct — the per-member OR of
/// `(prep_sample_idx = p AND sequence_idx BETWEEN start AND stop)` — is the exact
/// residual on the pruned rows, so a split member never leaks a sibling block's
/// rows (independent of tiling order). The coarse pair is a superset of the OR,
/// so `coarse AND exact == exact`. `members` must be non-empty (caller guards);
/// all integers are HMAC-verified i64s, safe to inline.
fn block_read_where_clause(members: &[auth::ExportReadBlockMember]) -> String {
    let mut preps: Vec<i64> = members.iter().map(|m| m.prep_sample_idx).collect();
    preps.sort_unstable();
    preps.dedup();
    let in_list = preps
        .iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join(",");
    // Unwraps are safe: `members` is non-empty (caller guards).
    let block_min = members.iter().map(|m| m.sequence_idx_start).min().unwrap();
    let block_max = members.iter().map(|m| m.sequence_idx_stop).max().unwrap();
    let member_terms = members
        .iter()
        .map(|m| {
            format!(
                "(prep_sample_idx = {} AND sequence_idx BETWEEN {} AND {})",
                m.prep_sample_idx, m.sequence_idx_start, m.sequence_idx_stop
            )
        })
        .collect::<Vec<_>>()
        .join(" OR ");
    format!(
        "prep_sample_idx IN ({in_list}) \
         AND sequence_idx BETWEEN {block_min} AND {block_max} \
         AND ({member_terms})"
    )
}

/// Pull exactly one i64 out of a ticket filter column. The count path needs a
/// single `prep_sample_idx` / `mask_idx`, not an IN-set, so a missing, empty,
/// multi-valued, or non-integer column is a malformed-ticket error — the export
/// ticket always signs a one-element list per column. Input is HMAC-verified
/// (set by the control plane), but we validate anyway for defense in depth.
fn single_i64_filter(filter: &auth::TicketFilter, col: &str) -> Result<i64, Status> {
    let values = filter.get(col).ok_or_else(|| {
        Status::invalid_argument(format!("count_masked ticket missing filter column {col:?}"))
    })?;
    match values.as_slice() {
        [v] => v.as_i64().ok_or_else(|| {
            Status::invalid_argument(format!(
                "filter value for {col:?} must be an integer, got {v}"
            ))
        }),
        _ => Err(Status::invalid_argument(format!(
            "count_masked requires exactly one value for {col:?}, got {}",
            values.len()
        ))),
    }
}

/// Count the masked reads a `read_masked` ticket selects, without streaming them.
///
/// Runs `count(*)` against the light `read_mask` table (keyed
/// `(mask_idx, prep_sample_idx, sequence_idx)`) rather than the `read_masked`
/// view: every `reason = 'pass'` row in `read_mask` has its `read` row by
/// construction, so the filtered `read_mask` count equals the view's row count —
/// while touching only the small key/`reason` columns, never joining to `read`
/// or materializing the sequence/quality bytes. This is what makes the export's
/// idempotency probe cheap. Opens and drops its own connection so the caller can
/// run it on the blocking pool (mirrors `export_read_to_parquet`).
fn count_masked_reads(
    catalog_connstr: &str,
    data_path: &str,
    prep_sample_idx: i64,
    mask_idx: i64,
) -> Result<i64, Status> {
    let conn = open_ducklake(catalog_connstr, data_path)?;
    // `prep_sample_idx`/`mask_idx` are HMAC-verified i64s, safe to inline (same
    // rationale as build_query: parsed integers reach SQL, no string data); the
    // 'pass' filter mirrors the read_masked view's privacy filter.
    let sql = format!(
        "SELECT count(*) FROM qiita_lake.read_mask \
         WHERE mask_idx = {mask_idx} AND prep_sample_idx = {prep_sample_idx} \
         AND reason = 'pass'"
    );
    conn.query_row(&sql, [], |row| row.get(0))
        .map_err(|e| Status::internal(format!("count_masked query failed: {e}")))
}

/// Aggregate a sample's `read_mask` rows for one mask into the per-stage read
/// counts the block reconcile primitive persists onto `sequenced_sample`.
///
/// The counterpart of the per-sample read-mask's local-parquet rollup
/// (`qiita_control_plane.actions.library._read_mask_counts`), but read from the
/// persisted DuckLake `read_mask` table because a block-masked sample's rows are
/// written by SEVERAL blocks — any one block's local parquet covers only its
/// slice. Returns the both-mates (`*_r1r2`) totals `sequenced_sample` stores plus
/// `row_count` (one per read/pair) the reconcile count-assertion checks against
/// the sample's `sequence_range`.
///
/// `right_trim2` is non-NULL for paired-end and NULL for single-end, so
/// `count(right_trim2)` is the R2 count and `count(*) + count(right_trim2)` is the
/// both-mates total — matching `_read_mask_counts` exactly (SE / PE / mixed, no
/// branching). Bucketing mirrors it too: raw = every row, biological =
/// `reason NOT LIKE 'qc_%'` (pass + host_*), quality_filtered = `reason = 'pass'`.
/// Opens and drops its own connection so the caller can run it on the blocking
/// pool (mirrors `count_masked_reads`).
fn mask_metrics_counts(
    catalog_connstr: &str,
    data_path: &str,
    mask_idx: i64,
    prep_sample_idx: i64,
) -> Result<serde_json::Value, Status> {
    let conn = open_ducklake(catalog_connstr, data_path)?;
    // `mask_idx`/`prep_sample_idx` are HMAC-verified i64s, safe to inline (same
    // rationale as count_masked_reads: parsed integers reach SQL, no string data).
    let sql = format!(
        "SELECT \
           count(*) + count(right_trim2), \
           count(*) FILTER (WHERE reason NOT LIKE 'qc_%') \
             + count(right_trim2) FILTER (WHERE reason NOT LIKE 'qc_%'), \
           count(*) FILTER (WHERE reason = 'pass') \
             + count(right_trim2) FILTER (WHERE reason = 'pass'), \
           count(*) \
         FROM qiita_lake.read_mask \
         WHERE mask_idx = {mask_idx} AND prep_sample_idx = {prep_sample_idx}"
    );
    let (raw, biological, quality_filtered, row_count): (i64, i64, i64, i64) = conn
        .query_row(&sql, [], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })
        .map_err(|e| Status::internal(format!("mask_metrics query failed: {e}")))?;
    Ok(serde_json::json!({
        "raw": raw,
        "biological": biological,
        "quality_filtered": quality_filtered,
        "row_count": row_count,
    }))
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

    // Register every moved file in ONE DuckLake transaction so the catalog
    // update is all-or-nothing (mirrors delete_reference / delete_mask /
    // delete_pool_reads). A failure part-way through the loop rolls back every
    // prior ducklake_add_data_files call rather than leaving the reference
    // half-registered in the catalog.
    //
    // Atomicity here is CATALOG-LEVEL ONLY: the filesystem moves above have
    // already happened and are NOT rolled back. That is intentional and safe.
    // Each dest name is ticket-unique (lake_dest_filename prefixes the work
    // ticket) and move_file refuses to overwrite, so a rolled-back registration
    // leaves at most an unreferenced orphan Parquet on disk — never a collision
    // and never a double-registration. This matches how DuckLake already
    // tolerates orphan Parquets (the delete_* actions reclaim nothing from disk
    // either); a future maintenance pass sweeps them.
    conn.execute_batch("BEGIN TRANSACTION")
        .map_err(|e| Status::internal(format!("failed to begin transaction: {e}")))?;

    let registration = (|| -> Result<Vec<String>, Status> {
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
                Status::internal(format!(
                    "ducklake_add_data_files failed for {table}/{}: {e}",
                    dest.display()
                ))
            })?;
            registered.push(dest_str.to_string());
        }
        Ok(registered)
    })();

    let registered = match registration {
        Ok(registered) => registered,
        Err(e) => {
            // Best-effort rollback; the catalog is left untouched so the control
            // plane can retry from a clean slate. The moved files stay on disk
            // as ticket-unique orphans (see above) — inert until a successful
            // registration references them.
            let _ = conn.execute_batch("ROLLBACK");
            return Err(e);
        }
    };
    conn.execute_batch("COMMIT")
        .map_err(|e| Status::internal(format!("failed to commit registration transaction: {e}")))?;

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

/// Logically delete every `read` and `read_mask` row owned by a set of
/// prep_samples from DuckLake.
///
/// Called when the control plane purges a sequenced_pool: the pool's
/// prep_samples are exclusive to it, so their reads (written once by
/// `ingest_reads`) and any masks over them are orphaned once the pool's Postgres
/// rows are gone. Both tables are keyed by `prep_sample_idx`.
///
/// Mirrors `delete_reference` / `delete_mask`: one DuckLake transaction
/// (all-or-nothing, so a failure leaves both tables intact and the control plane
/// can safely retry), logical `DELETE` only — no raw parquet `unlink` (DuckLake
/// owns file lifecycle; orphan parquets are reclaimed by a future maintenance
/// pass). Idempotent: an empty set, or a set whose rows are already gone,
/// returns zero counts. The `prep_sample_idxs` are `i64` parsed from the
/// HMAC-signed payload, so inlining them into the `IN (...)` list carries no
/// injection surface and avoids per-row parameter binding for the large
/// (hundreds of samples) pool case.
fn delete_pool_reads(
    catalog_connstr: &str,
    data_path: &str,
    prep_sample_idxs: &[i64],
) -> Result<serde_json::Value, Status> {
    // Empty set: nothing to do, and `IN ()` is not valid SQL. Return the
    // zero-count shape without touching the catalog.
    if prep_sample_idxs.is_empty() {
        return Ok(serde_json::json!({
            "prep_sample_count": 0,
            "read_rows_deleted": 0,
            "read_mask_rows_deleted": 0,
        }));
    }

    let conn = duckdb::Connection::open_in_memory()
        .map_err(|e| Status::internal(format!("failed to open DuckDB: {e}")))?;
    ducklake::connect_ducklake(&conn, catalog_connstr, data_path)
        .map_err(|e| Status::internal(format!("failed to attach DuckLake: {e}")))?;

    // i64 literals — no injection surface (see fn docs).
    let in_list = prep_sample_idxs
        .iter()
        .map(|p| p.to_string())
        .collect::<Vec<_>>()
        .join(", ");

    // Both deletes run in one transaction so the action is all-or-nothing and
    // retriable: a mid-delete failure rolls both tables back rather than
    // leaving reads gone but masks behind (or vice versa).
    conn.execute_batch("BEGIN TRANSACTION")
        .map_err(|e| Status::internal(format!("failed to begin transaction: {e}")))?;

    let deletes = (|| -> Result<(usize, usize), Status> {
        let read_rows_deleted = conn
            .execute(
                &format!("DELETE FROM qiita_lake.read WHERE prep_sample_idx IN ({in_list})"),
                [],
            )
            .map_err(|e| Status::internal(format!("delete from read failed: {e}")))?;
        let read_mask_rows_deleted = conn
            .execute(
                &format!("DELETE FROM qiita_lake.read_mask WHERE prep_sample_idx IN ({in_list})"),
                [],
            )
            .map_err(|e| Status::internal(format!("delete from read_mask failed: {e}")))?;
        Ok((read_rows_deleted, read_mask_rows_deleted))
    })();

    let (read_rows_deleted, read_mask_rows_deleted) = match deletes {
        Ok(counts) => counts,
        Err(e) => {
            // Best-effort rollback; surface the original delete error.
            let _ = conn.execute_batch("ROLLBACK");
            return Err(e);
        }
    };
    conn.execute_batch("COMMIT")
        .map_err(|e| Status::internal(format!("failed to commit delete transaction: {e}")))?;

    Ok(serde_json::json!({
        "prep_sample_count": prep_sample_idxs.len(),
        "read_rows_deleted": read_rows_deleted,
        "read_mask_rows_deleted": read_mask_rows_deleted,
    }))
}

/// Delete exactly one block's footprint from the DuckLake `read_mask` table:
/// the rows for `mask_idx` whose `(prep_sample_idx, sequence_idx)` fall in the
/// members' sub-ranges. This is the idempotent-block-replace primitive — the
/// block workflow runs it immediately before `register-files`, so a re-run
/// deletes the prior run's rows before writing fresh ones and never double-counts
/// (the reconcile count-assertion would otherwise trip on a 2× row count).
///
/// The WHERE clause is the SAME exact-by-construction footprint selector
/// `export_read_block` emits (`block_read_where_clause`), scoped further by
/// `mask_idx = ?`: `mask_idx = {m} AND prep_sample_idx IN (...) AND sequence_idx
/// BETWEEN block_min AND block_max AND (per-member OR)`. The per-member OR
/// residual makes it exact — a split member deletes ONLY its own sub-range, so a
/// sibling block's rows for a shared sample survive (independent of tiling
/// order). The coarse `IN + BETWEEN` pair is a pushdown hint (see
/// `block_read_where_clause`).
///
/// Mirrors `delete_mask` / `delete_pool_reads`: one DuckLake transaction
/// (all-or-nothing, retriable — a failed call leaves the block's rows intact so a
/// retry sees the same set), logical `DELETE` only (no raw parquet unlink —
/// DuckLake owns file lifecycle). Idempotent: a fresh block (no rows yet) deletes
/// 0. Empty `members` is a control-plane bug (the DoAction arm rejects it before
/// this); guarded here too, returning a zero-count noop. All integers are
/// HMAC-verified i64s, safe to inline.
fn delete_read_mask_block(
    catalog_connstr: &str,
    data_path: &str,
    mask_idx: i64,
    members: &[auth::ExportReadBlockMember],
) -> Result<serde_json::Value, Status> {
    if members.is_empty() {
        return Ok(serde_json::json!({
            "mask_idx": mask_idx,
            "rows_deleted": 0,
        }));
    }

    let conn = duckdb::Connection::open_in_memory()
        .map_err(|e| Status::internal(format!("failed to open DuckDB: {e}")))?;
    ducklake::connect_ducklake(&conn, catalog_connstr, data_path)
        .map_err(|e| Status::internal(format!("failed to attach DuckLake: {e}")))?;

    // Scope the shared footprint selector to this filtering identity. The
    // `read` export needs no mask column; `read_mask` is keyed by mask_idx too.
    let where_clause = format!(
        "mask_idx = {mask_idx} AND {}",
        block_read_where_clause(members)
    );

    // Single-statement delete wrapped in an explicit transaction so the action
    // is all-or-nothing and the control plane can safely retry: a failed call
    // leaves the block's rows fully intact, so a retry sees the same row set.
    conn.execute_batch("BEGIN TRANSACTION")
        .map_err(|e| Status::internal(format!("failed to begin transaction: {e}")))?;

    let deleted = conn.execute(
        &format!("DELETE FROM qiita_lake.read_mask WHERE {where_clause}"),
        [],
    );

    let rows_deleted = match deleted {
        Ok(n) => n,
        Err(e) => {
            // Best-effort rollback; surface the original delete error.
            let _ = conn.execute_batch("ROLLBACK");
            return Err(Status::internal(format!(
                "delete failed (DELETE FROM qiita_lake.read_mask WHERE {where_clause}): {e}"
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
        // Defense-in-depth against a full-table read leak. `read_masked`
        // exposes per-sample human read data; the control plane scopes each
        // ticket to an explicit (prep_sample_idx, mask_idx) before signing, so
        // an empty filter should never reach here. If the CP ever mis-signed,
        // an empty filter would `SELECT *` every sample's pass-reads across all
        // studies — refuse it. This rejects only the *empty* case, not every
        // under-scoped one: a non-empty but non-scoping filter (e.g. feature_idx
        // alone) still passes today. Making an unfiltered read opt-in via an
        // allowlist, and requiring prep_sample_idx for read_masked, is a tracked
        // durability follow-up.
        // The reference_* tables are broadly readable by design (this mirrors
        // the anonymous REST `GET /reference/{idx}`), so an unfiltered SELECT is
        // legitimate there — reject empty filters only for the read surface.
        if table == "read_masked" {
            return Err(Status::invalid_argument(
                "read_masked requires a non-empty filter (refusing full-table read)",
            ));
        }
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

    // --- validate_export_dest (pure; no DuckDB) ---

    #[test]
    fn validate_export_dest_accepts_path_under_scratch() {
        let root = Path::new("/scratch");
        let ok = validate_export_dest("/scratch/ticket/804/reads.parquet", root)
            .expect("path under scratch root should validate");
        assert_eq!(ok, PathBuf::from("/scratch/ticket/804/reads.parquet"));
    }

    #[test]
    fn validate_export_dest_rejects_outside_scratch() {
        let root = Path::new("/scratch");
        assert!(validate_export_dest("/etc/passwd", root).is_err());
    }

    #[test]
    fn validate_export_dest_rejects_parent_traversal() {
        let root = Path::new("/scratch");
        // Lexically starts with /scratch, but the `..` component is rejected.
        assert!(validate_export_dest("/scratch/../etc/passwd", root).is_err());
    }

    #[test]
    fn validate_export_dest_rejects_relative() {
        let root = Path::new("/scratch");
        assert!(validate_export_dest("ticket/804/reads.parquet", root).is_err());
    }

    #[test]
    fn validate_export_dest_rejects_single_quote() {
        // The dest is inlined into a DuckDB `COPY ... TO '<dest>'` literal.
        let root = Path::new("/scratch");
        assert!(validate_export_dest("/scratch/ti'ck/reads.parquet", root).is_err());
    }

    // --- single_i64_filter (pure; no DuckDB) ---

    fn filter_of(pairs: &[(&str, Vec<serde_json::Value>)]) -> auth::TicketFilter {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    #[test]
    fn single_i64_filter_extracts_lone_value() {
        let f = filter_of(&[("prep_sample_idx", vec![serde_json::json!(42)])]);
        assert_eq!(single_i64_filter(&f, "prep_sample_idx").unwrap(), 42);
    }

    #[test]
    fn single_i64_filter_rejects_missing_empty_multi_and_non_integer() {
        let f = filter_of(&[
            ("empty", vec![]),
            ("multi", vec![serde_json::json!(1), serde_json::json!(2)]),
            ("text", vec![serde_json::json!("x")]),
        ]);
        assert!(single_i64_filter(&f, "absent").is_err(), "missing column");
        assert!(single_i64_filter(&f, "empty").is_err(), "empty value list");
        assert!(
            single_i64_filter(&f, "multi").is_err(),
            "more than one value"
        );
        assert!(single_i64_filter(&f, "text").is_err(), "non-integer value");
    }

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

    /// `delete_read_mask_block` deletes EXACTLY one block's footprint: the
    /// per-member OR residual keeps a split sample's sibling-block sub-range, the
    /// `mask_idx` scope keeps a different mask's rows for the same sample, and a
    /// re-delete is an idempotent 0-row noop (the self-cleaning re-run guarantee).
    #[test]
    #[serial_test::serial]
    #[cfg(feature = "integration")]
    fn delete_read_mask_block_deletes_footprint_only() {
        let connstr = delete_test_catalog_connstr();
        let data_path = delete_test_data_path();
        let conn = Connection::open_in_memory().unwrap();
        ducklake::connect_ducklake(&conn, &connstr, &data_path).unwrap();
        ducklake::ensure_read_tables(&conn).unwrap();

        // Unique ids so leftover rows never collide with other serial tests.
        let mask_a: i64 = 940_000;
        let mask_b: i64 = 940_001;
        let prep_a: i64 = 940_010;
        let prep_b: i64 = 940_011;

        // mask_a/prep_a is a SPLIT sample: block 1 owns seq 100-101, block 2 owns
        // seq 102-103. mask_a/prep_b (seq 200-201) is whole in block 1. mask_b's
        // row for prep_a (seq 100) is a different filtering identity.
        conn.execute_batch(&format!(
            "DELETE FROM qiita_lake.read_mask WHERE mask_idx IN ({mask_a}, {mask_b});
             INSERT INTO qiita_lake.read_mask \
                 (mask_idx, prep_sample_idx, sequence_idx, reason) VALUES \
                 ({mask_a}, {prep_a}, 100, 'pass'), \
                 ({mask_a}, {prep_a}, 101, 'pass'), \
                 ({mask_a}, {prep_a}, 102, 'pass'), \
                 ({mask_a}, {prep_a}, 103, 'pass'), \
                 ({mask_a}, {prep_b}, 200, 'pass'), \
                 ({mask_a}, {prep_b}, 201, 'pass'), \
                 ({mask_b}, {prep_a}, 100, 'pass');"
        ))
        .unwrap();

        // Block 1's footprint: prep_a[100,101] (its half of the split) + prep_b
        // whole. block_min=100, block_max=201 spans prep_a's 102-103 too, so the
        // per-member OR is what keeps block 2's sub-range intact.
        let members = vec![
            auth::ExportReadBlockMember {
                prep_sample_idx: prep_a,
                sequence_idx_start: 100,
                sequence_idx_stop: 101,
            },
            auth::ExportReadBlockMember {
                prep_sample_idx: prep_b,
                sequence_idx_start: 200,
                sequence_idx_stop: 201,
            },
        ];

        let first = delete_read_mask_block(&connstr, &data_path, mask_a, &members)
            .expect("delete_read_mask_block failed");
        assert_eq!(
            first["rows_deleted"], 4,
            "block 1's 4 footprint rows deleted"
        );
        assert_eq!(first["mask_idx"], mask_a);

        let count = |mask: i64, prep: i64| -> i64 {
            conn.query_row(
                &format!(
                    "SELECT count(*) FROM qiita_lake.read_mask \
                     WHERE mask_idx = {mask} AND prep_sample_idx = {prep}"
                ),
                [],
                |r| r.get(0),
            )
            .unwrap()
        };
        // Block 2's sub-range of the split sample survives (per-member OR exact).
        assert_eq!(
            count(mask_a, prep_a),
            2,
            "prep_a 102-103 (block 2) untouched"
        );
        // prep_b's whole sample was in block 1 — fully deleted.
        assert_eq!(count(mask_a, prep_b), 0, "prep_b fully deleted");
        // The different mask's row for the same sample is out of scope.
        assert_eq!(count(mask_b, prep_a), 1, "mask_b untouched");

        // Idempotency: re-deleting the same footprint removes nothing.
        let second = delete_read_mask_block(&connstr, &data_path, mask_a, &members)
            .expect("idempotent re-delete failed");
        assert_eq!(second["rows_deleted"], 0, "second delete removes nothing");

        let _ = conn.execute_batch(&format!(
            "DELETE FROM qiita_lake.read_mask WHERE mask_idx IN ({mask_a}, {mask_b});"
        ));
    }

    /// `count_masked_reads` counts exactly the `read_mask` rows for the target
    /// `(prep_sample_idx, mask_idx)` with `reason = 'pass'`: non-`pass` rows (the
    /// view's privacy filter), a different mask, and a different prep_sample are
    /// all excluded.
    #[test]
    #[serial_test::serial]
    #[cfg(feature = "integration")]
    fn count_masked_reads_counts_pass_rows_for_target_only() {
        let connstr = delete_test_catalog_connstr();
        let data_path = delete_test_data_path();
        let conn = Connection::open_in_memory().unwrap();
        ducklake::connect_ducklake(&conn, &connstr, &data_path).unwrap();
        ducklake::ensure_read_tables(&conn).unwrap();

        // Unique ids so leftover rows never collide with other serial tests.
        let mask_a: i64 = 950_000;
        let mask_b: i64 = 950_001;
        let prep_a: i64 = 950_010;
        let prep_b: i64 = 950_011;

        conn.execute_batch(&format!(
            "DELETE FROM qiita_lake.read_mask WHERE mask_idx IN ({mask_a}, {mask_b});
             INSERT INTO qiita_lake.read_mask \
                 (mask_idx, prep_sample_idx, sequence_idx, reason) VALUES \
                 ({mask_a}, {prep_a}, 950100, 'pass'), \
                 ({mask_a}, {prep_a}, 950101, 'pass'), \
                 ({mask_a}, {prep_a}, 950102, 'host_human'), \
                 ({mask_a}, {prep_b}, 950103, 'pass'), \
                 ({mask_b}, {prep_a}, 950104, 'pass');"
        ))
        .unwrap();

        // Two 'pass' rows for (prep_a, mask_a); the host-filtered row, prep_b's
        // row, and mask_b's row are all excluded.
        let n = count_masked_reads(&connstr, &data_path, prep_a, mask_a).expect("count failed");
        assert_eq!(n, 2);

        // A (prep, mask) pair with no rows counts zero, not an error.
        let none = count_masked_reads(&connstr, &data_path, prep_b, mask_b).expect("count failed");
        assert_eq!(none, 0);

        let _ = conn.execute_batch(&format!(
            "DELETE FROM qiita_lake.read_mask WHERE mask_idx IN ({mask_a}, {mask_b});"
        ));
    }

    /// `mask_metrics_counts` aggregates the target `(mask_idx, prep_sample_idx)`
    /// rows across a mix of SE (right_trim2 NULL) and PE (right_trim2 non-NULL)
    /// reads with mixed reasons, and excludes a different mask / prep_sample. It
    /// mirrors `_read_mask_counts`: raw = both-mates total, biological = non-qc,
    /// quality_filtered = pass; plus row_count = one-per-read for the assertion.
    #[test]
    #[serial_test::serial]
    #[cfg(feature = "integration")]
    fn mask_metrics_counts_buckets_both_mates_for_target_only() {
        let connstr = delete_test_catalog_connstr();
        let data_path = delete_test_data_path();
        let conn = Connection::open_in_memory().unwrap();
        ducklake::connect_ducklake(&conn, &connstr, &data_path).unwrap();
        ducklake::ensure_read_tables(&conn).unwrap();

        let mask_a: i64 = 960_000;
        let mask_b: i64 = 960_001;
        let prep_a: i64 = 960_010;
        let prep_b: i64 = 960_011;

        // For (mask_a, prep_a): 3 PE rows (right_trim2 = 0, a mate each) +
        // 2 SE rows (right_trim2 NULL). Reasons: 2 pass (1 PE, 1 SE),
        // 1 host_rype (PE, biological but not quality_filtered), 1 qc_too_short
        // (PE, excluded from biological), 1 qc_low_quality (SE, excluded).
        //   row_count  = 5
        //   raw        = 5 rows + 3 R2 (the PE rows) = 8
        //   biological = pass+host = 3 rows (2 PE + wait) ...
        // Enumerate explicitly to keep the arithmetic auditable:
        //   PE pass         (R2)      -> raw 2, bio 2, qf 2
        //   SE pass                   -> raw 1, bio 1, qf 1
        //   PE host_rype    (R2)      -> raw 2, bio 2, qf 0
        //   PE qc_too_short (R2)      -> raw 2, bio 0, qf 0
        //   SE qc_low_quality         -> raw 1, bio 0, qf 0
        // Totals: row_count 5, raw 8, biological 5, quality_filtered 3.
        conn.execute_batch(&format!(
            "DELETE FROM qiita_lake.read_mask WHERE mask_idx IN ({mask_a}, {mask_b});
             INSERT INTO qiita_lake.read_mask \
                 (mask_idx, prep_sample_idx, sequence_idx, reason, \
                  left_trim1, right_trim1, left_trim2, right_trim2) VALUES \
                 ({mask_a}, {prep_a}, 960100, 'pass',          0, 0, 0, 0), \
                 ({mask_a}, {prep_a}, 960101, 'pass',          0, 0, NULL, NULL), \
                 ({mask_a}, {prep_a}, 960102, 'host_rype',     0, 0, 0, 0), \
                 ({mask_a}, {prep_a}, 960103, 'qc_too_short',  0, 0, 0, 0), \
                 ({mask_a}, {prep_a}, 960104, 'qc_low_quality',0, 0, NULL, NULL), \
                 ({mask_a}, {prep_b}, 960105, 'pass',          0, 0, 0, 0), \
                 ({mask_b}, {prep_a}, 960106, 'pass',          0, 0, 0, 0);"
        ))
        .unwrap();

        let counts =
            mask_metrics_counts(&connstr, &data_path, mask_a, prep_a).expect("counts failed");
        assert_eq!(
            counts["row_count"], 5,
            "one row per read/pair for the target"
        );
        assert_eq!(counts["raw"], 8, "5 rows + 3 R2 mates");
        assert_eq!(counts["biological"], 5, "pass + host, both-mates");
        assert_eq!(counts["quality_filtered"], 3, "pass only, both-mates");

        // A (mask, prep) pair with no rows is all-zero, not an error.
        let empty =
            mask_metrics_counts(&connstr, &data_path, mask_b, prep_b).expect("counts failed");
        assert_eq!(empty["row_count"], 0);
        assert_eq!(empty["raw"], 0);
        assert_eq!(empty["biological"], 0);
        assert_eq!(empty["quality_filtered"], 0);

        let _ = conn.execute_batch(&format!(
            "DELETE FROM qiita_lake.read_mask WHERE mask_idx IN ({mask_a}, {mask_b});"
        ));
    }

    /// An empty prep_sample set short-circuits: zero counts, no catalog touched
    /// (so this needs no DuckLake — runs in the pure-unit tier). Guards against
    /// emitting an invalid `IN ()` clause.
    #[test]
    fn delete_pool_reads_empty_set_is_zero_count_noop() {
        let counts = delete_pool_reads("unused-connstr", "unused-data-path", &[])
            .expect("empty-set delete should succeed without touching the catalog");
        assert_eq!(counts["prep_sample_count"], 0);
        assert_eq!(counts["read_rows_deleted"], 0);
        assert_eq!(counts["read_mask_rows_deleted"], 0);
    }

    /// `delete_pool_reads` drops exactly the target prep_samples' `read` and
    /// `read_mask` rows, leaves another pool's prep_sample untouched, and is
    /// idempotent: a second delete of the same set succeeds and reports 0 rows.
    #[test]
    #[serial_test::serial]
    #[cfg(feature = "integration")]
    fn delete_pool_reads_drops_target_idempotently() {
        let connstr = delete_test_catalog_connstr();
        let data_path = delete_test_data_path();
        let conn = Connection::open_in_memory().unwrap();
        ducklake::connect_ducklake(&conn, &connstr, &data_path).unwrap();
        ducklake::ensure_read_tables(&conn).unwrap();

        // Unique ids so leftover rows never collide with other serial tests.
        // prep_a / prep_b belong to the deleted pool; prep_other to another.
        let prep_a: i64 = 940_000;
        let prep_b: i64 = 940_001;
        let prep_other: i64 = 940_002;
        let mask: i64 = 940_010;

        conn.execute_batch(&format!(
            "DELETE FROM qiita_lake.read WHERE prep_sample_idx IN ({prep_a}, {prep_b}, {prep_other});
             DELETE FROM qiita_lake.read_mask WHERE prep_sample_idx IN ({prep_a}, {prep_b}, {prep_other});
             INSERT INTO qiita_lake.read (prep_sample_idx, sequence_idx, read_id, sequence1) VALUES \
                 ({prep_a}, 1, 'r1', 'ACGT'), \
                 ({prep_a}, 2, 'r2', 'TTTT'), \
                 ({prep_b}, 3, 'r3', 'GGGG'), \
                 ({prep_other}, 4, 'r4', 'CCCC');
             INSERT INTO qiita_lake.read_mask \
                 (mask_idx, prep_sample_idx, sequence_idx, reason) VALUES \
                 ({mask}, {prep_a}, 1, 'pass'), \
                 ({mask}, {prep_b}, 3, 'pass'), \
                 ({mask}, {prep_other}, 4, 'pass');"
        ))
        .unwrap();

        let first = delete_pool_reads(&connstr, &data_path, &[prep_a, prep_b])
            .expect("delete_pool_reads failed");
        assert_eq!(first["prep_sample_count"], 2);
        assert_eq!(first["read_rows_deleted"], 3, "prep_a (2) + prep_b (1)");
        assert_eq!(first["read_mask_rows_deleted"], 2, "prep_a + prep_b masks");

        let read_count = |prep: i64| -> i64 {
            conn.query_row(
                &format!("SELECT count(*) FROM qiita_lake.read WHERE prep_sample_idx = {prep}"),
                [],
                |r| r.get(0),
            )
            .unwrap()
        };
        let mask_count = |prep: i64| -> i64 {
            conn.query_row(
                &format!(
                    "SELECT count(*) FROM qiita_lake.read_mask WHERE prep_sample_idx = {prep}"
                ),
                [],
                |r| r.get(0),
            )
            .unwrap()
        };
        assert_eq!(read_count(prep_a), 0);
        assert_eq!(read_count(prep_b), 0);
        assert_eq!(read_count(prep_other), 1, "other pool's read untouched");
        assert_eq!(mask_count(prep_other), 1, "other pool's mask untouched");

        // Idempotency: re-deleting the now-empty set is success with 0 rows.
        let second = delete_pool_reads(&connstr, &data_path, &[prep_a, prep_b])
            .expect("idempotent re-delete failed");
        assert_eq!(second["read_rows_deleted"], 0);
        assert_eq!(second["read_mask_rows_deleted"], 0);

        // Best-effort cleanup of the surviving other-pool rows.
        let _ = conn.execute_batch(&format!(
            "DELETE FROM qiita_lake.read WHERE prep_sample_idx = {prep_other};
             DELETE FROM qiita_lake.read_mask WHERE prep_sample_idx = {prep_other};"
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

    // The streaming DoGet helper: drives query_arrow inside a blocking task and
    // hands batches back over a bounded channel (do_get's body). Pins that it
    // streams every row, preserves the UTINYINT[] -> List<UInt8> shape, and
    // emits one empty schema batch for a zero-row result — the same contract
    // the old buffered `.collect()` path had, now without buffering the whole
    // result set in memory.
    #[tokio::test]
    #[serial_test::serial]
    #[cfg(feature = "integration")]
    async fn stream_ducklake_batches_streams_rows_and_empty_schema_branch() {
        use arrow_schema::DataType;

        let connstr = delete_test_catalog_connstr();
        let data_path = delete_test_data_path();

        let prep: i64 = 921_000;
        let mask: i64 = 921_001;
        let (s0, s1, s2) = (prep + 1, prep + 2, prep + 3);
        {
            let conn = Connection::open_in_memory().unwrap();
            ducklake::connect_ducklake(&conn, &connstr, &data_path).unwrap();
            ducklake::ensure_read_tables(&conn).unwrap();
            conn.execute_batch(&format!(
                "DELETE FROM qiita_lake.read WHERE prep_sample_idx = {prep};
                 DELETE FROM qiita_lake.read_mask WHERE prep_sample_idx = {prep};
                 INSERT INTO qiita_lake.read \
                     (prep_sample_idx, sequence_idx, read_id, sequence1, qual1, sequence2, qual2) VALUES \
                     ({prep}, {s0}, 'r0', 'ACGT', [5,6,7,8]::UTINYINT[], NULL, NULL), \
                     ({prep}, {s1}, 'r1', 'TTGG', [9,9,9,9]::UTINYINT[], NULL, NULL), \
                     ({prep}, {s2}, 'r2', 'CCAA', [3,3,3,3]::UTINYINT[], NULL, NULL);
                 INSERT INTO qiita_lake.read_mask \
                     (mask_idx, prep_sample_idx, sequence_idx, reason, left_trim1, right_trim1) VALUES \
                     ({mask}, {prep}, {s0}, 'pass', 0, 0), \
                     ({mask}, {prep}, {s1}, 'pass', 0, 0), \
                     ({mask}, {prep}, {s2}, 'pass', 0, 0);"
            ))
            .unwrap();
        }

        let mut filter = auth::TicketFilter::new();
        filter.insert("mask_idx".to_string(), vec![serde_json::Value::from(mask)]);
        filter.insert(
            "prep_sample_idx".to_string(),
            vec![serde_json::Value::from(prep)],
        );
        let (sql, table) = build_query("read_masked", &filter).unwrap();
        let batches: Vec<arrow_array::RecordBatch> =
            stream_ducklake_batches(connstr.clone(), data_path.clone(), sql, table)
                .collect::<Vec<_>>()
                .await
                .into_iter()
                .map(|r| r.expect("stream item should be Ok"))
                .collect();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3, "all three pass reads should stream through");
        let qual1 = batches[0]
            .schema()
            .field_with_name("qual1")
            .unwrap()
            .data_type()
            .clone();
        match qual1 {
            DataType::List(item) | DataType::LargeList(item) => {
                assert_eq!(
                    item.data_type(),
                    &DataType::UInt8,
                    "qual1 must be List<UInt8>"
                )
            }
            other => panic!("qual1 should be an Arrow List, got: {other:?}"),
        }

        // Empty-result branch: one zero-row batch carrying the schema.
        let mut empty_filter = auth::TicketFilter::new();
        empty_filter.insert(
            "mask_idx".to_string(),
            vec![serde_json::Value::from(mask + 999_999)],
        );
        empty_filter.insert(
            "prep_sample_idx".to_string(),
            vec![serde_json::Value::from(prep)],
        );
        let (esql, etable) = build_query("read_masked", &empty_filter).unwrap();
        let empty: Vec<arrow_array::RecordBatch> =
            stream_ducklake_batches(connstr.clone(), data_path.clone(), esql, etable)
                .collect::<Vec<_>>()
                .await
                .into_iter()
                .map(|r| r.expect("empty stream item should be Ok"))
                .collect();
        assert_eq!(empty.len(), 1, "empty result still yields one schema batch");
        assert_eq!(empty[0].num_rows(), 0, "the schema batch has no rows");
        assert!(
            empty[0].schema().field_with_name("qual1").is_ok(),
            "empty batch carries the full read_masked schema"
        );

        let conn = Connection::open_in_memory().unwrap();
        ducklake::connect_ducklake(&conn, &connstr, &data_path).unwrap();
        let _ = conn.execute_batch(&format!(
            "DELETE FROM qiita_lake.read WHERE prep_sample_idx = {prep};
             DELETE FROM qiita_lake.read_mask WHERE prep_sample_idx = {prep};"
        ));
    }

    // A producer-side error (here, a query against a missing table) must surface
    // as a single Err stream item — never a silently-truncated empty stream.
    #[tokio::test]
    #[serial_test::serial]
    #[cfg(feature = "integration")]
    async fn stream_ducklake_batches_propagates_query_error() {
        let connstr = delete_test_catalog_connstr();
        let data_path = delete_test_data_path();
        let items: Vec<_> = stream_ducklake_batches(
            connstr,
            data_path,
            "SELECT * FROM qiita_lake.does_not_exist_table".to_string(),
            "qiita_lake.does_not_exist_table".to_string(),
        )
        .collect::<Vec<_>>()
        .await;
        assert_eq!(items.len(), 1, "a producer error yields exactly one item");
        assert!(
            items[0].is_err(),
            "the item must be an Err, not a silent empty stream"
        );
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

    // --- register_files filename validation (pure; no DuckDB) ---

    /// `register_files` rejects any filename that could escape the staging dir
    /// before it touches the filesystem or the catalog. `payload.files` is
    /// HMAC-signed by the control plane, but this defense-in-depth check keeps
    /// the data plane's filesystem contract independent of CP correctness. A
    /// `..` (parent) or a rooted/absolute component must be refused; the check
    /// runs first, so a bogus connstr/data_path is never reached.
    #[test]
    fn register_files_rejects_filename_traversal() {
        for bad in [
            "../escape.parquet",
            "/etc/passwd",
            "sub/../../escape.parquet",
        ] {
            let mut files = std::collections::HashMap::new();
            files.insert(bad.to_string(), "reference_membership".to_string());
            let payload = auth::ActionPayload {
                action: "register_files".to_string(),
                staging_dir: "/unused/staging".to_string(),
                files,
                work_ticket_idx: 1,
            };
            let err = register_files("unused-connstr", "unused-data-path", &payload)
                .expect_err("a traversal filename must be rejected");
            assert_eq!(
                err.code(),
                tonic::Code::InvalidArgument,
                "filename {bad:?} must be rejected as invalid, not reach the catalog"
            );
        }
    }

    // --- do_action dispatch trust checks (pure; no DuckDB) ---

    /// An action whose `Action.type` header disagrees with the signed
    /// `payload.action` is rejected. `verify_action` succeeds (HMAC + shape are
    /// valid), then the handler's discriminator check catches the mismatch — the
    /// two must agree so a token minted for one action can't be replayed under a
    /// different action header.
    #[tokio::test]
    async fn do_action_rejects_type_payload_mismatch() {
        let tmp = tempfile::tempdir().unwrap();
        let service = make_service(tmp.path().to_path_buf());
        // Validly-signed register_files-shaped payload, but its action field
        // says delete_reference — sent under the register_files header.
        let payload =
            br#"{"action":"delete_reference","staging_dir":"/unused","files":{},"work_ticket_idx":1}"#;
        let body = sign_raw(payload, b"dev-secret", future_expiry_secs(300));
        let action = Action {
            r#type: "register_files".to_string(),
            body: body.into(),
        };
        // The success type (a boxed Stream) is not Debug, so `expect_err` won't
        // compile — match instead.
        let err = match service.do_action(Request::new(action)).await {
            Ok(_) => panic!("action-type/payload mismatch must be rejected"),
            Err(e) => e,
        };
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(
            err.message().contains("mismatch"),
            "error should name the mismatch: {}",
            err.message()
        );
    }

    /// An unrecognized `Action.type` is rejected as invalid rather than silently
    /// ignored or dispatched — the dispatcher only ever runs known handlers.
    #[tokio::test]
    async fn do_action_rejects_unknown_action_type() {
        let tmp = tempfile::tempdir().unwrap();
        let service = make_service(tmp.path().to_path_buf());
        let action = Action {
            r#type: "definitely_not_a_real_action".to_string(),
            body: Vec::<u8>::new().into(),
        };
        let err = match service.do_action(Request::new(action)).await {
            Ok(_) => panic!("unknown action type must be rejected"),
            Err(e) => e,
        };
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    /// The replay-safe registry and the do_action dispatcher must stay in
    /// lockstep. Every `REPLAY_SAFE_ACTIONS` entry reaches a real handler — it
    /// then fails verifying the empty token body (`Unauthenticated`), NOT the
    /// replay guard (`InvalidArgument`) — and an action outside the registry is
    /// rejected by the guard. So a new match arm added without a registry entry
    /// is unreachable and surfaces the moment it is exercised, forcing a
    /// conscious replay classification (see the `# replay:` note in do_action).
    #[tokio::test]
    async fn replay_safe_actions_matches_dispatcher() {
        let tmp = tempfile::tempdir().unwrap();
        let service = make_service(tmp.path().to_path_buf());

        for name in REPLAY_SAFE_ACTIONS {
            let action = Action {
                r#type: name.to_string(),
                body: Vec::<u8>::new().into(),
            };
            let err = match service.do_action(Request::new(action)).await {
                Ok(_) => panic!("empty-body action {name:?} must fail"),
                Err(e) => e,
            };
            assert_eq!(
                err.code(),
                tonic::Code::Unauthenticated,
                "classified action {name:?} must be dispatched to a handler \
                 (fail on token verification), not rejected as unknown"
            );
        }

        // An action absent from the registry is turned away by the replay guard.
        let bogus = Action {
            r#type: "definitely_not_a_real_action".to_string(),
            body: Vec::<u8>::new().into(),
        };
        let err = match service.do_action(Request::new(bogus)).await {
            Ok(_) => panic!("unclassified action must be rejected"),
            Err(e) => e,
        };
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    /// End-to-end `register_files`: seed a Parquet in a staging dir, register it
    /// into DuckLake, and assert the file was moved to ticket-unique lake storage
    /// and its rows are queryable through the catalog. Exercises the
    /// move-then-register path and its wrapping transaction against a real
    /// DuckLake catalog.
    #[test]
    #[serial_test::serial]
    #[cfg(feature = "integration")]
    fn register_files_moves_and_registers_end_to_end() {
        let connstr = delete_test_catalog_connstr();
        let data_path = delete_test_data_path();

        // Unique ids so leftover rows never collide with other serial tests.
        let ref_idx: i64 = 970_000;
        let feat_a: i64 = 970_010;
        let feat_b: i64 = 970_011;
        // Ticket-unique dest names come from work_ticket_idx (lake_dest_filename).
        // Derive it from the PID so a manual re-run against a persistent catalog
        // mints a fresh file name instead of colliding with the prior run's
        // still-registered lake file (move_file refuses to overwrite). CI resets
        // the catalog each run, so this only matters for local re-runs.
        let ticket: i64 = 970_000_000 + std::process::id() as i64;

        // Ensure the target table exists, and tombstone any rows a prior local
        // run left behind so the post-register count reflects only this run.
        {
            let conn = Connection::open_in_memory().unwrap();
            ducklake::connect_ducklake(&conn, &connstr, &data_path).unwrap();
            ducklake::ensure_reference_tables(&conn).unwrap();
            conn.execute_batch(&format!(
                "DELETE FROM qiita_lake.reference_membership WHERE reference_idx = {ref_idx};"
            ))
            .unwrap();
        }

        // Seed a staging Parquet whose schema matches reference_membership
        // (two BIGINT columns) — written by DuckDB so the types match exactly.
        let staging = tempfile::tempdir().unwrap();
        let src = staging.path().join("reference_membership.parquet");
        let src_str = src.to_str().unwrap();
        {
            let writer = Connection::open_in_memory().unwrap();
            writer
                .execute_batch(&format!(
                    "COPY (SELECT * FROM (VALUES \
                         ({ref_idx}::BIGINT, {feat_a}::BIGINT), \
                         ({ref_idx}::BIGINT, {feat_b}::BIGINT)) \
                         t(reference_idx, feature_idx)) \
                     TO '{src_str}' (FORMAT PARQUET)"
                ))
                .unwrap();
        }
        assert!(src.exists(), "staging parquet seeded");

        let mut files = std::collections::HashMap::new();
        files.insert(
            "reference_membership.parquet".to_string(),
            "reference_membership".to_string(),
        );
        let payload = auth::ActionPayload {
            action: "register_files".to_string(),
            staging_dir: staging.path().to_str().unwrap().to_string(),
            files,
            work_ticket_idx: ticket,
        };

        let registered =
            register_files(&connstr, &data_path, &payload).expect("register_files failed");
        assert_eq!(registered.len(), 1, "one file registered");
        // The dest carries the ticket-unique minted name under the per-table dir.
        let dest = std::path::Path::new(&registered[0]);
        assert_eq!(
            dest.file_name().and_then(|f| f.to_str()).unwrap(),
            lake_dest_filename(ticket, "reference_membership.parquet")
        );
        assert!(dest.exists(), "registered lake file present on disk");
        assert!(
            !src.exists(),
            "staging source was moved out, not left behind"
        );

        // The rows are queryable through the catalog via a fresh connection.
        let reader = Connection::open_in_memory().unwrap();
        ducklake::connect_ducklake(&reader, &connstr, &data_path).unwrap();
        let n: i64 = reader
            .query_row(
                &format!(
                    "SELECT count(*) FROM qiita_lake.reference_membership \
                     WHERE reference_idx = {ref_idx}"
                ),
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(n, 2, "both seeded membership rows registered");

        // Best-effort cleanup: tombstone the catalog rows only. Do NOT remove
        // the physical lake file — it stays registered in the DuckLake catalog
        // until compaction, and unlinking a still-registered data file breaks
        // any later full-table scan of reference_membership (e.g. the
        // delete_reference orphan subquery) with a missing-file IO error.
        let _ = reader.execute_batch(&format!(
            "DELETE FROM qiita_lake.reference_membership WHERE reference_idx = {ref_idx};"
        ));
    }

    /// Pins the DuckLake-transaction semantics `register_files` relies on: a
    /// `ducklake_add_data_files` performed inside a transaction that is then
    /// ROLLBACK'd leaves ZERO rows registered — visible within the open
    /// transaction, gone after the rollback. If DuckLake auto-committed catalog
    /// mutations (ignoring the enclosing DuckDB transaction), `register_files`'
    /// BEGIN/ROLLBACK wrap would be a no-op and a mid-loop failure would leak a
    /// half-registered reference; this asserts it is not.
    #[test]
    #[serial_test::serial]
    #[cfg(feature = "integration")]
    fn register_ducklake_add_data_files_rolls_back_within_transaction() {
        let connstr = delete_test_catalog_connstr();
        let data_path = delete_test_data_path();

        let ref_idx: i64 = 972_000;
        let feat: i64 = 972_010;

        let conn = Connection::open_in_memory().unwrap();
        ducklake::connect_ducklake(&conn, &connstr, &data_path).unwrap();
        ducklake::ensure_reference_tables(&conn).unwrap();
        conn.execute_batch(&format!(
            "DELETE FROM qiita_lake.reference_membership WHERE reference_idx = {ref_idx};"
        ))
        .unwrap();

        // A valid reference_membership Parquet to register (types match exactly).
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("m.parquet");
        let src_str = src.to_str().unwrap();
        conn.execute_batch(&format!(
            "COPY (SELECT {ref_idx}::BIGINT AS reference_idx, {feat}::BIGINT AS feature_idx) \
             TO '{src_str}' (FORMAT PARQUET)"
        ))
        .unwrap();

        let count = |c: &Connection| -> i64 {
            c.query_row(
                &format!(
                    "SELECT count(*) FROM qiita_lake.reference_membership \
                     WHERE reference_idx = {ref_idx}"
                ),
                [],
                |r| r.get(0),
            )
            .unwrap()
        };

        conn.execute_batch("BEGIN TRANSACTION").unwrap();
        conn.execute(
            "CALL ducklake_add_data_files('qiita_lake', ?, ?)",
            duckdb::params!["reference_membership", src_str],
        )
        .unwrap();
        assert_eq!(count(&conn), 1, "registration is visible inside the txn");
        conn.execute_batch("ROLLBACK").unwrap();
        assert_eq!(
            count(&conn),
            0,
            "ROLLBACK must unwind the registration — the wrap in register_files \
             is only atomic if DuckLake honors the enclosing transaction"
        );
    }

    /// `export_read_to_parquet` writes one sample's full reads from the DuckLake
    /// `read` table to a Parquet drop-in: the 7-col schema with `qual` as
    /// UTINYINT[], the seeded rows, mode 0o440. An unknown sample writes NO file
    /// and returns 0 (the control plane turns that into a submission failure).
    #[test]
    #[serial_test::serial]
    #[cfg(feature = "integration")]
    fn export_read_writes_sample_parquet() {
        use std::os::unix::fs::PermissionsExt;

        let connstr = delete_test_catalog_connstr();
        let data_path = delete_test_data_path();
        let conn = Connection::open_in_memory().unwrap();
        ducklake::connect_ducklake(&conn, &connstr, &data_path).unwrap();
        ducklake::ensure_read_tables(&conn).unwrap();

        // Unique ids so leftover rows never collide with other serial tests.
        let prep: i64 = 940_000;
        let absent: i64 = 940_999;
        let seq_pe: i64 = 940_010;
        let seq_se: i64 = 940_011;

        conn.execute_batch(&format!(
            "DELETE FROM qiita_lake.read WHERE prep_sample_idx = {prep};
             INSERT INTO qiita_lake.read \
                 (prep_sample_idx, sequence_idx, read_id, sequence1, qual1, sequence2, qual2) VALUES \
                 ({prep}, {seq_pe}, 'r_pe', 'AACGT', [10,11,12,13,14]::UTINYINT[], 'TTGCA', [20,21,22,23,24]::UTINYINT[]), \
                 ({prep}, {seq_se}, 'r_se', 'GGGCC', [30,31,32,33,34]::UTINYINT[], NULL, NULL);"
        ))
        .unwrap();

        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("reads.parquet");

        let count = export_read_to_parquet(&connstr, &data_path, prep, &dest, dir.path())
            .expect("export_read_to_parquet failed");
        assert_eq!(count, 2, "both seeded rows exported");
        assert!(dest.exists(), "destination parquet written");

        // Mode 0o440 (owner/group read-only) — the read result-file convention.
        let mode = std::fs::metadata(&dest).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o440, "exported parquet is mode 440");

        // Read it back: row count, qual1 round-trips as a list, full 7-col schema.
        let reader = Connection::open_in_memory().unwrap();
        let dest_str = dest.to_str().unwrap();
        let n: i64 = reader
            .query_row(
                &format!("SELECT count(*) FROM read_parquet('{dest_str}')"),
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(n, 2);
        let qual_type: String = reader
            .query_row(
                &format!(
                    "SELECT typeof(qual1) FROM read_parquet('{dest_str}') WHERE sequence_idx = {seq_pe}"
                ),
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(qual_type, "UTINYINT[]", "qual1 round-trips as UTINYINT[]");
        // A missing column in the projection below would error — pins the schema.
        let full: i64 = reader
            .query_row(
                &format!(
                    "SELECT count(*) FROM read_parquet('{dest_str}') \
                     WHERE prep_sample_idx = {prep} AND read_id IS NOT NULL \
                       AND sequence1 IS NOT NULL"
                ),
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(full, 2);

        // An unknown sample writes no file and reports 0.
        let dest_absent = dir.path().join("absent.parquet");
        let zero = export_read_to_parquet(&connstr, &data_path, absent, &dest_absent, dir.path())
            .expect("export of an unknown sample should succeed with 0");
        assert_eq!(zero, 0);
        assert!(!dest_absent.exists(), "no file written for an empty result");
        assert!(
            !dir.path().join("absent.parquet.partial").exists(),
            "the temp file is cleaned up on the empty path"
        );

        // Best-effort cleanup of the seeded rows.
        let _ = conn.execute_batch(&format!(
            "DELETE FROM qiita_lake.read WHERE prep_sample_idx = {prep};"
        ));
    }

    /// `export_read_block` materializes the UNION of its members' `read`
    /// sub-ranges and nothing else: a sample whose `sequence_idx` falls in the
    /// gap between two block members (but whose prep_sample is not a member) is
    /// excluded, and a split member contributes only its sub-range (rows beyond
    /// its `sequence_idx_stop` stay out). Per-row `prep_sample_idx` is preserved
    /// so the block kernel can group by it.
    #[test]
    #[serial_test::serial]
    #[cfg(feature = "integration")]
    fn export_read_block_writes_union_and_excludes_gap_and_split() {
        use std::os::unix::fs::PermissionsExt;

        let connstr = delete_test_catalog_connstr();
        let data_path = delete_test_data_path();
        let conn = Connection::open_in_memory().unwrap();
        ducklake::connect_ducklake(&conn, &connstr, &data_path).unwrap();
        ducklake::ensure_read_tables(&conn).unwrap();

        // Unique ids so leftover rows never collide with other serial tests.
        let prep_a: i64 = 941_000; // fully in block
        let prep_gap: i64 = 941_001; // sequence_idx in [block_min, block_max] but NOT a member
        let prep_c: i64 = 941_002; // split: block covers only a sub-range

        conn.execute_batch(&format!(
            "DELETE FROM qiita_lake.read WHERE prep_sample_idx IN ({prep_a}, {prep_gap}, {prep_c});
             INSERT INTO qiita_lake.read \
                 (prep_sample_idx, sequence_idx, read_id, sequence1, qual1, sequence2, qual2) VALUES \
                 ({prep_a}, 941010, 'a0', 'AAAAA', [10,10,10,10,10]::UTINYINT[], NULL, NULL), \
                 ({prep_a}, 941011, 'a1', 'AAAAC', [10,10,10,10,10]::UTINYINT[], NULL, NULL), \
                 ({prep_a}, 941012, 'a2', 'AAAAG', [10,10,10,10,10]::UTINYINT[], NULL, NULL), \
                 ({prep_gap}, 941020, 'g0', 'CCCCC', [10,10,10,10,10]::UTINYINT[], NULL, NULL), \
                 ({prep_gap}, 941021, 'g1', 'CCCCA', [10,10,10,10,10]::UTINYINT[], NULL, NULL), \
                 ({prep_c}, 941030, 'c0', 'GGGGG', [10,10,10,10,10]::UTINYINT[], NULL, NULL), \
                 ({prep_c}, 941031, 'c1', 'GGGGA', [10,10,10,10,10]::UTINYINT[], NULL, NULL), \
                 ({prep_c}, 941032, 'c2', 'GGGGC', [10,10,10,10,10]::UTINYINT[], NULL, NULL), \
                 ({prep_c}, 941033, 'c3', 'GGGGT', [10,10,10,10,10]::UTINYINT[], NULL, NULL), \
                 ({prep_c}, 941034, 'c4', 'GGGTT', [10,10,10,10,10]::UTINYINT[], NULL, NULL);"
        ))
        .unwrap();

        // Block = prep_a (whole) + prep_c (sub-range [941030, 941031], boundary-
        // aligned split). block_min=941010, block_max=941031 spans prep_gap's
        // window (941020-941021), so the IN(prep) clause is what excludes it.
        let members = vec![
            auth::ExportReadBlockMember {
                prep_sample_idx: prep_a,
                sequence_idx_start: 941010,
                sequence_idx_stop: 941012,
            },
            auth::ExportReadBlockMember {
                prep_sample_idx: prep_c,
                sequence_idx_start: 941030,
                sequence_idx_stop: 941031,
            },
        ];

        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("reads.parquet");
        let count = export_read_block_to_parquet(&connstr, &data_path, &members, &dest, dir.path())
            .expect("export_read_block_to_parquet failed");
        assert_eq!(
            count, 5,
            "3 (prep_a) + 2 (prep_c sub-range) = 5; gap excluded"
        );
        assert!(dest.exists());
        let mode = std::fs::metadata(&dest).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o440, "exported parquet is mode 440");

        let reader = Connection::open_in_memory().unwrap();
        let dest_str = dest.to_str().unwrap();
        // The gap sample must be entirely absent.
        let gap_rows: i64 = reader
            .query_row(
                &format!(
                    "SELECT count(*) FROM read_parquet('{dest_str}') WHERE prep_sample_idx = {prep_gap}"
                ),
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            gap_rows, 0,
            "gap sample excluded by the prep_sample_idx IN clause"
        );
        // The split sample contributes only its sub-range (no 941032..034).
        let c_max: i64 = reader
            .query_row(
                &format!(
                    "SELECT max(sequence_idx) FROM read_parquet('{dest_str}') WHERE prep_sample_idx = {prep_c}"
                ),
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(c_max, 941031, "split member stops at its sequence_idx_stop");
        // Per-row prep_sample_idx preserved for both members.
        let distinct_preps: i64 = reader
            .query_row(
                &format!("SELECT count(DISTINCT prep_sample_idx) FROM read_parquet('{dest_str}')"),
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(distinct_preps, 2, "both members present, keyed per-row");

        // An empty block writes no file and reports 0 (defense in depth — the
        // DoAction arm also rejects empty members before calling this).
        let dest_empty = dir.path().join("empty.parquet");
        let zero = export_read_block_to_parquet(&connstr, &data_path, &[], &dest_empty, dir.path())
            .expect("empty block should succeed with 0");
        assert_eq!(zero, 0);
        assert!(!dest_empty.exists(), "no file written for an empty block");

        let _ = conn.execute_batch(&format!(
            "DELETE FROM qiita_lake.read WHERE prep_sample_idx IN ({prep_a}, {prep_gap}, {prep_c});"
        ));
    }

    /// A split member whose `sequence_idx_stop` is NOT the block's max still
    /// contributes only its own sub-range: the per-member predicate excludes the
    /// part of that sample living in a sibling block, even though those rows fall
    /// inside the block's overall [min, max] span and the sample is in the IN-set.
    /// This is the case a bare global `BETWEEN block_min AND block_max` would leak.
    #[test]
    #[serial_test::serial]
    #[cfg(feature = "integration")]
    fn export_read_block_split_member_not_at_max_is_exact() {
        let connstr = delete_test_catalog_connstr();
        let data_path = delete_test_data_path();
        let conn = Connection::open_in_memory().unwrap();
        ducklake::connect_ducklake(&conn, &connstr, &data_path).unwrap();
        ducklake::ensure_read_tables(&conn).unwrap();

        let prep_x: i64 = 942_000; // split: full [942010, 942019], block covers only [942010, 942013]
        let prep_y: i64 = 942_001; // whole: [942050, 942051] — holds block_max

        conn.execute_batch(&format!(
            "DELETE FROM qiita_lake.read WHERE prep_sample_idx IN ({prep_x}, {prep_y});
             INSERT INTO qiita_lake.read (prep_sample_idx, sequence_idx, read_id, sequence1) \
                 SELECT {prep_x}, s, 'x' || s, 'AAAAA' FROM range(942010, 942020) t(s);
             INSERT INTO qiita_lake.read (prep_sample_idx, sequence_idx, read_id, sequence1) VALUES \
                 ({prep_y}, 942050, 'y0', 'CCCCC'), ({prep_y}, 942051, 'y1', 'CCCCA');"
        ))
        .unwrap();

        // prep_x is split at 942013 (< block_max=942051). A global BETWEEN would
        // pull prep_x rows 942014..942019 (in [942010,942051], prep in IN-set);
        // the per-member predicate must exclude them.
        let members = vec![
            auth::ExportReadBlockMember {
                prep_sample_idx: prep_x,
                sequence_idx_start: 942010,
                sequence_idx_stop: 942013,
            },
            auth::ExportReadBlockMember {
                prep_sample_idx: prep_y,
                sequence_idx_start: 942050,
                sequence_idx_stop: 942051,
            },
        ];

        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("reads.parquet");
        let count = export_read_block_to_parquet(&connstr, &data_path, &members, &dest, dir.path())
            .expect("export_read_block_to_parquet failed");
        assert_eq!(
            count, 6,
            "4 (prep_x sub-range 942010..942013) + 2 (prep_y) = 6; tail excluded"
        );

        let reader = Connection::open_in_memory().unwrap();
        let dest_str = dest.to_str().unwrap();
        let x_max: i64 = reader
            .query_row(
                &format!(
                    "SELECT max(sequence_idx) FROM read_parquet('{dest_str}') WHERE prep_sample_idx = {prep_x}"
                ),
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            x_max, 942013,
            "split member's out-of-block tail (942014..019) excluded"
        );

        let _ = conn.execute_batch(&format!(
            "DELETE FROM qiita_lake.read WHERE prep_sample_idx IN ({prep_x}, {prep_y});"
        ));
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

    #[test]
    fn build_query_read_masked_rejects_empty_filter() {
        // An empty filter on the human-read surface would SELECT * every
        // sample's pass-reads across all studies — refuse it (the CP always
        // scopes read_masked tickets, this is defense-in-depth).
        let empty = auth::TicketFilter::new();
        let result = build_query("read_masked", &empty);
        assert!(
            result.is_err(),
            "empty filter on read_masked must be rejected"
        );
    }

    #[test]
    fn build_query_reference_table_allows_empty_filter() {
        // Reference tables are broadly readable by design (mirrors the
        // anonymous REST reference GET), so an unfiltered SELECT is legitimate.
        let empty = auth::TicketFilter::new();
        let (sql, table) = build_query("reference_sequences", &empty)
            .expect("empty filter on a reference table is allowed");
        assert_eq!(table, "qiita_lake.reference_sequences");
        assert_eq!(sql, "SELECT * FROM qiita_lake.reference_sequences");
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
        // DoPut tests don't exercise export_read; any scratch root works here.
        let scratch_root = staging_root
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| staging_root.clone());
        QiitaFlightService::new(
            b"dev-secret".to_vec(),
            // catalog + data_path unused by DoPut path
            "dbname=unused host=localhost".to_string(),
            "/tmp/unused".to_string(),
            staging_root,
            scratch_root,
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
    async fn do_put_alreadyexists_wins_over_mid_stream_decode_error() {
        // Regression: with the async-decoder/blocking-writer bridge, a second
        // DoPut to an occupied upload_idx whose stream ALSO errors mid-flight
        // must still surface AlreadyExists — not the decode error. do_put_inner
        // skips its partial-file cleanup only for AlreadyExists, and that staged
        // file belongs to the first, legitimate upload; masking it as the decode
        // error would unlink their file. Reproduces the writer-task error vs
        // decode-error precedence.
        let tmp = tempfile::tempdir().unwrap();
        let service = make_service(tmp.path().to_path_buf());

        // First upload occupies upload_idx=88.
        let t1 = sign_doput_for_test(88, b"dev-secret", future_expiry_secs(300));
        let m1 = flight_stream_with_ticket(vec![sample_batch()], t1).await;
        service
            .do_put_inner(stream::iter(m1))
            .await
            .expect("first DoPut should succeed");
        let staged = tmp.path().join("uploads/88/upload.parquet");
        let bytes_before = std::fs::read(&staged).unwrap();

        // Second DoPut to the same idx: keep only the schema frame, then inject a
        // mid-stream error. The writer hits AlreadyExists on create_new while the
        // decoder surfaces the error, exercising the precedence.
        let t2 = sign_doput_for_test(88, b"dev-secret", future_expiry_secs(300));
        let mut m2 = flight_stream_with_ticket(vec![sample_batch()], t2).await;
        m2.truncate(1);
        m2.push(Err(Status::internal("simulated mid-stream drop")));

        let err = service
            .do_put_inner(stream::iter(m2))
            .await
            .expect_err("second DoPut must fail");
        assert_eq!(
            err.code(),
            tonic::Code::AlreadyExists,
            "AlreadyExists must win over the decode error so the first file is preserved"
        );

        // The first upload's file is untouched — not unlinked by cleanup.
        assert!(staged.exists(), "the first upload's file must survive");
        assert_eq!(
            std::fs::read(&staged).unwrap(),
            bytes_before,
            "first upload bytes unchanged"
        );
        assert_eq!(
            std::fs::metadata(&staged).unwrap().permissions().mode() & 0o777,
            0o440
        );
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

    #[tokio::test]
    async fn do_put_writes_multi_batch_stream() {
        // Exercise the async-decoder → blocking-writer channel bridge with more
        // than one RecordBatch: every batch must flow through the mpsc channel,
        // be written, and be counted. Three 3-row batches => 9 rows, and the
        // PutResult sha256/bytes must match the file on disk byte-for-byte.
        let tmp = tempfile::tempdir().unwrap();
        let service = make_service(tmp.path().to_path_buf());

        let ticket = sign_doput_for_test(55, b"dev-secret", future_expiry_secs(300));
        let batches = vec![sample_batch(), sample_batch(), sample_batch()];
        let messages = flight_stream_with_ticket(batches, ticket).await;

        let result = service
            .do_put_inner(stream::iter(messages))
            .await
            .expect("multi-batch do_put should succeed");

        let body: serde_json::Value = serde_json::from_slice(&result.app_metadata).unwrap();
        assert_eq!(body["upload_idx"], 55);
        assert_eq!(body["row_count"], 9, "three 3-row batches stream through");

        let staged = tmp.path().join("uploads/55/upload.parquet");
        let actual_bytes = std::fs::metadata(&staged).unwrap().len();
        assert_eq!(body["bytes_received"].as_u64().unwrap(), actual_bytes);
        let mut hasher = Sha256::new();
        hasher.update(std::fs::read(&staged).unwrap());
        let actual_sha: String = hasher
            .finalize()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        assert_eq!(body["sha256"].as_str().unwrap(), actual_sha);

        // Round-trips as a 9-row Parquet.
        let reader = Connection::open_in_memory().unwrap();
        let n: i64 = reader
            .query_row(
                &format!(
                    "SELECT count(*) FROM read_parquet('{}')",
                    staged.to_str().unwrap()
                ),
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(n, 9);
    }

    #[test]
    fn staging_path_for_layout() {
        let root = Path::new("/scratch/ephemeral/staging");
        assert_eq!(
            staging_path_for(root, 42),
            Path::new("/scratch/ephemeral/staging/uploads/42/upload.parquet")
        );
    }

    // --- pushdown performance assessment helpers/tests ----------------------

    /// Write one per-sample `read` Parquet (matching the durable ingest layout:
    /// one file per prep_sample, sorted by sequence_idx, small row groups so
    /// intra-file pruning is exercised) and register it into DuckLake by path.
    #[cfg(feature = "integration")]
    fn seed_one_read_file(
        conn: &Connection,
        seed_dir: &Path,
        prep_sample_idx: i64,
        seq_start: i64,
        n_reads: i64,
    ) {
        let file = seed_dir.join(format!("read_{prep_sample_idx}.parquet"));
        let file_str = file.to_str().unwrap();
        let seq_stop_excl = seq_start + n_reads;
        conn.execute_batch(&format!(
            "COPY (SELECT {prep_sample_idx}::BIGINT AS prep_sample_idx, \
                    s::BIGINT AS sequence_idx, ('r' || s) AS read_id, 'AAAAA' AS sequence1, \
                    NULL::UTINYINT[] AS qual1, NULL::VARCHAR AS sequence2, NULL::UTINYINT[] AS qual2 \
                 FROM range({seq_start}, {seq_stop_excl}) t(s)) \
             TO '{file_str}' (FORMAT PARQUET, ROW_GROUP_SIZE 2048)"
        ))
        .unwrap();
        conn.execute(
            "CALL ducklake_add_data_files('qiita_lake', 'read', ?)",
            duckdb::params![file_str],
        )
        .unwrap();
    }

    /// Sum of `Total Files Read: N` across every scan in a query's EXPLAIN
    /// ANALYZE tree — the deterministic DuckLake file-pruning signal (how many
    /// data files the scan actually opened). Ties the assertion to the pinned
    /// DuckDB build that emits this token.
    #[cfg(feature = "integration")]
    fn files_read_for(conn: &Connection, query: &str) -> i64 {
        let mut stmt = conn.prepare(&format!("EXPLAIN ANALYZE {query}")).unwrap();
        let mut rows = stmt.query([]).unwrap();
        let mut plan = String::new();
        while let Some(row) = rows.next().unwrap() {
            // The tree text is in the last column; concatenate every cell.
            for i in 0..2 {
                if let Ok(s) = row.get::<usize, String>(i) {
                    plan.push_str(&s);
                    plan.push('\n');
                }
            }
        }
        let mut total = 0i64;
        for line in plan.lines() {
            if let Some(idx) = line.find("Total Files Read:") {
                let tail = &line[idx + "Total Files Read:".len()..];
                let n: String = tail.chars().filter(|c| c.is_ascii_digit()).collect();
                if let Ok(v) = n.parse::<i64>() {
                    total += v;
                }
            }
        }
        total
    }

    /// PERFORMANCE ASSESSMENT: prove the block export prunes to the
    /// block's own files and that the pruning is INVARIANT as the `read` table
    /// grows — i.e. a block's cost is bounded by the block, not the table size.
    /// Assumes a fresh catalog (CI resets `qiita_ducklake` before the Rust tier).
    ///
    /// Layout mirrors production: one file per sample (`ducklake_add_data_files`
    /// of a per-sample Parquet sorted by sequence_idx, small row groups). A fixed
    /// 4-file block (one a mid-file split member) is queried after seeding a
    /// SMALL then a LARGE set of disjoint filler files; `Total Files Read` must
    /// stay == the block's file count both times. Also confirms the shipped
    /// `IN + BETWEEN + OR` (V3) prunes exactly as well as `IN + BETWEEN` (V2) and
    /// the per-member `OR` alone (V1) — the exactness residual does not defeat
    /// file pruning.
    #[test]
    #[serial_test::serial]
    #[cfg(feature = "integration")]
    fn export_read_block_prunes_and_scales() {
        let connstr = delete_test_catalog_connstr();
        let data_path = delete_test_data_path();
        let conn = Connection::open_in_memory().unwrap();
        ducklake::connect_ducklake(&conn, &connstr, &data_path).unwrap();
        // Hermetic + re-runnable: drop any prior `read` registrations (leftover
        // external-file entries from a previous run would double the files-read
        // count) and rebuild the tables fresh. Safe under #[serial]: no other
        // read test runs concurrently, and each seeds its own data.
        conn.execute_batch(
            "DROP VIEW IF EXISTS qiita_lake.read_masked; \
             DROP TABLE IF EXISTS qiita_lake.read;",
        )
        .unwrap();
        ducklake::ensure_read_tables(&conn).unwrap();

        let seed_dir = Path::new(&data_path).join("seed_scale");
        std::fs::create_dir_all(&seed_dir).unwrap();

        // Fixed block: 4 samples, each seeded as a whole 6000-read file. Member
        // 970_002 is a SPLIT — its block sub-range is only the first 2000 reads.
        let bp: i64 = 970_000;
        let members: [(i64, i64, i64, i64); 4] = [
            // (prep, file_seq_start, member_start, member_stop)
            (bp, 6_000_000, 6_000_000, 6_005_999),
            (bp + 1, 6_020_000, 6_020_000, 6_025_999),
            (bp + 2, 6_040_000, 6_040_000, 6_041_999), // split: file is [..,6_045_999]
            (bp + 3, 6_060_000, 6_060_000, 6_065_999),
        ];
        for (prep, file_seq_start, _, _) in members {
            seed_one_read_file(&conn, &seed_dir, prep, file_seq_start, 6_000);
        }
        let block_files = members.len() as i64;
        let expected_result: i64 = 6_000 + 6_000 + 2_000 + 6_000; // split trims 4000

        // V3 is built from the SAME production helper the export uses, so this
        // test can't drift from the query the code actually emits. V1/V2 are
        // hand-written comparison baselines (not production shapes).
        let member_structs: Vec<auth::ExportReadBlockMember> = members
            .iter()
            .map(|(p, _, s, e)| auth::ExportReadBlockMember {
                prep_sample_idx: *p,
                sequence_idx_start: *s,
                sequence_idx_stop: *e,
            })
            .collect();
        let in_list = "970000,970001,970002,970003";
        let block_min = 6_000_000;
        let block_max = 6_065_999;
        let member_or = members
            .iter()
            .map(|(p, _, s, e)| {
                format!("(prep_sample_idx = {p} AND sequence_idx BETWEEN {s} AND {e})")
            })
            .collect::<Vec<_>>()
            .join(" OR ");
        let v1 = format!("SELECT prep_sample_idx FROM qiita_lake.read WHERE ({member_or})");
        let v2 = format!(
            "SELECT prep_sample_idx FROM qiita_lake.read WHERE prep_sample_idx IN ({in_list}) AND sequence_idx BETWEEN {block_min} AND {block_max}"
        );
        let v3 = format!(
            "SELECT prep_sample_idx FROM qiita_lake.read WHERE {}",
            block_read_where_clause(&member_structs)
        );

        // Seed a SMALL set of disjoint filler files (far-away ranges), measure.
        let seed_filler = |conn: &Connection, from: i64, to: i64| {
            for i in from..to {
                seed_one_read_file(conn, &seed_dir, 971_000 + i, 7_000_000 + i * 2_000, 500);
            }
        };
        seed_filler(&conn, 0, 16); // total 4 block + 16 filler = 20 files
        let files_small: i64 = conn
            .query_row(
                "SELECT count(DISTINCT prep_sample_idx) FROM qiita_lake.read",
                [],
                |r| r.get(0),
            )
            .unwrap();
        let v3_small = files_read_for(&conn, &v3);
        eprintln!("[1b] {files_small} sample-files total; V3 files read = {v3_small} (block = {block_files})");
        assert_eq!(v3_small, block_files, "V3 must read only the block's files");

        // Grow the table ~5x with more disjoint filler, re-measure the SAME block.
        seed_filler(&conn, 16, 96); // total 4 + 96 = 100 files
        let files_large: i64 = conn
            .query_row(
                "SELECT count(DISTINCT prep_sample_idx) FROM qiita_lake.read",
                [],
                |r| r.get(0),
            )
            .unwrap();
        let v3_large = files_read_for(&conn, &v3);
        let v2_large = files_read_for(&conn, &v2);
        let v1_large = files_read_for(&conn, &v1);
        eprintln!(
            "[1b] {files_large} sample-files total; files read V1={v1_large} V2={v2_large} V3={v3_large} (block = {block_files})"
        );

        // SCALE INVARIANCE: 5x more files, same block → same files read.
        assert_eq!(
            v3_large, block_files,
            "V3 file pruning must be invariant to table size (read only the block's files)"
        );
        assert_eq!(
            v3_large, v3_small,
            "files read must not grow with table size"
        );
        // The coarse IN+BETWEEN is load-bearing: V2 prunes to the block's files,
        // but V1 (the exact per-member OR ALONE) does NOT prune — a bare
        // OR-of-ANDs full-scans every file. That is precisely why the shipped V3
        // keeps IN+BETWEEN in front of the OR: those top-level conjuncts drive the
        // file pruning, and the OR rides along as an exact residual on the pruned
        // rows without defeating it (V3 == V2, not V1).
        assert_eq!(
            v2_large, block_files,
            "V2 (IN+BETWEEN) prunes to block files"
        );
        assert_eq!(
            v1_large, files_large,
            "per-member OR ALONE does not prune (full scan) — coarse IN+BETWEEN is load-bearing"
        );

        // Exactness: V3 returns the split-trimmed result; V2 over-selects the
        // split member's tail (proving the OR residual is load-bearing).
        let v3_rows: i64 = conn
            .query_row(&format!("SELECT count(*) FROM ({v3})"), [], |r| r.get(0))
            .unwrap();
        let v2_rows: i64 = conn
            .query_row(&format!("SELECT count(*) FROM ({v2})"), [], |r| r.get(0))
            .unwrap();
        assert_eq!(v3_rows, expected_result, "V3 exact (split trimmed)");
        assert_eq!(
            v2_rows,
            expected_result + 4_000,
            "V2 over-selects the split tail"
        );
    }

    /// BENCHMARK (post-compaction): DuckLake may compact our per-sample files into
    /// one big file sorted by (prep_sample_idx, sequence_idx) — we don't control
    /// that ("blind to" compaction). File-level pruning then can't skip the merged
    /// file (its prep range spans the block), so efficiency rests on PARQUET
    /// ROW-GROUP pruning inside the file. This benchmark seeds one large merged
    /// file of INCOMPRESSIBLE rows and times a 4-sample block (and a 1-sample
    /// "tight" query) against a forced full scan.
    ///
    /// VERDICT (DuckDB crate 1.10504.0 / DuckLake, measured): row-group pruning IS
    /// active and its benefit SCALES — full/block was ≈3.6x at 159 MB and ≈6.3x at
    /// 477 MB, with the block query staying ~flat (~6 ms) as the file grew while
    /// the full scan grew linearly. So after compaction a block export degrades
    /// GRACEFULLY (bounded by the block's row groups + fixed footer/setup cost),
    /// not to a full-file scan. NB: DuckDB's `operator_rows_scanned` profiling
    /// metric is unreliable here (constant ~32x inflation, identical for pruned and
    /// full queries) — timing on incompressible data is the trustworthy signal.
    ///
    /// `#[ignore]`: a wall-clock benchmark, not a CI regression guard (timing
    /// ratios flake under load). Run manually:
    ///   cargo test --features integration bench_merged_file_rowgroup_pruning -- --ignored --nocapture
    #[test]
    #[ignore = "wall-clock benchmark; run with --ignored"]
    #[serial_test::serial]
    #[cfg(feature = "integration")]
    fn bench_merged_file_rowgroup_pruning() {
        let connstr = delete_test_catalog_connstr();
        let data_path = delete_test_data_path();
        let conn = Connection::open_in_memory().unwrap();
        ducklake::connect_ducklake(&conn, &connstr, &data_path).unwrap();
        conn.execute_batch(
            "DROP VIEW IF EXISTS qiita_lake.read_masked; DROP TABLE IF EXISTS qiita_lake.read;",
        )
        .unwrap();
        ducklake::ensure_read_tables(&conn).unwrap();

        let seed_dir = Path::new(&data_path).join("seed_merged");
        std::fs::create_dir_all(&seed_dir).unwrap();

        // ONE file: 100 samples x 10k reads = 1M rows, sorted by (prep, seq),
        // ROW_GROUP_SIZE 25k -> ~40 row groups. sequence1 is ~150 INCOMPRESSIBLE
        // chars (5x md5) so the file is large (I/O real) — a constant string would
        // zstd away to nothing and mask any full-scan-vs-pruned I/O difference.
        let base: i64 = 980_000;
        let reads_per: i64 = 30_000;
        let n_samples: i64 = 100;
        let seq_base: i64 = 8_000_000;
        let file = seed_dir.join("merged.parquet");
        let file_str = file.to_str().unwrap();
        conn.execute_batch(&format!(
            "COPY (SELECT ({base} + (i // {reads_per}))::BIGINT AS prep_sample_idx, \
                    ({seq_base} + i)::BIGINT AS sequence_idx, ('r' || i) AS read_id, \
                    substr(md5(i::VARCHAR) || md5((i*7)::VARCHAR) || md5((i*13)::VARCHAR) \
                           || md5((i*17)::VARCHAR) || md5((i*19)::VARCHAR), 1, 150) AS sequence1, \
                    NULL::UTINYINT[] AS qual1, NULL::VARCHAR AS sequence2, NULL::UTINYINT[] AS qual2 \
                 FROM range(0, {n_samples} * {reads_per}) t(i) \
                 ORDER BY prep_sample_idx, sequence_idx) \
             TO '{file_str}' (FORMAT PARQUET, ROW_GROUP_SIZE 25000)"
        ))
        .unwrap();
        let file_bytes = std::fs::metadata(&file).map(|m| m.len()).unwrap_or(0);
        eprintln!("[merged] merged file size = {} MB", file_bytes / 1_000_000);
        conn.execute(
            "CALL ducklake_add_data_files('qiita_lake', 'read', ?)",
            duckdb::params![file_str],
        )
        .unwrap();

        // Block = 4 SCATTERED samples (worst case for row-group locality).
        let members = [
            (
                base + 10,
                seq_base + 10 * reads_per,
                seq_base + 10 * reads_per + reads_per - 1,
            ),
            (
                base + 40,
                seq_base + 40 * reads_per,
                seq_base + 40 * reads_per + reads_per - 1,
            ),
            (
                base + 70,
                seq_base + 70 * reads_per,
                seq_base + 70 * reads_per + reads_per - 1,
            ),
            (
                base + 95,
                seq_base + 95 * reads_per,
                seq_base + 95 * reads_per + reads_per - 1,
            ),
        ];
        let member_structs: Vec<auth::ExportReadBlockMember> = members
            .iter()
            .map(|(p, s, e)| auth::ExportReadBlockMember {
                prep_sample_idx: *p,
                sequence_idx_start: *s,
                sequence_idx_stop: *e,
            })
            .collect();
        let where_v3 = block_read_where_clause(&member_structs);
        let q =
            format!("SELECT prep_sample_idx, sequence_idx FROM qiita_lake.read WHERE {where_v3}");

        let total: i64 = conn
            .query_row("SELECT count(*) FROM qiita_lake.read", [], |r| r.get(0))
            .unwrap();
        eprintln!(
            "[merged] total rows = {total} in ONE file; files read = {}",
            files_read_for(&conn, &q)
        );

        // Full scan: a predicate matching everything on a non-stat column, so no
        // prep/seq row-group stats can prune. Tight: a single prep (its rows are
        // one contiguous run → a couple of row groups).
        let full_q = "SELECT prep_sample_idx FROM qiita_lake.read WHERE sequence1 <> ''";
        let tight_q = format!(
            "SELECT prep_sample_idx FROM qiita_lake.read WHERE prep_sample_idx = {}",
            base + 40
        );

        // Wall-clock, min of 7 (the trustworthy signal; operator_rows_scanned is
        // unreliable here — see the doc comment). If row groups are skipped,
        // block/tight are materially faster than the full scan.
        let time_min = |conn: &Connection, query: &str| -> f64 {
            let mut best = f64::MAX;
            for _ in 0..7 {
                let t = std::time::Instant::now();
                let _ = conn
                    .query_row(&format!("SELECT count(*) FROM ({query})"), [], |r| {
                        r.get::<usize, i64>(0)
                    })
                    .unwrap();
                best = best.min(t.elapsed().as_secs_f64());
            }
            best
        };
        let block_t = time_min(&conn, &q);
        let tight_t = time_min(&conn, &tight_q);
        let full_t = time_min(&conn, full_q);
        eprintln!(
            "[merged] time(s) min-of-7: block={block_t:.4} tight_1prep={tight_t:.4} full={full_t:.4}; \
             full/block = {:.2}, full/tight = {:.2}",
            full_t / block_t.max(1e-9),
            full_t / tight_t.max(1e-9)
        );

        // Coarse pruning-active check (generous margin; the measured ratio is
        // several-fold). If this ever fails, DuckLake stopped row-group pruning
        // merged-file scans — investigate before trusting post-compaction perf.
        assert!(
            full_t > block_t * 1.5,
            "expected the block query to be materially faster than a full scan \
             (row-group pruning active); block={block_t:.4}s full={full_t:.4}s"
        );
    }
}
