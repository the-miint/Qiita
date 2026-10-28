//! The qiita data plane's Arrow Flight service.
//!
//! This file holds the `QiitaFlightService` struct, its `blocking_lake` /
//! `blocking_lake_write` helpers, the DoAction helpers `verify_action_payload`
//! (verify the action token and check its `action` field against the header) and
//! `action_response` (serialize a result into the DoAction response), the
//! `REPLAY_SAFE_ACTIONS` registry and the `FlightService` impl with the
//! `do_action` dispatcher. The submodules own the rest:
//!
//! - `doget`: the IPC body codec a DoGet client asks for, and streaming a
//!   DuckLake query's batches over a bounded channel.
//! - `query`: the DoGet table and filter allowlists, projection, and building the
//!   SQL for each ticket surface, including the block-read filter helpers.
//! - `doput`: the staging path an upload lands at, and staging an uploaded Arrow
//!   stream as Parquet.
//! - `export`: the `export_read`, `count_masked` and `mask_metrics` DoActions.
//! - `registration`: the `register_files` DoAction, the lake commit retry and
//!   registration lock, and placing files in lake storage.
//! - `lake_edit`: the DoActions that change the lake other than registration: the
//!   `delete_*` actions, `sync_reference_exclusion` and `mint_phylogeny_edge_id`.

use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::{
    Action, ActionType, Criteria, FlightData, FlightDescriptor, FlightInfo, HandshakeRequest,
    HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use arrow_ipc::writer::IpcWriteOptions;
use duckdb::Connection;
use futures::stream::{self, StreamExt};
use tonic::{Request, Response, Status, Streaming};

use crate::auth;
use crate::pool::{LakePool, LakeWriter};

mod doget;
mod doput;
mod export;
mod lake_edit;
mod query;
mod registration;
#[cfg(test)]
mod test_support;

use doget::{requested_ipc_codec, stream_ducklake_batches};
use query::{build_query, ALLOWED_TABLES};

/// The qiita data plane Flight service.
pub struct QiitaFlightService {
    /// Ed25519 PUBLIC key for ticket verification. Verify-only — the private
    /// signing seed lives only in the control plane.
    flight_public_key: ed25519_dalek::VerifyingKey,
    /// The bounded pool of attached DuckLake instances every request borrows
    /// from. Its size is the in-flight bound: a request that finds it fully
    /// lent out is refused with a retriable status rather than opening another
    /// DuckDB that would size itself against the whole host.
    pool: Arc<LakePool>,
    /// Permits for concurrent DoPuts, which borrow no connection and so are not
    /// bounded by the pool. An upload decodes Arrow batches and writes Parquet,
    /// so its cost is the batches in flight; the measurement the default rests
    /// on is on `DEFAULT_MAX_CONCURRENT_DOPUT` in `config.rs`.
    doput_permits: Arc<tokio::sync::Semaphore>,
    /// The permit count, kept because `available_permits()` is 0 once
    /// `try_acquire` has failed and the refusal names the bound it hit.
    max_concurrent_doput: usize,
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
        flight_public_key: ed25519_dalek::VerifyingKey,
        pool: Arc<LakePool>,
        max_concurrent_doput: usize,
        upload_staging_root: PathBuf,
        scratch_root: PathBuf,
    ) -> Self {
        Self {
            flight_public_key,
            pool,
            doput_permits: Arc::new(tokio::sync::Semaphore::new(max_concurrent_doput)),
            max_concurrent_doput,
            upload_staging_root,
            scratch_root,
        }
    }

    /// Run one blocking lake operation on a borrowed connection.
    ///
    /// Every DoAction that touches the lake is this shape, and the shape is the
    /// reason: DuckDB work is synchronous and some of it is long-lived, so it runs
    /// on the blocking pool rather than starving a tonic async worker. The
    /// checkout is part of it, since it can build and attach a connection. The
    /// connection is borrowed inside the task, which keeps it clear of any await;
    /// it returns to the pool when the task ends,
    /// including when the task ends because the request failed or the client went
    /// away. `what` names the operation in the one error this adds — a join
    /// failure, which means the blocking task panicked or was cancelled.
    async fn blocking_lake<T, F>(&self, what: &str, work: F) -> Result<T, Status>
    where
        F: FnOnce(&Connection) -> Result<T, Status> + Send + 'static,
        T: Send + 'static,
    {
        let pool = Arc::clone(&self.pool);
        tokio::task::spawn_blocking(move || work(&*pool.checkout()?))
            .await
            .map_err(|e| Status::internal(format!("{what} task join failed: {e}")))?
    }

    /// `blocking_lake` for an action that writes to the lake: the same bound, on a
    /// `LakeWriter`.
    async fn blocking_lake_write<T, F>(&self, what: &str, work: F) -> Result<T, Status>
    where
        F: FnOnce(&LakeWriter) -> Result<T, Status> + Send + 'static,
        T: Send + 'static,
    {
        let pool = Arc::clone(&self.pool);
        tokio::task::spawn_blocking(move || work(&pool.checkout_for_write()?))
            .await
            .map_err(|e| Status::internal(format!("{what} task join failed: {e}")))?
    }

    /// Verify a DoAction body with its action's `verify` function, then check that
    /// the payload's own `action` field names the action in the `Action.type`
    /// header, so a token minted for one action is refused under another header.
    fn verify_action_payload<P, E: std::fmt::Display>(
        &self,
        action: &Action,
        verify: impl FnOnce(&[u8], &ed25519_dalek::VerifyingKey) -> Result<P, E>,
        payload_action: impl FnOnce(&P) -> &str,
    ) -> Result<P, Status> {
        let payload = verify(&action.body, &self.flight_public_key)
            .map_err(|e| Status::unauthenticated(e.to_string()))?;
        let named = payload_action(&payload);
        if named != action.r#type {
            return Err(Status::invalid_argument(format!(
                "action type mismatch: header says '{}', payload says {named:?}",
                action.r#type
            )));
        }
        Ok(payload)
    }

    /// The DoAction response: `result` as JSON in a single `arrow_flight::Result`.
    fn action_response<T: serde::Serialize>(
        result: &T,
    ) -> Result<Response<<Self as FlightService>::DoActionStream>, Status> {
        let result_body = serde_json::to_vec(result)
            .map_err(|e| Status::internal(format!("json serialization failed: {e}")))?;
        let result = arrow_flight::Result {
            body: result_body.into(),
        };
        let output = stream::once(futures::future::ready(Ok(result)));
        Ok(Response::new(Box::pin(output)))
    }
}

/// DoAction variants that are safe to replay — the accepted-risk registry.
///
/// Flight action tokens are Ed25519-authenticated but carry **no single-use
/// ledger**: within a token's lifetime (bounded by `MAX_TICKET_LIFETIME`, ~1h)
/// a captured, still-valid token can be replayed. We deliberately do NOT add a
/// server-side nonce/consumed-token store — the operational cost of one is not
/// justified because every action the data plane dispatches is idempotent or
/// otherwise replay-safe (see `docs/auth.md#ticket-replay`):
///
/// - `register_files` — the dest name is deterministic for the registration
///   (see `lake_dest_filename`), so a replay after success finds every file
///   already a data file of its table and returns the earlier registration
///   without writing; a replay that finds the files moved but not all
///   registered fails closed. A call that fails after its moves returns the
///   files to staging unless `return_to_staging` could not confirm that safe,
///   so that state means a call still running, one the data plane stopped
///   mid-call, or one whose error says it kept the files.
/// - `delete_reference` / `delete_mask` / `delete_pool_reads` /
///   `delete_read_mask_block` / `delete_alignment` / `delete_alignment_block` /
///   `delete_alignment_sample` — logical DELETEs, idempotent against themselves:
///   a replay with no write in between deletes zero rows. Not commutative with
///   one. Two run as a pre-`register-files` replace —
///   `delete_read_mask_block` in `read-mask-block` and `delete_alignment_block`
///   in `align` — so they sit in a workflow that registers rows under the same
///   scope a few steps later and a replay landing after that registration drops
///   what it wrote. `delete_alignment_sample` is the same shape and takes on the
///   same exposure once a workflow adopts it. What bounds that window is the
///   token's own expiry, checked on every DoAction body in
///   `auth::verify_ticket_raw`: 300s from
///   `qiita_control_plane.auth.tickets.sign_action`'s default, and
///   `MAX_TICKET_LIFETIME` refuses any token whose expiry is more than 3600s out.
///   The expiry is checked when a request arrives, so it bounds a replayed
///   request, not a call already accepted and still running: a DuckLake
///   transaction reads the snapshot of its first statement, so such a call's
///   DELETE can still run after the rerun's registration and drop it
///   (`ducklake_transaction_reads_the_snapshot_of_its_first_statement`).
/// - `export_read` — re-materializes the same prep_sample's bytes to the same ticket
///   path via atomic publish; a replay reproduces identical output. (The block
///   exports that used to sit beside it are gone: block-scoped compute now
///   STREAMS its reads over the `read_block` / `read_masked_block` DoGet
///   selectors, which are read-only and need no replay classification.)
/// - `count_masked` / `mask_metrics` — read-only aggregates.
/// - `sync_reference_exclusion` — full-replace of the `reference_exclusion`
///   mirror from the CP's resolved blocklist Parquet (one DELETE + INSERT
///   transaction); a replay reloads the same authoritative set, so the table
///   converges to the same state.
/// - `mint_phylogeny_edge_id` — sets `edge_id = node_index` on ONE reference's
///   `reference_phylogeny` rows, and only when every one of them carries NULL.
///   A replay re-reads the counts, finds the tree numbered, and writes nothing.
///   The only writer that could put all-NULL rows back under the same
///   `reference_idx` is `reference_load._write_phylogeny`, which mints them
///   itself, so there is no window a replay could fill differently.
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
    "delete_alignment",
    "delete_alignment_block",
    "delete_alignment_sample",
    "export_read",
    "count_masked",
    "mask_metrics",
    "sync_reference_exclusion",
    "mint_phylogeny_edge_id",
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
        // Read the request metadata before `into_inner()` consumes it.
        let codec = requested_ipc_codec(request.metadata())?;
        let ticket_bytes = &request.into_inner().ticket;

        // Verify Ed25519 signature, expiry, and parse payload
        let payload = auth::verify_ticket(ticket_bytes, &self.flight_public_key)
            .map_err(|e| Status::unauthenticated(e.to_string()))?;

        // Validate table name
        if !ALLOWED_TABLES.contains(&payload.table.as_str()) {
            return Err(Status::invalid_argument(format!(
                "unknown table: {:?}",
                payload.table
            )));
        }

        // Build query from filter
        let (sql, table) = build_query(
            &payload.table,
            &payload.filter,
            &payload.members,
            &payload.columns,
        )?;

        // Stream the result incrementally, from an instance borrowed for the
        // life of the stream: a blocking task feeds RecordBatches through a
        // bounded channel, so the data plane never buffers the whole result set
        // (the non-blocking, memory-bounded path). Ticket/table/query-shape
        // errors above are returned synchronously, and so is a pool with nothing
        // left to lend; per-request DB errors surface as a stream item.
        let conn = self.pool.checkout()?;
        let batch_stream = stream_ducklake_batches(conn, sql, table);
        // With no codec these options are structurally the encoder default that
        // preceded this change (pinned by
        // `no_codec_write_options_match_the_encoder_default`), which is what keeps
        // every existing client unaffected.
        //
        // `try_with_compression` is NOT the missing-feature check: its only error
        // is `metadata_version < V5`, which `IpcWriteOptions::default()` cannot
        // hit. A build without `arrow-ipc/zstd` fails instead inside arrow-ipc's
        // `compress_zstd`, per batch, after the schema message has already
        // shipped — so it arrives as a stream error, not from this call. The
        // `map_err` stays because an error here is a build mistake either way and
        // must not read as bad client input.
        let write_options = IpcWriteOptions::default()
            .try_with_compression(codec)
            .map_err(|e| Status::internal(format!("IPC codec {codec:?} unavailable: {e}")))?;
        let flight_stream = FlightDataEncoderBuilder::new()
            .with_options(write_options)
            .build(batch_stream);
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

        // replay: Flight action tokens are Ed25519-authenticated but have NO
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
            "register_files" => self.register_files_action(&action).await,
            "delete_reference" => self.delete_reference_action(&action).await,
            "delete_mask" => self.delete_mask_action(&action).await,
            "delete_pool_reads" => self.delete_pool_reads_action(&action).await,
            "delete_read_mask_block" => self.delete_read_mask_block_action(&action).await,
            "delete_alignment" => self.delete_alignment_action(&action).await,
            "delete_alignment_block" => self.delete_alignment_block_action(&action).await,
            "delete_alignment_sample" => self.delete_alignment_sample_action(&action).await,
            "export_read" => self.export_read_action(&action).await,
            "count_masked" => self.count_masked_action(&action).await,
            "mask_metrics" => self.mask_metrics_action(&action).await,
            "sync_reference_exclusion" => self.sync_reference_exclusion_action(&action).await,
            "mint_phylogeny_edge_id" => self.mint_phylogeny_edge_id_action(&action).await,
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

#[cfg(test)]
mod tests;
