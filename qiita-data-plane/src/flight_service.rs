//! Arrow Flight service implementation for the qiita data plane.
//!
//! Handles DoGet requests by verifying Ed25519-signed tickets, querying DuckLake,
//! and streaming results as Arrow RecordBatches.
//!
//! Each request opens its own DuckDB connection and attaches DuckLake. This
//! avoids shared mutable state and allows concurrent requests; DuckLake's
//! snapshot isolation in the shared Postgres catalog keeps readers off each
//! other. It is not sufficient for every writer, though — it detects a conflict
//! only where two transactions touch the same existing row, so writers that must
//! not both commit take an explicit lock (`take_registration_lock`).

use std::collections::BTreeMap;
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
use arrow_ipc::writer::IpcWriteOptions;
use arrow_ipc::CompressionType;
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
/// per call. The break-even arithmetic is in `docs/architecture.md`.
///
/// An unrecognised value is an **error, not a fallback**. A client that asked
/// for compression, silently did not get it, and measured the result would draw
/// the wrong conclusion about its own transfer. `lz4` is rejected along with
/// everything else rather than served as a quietly worse stream.
fn requested_ipc_codec(
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

/// The qiita data plane Flight service.
pub struct QiitaFlightService {
    /// Ed25519 PUBLIC key for ticket verification. Verify-only — the private
    /// signing seed lives only in the control plane.
    flight_public_key: ed25519_dalek::VerifyingKey,
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
        flight_public_key: ed25519_dalek::VerifyingKey,
        catalog_connstr: String,
        data_path: String,
        upload_staging_root: PathBuf,
        scratch_root: PathBuf,
    ) -> Self {
        Self {
            flight_public_key,
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
///   * The `Arrow`/`ArrowStream` iterator borrows the `Statement`, which borrows
///     the `Connection`; the `DoGetStream` we return must be `'static`, so the
///     DuckDB iterator cannot be handed out directly. A blocking task owns the
///     connection for the query's lifetime and pushes each `RecordBatch` into a
///     bounded channel (`DOGET_BATCH_CHANNEL_DEPTH`); the returned stream drains
///     the receiver, applying backpressure to the streaming producer.
///
/// `stream_arrow` needs the result schema up front (streaming execution doesn't
/// surface it until a chunk is fetched), so we first probe it with a zero-row
/// prepare — see the body.
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

            // Obtain the result schema WITHOUT materializing the result. DuckDB's
            // streaming execution (below) doesn't expose the schema until the
            // first chunk is fetched, and `stream_arrow` needs it up front, so
            // probe with a zero-row prepare: `LIMIT 0` plans the query and returns
            // its column schema having produced no rows (trivial memory).
            let schema = {
                let mut probe = conn
                    .prepare(&format!("SELECT * FROM ({sql}) AS _schema_probe LIMIT 0"))
                    .map_err(|e| {
                        Status::internal(format!(
                            "schema probe preparation failed for {table}: {e}"
                        ))
                    })?;
                probe
                    .query_arrow([])
                    .map_err(|e| {
                        Status::internal(format!("schema probe execution failed for {table}: {e}"))
                    })?
                    .get_schema()
            };

            let mut stmt = conn.prepare(&sql).map_err(|e| {
                Status::internal(format!("query preparation failed for {table}: {e}"))
            })?;
            // STREAMING execution (duckdb_execute_prepared_streaming): DuckDB
            // fetches one data chunk at a time instead of materializing the whole
            // result set. This is load-bearing — the materialized `query_arrow`
            // buffered the ENTIRE result in DuckDB before the first batch, which
            // OOM-killed the data plane (~374 GB) on a whole-reference DoGet (the
            // rype router streaming every genome's `chunk_data`). Peak memory is
            // now the streaming query's working set (a small hash-join build side
            // + a few vectors) plus DOGET_BATCH_CHANNEL_DEPTH batches in flight.
            let stream = stmt.stream_arrow([], schema.clone()).map_err(|e| {
                Status::internal(format!("query execution failed for {table}: {e}"))
            })?;
            let mut produced = false;
            // `stream`'s Item is a bare `RecordBatch`, not a `Result` — a failure
            // once iteration has begun cannot be observed here; the loop just ends
            // and the consumer sees a clean EOF (see the fn-level caveat). Nothing
            // to do about it until the duckdb API is fallible.
            for batch in stream {
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
/// PRIVACY: the bare `read` and `read_mask` tables are deliberately absent, and
/// must stay absent. A whole-table name here would make an unscoped raw-read
/// SELECT representable. `read_masked` is the only broadly-reachable read
/// surface, and it excludes host/human and QC-failed rows by construction (an
/// unconditional `reason = 'pass'`). It is no longer unrestricted either: it is a
/// table MACRO, not a relation, and what its required arguments foreclose is
/// documented where they are declared (`ducklake.rs`).
///
/// `alignment_origin_spanning` is absent too. It carries `feature_idx`, so
/// exposing it would need the same `reference_exclusion` anti-join
/// `alignment_visible` has — otherwise a blocked genome reaches a consumer
/// through the side table while the view refuses it. Nothing reads it over
/// Flight today, so no view is built.
///
/// `read_block` is the one path to raw `read` rows over Flight, and it is
/// admissible only because it CANNOT express an unscoped read: it is not a table
/// name, it is a block-read *selector* form that `build_query` rejects unless the
/// ticket carries a non-empty `members` list (see `BLOCK_READ_SOURCES`). The
/// bytes it streams are the block's own reads, delivered to one authenticated
/// compute job rather than written as a human-readable Parquet onto a shared
/// filesystem — the narrower of the two transports. Direct DB tooling on the host
/// remains the path for anything unscoped.
const ALLOWED_TABLES: &[&str] = &[
    "reference_sequences",
    "reference_sequence_chunks",
    // The exclusion-aware taxonomy view (`reference_taxonomy` ANTI JOIN the
    // resolved blocklist). Reads go through the VIEW, never the raw base table
    // — like `read_masked` over `read`, so a curated exclusion can't be bypassed
    // by any consumer. The raw `reference_taxonomy` is deliberately absent (it is
    // still the register-files write target, but not Flight-readable).
    "reference_taxonomy_visible",
    "reference_phylogeny",
    "reference_placements",
    "reference_annotation",
    "read_masked",
    // Block-read selectors — members-scoped by construction (BLOCK_READ_SOURCES).
    "read_block",
    "read_masked_block",
    // The alignment sink's read-side, for the feature-table (OGU) consumer, as
    // the exclusion-aware VIEW (`alignment` ANTI JOIN the resolved blocklist) —
    // raw `alignment` is deliberately absent so a blocked feature can't reach an
    // OGU rollup. It holds host-depleted, derived per-read alignments (not raw
    // human reads), so — unlike read_masked — it is not the human-read privacy
    // surface. Reads are projected to the ticket's signed column list — required
    // here, unlike every other table — and always scoped by alignment_idx +
    // prep_sample_idx (see build_query / ALIGNMENT_PROJECTION_COLUMNS).
    "alignment_visible",
    // One assembly run's contigs — sample-derived sequence, where everything
    // above is reference data or per-read derived output. Neither table has a
    // prep_sample_idx column; a ticket names the run by `(prep_sample_idx,
    // processing_idx)` and `build_assembly_run_query` resolves it through
    // `qiita_lake.assembly_membership`, the same shape as the `reference_idx`
    // resolution the MEMBERSHIP_JOIN_TABLES get. A `feature_idx` filter is NOT
    // accepted on either, and neither is an empty one.
    "assembled_sequence",
    "assembled_sequence_chunks",
];

/// Allowed column names for filter clauses. All identifier columns that can
/// appear in a signed ticket's filter. Whitelist prevents information leakage
/// via error messages for non-existent columns.
const ALLOWED_FILTER_COLUMNS: &[&str] = &[
    "feature_idx",
    "parent_feature_idx",
    "annotation_idx",
    "reference_idx",
    "node_index",
    "mask_idx",
    "prep_sample_idx",
    // Scopes an alignment DoGet to a single alignment run (feature-table consumer).
    "alignment_idx",
    // With prep_sample_idx, names the assembly RUN an assembly DoGet resolves
    // through `assembly_membership` (`build_assembly_run_query`).
    "processing_idx",
];

/// Columns a signed ticket may ask the alignment DoGet to project: every column
/// of `qiita_lake.alignment`, which `alignment_visible` mirrors (`SELECT a.*`,
/// see `ducklake::ensure_exclusion_tables`). Keep in step with
/// `ensure_alignment_tables`' DDL.
///
/// This is the Rust half of a CP-mirrored pair — the control plane validates the
/// same set at mint time, so an unknown column is refused before it is ever
/// signed. Both halves exist on purpose: the CP's copy turns a consumer's typo
/// into a 422 with a useful message, and this one is the defense-in-depth that
/// keeps a signed name out of interpolated SQL.
///
/// The allowlist is per-table (see `projection_allowlist`) and today only the
/// alignment surface has one; every other DoGet table streams `SELECT *` and
/// refuses a column list outright. Why the asymmetry: `docs/architecture.md`.
const ALIGNMENT_PROJECTION_COLUMNS: &[&str] = &[
    "alignment_idx",
    "prep_sample_idx",
    "sequence_idx",
    "feature_idx",
    "mate_feature_idx",
    "flags",
    "position",
    "stop_position",
    "mapq",
    "cigar",
    "mate_position",
    "template_length",
    "tag_as",
    "tag_xs",
    "tag_ys",
    "tag_xn",
    "tag_xm",
    "tag_xo",
    "tag_xg",
    "tag_nm",
    "tag_yt",
    "tag_md",
    "tag_sa",
];

/// Tables that resolve a `reference_idx` filter through a JOIN against
/// `reference_membership` — they have no `reference_idx` column of their own.
///
/// Named rather than inlined at the one `if` that needs them because the JOIN and
/// the projection do not compose (see `build_query`), and an invariant nothing can
/// name is an invariant nothing can check —
/// `no_membership_join_table_has_a_projection_allowlist` does.
const MEMBERSHIP_JOIN_TABLES: &[&str] = &["reference_sequences", "reference_sequence_chunks"];

/// The projection allowlist for `table`, or `None` when the table takes no
/// column list at all (it streams `SELECT *`, and a list is a control-plane bug).
fn projection_allowlist(table: &str) -> Option<&'static [&'static str]> {
    is_alignment_doget_surface(table).then_some(ALIGNMENT_PROJECTION_COLUMNS)
}

/// The SQL select list for `table`, given the ticket's (possibly empty) column
/// list: the signed columns in the ticket's own order, or `*` for a table that
/// takes no projection.
///
/// **Having an allowlist and requiring a list are the same property.** A table
/// only gets an allowlist because serving it unprojected is the wrong default,
/// so the four cases below are total and there is no "projection is optional
/// here" state to reason about. Splitting them is a one-line change if that ever
/// becomes something we want.
///
/// Every rejection is a control-plane bug rather than client input — the CP
/// validates the same set before signing — so failing loudly is the point: the
/// alternative is quietly serving a different set of columns than was signed.
fn select_list_for(table: &str, columns: &[String]) -> Result<String, Status> {
    match (projection_allowlist(table), columns.is_empty()) {
        (None, true) => Ok("*".to_string()),
        // No server-side default to fall back to, deliberately: the consumer is
        // the only component that knows which columns it binds, and a fallback
        // here would be a second answer to that question, free to drift wider
        // than what was asked for. A ticket minted before this shipped and
        // redeemed after lands here — loudly, inside its 300 s TTL — rather than
        // being silently widened.
        (Some(_), true) => Err(Status::invalid_argument(format!(
            "{table} requires an explicit projection column list"
        ))),
        // Ignoring the list would serve wider rows than the ticket asked for,
        // which is the silent widening this whole mechanism exists to prevent.
        (None, false) => Err(Status::invalid_argument(format!(
            "table {table:?} does not accept a projection column list"
        ))),
        (Some(allowed), false) => {
            check_projection_columns(allowed, columns)?;
            Ok(columns.join(", "))
        }
    }
}

/// Reject a projection column that is not on `allowed`, or named twice.
///
/// Names are whitelisted even though the ticket is signature-verified, because
/// they are interpolated into SQL — the same defense-in-depth argument
/// `ALLOWED_FILTER_COLUMNS` makes. A repeated name is refused rather than
/// deduped: it produces two identically-named Arrow fields, which consumers
/// collapse or reject inconsistently, and picking a behaviour for them would be
/// guessing.
fn check_projection_columns(allowed: &[&str], columns: &[String]) -> Result<(), Status> {
    let mut seen: Vec<&str> = Vec::with_capacity(columns.len());
    for col in columns {
        if !allowed.contains(&col.as_str()) {
            return Err(Status::invalid_argument(format!(
                "unknown projection column: {col:?}"
            )));
        }
        if seen.contains(&col.as_str()) {
            return Err(Status::invalid_argument(format!(
                "duplicate projection column: {col:?}"
            )));
        }
        seen.push(col);
    }
    Ok(())
}

/// The block-read DoGet selectors, mapped to the DuckLake relation each streams.
///
/// These are ticket `table` values, not table names: a block is a set of
/// `(prep_sample_idx, sequence_idx sub-range)` members, which the flat
/// column/value `TicketFilter` cannot express, so the ticket carries `members`
/// and `build_query` translates them through the shared `block_read_where_clause`
/// — the same selector the block DELETE actions use, so a block's read footprint
/// and its delete footprint can never drift.
///
/// * `read_block` → raw `read` rows: the reads a read-mask block masks.
/// * `read_masked_block` → `read_masked` rows (trimmed, host/QC-`pass`-filtered),
///   additionally scoped to one `mask_idx`: the reads an align block aligns.
///
/// Both REQUIRE a non-empty `members` list; `read_masked_block` additionally
/// requires exactly one `mask_idx`. That requirement is what makes `read_block`
/// safe to expose at all (see the PRIVACY note on `ALLOWED_TABLES`): there is no
/// ticket shape that reaches raw reads without naming the exact sub-ranges.
const BLOCK_READ_SOURCES: &[(&str, &str)] =
    &[("read_block", "read"), ("read_masked_block", "read_masked")];

/// The DuckLake relation a block-read selector streams, or `None` if `table` is
/// not one. Single lookup point for both the guard and the query build.
fn block_read_source(table: &str) -> Option<&'static str> {
    BLOCK_READ_SOURCES
        .iter()
        .find(|(name, _)| *name == table)
        .map(|(_, source)| *source)
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
/// - `register_files` — a replay after success hits the staging-existence check
///   first and returns `not_found`, its files having been moved out. Where the
///   source survives instead (the EXDEV copy branch of `move_file`), the minted
///   dest name is deterministic for the registration — see `lake_dest_filename`
///   — so `move_file` refuses to overwrite and it fails closed rather than
///   double-registering.
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
/// - `export_read` — re-materializes the same sample's bytes to the same ticket
///   path via atomic publish; a replay reproduces identical output. (The block
///   exports that used to sit beside it are gone: block-scoped compute now
///   STREAMS its reads over the `read_block` / `read_masked_block` DoGet
///   selectors, which are read-only and need no replay classification.)
/// - `count_masked` / `mask_metrics` — read-only aggregates.
/// - `sync_reference_exclusion` — full-replace of the `reference_exclusion`
///   mirror from the CP's resolved blocklist Parquet (one DELETE + INSERT
///   transaction); a replay reloads the same authoritative set, so the table
///   converges to the same state.
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
            "register_files" => {
                let payload = auth::verify_action(&action.body, &self.flight_public_key)
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
                let scratch_root = self.scratch_root.clone();
                let registration = tokio::task::spawn_blocking(move || {
                    register_files(&catalog, &data_path, &scratch_root, &payload)
                })
                .await
                .map_err(|e| Status::internal(format!("register_files task join failed: {e}")))??;

                let result_body = serde_json::to_vec(&serde_json::json!({
                    "registered": registration.registered,
                    "replaced": registration.replaced,
                }))
                .map_err(|e| Status::internal(format!("json serialization failed: {e}")))?;

                let result = arrow_flight::Result {
                    body: result_body.into(),
                };
                let output = stream::once(futures::future::ready(Ok(result)));
                Ok(Response::new(Box::pin(output)))
            }
            "delete_reference" => {
                let payload = auth::verify_delete_reference(&action.body, &self.flight_public_key)
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
                let payload = auth::verify_delete_mask(&action.body, &self.flight_public_key)
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
                let payload = auth::verify_delete_pool_reads(&action.body, &self.flight_public_key)
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
                let payload =
                    auth::verify_delete_read_mask_block(&action.body, &self.flight_public_key)
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

                // Blocking DuckLake delete transaction — run it on the blocking
                // pool so it never starves a tonic async worker (mirrors
                // delete_alignment_block). The closure opens and drops its own
                // connection, so it is Send and crosses no await.
                let catalog = self.catalog_connstr.clone();
                let data_path = self.data_path.clone();
                let mask_idx = payload.mask_idx;
                let members = payload.members;
                let deleted = tokio::task::spawn_blocking(move || {
                    delete_read_mask_block(&catalog, &data_path, mask_idx, &members)
                })
                .await
                .map_err(|e| {
                    Status::internal(format!("delete_read_mask_block task join failed: {e}"))
                })??;

                let result_body = serde_json::to_vec(&deleted)
                    .map_err(|e| Status::internal(format!("json serialization failed: {e}")))?;

                let result = arrow_flight::Result {
                    body: result_body.into(),
                };
                let output = stream::once(futures::future::ready(Ok(result)));
                Ok(Response::new(Box::pin(output)))
            }
            "delete_alignment" => {
                let payload = auth::verify_delete_alignment(&action.body, &self.flight_public_key)
                    .map_err(|e| Status::unauthenticated(e.to_string()))?;

                if payload.action != "delete_alignment" {
                    return Err(Status::invalid_argument(format!(
                        "action type mismatch: header says 'delete_alignment', payload says {:?}",
                        payload.action
                    )));
                }

                // Blocking DuckLake delete transaction — run it on the blocking
                // pool so it never starves a tonic async worker (mirrors
                // delete_mask). The closure opens and drops its own connection, so
                // it is Send and crosses no await.
                let catalog = self.catalog_connstr.clone();
                let data_path = self.data_path.clone();
                let alignment_idx = payload.alignment_idx;
                let deleted = tokio::task::spawn_blocking(move || {
                    delete_alignment(&catalog, &data_path, alignment_idx)
                })
                .await
                .map_err(|e| {
                    Status::internal(format!("delete_alignment task join failed: {e}"))
                })??;

                let result_body = serde_json::to_vec(&deleted)
                    .map_err(|e| Status::internal(format!("json serialization failed: {e}")))?;

                let result = arrow_flight::Result {
                    body: result_body.into(),
                };
                let output = stream::once(futures::future::ready(Ok(result)));
                Ok(Response::new(Box::pin(output)))
            }
            "delete_alignment_block" => {
                let payload =
                    auth::verify_delete_alignment_block(&action.body, &self.flight_public_key)
                        .map_err(|e| Status::unauthenticated(e.to_string()))?;

                if payload.action != "delete_alignment_block" {
                    return Err(Status::invalid_argument(format!(
                        "action type mismatch: header says 'delete_alignment_block', \
                         payload says {:?}",
                        payload.action
                    )));
                }
                // An empty block is a control-plane bug, not a valid ask —
                // reject it loudly rather than deleting nothing silently.
                if payload.members.is_empty() {
                    return Err(Status::invalid_argument(
                        "delete_alignment_block requires a non-empty members list",
                    ));
                }

                // Blocking DuckLake delete transaction — run it on the blocking
                // pool so it never starves a tonic async worker (mirrors
                // delete_alignment). The closure opens and drops its own
                // connection, so it is Send and crosses no await.
                let catalog = self.catalog_connstr.clone();
                let data_path = self.data_path.clone();
                let alignment_idx = payload.alignment_idx;
                let members = payload.members;
                let deleted = tokio::task::spawn_blocking(move || {
                    delete_alignment_block(&catalog, &data_path, alignment_idx, &members)
                })
                .await
                .map_err(|e| {
                    Status::internal(format!("delete_alignment_block task join failed: {e}"))
                })??;

                let result_body = serde_json::to_vec(&deleted)
                    .map_err(|e| Status::internal(format!("json serialization failed: {e}")))?;

                let result = arrow_flight::Result {
                    body: result_body.into(),
                };
                let output = stream::once(futures::future::ready(Ok(result)));
                Ok(Response::new(Box::pin(output)))
            }
            "delete_alignment_sample" => {
                let payload =
                    auth::verify_delete_alignment_sample(&action.body, &self.flight_public_key)
                        .map_err(|e| Status::unauthenticated(e.to_string()))?;

                if payload.action != "delete_alignment_sample" {
                    return Err(Status::invalid_argument(format!(
                        "action type mismatch: header says 'delete_alignment_sample', \
                         payload says {:?}",
                        payload.action
                    )));
                }

                // Blocking DuckLake delete transaction — run it on the blocking
                // pool so it never starves a tonic async worker (mirrors
                // delete_alignment). The closure opens and drops its own
                // connection, so it is Send and crosses no await.
                let catalog = self.catalog_connstr.clone();
                let data_path = self.data_path.clone();
                let alignment_idx = payload.alignment_idx;
                let prep_sample_idx = payload.prep_sample_idx;
                let deleted = tokio::task::spawn_blocking(move || {
                    delete_alignment_sample(&catalog, &data_path, alignment_idx, prep_sample_idx)
                })
                .await
                .map_err(|e| {
                    Status::internal(format!("delete_alignment_sample task join failed: {e}"))
                })??;

                let result_body = serde_json::to_vec(&deleted)
                    .map_err(|e| Status::internal(format!("json serialization failed: {e}")))?;

                let result = arrow_flight::Result {
                    body: result_body.into(),
                };
                let output = stream::once(futures::future::ready(Ok(result)));
                Ok(Response::new(Box::pin(output)))
            }
            "export_read" => {
                let payload = auth::verify_export_read(&action.body, &self.flight_public_key)
                    .map_err(|e| Status::unauthenticated(e.to_string()))?;

                if payload.action != "export_read" {
                    return Err(Status::invalid_argument(format!(
                        "action type mismatch: header says 'export_read', payload says {:?}",
                        payload.action
                    )));
                }

                // Defense in depth on the signature-trusted destination before it is
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
            "count_masked" => {
                // Reuse the *DoGet* read_masked ticket rather than minting a
                // bespoke action token: counting the rows a ticket selects is
                // strictly less than streaming them, so the ticket's
                // (prep_sample_idx, mask_idx) authorization already covers it —
                // no new ticket type or control-plane route is needed, the CLI
                // sends the same signed bytes it would otherwise stream with.
                let payload = auth::verify_ticket(&action.body, &self.flight_public_key)
                    .map_err(|e| Status::unauthenticated(e.to_string()))?;
                if payload.table != "read_masked" {
                    return Err(Status::invalid_argument(format!(
                        "count_masked requires a read_masked ticket, got table {:?}",
                        payload.table
                    )));
                }
                // Refused, not ignored — the same rule the DoGet projection
                // follows. Unreachable today (`read_masked` takes no projection,
                // so the control plane cannot sign one) and harmless if it were
                // reached, since this returns a count and no rows. It is here
                // because "a signed field this arm silently disregards" is the one
                // shape that turns a narrowed ticket into a wider answer, and this
                // was the only place in the ticket surface still allowing it.
                if !payload.columns.is_empty() {
                    return Err(Status::invalid_argument(
                        "count_masked takes no projection column list",
                    ));
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
                let payload = auth::verify_mask_metrics(&action.body, &self.flight_public_key)
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
            "sync_reference_exclusion" => {
                let payload =
                    auth::verify_sync_reference_exclusion(&action.body, &self.flight_public_key)
                        .map_err(|e| Status::unauthenticated(e.to_string()))?;

                if payload.action != "sync_reference_exclusion" {
                    return Err(Status::invalid_argument(format!(
                        "action type mismatch: header says 'sync_reference_exclusion', \
                         payload says {:?}",
                        payload.action
                    )));
                }

                // Defense in depth on the signature-trusted destination before it
                // is inlined into a DuckDB `read_parquet(...)` literal and read.
                let dest = validate_export_dest(&payload.dest, &self.scratch_root)?;

                // The full-replace runs a blocking DuckLake transaction (and a
                // blocking `canonicalize` in the handler) — run it on the blocking
                // pool so it never starves a tonic async worker (mirrors
                // register_files / export_read). The closure opens and drops its
                // own connection, so it is Send and crosses no await.
                let catalog = self.catalog_connstr.clone();
                let data_path = self.data_path.clone();
                let scratch_root = self.scratch_root.clone();
                let result = tokio::task::spawn_blocking(move || {
                    sync_reference_exclusion(&catalog, &data_path, &dest, &scratch_root)
                })
                .await
                .map_err(|e| {
                    Status::internal(format!("sync_reference_exclusion task join failed: {e}"))
                })??;

                let result_body = serde_json::to_vec(&result)
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
        let payload = auth::verify_doput(&descriptor.cmd, &self.flight_public_key)
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

/// The read projection, in `read` / `read_masked` table order. Shared by the
/// per-sample `export_read` DoAction (from `qiita_lake.read`) and by BOTH
/// block-read DoGet selectors (`read_block` from `qiita_lake.read`,
/// `read_masked_block` from the `read_masked` MACRO), so every read payload the
/// data plane hands a compute job has the identical column shape — the shape
/// `align_sharded.reads` / the read-mask jobs bind. `read_masked` exposes exactly
/// these columns (plus `mask_idx`), already trimmed and `pass`-filtered.
const EXPORT_READ_COLUMNS: &str =
    "prep_sample_idx, sequence_idx, read_id, sequence1, qual1, sequence2, qual2";

/// Validate a control-plane-signed `export_read` destination before the data
/// plane writes to it. The token is signature-trusted, so this is defense in depth:
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

/// Run a caller-supplied `select_sql` (an already-safe SELECT — signature-verified
/// inlined integers only) and materialize its rows into a Parquet at `dest`.
/// Returns the row count.
///
/// Machinery for the `export_read` DoAction — one whole sample from
/// `qiita_lake.read`. (Its block siblings are gone: a block's reads STREAM over
/// the `read_block` / `read_masked_block` DoGet selectors.) The caller builds its
/// own SELECT (same `EXPORT_READ_COLUMNS` projection so the output shape is
/// identical); this function owns the publish. An empty selection writes NO file
/// and returns 0 — the control plane turns that into a clean submission failure.
/// The `COPY` streams row groups to disk, so memory stays bounded regardless of
/// selection size. Opens and drops its own connection so the caller can run it on
/// the blocking pool (mirrors `register_files`).
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
fn export_select_to_parquet(
    catalog_connstr: &str,
    data_path: &str,
    select_sql: &str,
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
    // `.partial` suffix preserves all of that; the `select_sql` carries only
    // signature-verified inlined integers — all safe to inline. Callers project the
    // full `EXPORT_READ_COLUMNS` set in table order, so the file is a drop-in for
    // the durable staging copy (modulo row order, which does not matter).
    let tmp = {
        let mut s = dest.as_os_str().to_os_string();
        s.push(".partial");
        PathBuf::from(s)
    };
    let tmp_sql = tmp
        .to_str()
        .ok_or_else(|| Status::internal(format!("non-UTF-8 dest path: {}", tmp.display())))?;
    let copy_sql = format!("COPY ({select_sql}) TO '{tmp_sql}' ({EXPORT_READ_PARQUET_OPTS})");

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
/// writes NO file and returns 0. `prep_sample_idx` is a signature-verified i64, safe
/// to inline. See `export_select_to_parquet` for the shared write/publish.
fn export_read_to_parquet(
    catalog_connstr: &str,
    data_path: &str,
    prep_sample_idx: i64,
    dest: &Path,
    scratch_root: &Path,
) -> Result<i64, Status> {
    export_select_to_parquet(
        catalog_connstr,
        data_path,
        &format!(
            "SELECT {EXPORT_READ_COLUMNS} FROM qiita_lake.read \
             WHERE prep_sample_idx = {prep_sample_idx}"
        ),
        dest,
        scratch_root,
    )
}

/// Build a block's `read` WHERE clause from its members. The ONE translator from
/// block members to SQL: the block-read DoGet selectors (`build_block_read_query`)
/// and the block DELETE actions all go through it, so a block's read footprint and
/// its delete footprint cannot drift, and the pushdown performance-assessment test
/// exercises the EXACT predicate they emit rather than a hand-written copy.
///
/// Two coarse conjuncts drive pruning (both are top-level, so DuckDB pushes them
/// to the scan): `prep_sample_idx IN (...)` prunes DuckLake data files by their
/// per-file `prep_sample_idx` stats, and `sequence_idx BETWEEN block_min AND
/// block_max` bounds the row-group span. A third conjunct — the per-member OR of
/// `(prep_sample_idx = p AND sequence_idx BETWEEN start AND stop)` — is the exact
/// residual on the pruned rows, so a split member never leaks a sibling block's
/// rows (independent of tiling order). The coarse pair is a superset of the OR,
/// so `coarse AND exact == exact`. `members` must be non-empty (caller guards);
/// all integers are signature-verified i64s, safe to inline.
fn block_read_where_clause(members: &[auth::BlockReadMember]) -> String {
    let in_list = block_member_preps(members)
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

/// Pull exactly one i64 out of a ticket filter column. Several paths need a
/// single-valued scope rather than an IN-set — the count/metrics aggregates on
/// `prep_sample_idx` / `mask_idx`, and the `read_masked_block` DoGet on
/// `mask_idx` — so a missing, empty, multi-valued, or non-integer column is a
/// malformed-ticket error; the control plane always signs a one-element list for
/// these. Input is signature-verified (set by the control plane), but we validate
/// anyway for defense in depth.
fn single_i64_filter(filter: &auth::TicketFilter, col: &str) -> Result<i64, Status> {
    let values = filter.get(col).ok_or_else(|| {
        Status::invalid_argument(format!(
            "ticket missing single-valued filter column {col:?}"
        ))
    })?;
    match values.as_slice() {
        [v] => v.as_i64().ok_or_else(|| {
            Status::invalid_argument(format!(
                "filter value for {col:?} must be an integer, got {v}"
            ))
        }),
        _ => Err(Status::invalid_argument(format!(
            "expected exactly one value for {col:?}, got {}",
            values.len()
        ))),
    }
}

/// A non-empty integer set from a ticket filter column.
fn i64_list_filter(filter: &auth::TicketFilter, col: &str) -> Result<Vec<i64>, Status> {
    let values = filter
        .get(col)
        .ok_or_else(|| Status::invalid_argument(format!("ticket missing filter column {col:?}")))?;
    if values.is_empty() {
        return Err(Status::invalid_argument(format!(
            "filter column {col:?} has empty values list"
        )));
    }
    values
        .iter()
        .map(|v| {
            v.as_i64().ok_or_else(|| {
                Status::invalid_argument(format!(
                    "filter values for {col:?} must be integers, got {v}"
                ))
            })
        })
        .collect()
}

/// The sorted, deduplicated sample set a block's members cover.
fn block_member_preps(members: &[auth::BlockReadMember]) -> Vec<i64> {
    let mut preps: Vec<i64> = members.iter().map(|m| m.prep_sample_idx).collect();
    preps.sort_unstable();
    preps.dedup();
    preps
}

/// The `read_masked` table-macro call for one (mask, samples) scope.
///
/// `read_masked` is a MACRO, not a relation — it takes its scope as arguments;
/// `ducklake.rs` carries why.
///
/// `preps` must be non-empty, and that is the CALLER's guarantee, not this
/// function's: `build_read_masked_query` gets it from `i64_list_filter` (which
/// rejects an empty list) and `build_block_read_query` refuses empty `members`
/// first. Those two are the enforcement. An empty list here would emit
/// `read_masked(m, [])`, which the macro reads as "match nothing" — safe, but a
/// silent zero-row answer rather than a loud error. The `debug_assert` below is a
/// development-time tripwire for a future caller that forgets to guard; it is
/// compiled out of the release binary the deploy builds, so it protects the next
/// edit rather than production.
fn read_masked_relation(mask_idx: i64, preps: &[i64]) -> String {
    debug_assert!(
        !preps.is_empty(),
        "read_masked scope must name at least one sample; callers guard this"
    );
    let csv = preps
        .iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join(",");
    format!("qiita_lake.read_masked({mask_idx}, [{csv}])")
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
    // `prep_sample_idx`/`mask_idx` are signature-verified i64s, safe to inline (same
    // rationale as build_query: parsed integers reach SQL, no string data); the
    // 'pass' filter mirrors the read_masked macro's privacy filter.
    let sql = format!(
        "SELECT count(*) FROM qiita_lake.read_mask \
         WHERE mask_idx = {mask_idx} AND prep_sample_idx = {prep_sample_idx} \
         AND reason = 'pass'"
    );
    conn.query_row(&sql, [], |row| row.get(0))
        .map_err(|e| Status::internal(format!("count_masked query failed: {e}")))
}

/// Reason lists for the read-mask count buckets — the Rust twin of qiita-common's
/// `READ_MASK_BUCKET` (`read_mask_reason_sql_list`). A WHITELIST: a ReadMaskReason
/// absent from both lists counts toward `raw` only, which is the correct default
/// for a QC failure or `twist_no_adaptor`, and is why the fail-open
/// `NOT LIKE 'qc_%'` predicate was retired. Adding a reason means editing here AND
/// the Python map; `test_rust_reason_lists_match_the_python_bucket_map` asserts the two
/// paths agree (NOT the block e2e test — see the note on the counts fn below).
const BIOLOGICAL_REASONS: &str = "'host_minimap2', 'host_rype', 'pass'";
const SPIKEIN_REASONS: &str = "'spikein_syndna'";

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
/// branching).
///
/// Bucketing mirrors it too, and is a WHITELIST rather than the fail-open
/// `reason NOT LIKE 'qc_%'` it replaced: raw = every row; biological = `pass` +
/// the `host_*` hits (a human read is still a biological read); spikein =
/// `spikein_syndna`, disjoint from biological (a spike-in is added in the lab);
/// quality_filtered = the `pass` subset. `qc_*` and `twist_no_adaptor` count
/// toward raw only.
///
/// The reason lists below are the Rust twin of `READ_MASK_BUCKET` in qiita-common
/// (`read_mask_reason_sql_list`). Rust cannot import it, so the two are pinned by
/// `qiita-control-plane/tests/test_read_mask_counts.py::test_rust_reason_lists_match_the_python_bucket_map`,
/// which parses these consts out of this file and compares them character for
/// character. (NOT the block e2e test: its fixture emits no `spikein_syndna` or
/// `host_minimap2` rows, so a typo here would miscount silently there.) Adding a
/// ReadMaskReason means touching BOTH sides.
///
/// Opens and drops its own connection so the caller can run it on the blocking
/// pool (mirrors `count_masked_reads`).
fn mask_metrics_counts(
    catalog_connstr: &str,
    data_path: &str,
    mask_idx: i64,
    prep_sample_idx: i64,
) -> Result<serde_json::Value, Status> {
    let conn = open_ducklake(catalog_connstr, data_path)?;
    // `mask_idx`/`prep_sample_idx` are signature-verified i64s, safe to inline (same
    // rationale as count_masked_reads: parsed integers reach SQL, no string data).
    let sql = format!(
        "SELECT \
           count(*) + count(right_trim2), \
           count(*) FILTER (WHERE reason IN ({BIOLOGICAL_REASONS})) \
             + count(right_trim2) FILTER (WHERE reason IN ({BIOLOGICAL_REASONS})), \
           count(*) FILTER (WHERE reason = 'pass') \
             + count(right_trim2) FILTER (WHERE reason = 'pass'), \
           count(*) FILTER (WHERE reason IN ({SPIKEIN_REASONS})) \
             + count(right_trim2) FILTER (WHERE reason IN ({SPIKEIN_REASONS})), \
           count(*) \
         FROM qiita_lake.read_mask \
         WHERE mask_idx = {mask_idx} AND prep_sample_idx = {prep_sample_idx}"
    );
    let (raw, biological, quality_filtered, spikein, row_count): (i64, i64, i64, i64, i64) = conn
        .query_row(&sql, [], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
            ))
        })
        .map_err(|e| Status::internal(format!("mask_metrics query failed: {e}")))?;
    Ok(serde_json::json!({
        "raw": raw,
        "biological": biological,
        "quality_filtered": quality_filtered,
        "spikein": spikein,
        "row_count": row_count,
    }))
}

/// Lake tables a registration REPLACES by key rather than appends to, and the
/// key each replaces on.
///
/// `feature_idx` is minted from the canonical sequence hash
/// (`qiita_common.chunking.canonical_sequence_hash_expr`), so identical bytes
/// carry one feature across every producer: two references that share a
/// sequence, or two assemblies that produce the same contig, each emit that
/// feature's rows in full. The producer cannot anti-join them away — the compute
/// job writing the staging Parquet has no DuckLake access — and DuckLake enforces
/// no PK/UNIQUE, so an append leaves N copies and
/// `string_agg(chunk_data, '' ORDER BY chunk_index)` returns the sequence
/// concatenated with itself while `sequence_length_bp` still describes one copy.
/// Replacing on the key is what makes a second load converge instead of
/// accumulate.
///
/// Two conditions admit a table:
///
/// 1. The incoming files carry the COMPLETE row set for every key they mention.
///    True of the `_feature_load` writers, which bin-pack whole features into
///    parts, so no part holds a fragment of a feature.
/// 2. Every row set carrying one key is an acceptable substitute for any other.
///    `sequence_hash` and `sequence_length_bp` are functions of the feature, so
///    those are identical. `chunk_data` is NOT: the canonical hash is
///    `LEAST(md5(seq), md5(revcomp(seq)))`, so a sequence and its reverse
///    complement share one `feature_idx` while differing byte for byte. Replacing
///    therefore lets the newest load's strand win. Case does not vary here — the
///    write side normalizes it (`qiita_common.chunking.normalized_sequence_expr`).
///
/// Without the replace both byte strings persist and a reader gets them
/// concatenated — neither strand, and a length that matches nothing. With it a
/// reader gets one coherent sequence that `sequence_length_bp` describes.
/// Nothing records which chunk arrived in which load, so keeping the older
/// strand instead is not expressible.
///
/// The incoming file is taken whole: it is a single load, self-consistent by
/// construction, so no chunk of one strand lands beside a chunk of another. A
/// rule that picked per `chunk_index` from rows already in the lake could,
/// since nothing there records which load a chunk came from.
///
/// Writers of these tables also SERIALIZE against each other, on
/// `registration_lock`. Why the replace alone does not suffice is at the site
/// that takes it — `register_files`' transaction.
///
/// `assembly_membership` / `bin_quality` are keyed on `(prep_sample_idx,
/// processing_idx)` instead. A second `long-read-assembly` run over a sample
/// resolves to the same `processing_idx` whenever the inputs
/// `runner/_processing.py` hashes are unchanged — an edited workflow file
/// included — and `routes/work_ticket.py` admits the submission. Appending
/// leaves both runs' rows under one identity with nothing on the row to tell
/// them apart.
///
/// Condition 1 holds per run: `assembly_load` derives both files from the job's
/// own workspace (`bin_map` ⋈ `id_map`, and the CheckM/DAS_Tool tables) and
/// never reads the lake back, so each carries the run's whole row set for its
/// one key. Condition 2 is the run identity itself — same hashed inputs, so the
/// later rows stand in for the earlier.
const REPLACE_KEY_TABLES: &[ReplaceKey] = &[
    ReplaceKey::own("reference_sequences", &["feature_idx"]),
    ReplaceKey::own("reference_sequence_chunks", &["feature_idx"]),
    ReplaceKey::own("assembled_sequence", &["feature_idx"]),
    ReplaceKey::own("assembled_sequence_chunks", &["feature_idx"]),
    ReplaceKey::own(
        "assembly_membership",
        &["prep_sample_idx", "processing_idx"],
    ),
    ReplaceKey {
        table: "bin_quality",
        key: &["prep_sample_idx", "processing_idx"],
        key_source: "assembly_membership",
    },
];

/// One `REPLACE_KEY_TABLES` entry.
struct ReplaceKey {
    /// Lake table whose rows a registration supersedes.
    table: &'static str,
    /// Columns compared together as one key.
    key: &'static [&'static str],
    /// Table in the same registration whose incoming files name the key set to
    /// delete on, unioned with this table's own files.
    ///
    /// `bin_quality` borrows `assembly_membership`'s, because CheckM covers
    /// refined bins only: a run with no MAG writes `bin_quality` with zero rows,
    /// which names no key and so deletes nothing, leaving the previous run's
    /// rows joined to a membership set that was replaced out from under them.
    /// `assembly_membership` carries the run's key on every row and is never
    /// empty where the load runs at all (`assembly_hash` raises `StepNoData` at
    /// zero contigs of any kind). Every other entry is its own source.
    key_source: &'static str,
}

impl ReplaceKey {
    /// An entry whose delete keys come from its own incoming files.
    const fn own(table: &'static str, key: &'static [&'static str]) -> Self {
        Self {
            table,
            key,
            key_source: table,
        }
    }
}

/// The replace-by-key DELETE for one `REPLACE_KEY_TABLES` entry: drop every lake
/// row whose key appears in ANY of the `n_files` Parquets the caller passes.
/// Takes one bound path parameter per file, in order. The files are the ones
/// headed for the entry's table plus, where they differ, the ones headed for its
/// `key_source`.
///
/// A multi-column entry matches on the whole key — the row constructor compares
/// the columns together, so a lake row agreeing on one component and differing
/// on another survives.
///
/// One statement for the whole table, not one per file. A multi-file table
/// arrives as several parts, and deleting part-by-part would let a later part's
/// delete drop rows an earlier part had just added whenever the two share a key;
/// it would also re-scan the lake table once per part.
///
/// The `IN` operand is a subquery, not a literal list: DuckDB plans it as a SEMI
/// hash join and pushes the incoming keys' min/max into the lake scan as a
/// dynamic filter. What the delete reads therefore follows the SPREAD of the
/// incoming key set against the per-file key ranges the catalog holds — not the
/// key's arity, and not the table's size.
///
/// Measured on DuckDB 1.5.4 / ducklake d318a545. Against a catalog holding 1.0M
/// rows over 57 files per table, 16k incoming keys over 4 files:
///
/// * composite `(prep_sample_idx, processing_idx)`, one pair per load: scans
///   17,544 rows of 1,000,008 and opens 1 of the 57 files.
/// * `feature_idx` spread over the identity space: scans 1,003,121 of 1,003,200
///   and opens 57 of 57 — the derived range covers every file, so the scan reads
///   the table.
/// * `feature_idx` confined to one narrow window: scans 17,602 of 1,003,200 and
///   still opens 57 of 57 — all 57 files hold a `feature_idx` below the window's
///   maximum, so the range prunes row groups and no file.
///
/// Against a second catalog, single-column `feature_idx`, 2k incoming keys in
/// one file, table size varied: one contiguous incoming block scans 2,000 rows
/// and opens 1 file at both 40k rows over 20 files and 400k over 200; the same
/// count of keys spread over the identity space scans 39,982 of 40,000 and
/// 399,819 of 400,000, opening every file at both sizes.
///
/// A `WITH … DELETE … USING` rewrite plans the same apart from INNER vs SEMI —
/// same dynamic filters, same scan cardinality, same files read — on each of the
/// four key sets measured on the first catalog (the three above plus one
/// matching no lake row). Over 25 alternating pairs per key set the mean paired
/// difference (this statement minus the rewrite) ran from -0.8 ms to +1.0 ms on
/// statements of 3-37 ms, the widest 95% CI being [-3.2, +2.9] ms.
///
/// `table` / `keys` are interpolated because they are `REPLACE_KEY_TABLES`
/// literals (the caller looks them up there, never using the payload's own
/// string); the file paths are bound parameters, so a basename carrying a quote
/// cannot reach the SQL text.
fn replace_key_delete_sql(table: &str, keys: &[&str], n_files: usize) -> String {
    let key_list = keys.join(", ");
    let incoming_keys = (0..n_files)
        .map(|_| format!("SELECT {key_list} FROM read_parquet(?)"))
        .collect::<Vec<_>>()
        .join(" UNION ALL ");
    // DISTINCT because a chunk table repeats its key once per 64 KB chunk and a
    // run-scoped table repeats its pair on every row, and that whole multiset
    // would otherwise become the semi-join's build side.
    format!(
        "DELETE FROM qiita_lake.{table} WHERE ({key_list}) IN \
         (SELECT DISTINCT {key_list} FROM ({incoming_keys}))"
    )
}

/// How long a lake writer keeps re-running its transaction after a failed COMMIT
/// before giving up.
///
/// A budget rather than an attempt count, because the retries a writer needs is
/// the number of writers ahead of it, which nothing here knows. It is a livelock
/// backstop. Measured against a DuckLake catalog holding 40k rows over 20 files,
/// with a contiguous incoming key set: a whole registration transaction (lock
/// UPDATE + replace-by-key DELETE + one `ducklake_add_data_files`) takes ~14 ms
/// against a lake that already holds the incoming keys, ~7 ms when it does not.
/// Since every writer queues behind one row, that per-transaction cost IS the
/// queue rate, and this budget covers a queue far longer than a deploy produces.
/// The DELETE's share of it is set by the spread of the incoming key set rather
/// than by the table's size — `replace_key_delete_sql` carries what each key set
/// scans, including the sets where it reads the whole table. Exceeding the
/// budget means something other than contention is wrong, and the error says so.
///
/// A registration cannot be retried from the top — its staging files were already
/// moved — so an exhausted budget loses the load.
const LAKE_COMMIT_BUDGET: std::time::Duration = std::time::Duration::from_secs(120);

/// Ceiling on the backoff between retries. Without a cap the doubling below would
/// soon sleep away the whole budget in one wait.
const LAKE_COMMIT_BACKOFF_CAP: std::time::Duration = std::time::Duration::from_millis(500);

/// Starting backoff, doubled per attempt up to `LAKE_COMMIT_BACKOFF_CAP`.
const LAKE_COMMIT_BACKOFF_BASE: std::time::Duration = std::time::Duration::from_millis(10);

/// Backoff before re-running a conflicted lake transaction.
///
/// Doubles per attempt to a cap, offset by a caller-supplied `salt` so writers
/// that conflicted together are less likely to wake together. The salt is an
/// identifier already in hand (the work ticket, the reference) rather than an
/// RNG, which keeps the data plane's write path deterministic; two callers whose
/// salts happen to be congruent modulo the current spread still collide, which
/// costs an attempt and not correctness.
fn lake_commit_backoff(attempt: u32, salt: i64) -> std::time::Duration {
    let base = LAKE_COMMIT_BACKOFF_BASE
        .saturating_mul(1u32 << attempt.min(6))
        .min(LAKE_COMMIT_BACKOFF_CAP);
    let spread = base.as_millis() as u64;
    let offset = if spread == 0 {
        0
    } else {
        salt.unsigned_abs() % spread
    };
    base + std::time::Duration::from_millis(offset)
}

/// Take the lock that serializes writers of the replace-keyed tables. Call
/// inside an open transaction; see `register_files` for why.
fn take_registration_lock(conn: &duckdb::Connection) -> Result<(), Status> {
    let locked = conn
        .execute(
            "UPDATE qiita_lake.registration_lock SET epoch = epoch + 1",
            [],
        )
        .map_err(|e| Status::internal(format!("failed to take the registration lock: {e}")))?;
    // An UPDATE matching no row succeeds and locks nothing, so the serialization
    // would be silently absent and the symptom would be the duplication it exists
    // to prevent. `ensure_registration_lock` seeds the row at boot.
    if locked == 0 {
        return Err(Status::internal(
            "qiita_lake.registration_lock holds no row, so concurrent lake writers \
             would not serialize",
        ));
    }
    Ok(())
}

/// Run `body` inside a DuckLake transaction, re-running the whole thing when the
/// COMMIT fails, until `LAKE_COMMIT_BUDGET` is spent.
///
/// Conflicts surface at COMMIT, not at the statement: measured with 16 concurrent
/// writers contending on `registration_lock`, with the retry disabled, every
/// failure was the COMMIT and none was the lock UPDATE or the replace-by-key
/// DELETE. So an error out of `body` is not contention, will not resolve on a
/// retry, and is surfaced immediately.
///
/// `body` must therefore be idempotent across attempts — each one re-reads a
/// fresh snapshot after DuckLake rolled the last one back. `salt` only spreads
/// the backoff (see `lake_commit_backoff`).
fn transact_with_retry<T>(
    conn: &duckdb::Connection,
    what: &str,
    salt: i64,
    mut body: impl FnMut() -> Result<T, Status>,
) -> Result<T, Status> {
    let deadline = std::time::Instant::now() + LAKE_COMMIT_BUDGET;
    let mut attempt: u32 = 0;
    loop {
        conn.execute_batch("BEGIN TRANSACTION")
            .map_err(|e| Status::internal(format!("failed to begin transaction: {e}")))?;

        let value = match body() {
            Ok(value) => value,
            Err(e) => {
                let _ = conn.execute_batch("ROLLBACK");
                return Err(e);
            }
        };

        let commit_error = match conn.execute_batch("COMMIT") {
            Ok(()) => return Ok(value),
            Err(e) => e,
        };

        if std::time::Instant::now() >= deadline {
            return Err(Status::internal(format!(
                "failed to commit {what} within {}s ({} attempts): {commit_error}",
                LAKE_COMMIT_BUDGET.as_secs(),
                attempt.saturating_add(1),
            )));
        }
        std::thread::sleep(lake_commit_backoff(attempt, salt));
        attempt = attempt.saturating_add(1);
    }
}

/// What one `register_files` call did.
#[derive(Debug)]
struct Registration {
    /// Permanent lake paths registered, in payload iteration order.
    registered: Vec<String>,
    /// Rows the replace-by-key pass removed, per table — non-zero entries only.
    /// Empty when nothing this call registered was a `REPLACE_KEY_TABLES`
    /// target, or when every key it carried was new to the lake.
    ///
    /// Rides back to the control plane in the DoAction body, which logs it: a
    /// delete nothing recorded is the one thing an operator reconciling row
    /// counts cannot reconstruct.
    replaced: BTreeMap<&'static str, usize>,
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
/// Some tables are REPLACED on their key rather than appended to — see
/// `REPLACE_KEY_TABLES` for which, and why.
///
/// Note: the action token is scoped to staging_dir + files, not to a specific
/// reference_idx. The control plane is responsible for issuing tokens only for
/// valid references in the correct state.
fn register_files(
    catalog_connstr: &str,
    data_path: &str,
    scratch_root: &std::path::Path,
    payload: &auth::ActionPayload,
) -> Result<Registration, Status> {
    let staging = std::path::Path::new(&payload.staging_dir);
    let perm_root = std::path::Path::new(data_path);

    // Validate every filename is a safe relative path under
    // staging_dir. Filenames may carry a subdir prefix
    // (e.g. "reference_sequence_chunks/part_00000.parquet") to register
    // multiple parts under one DuckLake table, but must not contain
    // `..` or absolute components. Although `payload.files` is
    // Ed25519-signed by the control plane and so already trusted, this
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

    // One scope key for the whole registration; it does not vary per file.
    let scope = staging_scope(&payload.staging_dir, scratch_root);

    // Move all files to permanent storage.
    // (DuckLake table, permanent path). The path is carried as a String because
    // every consumer below binds it into SQL or reports it.
    let mut moved: Vec<(String, String)> = Vec::new();
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
        let dest = dest_dir.join(lake_dest_filename(
            payload.work_ticket_idx,
            &scope,
            basename,
        ));
        move_file(&src, &dest)?;
        // Both SQL passes below name the destination as a string, so resolve it
        // once here rather than re-deriving (and re-erroring on) it twice.
        let dest_str = dest
            .to_str()
            .ok_or_else(|| Status::internal(format!("non-UTF-8 path: {}", dest.display())))?
            .to_string();
        moved.push((table.clone(), dest_str));
    }

    // Register in DuckLake. Tables are ensured at startup in main.rs.
    let conn = duckdb::Connection::open_in_memory()
        .map_err(|e| Status::internal(format!("failed to open DuckDB: {e}")))?;
    ducklake::connect_ducklake(&conn, catalog_connstr, data_path)
        .map_err(|e| Status::internal(format!("failed to attach DuckLake: {e}")))?;

    // Replace-by-key the `REPLACE_KEY_TABLES` targets, then register every moved
    // file, in ONE DuckLake transaction so the catalog update is all-or-nothing
    // (mirrors delete_reference / delete_mask / delete_pool_reads). A failure
    // part-way through rolls back every prior delete and
    // ducklake_add_data_files call rather than leaving the reference
    // half-registered in the catalog.
    //
    // Atomicity here is CATALOG-LEVEL ONLY: the filesystem moves above have
    // already happened and are NOT rolled back. That is intentional and safe.
    // Each dest name is registration-unique (lake_dest_filename keys on the work
    // ticket and the staging dir) and move_file refuses to overwrite, so a rolled-back registration
    // leaves at most an unreferenced orphan Parquet on disk — never a collision
    // and never a double-registration. This matches how DuckLake already
    // tolerates orphan Parquets (the delete_* actions reclaim nothing from disk
    // either); a future maintenance pass sweeps them.
    //
    // Registrations that touch a replace-keyed table SERIALIZE against each
    // other, and retry when they lose. Both halves are needed:
    //
    // DuckLake detects a conflict only where two transactions touch the same
    // EXISTING row, so the replace-by-key DELETE serializes writers of a feature
    // the lake already holds — but NOT writers of a feature that is new, whose
    // deletes match nothing. Measured with 4 concurrent writers of one feature:
    // 1 row when it already existed, 4 when it did not, and 4 again for two bare
    // `ducklake_add_data_files` with no delete at all. Bumping
    // `registration_lock` gives every such registration a row to contend for, so
    // the new-feature case conflicts too (measured: back to 1 row).
    //
    // The loser's work is fully re-runnable — its files are already at the paths
    // `lake_dest_filename` minted and the delete+add is idempotent against whatever
    // snapshot it re-reads — so it retries here rather than failing the ticket. A
    // retry from the top would not work: the staging files were moved above, so
    // the caller's next attempt gets `not_found`.
    //
    // Registrations touching none of those tables (read_mask, alignment) skip the
    // lock and so never contend.
    // Which tables this registration replaces by key, and the files whose keys
    // each delete reads — its own, plus its `key_source`'s where the two differ
    // and that table is in this registration too. Loop-invariant, so it is built
    // once outside the retry.
    let mut files_for: BTreeMap<&'static str, Vec<&str>> = BTreeMap::new();
    for (table, dest) in &moved {
        if let Some(entry) = REPLACE_KEY_TABLES
            .iter()
            .find(|candidate| candidate.table == table.as_str())
        {
            files_for
                .entry(entry.table)
                .or_default()
                .push(dest.as_str());
        }
    }
    let incoming: Vec<(&'static ReplaceKey, Vec<&str>)> = REPLACE_KEY_TABLES
        .iter()
        .filter_map(|entry| {
            let mut dests = files_for.get(entry.table)?.clone();
            if entry.key_source != entry.table {
                if let Some(source_dests) = files_for.get(entry.key_source) {
                    dests.extend(source_dests.iter().copied());
                }
            }
            Some((entry, dests))
        })
        .collect();
    let takes_lock = !incoming.is_empty();

    let registration = transact_with_retry(
        &conn,
        "the registration transaction",
        payload.work_ticket_idx,
        || {
            if takes_lock {
                take_registration_lock(&conn)?;
            }

            // Replace-by-key runs as its OWN pass, ahead of every add: one statement
            // per target table over all the files headed for it, so no delete can
            // touch a row this same registration already added.
            let mut replaced: BTreeMap<&'static str, usize> = BTreeMap::new();
            for (entry, dests) in &incoming {
                let sql = replace_key_delete_sql(entry.table, entry.key, dests.len());
                let params: Vec<&dyn duckdb::ToSql> =
                    dests.iter().map(|d| d as &dyn duckdb::ToSql).collect();
                let deleted = conn.execute(&sql, params.as_slice()).map_err(|e| {
                    let table = entry.table;
                    Status::internal(format!("replace-by-key delete failed for {table}: {e}"))
                })?;
                // Zero is the ordinary case (a first load, or a run whose features
                // are all new) and says nothing; only a real supersede earns an entry.
                if deleted > 0 {
                    replaced.insert(entry.table, deleted);
                }
            }

            let mut registered = Vec::new();
            for (table, dest) in &moved {
                conn.execute(
                    "CALL ducklake_add_data_files('qiita_lake', ?, ?)",
                    duckdb::params![table, dest],
                )
                .map_err(|e| {
                    Status::internal(format!(
                        "ducklake_add_data_files failed for {table}/{dest}: {e}"
                    ))
                })?;
                registered.push(dest.clone());
            }
            Ok(Registration {
                registered,
                replaced,
            })
        },
    )?;

    Ok(registration)
}

/// Delete every DuckLake row belonging to a reference.
///
/// Scoping rules mirror the identifier hierarchy:
/// - `reference_taxonomy`, `reference_phylogeny`, `reference_placements`,
///   `reference_annotation`, and `reference_membership` carry `reference_idx`
///   directly → deleted by a plain `WHERE reference_idx = ?`.
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

    // All seven deletes are one DuckLake transaction so the action is
    // all-or-nothing: a mid-delete failure rolls every table back rather than
    // leaving a half-purged reference. That atomicity is what lets the control
    // plane safely retry — a failed call leaves DuckLake membership fully
    // intact, so the orphan recomputation on the next attempt is unchanged.
    //
    // It runs under the same lock and retry as `register_files`, because it
    // writes two of the same content-addressed tables. Without the lock a
    // registration could add a feature between this transaction's snapshot and
    // its commit, and the orphan filter — which reads `reference_membership` —
    // would not see the claim; with it, the two serialize. Without the retry a
    // registration's DELETE would newly conflict this one out, which the bare
    // COMMIT here predates.
    //
    // Orphan features: this reference's features minus every other reference's.
    //
    // A reference claims a feature in TWO ways and both count: as a MEMBER (a whole
    // sequence, indexed and aligned against) or as an annotated INTERVAL (a SynDNA
    // insert on its plasmid — minted its own feature_idx, deliberately kept out of
    // membership). Omitting reference_annotation on the left would leak annotated
    // features; omitting it on the right would delete a sequence another reference
    // still annotates.
    //
    // These two claim sets MUST match the Postgres-side orphan computation in
    // qiita_control_plane.actions.reference.delete_reference_cascade — the two
    // stores GC the same features independently, so a change to one query must
    // change the other or sequences/features desync across stores. That query
    // carries one further term this filter omits (`qiita.assembly_membership`);
    // its comment holds the rationale for the asymmetry.
    let orphan_filter = "feature_idx IN (
            (SELECT feature_idx FROM qiita_lake.reference_membership WHERE reference_idx = ?
             UNION
             SELECT feature_idx FROM qiita_lake.reference_annotation WHERE reference_idx = ?)
            EXCEPT
            (SELECT feature_idx FROM qiita_lake.reference_membership WHERE reference_idx <> ?
             UNION
             SELECT feature_idx FROM qiita_lake.reference_annotation WHERE reference_idx <> ?)
        )";

    // Sequence/chunk deletes run BEFORE the membership AND annotation deletes: the
    // orphan subquery reads both of this reference's claim tables, so both must
    // still be present.
    let counts = transact_with_retry(&conn, "the reference delete", reference_idx, || {
        take_registration_lock(&conn)?;
        let sequences_deleted = exec(
            &format!("DELETE FROM qiita_lake.reference_sequences WHERE {orphan_filter}"),
            &[reference_idx, reference_idx, reference_idx, reference_idx],
        )?;
        let chunks_deleted = exec(
            &format!("DELETE FROM qiita_lake.reference_sequence_chunks WHERE {orphan_filter}"),
            &[reference_idx, reference_idx, reference_idx, reference_idx],
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
        // Annotation ROWS delete by reference_idx (they carry it directly), not by
        // the orphan filter — but they must go AFTER the sequence/chunk deletes
        // above, which read this table as one of the two claim sets in
        // `orphan_filter`. The annotated FEATURES themselves are GC'd through that
        // filter, like any other orphan.
        let annotations_deleted = exec(
            "DELETE FROM qiita_lake.reference_annotation WHERE reference_idx = ?",
            &[reference_idx],
        )?;
        Ok(serde_json::json!({
            "sequences_deleted": sequences_deleted,
            "chunks_deleted": chunks_deleted,
            "membership_deleted": membership_deleted,
            "taxonomy_deleted": taxonomy_deleted,
            "phylogeny_deleted": phylogeny_deleted,
            "placements_deleted": placements_deleted,
            "annotations_deleted": annotations_deleted,
        }))
    })?;

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

/// Replace the DuckLake `reference_exclusion` mirror wholesale from the control
/// plane's resolved-blocklist Parquet.
///
/// The control plane owns the authoritative blocklist (`qiita.reference_exclusion`),
/// resolves it to the excluded `feature_idx` set (direct feature blocks plus every
/// feature of a blocked genome), and writes that set to `dest` — a single-column
/// (`feature_idx`) Parquet on the shared scratch tree. This handler clears the
/// mirror and reloads it from that file in ONE transaction, so the two-statement
/// swap is all-or-nothing: a failure leaves the previous mirror intact and the
/// control plane can safely retry.
///
/// Full-replace makes it idempotent / replay-safe: re-running with the same
/// Parquet converges to the same table, and a replay of a stale-but-valid token
/// reloads whatever the (authoritative) file holds. An empty blocklist ships a
/// valid zero-row Parquet, so the DELETE + zero-row INSERT simply clears the
/// mirror (re-enabling everything). Opens and drops its own connection so the
/// caller can run it on the blocking pool (mirrors `delete_mask` /
/// `register_files`). Returns the loaded row count.
///
/// `dest` arrives lexically validated (`validate_export_dest`: absolute, under
/// the scratch root, no `..`, no single quote), but that check is a string test
/// done before any I/O — insufficient on the shared scratch tree, where another
/// job could plant a symlink that redirects this read outside the controlled
/// workspace. That matters MORE here than for the read-export sibling: this
/// feeds the GLOBAL `reference_exclusion` mirror both `_visible` views enforce
/// against, so a redirected read of an attacker-planted Parquet's `feature_idx`
/// column would mass-exclude arbitrary features system-wide. So — the read-path
/// analogue of `export_select_to_parquet`'s parent-canonicalize on write — we
/// canonicalize `dest` ITSELF (the file already exists; the CP wrote it before
/// signing) and re-assert it resolves under the canonicalized scratch root
/// before reading, then inline the CANONICALIZED path (re-checked for a single
/// quote a resolved symlink target could introduce) into `read_parquet`.
fn sync_reference_exclusion(
    catalog_connstr: &str,
    data_path: &str,
    dest: &Path,
    scratch_root: &Path,
) -> Result<serde_json::Value, Status> {
    // Symlink-safe containment (see the doc comment): resolve the real target
    // and re-assert it stays under the real scratch root.
    let real_dest = std::fs::canonicalize(dest)
        .map_err(|e| Status::internal(format!("failed to resolve {}: {e}", dest.display())))?;
    let real_root = std::fs::canonicalize(scratch_root).map_err(|e| {
        Status::internal(format!(
            "failed to resolve scratch root {}: {e}",
            scratch_root.display()
        ))
    })?;
    if !real_dest.starts_with(&real_root) {
        return Err(Status::permission_denied(format!(
            "sync dest {} resolves outside the scratch root {}",
            real_dest.display(),
            real_root.display()
        )));
    }
    let dest_str = real_dest
        .to_str()
        .ok_or_else(|| Status::internal(format!("non-UTF-8 path: {}", real_dest.display())))?;
    // The lexical check rejected a single quote in the requested path, but a
    // resolved symlink could land on a real directory whose name contains one;
    // re-check the canonicalized string we are about to inline.
    if dest_str.contains('\'') {
        return Err(Status::invalid_argument(format!(
            "resolved sync dest must not contain a single quote: {dest_str:?}"
        )));
    }

    let conn = duckdb::Connection::open_in_memory()
        .map_err(|e| Status::internal(format!("failed to open DuckDB: {e}")))?;
    ducklake::connect_ducklake(&conn, catalog_connstr, data_path)
        .map_err(|e| Status::internal(format!("failed to attach DuckLake: {e}")))?;

    // Clear-then-reload in one transaction so the swap is atomic and retryable:
    // a failure leaves the previous mirror intact.
    conn.execute_batch("BEGIN TRANSACTION")
        .map_err(|e| Status::internal(format!("failed to begin transaction: {e}")))?;

    let loaded = (|| -> Result<i64, Status> {
        conn.execute(
            "DELETE FROM qiita_lake.reference_exclusion",
            duckdb::params![],
        )
        .map_err(|e| Status::internal(format!("failed to clear reference_exclusion: {e}")))?;
        // `dest_str` is the canonicalized path, verified above to resolve under
        // the scratch root and to carry no single quote, so inlining it into the
        // read_parquet literal is safe. Project `feature_idx` by name so the load
        // is insensitive to any extra column the producer might add later.
        let inserted = conn
            .execute(
                &format!(
                    "INSERT INTO qiita_lake.reference_exclusion (feature_idx) \
                     SELECT feature_idx FROM read_parquet('{dest_str}')"
                ),
                duckdb::params![],
            )
            .map_err(|e| {
                Status::internal(format!(
                    "failed to load reference_exclusion from {dest_str}: {e}"
                ))
            })?;
        Ok(inserted as i64)
    })();

    let loaded = match loaded {
        Ok(n) => n,
        Err(e) => {
            // Best-effort rollback; surface the original error.
            let _ = conn.execute_batch("ROLLBACK");
            return Err(e);
        }
    };
    conn.execute_batch("COMMIT")
        .map_err(|e| Status::internal(format!("failed to commit sync transaction: {e}")))?;

    Ok(serde_json::json!({
        "feature_count": loaded,
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
/// Ed25519-signed payload, so inlining them into the `IN (...)` list carries no
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
/// the `read_block` selector emits (`block_read_where_clause`), scoped further by
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
/// signature-verified i64s, safe to inline.
fn delete_read_mask_block(
    catalog_connstr: &str,
    data_path: &str,
    mask_idx: i64,
    members: &[auth::BlockReadMember],
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

/// Every `qiita_lake` table an alignment delete clears, in delete order.
/// `alignment` leads: every `delete_alignment*` reports its count as
/// `rows_deleted`.
///
/// `alignment_delete_covers_every_alignment_scoped_lake_table` pins the set
/// against the catalog, so a third table keyed by `alignment_idx` cannot be added
/// without joining this list.
const ALIGNMENT_DELETE_TABLES: &[&str] = &["alignment", "alignment_origin_spanning"];

/// Every delete reads the leading count as `counts[0]`, which panics on an empty
/// list. Emptying the list is a build failure instead.
const _: () = assert!(!ALIGNMENT_DELETE_TABLES.is_empty());

/// Delete `where_clause`'s rows from each of `tables`, in order, returning one
/// count per table positionally. The shared body of the `delete_alignment*`
/// handlers; each builds its own clause and shapes its own response.
///
/// The tables go in lockstep because they describe each other:
/// `alignment_origin_spanning` names the `alignment` rows that make up one
/// origin-spanning read, so dropping the fragments while keeping the evidence
/// leaves a read described by rows that are gone.
///
/// One DuckLake transaction, so the action is all-or-nothing and the control
/// plane can safely retry: a failed call leaves every table's rows fully intact,
/// so a retry sees the same row set. Logical `DELETE` only. No raw parquet
/// `unlink` — DuckLake owns file lifecycle and a manual unlink would corrupt the
/// catalog; orphan parquets are tolerated until a future maintenance pass
/// (matches `delete_mask`).
///
/// `tables` and `where_clause` are interpolated and `params` binds any `?` the
/// clause carries, so the caller owns the injection argument for both — as
/// `replace_key_delete_sql`'s does at its own site. Every caller passes an
/// `ALIGNMENT_DELETE_TABLES` literal and a clause whose values are either bound
/// `?`s or inlined Ed25519-verified i64s.
fn delete_lake_rows(
    conn: &duckdb::Connection,
    tables: &[&str],
    where_clause: &str,
    params: &[&dyn duckdb::ToSql],
) -> Result<Vec<usize>, Status> {
    conn.execute_batch("BEGIN TRANSACTION")
        .map_err(|e| Status::internal(format!("failed to begin transaction: {e}")))?;

    let deletes = tables
        .iter()
        .map(|table| {
            let sql = format!("DELETE FROM qiita_lake.{table} WHERE {where_clause}");
            conn.execute(&sql, params)
                .map_err(|e| Status::internal(format!("delete failed ({sql}): {e}")))
        })
        .collect::<Result<Vec<usize>, Status>>();

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
    Ok(counts)
}

/// Logically delete one `alignment_idx`'s rows from every
/// `ALIGNMENT_DELETE_TABLES` table in DuckLake — the whole-alignment purge the
/// disallow-without-delete resubmission rule needs (a completed
/// `alignment_sample` must be cleared before re-aligning).
///
/// The alignment twin of `delete_mask`; transaction, lockstep and parquet
/// lifecycle are `delete_lake_rows`'. Idempotent: deleting an `alignment_idx`
/// with zero rows is success and returns `rows_deleted: 0`, so the control plane
/// can safely retry. `alignment_idx` is an Ed25519-verified i64.
///
/// `rows_deleted` is the `alignment` count alone; the side table's count is not
/// reported. `delete_pool_reads` reports one qualified key per table because the
/// control plane consumes both (`PoolReadPurgeResponse`); here there is no
/// consumer, and no producer writing the side table, so a second key would carry
/// a structural zero. A producer PR that adds a consumer adds the key with it.
fn delete_alignment(
    catalog_connstr: &str,
    data_path: &str,
    alignment_idx: i64,
) -> Result<serde_json::Value, Status> {
    let conn = duckdb::Connection::open_in_memory()
        .map_err(|e| Status::internal(format!("failed to open DuckDB: {e}")))?;
    ducklake::connect_ducklake(&conn, catalog_connstr, data_path)
        .map_err(|e| Status::internal(format!("failed to attach DuckLake: {e}")))?;

    let counts = delete_lake_rows(
        &conn,
        ALIGNMENT_DELETE_TABLES,
        "alignment_idx = ?",
        &[&alignment_idx as &dyn duckdb::ToSql],
    )?;

    Ok(serde_json::json!({
        "alignment_idx": alignment_idx,
        "rows_deleted": counts[0],
    }))
}

/// Delete exactly one block's footprint from every `ALIGNMENT_DELETE_TABLES`
/// table: the rows for `alignment_idx` whose `(prep_sample_idx, sequence_idx)`
/// fall in the members' sub-ranges. This is the idempotent-block-replace
/// primitive — the `align` workflow runs it immediately before `register-files`,
/// so a re-run deletes the prior run's rows before writing fresh ones and never
/// double-counts.
///
/// The alignment twin of `delete_read_mask_block`: same exact-by-construction
/// footprint selector (`block_read_where_clause`) scoped further by
/// `alignment_idx = ?`. The per-member OR residual makes it exact — a split member
/// deletes ONLY its own sub-range, so a sibling block's rows for a shared sample
/// survive (independent of tiling order). The selector is on `(prep_sample_idx,
/// sequence_idx)` and is feature_idx-agnostic, so it clears ALL of a read's
/// alignment rows (a read produces multiple rows via cross-shard + PE
/// multiplicity) — exactly what a re-run must replace.
///
/// Mirrors `delete_read_mask_block`; transaction, lockstep and parquet lifecycle
/// are `delete_lake_rows`', count reporting is `delete_alignment`'s. Idempotent:
/// a fresh block (no rows yet) deletes 0. Empty `members` is a control-plane bug
/// (the DoAction arm rejects it before this); guarded here too, returning a
/// zero-count noop. All integers are Ed25519-verified i64s, safe to inline.
fn delete_alignment_block(
    catalog_connstr: &str,
    data_path: &str,
    alignment_idx: i64,
    members: &[auth::BlockReadMember],
) -> Result<serde_json::Value, Status> {
    if members.is_empty() {
        return Ok(serde_json::json!({
            "alignment_idx": alignment_idx,
            "rows_deleted": 0,
        }));
    }

    let conn = duckdb::Connection::open_in_memory()
        .map_err(|e| Status::internal(format!("failed to open DuckDB: {e}")))?;
    ducklake::connect_ducklake(&conn, catalog_connstr, data_path)
        .map_err(|e| Status::internal(format!("failed to attach DuckLake: {e}")))?;

    // Scope the shared footprint selector to this align-config identity. The
    // `read`/`read_mask` blocks key on sequence_idx; every
    // `ALIGNMENT_DELETE_TABLES` table also carries `prep_sample_idx` and
    // `sequence_idx`, so the one clause applies to each. The side table holds one
    // row per read against `alignment`'s one per SAM record, and a member's
    // sub-range selects the same reads either way.
    let where_clause = format!(
        "alignment_idx = {alignment_idx} AND {}",
        block_read_where_clause(members)
    );

    let counts = delete_lake_rows(&conn, ALIGNMENT_DELETE_TABLES, &where_clause, &[])?;

    Ok(serde_json::json!({
        "alignment_idx": alignment_idx,
        "rows_deleted": counts[0],
    }))
}

/// Delete one `(alignment_idx, prep_sample_idx)` pair's rows from every
/// `ALIGNMENT_DELETE_TABLES` table — the idempotent-sample-replace primitive: run
/// immediately before `register-files` and a re-run deletes the prior run's rows
/// before writing fresh ones, so it never double-counts.
///
/// The pair is the unit because none of the three mechanisms already here selects
/// it:
///
/// * `delete_alignment` keys on `alignment_idx` alone, so it takes every other
///   sample's rows with it.
/// * `delete_alignment_block` needs a `block_member` cover-map, which a caller
///   holding one prep_sample has none of.
/// * `REPLACE_KEY_TABLES` is matched on the destination TABLE name alone (see
///   `register_files`), so an `alignment` entry keyed on this pair would fire on
///   the block-scoped `align` workflow's registrations too. `tile_partition`
///   splits a straddling sample across consecutive blocks and
///   `replace_key_delete_sql` deletes every lake row whose key tuple appears in
///   the incoming Parquet, so the second block's registration would delete the
///   first's rows for the shared sample — `REPLACE_KEY_TABLES`' condition 1 (the
///   incoming files carry the complete row set for every key they mention)
///   failing.
///
/// Both key columns are in the DDL of both tables
/// (`ducklake::ensure_alignment_tables`), so the one clause applies to each. The
/// predicate carries no `sequence_idx` bound — the sample is the unit — and is
/// feature_idx-agnostic, so ALL of a read's alignment rows go.
///
/// Transaction, lockstep and parquet lifecycle are `delete_lake_rows`', count
/// reporting is `delete_alignment`'s. Idempotent: a sample with no rows yet
/// deletes 0 and still succeeds.
fn delete_alignment_sample(
    catalog_connstr: &str,
    data_path: &str,
    alignment_idx: i64,
    prep_sample_idx: i64,
) -> Result<serde_json::Value, Status> {
    let conn = duckdb::Connection::open_in_memory()
        .map_err(|e| Status::internal(format!("failed to open DuckDB: {e}")))?;
    ducklake::connect_ducklake(&conn, catalog_connstr, data_path)
        .map_err(|e| Status::internal(format!("failed to attach DuckLake: {e}")))?;

    let counts = delete_lake_rows(
        &conn,
        ALIGNMENT_DELETE_TABLES,
        "alignment_idx = ? AND prep_sample_idx = ?",
        &[
            &alignment_idx as &dyn duckdb::ToSql,
            &prep_sample_idx as &dyn duckdb::ToSql,
        ],
    )?;

    Ok(serde_json::json!({
        "alignment_idx": alignment_idx,
        "rows_deleted": counts[0],
    }))
}

/// Mint a unique, ticket-traceable lake-storage filename for a registered
/// Parquet.
///
/// The producer (the reference-load job) reuses fixed basenames
/// (`part_00000.parquet`, `reference_<table>.parquet`) on every load, so the
/// bare basename is NOT unique within a per-table lake dir: two registrations
/// into the same table would target the same path and the second would clobber
/// the first's live, catalog-registered file. The basename — part index or
/// table name — still distinguishes files within one registration; the two
/// components below separate one registration from another:
///
/// * `wt{work_ticket_idx}` traces the file back to the ticket that wrote it.
/// * A digest of the registration's staging dir separates two loads from ONE
///   ticket. A ticket can load twice: a redrive replays its storage tail, and
///   the ticket alone yields the byte-identical path the first load already
///   registered, which [`move_file`] refuses.
///
///   The scope of that: `staging_dir` is the PRODUCER step's output directory,
///   so it separates the two loads only where the producer itself re-ran — the
///   case a redrive that drops the producer's `qiita.work_ticket_step` row
///   produces. A redrive that leaves the producer fast-forwarded rebuilds its
///   outputs under the original attempt, yielding the same `staging_dir` and the
///   same collision. That still fails at
///   [`move_file`] rather than corrupting anything, but it is not covered here.
///
/// Deterministic for a given (ticket, staging dir), so a replayed DoAction
/// recomputes the same name. Most replays never reach [`move_file`]: the first
/// run moved the staging files out, so the source-existence check in
/// `register_files` returns `not_found` first. The name carries the guard on the
/// one path where the source survives — the EXDEV branch below copies and then
/// tolerates a failed `remove_file(src)`, leaving source and destination both in
/// place. There the refusal is what stops a second registration of the tables
/// outside `REPLACE_KEY_TABLES` (e.g. `reference_membership`), which have no
/// replace-by-key to absorb one; a random name would register them twice.
///
/// DuckLake names its own INSERT-written data files uniquely for the same
/// reason; this is the equivalent for our "register an existing file" path.
///
/// `scope` comes from [`staging_scope`], not from the raw `staging_dir`.
fn lake_dest_filename(work_ticket_idx: i64, scope: &str, basename: &str) -> String {
    let digest = Sha256::digest(scope.as_bytes());
    // 48 bits, enough to separate the attempts one ticket can have
    // (`max_retries` bounds them). A collision lands on move_file's
    // AlreadyExists, not a silent clobber.
    let hex: String = digest[..6].iter().map(|b| format!("{b:02x}")).collect();
    format!("wt{work_ticket_idx}-{hex}-{basename}")
}

/// The part of a registration's staging dir that identifies WHICH registration
/// it is, independent of where scratch happens to be mounted.
///
/// `PATH_SCRATCH` is host configuration: a migration, a remount, or a differently
/// laid-out replacement host changes it without changing which registration a
/// given staging dir denotes. Keying [`lake_dest_filename`] on the absolute path
/// would make the digest — and so the destination name — move with it, which
/// would break the determinism that guard depends on. Keying on the path
/// RELATIVE to the scratch root does not.
///
/// A staging dir outside the scratch root falls back to the full path. That is
/// still correct (it only has to be stable and distinct), just not stable across
/// a move of whatever holds it.
fn staging_scope(staging_dir: &str, scratch_root: &std::path::Path) -> String {
    std::path::Path::new(staging_dir)
        .strip_prefix(scratch_root)
        .map(|rel| rel.to_string_lossy().into_owned())
        .unwrap_or_else(|_| staging_dir.to_string())
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

/// The alignment DoGet surface: the exclusion-aware view `alignment_visible`, and
/// ONLY that — never the raw `alignment` base table (which is out of
/// `ALLOWED_TABLES`, so unreachable via `do_get`). `build_query` gives this name
/// the mandatory projection column list and the mandatory (non-empty, single
/// `alignment_idx`) scoping. Deliberately NOT recognizing the raw name: if
/// `"alignment"` were ever re-added to `ALLOWED_TABLES` by mistake, it would fall
/// through to a bare `SELECT *`, producing an obviously-malformed, unscoped result
/// rather than a clean-looking one that silently bypasses exclusion — the failure
/// stays loud.
fn is_alignment_doget_surface(table: &str) -> bool {
    table == "alignment_visible"
}

/// Tables `build_query` refuses to serve on an empty filter.
///
/// The `reference_*` tables are broadly readable by design (an unfiltered SELECT
/// there mirrors the anonymous REST `GET /reference/{idx}`), so the refusal is
/// per-table rather than global. What is listed is the one table an empty filter
/// would turn into the whole alignment sink. The assembly surfaces are not
/// listed and do not need to be: like `read_masked`, their scope IS their query
/// shape (`build_assembly_run_query`), which has no empty form to refuse.
fn requires_scoped_filter(table: &str) -> bool {
    is_alignment_doget_surface(table)
}

/// The DoGet surfaces whose scope is one assembly RUN.
///
/// Neither carries `prep_sample_idx` — a contig is stored once, keyed by the
/// content-deduped `feature_idx` it shares with every other run that produced
/// the same bytes — so which contigs are "this run's" is a fact held by
/// `assembly_membership`, and `build_assembly_run_query` reads it there.
fn is_assembly_run_surface(table: &str) -> bool {
    matches!(table, "assembled_sequence" | "assembled_sequence_chunks")
}

/// Build a SQL query for the given table and filter.
///
/// SQL injection defense model:
/// - Table name: whitelist (`ALLOWED_TABLES`) — only known-safe values
/// - Column names: whitelist (`ALLOWED_FILTER_COLUMNS`) — only known identifier columns
/// - Values: parsed as i64 then stringified — no string data reaches SQL
/// - All inputs are also signature-verified (set by the control plane, not the client)
///
/// DuckDB does not support parameterized identifiers (table/column names), so
/// whitelisting is the correct defense. Values could be parameterized but are
/// already safe as parsed integers.
fn build_query(
    table: &str,
    filter: &auth::TicketFilter,
    members: &[auth::BlockReadMember],
    columns: &[String],
) -> Result<(String, String), Status> {
    // Resolve the projection FIRST, before any early return below can build SQL
    // on its own path — otherwise the block-read and `read_masked` selectors
    // would silently ignore a column list rather than refuse it, which is the
    // silent-widening failure this mechanism exists to prevent.
    let select_list = select_list_for(table, columns)?;

    // Block-read selectors resolve to a different relation than their ticket name
    // and are scoped by `members`, not by a column filter — handle them first, and
    // reject `members` on any other table so a stray selector can never silently
    // widen a normal ticket into an unscoped read.
    if let Some(source) = block_read_source(table) {
        return build_block_read_query(table, source, filter, members);
    }
    if !members.is_empty() {
        return Err(Status::invalid_argument(format!(
            "table {table:?} does not accept a block `members` selector"
        )));
    }

    let full_table = format!("qiita_lake.{table}");

    // `read_masked` is a table macro whose (mask, samples) scope IS its argument
    // list, so it cannot be assembled by the generic WHERE-clause path below.
    if table == "read_masked" {
        return build_read_masked_query(filter);
    }

    // An assembly surface is scoped by a run, which is a fact in another table
    // rather than a column of this one — also not a generic WHERE clause.
    if is_assembly_run_surface(table) {
        return build_assembly_run_query(table, filter);
    }

    if filter.is_empty() {
        // Defense-in-depth against a full-table read leak. The human-read surface
        // no longer reaches here at all — `read_masked` is a macro that cannot be
        // called without a scope (above), which retires the "requiring
        // prep_sample_idx for read_masked" half of the follow-up this comment used
        // to track. `alignment_visible` is still guarded here: the CP always
        // scopes it to (alignment_idx, prep_sample_idx), and an empty filter would
        // dump the whole sink (and bypass the projection). This rejects only the
        // *empty* case, not every under-scoped one: a non-empty but non-scoping
        // filter (e.g. feature_idx alone) still passes today. Making an unfiltered
        // read opt-in via an allowlist is still a tracked durability follow-up.
        // Which tables are refused, and why the reference_* ones are not, is at
        // `requires_scoped_filter`.
        if requires_scoped_filter(table) {
            return Err(Status::invalid_argument(format!(
                "{table} requires a non-empty filter (refusing full-table read)"
            )));
        }
        return Ok((
            format!("SELECT {select_list} FROM {full_table}"),
            full_table,
        ));
    }

    // A feature-table DoGet builds a table for exactly ONE alignment run, and a
    // consumer typically leaves alignment_idx out of its projection (every row
    // shares it), so require it present and single-valued. Otherwise a ticket
    // could omit the scope, or pass several alignment_idx values and blend rows
    // from heterogeneous runs into one indistinguishable stream. Fail loud.
    if is_alignment_doget_surface(table) {
        match filter.get("alignment_idx") {
            Some(values) if values.len() == 1 => {}
            _ => {
                return Err(Status::invalid_argument(
                    "alignment DoGet requires exactly one alignment_idx value",
                ));
            }
        }
    }

    // The MEMBERSHIP_JOIN_TABLES have no reference_idx column of their own. When
    // the filter includes reference_idx, resolve it via a JOIN with the membership
    // table.
    let needs_membership_join =
        MEMBERSHIP_JOIN_TABLES.contains(&table) && filter.contains_key("reference_idx");

    let mut where_clauses = Vec::new();
    for (col, values) in filter {
        // Whitelist column names — all SQL is constructed from known-safe identifiers.
        // Input is signature-verified (set by control plane), but we validate anyway for
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
        } else if needs_membership_join {
            // Under the membership JOIN, feature_idx exists on BOTH the base
            // table (t) and the membership table (m), so an unqualified
            // reference is ambiguous — a combined {reference_idx, feature_idx}
            // filter (what the CP's feature_idx-scoped DoGet ticket mints)
            // would otherwise fail to bind. Qualify with the base alias.
            where_clauses.push(format!("t.{col} IN ({csv})"));
        } else {
            where_clauses.push(format!("{col} IN ({csv})"));
        }
    }

    let where_str = where_clauses.join(" AND ");
    let sql = if needs_membership_join {
        // This arm hardcodes `SELECT t.*` and does NOT use `select_list`, so a
        // projection reaching it would be silently dropped and the stream would
        // carry wider rows than the ticket signed. That is the one failure the
        // projection mechanism exists to prevent, so refuse instead.
        //
        // Unreachable twice over today: no MEMBERSHIP_JOIN_TABLES entry has a
        // projection allowlist (pinned by
        // `no_membership_join_table_has_a_projection_allowlist`), and
        // `select_list_for` at the top of this function already rejects a list for
        // an allowlist-less table. The day one of them gains an allowlist, this is
        // what stands between that and a silently widened stream — the two
        // features compose badly (under the JOIN a bare column name is ambiguous,
        // since both sides carry feature_idx, so a projection here would need
        // `t.`-qualifying), so it must be a decision, not a default.
        if !columns.is_empty() {
            return Err(Status::internal(format!(
                "projection column list is not supported on {table:?} (membership JOIN)"
            )));
        }
        format!(
            "SELECT t.* FROM {full_table} t \
             JOIN qiita_lake.reference_membership m ON t.feature_idx = m.feature_idx \
             WHERE {where_str}"
        )
    } else {
        format!("SELECT {select_list} FROM {full_table} WHERE {where_str}")
    };
    Ok((sql, full_table))
}

/// Build the `read_masked` DoGet: a call to the table macro, not a filtered SELECT.
///
/// The ticket must name exactly the macro's scope — one `mask_idx` and a non-empty
/// `prep_sample_idx` set — which is what every control-plane signing site produces
/// (deliberately not enumerated: the list drifts, and `grep 'read_masked'` on the
/// control plane is exact). Any other column is refused rather than appended as an
/// outer filter: on the human-read surface an unrecognised scope column is a
/// control-plane bug, and quietly reading "the macro's scope, plus whatever else"
/// is how an under-scoped read would pass.
fn build_read_masked_query(filter: &auth::TicketFilter) -> Result<(String, String), Status> {
    let mask_idx = single_i64_filter(filter, "mask_idx")?;
    let preps = i64_list_filter(filter, "prep_sample_idx")?;
    if filter.len() != 2 {
        return Err(Status::invalid_argument(format!(
            "read_masked accepts only mask_idx and prep_sample_idx, got {} columns",
            filter.len()
        )));
    }
    Ok((
        format!("SELECT * FROM {}", read_masked_relation(mask_idx, &preps)),
        "qiita_lake.read_masked".to_string(),
    ))
}

/// Build the assembly DoGet: one run's contigs, selected by a semi join against
/// the lake's own `assembly_membership` rather than by a roster the ticket
/// carries.
///
/// The ticket names the run and nothing else — exactly one `prep_sample_idx`,
/// exactly one `processing_idx`, no third column. Several values, or an extra
/// column, would blend contigs from heterogeneous runs into one
/// indistinguishable stream, the same failure the single-`alignment_idx` guard
/// prevents; `feature_idx` in particular is refused, so no ticket can name
/// contigs directly on these tables.
///
/// `IN (subquery)`, not a literal list: DuckDB plans it as a SEMI hash join and
/// pushes the resolved keys' min/max and a Bloom filter into the lake scan as
/// dynamic filters. Measured on DuckDB 1.5.4 / ducklake d318a545, a catalog of
/// 3.6M chunk rows over 200 files, a 26,129-contig run: this form's scan emits
/// 245,457 rows in 140 ms, while the same roster as 26,129 literals is rewritten
/// into a MARK join above an unfiltered scan — 3,600,000 rows, 1,793 ms — with
/// both forms opening the same 200 files and returning the same 235,161 rows.
/// Where per-file `feature_idx` ranges are narrow enough to prune at all, the two
/// prune identically (1 file of 200 on a contiguous run). Semi-join semantics
/// also make the DISTINCT implicit: a contig that two `(kind, bin_id)` rows claim
/// is one output row, not two.
///
/// `assembly_membership` stays out of `ALLOWED_TABLES` — it is readable here as
/// the scope resolver, never as a stream: no column of it reaches the output.
fn build_assembly_run_query(
    table: &str,
    filter: &auth::TicketFilter,
) -> Result<(String, String), Status> {
    let prep_sample_idx = single_i64_filter(filter, "prep_sample_idx")?;
    let processing_idx = single_i64_filter(filter, "processing_idx")?;
    if filter.len() != 2 {
        return Err(Status::invalid_argument(format!(
            "{table} accepts only prep_sample_idx and processing_idx, got {} columns",
            filter.len()
        )));
    }
    let full_table = format!("qiita_lake.{table}");
    Ok((
        format!(
            "SELECT * FROM {full_table} WHERE feature_idx IN (\
             SELECT feature_idx FROM qiita_lake.assembly_membership \
             WHERE prep_sample_idx = {prep_sample_idx} AND processing_idx = {processing_idx})"
        ),
        full_table,
    ))
}

/// Build the SELECT for a block-read DoGet (`read_block` / `read_masked_block`).
///
/// Source relation, `block_read_where_clause` selector and `EXPORT_READ_COLUMNS`
/// projection are all shared with the block DELETE path — deliberately: a block's read footprint and its delete
/// footprint are the same footprint, and one translator means they cannot drift.
///
/// Guards, all fail-loud (a violation is a control-plane bug, and the ticket is
/// signature-verified, so these are defense in depth):
///
/// * `members` must be non-empty. This is the invariant that makes `read_block`
///   admissible at all — an empty selector must never degrade to "all reads"
///   (see the PRIVACY note on `ALLOWED_TABLES`).
/// * `read_masked_block` must carry exactly one `mask_idx` and nothing else;
///   `read_block` must carry no filter at all. A masked block without its mask
///   scope would stream every mask's rows for those ranges, blending pass-sets
///   from different filtering identities into one indistinguishable stream — the
///   same class of error the single-`alignment_idx` guard prevents.
fn build_block_read_query(
    table: &str,
    source: &str,
    filter: &auth::TicketFilter,
    members: &[auth::BlockReadMember],
) -> Result<(String, String), Status> {
    if members.is_empty() {
        return Err(Status::invalid_argument(format!(
            "{table} requires a non-empty `members` selector (refusing an unscoped read)"
        )));
    }
    let full_table = format!("qiita_lake.{source}");
    let member_clause = block_read_where_clause(members);

    // `read_masked` is a macro (scope-as-arguments); `read` is a plain relation.
    // The member clause stays an outer filter either way — it carries the
    // per-sample sequence sub-ranges the macro's sample scope does not express,
    // and it is the SAME selector the block DELETE path uses, so a block's read
    // footprint and its delete footprint cannot drift.
    let (relation, where_str) = if table == "read_masked_block" {
        let mask_idx = single_i64_filter(filter, "mask_idx")?;
        if filter.len() != 1 {
            return Err(Status::invalid_argument(format!(
                "{table} accepts only a mask_idx filter, got {} columns",
                filter.len()
            )));
        }
        // `mask_idx` moves into the macro call; the members' samples scope its
        // `read`/`read_mask` inputs so DuckLake prunes to their files rather than
        // scanning the lake (see the measurements on the macro in ducklake.rs).
        let relation = read_masked_relation(mask_idx, &block_member_preps(members));
        (relation, member_clause)
    } else {
        if !filter.is_empty() {
            return Err(Status::invalid_argument(format!(
                "{table} accepts no filter columns (scope it with `members`), got {} columns",
                filter.len()
            )));
        }
        (full_table.clone(), member_clause)
    };

    Ok((
        format!("SELECT {EXPORT_READ_COLUMNS} FROM {relation} WHERE {where_str}"),
        full_table,
    ))
}

#[cfg(test)]
#[path = "flight_service_tests.rs"]
mod tests;
