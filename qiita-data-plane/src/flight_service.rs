//! Arrow Flight service implementation for the qiita data plane.
//!
//! Handles DoGet requests by verifying HMAC-signed tickets, querying DuckLake,
//! and streaming results as Arrow RecordBatches.
//!
//! Each request opens its own DuckDB connection and attaches DuckLake. This
//! avoids shared mutable state and allows concurrent requests — DuckLake's
//! snapshot isolation in the shared Postgres catalog handles concurrency.

use std::pin::Pin;

use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::{
    Action, ActionType, Criteria, FlightData, FlightDescriptor, FlightInfo, HandshakeRequest,
    HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use duckdb::Connection;
use futures::stream::{self, StreamExt};
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
}

impl QiitaFlightService {
    pub fn new(hmac_secret: Vec<u8>, catalog_connstr: String, data_path: String) -> Self {
        Self {
            hmac_secret,
            catalog_connstr,
            data_path,
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

/// Allowed table names for DoGet queries. Reject anything else.
const ALLOWED_TABLES: &[&str] = &[
    "reference_sequences",
    "reference_sequence_chunks",
    "reference_taxonomy",
    "reference_phylogeny",
    "reference_placements",
];

/// Allowed column names for filter clauses. All identifier columns that can
/// appear in a signed ticket's filter. Whitelist prevents information leakage
/// via error messages for non-existent columns.
const ALLOWED_FILTER_COLUMNS: &[&str] = &["feature_idx", "reference_idx", "node_index"];

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
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("do_put not yet implemented"))
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

/// Move Parquet files from staging to permanent storage and register in DuckLake.
///
/// Phase 1: validate all requested files exist in staging.
/// Phase 2: move all files to permanent locations under `data_path/{table_name}/`.
/// Phase 3: attach DuckLake and register all moved files.
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

    // Phase 1: validate all requested files exist.
    for filename in payload.files.keys() {
        let src = staging.join(filename);
        if !src.exists() {
            return Err(Status::not_found(format!(
                "staging file not found: {}",
                src.display()
            )));
        }
    }

    // Phase 2: move all files to permanent storage.
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
        let dest = dest_dir.join(filename);
        move_file(&src, &dest)?;
        moved.push((table.clone(), dest));
    }

    // Phase 3: register in DuckLake. Tables are ensured at startup in main.rs.
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

/// Move a file, falling back to copy+delete for cross-filesystem moves.
///
/// If the copy succeeds but delete fails, the dest file is kept (it's the
/// correct data) and the error message includes the orphaned source path
/// for cleanup.
fn move_file(src: &std::path::Path, dest: &std::path::Path) -> Result<(), Status> {
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
/// - Table name: whitelist (`ALLOWED_TABLES`) — only 3 known-safe values
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
}
