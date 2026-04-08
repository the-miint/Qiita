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
    "reference_taxonomy",
    "reference_phylogeny",
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
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("do_action not yet implemented"))
    }

    async fn list_actions(
        &self,
        _request: Request<arrow_flight::Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("list_actions not supported"))
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
        where_clauses.push(format!("{col} IN ({csv})"));
    }

    let sql = format!(
        "SELECT * FROM {full_table} WHERE {}",
        where_clauses.join(" AND ")
    );
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
}
