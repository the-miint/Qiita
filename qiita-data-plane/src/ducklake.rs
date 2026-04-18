//! DuckLake connection management and reference table setup.
//!
//! DuckLake uses a Postgres catalog database for metadata and stores
//! Parquet data on the shared filesystem. Each data plane instance
//! holds an independent DuckDB+DuckLake connection.
//!
//! ATTACH syntax: `ducklake:postgres:<libpq connection string>`
//! with DATA_PATH as a separate option specifying Parquet storage location.
//! Requires both `ducklake` and `postgres` extensions.
//!
//! DuckLake does not support UNIQUE or FK constraints. Data integrity
//! (no duplicate feature_idx, valid reference_idx) must be enforced
//! programmatically before insertion — the control plane owns dedup,
//! the orchestrator verifies before loading.

use duckdb::Connection;

/// Characters disallowed in connection strings and paths interpolated into SQL.
/// DuckDB ATTACH does not support parameterized queries, so we validate inputs
/// before interpolation. This is input validation, not sanitization.
fn validate_sql_literal(value: &str, field: &str) -> Result<(), String> {
    if value.contains('\'') || value.contains(';') || value.contains('\0') {
        return Err(format!(
            "{field} contains disallowed characters (single quote, semicolon, or null byte)"
        ));
    }
    Ok(())
}

/// Connect to DuckLake backed by a Postgres catalog.
///
/// Attaches the DuckLake catalog as `qiita_lake` in the DuckDB session.
/// `catalog_connstr` is a libpq-style connection string (e.g., "dbname=qiita_ducklake host=localhost").
/// `data_path` is the directory where DuckLake stores Parquet files.
pub fn connect_ducklake(
    conn: &Connection,
    catalog_connstr: &str,
    data_path: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    validate_sql_literal(catalog_connstr, "catalog_connstr")?;
    validate_sql_literal(data_path, "data_path")?;
    conn.execute_batch("LOAD ducklake; LOAD postgres;")?;
    conn.execute_batch(&format!(
        "ATTACH 'ducklake:postgres:{catalog_connstr}' AS qiita_lake (DATA_PATH '{data_path}');"
    ))?;
    Ok(())
}

/// Create the reference data tables in DuckLake if they don't already exist.
///
/// Note on DuckLake constraints: DuckLake does not support UNIQUE, PK, or FK
/// constraints. Data integrity is enforced upstream:
/// - The control plane deduplicates features by sequence_hash before minting feature_idx
/// - The orchestrator verifies data before loading
/// - The data plane validates identifier sets programmatically on DoAction
///
/// Per-table storage tuning (compression, row group size) is deferred to a
/// separate configuration pass — see DuckLake configuration docs.
pub fn ensure_reference_tables(conn: &Connection) -> Result<(), Box<dyn std::error::Error>> {
    conn.execute_batch(
        "-- Sequence metadata: one row per feature (hash, length).
        -- Actual sequence data lives in reference_sequence_chunks.
        CREATE TABLE IF NOT EXISTS qiita_lake.reference_sequences (
            feature_idx BIGINT NOT NULL,
            sequence_hash UUID NOT NULL,
            sequence_length_bp BIGINT NOT NULL
        );

        -- Chunked sequence data: sequences split into fixed-size chunks
        -- (default 64 KB) for efficient Parquet storage. Short sequences
        -- (e.g., 16S at 1.5 kb) are a single chunk. Reassemble with:
        --   string_agg(chunk_data, '' ORDER BY chunk_index)
        CREATE TABLE IF NOT EXISTS qiita_lake.reference_sequence_chunks (
            feature_idx BIGINT NOT NULL,
            chunk_index INTEGER NOT NULL,
            chunk_data VARCHAR NOT NULL
        );

        CREATE TABLE IF NOT EXISTS qiita_lake.reference_taxonomy (
            reference_idx BIGINT NOT NULL,
            feature_idx BIGINT NOT NULL,
            domain VARCHAR,
            phylum VARCHAR,
            class VARCHAR,
            \"order\" VARCHAR,
            family VARCHAR,
            genus VARCHAR,
            species VARCHAR,
            strain VARCHAR,
            ncbi_taxon_id BIGINT
        );

        -- Phylogeny nodes. feature_idx is populated for tip nodes (links
        -- directly to sequences), NULL for internal nodes.
        CREATE TABLE IF NOT EXISTS qiita_lake.reference_phylogeny (
            reference_idx BIGINT NOT NULL,
            node_index BIGINT NOT NULL,
            name VARCHAR,
            branch_length DOUBLE,
            edge_id BIGINT,
            parent_index BIGINT,
            is_tip BOOLEAN NOT NULL,
            feature_idx BIGINT
        );

        CREATE TABLE IF NOT EXISTS qiita_lake.reference_membership (
            reference_idx BIGINT NOT NULL,
            feature_idx BIGINT NOT NULL
        );

        -- Phylogenetic placements: maps placed sequences (by feature_idx)
        -- to edges in the backbone tree.
        CREATE TABLE IF NOT EXISTS qiita_lake.reference_placements (
            reference_idx BIGINT NOT NULL,
            feature_idx BIGINT NOT NULL,
            edge_num INTEGER NOT NULL,
            likelihood DOUBLE,
            like_weight_ratio DOUBLE,
            distal_length DOUBLE,
            pendant_length DOUBLE
        );",
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// Atomic counter to generate unique test IDs across parallel tests.
    static TEST_ID: AtomicU64 = AtomicU64::new(800_000);

    fn next_test_id() -> i64 {
        TEST_ID.fetch_add(1, Ordering::Relaxed) as i64
    }

    fn test_catalog_connstr() -> String {
        std::env::var("DUCKLAKE_CATALOG_CONNSTR").unwrap_or_else(|_| {
            "dbname=qiita_ducklake host=localhost port=5433 user=qiita password=qiita".to_string()
        })
    }

    fn setup_conn() -> Connection {
        let conn = Connection::open_in_memory().expect("open in-memory DuckDB");
        let connstr = test_catalog_connstr();
        let data_path = std::env::var("DUCKLAKE_DATA_PATH")
            .unwrap_or_else(|_| "/tmp/qiita-integration-ducklake-data".to_string());
        std::fs::create_dir_all(&data_path).unwrap();
        connect_ducklake(&conn, &connstr, &data_path)
            .expect("failed to connect DuckLake — is Postgres running on :5433?");
        conn
    }

    /// Guard that cleans up test rows on drop, even if the test panics.
    /// Uses format! for SQL — safe because table/column are &'static str from
    /// compile-time constants in the test code, and id is an integer.
    /// This pattern is test-only and must not be copied to production code.
    struct Cleanup<'a> {
        conn: &'a Connection,
        table: &'static str,
        column: &'static str,
        id: i64,
    }

    impl Drop for Cleanup<'_> {
        fn drop(&mut self) {
            let _ = self.conn.execute_batch(&format!(
                "DELETE FROM qiita_lake.{} WHERE {} = {};",
                self.table, self.column, self.id
            ));
        }
    }

    #[test]
    #[serial]
    #[cfg(feature = "integration")]
    fn connect_and_verify_table_schemas() {
        let conn = setup_conn();
        ensure_reference_tables(&conn).expect("failed to create tables");

        // Verify reference_sequences has the expected columns
        let mut stmt = conn
            .prepare(
                "SELECT column_name FROM information_schema.columns \
                 WHERE table_name = 'reference_sequences' \
                 ORDER BY ordinal_position",
            )
            .unwrap();
        let cols: Vec<String> = stmt
            .query_map([], |row| row.get(0))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert!(
            cols.contains(&"feature_idx".to_string()),
            "missing feature_idx column, got: {cols:?}"
        );
        assert!(
            cols.contains(&"sequence_hash".to_string()),
            "missing sequence_hash column, got: {cols:?}"
        );
        assert!(
            cols.contains(&"sequence_length_bp".to_string()),
            "missing sequence_length_bp column, got: {cols:?}"
        );
    }

    #[test]
    #[serial]
    #[cfg(feature = "integration")]
    fn insert_and_read_reference_sequence() {
        let conn = setup_conn();
        ensure_reference_tables(&conn).unwrap();
        let id = next_test_id();
        let _cleanup = Cleanup {
            conn: &conn,
            table: "reference_sequences",
            column: "feature_idx",
            id,
        };

        conn.execute_batch(&format!(
            "INSERT INTO qiita_lake.reference_sequences VALUES \
             ({id}, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::UUID, 4);"
        ))
        .unwrap();

        let mut stmt = conn
            .prepare(&format!(
                "SELECT feature_idx, sequence_length_bp \
                 FROM qiita_lake.reference_sequences WHERE feature_idx = {id}"
            ))
            .unwrap();
        let (idx, len): (i64, i64) = stmt
            .query_row([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap();
        assert_eq!(idx, id);
        assert_eq!(len, 4);
    }

    #[test]
    #[serial]
    #[cfg(feature = "integration")]
    fn insert_and_read_taxonomy() {
        let conn = setup_conn();
        ensure_reference_tables(&conn).unwrap();
        let id = next_test_id();
        let _cleanup = Cleanup {
            conn: &conn,
            table: "reference_taxonomy",
            column: "reference_idx",
            id,
        };

        conn.execute_batch(&format!(
            "INSERT INTO qiita_lake.reference_taxonomy \
             (reference_idx, feature_idx, domain, phylum) \
             VALUES ({id}, {id}, 'd__Bacteria', 'p__Bacillota');"
        ))
        .unwrap();

        let mut stmt = conn
            .prepare(&format!(
                "SELECT domain, phylum FROM qiita_lake.reference_taxonomy \
                 WHERE reference_idx = {id} AND feature_idx = {id}"
            ))
            .unwrap();
        let (domain, phylum): (String, String) = stmt
            .query_row([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap();
        assert_eq!(domain, "d__Bacteria");
        assert_eq!(phylum, "p__Bacillota");
    }

    #[test]
    #[serial]
    #[cfg(feature = "integration")]
    fn insert_and_read_phylogeny() {
        let conn = setup_conn();
        ensure_reference_tables(&conn).unwrap();
        let id = next_test_id();
        let _cleanup = Cleanup {
            conn: &conn,
            table: "reference_phylogeny",
            column: "reference_idx",
            id,
        };

        conn.execute_batch(&format!(
            "INSERT INTO qiita_lake.reference_phylogeny VALUES ({id}, 0, 'root', 0.0, NULL, NULL, false, NULL);
             INSERT INTO qiita_lake.reference_phylogeny VALUES ({id}, 1, 'tip1', 0.5, 0, 0, true, {id});"
        ))
        .unwrap();

        let mut stmt = conn
            .prepare(&format!(
                "SELECT count(*) FROM qiita_lake.reference_phylogeny WHERE reference_idx = {id}"
            ))
            .unwrap();
        let count: i64 = stmt.query_row([], |row| row.get(0)).unwrap();
        assert_eq!(count, 2);
    }

    #[test]
    fn reject_connstr_with_quote() {
        let conn = Connection::open_in_memory().expect("open in-memory DuckDB");
        let result = connect_ducklake(&conn, "dbname=test'; DROP TABLE x;--", "/tmp/safe");
        assert!(result.is_err());
        assert!(
            result.unwrap_err().to_string().contains("disallowed"),
            "error should mention disallowed characters"
        );
    }

    #[test]
    fn reject_data_path_with_quote() {
        let conn = Connection::open_in_memory().expect("open in-memory DuckDB");
        let result = connect_ducklake(&conn, "dbname=test", "/tmp/it's bad");
        assert!(result.is_err());
    }
}
