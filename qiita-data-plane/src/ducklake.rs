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

/// Create the read + read_mask tables and the read_masked view in DuckLake.
///
/// These hold per-sample sequencing reads and the downstream masks that record,
/// per read, whether it survives QC/host filtering and how it should be trimmed.
/// The full reads are stored ONCE and never physically filtered; masks are
/// downstream state keyed by the CP-minted `mask_idx` (filtering-config identity).
/// Multiple masks coexist over the same reads (e.g. host-filter vXXX and vYYY).
///
/// Same DuckLake constraint story as the reference tables: no PK/UNIQUE/FK.
/// Integrity is enforced upstream (CP mints `mask_idx`/`sequence_idx`, the
/// orchestrator verifies before loading).
///
/// PRIVACY: `read` and `read_mask` are deliberately NOT exposed via Flight
/// (they are absent from `flight_service::ALLOWED_TABLES`). The only
/// Flight-reachable read surface is the `read_masked` view, which joins read to
/// read_mask, applies the recorded trims, and excludes every non-`pass` row
/// (host/human hits + QC failures) via `WHERE m.reason = 'pass'`. Human reads
/// are therefore unreachable by construction, not by a scope check.
pub fn ensure_read_tables(conn: &Connection) -> Result<(), Box<dyn std::error::Error>> {
    conn.execute_batch(
        "-- Full reads, written ONCE per sequenced sample. Independent of any mask.
        -- Keyed by prep_sample_idx + the globally-unique sequence_idx (the read
        -- join key). qual1/qual2 are PHRED scores as UTINYINT arrays; NULL for
        -- FASTA (qual1) or single-end (sequence2/qual2). The producer writes the
        -- Parquet sorted by (prep_sample_idx, sequence_idx) — the view's join
        -- key — for row-group pruning; not the canonical six-column result sort
        -- order (those identifier columns don't exist on reads).
        CREATE TABLE IF NOT EXISTS qiita_lake.read (
            prep_sample_idx BIGINT NOT NULL,
            sequence_idx BIGINT NOT NULL,
            read_id VARCHAR NOT NULL,
            sequence1 VARCHAR NOT NULL,
            qual1 UTINYINT[],
            sequence2 VARCHAR,
            qual2 UTINYINT[]
        );

        -- One row per (mask, read). mask_idx is the CP-minted filtering-config
        -- discriminator; reason is a ReadMaskReason value ('pass' survives, all
        -- others — qc_* and host_* — are excluded from read_masked). Trims are
        -- the cumulative bases removed from each end, recorded even for failing
        -- reads so an admin reading raw `read` can reconstruct. PE never
        -- populates the left pair (3'-only trimming); left_trim2/right_trim2 are
        -- NULL for single-end.
        CREATE TABLE IF NOT EXISTS qiita_lake.read_mask (
            mask_idx BIGINT NOT NULL,
            prep_sample_idx BIGINT NOT NULL,
            sequence_idx BIGINT NOT NULL,
            reason VARCHAR NOT NULL,
            left_trim1 UINTEGER NOT NULL DEFAULT 0,
            right_trim1 UINTEGER NOT NULL DEFAULT 0,
            left_trim2 UINTEGER,
            right_trim2 UINTEGER
        );

        -- The masking + access boundary: join read to read_mask, apply trims,
        -- and exclude every non-'pass' row. This is the ONLY Flight-reachable
        -- read surface. substr() takes a 1-based start and a LENGTH; list slicing
        -- is 1-based and inclusive on both ends. The qual arrays are guarded for
        -- NULL (FASTA / single-end) symmetrically with their sequence columns.
        --
        -- Trim arithmetic: length() is signed BIGINT, so `length - left - right`
        -- promotes the UINTEGER trims to signed — no unsigned underflow even when
        -- the result is negative. At the exact full-trim boundary
        -- (left+right == length) substr length is 0 -> '' and the slice end < start
        -- -> [], consistently. The view ASSUMES the upstream invariant
        -- left_trim+right_trim <= length (enforced upstream at mask-emit time: a
        -- read trimmed below min_length is reason='qc_too_short', never 'pass').
        -- An out-of-contract over-trim row would yield inconsistent bytes; it is
        -- a producer bug, not handled here.
        --
        -- CREATE OR REPLACE (not IF NOT EXISTS): the view is pure metadata, so a
        -- definition change here is reconciled on every DP startup. IF NOT EXISTS
        -- would silently keep a stale definition on an already-attached catalog —
        -- a privacy-surface footgun (the WHERE reason='pass' predicate lives
        -- here). Tables stay IF NOT EXISTS — they hold data.
        CREATE OR REPLACE VIEW qiita_lake.read_masked AS
        SELECT
            m.mask_idx,
            m.prep_sample_idx,
            r.sequence_idx,
            r.read_id,
            substr(r.sequence1, m.left_trim1 + 1,
                   length(r.sequence1) - m.left_trim1 - m.right_trim1) AS sequence1,
            CASE WHEN r.qual1 IS NULL THEN NULL ELSE
                r.qual1[m.left_trim1 + 1 : len(r.qual1) - m.right_trim1] END AS qual1,
            CASE WHEN r.sequence2 IS NULL THEN NULL ELSE
                substr(r.sequence2, m.left_trim2 + 1,
                       length(r.sequence2) - m.left_trim2 - m.right_trim2) END AS sequence2,
            CASE WHEN r.qual2 IS NULL THEN NULL ELSE
                r.qual2[m.left_trim2 + 1 : len(r.qual2) - m.right_trim2] END AS qual2
        FROM qiita_lake.read r
        JOIN qiita_lake.read_mask m
          ON r.prep_sample_idx = m.prep_sample_idx
         AND r.sequence_idx = m.sequence_idx
        WHERE m.reason = 'pass';",
    )?;
    Ok(())
}

/// Create the assembled_genome + genome_quality tables in DuckLake.
///
/// These hold per-sample metagenome-assembly results from the pacbio-processing
/// workflow: assembled genome sequences (circular LCG genomes + refined MAG
/// bins) and their CheckM quality metrics. Keyed by the CP-minted
/// `prep_sample_idx` — the key a later cross-sample merge selects on to gather
/// many samples' genomes (across preps/studies) into one dereplicated set.
///
/// Same DuckLake constraint story as the read/reference tables: no PK/UNIQUE/FK.
/// A "genome" (one circular LCG genome or one MAG bin) is the group of contig
/// rows sharing (prep_sample_idx, kind, genome_local_id); `genome_local_id` is
/// unique within a sample, so a merge can namespace it globally as
/// `<prep_sample_idx>_<genome_local_id>`.
///
/// NOTE: not yet exposed via Flight (absent from `flight_service::ALLOWED_TABLES`).
/// register_files loads them and they are SQL-queryable in the catalog; external
/// Flight read-back is added when the cross-sample merge stage lands.
pub fn ensure_genome_tables(conn: &Connection) -> Result<(), Box<dyn std::error::Error>> {
    conn.execute_batch(
        "-- One row per assembled contig. A genome (a circular LCG genome or a
        -- refined MAG bin) is the set of rows sharing
        -- (prep_sample_idx, kind, genome_local_id). kind is 'LCG' | 'MAG'.
        -- `sequence` holds the whole contig; length_bp is a convenience/prune
        -- column. `assembler` records which step-1 tool produced it (provenance).
        CREATE TABLE IF NOT EXISTS qiita_lake.assembled_genome (
            prep_sample_idx BIGINT NOT NULL,
            kind VARCHAR NOT NULL,
            genome_local_id VARCHAR NOT NULL,
            contig_id VARCHAR NOT NULL,
            sequence VARCHAR NOT NULL,
            length_bp BIGINT NOT NULL,
            assembler VARCHAR NOT NULL
        );

        -- One row per MAG (CheckM quality). LCG quality is (re)computed later in
        -- the cross-sample merge, so `kind` is 'MAG' here. completeness /
        -- contamination / strain_heterogeneity + marker_lineage come from
        -- `checkm lineage_wf --tab_table`; genome_size / n_contigs from
        -- `checkm qa -o 2` (NOT emitted by --tab_table); das_tool_score /
        -- source_binner are provenance carried over from DAS_Tool.
        CREATE TABLE IF NOT EXISTS qiita_lake.genome_quality (
            prep_sample_idx BIGINT NOT NULL,
            kind VARCHAR NOT NULL,
            genome_local_id VARCHAR NOT NULL,
            marker_lineage VARCHAR,
            completeness DOUBLE,
            contamination DOUBLE,
            strain_heterogeneity DOUBLE,
            genome_size BIGINT,
            n_contigs BIGINT,
            das_tool_score DOUBLE,
            source_binner VARCHAR,
            assembler VARCHAR NOT NULL
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
        // Fallback matches the Docker-compose Postgres at `:5433` so
        // local Docker-mode runs work without setting the env var. The
        // Makefile's test-integration recipe in host-Postgres mode (CI
        // macOS, dev macOS without Docker) sets the env var explicitly
        // to `:5432`. Tests that mutate this env var must use
        // `EnvSnapshot` (see main.rs::tests) so they don't leak the
        // default-only state into other `#[serial]` tests.
        std::env::var("DUCKLAKE_CATALOG_CONNSTR").unwrap_or_else(|_| {
            "dbname=qiita_ducklake host=localhost port=5433 user=qiita password=qiita".to_string()
        })
    }

    fn setup_conn() -> Connection {
        let conn = Connection::open_in_memory().expect("open in-memory DuckDB");
        let connstr = test_catalog_connstr();
        // `/ducklake` must match config.rs's PATH_PERSISTENT/ducklake derivation.
        let data_path = std::env::var("PATH_PERSISTENT")
            .map(|base| format!("{base}/ducklake"))
            .unwrap_or_else(|_| "/tmp/qiita-integration-ducklake-data".to_string());
        std::fs::create_dir_all(&data_path).unwrap();
        connect_ducklake(&conn, &connstr, &data_path)
            .expect("failed to connect DuckLake — check DUCKLAKE_CATALOG_CONNSTR");
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
    #[serial]
    #[cfg(feature = "integration")]
    fn ensure_read_tables_is_idempotent() {
        // Re-running ensure_read_tables (as happens on every DP restart) must
        // not error — CREATE TABLE/VIEW IF NOT EXISTS. The view is catalog-stored
        // (Postgres catalog), so it persists across re-attach; re-ensuring is a
        // no-op rather than a failure.
        let conn = setup_conn();
        ensure_read_tables(&conn).expect("first ensure_read_tables");
        ensure_read_tables(&conn).expect("second ensure_read_tables (idempotent)");

        // The view exists and is queryable.
        let mut stmt = conn
            .prepare(
                "SELECT count(*) FROM information_schema.tables \
                 WHERE table_name = 'read_masked'",
            )
            .unwrap();
        let n: i64 = stmt.query_row([], |row| row.get(0)).unwrap();
        assert_eq!(n, 1, "read_masked view should exist exactly once");
    }

    #[test]
    #[serial]
    #[cfg(feature = "integration")]
    fn ensure_genome_tables_is_idempotent() {
        // Re-running on every DP restart must be a no-op (CREATE TABLE IF NOT
        // EXISTS), and both tables must exist and be queryable afterwards.
        let conn = setup_conn();
        ensure_genome_tables(&conn).expect("first ensure_genome_tables");
        ensure_genome_tables(&conn).expect("second ensure_genome_tables (idempotent)");

        for table in ["assembled_genome", "genome_quality"] {
            // table is a &'static str literal, so the format! is injection-safe
            // (test-only pattern; see Cleanup above).
            let sql = format!(
                "SELECT count(*) FROM information_schema.tables WHERE table_name = '{table}'"
            );
            let mut stmt = conn.prepare(&sql).unwrap();
            let n: i64 = stmt.query_row([], |row| row.get(0)).unwrap();
            assert_eq!(n, 1, "{table} table should exist exactly once");
        }
    }

    /// read_masked applies the recorded trims (substr on the sequence, list
    /// slice on the UTINYINT[] qual) and excludes every non-'pass' row. A
    /// paired-end pass row round-trips qual2; a host_rype row is excluded.
    #[test]
    #[serial]
    #[cfg(feature = "integration")]
    fn read_masked_trims_and_excludes_non_pass() {
        let conn = setup_conn();
        ensure_read_tables(&conn).unwrap();

        let prep = next_test_id();
        let mask = next_test_id();
        // sequence_idx values, disjoint per the global-uniqueness invariant.
        let seq_se = next_test_id(); // single-end pass, non-zero trims
        let seq_pe = next_test_id(); // paired-end pass, qual2 present
        let seq_host = next_test_id(); // host hit, must be excluded

        let _c_read = Cleanup {
            conn: &conn,
            table: "read",
            column: "prep_sample_idx",
            id: prep,
        };
        let _c_mask = Cleanup {
            conn: &conn,
            table: "read_mask",
            column: "prep_sample_idx",
            id: prep,
        };

        // sequence1 = "AACGTACGTT" (len 10). left_trim1=2, right_trim1=3 →
        // substr(seq, 3, 10-2-3=5) = chars 3..7 = "CGTAC".
        // qual1 = [10,11,12,13,14,15,16,17,18,19]; slice [3 : 10-3=7] (1-based,
        // inclusive) = positions 3..7 = [12,13,14,15,16].
        conn.execute_batch(&format!(
            "INSERT INTO qiita_lake.read \
                 (prep_sample_idx, sequence_idx, read_id, sequence1, qual1, sequence2, qual2) VALUES \
                 ({prep}, {seq_se}, 'r_se', 'AACGTACGTT', \
                  [10,11,12,13,14,15,16,17,18,19]::UTINYINT[], NULL, NULL);
             INSERT INTO qiita_lake.read \
                 (prep_sample_idx, sequence_idx, read_id, sequence1, qual1, sequence2, qual2) VALUES \
                 ({prep}, {seq_pe}, 'r_pe', 'GGGGTTTT', \
                  [20,21,22,23,24,25,26,27]::UTINYINT[], \
                  'CCCCAAAA', [30,31,32,33,34,35,36,37]::UTINYINT[]);
             INSERT INTO qiita_lake.read \
                 (prep_sample_idx, sequence_idx, read_id, sequence1, qual1, sequence2, qual2) VALUES \
                 ({prep}, {seq_host}, 'r_host', 'TTTTTTTT', \
                  [40,41,42,43,44,45,46,47]::UTINYINT[], NULL, NULL);"
        ))
        .unwrap();

        // Masks: SE pass (trims 2/3), PE pass (left_trim*=0, right_trim1=1,
        // right_trim2=2), host_rype (must be excluded regardless of trims).
        conn.execute_batch(&format!(
            "INSERT INTO qiita_lake.read_mask \
                 (mask_idx, prep_sample_idx, sequence_idx, reason, left_trim1, right_trim1, left_trim2, right_trim2) VALUES \
                 ({mask}, {prep}, {seq_se}, 'pass', 2, 3, NULL, NULL);
             INSERT INTO qiita_lake.read_mask \
                 (mask_idx, prep_sample_idx, sequence_idx, reason, left_trim1, right_trim1, left_trim2, right_trim2) VALUES \
                 ({mask}, {prep}, {seq_pe}, 'pass', 0, 1, 0, 2);
             INSERT INTO qiita_lake.read_mask \
                 (mask_idx, prep_sample_idx, sequence_idx, reason, left_trim1, right_trim1, left_trim2, right_trim2) VALUES \
                 ({mask}, {prep}, {seq_host}, 'host_rype', 0, 0, NULL, NULL);"
        ))
        .unwrap();

        // (b) non-'pass' rows excluded: exactly 2 rows for this (mask, prep).
        let total: i64 = conn
            .query_row(
                &format!(
                    "SELECT count(*) FROM qiita_lake.read_masked \
                     WHERE mask_idx = {mask} AND prep_sample_idx = {prep}"
                ),
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            total, 2,
            "host_rype row must be excluded by WHERE reason='pass'"
        );

        // The duckdb FromSql path doesn't decode LIST columns into Vec<T>, so we
        // render the UTINYINT[] arrays to a comma-joined string in SQL and assert
        // on that — this still exercises the list-slice trim AND the round-trip
        // (a wrong element or count would change the string). A NULL array
        // (FASTA / single-end) renders as a SQL NULL → Option::None.

        // (a) SE trim math: sequence1 = "CGTAC", qual1 = [12,13,14,15,16].
        let mut stmt = conn
            .prepare(&format!(
                "SELECT sequence1, array_to_string(qual1, ','), sequence2, \
                        array_to_string(qual2, ',') \
                 FROM qiita_lake.read_masked \
                 WHERE mask_idx = {mask} AND sequence_idx = {seq_se}"
            ))
            .unwrap();
        let (seq1, qual1, seq2, qual2): (String, String, Option<String>, Option<String>) = stmt
            .query_row([], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
            })
            .unwrap();
        assert_eq!(seq1, "CGTAC", "SE substr trim");
        // (c) UTINYINT[] round-trip + list-slice trim.
        assert_eq!(qual1, "12,13,14,15,16", "SE qual list slice");
        assert_eq!(seq2, None, "single-end has no sequence2");
        assert_eq!(qual2, None, "single-end has no qual2");

        // PE trim math: sequence1 "GGGGTTTT" trim 0/1 → "GGGGTTT" (len 7);
        // qual1 slice [1 : 8-1=7] = first 7 = [20..26]. sequence2 "CCCCAAAA"
        // trim 0/2 → "CCCCAA" (len 6); qual2 slice [1 : 8-2=6] = [30..35].
        let mut stmt2 = conn
            .prepare(&format!(
                "SELECT sequence1, array_to_string(qual1, ','), sequence2, \
                        array_to_string(qual2, ',') \
                 FROM qiita_lake.read_masked \
                 WHERE mask_idx = {mask} AND sequence_idx = {seq_pe}"
            ))
            .unwrap();
        let (pseq1, pqual1, pseq2, pqual2): (String, String, Option<String>, Option<String>) =
            stmt2
                .query_row([], |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
                })
                .unwrap();
        assert_eq!(pseq1, "GGGGTTT", "PE seq1 3' trim");
        assert_eq!(pqual1, "20,21,22,23,24,25,26", "PE qual1 slice");
        assert_eq!(pseq2.as_deref(), Some("CCCCAA"), "PE seq2 3' trim");
        assert_eq!(
            pqual2.as_deref(),
            Some("30,31,32,33,34,35"),
            "PE qual2 round-trip + slice"
        );
    }

    /// Trim boundaries the view relies on: exact full-trim
    /// (left+right == length) yields '' and an EMPTY array (asserted via len, not
    /// a joined string that would also be '' for a wrong result); zero-trim (0/0)
    /// is identity. length() is signed, so the arithmetic clamps cleanly here.
    #[test]
    #[serial]
    #[cfg(feature = "integration")]
    fn read_masked_trim_boundaries() {
        let conn = setup_conn();
        ensure_read_tables(&conn).unwrap();

        let prep = next_test_id();
        let mask = next_test_id();
        let seq_full = next_test_id(); // full-trim SE → '' + []
        let seq_full_pe = next_test_id(); // full-trim on both mates
        let seq_zero = next_test_id(); // zero-trim identity

        let _c_read = Cleanup {
            conn: &conn,
            table: "read",
            column: "prep_sample_idx",
            id: prep,
        };
        let _c_mask = Cleanup {
            conn: &conn,
            table: "read_mask",
            column: "prep_sample_idx",
            id: prep,
        };

        conn.execute_batch(&format!(
            "INSERT INTO qiita_lake.read \
                 (prep_sample_idx, sequence_idx, read_id, sequence1, qual1, sequence2, qual2) VALUES \
                 ({prep}, {seq_full}, 'r_full', 'ACGTAC', [1,2,3,4,5,6]::UTINYINT[], NULL, NULL);
             INSERT INTO qiita_lake.read \
                 (prep_sample_idx, sequence_idx, read_id, sequence1, qual1, sequence2, qual2) VALUES \
                 ({prep}, {seq_full_pe}, 'r_full_pe', 'AAAA', [1,2,3,4]::UTINYINT[], \
                  'TTTT', [5,6,7,8]::UTINYINT[]);
             INSERT INTO qiita_lake.read \
                 (prep_sample_idx, sequence_idx, read_id, sequence1, qual1, sequence2, qual2) VALUES \
                 ({prep}, {seq_zero}, 'r_zero', 'GGGG', [9,9,9,9]::UTINYINT[], NULL, NULL);"
        ))
        .unwrap();

        conn.execute_batch(&format!(
            "INSERT INTO qiita_lake.read_mask \
                 (mask_idx, prep_sample_idx, sequence_idx, reason, left_trim1, right_trim1, left_trim2, right_trim2) VALUES \
                 ({mask}, {prep}, {seq_full}, 'pass', 4, 2, NULL, NULL);
             INSERT INTO qiita_lake.read_mask \
                 (mask_idx, prep_sample_idx, sequence_idx, reason, left_trim1, right_trim1, left_trim2, right_trim2) VALUES \
                 ({mask}, {prep}, {seq_full_pe}, 'pass', 1, 3, 2, 2);
             INSERT INTO qiita_lake.read_mask \
                 (mask_idx, prep_sample_idx, sequence_idx, reason, left_trim1, right_trim1, left_trim2, right_trim2) VALUES \
                 ({mask}, {prep}, {seq_zero}, 'pass', 0, 0, NULL, NULL);"
        ))
        .unwrap();

        // Full-trim SE: 4+2 == 6 == length → '' and an empty (not garbage) array.
        let (s_full, qlen_full): (String, i64) = conn
            .query_row(
                &format!(
                    "SELECT sequence1, len(qual1) FROM qiita_lake.read_masked \
                     WHERE mask_idx = {mask} AND sequence_idx = {seq_full}"
                ),
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(s_full, "", "full-trim sequence1 is empty");
        assert_eq!(
            qlen_full, 0,
            "full-trim qual1 is an empty array, not garbage"
        );

        // Full-trim PE: seq1 1/3 on len4 → ''; seq2 2/2 on len4 → ''. Both quals empty.
        let (pe_s1, pe_q1len, pe_s2, pe_q2len): (String, i64, Option<String>, Option<i64>) = conn
            .query_row(
                &format!(
                    "SELECT sequence1, len(qual1), sequence2, len(qual2) \
                     FROM qiita_lake.read_masked \
                     WHERE mask_idx = {mask} AND sequence_idx = {seq_full_pe}"
                ),
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .unwrap();
        assert_eq!(pe_s1, "", "PE full-trim seq1 empty");
        assert_eq!(pe_q1len, 0, "PE full-trim qual1 empty");
        assert_eq!(pe_s2.as_deref(), Some(""), "PE full-trim seq2 empty");
        assert_eq!(pe_q2len, Some(0), "PE full-trim qual2 empty");

        // Zero-trim: identity.
        let (s_zero, qlen_zero): (String, i64) = conn
            .query_row(
                &format!(
                    "SELECT sequence1, len(qual1) FROM qiita_lake.read_masked \
                     WHERE mask_idx = {mask} AND sequence_idx = {seq_zero}"
                ),
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(s_zero, "GGGG", "zero-trim is identity");
        assert_eq!(qlen_zero, 4, "zero-trim keeps all quals");
    }

    /// The read_masked view is stored in the Postgres catalog, not the session: a
    /// fresh ATTACH (a real DP restart) sees it WITHOUT re-running
    /// ensure_read_tables. Asserts that catalog-stored view persistence holds on
    /// the Postgres catalog the data plane actually uses.
    #[test]
    #[serial]
    #[cfg(feature = "integration")]
    fn read_masked_view_persists_across_reattach() {
        let prep = next_test_id();
        let mask = next_test_id();
        let seq = next_test_id();

        // Connection 1: ensure the view + tables, insert a pass row, then drop it.
        {
            let conn1 = setup_conn();
            ensure_read_tables(&conn1).unwrap();
            conn1
                .execute_batch(&format!(
                    "INSERT INTO qiita_lake.read \
                         (prep_sample_idx, sequence_idx, read_id, sequence1, qual1, sequence2, qual2) VALUES \
                         ({prep}, {seq}, 'r', 'ACGT', [1,2,3,4]::UTINYINT[], NULL, NULL);
                     INSERT INTO qiita_lake.read_mask \
                         (mask_idx, prep_sample_idx, sequence_idx, reason, left_trim1, right_trim1, left_trim2, right_trim2) VALUES \
                         ({mask}, {prep}, {seq}, 'pass', 0, 0, NULL, NULL);"
                ))
                .unwrap();
        }

        // Connection 2: fresh ATTACH, NO ensure_read_tables — the catalog-stored
        // view must already exist and be queryable.
        let conn2 = setup_conn();
        let _c_read = Cleanup {
            conn: &conn2,
            table: "read",
            column: "prep_sample_idx",
            id: prep,
        };
        let _c_mask = Cleanup {
            conn: &conn2,
            table: "read_mask",
            column: "prep_sample_idx",
            id: prep,
        };
        let s: String = conn2
            .query_row(
                &format!(
                    "SELECT sequence1 FROM qiita_lake.read_masked \
                     WHERE mask_idx = {mask} AND sequence_idx = {seq}"
                ),
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            s, "ACGT",
            "view persisted across re-attach (zero-trim identity)"
        );
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
