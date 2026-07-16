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

/// Parquet row-group size (rows) DuckLake should use when it rewrites the chunked
/// sequence tables. Must equal `qiita_common.chunking.CHUNK_ROW_GROUP_SIZE`: the
/// orchestrator writes those chunk Parquets at this row count
/// (`PARQUET_OPTS_CHUNKED` `ROW_GROUP_SIZE`), and the per-table `set_option` calls
/// below pin DuckLake's own rewrites to the same layout.
const CHUNK_ROW_GROUP_SIZE: u64 = 16384;

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

/// Set the catalog-global Parquet options DuckLake uses for its OWN writes
/// (compaction, merge, any future direct insert), aligning them with how
/// register_files writes our files: `qiita_common.parquet.PARQUET_OPTS` is
/// zstd + Parquet v2, whereas DuckLake defaults to snappy + v1. Without this,
/// DuckLake's maintenance rewrites would drift from the register-time format.
///
/// These options are PERSISTED in `ducklake_metadata` (catalog-global), so they
/// only need to be set ONCE per catalog. Call this on the PROCESS-START
/// connection only — NEVER on a per-request attach. Setting it on every attach
/// made each concurrent Flight request UPDATE the same `ducklake_metadata` row,
/// which serialized and failed under load with Postgres SQLSTATE 40001
/// (`could not serialize access due to concurrent update`). Keep the values in
/// sync with PARQUET_OPTS.
pub fn set_catalog_options(conn: &Connection) -> Result<(), Box<dyn std::error::Error>> {
    conn.execute_batch(
        "CALL qiita_lake.set_option('parquet_compression', 'zstd');
         CALL qiita_lake.set_option('parquet_version', 2);",
    )?;
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
        );

        -- Annotations: a feature that is a REGION OF another feature — a SynDNA
        -- insert on its plasmid, a gene on a chromosome. Every other reference table
        -- treats feature_idx as a WHOLE sequence; this is the one place it is a
        -- sub-interval, which is what lets a quantification be keyed by the thing
        -- measured (the insert) while reads align to the thing sequenced (the plasmid).
        --
        --   annotation_idx     -- the OCCURRENCE's identity, minted by the control
        --                        plane. This is the join back to the Postgres claim
        --                        (qiita.reference_annotation) and to the semantic terms
        --                        (qiita.annotation_term via qiita.annotation_to_term).
        --   feature_idx        -- the annotated interval's BYTES (minted from the
        --                        canonical hash of the EXTRACTED sub-sequence).
        --   parent_feature_idx -- the sequence it sits on, and what reads align to.
        --   annotation_id      -- the GFF3 `ID`. PROVENANCE ONLY — nullable, and NOT
        --                        unique: GFF3 lets a discontinuous feature repeat one ID
        --                        across N lines (NCBI's E. coli RefSeq has 20 such).
        --
        -- feature_idx is NOT the occurrence's identity either: identical bases share one
        -- feature_idx (a bacterial 16S occurs in 5-7 byte-identical copies), so a feature
        -- is a SEQUENCE and an annotation is an OCCURRENCE of it at a place. A consumer
        -- aggregating coverage over a feature sums across its occurrences.
        --
        -- Annotated features are deliberately absent from reference_membership (which
        -- is what gets INDEXED and aligned against) and from reference_sequences /
        -- _chunks (the bytes are recoverable from parent + interval). Rationale in
        -- full: qiita-control-plane/db/migrations/20260713020000_reference_annotation.sql
        --
        -- Coordinates are 1-based HALF-OPEN [position, stop_position) — matching
        -- read_alignments / alignment_slice / qiita_lake.alignment, NOT the closed
        -- convention GFF3 arrives in. Converted once, at ingest, in hash_sequences.
        --
        -- `attributes` is kept RAW and lossless (it is what the GFF3 said). The
        -- normalized cross-references parsed out of it live in Postgres as
        -- qiita.annotation_term — the MAP stays so that a system we do not yet parse is
        -- still recoverable without a re-ingest.
        CREATE TABLE IF NOT EXISTS qiita_lake.reference_annotation (
            annotation_idx BIGINT NOT NULL,
            reference_idx BIGINT NOT NULL,
            feature_idx BIGINT NOT NULL,
            parent_feature_idx BIGINT NOT NULL,
            annotation_id VARCHAR,
            source VARCHAR,
            annotation_type VARCHAR NOT NULL,
            position BIGINT NOT NULL,
            stop_position BIGINT NOT NULL,
            strand VARCHAR NOT NULL,
            score DOUBLE,
            phase SMALLINT,
            attributes MAP(VARCHAR, VARCHAR)
        );",
    )?;
    // Pin DuckLake's own rewrites of the chunk table to the row-group the chunk
    // writer uses (see CHUNK_ROW_GROUP_SIZE).
    conn.execute_batch(&format!(
        "CALL qiita_lake.set_option('parquet_row_group_size', {CHUNK_ROW_GROUP_SIZE}, \
         table_name => 'reference_sequence_chunks');"
    ))?;
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

/// Create the DuckLake `alignment` table — the sink for the sharded-alignment
/// consumer.
///
/// One row per emitted alignment: the `align_sharded` native job aligns a block
/// of a sample's HOST-DEPLETED reads against a sharded reference and register-files
/// lands its `alignment.parquet` here. The table is keyed by the CP-minted
/// `alignment_idx` (the align config's params-hash identity: reference, aligner,
/// mask, and shard-set), NOT by the deferred processing_idx / processed_prep_sample
/// hierarchy. It carries `feature_idx` (the aligned subject) but NOT
/// `reference_idx`: reference scoping is a query-time join against
/// `reference_membership`, and a feature's shard is likewise derivable via
/// `reference_membership.shard_id`, so there is no per-row `shard_id` column either
/// (the identifier-ownership design in CLAUDE.md).
///
/// Column order = the exact order `align_sharded`'s COPY writes (so register-files'
/// `ducklake_add_data_files` schema-matches for free): the five CP identity columns
/// (`alignment_idx`, `prep_sample_idx`, `sequence_idx`, `feature_idx`,
/// `mate_feature_idx`) followed by the miint aligner output
/// `a.* EXCLUDE (read_id, reference, mate_reference)` — the SAM columns MINUS the raw
/// VARCHAR subject ids, which are dropped because `feature_idx` / `mate_feature_idx`
/// (cast from them) already carry that identity. That miint output was qiita-verified
/// against the team-mirror v1.5.4 build for BOTH `align_minimap2_sharded` and
/// `align_bowtie2_sharded` (identical schema; see docs/duckdb-miint.md). The types
/// match miint exactly (`flags` USMALLINT, `mapq` UTINYINT, positions/lengths BIGINT)
/// so the parquet registers without a cast; a miint SAM-schema change would need a
/// matching migration here (coupled to the pinned DuckDB/miint version, per the
/// version-lockstep discipline).
///
/// Same DuckLake constraint story as the read/reference tables: no PK/UNIQUE/FK
/// (integrity is enforced upstream — the CP mints alignment_idx; align_sharded
/// stamps feature_idx). Exposed via Flight DoGet (in flight_service::ALLOWED_TABLES)
/// for the feature-table (OGU) consumer: reads are always scoped to a single
/// alignment_idx + an explicit prep_sample_idx set and projected to the
/// coverage/OGU columns (see ALIGNMENT_DOGET_PROJECTION); an unscoped read is
/// refused. This is host-depleted derived data, not raw human reads.
pub fn ensure_alignment_tables(conn: &Connection) -> Result<(), Box<dyn std::error::Error>> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS qiita_lake.alignment (
            -- CP identity columns (align_sharded prepends these to the aligner output).
            alignment_idx    BIGINT NOT NULL,
            prep_sample_idx  BIGINT NOT NULL,
            sequence_idx     BIGINT NOT NULL,
            feature_idx      BIGINT NOT NULL,
            mate_feature_idx BIGINT,
            -- miint aligner output minus the raw VARCHAR subject ids
            -- (reference / mate_reference): their identity is already carried by
            -- feature_idx / mate_feature_idx (cast from them in align_sharded), so
            -- persisting the strings too would be redundant.
            flags            USMALLINT,
            position         BIGINT,
            stop_position    BIGINT,
            mapq             UTINYINT,
            cigar            VARCHAR,
            mate_position    BIGINT,
            template_length  BIGINT,
            tag_as           BIGINT,
            tag_xs           BIGINT,
            tag_ys           BIGINT,
            tag_xn           BIGINT,
            tag_xm           BIGINT,
            tag_xo           BIGINT,
            tag_xg           BIGINT,
            tag_nm           BIGINT,
            tag_yt           VARCHAR,
            tag_md           VARCHAR,
            tag_sa           VARCHAR
        );",
    )?;
    Ok(())
}

/// Create the assembly-result tables in DuckLake — the assembly analogue of the
/// reference-sequence tables, following the SAME chunked + content-hashed model.
///
/// A contig is stored ONCE, deduped by content hash and keyed by the CP-minted
/// `feature_idx` (the shared `qiita.feature` space, minted via `mint-features`),
/// exactly like a reference sequence. The bytes are 64 KB chunks (reassemble via
/// `string_agg(chunk_data, '' ORDER BY chunk_index)`), never a bulk VARCHAR cell.
/// `assembly_membership` records which features a prep_sample's assembly contains
/// and in which bin (a circular LCG genome or a refined MAG) — the DuckLake copy
/// of `qiita.assembly_membership`, for bulk joins against the sequences.
/// `bin_quality` is per-MAG CheckM, joined to its contigs via assembly_membership
/// on (prep_sample_idx, kind, bin_id).
///
/// Same DuckLake constraint story as the read/reference tables: no PK/UNIQUE/FK
/// (integrity is enforced upstream — the CP mints feature_idx/dedups on
/// sequence_hash, the orchestrator verifies before load).
///
/// NOTE: not yet exposed via Flight (absent from `flight_service::ALLOWED_TABLES`).
/// register_files loads them and they are SQL-queryable in the catalog; they are
/// intentionally not on the external Flight read-back path (`ALLOWED_TABLES`).
pub fn ensure_assembly_tables(conn: &Connection) -> Result<(), Box<dyn std::error::Error>> {
    conn.execute_batch(
        "-- One row per UNIQUE contig (content-hash deduped), keyed by the minted
        -- feature_idx. Mirrors reference_sequences: sequence_length_bp lives here
        -- (kept for coverage), the bytes live in the chunks table.
        CREATE TABLE IF NOT EXISTS qiita_lake.assembled_sequence (
            feature_idx BIGINT NOT NULL,
            sequence_hash UUID NOT NULL,
            sequence_length_bp BIGINT NOT NULL
        );

        -- The contig bytes in 64 KB chunks (reassemble with
        -- string_agg(chunk_data, '' ORDER BY chunk_index)). Mirrors
        -- reference_sequence_chunks; loaded multi-file (a <table>/ subdir of parts)
        -- so a large assembly never OOMs a single-file sort+write.
        CREATE TABLE IF NOT EXISTS qiita_lake.assembled_sequence_chunks (
            feature_idx BIGINT NOT NULL,
            chunk_index INTEGER NOT NULL,
            chunk_data VARCHAR NOT NULL
        );

        -- Which features a (prep_sample, processing) assembly run contains, and in
        -- which bin. processing_idx disambiguates runs (bin_id reused across
        -- samples AND runs); kind is 'LCG' | 'MAG' (value set owned by the
        -- producer). The DuckLake copy of qiita.assembly_membership for bulk joins
        -- with the sequences.
        CREATE TABLE IF NOT EXISTS qiita_lake.assembly_membership (
            prep_sample_idx BIGINT NOT NULL,
            processing_idx BIGINT NOT NULL,
            kind VARCHAR NOT NULL,
            bin_id VARCHAR NOT NULL,
            feature_idx BIGINT NOT NULL
        );

        -- Per-MAG CheckM quality. Joins to its contigs via assembly_membership on
        -- (prep_sample_idx, processing_idx, kind, bin_id). completeness /
        -- contamination / strain_heterogeneity + marker_lineage from `checkm
        -- lineage_wf --tab_table`; genome_size / n_contigs from `checkm qa -o 2`;
        -- das_tool_score / source_binner are DAS_Tool provenance. The assembler is
        -- captured in qiita.processing (processing_idx), not repeated here.
        CREATE TABLE IF NOT EXISTS qiita_lake.bin_quality (
            prep_sample_idx BIGINT NOT NULL,
            processing_idx BIGINT NOT NULL,
            kind VARCHAR NOT NULL,
            bin_id VARCHAR NOT NULL,
            marker_lineage VARCHAR,
            completeness DOUBLE,
            contamination DOUBLE,
            strain_heterogeneity DOUBLE,
            genome_size BIGINT,
            n_contigs BIGINT,
            das_tool_score DOUBLE,
            source_binner VARCHAR
        );",
    )?;
    // Pin DuckLake's own rewrites of the chunk table to the row-group the chunk
    // writer uses (see CHUNK_ROW_GROUP_SIZE).
    conn.execute_batch(&format!(
        "CALL qiita_lake.set_option('parquet_row_group_size', {CHUNK_ROW_GROUP_SIZE}, \
         table_name => 'assembled_sequence_chunks');"
    ))?;
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
        // This helper mirrors the production BOOT connection (main.rs): it creates
        // the tables AND sets the catalog-global Parquet options once. connect_ducklake
        // no longer does. The options persist in the shared Postgres catalog, so
        // per-request connections (flight_service, in tests as in production) inherit
        // them and correctly do NOT re-set them.
        set_catalog_options(&conn).expect("failed to set catalog options");
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

    /// `reference_annotation` is the first `qiita_lake` table with a MAP column, and
    /// the only one a producer writes as a ZERO-ROW file on the common path (every
    /// reference ingested without a GFF3 emits one). Both properties are load-bearing
    /// and neither is exercised anywhere else, so pin them here — against the real
    /// DDL rather than a copy of it.
    ///
    /// The zero-row half is not a formality: `register-files` moves EVERY staging
    /// `*.parquet` into the lake, so a no-GFF reference-add registers an empty file on
    /// every single run. If DuckLake rejected that, the annotation work would break
    /// reference ingest for references that have nothing to do with annotations.
    #[test]
    #[serial]
    #[cfg(feature = "integration")]
    fn reference_annotation_accepts_a_map_column_and_a_zero_row_file() {
        let conn = setup_conn();
        ensure_reference_tables(&conn).unwrap();
        let id = next_test_id();
        let _cleanup = Cleanup {
            conn: &conn,
            table: "reference_annotation",
            column: "reference_idx",
            id,
        };

        // A populated row: the MAP round-trips, and the per-insert mass the cell-count
        // model needs is reachable by key. Columns are named rather than positional —
        // the row carries a minted annotation_idx now, and a positional VALUES list
        // silently shifts every column when one is added.
        conn.execute_batch(&format!(
            "INSERT INTO qiita_lake.reference_annotation \
             (annotation_idx, reference_idx, feature_idx, parent_feature_idx, annotation_id, \
              source, annotation_type, position, stop_position, strand, score, phase, attributes) \
             VALUES \
             (9001, {id}, 7, 42, 'insert_01', 'syndna', 'insert', 2001, 3001, '+', NULL, NULL, \
              MAP{{'ID': 'insert_01', 'mass_ng': '0.5'}});"
        ))
        .unwrap();

        // A zero-row insert, the no-GFF shape: must be a clean no-op, not an error.
        conn.execute_batch(&format!(
            "INSERT INTO qiita_lake.reference_annotation \
             SELECT * FROM qiita_lake.reference_annotation WHERE reference_idx = {id} AND false;"
        ))
        .unwrap();

        let mut stmt = conn
            .prepare(&format!(
                "SELECT annotation_idx, feature_idx, parent_feature_idx, position, stop_position, \
                        attributes['mass_ng'] \
                 FROM qiita_lake.reference_annotation WHERE reference_idx = {id}"
            ))
            .unwrap();
        let (annotation_idx, feature_idx, parent, position, stop, mass): (
            i64,
            i64,
            i64,
            i64,
            i64,
            String,
        ) = stmt
            .query_row([], |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            })
            .unwrap();
        // The lake row's join back to its Postgres claim, and to the annotation's
        // semantic terms. Minted by the control plane; the data plane never derives it.
        assert_eq!(annotation_idx, 9001);
        assert_eq!(feature_idx, 7);
        assert_eq!(parent, 42);
        // Half-open: a 1000 bp insert starting at 2001 stops at 3001, not 3000.
        assert_eq!(position, 2001);
        assert_eq!(stop, 3001);
        assert_eq!(stop - position, 1000);
        assert_eq!(mass, "0.5");
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

    /// ensure_alignment_tables is idempotent (CREATE TABLE IF NOT EXISTS, run on
    /// every DP restart) and lays down the alignment sink in the EXACT column
    /// order + types align_sharded's COPY writes, so register-files'
    /// ducklake_add_data_files schema-matches. The full column list is pinned
    /// here so a drift from the align_sharded output or the miint SAM
    /// schema is caught at unit time rather than at register-files runtime.
    #[test]
    #[serial]
    #[cfg(feature = "integration")]
    fn ensure_alignment_tables_is_idempotent_and_matches_align_output() {
        let conn = setup_conn();
        ensure_alignment_tables(&conn).expect("first ensure_alignment_tables");
        ensure_alignment_tables(&conn).expect("second ensure_alignment_tables (idempotent)");

        let mut stmt = conn
            .prepare(
                "SELECT column_name, data_type FROM information_schema.columns \
                 WHERE table_name = 'alignment' ORDER BY ordinal_position",
            )
            .unwrap();
        let cols: Vec<(String, String)> = stmt
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        // 5 CP identity columns + the miint SAM columns MINUS the raw subject ids
        // (a.* EXCLUDE (read_id, reference, mate_reference)), in align_sharded COPY
        // order.
        let expected: &[(&str, &str)] = &[
            ("alignment_idx", "BIGINT"),
            ("prep_sample_idx", "BIGINT"),
            ("sequence_idx", "BIGINT"),
            ("feature_idx", "BIGINT"),
            ("mate_feature_idx", "BIGINT"),
            ("flags", "USMALLINT"),
            ("position", "BIGINT"),
            ("stop_position", "BIGINT"),
            ("mapq", "UTINYINT"),
            ("cigar", "VARCHAR"),
            ("mate_position", "BIGINT"),
            ("template_length", "BIGINT"),
            ("tag_as", "BIGINT"),
            ("tag_xs", "BIGINT"),
            ("tag_ys", "BIGINT"),
            ("tag_xn", "BIGINT"),
            ("tag_xm", "BIGINT"),
            ("tag_xo", "BIGINT"),
            ("tag_xg", "BIGINT"),
            ("tag_nm", "BIGINT"),
            ("tag_yt", "VARCHAR"),
            ("tag_md", "VARCHAR"),
            ("tag_sa", "VARCHAR"),
        ];
        let got: Vec<(&str, &str)> = cols.iter().map(|(n, t)| (n.as_str(), t.as_str())).collect();
        assert_eq!(got, expected, "alignment table schema/order drift");
    }

    #[test]
    #[serial]
    #[cfg(feature = "integration")]
    fn ensure_assembly_tables_is_idempotent() {
        // Re-running on every DP restart must be a no-op (CREATE TABLE IF NOT
        // EXISTS), and every table must exist and be queryable afterwards.
        let conn = setup_conn();
        ensure_assembly_tables(&conn).expect("first ensure_assembly_tables");
        ensure_assembly_tables(&conn).expect("second ensure_assembly_tables (idempotent)");

        for table in [
            "assembled_sequence",
            "assembled_sequence_chunks",
            "assembly_membership",
            "bin_quality",
        ] {
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
