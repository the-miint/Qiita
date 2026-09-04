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
//! DuckLake does not support UNIQUE or FK constraints, so data integrity
//! (no duplicate feature_idx, valid reference_idx) is enforced programmatically.
//! Mostly before insertion — the control plane owns dedup, the orchestrator
//! verifies before loading — but not entirely: a feature is shared across
//! producers, so no producer can know whether the lake already holds it. That
//! part is enforced AT insertion, by the data plane
//! (`flight_service::REPLACE_KEY_TABLES`).

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
/// constraints. Data integrity is enforced upstream, plus one rule at register
/// time that no upstream producer is in a position to enforce:
/// - The control plane deduplicates features by sequence_hash before minting feature_idx
/// - The orchestrator verifies data before loading
/// - The data plane validates identifier sets programmatically on DoAction
/// - `reference_sequences` / `reference_sequence_chunks` are additionally
///   REPLACED on `feature_idx` at register time — `flight_service::REPLACE_KEY_TABLES`
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

/// Create the read + read_mask tables and the read_masked macro in DuckLake.
///
/// These hold per-prep_sample sequencing reads and the downstream masks that record,
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
/// Flight-reachable read surface is the `read_masked` MACRO, which joins read to
/// read_mask, applies the recorded trims, and excludes every non-`pass` row
/// (host/human hits + QC failures) via an unconditional `reason = 'pass'`. Human
/// reads are therefore unreachable by construction, not by a scope check. What
/// its required (mask, prep_samples) parameters foreclose is on the macro itself,
/// below.
pub fn ensure_read_tables(conn: &Connection) -> Result<(), Box<dyn std::error::Error>> {
    conn.execute_batch(
        "-- Full reads, written ONCE per `sequenced_sample`, the 1:1
        -- processing_kind = 'sequenced' subtype of prep_sample. Independent of
        -- any mask.
        -- Keyed by prep_sample_idx + the globally-unique sequence_idx (the read
        -- join key). qual1/qual2 are PHRED scores as UTINYINT arrays; NULL for
        -- FASTA (qual1) or single-end (sequence2/qual2). The producer writes the
        -- Parquet sorted by (prep_sample_idx, sequence_idx) — the view's join
        -- key — for row-group pruning.
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
        -- read surface.
        --
        -- A MACRO, not a view, and the parameters are the point. DuckDB derives a
        -- transitive predicate across a join equality for `col = const` but NOT for
        -- `col IN (list)`, so a view can only ever receive a multi-prep_sample scope on
        -- ONE side of this join: the `read` scan got no filter at all, DuckLake
        -- pruned nothing, and every file in the lake was read. Taking the scope as
        -- a parameter puts it on BOTH inputs explicitly instead of hoping the
        -- optimizer propagates it.
        --
        -- Upstream cause, traced 2026-08-03 and NOT reported as of that date (no
        -- issue number to cite yet — file it against duckdb/duckdb, not ducklake:
        -- the reproducer needs no DuckLake). DuckDB's filter pull-up
        -- (`src/optimizer/filter_combiner.cpp`) mirrors only comparison
        -- expressions: `FilterCombiner::AddFilter` returns UNSUPPORTED for
        -- anything that is not a comparison/BETWEEN, and `SupportedFilterComparison`
        -- omits COMPARE_IN, so an IN or an OR-of-equalities lands in
        -- `remaining_filters` and is emitted verbatim, never entering the
        -- equivalence-set maps that do the mirroring. Still present on main after
        -- the join-filter-mirroring work (duckdb PR #23009), so this is not a
        -- version we can wait out. Note a CONTIGUOUS integer IN list is rewritten
        -- to a range and DOES mirror — any reproducer must use a SPARSE list.
        --
        -- Measured on DuckDB 1.5.4 against a local DuckLake of 1,000,000 `read`
        -- rows over 200 prep_samples, one file per prep_sample (the layout fastq_to_parquet
        -- writes), 10% of rows non-'pass'. Query is a realistic block — one partial
        -- head prep_sample, 18 complete, one partial tail — selecting 84,600 rows.
        -- Figures are rows the scans actually produced (EXPLAIN ANALYZE):
        --
        --     view    948,999 read +  84,600 read_mask = 1,033,599
        --     macro    93,999 read +  84,600 read_mask =   178,599
        --     floor    84,600 read +  84,600 read_mask =   169,200
        --
        -- The macro's `read` figure is 84,600/0.9 to the row, i.e. its entire
        -- residual is the non-'pass' rate and the two partial end prep_samples cost
        -- nothing. In production this shape fully scanned a ~20.7-billion-row
        -- `read`; a single-prep_sample equality scoped the same query to 5,356 rows in
        -- 0.147 s — which is why this went unnoticed. A one-element IN is rewritten
        -- to `=`, so single-prep_sample blocks (every long-read tile) were always fine.
        --
        -- Two rejected alternatives, both measured, so they are not re-proposed:
        -- passing the block's sequence_idx range as further parameters is identical
        -- to the row (once the prep_sample scope prunes to the right per-prep_sample
        -- files the block's global range spans them anyway), and pushing per-member
        -- (prep_sample, range) pairs down as an EXISTS is far WORSE than the view —
        -- 1,900,000 rows, because it defeats file pruning entirely. Hence ONE macro,
        -- with `read_masked_block` reusing it and leaving its member terms an outer
        -- filter.
        --
        -- Required parameters also make an unscoped fleet-wide masked read
        -- UNREPRESENTABLE rather than merely refused — there is no argument list
        -- that means `every prep_sample`, so the macro has no whole-table form to
        -- construct. THIS IS THE ONE SITE THAT ENFORCES THAT, and every other
        -- comment on the subject points here. The control plane's mandatory-filter
        -- invariant (routes/read_masked.py) stays as defence in depth but is no
        -- longer the only thing between a mis-signed ticket and every study's reads.
        --
        -- Needs DuckDB >= 1.5 (we pin libduckdb 1.5.4): on 1.4 this CREATE fails
        -- outright with `DuckLake does not support functions`, so the parameterized
        -- form is not available on an older engine at all.
        --
        -- substr() takes a 1-based start and a LENGTH; list slicing is 1-based and
        -- inclusive on both ends. The qual arrays are guarded for NULL (FASTA /
        -- single-end) symmetrically with their sequence columns.
        --
        -- Trim arithmetic: length() is signed BIGINT, so `length - left - right`
        -- promotes the UINTEGER trims to signed — no unsigned underflow even when
        -- the result is negative. At the exact full-trim boundary
        -- (left+right == length) substr length is 0 -> '' and the slice end < start
        -- -> [], consistently. This ASSUMES the upstream invariant
        -- left_trim+right_trim <= length (enforced upstream at mask-emit time: a
        -- read trimmed below min_length is reason='qc_too_short', never 'pass').
        -- An out-of-contract over-trim row would yield inconsistent bytes; it is
        -- a producer bug, not handled here.
        --
        -- CREATE OR REPLACE (not IF NOT EXISTS): the macro is pure metadata, so a
        -- definition change here is reconciled on every DP startup. IF NOT EXISTS
        -- would silently keep a stale definition on an already-attached catalog —
        -- a privacy-surface footgun (the reason='pass' predicate lives here).
        -- Tables stay IF NOT EXISTS — they hold data.
        --
        -- The DROP VIEW migrates an already-deployed catalog off the view this
        -- macro replaces, and it is NOT what keeps the CREATE below working:
        -- DuckLake keeps views and macros in separate catalog tables, so the two
        -- coexist under one name and BOTH call forms resolve (probed on 1.5.4
        -- against a real DuckLake catalog, either creation order; a table-vs-view
        -- collision does error, so the probe could have failed). That coexistence
        -- is precisely the problem: without the DROP, the old unparameterized view
        -- survives the upgrade and `SELECT * FROM qiita_lake.read_masked` keeps
        -- working unscoped on every catalog that already has one — the exact form
        -- the arguments exist to remove. With it, that call is a Catalog Error and
        -- only the parameterized form remains.
        DROP VIEW IF EXISTS qiita_lake.read_masked;

        CREATE OR REPLACE MACRO qiita_lake.read_masked(p_mask_idx, p_preps) AS TABLE
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
        FROM (SELECT * FROM qiita_lake.read
              WHERE prep_sample_idx IN (SELECT unnest(p_preps))) r
        JOIN (SELECT * FROM qiita_lake.read_mask
              WHERE mask_idx = p_mask_idx
                AND reason = 'pass'
                AND prep_sample_idx IN (SELECT unnest(p_preps))) m
          ON r.prep_sample_idx = m.prep_sample_idx
         AND r.sequence_idx = m.sequence_idx;",
    )?;
    Ok(())
}

/// Create the DuckLake `alignment` table — the sink for the sharded-alignment
/// consumer.
///
/// One row per emitted alignment: the `align_sharded` native job aligns a block
/// of a prep_sample's HOST-DEPLETED reads against a sharded reference and register-files
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
/// alignment_idx + an explicit prep_sample_idx set, and projected to the columns
/// the ticket signed — required on this surface, and drawn from the allowlist
/// `flight_service::ALIGNMENT_PROJECTION_COLUMNS`, which mirrors the column list
/// below. An unscoped or unprojected read is refused. This is host-depleted
/// derived data, not raw human reads.
///
/// The second table, `alignment_origin_spanning`, records which `alignment` rows
/// are one read that crossed a circular contig's origin; its contract is on its
/// DDL below. It is created here rather than by its own `ensure_*` because every
/// `flight_service::ALIGNMENT_DELETE_TABLES` delete drops from both, so a catalog
/// holding one without the other fails those deletes.
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
        );

        -- Evidence that a set of `alignment` rows is one read crossing the origin
        -- of a circular contig. An aligner treats a circular contig as a linear
        -- one, so a read that crosses the origin emits one SAM record per side of
        -- it — more when the read is longer than the contig and wraps past the
        -- origin more than once. Identity and mapq do not separate those records
        -- from any other good alignment; query coverage does, because each covers
        -- only its own share of the read. Upstream documents the behaviour and
        -- ships the aggregate that pools it —
        -- https://the-miint.github.io/duckdb-miint/alignment_analysis/#circular-query-coverage
        -- — and `test_origin_spanning_read_splits_into_one_record_per_side` pins
        -- the per-record numbers our own gate turns on. The fragment rows stay in
        -- `alignment` unchanged, one per SAM record with its CIGAR; only the
        -- merged read is recorded here, and only where the evidence exists.
        --
        -- Scope: this describes rows written by a producer that scores a read the
        -- way miint's `circular_query_coverage` does — pooling the fragments'
        -- QUERY INTERVALS, i.e. the union of the query bases they align over the
        -- read length — because that is what admits the fragments to `alignment`
        -- in the first place. Pooling a CONCATENATED CIGAR is a different number
        -- and does not admit them: each fragment clips the other's share of the
        -- read and that clip re-enters `cigar_query_coverage`'s denominator, so
        -- `string_agg(cigar, '')` over an origin-spanning pair scores what one
        -- fragment alone scores. The concatenating form is the only pooled
        -- query-coverage gate in the repo (`align_sharded`'s phase-2 QUALIFY), so
        -- extending that gate to the single-end arm admits nothing and leaves this
        -- table empty. Identity is unaffected by the same concatenation — a clip
        -- is not an aligned column, so it never enters the denominator.
        --
        -- The sharded reference aligner is not a producer as it stands, either.
        -- `align_sharded`'s phase-1 COPY applies
        -- `_MIN_QUERY_COVERAGE_MINIMAP2` per SAM record on the way into the
        -- staging Parquet, and minimap2 serves only the single-end long-read arm
        -- there (`align_planner._ALIGNER_BY_PLATFORM`), so an origin-spanning
        -- read's fragments are filtered out before they are persisted; the pooled
        -- QUALIFY in its phase 2 runs on the paired-end arm alone.
        --
        -- The producer is `qiita_compute_orchestrator.jobs.align_denovo`, which
        -- aligns a prep_sample's masked reads against its OWN assembled contigs (where a
        -- circular contig is a real, assembler-called thing rather than a claim about
        -- a reference). `register_files` loads its staging
        -- `alignment_origin_spanning.parquet` — the control plane derives the table
        -- name from the file stem. That job records a group only where the
        -- coordinates show a single origin crossing: one fragment reaching the contig
        -- end and one starting at its beginning, and no fragment spanning it end to
        -- end. A read the gate cleared but that split across two loci, or that lapped
        -- the contig, keeps its `alignment` rows and gets no row here — for those the
        -- pair below is not a covering interval.
        --
        -- CONTRACT for such a producer's rows: one row per (read, feature), and a
        -- consumer applying a query-coverage predicate to `alignment` MUST LEFT
        -- JOIN this table on (alignment_idx, prep_sample_idx, sequence_idx,
        -- feature_idx) and judge a matched read on `pooled_coverage` instead —
        -- `alignment` alone shows the read as one row per fragment, each of
        -- partial query coverage, so a per-row floor drops it. feature_idx is in
        -- the key because a read's placements on different features are different
        -- subjects, scored separately; the same reason
        -- `qiita_common.feature_table.PAIRED_PLACEMENT_PARTITION` carries it.
        -- Dropping it pools a fragment onto another feature's score.
        --
        -- That key does NOT separate a secondary from the read's primary placement
        -- on the same feature, and the producer now collects secondaries. A
        -- consumer joining on it would judge such a secondary on the primary's
        -- `pooled_coverage`, which was never computed over it — the macro excludes
        -- secondary records. Exclude them (`alignment_is_secondary(flags)`) before
        -- the join: they are alternative placements of the whole read, scored on
        -- their own CIGAR, and no row here describes one.
        --
        -- Superseded by delete-then-register, like `alignment` itself, not by
        -- `flight_service::REPLACE_KEY_TABLES`: a replace-by-key delete reads the
        -- keys out of the incoming file, and a re-run that no longer finds a read
        -- origin-spanning writes no row for it, so it would name no key and the
        -- stale row would survive its own fragments. `delete_alignment_block`
        -- deletes by footprint instead and so covers reads the new run omits.
        CREATE TABLE IF NOT EXISTS qiita_lake.alignment_origin_spanning (
            -- `alignment`'s leading four, same names, types and order.
            -- alignment_idx / prep_sample_idx / sequence_idx are NOT NULL because
            -- a `flight_service::delete_alignment*` predicate keys on each, and a
            -- NULL is a row no delete can reach. feature_idx is NOT NULL because
            -- it is the contig whose origin the interval below wraps, and because
            -- it completes the join key above.
            alignment_idx   BIGINT NOT NULL,
            prep_sample_idx BIGINT NOT NULL,
            sequence_idx    BIGINT NOT NULL,
            feature_idx     BIGINT NOT NULL,
            -- The merged QUERY interval. A read that wraps the origin still covers
            -- the query contiguously, so one pair describes it.
            query_start     BIGINT,
            query_stop      BIGINT,
            -- The circular REFERENCE interval, on `alignment.position` /
            -- `stop_position`'s axis. feature_start > feature_stop means the
            -- interval wraps the origin. Resolving a wrapped interval to bases
            -- needs the subject's length, which is not duplicated here: join
            -- `sequence_length_bp` on feature_idx in whichever table holds this
            -- producer's subjects — `reference_sequences` when they are reference
            -- features, `assembled_sequence` when they are a prep_sample's own contigs.
            feature_start   BIGINT,
            feature_stop    BIGINT,
            -- Strand of the read relative to the contig — miint's
            -- `alignment_is_reverse` over the fragments' `flags`. Derivable from a
            -- join back to those rows; carried here so this table stands alone,
            -- since reverse is where the interval interpretation flips.
            is_reverse      BOOLEAN,
            -- The pooled scores that admitted the read. Defined as miint's
            -- `circular_query_coverage` defines them, so two producers cannot fill
            -- these columns with different numbers: `pooled_coverage` is its
            -- `coverage` (the fragments' query intervals unioned, over the read
            -- length) and `pooled_identity` is its `identity`, which it takes from
            -- `cigar_pooled_identity` — matching bases over aligned bases across
            -- the fragments, not a mean of their per-fragment identities. The
            -- upstream link in the table note above carries both. Recorded nowhere
            -- else: `alignment` carries the per-fragment CIGARs these are pooled
            -- from, not the pooled result.
            pooled_identity DOUBLE,
            pooled_coverage DOUBLE,
            -- SAM records merged into this row. BIGINT so a producer's count(*)
            -- registers without a cast.
            fragment_count  BIGINT
        );",
    )?;
    Ok(())
}

/// Create the reference-exclusion table and the exclusion-aware `_visible` views.
///
/// `reference_exclusion` is the data-plane mirror of the control-plane blocklist
/// (`qiita.reference_exclusion`): the RESOLVED set of `feature_idx` to exclude
/// (direct feature blocks plus every feature of a blocked genome). The control
/// plane recomputes and ships it WHOLESALE via the `sync_reference_exclusion`
/// DoAction; here it is just a one-column table the views anti-join against.
///
/// `alignment_visible` / `reference_taxonomy_visible` are the ONLY
/// exclusion-aware read surfaces: each is its base table minus every row whose
/// `feature_idx` is blocked (an `ANTI JOIN` — cheap in DuckDB; the build side is
/// a tiny curated list). A blocked genome/feature therefore never reaches an OGU
/// feature table or a taxonomy lineage, even though it stays in the base table:
/// exclusion is read-time only, so no aligner index is rebuilt. Phylogeny is
/// deliberately NOT viewed here — `reference_phylogeny` is not row-independent
/// (a node carries parent/child structure; `feature_idx` is on tips only), so a
/// row-wise anti-join would orphan internal parents and malform the tree. The
/// contract instead is that a tree consumer shears the tree to the keep-set
/// (`tips WHERE feature_idx NOT IN reference_exclusion`) with miint's
/// `shear_tree`, so no phylogeny view is built. The consumer today is the
/// client-side `qiita feature-table build --tree`, which honours that keep-set by
/// reading the blocklist over REST — this being the one read surface that cannot
/// hand it an exclusion-aware view — and intersects it with the genomes its table
/// publishes. Neither set implies the other: a curator can block one contig of a
/// genome that still publishes on a sibling's alignments.
///
/// `CREATE OR REPLACE VIEW` (not `IF NOT EXISTS`), for the same reason the
/// `read_masked` macro is `CREATE OR REPLACE`: the
/// anti-join predicate IS the enforcement surface, so a definition change must
/// reconcile on every DP startup rather than silently keep a stale view on an
/// already-attached catalog. Must run AFTER `ensure_reference_tables` +
/// `ensure_alignment_tables` (the views reference `reference_taxonomy` +
/// `alignment`).
pub fn ensure_exclusion_tables(conn: &Connection) -> Result<(), Box<dyn std::error::Error>> {
    conn.execute_batch(
        "-- Resolved excluded feature_idx set, mirrored wholesale from the CP
        -- blocklist. Same DuckLake constraint story as the sibling tables: no
        -- PK/UNIQUE/FK (the CP owns integrity; a replayed sync is a full replace).
        CREATE TABLE IF NOT EXISTS qiita_lake.reference_exclusion (
            feature_idx BIGINT NOT NULL
        );

        CREATE OR REPLACE VIEW qiita_lake.alignment_visible AS
        SELECT a.*
        FROM qiita_lake.alignment a
        ANTI JOIN qiita_lake.reference_exclusion x USING (feature_idx);

        CREATE OR REPLACE VIEW qiita_lake.reference_taxonomy_visible AS
        SELECT t.*
        FROM qiita_lake.reference_taxonomy t
        ANTI JOIN qiita_lake.reference_exclusion x USING (feature_idx);",
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
/// and in which bin — the DuckLake copy of `qiita.assembly_membership`, for bulk
/// joins against the sequences.
/// `bin_quality` is per-subject CheckM — one row per refined bin, per circular
/// contig, and per unbinned contig above the residue length cut. The DDL below
/// carries its join key and column provenance.
///
/// Same DuckLake constraint story as the read/reference tables: no PK/UNIQUE/FK
/// (the CP mints feature_idx/dedups on sequence_hash, the orchestrator verifies
/// before load, and the data plane replaces on the key at register time).
/// All four are additionally REPLACED at register time — `assembled_sequence` /
/// `assembled_sequence_chunks` on `feature_idx`, `assembly_membership` /
/// `bin_quality` on `(prep_sample_idx, processing_idx)`. The keys and what
/// admits each table are in `flight_service::REPLACE_KEY_TABLES`.
///
/// `assembled_sequence` / `assembled_sequence_chunks` are Flight-readable, and so
/// is `bin_quality` — all three are in `flight_service::ALLOWED_TABLES`, scoped to
/// one `(prep_sample_idx, processing_idx)` run. `assembly_membership` is not: it is
/// a register_files write target, SQL-queryable in the catalog, off the external
/// read-back path, and additionally what resolves the two sequence surfaces' run
/// scope — read by `flight_service::build_assembly_run_query` as a semi join,
/// never streamed.
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
        -- prep_samples AND runs); the `kind` value set is enumerated in
        -- qiita_common.assembly_constants. The DuckLake copy of
        -- qiita.assembly_membership for bulk joins with the sequences.
        --
        -- The four trailing columns are the assembler's own per-contig report,
        -- nullable and NULL for every row written before they existed. Their
        -- meaning is stated once, as COMMENT ON COLUMN on the Postgres twin
        -- qiita.assembly_membership; do not restate it here. The assembler
        -- itself is NOT among them -- it is captured in qiita.processing via
        -- processing_idx, as bin_quality's comment below says of the same field.
        CREATE TABLE IF NOT EXISTS qiita_lake.assembly_membership (
            prep_sample_idx BIGINT NOT NULL,
            processing_idx BIGINT NOT NULL,
            kind VARCHAR NOT NULL,
            bin_id VARCHAR NOT NULL,
            feature_idx BIGINT NOT NULL,
            raw_name VARCHAR,
            circularity VARCHAR,
            depth DOUBLE,
            mult DOUBLE
        );

        -- Per-subject CheckM quality: one row per refined bin (kind MAG), per
        -- circular contig (kind LCG), and per unbinned contig above the length cut
        -- (kind UNBINNED), from the three runs checkm.sh scores separately. The
        -- UNBINNED rows are a SUBSET of the UNBINNED memberships — a contig under
        -- the cut has a membership row and no row here, so the two join LEFT.
        -- Joins to its contigs via assembly_membership on
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
    // `CREATE TABLE IF NOT EXISTS` leaves a lake that already holds
    // assembly_membership at its earlier five columns untouched, and
    // `ducklake_add_data_files` refuses a Parquet carrying a column the target
    // lacks ("Column ... exists in file ... but was not found in table"), so
    // without this every assembly registration into an existing lake fails.
    // Evolving here rather than in a one-shot operator step keeps the boot path
    // the only place the lake schema is defined. The column list is stated twice
    // inside this function -- once in the CREATE for a fresh lake, once here for
    // an existing one -- and a column added to only one of them leaves a deployed
    // lake narrow, which surfaces at the next registration rather than at boot.
    // ensure_assembly_tables_is_idempotent asserts the full nine-column shape, so
    // it fails on the CREATE half; the widening test covers the ALTER half.
    conn.execute_batch(
        "ALTER TABLE qiita_lake.assembly_membership ADD COLUMN IF NOT EXISTS raw_name VARCHAR;
         ALTER TABLE qiita_lake.assembly_membership ADD COLUMN IF NOT EXISTS circularity VARCHAR;
         ALTER TABLE qiita_lake.assembly_membership ADD COLUMN IF NOT EXISTS depth DOUBLE;
         ALTER TABLE qiita_lake.assembly_membership ADD COLUMN IF NOT EXISTS mult DOUBLE;",
    )?;
    // Pin DuckLake's own rewrites of the chunk table to the row-group the chunk
    // writer uses (see CHUNK_ROW_GROUP_SIZE).
    conn.execute_batch(&format!(
        "CALL qiita_lake.set_option('parquet_row_group_size', {CHUNK_ROW_GROUP_SIZE}, \
         table_name => 'assembled_sequence_chunks');"
    ))?;
    Ok(())
}

/// Create the one row that registrations into the replace-keyed tables
/// serialize on, and seed it.
///
/// Why a lock is needed at all, and what it buys, lives at the one site that
/// takes it — `flight_service::register_files`. What matters here is the shape:
/// EXACTLY ONE row, holding a value nothing reads. `register_files` fails loudly
/// if its UPDATE matches no row, because a lock that silently stopped locking
/// reintroduces the duplication this table exists to prevent.
///
/// The seeding INSERT is guarded so a restart does not add a second row. Two data
/// planes booting together could still both insert; that degrades nothing — an
/// extra row is one more thing every writer updates, so they still contend.
pub fn ensure_registration_lock(conn: &Connection) -> Result<(), Box<dyn std::error::Error>> {
    conn.execute_batch(
        "-- One row, ever. `epoch` is a bump counter, not a timestamp: it is never
        -- read, and it is a counter rather than a constant only so the UPDATE that
        -- takes the lock is a real mutation.
        CREATE TABLE IF NOT EXISTS qiita_lake.registration_lock (epoch BIGINT NOT NULL);

        INSERT INTO qiita_lake.registration_lock (epoch)
        SELECT 0 WHERE NOT EXISTS (SELECT 1 FROM qiita_lake.registration_lock);",
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::DataType;
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

    /// `(column_name, data_type, is_nullable)` for one `qiita_lake` table, in
    /// declared order, for the schema-drift assertions below. `is_nullable` is
    /// carried because NOT NULL is a property both alignment DDLs argue for
    /// explicitly; without it, dropping every NOT NULL leaves these tests green.
    fn table_schema(conn: &Connection, table: &str) -> Vec<(String, String, String)> {
        let mut stmt = conn
            .prepare(
                "SELECT column_name, data_type, is_nullable \
                 FROM information_schema.columns \
                 WHERE table_name = ? ORDER BY ordinal_position",
            )
            .unwrap();
        stmt.query_map([table], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
            .unwrap()
            .map(|r| r.unwrap())
            .collect()
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

        // The macro exists, and — the migration half — no view of that name is
        // left behind. A catalog that still carried the view would mean the DROP
        // silently no-op'd, and the next boot would fail on the name collision.
        let macros: i64 = conn
            .query_row(
                "SELECT count(*) FROM duckdb_functions() \
                 WHERE function_name = 'read_masked' AND function_type = 'table_macro'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            macros, 1,
            "read_masked table macro should exist exactly once"
        );
        let views: i64 = conn
            .query_row(
                "SELECT count(*) FROM information_schema.tables \
                 WHERE table_name = 'read_masked'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            views, 0,
            "the superseded read_masked VIEW must be dropped — it can coexist with \
             the macro, and while it exists the unscoped call form still resolves"
        );
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

        let cols = table_schema(&conn, "alignment");
        // 5 CP identity columns + the miint SAM columns MINUS the raw subject ids
        // (a.* EXCLUDE (read_id, reference, mate_reference)), in align_sharded COPY
        // order.
        let expected: &[(&str, &str, &str)] = &[
            ("alignment_idx", "BIGINT", "NO"),
            ("prep_sample_idx", "BIGINT", "NO"),
            ("sequence_idx", "BIGINT", "NO"),
            ("feature_idx", "BIGINT", "NO"),
            ("mate_feature_idx", "BIGINT", "YES"),
            ("flags", "USMALLINT", "YES"),
            ("position", "BIGINT", "YES"),
            ("stop_position", "BIGINT", "YES"),
            ("mapq", "UTINYINT", "YES"),
            ("cigar", "VARCHAR", "YES"),
            ("mate_position", "BIGINT", "YES"),
            ("template_length", "BIGINT", "YES"),
            ("tag_as", "BIGINT", "YES"),
            ("tag_xs", "BIGINT", "YES"),
            ("tag_ys", "BIGINT", "YES"),
            ("tag_xn", "BIGINT", "YES"),
            ("tag_xm", "BIGINT", "YES"),
            ("tag_xo", "BIGINT", "YES"),
            ("tag_xg", "BIGINT", "YES"),
            ("tag_nm", "BIGINT", "YES"),
            ("tag_yt", "VARCHAR", "YES"),
            ("tag_md", "VARCHAR", "YES"),
            ("tag_sa", "VARCHAR", "YES"),
        ];
        let got: Vec<(&str, &str, &str)> = cols
            .iter()
            .map(|(n, t, null)| (n.as_str(), t.as_str(), null.as_str()))
            .collect();
        assert_eq!(got, expected, "alignment table schema/order drift");
    }

    /// `ensure_alignment_tables` also lays down the `alignment_origin_spanning`
    /// side table, idempotently (it runs on every DP restart), with the column
    /// order and types a producer's Parquet must carry to register without a cast.
    /// The identity columns' types are `alignment`'s; the interval columns are on
    /// `alignment.position` / `stop_position`'s axis and share their BIGINT.
    #[test]
    #[serial]
    #[cfg(feature = "integration")]
    fn ensure_alignment_tables_lays_down_alignment_origin_spanning() {
        let conn = setup_conn();
        ensure_alignment_tables(&conn).expect("first ensure_alignment_tables");
        ensure_alignment_tables(&conn).expect("second ensure_alignment_tables (idempotent)");

        let cols = table_schema(&conn, "alignment_origin_spanning");
        let expected: &[(&str, &str, &str)] = &[
            ("alignment_idx", "BIGINT", "NO"),
            ("prep_sample_idx", "BIGINT", "NO"),
            ("sequence_idx", "BIGINT", "NO"),
            ("feature_idx", "BIGINT", "NO"),
            ("query_start", "BIGINT", "YES"),
            ("query_stop", "BIGINT", "YES"),
            ("feature_start", "BIGINT", "YES"),
            ("feature_stop", "BIGINT", "YES"),
            ("is_reverse", "BOOLEAN", "YES"),
            ("pooled_identity", "DOUBLE", "YES"),
            ("pooled_coverage", "DOUBLE", "YES"),
            ("fragment_count", "BIGINT", "YES"),
        ];
        let got: Vec<(&str, &str, &str)> = cols
            .iter()
            .map(|(n, t, null)| (n.as_str(), t.as_str(), null.as_str()))
            .collect();
        assert_eq!(
            got, expected,
            "alignment_origin_spanning schema/order drift"
        );
    }

    /// The join key the `alignment_origin_spanning` DDL states — (alignment_idx,
    /// prep_sample_idx, sequence_idx, feature_idx) — attaches an evidence row to
    /// its own feature's fragments and to nothing else.
    ///
    /// The fixture is the case the key exists for: one read placed on two
    /// features, origin-spanning on the first only. Under the four-column key the
    /// feature-11 row is unmatched and a consumer judges it on its own per-row
    /// coverage; drop feature_idx from the key and it is admitted on the pooled
    /// coverage computed for feature 10, a different subject.
    #[test]
    #[serial]
    #[cfg(feature = "integration")]
    fn origin_spanning_join_key_matches_only_its_own_feature() {
        let conn = setup_conn();
        ensure_alignment_tables(&conn).expect("ensure_alignment_tables");

        // Unique ids so leftover rows never collide with the other serial tests.
        let align: i64 = 970_000;
        let prep: i64 = 970_010;
        let _cleanup = Cleanup {
            conn: &conn,
            table: "alignment",
            column: "alignment_idx",
            id: align,
        };
        let _cleanup_os = Cleanup {
            conn: &conn,
            table: "alignment_origin_spanning",
            column: "alignment_idx",
            id: align,
        };

        conn.execute_batch(&format!(
            "INSERT INTO qiita_lake.alignment \
                 (alignment_idx, prep_sample_idx, sequence_idx, feature_idx, cigar) VALUES \
                 ({align}, {prep}, 1, 10, '3000=3000S'), \
                 ({align}, {prep}, 1, 10, '3000H3000='), \
                 ({align}, {prep}, 1, 11, '3000=3000S');
             INSERT INTO qiita_lake.alignment_origin_spanning \
                 (alignment_idx, prep_sample_idx, sequence_idx, feature_idx, pooled_coverage) \
                 VALUES ({align}, {prep}, 1, 10, 1.0);"
        ))
        .unwrap();

        let matched: Vec<(i64, Option<f64>)> = conn
            .prepare(&format!(
                "SELECT a.feature_idx, os.pooled_coverage \
                 FROM qiita_lake.alignment a \
                 LEFT JOIN qiita_lake.alignment_origin_spanning os \
                   ON os.alignment_idx = a.alignment_idx \
                  AND os.prep_sample_idx = a.prep_sample_idx \
                  AND os.sequence_idx = a.sequence_idx \
                  AND os.feature_idx = a.feature_idx \
                 WHERE a.alignment_idx = {align} \
                 ORDER BY a.feature_idx, a.cigar"
            ))
            .unwrap()
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();

        assert_eq!(
            matched,
            vec![(10, Some(1.0)), (10, Some(1.0)), (11, None)],
            "the four-column key must attach the evidence row to feature 10's two \
             fragments and leave feature 11 unmatched"
        );
    }

    /// ensure_exclusion_tables is idempotent (run on every DP restart) and lays
    /// down the blocklist mirror table plus both `_visible` anti-join views.
    #[test]
    #[serial]
    #[cfg(feature = "integration")]
    fn ensure_exclusion_tables_is_idempotent() {
        let conn = setup_conn();
        ensure_reference_tables(&conn).unwrap();
        ensure_alignment_tables(&conn).unwrap();
        ensure_exclusion_tables(&conn).expect("first ensure_exclusion_tables");
        ensure_exclusion_tables(&conn).expect("second ensure_exclusion_tables (idempotent)");

        for name in [
            "reference_exclusion",
            "alignment_visible",
            "reference_taxonomy_visible",
        ] {
            let mut stmt = conn
                .prepare(&format!(
                    "SELECT count(*) FROM information_schema.tables WHERE table_name = '{name}'"
                ))
                .unwrap();
            let n: i64 = stmt.query_row([], |row| row.get(0)).unwrap();
            assert_eq!(n, 1, "{name} should exist exactly once");
        }
    }

    /// The `_visible` views anti-join the blocklist: a blocked feature_idx is
    /// absent from alignment_visible / reference_taxonomy_visible while the base
    /// tables retain it (read-time exclusion), and an unblocked feature passes
    /// through. Scoped to this test's own alignment_idx / reference_idx so it is
    /// robust against rows other #[serial] tests leave in the shared catalog.
    #[test]
    #[serial]
    #[cfg(feature = "integration")]
    fn visible_views_anti_join_the_blocklist() {
        let conn = setup_conn();
        ensure_reference_tables(&conn).unwrap();
        ensure_alignment_tables(&conn).unwrap();
        ensure_exclusion_tables(&conn).unwrap();

        let alignment_idx = next_test_id();
        let reference_idx = next_test_id();
        let feat_kept = next_test_id();
        let feat_blocked = next_test_id();

        let _c_align = Cleanup {
            conn: &conn,
            table: "alignment",
            column: "alignment_idx",
            id: alignment_idx,
        };
        let _c_tax = Cleanup {
            conn: &conn,
            table: "reference_taxonomy",
            column: "reference_idx",
            id: reference_idx,
        };
        let _c_excl = Cleanup {
            conn: &conn,
            table: "reference_exclusion",
            column: "feature_idx",
            id: feat_blocked,
        };

        conn.execute_batch(&format!(
            "INSERT INTO qiita_lake.alignment
                 (alignment_idx, prep_sample_idx, sequence_idx, feature_idx)
               VALUES ({alignment_idx}, 1, 1, {feat_kept}),
                      ({alignment_idx}, 1, 2, {feat_blocked});
             INSERT INTO qiita_lake.reference_taxonomy (reference_idx, feature_idx)
               VALUES ({reference_idx}, {feat_kept}), ({reference_idx}, {feat_blocked});
             INSERT INTO qiita_lake.reference_exclusion (feature_idx)
               VALUES ({feat_blocked});"
        ))
        .unwrap();

        // Base table keeps both rows — exclusion is read-time, not a delete.
        let base_align: i64 = conn
            .prepare(&format!(
                "SELECT count(*) FROM qiita_lake.alignment WHERE alignment_idx = {alignment_idx}"
            ))
            .unwrap()
            .query_row([], |r| r.get(0))
            .unwrap();
        assert_eq!(base_align, 2, "base alignment retains the blocked row");

        let vis_align: Vec<i64> = {
            let mut stmt = conn
                .prepare(&format!(
                    "SELECT feature_idx FROM qiita_lake.alignment_visible \
                     WHERE alignment_idx = {alignment_idx} ORDER BY feature_idx"
                ))
                .unwrap();
            stmt.query_map([], |r| r.get(0))
                .unwrap()
                .map(|r| r.unwrap())
                .collect()
        };
        assert_eq!(
            vis_align,
            vec![feat_kept],
            "alignment_visible excludes the blocked feature"
        );

        let vis_tax: Vec<i64> = {
            let mut stmt = conn
                .prepare(&format!(
                    "SELECT feature_idx FROM qiita_lake.reference_taxonomy_visible \
                     WHERE reference_idx = {reference_idx} ORDER BY feature_idx"
                ))
                .unwrap();
            stmt.query_map([], |r| r.get(0))
                .unwrap()
                .map(|r| r.unwrap())
                .collect()
        };
        assert_eq!(
            vis_tax,
            vec![feat_kept],
            "reference_taxonomy_visible excludes the blocked feature"
        );
    }

    /// The `_visible` views are catalog-stored, not session-local: a fresh ATTACH
    /// (a real DP restart) sees them WITHOUT re-running ensure_exclusion_tables and
    /// they still anti-join the mirror. Parity with
    /// read_masked_macro_persists_across_reattach.
    #[test]
    #[serial]
    #[cfg(feature = "integration")]
    fn visible_views_persist_across_reattach() {
        let alignment_idx = next_test_id();
        let feat_kept = next_test_id();
        let feat_blocked = next_test_id();

        // Connection 1: ensure tables + views, insert a base + blocklist row.
        {
            let conn1 = setup_conn();
            ensure_reference_tables(&conn1).unwrap();
            ensure_alignment_tables(&conn1).unwrap();
            ensure_exclusion_tables(&conn1).unwrap();
            conn1
                .execute_batch(&format!(
                    "INSERT INTO qiita_lake.alignment
                         (alignment_idx, prep_sample_idx, sequence_idx, feature_idx)
                       VALUES ({alignment_idx}, 1, 1, {feat_kept}),
                              ({alignment_idx}, 1, 2, {feat_blocked});
                     INSERT INTO qiita_lake.reference_exclusion (feature_idx)
                       VALUES ({feat_blocked});"
                ))
                .unwrap();
        }

        // Connection 2: fresh ATTACH, NO ensure_* — the catalog-stored view must
        // already exist and still exclude the blocked feature.
        let conn2 = setup_conn();
        let _c_align = Cleanup {
            conn: &conn2,
            table: "alignment",
            column: "alignment_idx",
            id: alignment_idx,
        };
        let _c_excl = Cleanup {
            conn: &conn2,
            table: "reference_exclusion",
            column: "feature_idx",
            id: feat_blocked,
        };
        let vis: Vec<i64> = {
            let mut stmt = conn2
                .prepare(&format!(
                    "SELECT feature_idx FROM qiita_lake.alignment_visible \
                     WHERE alignment_idx = {alignment_idx} ORDER BY feature_idx"
                ))
                .unwrap();
            stmt.query_map([], |r| r.get(0))
                .unwrap()
                .map(|r| r.unwrap())
                .collect()
        };
        assert_eq!(
            vis,
            vec![feat_kept],
            "alignment_visible persists + anti-joins after reattach"
        );
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

        // The shape assembly_load's COPY must match for ducklake_add_data_files
        // to register without a cast, in that COPY's column order.
        let cols = table_schema(&conn, "assembly_membership");
        let expected: &[(&str, &str, &str)] = &[
            ("prep_sample_idx", "BIGINT", "NO"),
            ("processing_idx", "BIGINT", "NO"),
            ("kind", "VARCHAR", "NO"),
            ("bin_id", "VARCHAR", "NO"),
            ("feature_idx", "BIGINT", "NO"),
            ("raw_name", "VARCHAR", "YES"),
            ("circularity", "VARCHAR", "YES"),
            ("depth", "DOUBLE", "YES"),
            ("mult", "DOUBLE", "YES"),
        ];
        let got: Vec<(&str, &str, &str)> = cols
            .iter()
            .map(|(n, t, null)| (n.as_str(), t.as_str(), null.as_str()))
            .collect();
        assert_eq!(got, expected, "assembly_membership schema/order drift");
    }

    /// A lake created before the attribute columns existed gains them on the next
    /// boot: `CREATE TABLE IF NOT EXISTS` alone would leave it at five columns and
    /// `ducklake_add_data_files` would then reject every membership Parquet.
    #[test]
    #[serial]
    #[cfg(feature = "integration")]
    fn ensure_assembly_tables_widens_a_preexisting_membership_table() {
        let conn = setup_conn();
        conn.execute_batch("DROP TABLE IF EXISTS qiita_lake.assembly_membership;")
            .unwrap();
        // The pre-attribute shape, as a lake deployed before this change holds it.
        conn.execute_batch(
            "CREATE TABLE qiita_lake.assembly_membership (
                prep_sample_idx BIGINT NOT NULL,
                processing_idx BIGINT NOT NULL,
                kind VARCHAR NOT NULL,
                bin_id VARCHAR NOT NULL,
                feature_idx BIGINT NOT NULL
            );",
        )
        .unwrap();
        assert_eq!(table_schema(&conn, "assembly_membership").len(), 5);

        ensure_assembly_tables(&conn).expect("ensure_assembly_tables over a narrow table");

        let names: Vec<String> = table_schema(&conn, "assembly_membership")
            .into_iter()
            .map(|(n, _, _)| n)
            .collect();
        assert_eq!(
            names,
            vec![
                "prep_sample_idx",
                "processing_idx",
                "kind",
                "bin_id",
                "feature_idx",
                "raw_name",
                "circularity",
                "depth",
                "mult",
            ],
            "the four attribute columns must be appended to an existing table"
        );
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
                &format!("SELECT count(*) FROM qiita_lake.read_masked({mask}, [{prep}])"),
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
                 FROM qiita_lake.read_masked({mask}, [{prep}]) \
                 WHERE sequence_idx = {seq_se}"
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
                 FROM qiita_lake.read_masked({mask}, [{prep}]) \
                 WHERE sequence_idx = {seq_pe}"
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
                    "SELECT sequence1, len(qual1) FROM qiita_lake.read_masked({mask}, [{prep}]) \
                     WHERE sequence_idx = {seq_full}"
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
                     FROM qiita_lake.read_masked({mask}, [{prep}]) \
                     WHERE sequence_idx = {seq_full_pe}"
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
                    "SELECT sequence1, len(qual1) FROM qiita_lake.read_masked({mask}, [{prep}]) \
                     WHERE sequence_idx = {seq_zero}"
                ),
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(s_zero, "GGGG", "zero-trim is identity");
        assert_eq!(qlen_zero, 4, "zero-trim keeps all quals");
    }

    /// The read_masked MACRO is stored in the Postgres catalog, not the session: a
    /// fresh ATTACH (a real DP restart) sees it WITHOUT re-running
    /// ensure_read_tables. That is what makes a macro a drop-in for the view it
    /// replaced — same lifecycle, created once at boot — so it is worth pinning on
    /// the Postgres catalog the data plane actually uses.
    #[test]
    #[serial]
    #[cfg(feature = "integration")]
    fn read_masked_macro_persists_across_reattach() {
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
                    "SELECT sequence1 FROM qiita_lake.read_masked({mask}, [{prep}]) \
                     WHERE sequence_idx = {seq}"
                ),
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            s, "ACGT",
            "macro persisted across re-attach (zero-trim identity)"
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

    // --- What DuckDB hands the Flight encoder ---------------------------
    //
    // Structural facts, not measurements: they need a live DuckLake but no
    // production fixtures, and the export path's encoding choices rest on them.

    /// Everything about dictionary encoding follows from this: if the export
    /// never emits one, `DictionaryHandling` is dead config for us and any
    /// dictionary must be built data-plane-side.
    #[test]
    #[serial]
    #[cfg(feature = "integration")]
    fn ducklake_arrow_export_never_emits_dictionary_for_varchar() {
        let conn = setup_conn();
        let id = next_test_id();
        let table = format!("qiita_lake.dict_probe_{id}");
        conn.execute_batch(&format!(
            "CREATE OR REPLACE TABLE {table} AS
             SELECT i AS k, ['Bacteria', 'Archaea'][(i % 2) + 1] AS domain
             FROM range(50000) t(i);"
        ))
        .expect("create probe table");

        let schema = {
            let mut stmt = conn
                .prepare(&format!("SELECT k, domain FROM {table}"))
                .expect("prepare");
            stmt.query_arrow([]).expect("query_arrow").get_schema()
        };
        let _ = conn.execute_batch(&format!("DROP TABLE {table};"));

        let domain = schema.field_with_name("domain").expect("domain column");
        assert!(
            !matches!(domain.data_type(), DataType::Dictionary(..)),
            "DuckDB emitted a dictionary for a 2-distinct VARCHAR: {:?} — \
             dictionary encoding would need re-measuring",
            domain.data_type()
        );
    }

    /// The other half. DuckDB's ENUM is the one type that *should* map to an
    /// Arrow dictionary. If even ENUM does not, the question is closed for good
    /// and nothing we can store will ever arrive dictionary-encoded.
    #[test]
    #[serial]
    #[cfg(feature = "integration")]
    fn ducklake_arrow_export_emits_dictionary_for_enum() {
        let conn = setup_conn();
        let id = next_test_id();
        let enum_type = format!("enum_rank_{id}");
        conn.execute_batch(&format!(
            "CREATE TYPE {enum_type} AS ENUM ('Bacteria', 'Archaea');"
        ))
        .expect("create enum");

        let schema = {
            let mut stmt = conn
                .prepare(&format!(
                    "SELECT 'Bacteria'::{enum_type} AS domain FROM range(10) t(i)"
                ))
                .expect("prepare");
            stmt.query_arrow([]).expect("query_arrow").get_schema()
        };
        let _ = conn.execute_batch(&format!("DROP TYPE {enum_type};"));

        // Recorded either way: this is a fact about DuckDB we are pinning, not a
        // behaviour we require. A change here is a signal to re-measure, which is
        // why the failure message says so rather than just asserting.
        let domain = schema.field_with_name("domain").expect("domain column");
        assert!(
            matches!(domain.data_type(), DataType::Dictionary(..)),
            "DuckDB ENUM no longer maps to an Arrow dictionary (got {:?}) — \
             it did when this was measured; re-measure",
            domain.data_type()
        );
    }

    /// Run-end encoding and delta-style wins depend on rows arriving in the
    /// identifier order the files are written in, and the DoGet applies no
    /// `ORDER BY` — so a parallel scan over several files may interleave.
    ///
    /// Both DoGet shapes are covered because they answer differently: a plain
    /// scan is ordered by `preserve_insertion_order`, but `read_masked` is a
    /// JOIN, and a hash join carries no such guarantee. Production fixtures came
    /// through the join and showed 1-4 inversions; that is the distinction.
    ///
    /// Uses `stream_arrow`, the streaming form `stream_ducklake_batches` uses in
    /// production — the materialising `query_arrow` is a different execution mode
    /// and could order differently.
    #[test]
    #[serial]
    #[cfg(feature = "integration")]
    fn ducklake_parallel_scan_preserves_file_sort_order() {
        let conn = setup_conn();
        let id = next_test_id();
        let table = format!("qiita_lake.order_probe_{id}");
        let side = format!("qiita_lake.order_join_{id}");
        conn.execute_batch(&format!(
            "CREATE OR REPLACE TABLE {table} (grp BIGINT, seq BIGINT);
             CREATE OR REPLACE TABLE {side} (grp BIGINT, seq BIGINT);"
        ))
        .expect("create probe tables");
        // One INSERT per group, so each lands in its own DuckLake file — the
        // layout a partitioned writer produces, and the one a parallel scan can
        // interleave. Large enough that DuckDB actually parallelises.
        const GROUPS: i64 = 16;
        const PER_GROUP: i64 = 100_000;
        for group in 0..GROUPS {
            conn.execute_batch(&format!(
                "INSERT INTO {table} SELECT {group}, i FROM range({}, {}) t(i);
                 INSERT INTO {side} SELECT {group}, i FROM range({}, {}) t(i);",
                group * PER_GROUP,
                (group + 1) * PER_GROUP,
                group * PER_GROUP,
                (group + 1) * PER_GROUP,
            ))
            .expect("insert group");
        }

        let ordered = |sql: &str| -> (usize, usize, usize) {
            let schema = {
                let mut probe = conn
                    .prepare(&format!("SELECT * FROM ({sql}) AS _p LIMIT 0"))
                    .expect("prepare probe");
                probe.query_arrow([]).expect("probe").get_schema()
            };
            let mut stmt = conn.prepare(sql).expect("prepare");
            let stream = stmt.stream_arrow([], schema).expect("stream_arrow");
            let mut seen = Vec::new();
            let mut batches = 0usize;
            for batch in stream {
                batches += 1;
                let grp = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow_array::Int64Array>()
                    .expect("grp is Int64");
                let seq = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<arrow_array::Int64Array>()
                    .expect("seq is Int64");
                for row in 0..batch.num_rows() {
                    seen.push((grp.value(row), seq.value(row)));
                }
            }
            let inversions = seen.windows(2).filter(|w| w[1] < w[0]).count();
            (seen.len(), inversions, batches)
        };

        let scan = ordered(&format!("SELECT grp, seq FROM {table}"));
        let join = ordered(&format!(
            "SELECT l.grp, l.seq FROM {table} l JOIN {side} r
               ON l.grp = r.grp AND l.seq = r.seq"
        ));
        let _ = conn.execute_batch(&format!("DROP TABLE {table}; DROP TABLE {side};"));

        let expected = (GROUPS * PER_GROUP) as usize;
        assert_eq!(scan.0, expected, "scan lost rows");
        assert_eq!(join.0, expected, "join lost rows");
        println!(
            "scan order: scan {} rows / {} inversions / {} batches; \
             join {} rows / {} inversions / {} batches",
            scan.0, scan.1, scan.2, join.0, join.1, join.2
        );

        // Assert on *average run length*, not inversion count: the count scales
        // with row count (1 inversion at 160k rows became 1 at 1.6M for the scan
        // and 323 for the join), whereas run length is scale-free and is the
        // property run-end and delta encodings actually consume. Anything in the
        // thousands is coarse interleaving; row-level scatter would be single
        // digits.
        let run_len = |(rows, inversions, _): (usize, usize, usize)| rows / (inversions + 1);
        assert!(
            run_len(scan) >= 100_000,
            "a plain DuckLake scan is no longer near-ordered ({} rows/run) — check \
             preserve_insertion_order; the order-sensitive encodings depend on it",
            run_len(scan)
        );
        // The join carries no ordering guarantee at all — this is not a promise
        // DuckDB makes, so the bound is deliberately loose and exists to catch a
        // collapse to row-level scatter.
        assert!(
            run_len(join) >= 1_000,
            "the join produced {} rows/run — that is row-level interleaving, not \
             the coarse runs measured against production fixtures",
            run_len(join)
        );
    }
}
