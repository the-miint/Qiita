//! Unit tests for [`super`].

use super::*;
use crate::flight_service::test_support::filter_of;

#[cfg(feature = "integration")]
use std::os::unix::fs::PermissionsExt;

#[cfg(feature = "integration")]
use crate::ducklake;
#[cfg(feature = "integration")]
use crate::flight_service::test_support::attached_conn;

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

/// `count_masked_reads` counts exactly the `read_mask` rows for the target
/// `(prep_sample_idx, mask_idx)` with `reason = 'pass'`: non-`pass` rows (the
/// view's privacy filter), a different mask, and a different prep_sample are
/// all excluded.
#[test]
#[serial_test::serial]
#[cfg(feature = "integration")]
fn count_masked_reads_counts_pass_rows_for_target_only() {
    let connstr = crate::test_catalog::connstr();
    let data_path = crate::test_catalog::data_path();
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
    let n = count_masked_reads(&attached_conn(&connstr, &data_path), prep_a, mask_a)
        .expect("count failed");
    assert_eq!(n, 2);

    // A (prep, mask) pair with no rows counts zero, not an error.
    let none = count_masked_reads(&attached_conn(&connstr, &data_path), prep_b, mask_b)
        .expect("count failed");
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
    let connstr = crate::test_catalog::connstr();
    let data_path = crate::test_catalog::data_path();
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

    let counts = mask_metrics_counts(&attached_conn(&connstr, &data_path), mask_a, prep_a)
        .expect("counts failed");
    assert_eq!(
        counts["row_count"], 5,
        "one row per read/pair for the target"
    );
    assert_eq!(counts["raw"], 8, "5 rows + 3 R2 mates");
    assert_eq!(counts["biological"], 5, "pass + host, both-mates");
    assert_eq!(counts["quality_filtered"], 3, "pass only, both-mates");

    // A (mask, prep) pair with no rows is all-zero, not an error.
    let empty = mask_metrics_counts(&attached_conn(&connstr, &data_path), mask_b, prep_b)
        .expect("counts failed");
    assert_eq!(empty["row_count"], 0);
    assert_eq!(empty["raw"], 0);
    assert_eq!(empty["biological"], 0);
    assert_eq!(empty["quality_filtered"], 0);

    let _ = conn.execute_batch(&format!(
        "DELETE FROM qiita_lake.read_mask WHERE mask_idx IN ({mask_a}, {mask_b});"
    ));
}

/// `export_read_to_parquet` writes one prep_sample's full reads from the DuckLake
/// `read` table to a Parquet drop-in: the 7-col schema with `qual` as
/// UTINYINT[], the seeded rows, mode 0o440. An unknown prep_sample writes NO file
/// and returns 0 (the control plane turns that into a submission failure).
#[test]
#[serial_test::serial]
#[cfg(feature = "integration")]
fn export_read_writes_prep_sample_parquet() {
    let connstr = crate::test_catalog::connstr();
    let data_path = crate::test_catalog::data_path();
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

    let count = export_read_to_parquet(
        &attached_conn(&connstr, &data_path),
        prep,
        &dest,
        dir.path(),
    )
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

    // An unknown prep_sample writes no file and reports 0.
    let dest_absent = dir.path().join("absent.parquet");
    let zero = export_read_to_parquet(
        &attached_conn(&connstr, &data_path),
        absent,
        &dest_absent,
        dir.path(),
    )
    .expect("export of an unknown prep_sample should succeed with 0");
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

// --- pushdown performance assessment helpers/tests ----------------------

/// Write one per-prep_sample `read` Parquet (matching the durable ingest layout:
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
/// Layout mirrors production: one file per prep_sample (`ducklake_add_data_files`
/// of a per-prep_sample Parquet sorted by sequence_idx, small row groups). A fixed
/// 4-file block (one a mid-file split member) is queried after seeding a
/// SMALL then a LARGE set of disjoint filler files; `Total Files Read` must
/// stay == the block's file count both times. Also confirms the shipped
/// `IN + BETWEEN + OR` (V3) prunes exactly as well as `IN + BETWEEN` (V2) and
/// the per-member `OR` alone (V1) — the exactness residual does not defeat
/// file pruning.
#[test]
#[serial_test::serial]
#[cfg(feature = "integration")]
fn block_read_selector_prunes_and_scales() {
    let connstr = crate::test_catalog::connstr();
    let data_path = crate::test_catalog::data_path();
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

    // Fixed block: 4 prep_samples, each seeded as a whole 6000-read file. Member
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
    let member_structs: Vec<auth::BlockReadMember> = members
        .iter()
        .map(|(p, _, s, e)| auth::BlockReadMember {
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
        .map(|(p, _, s, e)| format!("(prep_sample_idx = {p} AND sequence_idx BETWEEN {s} AND {e})"))
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
    eprintln!(
        "[1b] {files_small} prep_sample files total; V3 files read = {v3_small} (block = {block_files})"
    );
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
        "[1b] {files_large} prep_sample files total; files read V1={v1_large} V2={v2_large} V3={v3_large} (block = {block_files})"
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

/// BENCHMARK (post-compaction): DuckLake may compact our per-prep_sample files into
/// one big file sorted by (prep_sample_idx, sequence_idx) — we don't control
/// that ("blind to" compaction). File-level pruning then can't skip the merged
/// file (its prep range spans the block), so efficiency rests on PARQUET
/// ROW-GROUP pruning inside the file. This benchmark seeds one large merged
/// file of INCOMPRESSIBLE rows and times a 4-prep_sample block (and a 1-prep_sample
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
    let connstr = crate::test_catalog::connstr();
    let data_path = crate::test_catalog::data_path();
    let conn = Connection::open_in_memory().unwrap();
    ducklake::connect_ducklake(&conn, &connstr, &data_path).unwrap();
    conn.execute_batch(
        "DROP VIEW IF EXISTS qiita_lake.read_masked; DROP TABLE IF EXISTS qiita_lake.read;",
    )
    .unwrap();
    ducklake::ensure_read_tables(&conn).unwrap();

    let seed_dir = Path::new(&data_path).join("seed_merged");
    std::fs::create_dir_all(&seed_dir).unwrap();

    // ONE file: 100 prep_samples x 30k reads = 3M rows, sorted by (prep, seq),
    // ROW_GROUP_SIZE 25k -> ~120 row groups. sequence1 is ~150 INCOMPRESSIBLE
    // chars (5x md5) so the file is large (I/O real) — a constant string would
    // zstd away to nothing and mask any full-scan-vs-pruned I/O difference.
    let base: i64 = 980_000;
    let reads_per: i64 = 30_000;
    let n_prep_samples: i64 = 100;
    let seq_base: i64 = 8_000_000;
    let file = seed_dir.join("merged.parquet");
    let file_str = file.to_str().unwrap();
    conn.execute_batch(&format!(
        "COPY (SELECT ({base} + (i // {reads_per}))::BIGINT AS prep_sample_idx, \
                ({seq_base} + i)::BIGINT AS sequence_idx, ('r' || i) AS read_id, \
                substr(md5(i::VARCHAR) || md5((i*7)::VARCHAR) || md5((i*13)::VARCHAR) \
                       || md5((i*17)::VARCHAR) || md5((i*19)::VARCHAR), 1, 150) AS sequence1, \
                NULL::UTINYINT[] AS qual1, NULL::VARCHAR AS sequence2, NULL::UTINYINT[] AS qual2 \
             FROM range(0, {n_prep_samples} * {reads_per}) t(i) \
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

    // Block = 4 SCATTERED prep_samples (worst case for row-group locality).
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
    let member_structs: Vec<auth::BlockReadMember> = members
        .iter()
        .map(|(p, s, e)| auth::BlockReadMember {
            prep_sample_idx: *p,
            sequence_idx_start: *s,
            sequence_idx_stop: *e,
        })
        .collect();
    let where_v3 = block_read_where_clause(&member_structs);
    let q = format!("SELECT prep_sample_idx, sequence_idx FROM qiita_lake.read WHERE {where_v3}");

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
