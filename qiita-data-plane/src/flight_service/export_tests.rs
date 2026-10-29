//! Unit tests for [`super`].

use super::*;

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
