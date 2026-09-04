//! Unit tests for [`super`]. Split out of `flight_service.rs`, which was 9398 lines
//! with 60% of them tests; the module is still a child of
//! `flight_service` via `#[path]`, so it reaches private items through `use super::*`.

use super::*;

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

// --- requested_ipc_codec (pure; no DuckDB) ---

fn metadata_with(value: &[u8]) -> tonic::metadata::MetadataMap {
    let mut map = tonic::metadata::MetadataMap::new();
    map.insert(
        IPC_COMPRESSION_HEADER,
        tonic::metadata::MetadataValue::try_from(value).expect("valid metadata value"),
    );
    map
}

#[test]
fn absent_compression_header_means_no_codec() {
    let map = tonic::metadata::MetadataMap::new();
    assert_eq!(requested_ipc_codec(&map).expect("absent is valid"), None);
}

#[test]
fn zstd_compression_header_selects_zstd() {
    let map = metadata_with(b"zstd");
    assert_eq!(
        requested_ipc_codec(&map).expect("zstd is accepted"),
        Some(CompressionType::ZSTD)
    );
}

#[test]
fn explicit_none_compression_header_means_no_codec() {
    let map = metadata_with(b"none");
    assert_eq!(requested_ipc_codec(&map).expect("none is accepted"), None);
}

/// `lz4` is in this list on purpose: it is rejected by decision, not by
/// omission, so a future reader does not "fix" the gap by accepting it.
#[test]
fn unknown_compression_header_is_rejected() {
    for value in [b"lz4".as_slice(), b"gzip", b"ZSTD", b"", b"zstd,none"] {
        let err = requested_ipc_codec(&metadata_with(value))
            .expect_err("unknown codec must be rejected, not ignored");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(
            err.message().contains("zstd") && err.message().contains("none"),
            "error should name the accepted values, got: {}",
            err.message()
        );
    }
}

/// A repeated header must be refused, not resolved to its first value.
///
/// `MetadataMap::get` returns only the first value of a repeated key, so
/// reading with it would apply `zstd` and silently ignore the `lz4` the client
/// also asked for — a client that requested an unsupported codec and got a
/// working stream anyway learns nothing. Both orders are exercised because
/// only one of them looks wrong under `get`.
#[test]
fn repeated_compression_header_is_rejected() {
    for pair in [["zstd", "lz4"], ["lz4", "zstd"], ["zstd", "zstd"]] {
        let mut map = tonic::metadata::MetadataMap::new();
        for value in pair {
            map.append(
                IPC_COMPRESSION_HEADER,
                tonic::metadata::MetadataValue::try_from(value).expect("valid value"),
            );
        }
        let err = requested_ipc_codec(&map)
            .expect_err("a repeated codec header is ambiguous and must be rejected");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(
            err.message().contains("more than once"),
            "error should say the header repeated, got: {}",
            err.message()
        );
    }
}

/// No codec must leave the encoder options structurally identical to the
/// default the uncompressed encoder used before this existed.
///
/// This is the mechanism behind "no header means today's behaviour byte for
/// byte": `do_get` always routes through `try_with_compression`, so the claim
/// rests on that call being a no-op when the codec is `None`. Compared through
/// `Debug` because `IpcWriteOptions` implements neither `PartialEq` nor field
/// accessors — the derived formatting covers every field, which is the
/// property being pinned.
#[test]
fn no_codec_write_options_match_the_encoder_default() {
    let with_none = IpcWriteOptions::default()
        .try_with_compression(None)
        .expect("None is always a valid codec");
    assert_eq!(
        format!("{with_none:?}"),
        format!("{:?}", IpcWriteOptions::default()),
        "an unrequested codec changed the write options"
    );
}

/// A value the transport accepts but that is not UTF-8 must be an error
/// rather than a panic or a silent fall-through to uncompressed.
#[test]
fn non_utf8_compression_header_is_rejected() {
    let mut map = tonic::metadata::MetadataMap::new();
    map.insert(
        IPC_COMPRESSION_HEADER,
        tonic::metadata::MetadataValue::try_from(b"\xff\xfe".as_slice())
            .expect("bytes are valid as an ASCII metadata value"),
    );
    let err = requested_ipc_codec(&map).expect_err("non-UTF-8 must be rejected");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

// --- single_i64_filter (pure; no DuckDB) ---

fn filter_of(pairs: &[(&str, Vec<serde_json::Value>)]) -> auth::TicketFilter {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect()
}

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

// --- delete_reference integration harness (mirrors ducklake.rs::tests) ---

#[cfg(feature = "integration")]
fn delete_test_catalog_connstr() -> String {
    std::env::var("DUCKLAKE_CATALOG_CONNSTR").unwrap_or_else(|_| {
        "dbname=qiita_ducklake host=localhost port=5433 user=qiita password=qiita".to_string()
    })
}

#[cfg(feature = "integration")]
fn delete_test_data_path() -> String {
    let data_path = std::env::var("PATH_PERSISTENT")
        .map(|base| format!("{base}/ducklake"))
        .unwrap_or_else(|_| "/tmp/qiita-integration-ducklake-data".to_string());
    std::fs::create_dir_all(&data_path).unwrap();
    data_path
}

/// Lay down every lake table the data plane creates at boot, in `main.rs`'s
/// order (exclusion last — its `_visible` views join reference + alignment).
///
/// The catalog-shape tests below decide what belongs in a registry by QUERYING
/// the catalog, so each one is only as complete as the tables its connection
/// happens to hold: a table this function does not create is invisible to the
/// shape query and joins no registry, silently. Adding an `ensure_*` here is
/// what keeps that a one-place edit instead of one per test.
#[cfg(feature = "integration")]
fn setup_full_lake(conn: &duckdb::Connection) {
    ducklake::ensure_reference_tables(conn).unwrap();
    ducklake::ensure_read_tables(conn).unwrap();
    ducklake::ensure_alignment_tables(conn).unwrap();
    ducklake::ensure_assembly_tables(conn).unwrap();
    ducklake::ensure_registration_lock(conn).unwrap();
    ducklake::ensure_exclusion_tables(conn).unwrap();
}

/// Orphan-only sequence deletion: a feature owned by another reference
/// keeps its sequence; a feature owned only by the deleted reference loses
/// it. Reference-scoped tables (membership, taxonomy) drop fully.
#[test]
#[serial_test::serial]
#[cfg(feature = "integration")]
fn delete_reference_drops_orphans_keeps_shared() {
    let connstr = delete_test_catalog_connstr();
    let data_path = delete_test_data_path();
    let conn = Connection::open_in_memory().unwrap();
    ducklake::connect_ducklake(&conn, &connstr, &data_path).unwrap();
    ducklake::ensure_reference_tables(&conn).unwrap();

    // Unique ids so leftover rows never collide with other tests.
    let ref_a: i64 = 910_000;
    let ref_b: i64 = 910_001;
    let shared: i64 = 910_010; // claimed by ref_a AND ref_b
    let orphan: i64 = 910_011; // claimed by ref_a only

    conn.execute_batch(&format!(
        "INSERT INTO qiita_lake.reference_membership VALUES \
             ({ref_a}, {shared}), ({ref_a}, {orphan}), ({ref_b}, {shared});
         INSERT INTO qiita_lake.reference_sequences VALUES \
             ({shared}, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::UUID, 4), \
             ({orphan}, 'b0eebc99-9c0b-4ef8-bb6d-6bb9bd380a22'::UUID, 5);
         INSERT INTO qiita_lake.reference_taxonomy (reference_idx, feature_idx, domain) VALUES \
             ({ref_a}, {shared}, 'd__Bacteria'), ({ref_a}, {orphan}, 'd__Bacteria');"
    ))
    .unwrap();

    let counts = delete_reference(&connstr, &data_path, ref_a).expect("delete_reference failed");
    assert_eq!(counts["sequences_deleted"], 1, "only the orphan sequence");
    assert_eq!(counts["membership_deleted"], 2, "both ref_a memberships");
    assert_eq!(counts["taxonomy_deleted"], 2);

    let remaining_seq = |feature: i64| -> i64 {
        conn.query_row(
            &format!(
                "SELECT count(*) FROM qiita_lake.reference_sequences WHERE feature_idx = {feature}"
            ),
            [],
            |r| r.get(0),
        )
        .unwrap()
    };
    assert_eq!(
        remaining_seq(shared),
        1,
        "shared feature keeps its sequence"
    );
    assert_eq!(remaining_seq(orphan), 0, "orphan feature sequence deleted");

    let ref_a_membership: i64 = conn
        .query_row(
            &format!(
                "SELECT count(*) FROM qiita_lake.reference_membership WHERE reference_idx = {ref_a}"
            ),
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(ref_a_membership, 0);
    let ref_b_membership: i64 = conn
        .query_row(
            &format!(
                "SELECT count(*) FROM qiita_lake.reference_membership WHERE reference_idx = {ref_b}"
            ),
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(ref_b_membership, 1, "ref_b membership untouched");

    // Best-effort cleanup of the surviving shared rows.
    let _ = conn.execute_batch(&format!(
        "DELETE FROM qiita_lake.reference_membership WHERE reference_idx = {ref_b};
         DELETE FROM qiita_lake.reference_sequences WHERE feature_idx = {shared};"
    ));
}

/// `delete_mask` drops exactly the target mask's `read_mask` rows, leaves a
/// different mask untouched, and is idempotent: a second delete of the same
/// mask_idx succeeds and reports `rows_deleted: 0`.
#[test]
#[serial_test::serial]
#[cfg(feature = "integration")]
fn delete_mask_drops_target_idempotently() {
    let connstr = delete_test_catalog_connstr();
    let data_path = delete_test_data_path();
    let conn = Connection::open_in_memory().unwrap();
    ducklake::connect_ducklake(&conn, &connstr, &data_path).unwrap();
    ducklake::ensure_read_tables(&conn).unwrap();

    // Unique ids so leftover rows never collide with other serial tests.
    let mask_a: i64 = 930_000;
    let mask_b: i64 = 930_001;
    let prep: i64 = 930_010;
    let seq1: i64 = 930_020;
    let seq2: i64 = 930_021;

    conn.execute_batch(&format!(
        "DELETE FROM qiita_lake.read_mask WHERE mask_idx IN ({mask_a}, {mask_b});
         INSERT INTO qiita_lake.read_mask \
             (mask_idx, prep_sample_idx, sequence_idx, reason) VALUES \
             ({mask_a}, {prep}, {seq1}, 'pass'), \
             ({mask_a}, {prep}, {seq2}, 'pass'), \
             ({mask_b}, {prep}, {seq1}, 'pass');"
    ))
    .unwrap();

    let first = delete_mask(&connstr, &data_path, mask_a).expect("delete_mask failed");
    assert_eq!(first["rows_deleted"], 2, "both mask_a rows deleted");
    assert_eq!(first["mask_idx"], mask_a);

    let count = |mask: i64| -> i64 {
        conn.query_row(
            &format!("SELECT count(*) FROM qiita_lake.read_mask WHERE mask_idx = {mask}"),
            [],
            |r| r.get(0),
        )
        .unwrap()
    };
    assert_eq!(count(mask_a), 0, "mask_a rows gone");
    assert_eq!(count(mask_b), 1, "mask_b untouched");

    // Idempotency: re-deleting the now-empty mask is success with 0 rows.
    let second = delete_mask(&connstr, &data_path, mask_a).expect("idempotent re-delete failed");
    assert_eq!(second["rows_deleted"], 0, "second delete removes nothing");

    // Best-effort cleanup of the surviving mask_b row.
    let _ = conn.execute_batch(&format!(
        "DELETE FROM qiita_lake.read_mask WHERE mask_idx = {mask_b};"
    ));
}

/// `delete_read_mask_block` deletes EXACTLY one block's footprint: the
/// per-member OR residual keeps a split prep_sample's sibling-block sub-range, the
/// `mask_idx` scope keeps a different mask's rows for the same prep_sample, and a
/// re-delete is an idempotent 0-row noop (the self-cleaning re-run guarantee).
#[test]
#[serial_test::serial]
#[cfg(feature = "integration")]
fn delete_read_mask_block_deletes_footprint_only() {
    let connstr = delete_test_catalog_connstr();
    let data_path = delete_test_data_path();
    let conn = Connection::open_in_memory().unwrap();
    ducklake::connect_ducklake(&conn, &connstr, &data_path).unwrap();
    ducklake::ensure_read_tables(&conn).unwrap();

    // Unique ids so leftover rows never collide with other serial tests.
    let mask_a: i64 = 940_000;
    let mask_b: i64 = 940_001;
    let prep_a: i64 = 940_010;
    let prep_b: i64 = 940_011;

    // mask_a/prep_a is a SPLIT prep_sample: block 1 owns seq 100-101, block 2 owns
    // seq 102-103. mask_a/prep_b (seq 200-201) is whole in block 1. mask_b's
    // row for prep_a (seq 100) is a different filtering identity.
    conn.execute_batch(&format!(
        "DELETE FROM qiita_lake.read_mask WHERE mask_idx IN ({mask_a}, {mask_b});
         INSERT INTO qiita_lake.read_mask \
             (mask_idx, prep_sample_idx, sequence_idx, reason) VALUES \
             ({mask_a}, {prep_a}, 100, 'pass'), \
             ({mask_a}, {prep_a}, 101, 'pass'), \
             ({mask_a}, {prep_a}, 102, 'pass'), \
             ({mask_a}, {prep_a}, 103, 'pass'), \
             ({mask_a}, {prep_b}, 200, 'pass'), \
             ({mask_a}, {prep_b}, 201, 'pass'), \
             ({mask_b}, {prep_a}, 100, 'pass');"
    ))
    .unwrap();

    // Block 1's footprint: prep_a[100,101] (its half of the split) + prep_b
    // whole. block_min=100, block_max=201 spans prep_a's 102-103 too, so the
    // per-member OR is what keeps block 2's sub-range intact.
    let members = vec![
        auth::BlockReadMember {
            prep_sample_idx: prep_a,
            sequence_idx_start: 100,
            sequence_idx_stop: 101,
        },
        auth::BlockReadMember {
            prep_sample_idx: prep_b,
            sequence_idx_start: 200,
            sequence_idx_stop: 201,
        },
    ];

    let first = delete_read_mask_block(&connstr, &data_path, mask_a, &members)
        .expect("delete_read_mask_block failed");
    assert_eq!(
        first["rows_deleted"], 4,
        "block 1's 4 footprint rows deleted"
    );
    assert_eq!(first["mask_idx"], mask_a);

    let count = |mask: i64, prep: i64| -> i64 {
        conn.query_row(
            &format!(
                "SELECT count(*) FROM qiita_lake.read_mask \
                 WHERE mask_idx = {mask} AND prep_sample_idx = {prep}"
            ),
            [],
            |r| r.get(0),
        )
        .unwrap()
    };
    // Block 2's sub-range of the split prep_sample survives (per-member OR exact).
    assert_eq!(
        count(mask_a, prep_a),
        2,
        "prep_a 102-103 (block 2) untouched"
    );
    // prep_b's whole prep_sample was in block 1 — fully deleted.
    assert_eq!(count(mask_a, prep_b), 0, "prep_b fully deleted");
    // The different mask's row for the same prep_sample is out of scope.
    assert_eq!(count(mask_b, prep_a), 1, "mask_b untouched");

    // Idempotency: re-deleting the same footprint removes nothing.
    let second = delete_read_mask_block(&connstr, &data_path, mask_a, &members)
        .expect("idempotent re-delete failed");
    assert_eq!(second["rows_deleted"], 0, "second delete removes nothing");

    let _ = conn.execute_batch(&format!(
        "DELETE FROM qiita_lake.read_mask WHERE mask_idx IN ({mask_a}, {mask_b});"
    ));
}

/// `delete_alignment` drops exactly the target alignment_idx's rows from
/// every `ALIGNMENT_DELETE_TABLES` table, leaves a different alignment
/// untouched in each, and is idempotent: a second delete of the same
/// alignment_idx succeeds and reports `rows_deleted: 0`. The alignment twin of
/// `delete_mask_drops_target_idempotently`.
///
/// The side table carries fewer rows than `alignment` here (only one of the
/// two reads is origin-spanning), so `rows_deleted` cannot be satisfied by the
/// side table's count.
#[test]
#[serial_test::serial]
#[cfg(feature = "integration")]
fn delete_alignment_drops_target_idempotently() {
    let connstr = delete_test_catalog_connstr();
    let data_path = delete_test_data_path();
    let conn = Connection::open_in_memory().unwrap();
    ducklake::connect_ducklake(&conn, &connstr, &data_path).unwrap();
    ducklake::ensure_alignment_tables(&conn).unwrap();

    // Unique ids so leftover rows never collide with other serial tests.
    let align_a: i64 = 960_100;
    let align_b: i64 = 960_101;
    let prep: i64 = 960_110;

    // Only the identity columns are populated — this exercises the delete
    // predicate, and the score columns are not derivable here without an
    // aligner.
    conn.execute_batch(&format!(
        "DELETE FROM qiita_lake.alignment WHERE alignment_idx IN ({align_a}, {align_b});
         DELETE FROM qiita_lake.alignment_origin_spanning \
             WHERE alignment_idx IN ({align_a}, {align_b});
         INSERT INTO qiita_lake.alignment \
             (alignment_idx, prep_sample_idx, sequence_idx, feature_idx) VALUES \
             ({align_a}, {prep}, 1, 10), \
             ({align_a}, {prep}, 2, 11), \
             ({align_b}, {prep}, 1, 10);
         INSERT INTO qiita_lake.alignment_origin_spanning \
             (alignment_idx, prep_sample_idx, sequence_idx, feature_idx) VALUES \
             ({align_a}, {prep}, 1, 10), \
             ({align_b}, {prep}, 1, 10);"
    ))
    .unwrap();

    let first = delete_alignment(&connstr, &data_path, align_a).expect("delete_alignment failed");
    assert_eq!(first["rows_deleted"], 2, "both align_a rows deleted");
    assert_eq!(first["alignment_idx"], align_a);

    let count = |table: &str, align: i64| -> i64 {
        conn.query_row(
            &format!("SELECT count(*) FROM qiita_lake.{table} WHERE alignment_idx = {align}"),
            [],
            |r| r.get(0),
        )
        .unwrap()
    };
    assert_eq!(count("alignment", align_a), 0, "align_a rows gone");
    assert_eq!(count("alignment", align_b), 1, "align_b untouched");
    assert_eq!(
        count("alignment_origin_spanning", align_a),
        0,
        "align_a evidence rows gone"
    );
    assert_eq!(
        count("alignment_origin_spanning", align_b),
        1,
        "align_b evidence untouched"
    );

    // Idempotency: re-deleting the now-empty alignment is success with 0 rows.
    let second =
        delete_alignment(&connstr, &data_path, align_a).expect("idempotent re-delete failed");
    assert_eq!(second["rows_deleted"], 0, "second delete removes nothing");

    let _ = conn.execute_batch(&format!(
        "DELETE FROM qiita_lake.alignment WHERE alignment_idx = {align_b};
         DELETE FROM qiita_lake.alignment_origin_spanning WHERE alignment_idx = {align_b};"
    ));
}

/// `delete_alignment_block` deletes EXACTLY one block's footprint from every
/// `ALIGNMENT_DELETE_TABLES` table: the per-member OR residual keeps a split
/// prep_sample's sibling-block sub-range, the `alignment_idx` scope keeps a
/// different alignment's rows for the same prep_sample, ALL of a read's rows go
/// (multiplicity — a read with two feature_idx rows loses both), and a
/// re-delete is an idempotent 0-row noop. The alignment twin of
/// `delete_read_mask_block_deletes_footprint_only`.
///
/// The side table gets a row in each of the four regions the footprint
/// distinguishes — inside block 1, inside block 2 (still within the coarse
/// `BETWEEN` span), a second prep_sample inside block 1, and a different
/// alignment_idx — so the one clause is shown to select the same reads there
/// as in `alignment`.
#[test]
#[serial_test::serial]
#[cfg(feature = "integration")]
fn delete_alignment_block_deletes_footprint_only() {
    let connstr = delete_test_catalog_connstr();
    let data_path = delete_test_data_path();
    let conn = Connection::open_in_memory().unwrap();
    ducklake::connect_ducklake(&conn, &connstr, &data_path).unwrap();
    ducklake::ensure_alignment_tables(&conn).unwrap();

    // Unique ids so leftover rows never collide with other serial tests.
    let align_a: i64 = 960_200;
    let align_b: i64 = 960_201;
    let prep_a: i64 = 960_210;
    let prep_b: i64 = 960_211;

    // align_a/prep_a is a SPLIT prep_sample: block 1 owns seq 100-101, block 2 owns
    // seq 102-103. seq 100 has TWO rows (feature 10 + 11 — a read aligned to
    // two shards' features), exercising the feature_idx-agnostic multiplicity
    // delete. align_a/prep_b (seq 200-201) is whole in block 1. align_b's row
    // for prep_a (seq 100) is a different align-config identity.
    //
    // The side table is one row per origin-spanning READ, so it has no
    // multiplicity twin for seq 100. Identity columns only, as in
    // `delete_alignment_drops_target_idempotently`.
    conn.execute_batch(&format!(
        "DELETE FROM qiita_lake.alignment WHERE alignment_idx IN ({align_a}, {align_b});
         DELETE FROM qiita_lake.alignment_origin_spanning \
             WHERE alignment_idx IN ({align_a}, {align_b});
         INSERT INTO qiita_lake.alignment \
             (alignment_idx, prep_sample_idx, sequence_idx, feature_idx) VALUES \
             ({align_a}, {prep_a}, 100, 10), \
             ({align_a}, {prep_a}, 100, 11), \
             ({align_a}, {prep_a}, 101, 10), \
             ({align_a}, {prep_a}, 102, 10), \
             ({align_a}, {prep_a}, 103, 10), \
             ({align_a}, {prep_b}, 200, 10), \
             ({align_a}, {prep_b}, 201, 10), \
             ({align_b}, {prep_a}, 100, 10);
         INSERT INTO qiita_lake.alignment_origin_spanning \
             (alignment_idx, prep_sample_idx, sequence_idx, feature_idx) VALUES \
             ({align_a}, {prep_a}, 100, 10), \
             ({align_a}, {prep_a}, 103, 10), \
             ({align_a}, {prep_b}, 200, 10), \
             ({align_b}, {prep_a}, 100, 10);"
    ))
    .unwrap();

    // Block 1's footprint: prep_a[100,101] (its half of the split) + prep_b
    // whole. block_min=100, block_max=201 spans prep_a's 102-103 too, so the
    // per-member OR is what keeps block 2's sub-range intact.
    let members = vec![
        auth::BlockReadMember {
            prep_sample_idx: prep_a,
            sequence_idx_start: 100,
            sequence_idx_stop: 101,
        },
        auth::BlockReadMember {
            prep_sample_idx: prep_b,
            sequence_idx_start: 200,
            sequence_idx_stop: 201,
        },
    ];

    let first = delete_alignment_block(&connstr, &data_path, align_a, &members)
        .expect("delete_alignment_block failed");
    // 3 rows for prep_a[100,101] (two at seq 100 + one at 101) + 2 for prep_b.
    assert_eq!(
        first["rows_deleted"], 5,
        "block 1's 5 footprint rows deleted"
    );
    assert_eq!(first["alignment_idx"], align_a);

    let count = |table: &str, align: i64, prep: i64| -> i64 {
        conn.query_row(
            &format!(
                "SELECT count(*) FROM qiita_lake.{table} \
                 WHERE alignment_idx = {align} AND prep_sample_idx = {prep}"
            ),
            [],
            |r| r.get(0),
        )
        .unwrap()
    };
    // Block 2's sub-range of the split prep_sample survives (per-member OR exact).
    assert_eq!(
        count("alignment", align_a, prep_a),
        2,
        "prep_a 102-103 (block 2) untouched"
    );
    // prep_b's whole prep_sample was in block 1 — fully deleted.
    assert_eq!(
        count("alignment", align_a, prep_b),
        0,
        "prep_b fully deleted"
    );
    // The different alignment's row for the same prep_sample is out of scope.
    assert_eq!(count("alignment", align_b, prep_a), 1, "align_b untouched");

    // The same three properties on the side table. prep_a's surviving row is
    // seq 103 — inside the coarse BETWEEN 100 AND 201 span, so only the
    // per-member OR residual keeps it.
    assert_eq!(
        count("alignment_origin_spanning", align_a, prep_a),
        1,
        "prep_a 103 (block 2) evidence untouched"
    );
    assert_eq!(
        count("alignment_origin_spanning", align_a, prep_b),
        0,
        "prep_b evidence fully deleted"
    );
    assert_eq!(
        count("alignment_origin_spanning", align_b, prep_a),
        1,
        "align_b evidence untouched"
    );

    // Idempotency: re-deleting the same footprint removes nothing.
    let second = delete_alignment_block(&connstr, &data_path, align_a, &members)
        .expect("idempotent re-delete failed");
    assert_eq!(second["rows_deleted"], 0, "second delete removes nothing");

    let _ = conn.execute_batch(&format!(
        "DELETE FROM qiita_lake.alignment WHERE alignment_idx IN ({align_a}, {align_b});
         DELETE FROM qiita_lake.alignment_origin_spanning \
             WHERE alignment_idx IN ({align_a}, {align_b});"
    ));
}

/// `delete_alignment_sample` deletes EXACTLY one `(alignment_idx,
/// prep_sample_idx)` pair's rows from every `ALIGNMENT_DELETE_TABLES` table.
/// Both halves of the pair are pinned: a sibling prep_sample under the SAME
/// alignment_idx survives (so the delete is not `delete_alignment`), and the
/// target prep_sample's rows under a DIFFERENT alignment_idx survive (so it is not
/// keyed on prep_sample_idx alone). All of a read's rows go regardless of
/// feature_idx, and a prep_sample with no rows deletes an idempotent 0.
///
/// Every one of those regions is seeded in the side table too, so the one
/// clause is shown to select the same rows there as in `alignment`.
#[test]
#[serial_test::serial]
#[cfg(feature = "integration")]
fn delete_alignment_sample_deletes_the_pair_only() {
    let connstr = delete_test_catalog_connstr();
    let data_path = delete_test_data_path();
    let conn = Connection::open_in_memory().unwrap();
    ducklake::connect_ducklake(&conn, &connstr, &data_path).unwrap();
    ducklake::ensure_alignment_tables(&conn).unwrap();

    // Unique ids so leftover rows never collide with other serial tests.
    let align_a: i64 = 960_300;
    let align_b: i64 = 960_301;
    let prep_a: i64 = 960_310;
    let prep_b: i64 = 960_311;
    // A prep_sample that never registered rows — the idempotent-zero case.
    let prep_empty: i64 = 960_312;

    // align_a/prep_a is the target: seq 100 has TWO rows (feature 10 + 11 — a
    // read aligned to two shards' features), exercising the feature_idx-agnostic
    // multiplicity delete. align_a/prep_b is a sibling prep_sample under the same
    // align-config identity; align_b/prep_a is the same prep_sample under a different
    // one. Identity columns only, as in the other alignment delete tests.
    conn.execute_batch(&format!(
        "DELETE FROM qiita_lake.alignment WHERE alignment_idx IN ({align_a}, {align_b});
         DELETE FROM qiita_lake.alignment_origin_spanning \
             WHERE alignment_idx IN ({align_a}, {align_b});
         INSERT INTO qiita_lake.alignment \
             (alignment_idx, prep_sample_idx, sequence_idx, feature_idx) VALUES \
             ({align_a}, {prep_a}, 100, 10), \
             ({align_a}, {prep_a}, 100, 11), \
             ({align_a}, {prep_a}, 101, 10), \
             ({align_a}, {prep_b}, 200, 10), \
             ({align_a}, {prep_b}, 201, 10), \
             ({align_b}, {prep_a}, 100, 10);
         INSERT INTO qiita_lake.alignment_origin_spanning \
             (alignment_idx, prep_sample_idx, sequence_idx, feature_idx) VALUES \
             ({align_a}, {prep_a}, 100, 10), \
             ({align_a}, {prep_b}, 200, 10), \
             ({align_b}, {prep_a}, 100, 10);"
    ))
    .unwrap();

    let first = delete_alignment_sample(&connstr, &data_path, align_a, prep_a)
        .expect("delete_alignment_sample failed");
    // Three rows: two at seq 100 (multiplicity) + one at 101.
    assert_eq!(first["rows_deleted"], 3, "the pair's 3 rows deleted");
    assert_eq!(first["alignment_idx"], align_a);

    let count = |table: &str, align: i64, prep: i64| -> i64 {
        conn.query_row(
            &format!(
                "SELECT count(*) FROM qiita_lake.{table} \
                 WHERE alignment_idx = {align} AND prep_sample_idx = {prep}"
            ),
            [],
            |r| r.get(0),
        )
        .unwrap()
    };
    assert_eq!(count("alignment", align_a, prep_a), 0, "the pair is gone");
    assert_eq!(
        count("alignment", align_a, prep_b),
        2,
        "a sibling prep_sample under the SAME alignment survives"
    );
    assert_eq!(
        count("alignment", align_b, prep_a),
        1,
        "the SAME prep_sample under a different alignment survives"
    );

    // The same three properties on the side table.
    assert_eq!(
        count("alignment_origin_spanning", align_a, prep_a),
        0,
        "the pair's evidence is gone"
    );
    assert_eq!(
        count("alignment_origin_spanning", align_a, prep_b),
        1,
        "the sibling prep_sample's evidence survives"
    );
    assert_eq!(
        count("alignment_origin_spanning", align_b, prep_a),
        1,
        "the other alignment's evidence survives"
    );

    // Idempotency: re-deleting the now-empty pair is success with 0 rows.
    let second = delete_alignment_sample(&connstr, &data_path, align_a, prep_a)
        .expect("idempotent re-delete failed");
    assert_eq!(second["rows_deleted"], 0, "second delete removes nothing");

    // A prep_sample that never registered rows is the same 0-row success.
    let never = delete_alignment_sample(&connstr, &data_path, align_a, prep_empty)
        .expect("delete of a prep_sample with no rows failed");
    assert_eq!(
        never["rows_deleted"], 0,
        "a prep_sample with no rows deletes 0"
    );

    let _ = conn.execute_batch(&format!(
        "DELETE FROM qiita_lake.alignment WHERE alignment_idx IN ({align_a}, {align_b});
         DELETE FROM qiita_lake.alignment_origin_spanning \
             WHERE alignment_idx IN ({align_a}, {align_b});"
    ));
}

/// `count_masked_reads` counts exactly the `read_mask` rows for the target
/// `(prep_sample_idx, mask_idx)` with `reason = 'pass'`: non-`pass` rows (the
/// view's privacy filter), a different mask, and a different prep_sample are
/// all excluded.
#[test]
#[serial_test::serial]
#[cfg(feature = "integration")]
fn count_masked_reads_counts_pass_rows_for_target_only() {
    let connstr = delete_test_catalog_connstr();
    let data_path = delete_test_data_path();
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
    let n = count_masked_reads(&connstr, &data_path, prep_a, mask_a).expect("count failed");
    assert_eq!(n, 2);

    // A (prep, mask) pair with no rows counts zero, not an error.
    let none = count_masked_reads(&connstr, &data_path, prep_b, mask_b).expect("count failed");
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
    let connstr = delete_test_catalog_connstr();
    let data_path = delete_test_data_path();
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

    let counts = mask_metrics_counts(&connstr, &data_path, mask_a, prep_a).expect("counts failed");
    assert_eq!(
        counts["row_count"], 5,
        "one row per read/pair for the target"
    );
    assert_eq!(counts["raw"], 8, "5 rows + 3 R2 mates");
    assert_eq!(counts["biological"], 5, "pass + host, both-mates");
    assert_eq!(counts["quality_filtered"], 3, "pass only, both-mates");

    // A (mask, prep) pair with no rows is all-zero, not an error.
    let empty = mask_metrics_counts(&connstr, &data_path, mask_b, prep_b).expect("counts failed");
    assert_eq!(empty["row_count"], 0);
    assert_eq!(empty["raw"], 0);
    assert_eq!(empty["biological"], 0);
    assert_eq!(empty["quality_filtered"], 0);

    let _ = conn.execute_batch(&format!(
        "DELETE FROM qiita_lake.read_mask WHERE mask_idx IN ({mask_a}, {mask_b});"
    ));
}

/// An empty prep_sample set short-circuits: zero counts, no catalog touched
/// (so this needs no DuckLake — runs in the pure-unit tier). Guards against
/// emitting an invalid `IN ()` clause.
#[test]
fn delete_pool_reads_empty_set_is_zero_count_noop() {
    let counts = delete_pool_reads("unused-connstr", "unused-data-path", &[])
        .expect("empty-set delete should succeed without touching the catalog");
    assert_eq!(counts["prep_sample_count"], 0);
    assert_eq!(counts["read_rows_deleted"], 0);
    assert_eq!(counts["read_mask_rows_deleted"], 0);
}

/// `delete_pool_reads` drops exactly the target prep_samples' `read` and
/// `read_mask` rows, leaves another pool's prep_sample untouched, and is
/// idempotent: a second delete of the same set succeeds and reports 0 rows.
#[test]
#[serial_test::serial]
#[cfg(feature = "integration")]
fn delete_pool_reads_drops_target_idempotently() {
    let connstr = delete_test_catalog_connstr();
    let data_path = delete_test_data_path();
    let conn = Connection::open_in_memory().unwrap();
    ducklake::connect_ducklake(&conn, &connstr, &data_path).unwrap();
    ducklake::ensure_read_tables(&conn).unwrap();

    // Unique ids so leftover rows never collide with other serial tests.
    // prep_a / prep_b belong to the deleted pool; prep_other to another.
    let prep_a: i64 = 940_000;
    let prep_b: i64 = 940_001;
    let prep_other: i64 = 940_002;
    let mask: i64 = 940_010;

    conn.execute_batch(&format!(
        "DELETE FROM qiita_lake.read WHERE prep_sample_idx IN ({prep_a}, {prep_b}, {prep_other});
         DELETE FROM qiita_lake.read_mask WHERE prep_sample_idx IN ({prep_a}, {prep_b}, {prep_other});
         INSERT INTO qiita_lake.read (prep_sample_idx, sequence_idx, read_id, sequence1) VALUES \
             ({prep_a}, 1, 'r1', 'ACGT'), \
             ({prep_a}, 2, 'r2', 'TTTT'), \
             ({prep_b}, 3, 'r3', 'GGGG'), \
             ({prep_other}, 4, 'r4', 'CCCC');
         INSERT INTO qiita_lake.read_mask \
             (mask_idx, prep_sample_idx, sequence_idx, reason) VALUES \
             ({mask}, {prep_a}, 1, 'pass'), \
             ({mask}, {prep_b}, 3, 'pass'), \
             ({mask}, {prep_other}, 4, 'pass');"
    ))
    .unwrap();

    let first = delete_pool_reads(&connstr, &data_path, &[prep_a, prep_b])
        .expect("delete_pool_reads failed");
    assert_eq!(first["prep_sample_count"], 2);
    assert_eq!(first["read_rows_deleted"], 3, "prep_a (2) + prep_b (1)");
    assert_eq!(first["read_mask_rows_deleted"], 2, "prep_a + prep_b masks");

    let read_count = |prep: i64| -> i64 {
        conn.query_row(
            &format!("SELECT count(*) FROM qiita_lake.read WHERE prep_sample_idx = {prep}"),
            [],
            |r| r.get(0),
        )
        .unwrap()
    };
    let mask_count = |prep: i64| -> i64 {
        conn.query_row(
            &format!("SELECT count(*) FROM qiita_lake.read_mask WHERE prep_sample_idx = {prep}"),
            [],
            |r| r.get(0),
        )
        .unwrap()
    };
    assert_eq!(read_count(prep_a), 0);
    assert_eq!(read_count(prep_b), 0);
    assert_eq!(read_count(prep_other), 1, "other pool's read untouched");
    assert_eq!(mask_count(prep_other), 1, "other pool's mask untouched");

    // Idempotency: re-deleting the now-empty set is success with 0 rows.
    let second = delete_pool_reads(&connstr, &data_path, &[prep_a, prep_b])
        .expect("idempotent re-delete failed");
    assert_eq!(second["read_rows_deleted"], 0);
    assert_eq!(second["read_mask_rows_deleted"], 0);

    // Best-effort cleanup of the surviving other-pool rows.
    let _ = conn.execute_batch(&format!(
        "DELETE FROM qiita_lake.read WHERE prep_sample_idx = {prep_other};
         DELETE FROM qiita_lake.read_mask WHERE prep_sample_idx = {prep_other};"
    ));
}

// DoGet round-trip for read_masked: drive the exact query path do_get uses
// (build_query → prepare → query_arrow → get_schema → collect, plus the
// empty-result RecordBatch::new_empty branch) against fixture data, and
// assert the UTINYINT[] qual column survives as an Arrow List of UInt8.
// This pins the one read-path behavior the reference tables don't cover
// (they have no list columns): a UTINYINT[] column round-trips through
// query_arrow → Arrow.
#[test]
#[serial_test::serial]
#[cfg(feature = "integration")]
fn read_masked_doget_roundtrips_utinyint_array() {
    use arrow_schema::DataType;

    let connstr = delete_test_catalog_connstr();
    let data_path = delete_test_data_path();
    let conn = Connection::open_in_memory().unwrap();
    ducklake::connect_ducklake(&conn, &connstr, &data_path).unwrap();
    ducklake::ensure_read_tables(&conn).unwrap();

    // Unique ids so leftover rows never collide with other serial tests.
    let prep: i64 = 920_000;
    let mask: i64 = 920_001;
    let seq: i64 = 920_010;

    conn.execute_batch(&format!(
        "DELETE FROM qiita_lake.read WHERE prep_sample_idx = {prep};
         DELETE FROM qiita_lake.read_mask WHERE prep_sample_idx = {prep};
         INSERT INTO qiita_lake.read \
             (prep_sample_idx, sequence_idx, read_id, sequence1, qual1, sequence2, qual2) VALUES \
             ({prep}, {seq}, 'r', 'ACGTAC', [5,6,7,8,9,10]::UTINYINT[], NULL, NULL);
         INSERT INTO qiita_lake.read_mask \
             (mask_idx, prep_sample_idx, sequence_idx, reason, left_trim1, right_trim1) VALUES \
             ({mask}, {prep}, {seq}, 'pass', 1, 1);"
    ))
    .unwrap();

    // Helper that mirrors do_get's query body for read_masked.
    let run = |filter: &auth::TicketFilter| -> Vec<arrow_array::RecordBatch> {
        let (sql, _) = build_query("read_masked", filter, &[], &[]).unwrap();
        let mut stmt = conn.prepare(&sql).unwrap();
        let arrow_result = stmt.query_arrow([]).unwrap();
        let schema = arrow_result.get_schema();
        let batches: Vec<_> = arrow_result.collect();
        if batches.is_empty() {
            vec![arrow_array::RecordBatch::new_empty(schema)]
        } else {
            batches
        }
    };

    // Non-empty: the qual1 column is an Arrow List whose items are UInt8.
    let mut filter = auth::TicketFilter::new();
    filter.insert("mask_idx".to_string(), vec![serde_json::Value::from(mask)]);
    filter.insert(
        "prep_sample_idx".to_string(),
        vec![serde_json::Value::from(prep)],
    );
    let batches = run(&filter);
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 1, "one pass read should round-trip");

    let schema = batches[0].schema();
    let qual1 = schema.field_with_name("qual1").unwrap();
    let item_type = match qual1.data_type() {
        DataType::List(item) | DataType::LargeList(item) => item.data_type().clone(),
        other => panic!("qual1 should be an Arrow List, got: {other:?}"),
    };
    assert_eq!(
        item_type,
        DataType::UInt8,
        "UTINYINT[] must round-trip as a List of UInt8"
    );

    // Empty-result branch: a mask_idx with no rows yields exactly one empty
    // batch carrying the schema (do_get's RecordBatch::new_empty path).
    let mut empty_filter = auth::TicketFilter::new();
    empty_filter.insert(
        "mask_idx".to_string(),
        vec![serde_json::Value::from(mask + 999_999)],
    );
    empty_filter.insert(
        "prep_sample_idx".to_string(),
        vec![serde_json::Value::from(prep)],
    );
    let empty = run(&empty_filter);
    assert_eq!(empty.len(), 1, "empty result still yields one schema batch");
    assert_eq!(empty[0].num_rows(), 0, "the schema batch has no rows");
    assert!(
        empty[0].schema().field_with_name("qual1").is_ok(),
        "empty batch carries the full read_masked schema"
    );

    // Cleanup.
    let _ = conn.execute_batch(&format!(
        "DELETE FROM qiita_lake.read WHERE prep_sample_idx = {prep};
         DELETE FROM qiita_lake.read_mask WHERE prep_sample_idx = {prep};"
    ));
}

// The streaming DoGet helper: drives query_arrow inside a blocking task and
// hands batches back over a bounded channel (do_get's body). Pins that it
// streams every row, preserves the UTINYINT[] -> List<UInt8> shape, and
// emits one empty schema batch for a zero-row result — the same contract
// the old buffered `.collect()` path had, now without buffering the whole
// result set in memory.
#[tokio::test]
#[serial_test::serial]
#[cfg(feature = "integration")]
async fn stream_ducklake_batches_streams_rows_and_empty_schema_branch() {
    use arrow_schema::DataType;

    let connstr = delete_test_catalog_connstr();
    let data_path = delete_test_data_path();

    let prep: i64 = 921_000;
    let mask: i64 = 921_001;
    let (s0, s1, s2) = (prep + 1, prep + 2, prep + 3);
    {
        let conn = Connection::open_in_memory().unwrap();
        ducklake::connect_ducklake(&conn, &connstr, &data_path).unwrap();
        ducklake::ensure_read_tables(&conn).unwrap();
        conn.execute_batch(&format!(
            "DELETE FROM qiita_lake.read WHERE prep_sample_idx = {prep};
             DELETE FROM qiita_lake.read_mask WHERE prep_sample_idx = {prep};
             INSERT INTO qiita_lake.read \
                 (prep_sample_idx, sequence_idx, read_id, sequence1, qual1, sequence2, qual2) VALUES \
                 ({prep}, {s0}, 'r0', 'ACGT', [5,6,7,8]::UTINYINT[], NULL, NULL), \
                 ({prep}, {s1}, 'r1', 'TTGG', [9,9,9,9]::UTINYINT[], NULL, NULL), \
                 ({prep}, {s2}, 'r2', 'CCAA', [3,3,3,3]::UTINYINT[], NULL, NULL);
             INSERT INTO qiita_lake.read_mask \
                 (mask_idx, prep_sample_idx, sequence_idx, reason, left_trim1, right_trim1) VALUES \
                 ({mask}, {prep}, {s0}, 'pass', 0, 0), \
                 ({mask}, {prep}, {s1}, 'pass', 0, 0), \
                 ({mask}, {prep}, {s2}, 'pass', 0, 0);"
        ))
        .unwrap();
    }

    let mut filter = auth::TicketFilter::new();
    filter.insert("mask_idx".to_string(), vec![serde_json::Value::from(mask)]);
    filter.insert(
        "prep_sample_idx".to_string(),
        vec![serde_json::Value::from(prep)],
    );
    let (sql, table) = build_query("read_masked", &filter, &[], &[]).unwrap();
    let batches: Vec<arrow_array::RecordBatch> =
        stream_ducklake_batches(connstr.clone(), data_path.clone(), sql, table)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(|r| r.expect("stream item should be Ok"))
            .collect();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3, "all three pass reads should stream through");
    let qual1 = batches[0]
        .schema()
        .field_with_name("qual1")
        .unwrap()
        .data_type()
        .clone();
    match qual1 {
        DataType::List(item) | DataType::LargeList(item) => {
            assert_eq!(
                item.data_type(),
                &DataType::UInt8,
                "qual1 must be List<UInt8>"
            )
        }
        other => panic!("qual1 should be an Arrow List, got: {other:?}"),
    }

    // Empty-result branch: one zero-row batch carrying the schema.
    let mut empty_filter = auth::TicketFilter::new();
    empty_filter.insert(
        "mask_idx".to_string(),
        vec![serde_json::Value::from(mask + 999_999)],
    );
    empty_filter.insert(
        "prep_sample_idx".to_string(),
        vec![serde_json::Value::from(prep)],
    );
    let (esql, etable) = build_query("read_masked", &empty_filter, &[], &[]).unwrap();
    let empty: Vec<arrow_array::RecordBatch> =
        stream_ducklake_batches(connstr.clone(), data_path.clone(), esql, etable)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(|r| r.expect("empty stream item should be Ok"))
            .collect();
    assert_eq!(empty.len(), 1, "empty result still yields one schema batch");
    assert_eq!(empty[0].num_rows(), 0, "the schema batch has no rows");
    assert!(
        empty[0].schema().field_with_name("qual1").is_ok(),
        "empty batch carries the full read_masked schema"
    );

    let conn = Connection::open_in_memory().unwrap();
    ducklake::connect_ducklake(&conn, &connstr, &data_path).unwrap();
    let _ = conn.execute_batch(&format!(
        "DELETE FROM qiita_lake.read WHERE prep_sample_idx = {prep};
         DELETE FROM qiita_lake.read_mask WHERE prep_sample_idx = {prep};"
    ));
}

// A producer-side error (here, a query against a missing table) must surface
// as a single Err stream item — never a silently-truncated empty stream.
#[tokio::test]
#[serial_test::serial]
#[cfg(feature = "integration")]
async fn stream_ducklake_batches_propagates_query_error() {
    let connstr = delete_test_catalog_connstr();
    let data_path = delete_test_data_path();
    let items: Vec<_> = stream_ducklake_batches(
        connstr,
        data_path,
        "SELECT * FROM qiita_lake.does_not_exist_table".to_string(),
        "qiita_lake.does_not_exist_table".to_string(),
    )
    .collect::<Vec<_>>()
    .await;
    assert_eq!(items.len(), 1, "a producer error yields exactly one item");
    assert!(
        items[0].is_err(),
        "the item must be an Err, not a silent empty stream"
    );
}

// Regression (data-plane lake-file placement): when `register_files`
// moves an externally-produced Parquet into managed lake storage, it must
// NEVER overwrite a file already present there. The reference-load job
// emits fixed basenames (`part_00000.parquet`, `reference_<table>.parquet`),
// so a second registration into the same table targeted the exact path of
// the first load's live, catalog-registered data file. Registered files are
// mode 0440, so on the live host the clobber surfaced as a cryptic EACCES
// ("cross-fs copy failed … Permission denied"); this pins the intended
// behavior independent of the dest's mode: refuse with AlreadyExists and
// leave the existing file byte-for-byte intact. The copy is the data
// plane's responsibility, so the guard lives at the copy primitive.
#[test]
fn move_file_refuses_to_overwrite_existing_dest() {
    let tmp = tempfile::tempdir().unwrap();
    let src = tmp.path().join("src.parquet");
    let dest = tmp.path().join("dest.parquet");
    std::fs::write(&src, b"new load output").unwrap();
    std::fs::write(&dest, b"REGISTERED LAKE DATA").unwrap();

    let err = move_file(&src, &dest)
        .expect_err("move_file must refuse to overwrite an existing destination");
    assert_eq!(
        err.code(),
        tonic::Code::AlreadyExists,
        "clobber must surface as AlreadyExists, not a cryptic permission error"
    );

    // The existing (registered) lake file is untouched ...
    assert_eq!(
        std::fs::read(&dest).unwrap(),
        b"REGISTERED LAKE DATA",
        "existing lake file must not be modified"
    );
    // ... and the source is preserved for diagnosis (the move is refused,
    // not half-applied).
    assert!(
        src.exists(),
        "source must be preserved when the move is refused"
    );
}

// The minted lake filename carries the work ticket (traceability) and is
// unique across loads: the same producer basename registered under two
// different tickets must land at distinct paths, so neither clobbers the
// other in the shared per-table lake dir.
#[test]
fn lake_dest_filename_is_traceable_and_unique_across_tickets() {
    let staging = "/scratch/ticket/27/assembly_load/attempt-0/output";
    let a = lake_dest_filename(27, staging, "part_00000.parquet");
    let b = lake_dest_filename(31, staging, "part_00000.parquet");
    assert!(a.starts_with("wt27-"), "name embeds the work ticket: {a}");
    assert!(
        a.ends_with("-part_00000.parquet"),
        "name preserves the producer basename: {a}"
    );
    assert_ne!(
        a, b,
        "same basename under different tickets must not collide"
    );
    // Deterministic — no randomness, so a resume/retry recomputes the same
    // name and the move_file guard can detect a true double-registration.
    assert_eq!(a, lake_dest_filename(27, staging, "part_00000.parquet"));
    // Distinct basenames within one registration stay distinct (multiple
    // parts and the flat per-table files share a ticket).
    assert_ne!(
        lake_dest_filename(27, staging, "part_00001.parquet"),
        lake_dest_filename(27, staging, "reference_membership.parquet")
    );
}

// Regression: ONE ticket can register twice. A redrive replays a ticket's
// storage tail, so the second load reaches register_files with the same
// work_ticket_idx and the same producer basenames as the first. Keyed on the
// ticket alone, that second load targeted the path the first had already
// registered and move_file refused it with AlreadyExists. The producer's
// re-run puts the second load under a different staging_dir, which is what
// separates them.
#[test]
fn lake_dest_filename_separates_redrives_of_one_ticket() {
    let first = lake_dest_filename(
        6939,
        "/scratch/ticket/6939/assembly_load/attempt-0/output",
        "part_00000.parquet",
    );
    let redrive = lake_dest_filename(
        6939,
        "/scratch/ticket/6939/assembly_load/attempt-1/output",
        "part_00000.parquet",
    );
    assert_ne!(
        first, redrive,
        "a redrive of one ticket must not target the first load's path"
    );
    for name in [&first, &redrive] {
        assert!(name.starts_with("wt6939-"), "still traceable: {name}");
    }
}

// PATH_SCRATCH is host configuration: a migration, a remount, or a
// differently laid-out replacement host changes it without changing which
// registration a staging dir denotes. Keying the digest on the absolute path
// would move the minted name with the mount point and break the determinism
// move_file's guard depends on.
#[test]
fn staging_scope_is_stable_across_a_moved_scratch_root() {
    let rel = "ticket/6939/assembly_load/attempt-1/output";
    let a = staging_scope(&format!("/scratch/{rel}"), std::path::Path::new("/scratch"));
    let b = staging_scope(
        &format!("/mnt/new-scratch/{rel}"),
        std::path::Path::new("/mnt/new-scratch"),
    );
    assert_eq!(a, rel, "the scope is the path relative to the scratch root");
    assert_eq!(a, b, "the scope must not carry the mount point");
    assert_eq!(
        lake_dest_filename(6939, &a, "part_00000.parquet"),
        lake_dest_filename(6939, &b, "part_00000.parquet"),
        "a moved scratch root must not change the minted name"
    );
    // Still separates attempts once the mount point is gone.
    let other_attempt = staging_scope(
        "/scratch/ticket/6939/assembly_load/attempt-0/output",
        std::path::Path::new("/scratch"),
    );
    assert_ne!(a, other_attempt);
    // A staging dir outside the root keeps the full path: distinct and
    // stable, just not portable across a move of whatever holds it.
    let outside = staging_scope("/elsewhere/staging", std::path::Path::new("/scratch"));
    assert_eq!(outside, "/elsewhere/staging");
    // config.rs takes PATH_SCRATCH through a bare PathBuf::from with no
    // normalization, so a host may hand us a trailing slash. strip_prefix
    // compares components, not bytes, so it must not change the answer.
    assert_eq!(
        staging_scope(
            &format!("/scratch/{rel}"),
            std::path::Path::new("/scratch/")
        ),
        rel,
        "a trailing slash on the scratch root must not change the scope"
    );
}

// --- register_files filename validation (pure; no DuckDB) ---

/// `register_files` rejects any filename that could escape the staging dir
/// before it touches the filesystem or the catalog. `payload.files` is
/// Ed25519-signed by the control plane, but this defense-in-depth check keeps
/// the data plane's filesystem contract independent of CP correctness. A
/// `..` (parent) or a rooted/absolute component must be refused; the check
/// runs first, so a bogus connstr/data_path is never reached.
#[test]
fn register_files_rejects_filename_traversal() {
    for bad in [
        "../escape.parquet",
        "/etc/passwd",
        "sub/../../escape.parquet",
    ] {
        let mut files = std::collections::HashMap::new();
        files.insert(bad.to_string(), "reference_membership".to_string());
        let payload = auth::ActionPayload {
            action: "register_files".to_string(),
            staging_dir: "/unused/staging".to_string(),
            files,
            work_ticket_idx: 1,
        };
        let err = register_files(
            "unused-connstr",
            "unused-data-path",
            std::path::Path::new("/"),
            &payload,
        )
        .expect_err("a traversal filename must be rejected");
        assert_eq!(
            err.code(),
            tonic::Code::InvalidArgument,
            "filename {bad:?} must be rejected as invalid, not reach the catalog"
        );
    }
}

// --- replace-by-key (in-memory DuckDB; no DuckLake catalog) ---

/// Every `REPLACE_KEY_TABLES` table appears once, under a non-empty key that
/// names no column twice. A repeated table would build two delete statements
/// for it, and the second would mask a wrong key column in the first by
/// deleting the same rows again. An empty key list would produce
/// `WHERE () IN (SELECT DISTINCT FROM …)`; a repeated column would compare
/// one component of the row constructor against itself.
#[test]
fn replace_key_tables_names_each_table_once() {
    let mut seen = std::collections::HashSet::new();
    for entry in REPLACE_KEY_TABLES {
        let table = entry.table;
        assert!(seen.insert(table), "{table} listed twice");
        assert!(!entry.key.is_empty(), "{table} has no key column");
        let mut seen_keys = std::collections::HashSet::new();
        for key in entry.key {
            assert!(seen_keys.insert(*key), "{table} names {key} twice");
        }
    }
}

/// Every `key_source` is itself a registered table carrying the same key.
/// The borrowing delete SELECTs this entry's key columns out of the source's
/// Parquet, so a source keyed on anything else would name a column that file
/// does not have and fail the whole registration.
#[test]
fn replace_key_tables_borrow_from_a_table_with_the_same_key() {
    for entry in REPLACE_KEY_TABLES {
        let (table, source) = (entry.table, entry.key_source);
        let found = REPLACE_KEY_TABLES
            .iter()
            .find(|candidate| candidate.table == source)
            .unwrap_or_else(|| panic!("{table} borrows keys from unregistered {source}"));
        assert_eq!(
            found.key, entry.key,
            "{table} borrows keys from {source}, which is keyed differently"
        );
    }
}

/// Write a `(feature_idx, chunk_index, chunk_data)` Parquet at `path`. The
/// COPY has no bound-parameter form, so it doubles any quote in the path to
/// escape it — precisely what the delete under test must NOT have to do.
#[cfg(test)]
fn seed_chunk_parquet(conn: &Connection, path: &std::path::Path, rows: &[(i64, i32, &str)]) {
    let values = rows
        .iter()
        .map(|(feature_idx, chunk_index, data)| {
            format!("({feature_idx}::BIGINT, {chunk_index}::INTEGER, '{data}')")
        })
        .collect::<Vec<_>>()
        .join(", ");
    let literal = path.to_str().unwrap().replace('\'', "''");
    conn.execute_batch(&format!(
        "COPY (SELECT * FROM (VALUES {values}) t(feature_idx, chunk_index, chunk_data)) \
         TO '{literal}' (FORMAT PARQUET)"
    ))
    .unwrap();
}

/// The replace-by-key DELETE removes exactly the rows whose key appears in
/// the incoming Parquets — the union across a multi-part table, not one part
/// — leaves every other key alone, and reads each file through a BOUND
/// parameter, so a lake path carrying a single quote is data, not SQL. Runs
/// against a plain in-memory DuckDB with a `qiita_lake` schema: the
/// statement's semantics are the thing under test, not DuckLake.
#[test]
fn replace_key_delete_removes_only_the_incoming_keys() {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        "CREATE SCHEMA qiita_lake;
         CREATE TABLE qiita_lake.assembled_sequence_chunks (
             feature_idx BIGINT NOT NULL,
             chunk_index INTEGER NOT NULL,
             chunk_data VARCHAR NOT NULL
         );
         -- Features 1 and 3 are contigs a second run also produces (1 has 2
         -- chunks); feature 2 belongs to an earlier run only and must survive.
         INSERT INTO qiita_lake.assembled_sequence_chunks VALUES
             (1, 0, 'AAAA'), (1, 1, 'CCCC'), (2, 0, 'GGGG'), (3, 0, 'TTTT');",
    )
    .unwrap();

    // Two parts, and a single quote in one of the names — register_files
    // mints each basename from the (signed, but CP-authored) staging
    // filename, so a path must never be interpolated into the SQL text.
    let dir = tempfile::tempdir().unwrap();
    let part_a = dir.path().join("part'00000.parquet");
    let part_b = dir.path().join("part_00001.parquet");
    seed_chunk_parquet(&conn, &part_a, &[(1, 0, "AAAA"), (1, 1, "CCCC")]);
    seed_chunk_parquet(&conn, &part_b, &[(3, 0, "TTTT")]);

    let sql = replace_key_delete_sql("assembled_sequence_chunks", &["feature_idx"], 2);
    let deleted = conn
        .execute(
            &sql,
            duckdb::params![part_a.to_str().unwrap(), part_b.to_str().unwrap()],
        )
        .expect("read_parquet(?) must accept each path as a bound parameter");
    assert_eq!(
        deleted, 3,
        "both of feature 1's chunk rows plus feature 3's"
    );

    let survivor_feature: i64 = conn
        .query_row(
            "SELECT feature_idx FROM qiita_lake.assembled_sequence_chunks",
            [],
            |r| r.get(0),
        )
        .expect("exactly one row must remain");
    assert_eq!(survivor_feature, 2, "the other run's feature is untouched");
}

/// The composite form matches on the WHOLE key: a lake row agreeing on one
/// component and differing on the other survives. Same in-memory DuckDB as
/// the single-key sibling above — this is the SQL text's semantics, not
/// DuckLake's.
#[test]
fn replace_key_delete_matches_the_whole_composite_key() {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        "CREATE SCHEMA qiita_lake;
         CREATE TABLE qiita_lake.assembly_membership (
             prep_sample_idx BIGINT NOT NULL,
             processing_idx BIGINT NOT NULL,
             kind VARCHAR NOT NULL,
             bin_id VARCHAR NOT NULL,
             feature_idx BIGINT NOT NULL
         );
         -- prep_sample 10 run 20 is the run being re-registered; prep_sample 11
         -- run 20 shares its processing_idx and prep_sample 10 run 21 shares its
         -- prep_sample_idx, so each agrees on one half and must survive.
         INSERT INTO qiita_lake.assembly_membership VALUES
             (10, 20, 'LCG', 'circular_1', 700),
             (10, 20, 'MAG', 'bin.1', 701),
             (11, 20, 'MAG', 'bin.1', 702),
             (10, 21, 'MAG', 'bin.1', 703);",
    )
    .unwrap();

    let dir = tempfile::tempdir().unwrap();
    let incoming = dir.path().join("assembly_membership.parquet");
    conn.execute_batch(&format!(
        "COPY (SELECT * FROM (VALUES \
             (10::BIGINT, 20::BIGINT, 'UNBINNED', 'contig_9', 704::BIGINT)) \
             t(prep_sample_idx, processing_idx, kind, bin_id, feature_idx)) \
         TO '{}' (FORMAT PARQUET)",
        incoming.to_str().unwrap()
    ))
    .unwrap();

    let sql = replace_key_delete_sql(
        "assembly_membership",
        &["prep_sample_idx", "processing_idx"],
        1,
    );
    let deleted = conn
        .execute(&sql, duckdb::params![incoming.to_str().unwrap()])
        .unwrap();
    assert_eq!(deleted, 2, "both of prep_sample 10 / run 20's rows");

    let mut stmt = conn
        .prepare(
            "SELECT prep_sample_idx, processing_idx FROM qiita_lake.assembly_membership \
             ORDER BY prep_sample_idx, processing_idx",
        )
        .unwrap();
    let survivors: Vec<(i64, i64)> = stmt
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(
        survivors,
        vec![(10, 21), (11, 20)],
        "a row agreeing on only one component of the key is not the same run"
    );
}

// --- do_action dispatch trust checks (pure; no DuckDB) ---

/// An action whose `Action.type` header disagrees with the signed
/// `payload.action` is rejected. `verify_action` succeeds (signature + shape are
/// valid), then the handler's discriminator check catches the mismatch — the
/// two must agree so a token minted for one action can't be replayed under a
/// different action header.
#[tokio::test]
async fn do_action_rejects_type_payload_mismatch() {
    let tmp = tempfile::tempdir().unwrap();
    let service = make_service(tmp.path().to_path_buf());
    // Validly-signed register_files-shaped payload, but its action field
    // says delete_reference — sent under the register_files header.
    let payload =
        br#"{"action":"delete_reference","staging_dir":"/unused","files":{},"work_ticket_idx":1}"#;
    let body = sign_raw(payload, &TEST_SEED, future_expiry_secs(300));
    let action = Action {
        r#type: "register_files".to_string(),
        body: body.into(),
    };
    // The success type (a boxed Stream) is not Debug, so `expect_err` won't
    // compile — match instead.
    let err = match service.do_action(Request::new(action)).await {
        Ok(_) => panic!("action-type/payload mismatch must be rejected"),
        Err(e) => e,
    };
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(
        err.message().contains("mismatch"),
        "error should name the mismatch: {}",
        err.message()
    );
}

/// An unrecognized `Action.type` is rejected as invalid rather than silently
/// ignored or dispatched — the dispatcher only ever runs known handlers.
#[tokio::test]
async fn do_action_rejects_unknown_action_type() {
    let tmp = tempfile::tempdir().unwrap();
    let service = make_service(tmp.path().to_path_buf());
    let action = Action {
        r#type: "definitely_not_a_real_action".to_string(),
        body: Vec::<u8>::new().into(),
    };
    let err = match service.do_action(Request::new(action)).await {
        Ok(_) => panic!("unknown action type must be rejected"),
        Err(e) => e,
    };
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

/// The replay-safe registry and the do_action dispatcher must stay in
/// lockstep. Every `REPLAY_SAFE_ACTIONS` entry reaches a real handler — it
/// then fails verifying the empty token body (`Unauthenticated`), NOT the
/// replay guard (`InvalidArgument`) — and an action outside the registry is
/// rejected by the guard. So a new match arm added without a registry entry
/// is unreachable and surfaces the moment it is exercised, forcing a
/// conscious replay classification (see the `# replay:` note in do_action).
#[tokio::test]
async fn replay_safe_actions_matches_dispatcher() {
    let tmp = tempfile::tempdir().unwrap();
    let service = make_service(tmp.path().to_path_buf());

    for name in REPLAY_SAFE_ACTIONS {
        let action = Action {
            r#type: name.to_string(),
            body: Vec::<u8>::new().into(),
        };
        let err = match service.do_action(Request::new(action)).await {
            Ok(_) => panic!("empty-body action {name:?} must fail"),
            Err(e) => e,
        };
        assert_eq!(
            err.code(),
            tonic::Code::Unauthenticated,
            "classified action {name:?} must be dispatched to a handler \
             (fail on token verification), not rejected as unknown"
        );
    }

    // An action absent from the registry is turned away by the replay guard.
    let bogus = Action {
        r#type: "definitely_not_a_real_action".to_string(),
        body: Vec::<u8>::new().into(),
    };
    let err = match service.do_action(Request::new(bogus)).await {
        Ok(_) => panic!("unclassified action must be rejected"),
        Err(e) => e,
    };
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

/// End-to-end `register_files`: seed a Parquet in a staging dir, register it
/// into DuckLake, and assert the file was moved to the lake path
/// and its rows are queryable through the catalog. Exercises the
/// move-then-register path and its wrapping transaction against a real
/// DuckLake catalog.
#[test]
#[serial_test::serial]
#[cfg(feature = "integration")]
fn register_files_moves_and_registers_end_to_end() {
    let connstr = delete_test_catalog_connstr();
    let data_path = delete_test_data_path();

    // Unique ids so leftover rows never collide with other serial tests.
    let ref_idx: i64 = 970_000;
    let feat_a: i64 = 970_010;
    let feat_b: i64 = 970_011;
    // The dest name is minted by `lake_dest_filename`, which keys on this.
    // Derive it from the PID so a manual re-run against a persistent catalog
    // mints a fresh file name instead of colliding with the prior run's
    // still-registered lake file (move_file refuses to overwrite). CI resets
    // the catalog each run, so this only matters for local re-runs.
    let ticket: i64 = 970_000_000 + std::process::id() as i64;

    // Ensure the target table exists, and tombstone any rows a prior local
    // run left behind so the post-register count reflects only this run.
    {
        let conn = Connection::open_in_memory().unwrap();
        ducklake::connect_ducklake(&conn, &connstr, &data_path).unwrap();
        ducklake::ensure_reference_tables(&conn).unwrap();
        ducklake::ensure_registration_lock(&conn).unwrap();
        conn.execute_batch(&format!(
            "DELETE FROM qiita_lake.reference_membership WHERE reference_idx = {ref_idx};"
        ))
        .unwrap();
    }

    // Seed a staging Parquet whose schema matches reference_membership
    // (two BIGINT columns) — written by DuckDB so the types match exactly.
    let staging = tempfile::tempdir().unwrap();
    let src = staging.path().join("reference_membership.parquet");
    let src_str = src.to_str().unwrap();
    {
        let writer = Connection::open_in_memory().unwrap();
        writer
            .execute_batch(&format!(
                "COPY (SELECT * FROM (VALUES \
                     ({ref_idx}::BIGINT, {feat_a}::BIGINT), \
                     ({ref_idx}::BIGINT, {feat_b}::BIGINT)) \
                     t(reference_idx, feature_idx)) \
                 TO '{src_str}' (FORMAT PARQUET)"
            ))
            .unwrap();
    }
    assert!(src.exists(), "staging parquet seeded");

    let mut files = std::collections::HashMap::new();
    files.insert(
        "reference_membership.parquet".to_string(),
        "reference_membership".to_string(),
    );
    let payload = auth::ActionPayload {
        action: "register_files".to_string(),
        staging_dir: staging.path().to_str().unwrap().to_string(),
        files,
        work_ticket_idx: ticket,
    };

    let outcome = register_files(&connstr, &data_path, std::path::Path::new("/"), &payload)
        .expect("register_files failed");
    assert_eq!(outcome.registered.len(), 1, "one file registered");
    assert!(
        outcome.replaced.is_empty(),
        "reference_membership is not a REPLACE_KEY_TABLES target, so nothing is replaced"
    );
    // The dest carries the registration-unique minted name under the
    // per-table dir. Composed through the same `staging_scope` the caller
    // uses — recomputing the scope here by hand would be a second
    // implementation that drifts the moment the derivation changes.
    let dest = std::path::Path::new(&outcome.registered[0]);
    let scope = staging_scope(&payload.staging_dir, std::path::Path::new("/"));
    assert_eq!(
        dest.file_name().and_then(|f| f.to_str()).unwrap(),
        lake_dest_filename(ticket, &scope, "reference_membership.parquet")
    );
    assert!(dest.exists(), "registered lake file present on disk");
    assert!(
        !src.exists(),
        "staging source was moved out, not left behind"
    );

    // The rows are queryable through the catalog via a fresh connection.
    let reader = Connection::open_in_memory().unwrap();
    ducklake::connect_ducklake(&reader, &connstr, &data_path).unwrap();
    let n: i64 = reader
        .query_row(
            &format!(
                "SELECT count(*) FROM qiita_lake.reference_membership \
                 WHERE reference_idx = {ref_idx}"
            ),
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(n, 2, "both seeded membership rows registered");

    // Best-effort cleanup: tombstone the catalog rows only. Do NOT remove
    // the physical lake file — it stays registered in the DuckLake catalog
    // until compaction, and unlinking a still-registered data file breaks
    // any later full-table scan of reference_membership (e.g. the
    // delete_reference orphan subquery) with a missing-file IO error.
    let _ = reader.execute_batch(&format!(
        "DELETE FROM qiita_lake.reference_membership WHERE reference_idx = {ref_idx};"
    ));
}

// --- replace-by-key against a real DuckLake catalog ---

/// Seed one assembly run's staging dir in the shapes `ensure_assembly_tables`
/// declares: `assembled_sequence.parquet`, plus an
/// `assembled_sequence_chunks/` dir holding ONE PART PER CONTIG — the
/// multi-file form `write_feature_sequence_chunks` emits, so the registration
/// under test takes the grouped replace-by-key path rather than a
/// single-file shortcut.
///
/// `contigs` is `(feature_idx, uuid, [chunk_data...])` — chunk_index is the
/// position, and `sequence_length_bp` is the summed chunk length, so a
/// reassembly check against it is meaningful rather than tautological.
#[cfg(feature = "integration")]
fn seed_assembly_staging(contigs: &[(i64, &str, &[&str])]) -> tempfile::TempDir {
    let staging = tempfile::tempdir().unwrap();
    let chunks_dir = staging.path().join("assembled_sequence_chunks");
    std::fs::create_dir_all(&chunks_dir).unwrap();
    let writer = Connection::open_in_memory().unwrap();

    let sequence_rows: Vec<String> = contigs
        .iter()
        .map(|(feature_idx, uuid, chunks)| {
            let length: usize = chunks.iter().map(|c| c.len()).sum();
            format!("({feature_idx}::BIGINT, '{uuid}'::UUID, {length}::BIGINT)")
        })
        .collect();
    let sequences = staging.path().join("assembled_sequence.parquet");
    writer
        .execute_batch(&format!(
            "COPY (SELECT * FROM (VALUES {}) \
                 t(feature_idx, sequence_hash, sequence_length_bp)) \
             TO '{}' (FORMAT PARQUET)",
            sequence_rows.join(", "),
            sequences.to_str().unwrap()
        ))
        .unwrap();

    for (part_index, (feature_idx, _uuid, chunks)) in contigs.iter().enumerate() {
        let rows: Vec<(i64, i32, &str)> = chunks
            .iter()
            .enumerate()
            .map(|(i, data)| (*feature_idx, i as i32, *data))
            .collect();
        seed_chunk_parquet(
            &writer,
            &chunks_dir.join(format!("part_{part_index:05}.parquet")),
            &rows,
        );
    }

    staging
}

#[cfg(feature = "integration")]
fn assembly_register_payload(
    staging: &tempfile::TempDir,
    n_parts: usize,
    ticket: i64,
) -> auth::ActionPayload {
    let mut files = std::collections::HashMap::new();
    files.insert(
        "assembled_sequence.parquet".to_string(),
        "assembled_sequence".to_string(),
    );
    for part_index in 0..n_parts {
        files.insert(
            format!("assembled_sequence_chunks/part_{part_index:05}.parquet"),
            "assembled_sequence_chunks".to_string(),
        );
    }
    auth::ActionPayload {
        action: "register_files".to_string(),
        staging_dir: staging.path().to_str().unwrap().to_string(),
        files,
        work_ticket_idx: ticket,
    }
}

/// One row per feature, whatever the lake already held: a contig two
/// assembly runs both produce collapses to ONE `feature_idx` (shared
/// canonical hash), and each run writes that feature's sequence + chunks in
/// full. Without replace-by-key the second load leaves two copies and
/// `string_agg(chunk_data, '' ORDER BY chunk_index)` returns the contig
/// concatenated with itself while `sequence_length_bp` still describes one.
///
/// Asserts the run-A-only feature survives run B untouched — the replace is
/// scoped to the keys the incoming file carries, not to "this run's rows".
#[test]
#[serial_test::serial]
#[cfg(feature = "integration")]
fn register_files_replaces_shared_contigs_across_assembly_runs() {
    let connstr = delete_test_catalog_connstr();
    let data_path = delete_test_data_path();

    let a_only: i64 = 971_010;
    let shared: i64 = 971_011;
    let b_only: i64 = 971_012;
    let ticket_a: i64 = 971_000_000 + std::process::id() as i64;
    let ticket_b: i64 = ticket_a + 1;

    {
        let conn = Connection::open_in_memory().unwrap();
        ducklake::connect_ducklake(&conn, &connstr, &data_path).unwrap();
        ducklake::ensure_assembly_tables(&conn).unwrap();
        ducklake::ensure_registration_lock(&conn).unwrap();
        conn.execute_batch(&format!(
            "DELETE FROM qiita_lake.assembled_sequence \
               WHERE feature_idx IN ({a_only}, {shared}, {b_only});
             DELETE FROM qiita_lake.assembled_sequence_chunks \
               WHERE feature_idx IN ({a_only}, {shared}, {b_only});"
        ))
        .unwrap();
    }

    // Run A: its own contig plus the one run B will also assemble.
    let staging_a = seed_assembly_staging(&[
        (
            a_only,
            "00000000-0000-0000-0000-000000971010",
            &["ACGTACGT"],
        ),
        (
            shared,
            "00000000-0000-0000-0000-000000971011",
            &["ACGTACGT", "ACGT"],
        ),
    ]);
    let outcome_a = register_files(
        &connstr,
        &data_path,
        std::path::Path::new("/"),
        &assembly_register_payload(&staging_a, 2, ticket_a),
    )
    .expect("run A register_files failed");
    assert!(
        outcome_a.replaced.is_empty(),
        "a fresh feature set replaces nothing, got {:?}",
        outcome_a.replaced
    );

    // Run B: the SAME contig (same feature_idx, same bytes) plus its own.
    let staging_b = seed_assembly_staging(&[
        (
            shared,
            "00000000-0000-0000-0000-000000971011",
            &["ACGTACGT", "ACGT"],
        ),
        (b_only, "00000000-0000-0000-0000-000000971012", &["ACGT"]),
    ]);
    let outcome_b = register_files(
        &connstr,
        &data_path,
        std::path::Path::new("/"),
        &assembly_register_payload(&staging_b, 2, ticket_b),
    )
    .expect("run B register_files failed");
    assert_eq!(
        outcome_b.replaced.get("assembled_sequence").copied(),
        Some(1),
        "run A's copy of the shared contig's sequence row is superseded"
    );
    assert_eq!(
        outcome_b.replaced.get("assembled_sequence_chunks").copied(),
        Some(2),
        "and both of its chunk rows"
    );

    let reader = Connection::open_in_memory().unwrap();
    ducklake::connect_ducklake(&reader, &connstr, &data_path).unwrap();
    for (feature_idx, expected_length) in [(a_only, 8_i64), (shared, 12), (b_only, 4)] {
        let rows: i64 = reader
            .query_row(
                &format!(
                    "SELECT count(*) FROM qiita_lake.assembled_sequence \
                     WHERE feature_idx = {feature_idx}"
                ),
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(rows, 1, "exactly one sequence row for {feature_idx}");

        // The acceptance check: the reassembled bytes are as long as the row
        // that describes them.
        let (declared, reassembled): (i64, i64) = reader
            .query_row(
                &format!(
                    "SELECT s.sequence_length_bp, \
                            length(string_agg(c.chunk_data, '' ORDER BY c.chunk_index)) \
                     FROM qiita_lake.assembled_sequence s \
                     JOIN qiita_lake.assembled_sequence_chunks c USING (feature_idx) \
                     WHERE s.feature_idx = {feature_idx} \
                     GROUP BY s.sequence_length_bp"
                ),
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(declared, expected_length, "declared length {feature_idx}");
        assert_eq!(
            reassembled, declared,
            "reassembled bytes for {feature_idx} must match sequence_length_bp"
        );
    }

    // Tombstone the catalog rows only — the physical lake files stay
    // registered until compaction (see the sibling register test).
    let _ = reader.execute_batch(&format!(
        "DELETE FROM qiita_lake.assembled_sequence \
           WHERE feature_idx IN ({a_only}, {shared}, {b_only});
         DELETE FROM qiita_lake.assembled_sequence_chunks \
           WHERE feature_idx IN ({a_only}, {shared}, {b_only});"
    ));
}

/// Registering the same contigs again converges instead of accumulating —
/// the DoAction's own idempotency, under a fresh work ticket each time (so
/// the lake gets a distinct file carrying the same keys, which is what makes
/// it a real second registration rather than a no-op).
///
/// This is the primitive's property, not a workflow scenario: within one
/// ticket the runner fast-forwards a COMPLETED `register-files` on resume
/// rather than re-running it. Across tickets it is reachable — a fresh
/// submission over a COMPLETED prep_sample is admitted (`REPLACE_KEY_TABLES`
/// carries the submit path) — as is one contig produced by two DIFFERENT
/// runs, the sibling test above.
#[test]
#[serial_test::serial]
#[cfg(feature = "integration")]
fn register_files_re_registering_the_same_contigs_does_not_accumulate() {
    let connstr = delete_test_catalog_connstr();
    let data_path = delete_test_data_path();

    let feature: i64 = 971_020;
    let base_ticket: i64 = 971_100_000 + std::process::id() as i64;

    {
        let conn = Connection::open_in_memory().unwrap();
        ducklake::connect_ducklake(&conn, &connstr, &data_path).unwrap();
        ducklake::ensure_assembly_tables(&conn).unwrap();
        ducklake::ensure_registration_lock(&conn).unwrap();
        conn.execute_batch(&format!(
            "DELETE FROM qiita_lake.assembled_sequence WHERE feature_idx = {feature};
             DELETE FROM qiita_lake.assembled_sequence_chunks WHERE feature_idx = {feature};"
        ))
        .unwrap();
    }

    let contigs: &[(i64, &str, &[&str])] = &[(
        feature,
        "00000000-0000-0000-0000-000000971020",
        &["ACGTACGT", "TTTT"],
    )];
    for attempt in 0..3 {
        let staging = seed_assembly_staging(contigs);
        register_files(
            &connstr,
            &data_path,
            std::path::Path::new("/"),
            &assembly_register_payload(&staging, 1, base_ticket + attempt),
        )
        .unwrap_or_else(|e| panic!("attempt {attempt} register_files failed: {e}"));
    }

    let reader = Connection::open_in_memory().unwrap();
    ducklake::connect_ducklake(&reader, &connstr, &data_path).unwrap();
    let sequence_rows: i64 = reader
        .query_row(
            &format!(
                "SELECT count(*) FROM qiita_lake.assembled_sequence \
                 WHERE feature_idx = {feature}"
            ),
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(sequence_rows, 1, "three loads, one sequence row");
    let chunk_rows: i64 = reader
        .query_row(
            &format!(
                "SELECT count(*) FROM qiita_lake.assembled_sequence_chunks \
                 WHERE feature_idx = {feature}"
            ),
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(chunk_rows, 2, "one chunk row per chunk_index, not six");
    let reassembled: i64 = reader
        .query_row(
            &format!(
                "SELECT length(string_agg(chunk_data, '' ORDER BY chunk_index)) \
                 FROM qiita_lake.assembled_sequence_chunks WHERE feature_idx = {feature}"
            ),
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(reassembled, 12, "the contig, not the contig three times");

    let _ = reader.execute_batch(&format!(
        "DELETE FROM qiita_lake.assembled_sequence WHERE feature_idx = {feature};
         DELETE FROM qiita_lake.assembled_sequence_chunks WHERE feature_idx = {feature};"
    ));
}

/// Register one staging Parquet per `(table, SELECT list)` the caller
/// supplies, as ONE registration under a caller-chosen ticket. Returns the
/// `Registration` so a test can read the replaced-row counts.
#[cfg(feature = "integration")]
fn register_parquets(
    connstr: &str,
    data_path: &str,
    tables: &[(&str, &str)],
    ticket: i64,
) -> Registration {
    let staging = tempfile::tempdir().unwrap();
    let writer = Connection::open_in_memory().unwrap();
    let mut files = std::collections::HashMap::new();
    for (table, values_sql) in tables {
        let src = staging.path().join(format!("{table}.parquet"));
        writer
            .execute_batch(&format!(
                "COPY ({values_sql}) TO '{}' (FORMAT PARQUET)",
                src.to_str().unwrap()
            ))
            .unwrap();
        files.insert(format!("{table}.parquet"), (*table).to_string());
    }

    let payload = auth::ActionPayload {
        action: "register_files".to_string(),
        staging_dir: staging.path().to_str().unwrap().to_string(),
        files,
        work_ticket_idx: ticket,
    };
    register_files(connstr, data_path, std::path::Path::new("/"), &payload).unwrap_or_else(|e| {
        let names: Vec<&str> = tables.iter().map(|(table, _)| *table).collect();
        panic!("register_files({names:?}) failed: {e}")
    })
}

#[cfg(feature = "integration")]
fn register_one_parquet(
    connstr: &str,
    data_path: &str,
    table: &str,
    values_sql: &str,
    ticket: i64,
) -> Registration {
    register_parquets(connstr, data_path, &[(table, values_sql)], ticket)
}

#[cfg(feature = "integration")]
fn lake_count(conn: &Connection, sql: &str) -> i64 {
    conn.query_row(sql, [], |r| r.get(0)).unwrap()
}

/// A second registration of one `(prep_sample_idx, processing_idx)`
/// SUPERSEDES that run's `assembly_membership` / `bin_quality` rows, and
/// reaches no other key: neither another prep_sample's rows under the same
/// `processing_idx`, nor the same prep_sample's rows under a different one. Both
/// halves of the composite key have to be compared for that to hold.
///
/// `reference_sequences` is the control — same two-registration sequence,
/// same `register_files` entry point, replace-keyed on a single column. It
/// isolates the key as the one variable: a converging count on the
/// run-scoped tables alone would not distinguish the key from
/// `register_files` collapsing everything it registers.
///
/// What this pins is what a re-run of `long-read-assembly` over an
/// already-COMPLETED prep_sample does to the lake — `REPLACE_KEY_TABLES`
/// carries why such a re-run resolves to the same `processing_idx` and why
/// the submit path admits it.
#[test]
#[serial_test::serial]
#[cfg(feature = "integration")]
fn register_files_replaces_run_scoped_tables_on_the_whole_key() {
    let connstr = delete_test_catalog_connstr();
    let data_path = delete_test_data_path();

    // prep_sample A run P is the re-run under test; prep_sample B run P shares
    // its processing_idx and prep_sample A run Q shares its prep_sample_idx, so
    // each agrees on one half of the key and must survive.
    let prep_sample_a: i64 = 972_010;
    let prep_sample_b: i64 = 972_011;
    let run_p: i64 = 972_020;
    let run_q: i64 = 972_021;
    let feature_a: i64 = 972_030;
    let feature_b: i64 = 972_031;
    let control_feature: i64 = 972_040;
    let base_ticket: i64 = 972_000_000 + std::process::id() as i64;

    let scope = |prep_sample: i64, run: i64| {
        format!("prep_sample_idx = {prep_sample} AND processing_idx = {run}")
    };
    let a_p = scope(prep_sample_a, run_p);
    let b_p = scope(prep_sample_b, run_p);
    let a_q = scope(prep_sample_a, run_q);
    let control_where = format!("feature_idx = {control_feature}");

    {
        let conn = Connection::open_in_memory().unwrap();
        ducklake::connect_ducklake(&conn, &connstr, &data_path).unwrap();
        ducklake::ensure_assembly_tables(&conn).unwrap();
        ducklake::ensure_reference_tables(&conn).unwrap();
        ducklake::ensure_registration_lock(&conn).unwrap();
        for where_clause in [&a_p, &b_p, &a_q] {
            conn.execute_batch(&format!(
                "DELETE FROM qiita_lake.assembly_membership WHERE {where_clause};
                 DELETE FROM qiita_lake.bin_quality WHERE {where_clause};"
            ))
            .unwrap();
        }
        conn.execute_batch(&format!(
            "DELETE FROM qiita_lake.reference_sequences WHERE {control_where};"
        ))
        .unwrap();
    }

    // One run's rows, byte-identical across both of its registrations —
    // exactly what a re-run under the same processing_idx re-derives.
    // Nine columns: `ducklake_add_data_files` refuses a Parquet missing a column
    // the target table has, just as it refuses an extra one, so these fixtures
    // carry the assembler attribute columns `ensure_assembly_tables` adds.
    let membership_values = |prep_sample: i64, run: i64| {
        format!(
            "SELECT * FROM (VALUES \
                 ({prep_sample}::BIGINT, {run}::BIGINT, 'LCG', 'circular_1', {feature_a}::BIGINT, \
                  'circular_1'::VARCHAR, 'yes'::VARCHAR, 30.5::DOUBLE, 1.02::DOUBLE), \
                 ({prep_sample}::BIGINT, {run}::BIGINT, 'MAG', 'bin.1', {feature_b}::BIGINT, \
                  NULL::VARCHAR, NULL::VARCHAR, NULL::DOUBLE, NULL::DOUBLE)) \
                 t(prep_sample_idx, processing_idx, kind, bin_id, feature_idx, \
                   raw_name, circularity, depth, mult)"
        )
    };
    let quality_values = |prep_sample: i64, run: i64| {
        format!(
            "SELECT * FROM (VALUES \
                 ({prep_sample}::BIGINT, {run}::BIGINT, 'MAG', 'bin.1', \
                  'k__Bacteria'::VARCHAR, 91.5::DOUBLE, 1.25::DOUBLE, 0.0::DOUBLE, \
                  4200000::BIGINT, 42::BIGINT, 0.87::DOUBLE, 'metabat2'::VARCHAR)) \
                 t(prep_sample_idx, processing_idx, kind, bin_id, marker_lineage, \
                   completeness, contamination, strain_heterogeneity, genome_size, \
                   n_contigs, das_tool_score, source_binner)"
        )
    };
    let control_values = format!(
        "SELECT * FROM (VALUES \
             ({control_feature}::BIGINT, \
              '00000000-0000-0000-0000-000000972040'::UUID, 8::BIGINT)) \
             t(feature_idx, sequence_hash, sequence_length_bp)"
    );

    let reader = Connection::open_in_memory().unwrap();
    ducklake::connect_ducklake(&reader, &connstr, &data_path).unwrap();
    let control_count =
        format!("SELECT count(*) FROM qiita_lake.reference_sequences WHERE {control_where}");
    let assert_run_rows = |where_clause: &str, membership: i64, quality: i64, label: &str| {
        assert_eq!(
            lake_count(
                &reader,
                &format!(
                    "SELECT count(*) FROM qiita_lake.assembly_membership WHERE {where_clause}"
                )
            ),
            membership,
            "assembly_membership {label}"
        );
        assert_eq!(
            lake_count(
                &reader,
                &format!("SELECT count(*) FROM qiita_lake.bin_quality WHERE {where_clause}")
            ),
            quality,
            "bin_quality {label}"
        );
    };

    assert_run_rows(&a_p, 0, 0, "before any load (A/P)");
    assert_run_rows(&b_p, 0, 0, "before any load (B/P)");
    assert_run_rows(&a_q, 0, 0, "before any load (A/Q)");
    assert_eq!(lake_count(&reader, &control_count), 0, "control empty");

    // A fresh ticket per registration: the lake gets a distinct file
    // carrying the same keys, which is what makes the second load a real
    // re-registration rather than a no-op.
    let mut ticket = base_ticket;
    let mut register = |table: &str, values: &str| {
        ticket += 1;
        register_one_parquet(&connstr, &data_path, table, values, ticket)
    };

    let m1 = register(
        "assembly_membership",
        &membership_values(prep_sample_a, run_p),
    );
    let q1 = register("bin_quality", &quality_values(prep_sample_a, run_p));
    let c1 = register("reference_sequences", &control_values);
    register(
        "assembly_membership",
        &membership_values(prep_sample_b, run_p),
    );
    register("bin_quality", &quality_values(prep_sample_b, run_p));
    register(
        "assembly_membership",
        &membership_values(prep_sample_a, run_q),
    );
    register("bin_quality", &quality_values(prep_sample_a, run_q));

    assert!(
        m1.replaced.is_empty() && q1.replaced.is_empty() && c1.replaced.is_empty(),
        "a key the lake does not hold replaces nothing: membership={:?} \
         bin_quality={:?} control={:?}",
        m1.replaced,
        q1.replaced,
        c1.replaced,
    );
    assert_run_rows(&a_p, 2, 1, "after the first load");
    assert_run_rows(&b_p, 2, 1, "after the first load");
    assert_run_rows(&a_q, 2, 1, "after the first load");
    assert_eq!(
        lake_count(&reader, &control_count),
        1,
        "control loaded once"
    );

    let m2 = register(
        "assembly_membership",
        &membership_values(prep_sample_a, run_p),
    );
    let q2 = register("bin_quality", &quality_values(prep_sample_a, run_p));
    let c2 = register("reference_sequences", &control_values);

    assert_eq!(
        m2.replaced.get("assembly_membership").copied(),
        Some(2),
        "the re-run supersedes A/P's two membership rows, and only those",
    );
    assert_eq!(
        q2.replaced.get("bin_quality").copied(),
        Some(1),
        "the re-run supersedes A/P's one bin_quality row, and only that",
    );
    assert_eq!(
        c2.replaced.get("reference_sequences").copied(),
        Some(1),
        "control: the second registration supersedes the first's row",
    );

    assert_run_rows(&a_p, 2, 1, "after the re-run (superseded, not appended)");
    assert_run_rows(&b_p, 2, 1, "after the re-run (same run, other prep_sample)");
    assert_run_rows(&a_q, 2, 1, "after the re-run (same prep_sample, other run)");
    assert_eq!(
        lake_count(&reader, &control_count),
        1,
        "control: one row per feature_idx after both registrations",
    );

    // Tombstone the catalog rows only — the physical lake files stay
    // registered until compaction (see the sibling register tests).
    for where_clause in [&a_p, &b_p, &a_q] {
        let _ = reader.execute_batch(&format!(
            "DELETE FROM qiita_lake.assembly_membership WHERE {where_clause};
             DELETE FROM qiita_lake.bin_quality WHERE {where_clause};"
        ));
    }
    let _ = reader.execute_batch(&format!(
        "DELETE FROM qiita_lake.reference_sequences WHERE {control_where};"
    ));
}

/// A re-run that yields NO MAG still clears the previous run's `bin_quality`
/// rows, because `bin_quality`'s delete reads the keys `assembly_membership`
/// names in the same registration. `bin_quality` alone names none — CheckM
/// covers refined bins only, so `assembly_load` writes it empty-with-schema —
/// and a delete keyed on the incoming file alone removes nothing, leaving MAG
/// rows behind a membership set that was replaced out from under them.
///
/// The control is the second registration's membership rows: they are
/// superseded in the same call, which is what makes the surviving-or-not
/// `bin_quality` rows the one variable.
#[test]
#[serial_test::serial]
#[cfg(feature = "integration")]
fn an_empty_bin_quality_still_supersedes_the_runs_rows() {
    let connstr = delete_test_catalog_connstr();
    let data_path = delete_test_data_path();

    let prep_sample: i64 = 974_010;
    let run: i64 = 974_020;
    let feature: i64 = 974_030;
    let base_ticket: i64 = 974_000_000 + std::process::id() as i64;
    let where_clause = format!("prep_sample_idx = {prep_sample} AND processing_idx = {run}");

    {
        let conn = Connection::open_in_memory().unwrap();
        ducklake::connect_ducklake(&conn, &connstr, &data_path).unwrap();
        ducklake::ensure_assembly_tables(&conn).unwrap();
        ducklake::ensure_registration_lock(&conn).unwrap();
        conn.execute_batch(&format!(
            "DELETE FROM qiita_lake.assembly_membership WHERE {where_clause};
             DELETE FROM qiita_lake.bin_quality WHERE {where_clause};"
        ))
        .unwrap();
    }

    let membership_values = |kind: &str, bin_id: &str| {
        format!(
            "SELECT * FROM (VALUES \
                 ({prep_sample}::BIGINT, {run}::BIGINT, '{kind}', '{bin_id}', {feature}::BIGINT, \
                  NULL::VARCHAR, NULL::VARCHAR, NULL::DOUBLE, NULL::DOUBLE)) \
                 t(prep_sample_idx, processing_idx, kind, bin_id, feature_idx, \
                   raw_name, circularity, depth, mult)"
        )
    };
    let quality_values = |suffix: &str| {
        format!(
            "SELECT * FROM (VALUES \
                 ({prep_sample}::BIGINT, {run}::BIGINT, 'MAG', 'bin.1', \
                  'k__Bacteria'::VARCHAR, 91.5::DOUBLE, 1.25::DOUBLE, 0.0::DOUBLE, \
                  4200000::BIGINT, 42::BIGINT, 0.87::DOUBLE, 'metabat2'::VARCHAR)) \
                 t(prep_sample_idx, processing_idx, kind, bin_id, marker_lineage, \
                   completeness, contamination, strain_heterogeneity, genome_size, \
                   n_contigs, das_tool_score, source_binner){suffix}"
        )
    };

    let reader = Connection::open_in_memory().unwrap();
    ducklake::connect_ducklake(&reader, &connstr, &data_path).unwrap();
    let membership_count =
        format!("SELECT count(*) FROM qiita_lake.assembly_membership WHERE {where_clause}");
    let quality_count = format!("SELECT count(*) FROM qiita_lake.bin_quality WHERE {where_clause}");

    // The first run: one refined MAG, so both files carry a row.
    register_parquets(
        &connstr,
        &data_path,
        &[
            ("assembly_membership", &membership_values("MAG", "bin.1")),
            ("bin_quality", &quality_values("")),
        ],
        base_ticket,
    );
    assert_eq!(
        lake_count(&reader, &membership_count),
        1,
        "first run loaded"
    );
    assert_eq!(lake_count(&reader, &quality_count), 1, "first run loaded");

    // The re-run: contigs, but no refined bin. `assembly_load` writes
    // bin_quality empty-with-schema and register-files registers it anyway.
    let replaced = register_parquets(
        &connstr,
        &data_path,
        &[
            (
                "assembly_membership",
                &membership_values("UNBINNED", "contig_1"),
            ),
            ("bin_quality", &quality_values(" WHERE FALSE")),
        ],
        base_ticket + 1,
    )
    .replaced;

    assert_eq!(
        replaced.get("assembly_membership").copied(),
        Some(1),
        "control: the re-run supersedes the first run's membership row",
    );
    assert_eq!(
        replaced.get("bin_quality").copied(),
        Some(1),
        "the empty bin_quality supersedes the first run's MAG row",
    );
    assert_eq!(lake_count(&reader, &membership_count), 1, "one run's rows");
    assert_eq!(
        lake_count(&reader, &quality_count),
        0,
        "a run with no MAG leaves no bin_quality row behind",
    );

    let _ = reader.execute_batch(&format!(
        "DELETE FROM qiita_lake.assembly_membership WHERE {where_clause};
         DELETE FROM qiita_lake.bin_quality WHERE {where_clause};"
    ));
}

/// Every `REPLACE_KEY_TABLES` entry names a real lake table with that key
/// column. The delete interpolates both names into SQL, so a typo or a
/// renamed column would otherwise surface as a failed load in production
/// rather than here.
#[test]
#[serial_test::serial]
#[cfg(feature = "integration")]
fn replace_key_tables_match_the_lake_schema() {
    let connstr = delete_test_catalog_connstr();
    let data_path = delete_test_data_path();
    let conn = Connection::open_in_memory().unwrap();
    ducklake::connect_ducklake(&conn, &connstr, &data_path).unwrap();
    setup_full_lake(&conn);

    for entry in REPLACE_KEY_TABLES {
        let table = entry.table;
        for key in entry.key {
            let found: i64 = conn
                .query_row(
                    "SELECT count(*) FROM duckdb_columns() \
                     WHERE database_name = 'qiita_lake' AND table_name = ? AND column_name = ?",
                    duckdb::params![table, key],
                    |r| r.get(0),
                )
                .unwrap_or_else(|e| panic!("column lookup for {table}.{key} failed: {e}"));
            assert_eq!(found, 1, "qiita_lake.{table} has no {key} column");
        }
    }
}

/// The inverse direction: every lake table shaped like a content-addressed
/// sequence store IS registered. `replace_key_tables_match_the_lake_schema`
/// only catches a wrong column in an existing entry; this catches a fifth table
/// added later with the same shape and no entry, which would duplicate
/// silently.
///
/// The shape is the column set, not the name: `(feature_idx, sequence_hash,
/// sequence_length_bp)` or `(feature_idx, chunk_index, chunk_data)`. A table
/// carrying a run-scoping column has a different set and is not matched.
#[test]
#[serial_test::serial]
#[cfg(feature = "integration")]
fn every_content_addressed_lake_table_is_registered() {
    let connstr = delete_test_catalog_connstr();
    let data_path = delete_test_data_path();
    let conn = Connection::open_in_memory().unwrap();
    ducklake::connect_ducklake(&conn, &connstr, &data_path).unwrap();
    setup_full_lake(&conn);

    // Content-addressed shape: keyed by feature_idx, carrying sequence bytes or
    // their hash, and scoped by nothing else. Matching the shape rather than an
    // exact column set means a fifth table with one extra column is still
    // caught; the scope-column exclusion is what keeps reference_taxonomy and
    // the alignment tables out.
    let mut stmt = conn
        .prepare(
            "SELECT table_name, list(column_name) AS cols \
             FROM duckdb_columns() WHERE database_name = 'qiita_lake' \
             GROUP BY table_name \
             HAVING list_contains(cols, 'feature_idx') \
                AND (list_contains(cols, 'chunk_data') \
                     OR list_contains(cols, 'sequence_hash')) \
                AND NOT list_has_any(cols, ['reference_idx', 'prep_sample_idx', \
                                            'processing_idx', 'mask_idx', \
                                            'alignment_idx']) \
             ORDER BY table_name",
        )
        .unwrap();
    let shaped: Vec<String> = stmt
        .query_map([], |r| r.get::<_, String>(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();

    assert!(
        !shaped.is_empty(),
        "the shape query matched nothing — it can no longer catch an omission"
    );
    for table in &shaped {
        assert!(
            REPLACE_KEY_TABLES.iter().any(|entry| entry.table == table),
            "qiita_lake.{table} is content-addressed but absent from REPLACE_KEY_TABLES, \
             so a second load carrying its keys would duplicate them; found shaped tables: \
             {shaped:?}"
        );
    }
}

/// Membership runs both ways. Every `qiita_lake` BASE TABLE scoped by
/// `alignment_idx` is in `ALIGNMENT_DELETE_TABLES`, and every table in the
/// list carries every column a `delete_alignment*` clause keys on.
///
/// The first direction is the shape query, as in
/// `every_content_addressed_lake_table_is_registered`: an `alignment_idx`
/// column means the rows belong to one align-config identity and die with it,
/// so a table added later carrying that column and left off the list would
/// survive a DELETE that is supposed to purge the whole alignment — and
/// disallow-without-delete would then re-admit a submission over rows that are
/// still there.
///
/// The second is the columns. `delete_lake_rows` applies ONE clause to every
/// listed table, so a table joining the list without `prep_sample_idx` or
/// `sequence_idx` makes the narrower deletes unrunnable. That is loud rather
/// than silent — DuckDB raises a Binder Error on the missing column and
/// `delete_lake_rows` ROLLBACKs, leaving the leading table's rows intact — so
/// this catches it in CI instead of at the first ticket.
///
/// Views are excluded: `alignment_visible` carries `alignment_idx` from the
/// base table it selects and has no rows of its own.
#[test]
#[serial_test::serial]
#[cfg(feature = "integration")]
fn alignment_delete_covers_every_alignment_scoped_lake_table() {
    let connstr = delete_test_catalog_connstr();
    let data_path = delete_test_data_path();
    let conn = Connection::open_in_memory().unwrap();
    ducklake::connect_ducklake(&conn, &connstr, &data_path).unwrap();
    setup_full_lake(&conn);

    let mut stmt = conn
        .prepare(
            "SELECT DISTINCT c.table_name FROM duckdb_columns() c \
             JOIN duckdb_tables() t \
               ON t.database_name = c.database_name AND t.table_name = c.table_name \
             WHERE c.database_name = 'qiita_lake' AND c.column_name = 'alignment_idx' \
             ORDER BY c.table_name",
        )
        .unwrap();
    let scoped: Vec<String> = stmt
        .query_map([], |r| r.get::<_, String>(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();

    assert!(
        !scoped.is_empty(),
        "the shape query matched nothing — it can no longer catch an omission"
    );
    for table in &scoped {
        assert!(
            ALIGNMENT_DELETE_TABLES.contains(&table.as_str()),
            "qiita_lake.{table} is scoped by alignment_idx but absent from \
             ALIGNMENT_DELETE_TABLES, so the delete_alignment* handlers leave its \
             rows behind; found scoped tables: {scoped:?}"
        );
    }
    assert_eq!(
        ALIGNMENT_DELETE_TABLES.first(),
        Some(&"alignment"),
        "the delete_alignment* handlers report counts[0] as rows_deleted"
    );

    // The columns the three clauses key on: `delete_alignment` on
    // alignment_idx, `delete_alignment_sample` on that plus prep_sample_idx,
    // `delete_alignment_block` on that plus `block_read_where_clause`'s
    // sequence_idx.
    for table in ALIGNMENT_DELETE_TABLES {
        let mut stmt = conn
            .prepare(
                "SELECT column_name FROM duckdb_columns() \
                 WHERE database_name = 'qiita_lake' AND table_name = ?",
            )
            .unwrap();
        let columns: Vec<String> = stmt
            .query_map([table], |r| r.get::<_, String>(0))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert!(
            !columns.is_empty(),
            "qiita_lake.{table} is in ALIGNMENT_DELETE_TABLES but the catalog \
             holds no such table"
        );
        for key in ["alignment_idx", "prep_sample_idx", "sequence_idx"] {
            assert!(
                columns.iter().any(|c| c == key),
                "qiita_lake.{table} is in ALIGNMENT_DELETE_TABLES but carries no \
                 {key}, which a delete_alignment* clause keys on — every delete \
                 using it would bind-error and roll back; found columns: {columns:?}"
            );
        }
    }
}

/// Concurrent registrations of one feature leave ONE row, and every writer
/// succeeds.
///
/// This is the case the replace-by-key DELETE alone does not cover — see
/// `register_files`' transaction for why — and so the test that `registration_lock`
/// answers for.
///
/// Runs against a lake that does NOT already hold the feature, because
/// pre-seeding it would make the DELETE conflict and the test would green with
/// the lock removed.
#[test]
#[serial_test::serial]
#[cfg(feature = "integration")]
fn concurrent_registrations_of_one_feature_leave_one_row() {
    let connstr = delete_test_catalog_connstr();
    let data_path = delete_test_data_path();

    let shared: i64 = 973_010;
    let base_ticket: i64 = 973_000_000 + std::process::id() as i64;
    const WRITERS: i64 = 4;

    {
        let conn = Connection::open_in_memory().unwrap();
        ducklake::connect_ducklake(&conn, &connstr, &data_path).unwrap();
        ducklake::ensure_assembly_tables(&conn).unwrap();
        ducklake::ensure_registration_lock(&conn).unwrap();
        conn.execute_batch(&format!(
            "DELETE FROM qiita_lake.assembled_sequence WHERE feature_idx = {shared};
             DELETE FROM qiita_lake.assembled_sequence_chunks WHERE feature_idx = {shared};"
        ))
        .unwrap();
    }

    // Stage every writer's Parquet BEFORE spawning, and release them from a
    // barrier, so the only work between the threads starting and their
    // transactions is `register_files` itself. Seeding inside the threads
    // staggers them past each other, and the test then greens with the lock
    // removed.
    let payloads: Vec<(tempfile::TempDir, auth::ActionPayload)> = (0..WRITERS)
        .map(|i| {
            let staging = seed_assembly_staging(&[(
                shared,
                "00000000-0000-0000-0000-000000973010",
                &["ACGTACGT", "ACGT"],
            )]);
            let payload = assembly_register_payload(&staging, 1, base_ticket + i);
            (staging, payload)
        })
        .collect();

    let start = std::sync::Barrier::new(WRITERS as usize);
    let outcomes: Vec<Result<Registration, Status>> = std::thread::scope(|scope| {
        let handles: Vec<_> = payloads
            .iter()
            .map(|(_staging, payload)| {
                let connstr = &connstr;
                let data_path = &data_path;
                let start = &start;
                scope.spawn(move || {
                    start.wait();
                    register_files(connstr, data_path, std::path::Path::new("/"), payload)
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });

    for (i, outcome) in outcomes.iter().enumerate() {
        assert!(
            outcome.is_ok(),
            "writer {i} failed instead of retrying its conflicted commit: {:?}",
            outcome.as_ref().err()
        );
    }

    let reader = Connection::open_in_memory().unwrap();
    ducklake::connect_ducklake(&reader, &connstr, &data_path).unwrap();
    let (sequences, chunks): (i64, i64) = reader
        .query_row(
            &format!(
                "SELECT (SELECT count(*) FROM qiita_lake.assembled_sequence \
                          WHERE feature_idx = {shared}), \
                        (SELECT count(*) FROM qiita_lake.assembled_sequence_chunks \
                          WHERE feature_idx = {shared})"
            ),
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(sequences, 1, "{WRITERS} concurrent loads, one sequence row");
    assert_eq!(chunks, 2, "one chunk row per chunk_index, not {WRITERS}x");

    let _ = reader.execute_batch(&format!(
        "DELETE FROM qiita_lake.assembled_sequence WHERE feature_idx = {shared};
         DELETE FROM qiita_lake.assembled_sequence_chunks WHERE feature_idx = {shared};"
    ));
}

/// The reference pair takes the same path as the assembly pair. Two
/// references sharing a sequence each ship that feature's rows; after both
/// loads there is one of each.
#[test]
#[serial_test::serial]
#[cfg(feature = "integration")]
fn register_files_replaces_sequences_shared_across_references() {
    let connstr = delete_test_catalog_connstr();
    let data_path = delete_test_data_path();

    let shared: i64 = 972_010;
    let b_only: i64 = 972_011;
    let ticket_a: i64 = 972_000_000 + std::process::id() as i64;

    {
        let conn = Connection::open_in_memory().unwrap();
        ducklake::connect_ducklake(&conn, &connstr, &data_path).unwrap();
        ducklake::ensure_reference_tables(&conn).unwrap();
        ducklake::ensure_registration_lock(&conn).unwrap();
        conn.execute_batch(&format!(
            "DELETE FROM qiita_lake.reference_sequences \
               WHERE feature_idx IN ({shared}, {b_only});
             DELETE FROM qiita_lake.reference_sequence_chunks \
               WHERE feature_idx IN ({shared}, {b_only});"
        ))
        .unwrap();
    }

    let seed = |contigs: &[(i64, &str, &[&str])], ticket: i64| {
        let staging = seed_assembly_staging(contigs);
        // Same two shapes, registered into the reference tables.
        std::fs::rename(
            staging.path().join("assembled_sequence.parquet"),
            staging.path().join("reference_sequences.parquet"),
        )
        .unwrap();
        std::fs::rename(
            staging.path().join("assembled_sequence_chunks"),
            staging.path().join("reference_sequence_chunks"),
        )
        .unwrap();
        let mut files = std::collections::HashMap::new();
        files.insert(
            "reference_sequences.parquet".to_string(),
            "reference_sequences".to_string(),
        );
        for part_index in 0..contigs.len() {
            files.insert(
                format!("reference_sequence_chunks/part_{part_index:05}.parquet"),
                "reference_sequence_chunks".to_string(),
            );
        }
        let payload = auth::ActionPayload {
            action: "register_files".to_string(),
            staging_dir: staging.path().to_str().unwrap().to_string(),
            files,
            work_ticket_idx: ticket,
        };
        let outcome = register_files(&connstr, &data_path, std::path::Path::new("/"), &payload)
            .unwrap_or_else(|e| panic!("register_files failed: {e}"));
        // Keep `staging` alive until after the call.
        drop(staging);
        outcome
    };

    // Reference A: one sequence, which reference B also carries.
    seed(
        &[(shared, "00000000-0000-0000-0000-000000972010", &["ACGT"])],
        ticket_a,
    );
    let outcome_b = seed(
        &[
            (shared, "00000000-0000-0000-0000-000000972010", &["ACGT"]),
            (b_only, "00000000-0000-0000-0000-000000972011", &["TTTT"]),
        ],
        ticket_a + 1,
    );
    assert_eq!(
        outcome_b.replaced.get("reference_sequences").copied(),
        Some(1),
        "reference A's copy of the shared sequence is superseded"
    );
    assert_eq!(
        outcome_b.replaced.get("reference_sequence_chunks").copied(),
        Some(1)
    );

    let reader = Connection::open_in_memory().unwrap();
    ducklake::connect_ducklake(&reader, &connstr, &data_path).unwrap();
    for feature_idx in [shared, b_only] {
        let (sequences, chunks): (i64, i64) = reader
            .query_row(
                &format!(
                    "SELECT (SELECT count(*) FROM qiita_lake.reference_sequences \
                              WHERE feature_idx = {feature_idx}), \
                            (SELECT count(*) FROM qiita_lake.reference_sequence_chunks \
                              WHERE feature_idx = {feature_idx})"
                ),
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(sequences, 1, "one sequence row for {feature_idx}");
        assert_eq!(chunks, 1, "one chunk row for {feature_idx}");
    }

    let _ = reader.execute_batch(&format!(
        "DELETE FROM qiita_lake.reference_sequences WHERE feature_idx IN ({shared}, {b_only});
         DELETE FROM qiita_lake.reference_sequence_chunks \
           WHERE feature_idx IN ({shared}, {b_only});"
    ));
}

/// Pins the DuckLake-transaction semantics `register_files` relies on: a
/// `ducklake_add_data_files` performed inside a transaction that is then
/// ROLLBACK'd leaves ZERO rows registered — visible within the open
/// transaction, gone after the rollback. If DuckLake auto-committed catalog
/// mutations (ignoring the enclosing DuckDB transaction), `register_files`'
/// BEGIN/ROLLBACK wrap would be a no-op and a mid-loop failure would leak a
/// half-registered reference; this asserts it is not.
#[test]
#[serial_test::serial]
#[cfg(feature = "integration")]
fn register_ducklake_add_data_files_rolls_back_within_transaction() {
    let connstr = delete_test_catalog_connstr();
    let data_path = delete_test_data_path();

    let ref_idx: i64 = 972_000;
    let feat: i64 = 972_010;

    let conn = Connection::open_in_memory().unwrap();
    ducklake::connect_ducklake(&conn, &connstr, &data_path).unwrap();
    ducklake::ensure_reference_tables(&conn).unwrap();
    conn.execute_batch(&format!(
        "DELETE FROM qiita_lake.reference_membership WHERE reference_idx = {ref_idx};"
    ))
    .unwrap();

    // A valid reference_membership Parquet to register (types match exactly).
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("m.parquet");
    let src_str = src.to_str().unwrap();
    conn.execute_batch(&format!(
        "COPY (SELECT {ref_idx}::BIGINT AS reference_idx, {feat}::BIGINT AS feature_idx) \
         TO '{src_str}' (FORMAT PARQUET)"
    ))
    .unwrap();

    let count = |c: &Connection| -> i64 {
        c.query_row(
            &format!(
                "SELECT count(*) FROM qiita_lake.reference_membership \
                 WHERE reference_idx = {ref_idx}"
            ),
            [],
            |r| r.get(0),
        )
        .unwrap()
    };

    conn.execute_batch("BEGIN TRANSACTION").unwrap();
    conn.execute(
        "CALL ducklake_add_data_files('qiita_lake', ?, ?)",
        duckdb::params!["reference_membership", src_str],
    )
    .unwrap();
    assert_eq!(count(&conn), 1, "registration is visible inside the txn");
    conn.execute_batch("ROLLBACK").unwrap();
    assert_eq!(
        count(&conn),
        0,
        "ROLLBACK must unwind the registration — the wrap in register_files \
         is only atomic if DuckLake honors the enclosing transaction"
    );
}

/// `sync_reference_exclusion` REPLACES the mirror wholesale from the CP's
/// blocklist Parquet: stale rows are dropped, the file's rows become the
/// entire table, a re-run with the same file is idempotent, and an empty
/// Parquet clears the table (re-enabling everything). Full-replace ⇒
/// replay-safe. Also asserts symlink containment: a dest that lexically sits
/// under the scratch root but resolves outside it is rejected before any read.
#[test]
#[serial_test::serial]
#[cfg(feature = "integration")]
fn sync_reference_exclusion_full_replace_is_idempotent() {
    let connstr = delete_test_catalog_connstr();
    let data_path = delete_test_data_path();
    let conn = Connection::open_in_memory().unwrap();
    ducklake::connect_ducklake(&conn, &connstr, &data_path).unwrap();
    // ensure_exclusion_tables also (re)creates the _visible views, which
    // reference reference_taxonomy + alignment — so create those first.
    ducklake::ensure_reference_tables(&conn).unwrap();
    ducklake::ensure_alignment_tables(&conn).unwrap();
    ducklake::ensure_exclusion_tables(&conn).unwrap();

    // Unique feature ids so leftover rows never collide with other serial
    // tests, and a full-table clean slate (the mirror is a global set with
    // no scoping column to filter on).
    let feat_a: i64 = 974_010;
    let feat_b: i64 = 974_011;
    let stale: i64 = 974_099;
    conn.execute_batch("DELETE FROM qiita_lake.reference_exclusion;")
        .unwrap();

    // Seed a stale row the wholesale replace must drop.
    conn.execute_batch(&format!(
        "INSERT INTO qiita_lake.reference_exclusion (feature_idx) VALUES ({stale});"
    ))
    .unwrap();

    // The CP's blocklist Parquet: a single BIGINT `feature_idx` column,
    // written by DuckDB so the type matches the table exactly.
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("reference_exclusion.parquet");
    let src_str = src.to_str().unwrap();
    {
        let writer = Connection::open_in_memory().unwrap();
        writer
            .execute_batch(&format!(
                "COPY (SELECT * FROM (VALUES ({feat_a}::BIGINT), ({feat_b}::BIGINT)) \
                     t(feature_idx)) \
                 TO '{src_str}' (FORMAT PARQUET)"
            ))
            .unwrap();
    }

    let contents = || -> Vec<i64> {
        let mut stmt = conn
            .prepare("SELECT feature_idx FROM qiita_lake.reference_exclusion ORDER BY feature_idx")
            .unwrap();
        let rows = stmt
            .query_map([], |r| r.get::<_, i64>(0))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        rows
    };

    // The tempdir is the scratch root the handler contains reads to.
    let root = dir.path();

    let first = sync_reference_exclusion(&connstr, &data_path, &src, root).expect("sync failed");
    assert_eq!(
        first["feature_count"], 2,
        "two rows loaded from the parquet"
    );
    assert_eq!(
        contents(),
        vec![feat_a, feat_b],
        "mirror is exactly the parquet's rows — the stale row was dropped"
    );

    // Idempotency: re-running with the same file converges to the same table.
    let second =
        sync_reference_exclusion(&connstr, &data_path, &src, root).expect("re-sync failed");
    assert_eq!(second["feature_count"], 2, "same load on replay");
    assert_eq!(
        contents(),
        vec![feat_a, feat_b],
        "table unchanged on replay"
    );

    // An empty blocklist Parquet clears the mirror (re-enables everything).
    let empty = dir.path().join("empty.parquet");
    let empty_str = empty.to_str().unwrap();
    {
        let writer = Connection::open_in_memory().unwrap();
        writer
            .execute_batch(&format!(
                "COPY (SELECT 0::BIGINT AS feature_idx WHERE false) \
                 TO '{empty_str}' (FORMAT PARQUET)"
            ))
            .unwrap();
    }
    let cleared =
        sync_reference_exclusion(&connstr, &data_path, &empty, root).expect("clear sync failed");
    assert_eq!(cleared["feature_count"], 0, "empty parquet loads zero rows");
    assert!(contents().is_empty(), "mirror cleared by the empty replace");

    // Symlink containment: a dest UNDER the scratch root that resolves
    // OUTSIDE it (a planted symlink) is rejected before any read — the
    // lexical `starts_with` check would have passed it. Guards the global
    // mirror against a redirected read of an attacker-planted Parquet.
    let outside = tempfile::tempdir().unwrap();
    let outside_pq = outside.path().join("evil.parquet");
    {
        let writer = Connection::open_in_memory().unwrap();
        writer
            .execute_batch(&format!(
                "COPY (SELECT 999999::BIGINT AS feature_idx) TO '{}' (FORMAT PARQUET)",
                outside_pq.to_str().unwrap()
            ))
            .unwrap();
    }
    let planted = dir.path().join("planted.parquet");
    std::os::unix::fs::symlink(&outside_pq, &planted).unwrap();
    let escaped = sync_reference_exclusion(&connstr, &data_path, &planted, root);
    assert_eq!(
        escaped.unwrap_err().code(),
        tonic::Code::PermissionDenied,
        "a dest resolving outside the scratch root must be rejected"
    );
    // And the mirror is untouched by the rejected attempt.
    assert!(
        contents().is_empty(),
        "rejected escape left the mirror empty"
    );
}

/// `export_read_to_parquet` writes one prep_sample's full reads from the DuckLake
/// `read` table to a Parquet drop-in: the 7-col schema with `qual` as
/// UTINYINT[], the seeded rows, mode 0o440. An unknown prep_sample writes NO file
/// and returns 0 (the control plane turns that into a submission failure).
#[test]
#[serial_test::serial]
#[cfg(feature = "integration")]
fn export_read_writes_prep_sample_parquet() {
    let connstr = delete_test_catalog_connstr();
    let data_path = delete_test_data_path();
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

    let count = export_read_to_parquet(&connstr, &data_path, prep, &dest, dir.path())
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
    let zero = export_read_to_parquet(&connstr, &data_path, absent, &dest_absent, dir.path())
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

/// Bind the SQL a block-read DoGet ticket produces as `view_name` on `conn`
/// and return its row count.
///
/// The streaming-path twin of the retired export-to-Parquet helpers: same
/// source relation, same `block_read_where_clause` selector, same
/// `EXPORT_READ_COLUMNS` projection — only the sink differs. The member
/// semantics these tests pin (a gap prep_sample excluded, a split member
/// contributing only its sub-range) are properties of the SELECTOR, so they
/// are exercised here exactly as they were through the export.
///
/// A **VIEW**, never a table: nothing is materialized on the data plane. The
/// count is an aggregate over the DuckLake scan — served from catalog and
/// Parquet metadata, not by reading rows into a temp table — and each
/// assertion below re-scans the lake through the same SQL `do_get` hands to
/// `stream_ducklake_batches`. Materializing here would have made the test
/// exercise a shape the server never takes.
#[cfg(feature = "integration")]
fn bind_block_read_doget(
    conn: &Connection,
    table: &str,
    filter: &auth::TicketFilter,
    members: &[auth::BlockReadMember],
    view_name: &str,
) -> i64 {
    let (sql, _) = build_query(table, filter, members, &[]).expect("build_query failed");
    conn.execute_batch(&format!("CREATE OR REPLACE TEMP VIEW {view_name} AS {sql}"))
        .expect("block-read DoGet SQL failed");
    conn.query_row(&format!("SELECT count(*) FROM ({sql})"), [], |r| r.get(0))
        .expect("count over the block-read DoGet SQL failed")
}

/// The `read_block` selector streams the UNION of its members' `read`
/// sub-ranges and nothing else: a prep_sample that is not a member, but whose
/// reads' `sequence_idx` values fall in the gap between two block members, is
/// excluded, and a split member contributes only its sub-range (rows beyond
/// its `sequence_idx_stop` stay out). Per-row `prep_sample_idx` is preserved
/// so the block kernel can group by it.
#[test]
#[serial_test::serial]
#[cfg(feature = "integration")]
fn read_block_selector_streams_union_and_excludes_gap_and_split() {
    let connstr = delete_test_catalog_connstr();
    let data_path = delete_test_data_path();
    let conn = Connection::open_in_memory().unwrap();
    ducklake::connect_ducklake(&conn, &connstr, &data_path).unwrap();
    ducklake::ensure_read_tables(&conn).unwrap();

    // Unique ids so leftover rows never collide with other serial tests.
    let prep_a: i64 = 941_000; // fully in block
    let prep_gap: i64 = 941_001; // sequence_idx in [block_min, block_max] but NOT a member
    let prep_c: i64 = 941_002; // split: block covers only a sub-range

    conn.execute_batch(&format!(
        "DELETE FROM qiita_lake.read WHERE prep_sample_idx IN ({prep_a}, {prep_gap}, {prep_c});
         INSERT INTO qiita_lake.read \
             (prep_sample_idx, sequence_idx, read_id, sequence1, qual1, sequence2, qual2) VALUES \
             ({prep_a}, 941010, 'a0', 'AAAAA', [10,10,10,10,10]::UTINYINT[], NULL, NULL), \
             ({prep_a}, 941011, 'a1', 'AAAAC', [10,10,10,10,10]::UTINYINT[], NULL, NULL), \
             ({prep_a}, 941012, 'a2', 'AAAAG', [10,10,10,10,10]::UTINYINT[], NULL, NULL), \
             ({prep_gap}, 941020, 'g0', 'CCCCC', [10,10,10,10,10]::UTINYINT[], NULL, NULL), \
             ({prep_gap}, 941021, 'g1', 'CCCCA', [10,10,10,10,10]::UTINYINT[], NULL, NULL), \
             ({prep_c}, 941030, 'c0', 'GGGGG', [10,10,10,10,10]::UTINYINT[], NULL, NULL), \
             ({prep_c}, 941031, 'c1', 'GGGGA', [10,10,10,10,10]::UTINYINT[], NULL, NULL), \
             ({prep_c}, 941032, 'c2', 'GGGGC', [10,10,10,10,10]::UTINYINT[], NULL, NULL), \
             ({prep_c}, 941033, 'c3', 'GGGGT', [10,10,10,10,10]::UTINYINT[], NULL, NULL), \
             ({prep_c}, 941034, 'c4', 'GGGTT', [10,10,10,10,10]::UTINYINT[], NULL, NULL);"
    ))
    .unwrap();

    // Block = prep_a (whole) + prep_c (sub-range [941030, 941031], boundary-
    // aligned split). block_min=941010, block_max=941031 spans prep_gap's
    // window (941020-941021), so the IN(prep) clause is what excludes it.
    let members = vec![
        auth::BlockReadMember {
            prep_sample_idx: prep_a,
            sequence_idx_start: 941010,
            sequence_idx_stop: 941012,
        },
        auth::BlockReadMember {
            prep_sample_idx: prep_c,
            sequence_idx_start: 941030,
            sequence_idx_stop: 941031,
        },
    ];

    let count = bind_block_read_doget(
        &conn,
        "read_block",
        &auth::TicketFilter::new(),
        &members,
        "block_doget_rows",
    );
    assert_eq!(
        count, 5,
        "3 (prep_a) + 2 (prep_c sub-range) = 5; gap excluded"
    );

    let reader = &conn;
    let rows_rel = "block_doget_rows";
    // The gap prep_sample must be entirely absent.
    let gap_rows: i64 = reader
        .query_row(
            &format!("SELECT count(*) FROM {rows_rel} WHERE prep_sample_idx = {prep_gap}"),
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        gap_rows, 0,
        "gap prep_sample excluded by the prep_sample_idx IN clause"
    );
    // The split prep_sample contributes only its sub-range (no 941032..034).
    let c_max: i64 = reader
        .query_row(
            &format!("SELECT max(sequence_idx) FROM {rows_rel} WHERE prep_sample_idx = {prep_c}"),
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(c_max, 941031, "split member stops at its sequence_idx_stop");
    // Per-row prep_sample_idx preserved for both members.
    let distinct_preps: i64 = reader
        .query_row(
            &format!("SELECT count(DISTINCT prep_sample_idx) FROM {rows_rel}"),
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(distinct_preps, 2, "both members present, keyed per-row");

    // An empty members list is REFUSED outright on the streaming path (an
    // unscoped raw read must not be representable), where the retired export
    // wrote no file and returned 0.
    assert!(
        build_query("read_block", &auth::TicketFilter::new(), &[], &[]).is_err(),
        "an empty members selector must be rejected, not treated as zero rows"
    );

    let _ = conn.execute_batch(&format!(
        "DELETE FROM qiita_lake.read WHERE prep_sample_idx IN ({prep_a}, {prep_gap}, {prep_c});"
    ));
}

/// The `read_masked_block` DoGet selector streams the block's members from the
/// `read_masked` MACRO scoped to `mask_idx`: it excludes non-`pass` reads (the
/// macro's privacy filter), a different mask's rows, and non-member prep_samples —
/// in the same `EXPORT_READ_COLUMNS` shape the raw `read_block` selector
/// yields. Because masked-out reads drop, a masked block can be a proper
/// subset of the raw range.
#[test]
#[serial_test::serial]
#[cfg(feature = "integration")]
fn read_masked_block_selector_streams_only_pass_rows_for_mask() {
    let connstr = delete_test_catalog_connstr();
    let data_path = delete_test_data_path();
    let conn = Connection::open_in_memory().unwrap();
    ducklake::connect_ducklake(&conn, &connstr, &data_path).unwrap();
    ducklake::ensure_read_tables(&conn).unwrap();

    // Unique ids so leftover rows never collide with other serial tests.
    let mask_a: i64 = 942_000;
    let mask_b: i64 = 942_001;
    let prep_a: i64 = 942_010; // the member prep_sample
    let prep_b: i64 = 942_011; // present in read_mask but NOT a member

    conn.execute_batch(&format!(
        "DELETE FROM qiita_lake.read WHERE prep_sample_idx IN ({prep_a}, {prep_b});
         DELETE FROM qiita_lake.read_mask WHERE mask_idx IN ({mask_a}, {mask_b});
         INSERT INTO qiita_lake.read \
             (prep_sample_idx, sequence_idx, read_id, sequence1, qual1, sequence2, qual2) VALUES \
             ({prep_a}, 100, 'a0', 'AAAAA', [30,30,30,30,30]::UTINYINT[], NULL, NULL), \
             ({prep_a}, 101, 'a1', 'CCCCC', [30,30,30,30,30]::UTINYINT[], NULL, NULL), \
             ({prep_a}, 102, 'a2', 'GGGGG', [30,30,30,30,30]::UTINYINT[], NULL, NULL), \
             ({prep_b}, 200, 'b0', 'TTTTT', [30,30,30,30,30]::UTINYINT[], NULL, NULL);
         -- mask_a: seq 100 & 102 pass, seq 101 is a host hit (excluded by the
         -- read_masked macro). prep_b's 200 passes but is not a block member.
         -- Trims 0 so bytes pass through unchanged.
         INSERT INTO qiita_lake.read_mask \
             (mask_idx, prep_sample_idx, sequence_idx, reason) VALUES \
             ({mask_a}, {prep_a}, 100, 'pass'), \
             ({mask_a}, {prep_a}, 101, 'host_minimap2'), \
             ({mask_a}, {prep_a}, 102, 'pass'), \
             ({mask_a}, {prep_b}, 200, 'pass'), \
             ({mask_b}, {prep_a}, 100, 'pass');"
    ))
    .unwrap();

    let members = vec![auth::BlockReadMember {
        prep_sample_idx: prep_a,
        sequence_idx_start: 100,
        sequence_idx_stop: 102,
    }];

    let mut mask_filter = auth::TicketFilter::new();
    mask_filter.insert(
        "mask_idx".to_string(),
        vec![serde_json::Value::from(mask_a)],
    );
    let count = bind_block_read_doget(
        &conn,
        "read_masked_block",
        &mask_filter,
        &members,
        "masked_block_doget_rows",
    );
    // seq 100 & 102 pass; seq 101 (host) excluded by the view => 2 rows.
    assert_eq!(count, 2, "only the 2 pass rows in the member range stream");

    let reader = &conn;
    let rows_rel = "masked_block_doget_rows";
    // Same column shape as the raw block export (EXPORT_READ_COLUMNS).
    let cols: Vec<String> = {
        let mut stmt = reader
            .prepare(&format!("DESCRIBE SELECT * FROM {rows_rel}"))
            .unwrap();
        stmt.query_map([], |r| r.get::<_, String>(0))
            .unwrap()
            .map(|r| r.unwrap())
            .collect()
    };
    assert_eq!(
        cols,
        vec![
            "prep_sample_idx",
            "sequence_idx",
            "read_id",
            "sequence1",
            "qual1",
            "sequence2",
            "qual2"
        ],
        "masked export has the EXPORT_READ_COLUMNS shape"
    );
    // Exactly the two pass sequence_idxs; the host row (101) and prep_b (200)
    // and mask_b are all excluded.
    let seqs: Vec<i64> = {
        let mut stmt = reader
            .prepare(&format!(
                "SELECT sequence_idx FROM {rows_rel} ORDER BY sequence_idx"
            ))
            .unwrap();
        stmt.query_map([], |r| r.get(0))
            .unwrap()
            .map(|r| r.unwrap())
            .collect()
    };
    assert_eq!(
        seqs,
        vec![100, 102],
        "host-masked seq 101 excluded; only pass rows"
    );

    // An empty members list is REFUSED outright on the streaming path —
    // stricter than the retired export, which wrote no file and returned 0.
    // An unscoped read must not be representable at all (see ALLOWED_TABLES).
    assert!(
        build_query("read_masked_block", &mask_filter, &[], &[]).is_err(),
        "an empty members selector must be rejected, not treated as zero rows"
    );

    let _ = conn.execute_batch(&format!(
        "DELETE FROM qiita_lake.read WHERE prep_sample_idx IN ({prep_a}, {prep_b});
         DELETE FROM qiita_lake.read_mask WHERE mask_idx IN ({mask_a}, {mask_b});"
    ));
}

/// A split member whose `sequence_idx_stop` is NOT the block's max still
/// contributes only its own sub-range: the per-member predicate excludes the
/// part of that prep_sample living in a sibling block, even though those rows fall
/// inside the block's overall [min, max] span and the prep_sample is in the IN-set.
/// This is the case a bare global `BETWEEN block_min AND block_max` would leak.
#[test]
#[serial_test::serial]
#[cfg(feature = "integration")]
fn read_block_selector_split_member_not_at_max_is_exact() {
    let connstr = delete_test_catalog_connstr();
    let data_path = delete_test_data_path();
    let conn = Connection::open_in_memory().unwrap();
    ducklake::connect_ducklake(&conn, &connstr, &data_path).unwrap();
    ducklake::ensure_read_tables(&conn).unwrap();

    let prep_x: i64 = 942_000; // split: full [942010, 942019], block covers only [942010, 942013]
    let prep_y: i64 = 942_001; // whole: [942050, 942051] — holds block_max

    conn.execute_batch(&format!(
        "DELETE FROM qiita_lake.read WHERE prep_sample_idx IN ({prep_x}, {prep_y});
         INSERT INTO qiita_lake.read (prep_sample_idx, sequence_idx, read_id, sequence1) \
             SELECT {prep_x}, s, 'x' || s, 'AAAAA' FROM range(942010, 942020) t(s);
         INSERT INTO qiita_lake.read (prep_sample_idx, sequence_idx, read_id, sequence1) VALUES \
             ({prep_y}, 942050, 'y0', 'CCCCC'), ({prep_y}, 942051, 'y1', 'CCCCA');"
    ))
    .unwrap();

    // prep_x is split at 942013 (< block_max=942051). A global BETWEEN would
    // pull prep_x rows 942014..942019 (in [942010,942051], prep in IN-set);
    // the per-member predicate must exclude them.
    let members = vec![
        auth::BlockReadMember {
            prep_sample_idx: prep_x,
            sequence_idx_start: 942010,
            sequence_idx_stop: 942013,
        },
        auth::BlockReadMember {
            prep_sample_idx: prep_y,
            sequence_idx_start: 942050,
            sequence_idx_stop: 942051,
        },
    ];

    let count = bind_block_read_doget(
        &conn,
        "read_block",
        &auth::TicketFilter::new(),
        &members,
        "block_doget_rows",
    );
    assert_eq!(
        count, 6,
        "4 (prep_x sub-range 942010..942013) + 2 (prep_y) = 6; tail excluded"
    );

    let reader = &conn;
    let rows_rel = "block_doget_rows";
    let x_max: i64 = reader
        .query_row(
            &format!("SELECT max(sequence_idx) FROM {rows_rel} WHERE prep_sample_idx = {prep_x}"),
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        x_max, 942013,
        "split member's out-of-block tail (942014..019) excluded"
    );

    let _ = conn.execute_batch(&format!(
        "DELETE FROM qiita_lake.read WHERE prep_sample_idx IN ({prep_x}, {prep_y});"
    ));
}

#[test]
fn build_query_no_filter() {
    let (sql, _) =
        build_query("reference_sequences", &auth::TicketFilter::new(), &[], &[]).unwrap();
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
    let (sql, _) = build_query("reference_sequences", &filter, &[], &[]).unwrap();
    assert!(sql.contains("feature_idx IN (1,2,3)"));
}

#[test]
fn build_query_rejects_bad_column() {
    let mut filter = auth::TicketFilter::new();
    filter.insert(
        "'; DROP TABLE".to_string(),
        vec![serde_json::Value::from(1)],
    );
    let result = build_query("reference_sequences", &filter, &[], &[]);
    assert!(result.is_err());
}

#[test]
fn build_query_rejects_non_integer_values() {
    let mut filter = auth::TicketFilter::new();
    filter.insert(
        "feature_idx".to_string(),
        vec![serde_json::Value::from("not_an_int")],
    );
    let result = build_query("reference_sequences", &filter, &[], &[]);
    assert!(result.is_err());
}

#[test]
fn build_query_rejects_empty_values() {
    let mut filter = auth::TicketFilter::new();
    filter.insert("feature_idx".to_string(), vec![]);
    let result = build_query("reference_sequences", &filter, &[], &[]);
    assert!(result.is_err());
}

#[test]
fn build_query_sequences_reference_idx_uses_join() {
    let mut filter = auth::TicketFilter::new();
    filter.insert(
        "reference_idx".to_string(),
        vec![serde_json::Value::from(42)],
    );
    let (sql, _) = build_query("reference_sequences", &filter, &[], &[]).unwrap();
    assert!(
        sql.contains("JOIN qiita_lake.reference_membership m ON t.feature_idx = m.feature_idx"),
        "expected JOIN for reference_sequences + reference_idx, got: {sql}"
    );
    assert!(sql.contains("m.reference_idx IN (42)"));
    assert!(sql.starts_with("SELECT t.* FROM"));
}

#[test]
fn build_query_chunks_reference_and_feature_idx_qualifies_columns() {
    // The shape the CP's feature_idx-scoped DoGet ticket mints: BOTH
    // reference_idx (→ membership JOIN) and feature_idx. Under the JOIN,
    // feature_idx lives on both t and m, so it MUST be qualified `t.` or the
    // query fails to bind ("Ambiguous reference to column name feature_idx").
    let mut filter = auth::TicketFilter::new();
    filter.insert(
        "reference_idx".to_string(),
        vec![serde_json::Value::from(5)],
    );
    filter.insert(
        "feature_idx".to_string(),
        vec![
            serde_json::Value::from(800001),
            serde_json::Value::from(800002),
        ],
    );
    let (sql, _) = build_query("reference_sequence_chunks", &filter, &[], &[]).unwrap();
    assert!(
        sql.contains("JOIN qiita_lake.reference_membership m ON t.feature_idx = m.feature_idx"),
        "expected membership JOIN, got: {sql}"
    );
    assert!(sql.contains("m.reference_idx IN (5)"), "got: {sql}");
    assert!(
        sql.contains("t.feature_idx IN (800001,800002)"),
        "feature_idx must be qualified with the base alias under the JOIN, got: {sql}"
    );
    // No unqualified `feature_idx IN` clause (the ambiguous form).
    assert!(
        !sql.contains(" feature_idx IN ("),
        "unqualified feature_idx clause is ambiguous under the JOIN, got: {sql}"
    );
}

#[test]
fn build_query_taxonomy_reference_idx_direct() {
    let mut filter = auth::TicketFilter::new();
    filter.insert(
        "reference_idx".to_string(),
        vec![serde_json::Value::from(42)],
    );
    let (sql, _) = build_query("reference_taxonomy", &filter, &[], &[]).unwrap();
    assert!(
        sql.contains("reference_idx IN (42)"),
        "expected direct filter, got: {sql}"
    );
    assert!(
        !sql.contains("JOIN"),
        "taxonomy should not use JOIN, got: {sql}"
    );
}

/// THE pruning regression guard. `read_masked` must be reached as a scoped
/// MACRO CALL, never as a relation with a `WHERE`: a filtered select puts the
/// prep_sample scope on only one side of the macro's internal join, DuckLake prunes
/// nothing, and the DoGet reads every file in the lake (see the measurements on
/// the macro in ducklake.rs). That failure is invisible in results — the rows
/// are correct, only the cost explodes — so nothing but this shape assertion
/// catches a regression to it.
#[test]
fn build_query_read_masked_calls_the_scoped_macro() {
    let mut filter = auth::TicketFilter::new();
    filter.insert("mask_idx".to_string(), vec![serde_json::Value::from(7)]);
    filter.insert(
        "prep_sample_idx".to_string(),
        vec![serde_json::Value::from(11), serde_json::Value::from(12)],
    );
    let (sql, table) = build_query("read_masked", &filter, &[], &[]).unwrap();
    assert_eq!(table, "qiita_lake.read_masked");
    assert_eq!(sql, "SELECT * FROM qiita_lake.read_masked(7, [11,12])");
    assert!(
        !sql.contains("WHERE"),
        "the scope must be macro arguments, not a WHERE — a WHERE reaches only \
         one side of the join and defeats file pruning. got: {sql}"
    );
}

#[test]
fn build_query_read_masked_requires_its_full_scope() {
    let mask_only = filter_of(&[("mask_idx", vec![serde_json::json!(7)])]);
    assert!(
        build_query("read_masked", &mask_only, &[], &[]).is_err(),
        "a mask with no prep_samples has no macro call — refuse it"
    );

    let preps_only = filter_of(&[("prep_sample_idx", vec![serde_json::json!(11)])]);
    assert!(
        build_query("read_masked", &preps_only, &[], &[]).is_err(),
        "prep_samples with no mask would blend pass-sets from different masks"
    );

    // An empty filter on the human-read surface must never degrade to a
    // fleet-wide read.
    assert!(
        build_query("read_masked", &auth::TicketFilter::new(), &[], &[]).is_err(),
        "empty filter on read_masked must be rejected"
    );

    // Extra columns are refused rather than silently appended as an outer
    // filter: on this surface an unrecognised scope column is a CP bug.
    let extra = filter_of(&[
        ("mask_idx", vec![serde_json::json!(7)]),
        ("prep_sample_idx", vec![serde_json::json!(11)]),
        ("feature_idx", vec![serde_json::json!(1)]),
    ]);
    assert!(
        build_query("read_masked", &extra, &[], &[]).is_err(),
        "read_masked takes exactly its scope, nothing else"
    );

    // sequence_idx is a column of the result but not an allowed scope.
    let bad = filter_of(&[("sequence_idx", vec![serde_json::json!(1)])]);
    assert!(
        build_query("read_masked", &bad, &[], &[]).is_err(),
        "sequence_idx is not an allowed filter column"
    );

    // An explicitly EMPTY prep_sample list. This is the one shape that would
    // otherwise reach `read_masked_relation` and emit `read_masked(7, [])` —
    // which the macro reads as "match nothing", so it would answer zero rows
    // instead of failing. Reject it at the boundary, loudly.
    let empty_preps = filter_of(&[
        ("mask_idx", vec![serde_json::json!(7)]),
        ("prep_sample_idx", vec![]),
    ]);
    assert!(
        build_query("read_masked", &empty_preps, &[], &[]).is_err(),
        "an empty prep_sample_idx list must be refused, not answered with zero rows"
    );
}

// --- block-read DoGet selectors (read_block / read_masked_block) ---

fn block_members() -> Vec<auth::BlockReadMember> {
    vec![
        auth::BlockReadMember {
            prep_sample_idx: 11,
            sequence_idx_start: 100,
            sequence_idx_stop: 199,
        },
        auth::BlockReadMember {
            prep_sample_idx: 12,
            sequence_idx_start: 500,
            sequence_idx_stop: 549,
        },
    ]
}

#[test]
fn build_query_read_block_streams_the_shared_projection_from_raw_read() {
    // Resolves to the raw read table, projects the shared EXPORT_READ_COLUMNS,
    // and scopes with the selector the block DELETE path also uses.
    let members = block_members();
    let (sql, table) =
        build_query("read_block", &auth::TicketFilter::new(), &members, &[]).unwrap();
    assert_eq!(
        table, "qiita_lake.read",
        "read_block is a selector name; it must resolve to the raw read table"
    );
    assert!(
        sql.starts_with(&format!(
            "SELECT {EXPORT_READ_COLUMNS} FROM qiita_lake.read WHERE"
        )),
        "expected the shared export projection, got: {sql}"
    );
    // The exact selector the block DELETE path emits — one translator, so a
    // block's read footprint and its delete footprint cannot drift.
    assert!(
        sql.contains(&block_read_where_clause(&members)),
        "expected the shared block_read_where_clause selector, got: {sql}"
    );
}

#[test]
fn build_query_read_masked_block_scopes_to_one_mask() {
    let members = block_members();
    let mut filter = auth::TicketFilter::new();
    filter.insert("mask_idx".to_string(), vec![serde_json::Value::from(7)]);
    let (sql, table) = build_query("read_masked_block", &filter, &members, &[]).unwrap();
    assert_eq!(table, "qiita_lake.read_masked");
    // The mask AND the block's prep_samples move into the macro call, so both of
    // the macro's inputs are pruned; the member clause stays outside because
    // it carries the per-prep_sample sequence sub-ranges the scope cannot express.
    let preps = block_member_preps(&members);
    assert!(
        sql.starts_with(&format!(
            "SELECT {EXPORT_READ_COLUMNS} FROM {} WHERE ",
            read_masked_relation(7, &preps)
        )),
        "expected a scoped macro call carrying the block's prep_samples, got: {sql}"
    );
    assert!(
        sql.contains(&block_read_where_clause(&members)),
        "got: {sql}"
    );
}

/// The block path must not regress to scanning the lake either: every member's
/// prep_sample has to reach the macro's argument list, not just the outer clause.
#[test]
fn build_query_read_masked_block_passes_every_block_prep_sample_into_the_macro() {
    let members = block_members();
    let filter = filter_of(&[("mask_idx", vec![serde_json::json!(7)])]);
    let (sql, _) = build_query("read_masked_block", &filter, &members, &[]).unwrap();
    let scope = read_masked_relation(7, &block_member_preps(&members));
    assert!(sql.contains(&scope), "expected {scope} in: {sql}");
    for m in &members {
        assert!(
            scope.contains(&m.prep_sample_idx.to_string()),
            "member {} missing from the macro scope {scope}",
            m.prep_sample_idx
        );
    }
}

#[test]
fn build_query_block_read_rejects_empty_members() {
    // THE load-bearing guard: an empty selector must never degrade to "all
    // reads". This is what makes exposing raw `read` via read_block
    // admissible at all (see the PRIVACY note on ALLOWED_TABLES).
    assert!(
        build_query("read_block", &auth::TicketFilter::new(), &[], &[]).is_err(),
        "read_block with no members must be rejected"
    );
    let mut filter = auth::TicketFilter::new();
    filter.insert("mask_idx".to_string(), vec![serde_json::Value::from(7)]);
    assert!(
        build_query("read_masked_block", &filter, &[], &[]).is_err(),
        "read_masked_block with no members must be rejected"
    );
}

#[test]
fn build_query_read_masked_block_requires_exactly_one_mask_idx() {
    let members = block_members();
    // Absent: would blend every mask's pass-set for those ranges.
    assert!(
        build_query(
            "read_masked_block",
            &auth::TicketFilter::new(),
            &members,
            &[]
        )
        .is_err(),
        "a masked block without its mask scope must be rejected"
    );
    // Multi-valued: same blending, just spelled differently.
    let mut multi = auth::TicketFilter::new();
    multi.insert(
        "mask_idx".to_string(),
        vec![serde_json::Value::from(7), serde_json::Value::from(8)],
    );
    assert!(
        build_query("read_masked_block", &multi, &members, &[]).is_err(),
        "a multi-valued mask_idx must be rejected"
    );
    // Extra columns: the ticket shape is pinned, not merely sufficient.
    let mut extra = auth::TicketFilter::new();
    extra.insert("mask_idx".to_string(), vec![serde_json::Value::from(7)]);
    extra.insert(
        "prep_sample_idx".to_string(),
        vec![serde_json::Value::from(11)],
    );
    assert!(
        build_query("read_masked_block", &extra, &members, &[]).is_err(),
        "an unexpected extra filter column must be rejected"
    );
}

#[test]
fn build_query_read_block_rejects_any_filter() {
    // read_block is scoped by members alone. A filter here would be a
    // control-plane bug, and silently ignoring it could under-scope.
    let mut filter = auth::TicketFilter::new();
    filter.insert(
        "prep_sample_idx".to_string(),
        vec![serde_json::Value::from(11)],
    );
    assert!(
        build_query("read_block", &filter, &block_members(), &[]).is_err(),
        "read_block must reject filter columns"
    );
}

#[test]
fn build_query_rejects_members_on_a_non_block_table() {
    // A stray selector on a normal ticket must fail loudly rather than be
    // dropped — a dropped selector is a silently WIDER read than intended.
    let mut filter = auth::TicketFilter::new();
    filter.insert("mask_idx".to_string(), vec![serde_json::Value::from(7)]);
    filter.insert(
        "prep_sample_idx".to_string(),
        vec![serde_json::Value::from(11)],
    );
    assert!(
        build_query("read_masked", &filter, &block_members(), &[]).is_err(),
        "read_masked must reject a block members selector"
    );
    assert!(
        build_query(
            "reference_sequences",
            &auth::TicketFilter::new(),
            &block_members(),
            &[]
        )
        .is_err(),
        "a reference table must reject a block members selector"
    );
}

#[test]
fn allowed_tables_excludes_the_bare_raw_read_tables() {
    // Pins the PRIVACY invariant on ALLOWED_TABLES: raw reads are reachable
    // only through the members-scoped `read_block` selector, never as a
    // whole-table name that an empty filter could turn into a full scan.
    for forbidden in ["read", "read_mask"] {
        assert!(
            !ALLOWED_TABLES.contains(&forbidden),
            "{forbidden:?} must never be a DoGet table name"
        );
    }
    for (selector, _) in BLOCK_READ_SOURCES {
        assert!(
            ALLOWED_TABLES.contains(selector),
            "block-read selector {selector:?} must be in ALLOWED_TABLES to be reachable"
        );
    }
}

#[test]
fn exclusion_views_are_doget_allowed_and_raw_bases_are_not() {
    // The alignment / taxonomy DoGet surfaces are the exclusion-aware VIEWS,
    // never the raw base tables — so a curated exclusion cannot be bypassed
    // by any consumer (the read_masked-over-read model). do_get gates on
    // ALLOWED_TABLES, so the raw names being absent makes them Flight-unreachable.
    assert!(
        ALLOWED_TABLES.contains(&"alignment_visible"),
        "alignment_visible must be DoGet-readable for the feature-table consumer"
    );
    assert!(
        ALLOWED_TABLES.contains(&"reference_taxonomy_visible"),
        "reference_taxonomy_visible must be DoGet-readable for the shard planner"
    );
    assert!(
        !ALLOWED_TABLES.contains(&"alignment"),
        "raw alignment must NOT be Flight-reachable (bypasses exclusion)"
    );
    assert!(
        !ALLOWED_TABLES.contains(&"reference_taxonomy"),
        "raw reference_taxonomy must NOT be Flight-reachable (bypasses exclusion)"
    );
    assert!(
        ALLOWED_FILTER_COLUMNS.contains(&"alignment_idx"),
        "alignment_idx must be an allowed filter column"
    );
}

#[test]
fn build_query_raw_alignment_is_not_the_doget_surface() {
    // Inverted canary: the raw base table is NOT the alignment DoGet surface
    // (only `alignment_visible` is). do_get can't reach it (out of
    // ALLOWED_TABLES), but build_query is a pure function tested directly — so
    // this pins the deliberate design that if `"alignment"` were ever re-added
    // to ALLOWED_TABLES by mistake, it would fall through to a bare, unscoped
    // `SELECT *` (an obviously-malformed dump, loudly wrong), NOT a clean
    // projected result that silently bypasses exclusion.
    let mut filter = auth::TicketFilter::new();
    filter.insert(
        "alignment_idx".to_string(),
        vec![serde_json::Value::from(7)],
    );
    let (sql, table) = build_query("alignment", &filter, &[], &[]).unwrap();
    assert_eq!(table, "qiita_lake.alignment");
    assert!(
        sql.starts_with("SELECT * FROM qiita_lake.alignment WHERE"),
        "raw alignment must NOT get the projection — expected a bare SELECT *, got: {sql}"
    );
    // And no mandatory-scope guard: an empty filter is not refused (unlike the
    // view), further proof it is not the special surface.
    let empty = auth::TicketFilter::new();
    assert!(
        build_query("alignment", &empty, &[], &[]).is_ok(),
        "raw alignment gets no alignment_idx requirement (it is not the surface)"
    );
}

#[test]
fn build_query_alignment_visible_gets_projection_and_scope() {
    // The exclusion-aware view is the ONLY Flight-reachable alignment name and
    // the sole alignment DoGet surface: projected to the coverage/OGU columns,
    // scoped by alignment_idx, no membership JOIN — the query targets the view,
    // so the anti-join drops blocked features before projection.
    let mut filter = auth::TicketFilter::new();
    filter.insert(
        "alignment_idx".to_string(),
        vec![serde_json::Value::from(7)],
    );
    filter.insert(
        "prep_sample_idx".to_string(),
        vec![serde_json::Value::from(3), serde_json::Value::from(4)],
    );
    let cols = columns(&["prep_sample_idx", "feature_idx", "position"]);
    let (sql, table) = build_query("alignment_visible", &filter, &[], &cols).unwrap();
    assert_eq!(table, "qiita_lake.alignment_visible");
    assert!(
        sql.starts_with(
            "SELECT prep_sample_idx, feature_idx, position \
             FROM qiita_lake.alignment_visible WHERE"
        ),
        "the view must get the ticket's signed columns, got: {sql}"
    );
    assert!(sql.contains("alignment_idx IN (7)"), "got: {sql}");
    assert!(sql.contains("prep_sample_idx IN (3,4)"), "got: {sql}");
    assert!(
        !sql.contains("JOIN"),
        "no membership JOIN for the view, got: {sql}"
    );
    assert!(!sql.contains("SELECT *"), "must project, got: {sql}");
}

#[test]
fn build_query_alignment_visible_requires_alignment_idx() {
    // The scoping guard applies to the view too — an omitted alignment_idx is
    // refused, so a ticket can't blend heterogeneous runs through the view.
    let mut filter = auth::TicketFilter::new();
    filter.insert(
        "prep_sample_idx".to_string(),
        vec![serde_json::Value::from(3)],
    );
    assert!(
        build_query("alignment_visible", &filter, &[], &[]).is_err(),
        "alignment_visible without alignment_idx must be rejected"
    );
    let empty = auth::TicketFilter::new();
    assert!(
        build_query("alignment_visible", &empty, &[], &[]).is_err(),
        "empty filter on alignment_visible must be rejected"
    );
}

/// A `feature_idx` filter, the only scope an assembly ticket carries.
fn feature_scope(values: &[i64]) -> auth::TicketFilter {
    let mut filter = auth::TicketFilter::new();
    filter.insert(
        "feature_idx".to_string(),
        values.iter().map(|v| serde_json::Value::from(*v)).collect(),
    );
    filter
}

#[test]
fn assembly_surfaces_are_doget_allowed_and_the_junction_is_not() {
    // The two sequence surfaces are Flight-readable, and so is the per-subject
    // quality the feature-table resolver reads. `assembly_membership` is read to
    // RESOLVE the sequence surfaces' scope (`build_assembly_run_query`) but is not
    // a table a ticket can name; the allowlist carries why.
    for readable in [
        "assembled_sequence",
        "assembled_sequence_chunks",
        "bin_quality",
    ] {
        assert!(
            ALLOWED_TABLES.contains(&readable),
            "{readable:?} must be DoGet-readable"
        );
    }
    assert!(
        !ALLOWED_TABLES.contains(&"assembly_membership"),
        "assembly_membership must not be Flight-reachable"
    );
}

/// A well-formed assembly ticket filter: the run, and only the run.
fn assembly_run_scope(prep_sample_idx: i64, processing_idx: i64) -> auth::TicketFilter {
    filter_of(&[
        ("prep_sample_idx", vec![serde_json::json!(prep_sample_idx)]),
        ("processing_idx", vec![serde_json::json!(processing_idx)]),
    ])
}

#[test]
fn build_query_assembly_resolves_the_run_through_membership() {
    for table in ["assembled_sequence", "assembled_sequence_chunks"] {
        let (sql, full) = build_query(table, &assembly_run_scope(42, 7), &[], &[]).unwrap();
        assert_eq!(full, format!("qiita_lake.{table}"));
        assert_eq!(
            sql,
            format!(
                "SELECT * FROM qiita_lake.{table} WHERE feature_idx IN (\
                 SELECT feature_idx FROM qiita_lake.assembly_membership \
                 WHERE prep_sample_idx = 42 AND processing_idx = 7)"
            ),
            "got: {sql}"
        );
    }
}

#[test]
fn build_query_assembly_requires_exactly_the_run_key() {
    // Every rejected shape below would stream contigs the named run did not
    // produce, or none at all.
    let empty = auth::TicketFilter::new();
    let cases: &[(&str, auth::TicketFilter)] = &[
        ("empty filter", empty.clone()),
        (
            "prep_sample_idx alone",
            filter_of(&[("prep_sample_idx", vec![serde_json::json!(42)])]),
        ),
        (
            "processing_idx alone",
            filter_of(&[("processing_idx", vec![serde_json::json!(7)])]),
        ),
        (
            "two runs",
            filter_of(&[
                ("prep_sample_idx", vec![serde_json::json!(42)]),
                (
                    "processing_idx",
                    vec![serde_json::json!(7), serde_json::json!(8)],
                ),
            ]),
        ),
        (
            "two prep_samples",
            filter_of(&[
                (
                    "prep_sample_idx",
                    vec![serde_json::json!(42), serde_json::json!(43)],
                ),
                ("processing_idx", vec![serde_json::json!(7)]),
            ]),
        ),
        (
            "the run plus a named contig",
            filter_of(&[
                ("prep_sample_idx", vec![serde_json::json!(42)]),
                ("processing_idx", vec![serde_json::json!(7)]),
                ("feature_idx", vec![serde_json::json!(11)]),
            ]),
        ),
        ("a contig roster alone", feature_scope(&[11, 22, 33])),
    ];
    for table in ["assembled_sequence", "assembled_sequence_chunks"] {
        for (label, filter) in cases {
            assert!(
                build_query(table, filter, &[], &[]).is_err(),
                "{table} must reject {label}"
            );
        }
    }
    // Control: the same empty filter IS served for a reference table, so the
    // first case above is about these tables and not about empty filters.
    assert!(build_query("reference_sequences", &empty, &[], &[]).is_ok());
}

#[test]
fn build_query_assembly_takes_no_projection_and_no_members() {
    // No projection allowlist (only the alignment surface has one), so a
    // signed column list is refused rather than dropped; and `members` is
    // only meaningful for the block-read selectors.
    for table in ["assembled_sequence", "assembled_sequence_chunks"] {
        assert!(
            build_query(
                table,
                &assembly_run_scope(42, 7),
                &[],
                &columns(&["feature_idx"])
            )
            .is_err(),
            "{table} must reject a projection column list"
        );
        assert!(
            build_query(table, &assembly_run_scope(42, 7), &block_members(), &[]).is_err(),
            "{table} must reject a block members selector"
        );
    }
}

#[test]
fn build_query_assembly_membership_reaches_no_output_column() {
    // The junction answers "which contigs are this run's" and stops there:
    // the subquery yields feature_idx, so no membership column can ride out
    // on a stream whose table is not it (see the allowlist test above).
    let (sql, full) = build_query(
        "assembled_sequence_chunks",
        &assembly_run_scope(42, 7),
        &[],
        &[],
    )
    .unwrap();
    assert_eq!(full, "qiita_lake.assembled_sequence_chunks");
    assert!(sql.starts_with("SELECT * FROM qiita_lake.assembled_sequence_chunks"));
    assert!(
        !sql.contains("kind") && !sql.contains("bin_id"),
        "got: {sql}"
    );
}

/// A well-formed `bin_quality` ticket filter: one run, over a cohort.
fn bin_quality_scope(prep_sample_idx: &[i64], processing_idx: i64) -> auth::TicketFilter {
    filter_of(&[
        (
            "prep_sample_idx",
            prep_sample_idx
                .iter()
                .map(|v| serde_json::json!(v))
                .collect(),
        ),
        ("processing_idx", vec![serde_json::json!(processing_idx)]),
    ])
}

#[test]
fn build_query_bin_quality_scopes_the_run_on_its_own_columns() {
    // No `assembly_membership` semi join: this table carries both halves of the
    // run key itself, which is why it is a separate builder rather than another
    // `is_assembly_run_surface` arm.
    let (sql, full) =
        build_query("bin_quality", &bin_quality_scope(&[42, 43], 7), &[], &[]).unwrap();
    assert_eq!(full, "qiita_lake.bin_quality");
    assert_eq!(
        sql,
        "SELECT * FROM qiita_lake.bin_quality \
         WHERE processing_idx = 7 AND prep_sample_idx IN (42, 43)",
        "got: {sql}"
    );
    assert!(
        !sql.contains("assembly_membership"),
        "the run scope is this table's own columns; got: {sql}"
    );
}

#[test]
fn build_query_bin_quality_requires_exactly_the_run_key() {
    // Each rejected shape below returns quality rows for subjects outside the
    // run the resolver asked about — a genome's completeness attributed to the
    // wrong assembly is a wrong gate decision, not a missing one.
    let empty = auth::TicketFilter::new();
    let cases: &[(&str, auth::TicketFilter)] = &[
        // Every prep_sample's every run.
        ("empty filter", empty.clone()),
        // Every run those prep_samples ever had.
        (
            "prep_sample_idx alone",
            filter_of(&[("prep_sample_idx", vec![serde_json::json!(42)])]),
        ),
        // Every prep_sample that run touched, cohort or not.
        (
            "processing_idx alone",
            filter_of(&[("processing_idx", vec![serde_json::json!(7)])]),
        ),
        // Two runs blended into one indistinguishable stream.
        (
            "two runs",
            filter_of(&[
                ("prep_sample_idx", vec![serde_json::json!(42)]),
                (
                    "processing_idx",
                    vec![serde_json::json!(7), serde_json::json!(8)],
                ),
            ]),
        ),
        // An empty cohort must never degrade to "every prep_sample".
        (
            "empty cohort",
            filter_of(&[
                ("prep_sample_idx", vec![]),
                ("processing_idx", vec![serde_json::json!(7)]),
            ]),
        ),
        // A third column riding along is what `filter.len()` catches.
        (
            "the run plus a named contig",
            filter_of(&[
                ("prep_sample_idx", vec![serde_json::json!(42)]),
                ("processing_idx", vec![serde_json::json!(7)]),
                ("feature_idx", vec![serde_json::json!(11)]),
            ]),
        ),
    ];
    for (label, filter) in cases {
        assert!(
            build_query("bin_quality", filter, &[], &[]).is_err(),
            "bin_quality must reject {label}"
        );
    }
    // Control: the same empty filter IS served for a reference table, so the
    // first case is about this table and not about empty filters in general.
    assert!(build_query("reference_sequences", &empty, &[], &[]).is_ok());
}

#[test]
fn build_query_bin_quality_takes_no_projection_and_no_members() {
    // Same two refusals the assembly surfaces make: only the alignment surface
    // is projectable, and `members` is only meaningful for the block selectors.
    assert!(
        build_query(
            "bin_quality",
            &bin_quality_scope(&[42], 7),
            &[],
            &columns(&["completeness"])
        )
        .is_err(),
        "bin_quality must reject a projection column list"
    );
    assert!(
        build_query(
            "bin_quality",
            &bin_quality_scope(&[42], 7),
            &block_members(),
            &[]
        )
        .is_err(),
        "bin_quality must reject a block members selector"
    );
}

/// A minimally-scoped alignment ticket filter. The scoping guards have their
/// own tests above; the projection tests below care only about `columns`.
fn alignment_scope() -> auth::TicketFilter {
    let mut filter = auth::TicketFilter::new();
    filter.insert(
        "alignment_idx".to_string(),
        vec![serde_json::Value::from(7)],
    );
    filter.insert(
        "prep_sample_idx".to_string(),
        vec![serde_json::Value::from(3)],
    );
    filter
}

fn columns(names: &[&str]) -> Vec<String> {
    names.iter().map(|s| s.to_string()).collect()
}

#[test]
fn signed_columns_are_projected_in_order() {
    // The whole point of the signed list: a consumer that wants `cigar` asks
    // for it, and one that doesn't never pays for it. Order is the caller's,
    // verbatim — it keeps the SQL a pure function of the ticket, and the
    // consumer's Arrow schema predictable rather than a function of our
    // allowlist's ordering.
    let cols = columns(&["feature_idx", "cigar", "position"]);
    let (sql, table) = build_query("alignment_visible", &alignment_scope(), &[], &cols).unwrap();
    assert_eq!(table, "qiita_lake.alignment_visible");
    assert!(
        sql.starts_with(
            "SELECT feature_idx, cigar, position FROM qiita_lake.alignment_visible WHERE"
        ),
        "expected exactly the signed columns, in the signed order, got: {sql}"
    );
}

#[test]
fn unknown_projection_column_is_rejected() {
    // Defense-in-depth. The list is signature-verified — the control plane
    // set it, not the client — but column names are interpolated into SQL,
    // so it is whitelisted anyway, exactly as ALLOWED_FILTER_COLUMNS is.
    for bad in ["no_such_column", "feature_idx; DROP TABLE alignment", "*"] {
        let cols = columns(&["feature_idx", bad]);
        let err = build_query("alignment_visible", &alignment_scope(), &[], &cols)
            .expect_err("unknown projection column must be rejected");
        assert!(
            err.message().contains(bad),
            "the error should name the offending column, got: {err}"
        );
    }
}

#[test]
fn duplicate_projection_columns_are_rejected() {
    // A repeated name produces two identically-named Arrow fields, which
    // consumers collapse or reject inconsistently. Refuse to emit the
    // ambiguous schema rather than pick a behaviour on their behalf.
    let cols = columns(&["feature_idx", "position", "feature_idx"]);
    assert!(
        build_query("alignment_visible", &alignment_scope(), &[], &cols).is_err(),
        "a duplicated projection column must be rejected"
    );
}

#[test]
fn projection_columns_are_rejected_on_a_table_with_no_allowlist() {
    // Only the alignment surface takes a column list; every other table
    // streams SELECT * by decision. A list elsewhere is a control-plane bug,
    // and *ignoring* it would serve wider rows than the ticket asked for —
    // the exact silent widening this whole mechanism exists to prevent.
    let cols = columns(&["feature_idx"]);

    let mut reference = auth::TicketFilter::new();
    reference.insert(
        "reference_idx".to_string(),
        vec![serde_json::Value::from(42)],
    );
    assert!(
        build_query("reference_taxonomy", &reference, &[], &cols).is_err(),
        "a reference table must refuse a projection column list"
    );

    // The block-read and read_masked selectors build their SQL on their own
    // early-return paths, so they are the cases that would silently skip a
    // gate placed further down build_query. Pin them explicitly.
    assert!(
        build_query(
            "read_block",
            &auth::TicketFilter::new(),
            &block_members(),
            &cols
        )
        .is_err(),
        "a block-read selector must refuse a projection column list"
    );
    let mut masked = auth::TicketFilter::new();
    masked.insert("mask_idx".to_string(), vec![serde_json::Value::from(1)]);
    masked.insert(
        "prep_sample_idx".to_string(),
        vec![serde_json::Value::from(3)],
    );
    assert!(
        build_query("read_masked", &masked, &[], &cols).is_err(),
        "the read_masked macro must refuse a projection column list"
    );
}

/// The allowlist must equal the `alignment` DDL's column list — checked here
/// without a catalog, so `make test` catches a drift.
///
/// `projection_allowlist_matches_the_alignment_schema_exactly` checks the same
/// property against a LIVE catalog, which needs Postgres and therefore only
/// runs in the integration tier: a column added to the DDL without an
/// allowlist entry would stay green through every pure-unit run until then.
/// This closes that window by reading the DDL out of the source at compile
/// time. `alignment_visible` is `SELECT a.*` over this table, so the view's
/// column set is this one — which is why the cheap check is worth having even
/// though the live one is stricter.
#[test]
fn projection_allowlist_matches_the_alignment_ddl() {
    const DUCKLAKE_SRC: &str = include_str!("ducklake.rs");

    let start = DUCKLAKE_SRC
        .find("CREATE TABLE IF NOT EXISTS qiita_lake.alignment (")
        .expect("the alignment DDL moved; this test reads it out of ducklake.rs");
    let tail = &DUCKLAKE_SRC[start..];
    let body = &tail[..tail
        .find(");")
        .expect("unterminated alignment CREATE TABLE")];

    let mut ddl_columns: std::collections::BTreeSet<&str> = std::collections::BTreeSet::new();
    for line in body.lines().skip(1) {
        let line = line.trim();
        if line.is_empty() || line.starts_with("--") {
            continue;
        }
        ddl_columns.insert(line.split_whitespace().next().expect("non-empty line"));
    }
    assert!(
        ddl_columns.len() > 5,
        "parsed only {ddl_columns:?} out of the alignment DDL — the shape changed \
         and this test is no longer reading it"
    );

    let allowed: std::collections::BTreeSet<&str> =
        ALIGNMENT_PROJECTION_COLUMNS.iter().copied().collect();
    assert_eq!(
        ddl_columns,
        allowed,
        "the projection allowlist and the alignment DDL have drifted; \
         only in the DDL: {:?}; only in the allowlist: {:?}",
        ddl_columns.difference(&allowed).collect::<Vec<_>>(),
        allowed.difference(&ddl_columns).collect::<Vec<_>>()
    );
}

/// The membership-JOIN arm of `build_query` hardcodes `SELECT t.*`, so a
/// projection reaching it would be silently dropped and the stream would carry
/// wider rows than the ticket signed.
///
/// Today nothing can: no joined table has an allowlist. That is an invariant,
/// not a coincidence, and this is where it is enforced — adding an allowlist to
/// a joined table fails here, at unit time, instead of at runtime behind a
/// guard someone may have decided was dead code.
#[test]
fn no_membership_join_table_has_a_projection_allowlist() {
    for table in MEMBERSHIP_JOIN_TABLES {
        assert!(
            projection_allowlist(table).is_none(),
            "{table} takes the membership JOIN, whose SELECT t.* would silently \
             drop a projection — teach build_query to qualify the columns with \
             the base alias before giving it an allowlist"
        );
    }
}

#[test]
fn alignment_doget_without_columns_is_rejected() {
    // The alignment surface has no default projection any more: the consumer
    // names its columns or gets nothing. Falling back to a server-side list
    // would put the wrong component in charge of the answer — only the job
    // knows what it binds — and a fallback that drifted wider would ship
    // `cigar` to callers that never asked.
    //
    // Empty and absent are one case, not two, and cannot be told apart here:
    // `#[serde(default)]` renders an omitted field as an empty Vec, exactly
    // as it does for `members`. Both are refused. An explicitly-empty list
    // is additionally refused at mint, which is the one layer where the
    // distinction still exists.
    let err = build_query("alignment_visible", &alignment_scope(), &[], &[])
        .expect_err("an alignment ticket with no column list must be rejected");
    assert_eq!(err.code(), tonic::Code::InvalidArgument, "got: {err}");

    // The guard is specific to the projected surface: every other table
    // still streams SELECT * with no column list, which is what keeps this
    // change scoped to the one surface that needed it.
    let mut reference = auth::TicketFilter::new();
    reference.insert(
        "reference_idx".to_string(),
        vec![serde_json::Value::from(42)],
    );
    assert!(
        build_query("reference_taxonomy", &reference, &[], &[]).is_ok(),
        "an unprojected table must not have acquired a column requirement"
    );
}

#[test]
fn build_query_taxonomy_visible_scopes_by_reference_idx_direct() {
    // The taxonomy view carries reference_idx (SELECT t.* over the base), so a
    // reference-scoped DoGet is a direct WHERE — no membership JOIN — and it
    // streams every column of the view (the anti-join having dropped blocked
    // features' rows).
    let mut filter = auth::TicketFilter::new();
    filter.insert(
        "reference_idx".to_string(),
        vec![serde_json::Value::from(42)],
    );
    let (sql, table) = build_query("reference_taxonomy_visible", &filter, &[], &[]).unwrap();
    assert_eq!(table, "qiita_lake.reference_taxonomy_visible");
    assert_eq!(
        sql,
        "SELECT * FROM qiita_lake.reference_taxonomy_visible WHERE reference_idx IN (42)"
    );
    assert!(
        !sql.contains("JOIN"),
        "taxonomy has reference_idx, no JOIN: {sql}"
    );
}

#[test]
fn build_query_alignment_visible_rejects_multivalued_alignment_idx() {
    // A feature table is built for ONE alignment run; alignment_idx is dropped
    // from the projection, so several values would blend heterogeneous runs
    // into one indistinguishable stream — refuse it. (The empty-filter and
    // missing-alignment_idx rejections are covered by
    // build_query_alignment_visible_requires_alignment_idx.)
    let mut filter = auth::TicketFilter::new();
    filter.insert(
        "alignment_idx".to_string(),
        vec![serde_json::Value::from(7), serde_json::Value::from(8)],
    );
    filter.insert(
        "prep_sample_idx".to_string(),
        vec![serde_json::Value::from(3)],
    );
    assert!(
        build_query("alignment_visible", &filter, &[], &[]).is_err(),
        "alignment_visible DoGet with multi-valued alignment_idx must be rejected"
    );
}

#[tokio::test]
#[serial_test::serial]
#[cfg(feature = "integration")]
async fn build_query_alignment_visible_streams_projected_columns() {
    // End-to-end through the VIEW (the only Flight-reachable alignment name):
    // the projected column list must match the real alignment schema, and the
    // prep_sample_idx scope must exclude out-of-cohort rows. No exclusions are
    // seeded here (a sibling test covers the anti-join); this pins projection +
    // scope over the view.
    let connstr = delete_test_catalog_connstr();
    let data_path = delete_test_data_path();

    let align: i64 = 962_000;
    let prep_a: i64 = 962_010;
    let prep_b: i64 = 962_011;
    let prep_other: i64 = 962_012;
    {
        let conn = Connection::open_in_memory().unwrap();
        ducklake::connect_ducklake(&conn, &connstr, &data_path).unwrap();
        // alignment_visible is a view over alignment; ensure_exclusion_tables
        // creates it (and needs reference_taxonomy + alignment to exist first).
        ducklake::ensure_reference_tables(&conn).unwrap();
        ducklake::ensure_alignment_tables(&conn).unwrap();
        ducklake::ensure_exclusion_tables(&conn).unwrap();
        conn.execute_batch(&format!(
            "DELETE FROM qiita_lake.alignment WHERE alignment_idx = {align};
             INSERT INTO qiita_lake.alignment \
                 (alignment_idx, prep_sample_idx, sequence_idx, feature_idx, \
                  flags, position, stop_position) VALUES \
                 ({align}, {prep_a}, 1, 10, 0, 100, 200), \
                 ({align}, {prep_b}, 2, 11, 0, 300, 400), \
                 ({align}, {prep_other}, 3, 12, 0, 500, 600);"
        ))
        .unwrap();
    }

    let mut filter = auth::TicketFilter::new();
    filter.insert(
        "alignment_idx".to_string(),
        vec![serde_json::Value::from(align)],
    );
    filter.insert(
        "prep_sample_idx".to_string(),
        vec![
            serde_json::Value::from(prep_a),
            serde_json::Value::from(prep_b),
        ],
    );
    let (sql, table) = build_query(
        "alignment_visible",
        &filter,
        &[],
        &columns(FEATURE_TABLE_COLUMNS),
    )
    .unwrap();
    let batches: Vec<arrow_array::RecordBatch> =
        stream_ducklake_batches(connstr.clone(), data_path.clone(), sql, table)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(|r| r.expect("stream item should be Ok"))
            .collect();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total_rows, 2,
        "prep_other must be excluded by the prep_sample_idx scope"
    );
    let names: Vec<String> = batches[0]
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    assert_eq!(
        names,
        vec![
            "prep_sample_idx",
            "sequence_idx",
            "feature_idx",
            "flags",
            "position",
            "stop_position"
        ],
        "only the projected coverage/OGU columns stream, in order"
    );

    let conn = Connection::open_in_memory().unwrap();
    ducklake::connect_ducklake(&conn, &connstr, &data_path).unwrap();
    let _ = conn.execute_batch(&format!(
        "DELETE FROM qiita_lake.alignment WHERE alignment_idx = {align};"
    ));
}

/// The Flight query path over `alignment_visible` drops a blocked feature's
/// rows before they stream: seed two features under one alignment run, block
/// one in the exclusion mirror, and assert only the unblocked feature's row
/// survives the projected DoGet query (the anti-join enforced at read time,
/// no aligner index rebuilt). This exercises the alignment anti-join view
/// end-to-end through `build_query` + `stream_ducklake_batches`.
#[tokio::test]
#[serial_test::serial]
#[cfg(feature = "integration")]
async fn alignment_visible_doget_omits_blocked_feature() {
    let connstr = delete_test_catalog_connstr();
    let data_path = delete_test_data_path();

    let align: i64 = 976_000;
    let prep: i64 = 976_010;
    let feat_keep: i64 = 976_100;
    let feat_blocked: i64 = 976_101;
    {
        let conn = Connection::open_in_memory().unwrap();
        ducklake::connect_ducklake(&conn, &connstr, &data_path).unwrap();
        ducklake::ensure_reference_tables(&conn).unwrap();
        ducklake::ensure_alignment_tables(&conn).unwrap();
        ducklake::ensure_exclusion_tables(&conn).unwrap();
        conn.execute_batch(&format!(
            "DELETE FROM qiita_lake.alignment WHERE alignment_idx = {align};
             DELETE FROM qiita_lake.reference_exclusion \
                 WHERE feature_idx IN ({feat_keep}, {feat_blocked});
             INSERT INTO qiita_lake.alignment \
                 (alignment_idx, prep_sample_idx, sequence_idx, feature_idx, \
                  flags, position, stop_position) VALUES \
                 ({align}, {prep}, 1, {feat_keep}, 0, 100, 200), \
                 ({align}, {prep}, 2, {feat_blocked}, 0, 300, 400);
             INSERT INTO qiita_lake.reference_exclusion (feature_idx) VALUES ({feat_blocked});"
        ))
        .unwrap();
    }

    let mut filter = auth::TicketFilter::new();
    filter.insert(
        "alignment_idx".to_string(),
        vec![serde_json::Value::from(align)],
    );
    filter.insert(
        "prep_sample_idx".to_string(),
        vec![serde_json::Value::from(prep)],
    );
    let (sql, table) = build_query(
        "alignment_visible",
        &filter,
        &[],
        &columns(FEATURE_TABLE_COLUMNS),
    )
    .unwrap();
    let batches: Vec<arrow_array::RecordBatch> =
        stream_ducklake_batches(connstr.clone(), data_path.clone(), sql, table)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(|r| r.expect("stream item should be Ok"))
            .collect();

    // Only the unblocked feature's row survives the anti-join.
    let feature_col: Vec<i64> = batches
        .iter()
        .flat_map(|b| {
            let col = b
                .column_by_name("feature_idx")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow_array::Int64Array>()
                .unwrap();
            (0..col.len()).map(|i| col.value(i)).collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(
        feature_col,
        vec![feat_keep],
        "the blocked feature's alignment row must not stream through the view"
    );

    let conn = Connection::open_in_memory().unwrap();
    ducklake::connect_ducklake(&conn, &connstr, &data_path).unwrap();
    let _ = conn.execute_batch(&format!(
        "DELETE FROM qiita_lake.alignment WHERE alignment_idx = {align};
         DELETE FROM qiita_lake.reference_exclusion WHERE feature_idx = {feat_blocked};"
    ));
}

#[test]
fn build_query_reference_table_allows_empty_filter() {
    // Reference tables are broadly readable by design (mirrors the
    // anonymous REST reference GET), so an unfiltered SELECT is legitimate.
    let empty = auth::TicketFilter::new();
    let (sql, table) = build_query("reference_sequences", &empty, &[], &[])
        .expect("empty filter on a reference table is allowed");
    assert_eq!(table, "qiita_lake.reference_sequences");
    assert_eq!(sql, "SELECT * FROM qiita_lake.reference_sequences");
}

// ------------------------------------------------------------------
// DoPut handler tests
// ------------------------------------------------------------------

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use ed25519_dalek::{Signer, SigningKey, VerifyingKey};
use sha2::Sha256;
use std::os::unix::fs::PermissionsExt;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

// Fixed test keypair; WRONG_SEED signs tickets that must NOT verify.
const TEST_SEED: [u8; 32] = [7u8; 32];
const WRONG_SEED: [u8; 32] = [9u8; 32];

fn test_vk() -> VerifyingKey {
    SigningKey::from_bytes(&TEST_SEED).verifying_key()
}

fn sign_doput_for_test(upload_idx: i64, seed: &[u8; 32], expiry: u64) -> Vec<u8> {
    let payload = format!(r#"{{"action":"doput","upload_idx":{upload_idx}}}"#);
    sign_raw(payload.as_bytes(), seed, expiry)
}

fn sign_raw(payload: &[u8], seed: &[u8; 32], expiry: u64) -> Vec<u8> {
    let version: u8 = 2;
    let payload_len = (payload.len() as u32).to_be_bytes();
    let expiry_bytes = expiry.to_be_bytes();
    let signed_input = [&[version][..], &payload_len[..], payload, &expiry_bytes[..]].concat();
    let sig = SigningKey::from_bytes(seed).sign(&signed_input).to_bytes();
    let mut ticket = Vec::new();
    ticket.push(version);
    ticket.extend_from_slice(&payload_len);
    ticket.extend_from_slice(payload);
    ticket.extend_from_slice(&sig);
    ticket.extend_from_slice(&expiry_bytes);
    ticket
}

fn future_expiry_secs(secs: u64) -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + secs
}

/// Build a tiny test RecordBatch — schema is arbitrary, DoPut is content-agnostic.
fn sample_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("read_id", DataType::Utf8, false),
        Field::new("seq_length", DataType::Int64, false),
    ]));
    let read_ids = Arc::new(StringArray::from(vec!["r1", "r2", "r3"]));
    let lengths = Arc::new(Int64Array::from(vec![12i64, 34, 56]));
    RecordBatch::try_new(schema, vec![read_ids, lengths]).unwrap()
}

/// Convert one or more RecordBatches into a Flight stream stamped with
/// the supplied ticket on the first message's FlightDescriptor.cmd.
async fn flight_stream_with_ticket(
    batches: Vec<RecordBatch>,
    ticket: Vec<u8>,
) -> Vec<Result<FlightData, Status>> {
    let batch_stream = stream::iter(
        batches
            .into_iter()
            .map(Ok::<_, arrow_flight::error::FlightError>),
    );
    let mut flight_data: Vec<FlightData> = FlightDataEncoderBuilder::new()
        .build(batch_stream)
        .filter_map(|r| async move { r.ok() })
        .collect()
        .await;
    // Stamp the ticket onto the first message's descriptor — pyarrow's
    // client does the equivalent via FlightDescriptor.for_command.
    let mut first = flight_data.remove(0);
    first.flight_descriptor = Some(FlightDescriptor::new_cmd(ticket));
    let mut out = vec![Ok(first)];
    out.extend(flight_data.into_iter().map(Ok));
    out
}

fn make_service(staging_root: PathBuf) -> QiitaFlightService {
    // DoPut tests don't exercise export_read; any scratch root works here.
    let scratch_root = staging_root
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| staging_root.clone());
    QiitaFlightService::new(
        test_vk(),
        // catalog + data_path unused by DoPut path
        "dbname=unused host=localhost".to_string(),
        "/tmp/unused".to_string(),
        staging_root,
        scratch_root,
    )
}

// ------------------------------------------------------------------
// DoGet IPC compression — the parser being right does not prove the encoder
// honoured it, so these drive the real `do_get` end to end and assert on
// what the encoder stamped into each record-batch message.
// ------------------------------------------------------------------

/// Seed alignment rows and return a signed `alignment_visible` ticket for
/// them projecting `cols`, plus a service wired to the same catalog.
///
/// Every row carries a `cigar` — the wide column the projection exists to
/// keep off the wire — so a test can prove both that asking for it delivers
/// it and that not asking for it costs nothing.
///
/// The returned `TempDir` owns the service's staging and scratch roots and
/// must stay bound for the test's lifetime — dropping it removes the
/// directory. `do_get` reaches neither root today (staging belongs to DoPut,
/// scratch to the export DoActions), but they are real, TMPDIR-resident and
/// self-cleaning rather than `/tmp` literals, so a DoGet that later does
/// write cannot litter a shared directory.
#[cfg(feature = "integration")]
fn doget_fixture(
    align: i64,
    prep: i64,
    cols: &[&str],
) -> (QiitaFlightService, Vec<u8>, tempfile::TempDir) {
    let connstr = delete_test_catalog_connstr();
    let data_path = delete_test_data_path();
    {
        let conn = Connection::open_in_memory().unwrap();
        ducklake::connect_ducklake(&conn, &connstr, &data_path).unwrap();
        ducklake::ensure_reference_tables(&conn).unwrap();
        ducklake::ensure_alignment_tables(&conn).unwrap();
        ducklake::ensure_exclusion_tables(&conn).unwrap();
        // Enough rows that a compressed body is unambiguously smaller than a
        // raw one; a handful would be dominated by framing.
        conn.execute_batch(&format!(
            "DELETE FROM qiita_lake.alignment WHERE alignment_idx = {align};
             INSERT INTO qiita_lake.alignment \
                 (alignment_idx, prep_sample_idx, sequence_idx, feature_idx, \
                  flags, position, stop_position, cigar) \
             SELECT {align}, {prep}, i, 1, 0, 100, 200, '100M' FROM range(20000) t(i);"
        ))
        .unwrap();
    }
    let quoted = cols
        .iter()
        .map(|c| format!("\"{c}\""))
        .collect::<Vec<_>>()
        .join(",");
    let payload = format!(
        r#"{{"table":"alignment_visible","filter":{{"alignment_idx":[{align}],"prep_sample_idx":[{prep}]}},"columns":[{quoted}]}}"#
    );
    let ticket = sign_raw(payload.as_bytes(), &TEST_SEED, future_expiry_secs(300));
    let tmp = tempfile::tempdir().unwrap();
    let staging = tmp.path().join("staging");
    std::fs::create_dir_all(&staging).unwrap();
    let service = QiitaFlightService::new(
        test_vk(),
        connstr,
        data_path,
        staging,
        tmp.path().to_path_buf(),
    );
    (service, ticket, tmp)
}

/// The projection the feature-table consumer signs. Mirrors
/// `_ALIGNMENT_COLUMNS` in `estimate_feature_table.py`; used by the
/// compression tests, which care about the stream, not the column set.
#[cfg(feature = "integration")]
const FEATURE_TABLE_COLUMNS: &[&str] = &[
    "prep_sample_idx",
    "sequence_idx",
    "feature_idx",
    "flags",
    "position",
    "stop_position",
];

/// Collect a DoGet response, returning the codec stamped into each
/// record-batch message and the total payload size.
///
/// Reads the codec back out of each message's IPC header rather than
/// trusting that the request was honoured — a codec the encoder silently
/// declined to apply would otherwise pass every test below.
/// Returns the per-message codec stamps, the total body size, and every
/// message's bytes (header ‖ body, concatenated) so a caller can compare two
/// streams for real byte-identity rather than for matching stamps.
#[cfg(feature = "integration")]
async fn doget_codecs(
    service: &QiitaFlightService,
    ticket: Vec<u8>,
    header: Option<&str>,
) -> Result<(Vec<Option<CompressionType>>, usize, Vec<u8>), Status> {
    let mut request = Request::new(Ticket {
        ticket: ticket.into(),
    });
    if let Some(value) = header {
        request.metadata_mut().insert(
            IPC_COMPRESSION_HEADER,
            tonic::metadata::MetadataValue::try_from(value).unwrap(),
        );
    }
    let response = service.do_get(request).await?;
    let messages: Vec<FlightData> = response
        .into_inner()
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<_, Status>>()?;
    let bytes = messages.iter().map(|m| m.data_body.len()).sum();
    let codecs = messages
        .iter()
        .filter_map(|m| {
            Some(
                arrow_ipc::root_as_message(&m.data_header)
                    .ok()?
                    .header_as_record_batch()?
                    .compression()
                    .map(|c| c.codec()),
            )
        })
        .collect();
    let raw = messages
        .iter()
        .flat_map(|m| m.data_header.iter().chain(m.data_body.iter()).copied())
        .collect();
    Ok((codecs, bytes, raw))
}

#[tokio::test]
#[serial_test::serial]
#[cfg(feature = "integration")]
async fn doget_with_zstd_header_stamps_the_codec_into_every_batch_message() {
    let (service, ticket, _tmp) = doget_fixture(977_000, 977_010, FEATURE_TABLE_COLUMNS);
    let (codecs, compressed, _) = doget_codecs(&service, ticket.clone(), Some("zstd"))
        .await
        .expect("zstd DoGet should succeed");

    assert!(!codecs.is_empty(), "no record-batch messages in the stream");
    assert!(
        codecs.iter().all(|c| *c == Some(CompressionType::ZSTD)),
        "a batch message shipped uncompressed despite the header: {codecs:?}"
    );

    // The codec stamp alone would be satisfied by a codec that ran and
    // achieved nothing; these rows are highly repetitive, so real
    // compression must show up in the body.
    let (_, raw, _) = doget_codecs(&service, ticket, None)
        .await
        .expect("uncompressed DoGet should succeed");
    assert!(
        compressed * 2 < raw,
        "expected zstd to at least halve the body, got {compressed} vs {raw} bytes"
    );
}

/// The regression guard for every existing client: neither way of asking for
/// no compression may change the stream.
///
/// Asserts two things the codec stamp alone does not. Every message comes back
/// unstamped, AND the two streams are byte-for-byte equal — an absent header
/// and an explicit `none` are the same stream, not merely two streams that
/// both claim to be uncompressed. What neither can observe is equality with
/// the encoder that preceded this change, which is not reachable from here;
/// `no_codec_write_options_match_the_encoder_default` pins that instead.
#[tokio::test]
#[serial_test::serial]
#[cfg(feature = "integration")]
async fn doget_without_the_header_streams_exactly_what_an_explicit_none_does() {
    let (service, ticket, _tmp) = doget_fixture(977_100, 977_110, FEATURE_TABLE_COLUMNS);
    let mut streams = Vec::new();
    for header in [None, Some("none")] {
        let (codecs, _, raw) = doget_codecs(&service, ticket.clone(), header)
            .await
            .unwrap_or_else(|e| panic!("DoGet with header {header:?} failed: {e}"));
        assert!(!codecs.is_empty(), "{header:?}: no record-batch messages");
        assert!(
            codecs.iter().all(Option::is_none),
            "{header:?} must leave the body uncompressed, got {codecs:?}"
        );
        streams.push(raw);
    }
    assert_eq!(
        streams[0], streams[1],
        "an absent header and an explicit `none` produced different bytes"
    );
}

#[tokio::test]
#[serial_test::serial]
#[cfg(feature = "integration")]
async fn doget_with_an_unsupported_codec_is_rejected_before_streaming() {
    let (service, ticket, _tmp) = doget_fixture(977_200, 977_210, FEATURE_TABLE_COLUMNS);
    let err = doget_codecs(&service, ticket, Some("lz4"))
        .await
        .expect_err("lz4 must be rejected, not silently ignored");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

// ------------------------------------------------------------------
// Signed projection — end to end, over a real Arrow stream. The unit
// tests prove build_query emits the right SQL; only these prove the ticket
// the control plane signs turns into the schema the consumer receives.
// ------------------------------------------------------------------

/// Drive a real `do_get` and return the streamed schema's field names.
#[cfg(feature = "integration")]
async fn doget_schema(service: &QiitaFlightService, ticket: Vec<u8>) -> Vec<String> {
    let response = service
        .do_get(Request::new(Ticket {
            ticket: ticket.into(),
        }))
        .await
        .expect("DoGet should succeed");
    let messages: Vec<FlightData> = response
        .into_inner()
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<_, Status>>()
        .expect("stream should not error");
    messages
        .iter()
        .find_map(|m| arrow_schema::Schema::try_from(m).ok())
        .expect("no schema message in the stream")
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect()
}

#[tokio::test]
#[serial_test::serial]
#[cfg(feature = "integration")]
async fn doget_streams_cigar_only_when_the_ticket_signed_it() {
    // The whole point of the signed projection, proven end to end. Two
    // tickets over identical rows; the only difference is what was signed.
    let (service, with, _tmp) = doget_fixture(
        977_300,
        977_310,
        &["prep_sample_idx", "feature_idx", "cigar"],
    );
    assert_eq!(
        doget_schema(&service, with).await,
        vec!["prep_sample_idx", "feature_idx", "cigar"],
        "the stream must carry exactly the signed columns, in the signed order"
    );

    let (service, without, _tmp) = doget_fixture(977_400, 977_410, FEATURE_TABLE_COLUMNS);
    let fields = doget_schema(&service, without).await;
    assert_eq!(fields, FEATURE_TABLE_COLUMNS);
    assert!(
        !fields.iter().any(|f| f == "cigar"),
        "cigar reached a consumer that never asked for it: {fields:?}"
    );
}

#[tokio::test]
#[serial_test::serial]
#[cfg(feature = "integration")]
async fn alignment_doget_without_a_signed_projection_is_rejected() {
    // The retired fallback, pinned end to end. A ticket minted before this
    // shipped and redeemed inside its 300 s TTL after it lands here.
    let (service, _, _tmp) = doget_fixture(977_500, 977_510, FEATURE_TABLE_COLUMNS);
    let payload = r#"{"table":"alignment_visible","filter":{"alignment_idx":[977500],"prep_sample_idx":[977510]}}"#;
    let ticket = sign_raw(payload.as_bytes(), &TEST_SEED, future_expiry_secs(300));
    let err = service
        .do_get(Request::new(Ticket {
            ticket: ticket.into(),
        }))
        .await
        .err()
        .expect("a columnless alignment ticket must be refused");
    assert_eq!(err.code(), tonic::Code::InvalidArgument, "got: {err}");
}

#[test]
#[serial_test::serial]
#[cfg(feature = "integration")]
fn projection_allowlist_matches_the_alignment_schema_exactly() {
    // ALIGNMENT_PROJECTION_COLUMNS is hand-copied from the DDL two files
    // over, and nothing else checks it. Drift is quiet in both directions:
    // a column added to the table but not the allowlist simply cannot be
    // requested (the feature silently does not exist), and one removed from
    // the table but left in the allowlist mints tickets that fail at bind
    // time, on the cluster, rather than here.
    let conn = Connection::open_in_memory().unwrap();
    ducklake::connect_ducklake(
        &conn,
        &delete_test_catalog_connstr(),
        &delete_test_data_path(),
    )
    .unwrap();
    ducklake::ensure_reference_tables(&conn).unwrap();
    ducklake::ensure_alignment_tables(&conn).unwrap();
    ducklake::ensure_exclusion_tables(&conn).unwrap();

    // The VIEW, not the base table: `alignment_visible` is what a ticket can
    // name, and its `SELECT a.*` is what makes the two column sets equal.
    let mut stmt = conn
        .prepare("SELECT column_name FROM duckdb_columns() WHERE table_name = 'alignment_visible'")
        .unwrap();
    let actual: std::collections::BTreeSet<String> = stmt
        .query_map([], |row| row.get::<_, String>(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    let allowed: std::collections::BTreeSet<String> = ALIGNMENT_PROJECTION_COLUMNS
        .iter()
        .map(|c| c.to_string())
        .collect();

    assert_eq!(
        actual,
        allowed,
        "the projection allowlist and alignment_visible's columns have drifted; \
         only in the view: {:?}; only in the allowlist: {:?}",
        actual.difference(&allowed).collect::<Vec<_>>(),
        allowed.difference(&actual).collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn do_put_writes_arrow_stream_to_parquet() {
    let tmp = tempfile::tempdir().unwrap();
    let service = make_service(tmp.path().to_path_buf());

    let ticket = sign_doput_for_test(42, &TEST_SEED, future_expiry_secs(300));
    let messages = flight_stream_with_ticket(vec![sample_batch()], ticket).await;

    let result = service
        .do_put_inner(stream::iter(messages))
        .await
        .expect("do_put should succeed on a well-formed stream");

    let staged = tmp.path().join("uploads/42/upload.parquet");
    assert!(staged.exists(), "staging file not written");

    // File mode is 440 (owner+group read, no write, no world)
    let perms = std::fs::metadata(&staged).unwrap().permissions();
    assert_eq!(perms.mode() & 0o777, 0o440);

    // PutResult body carries sha256/row_count/bytes/upload_idx — and
    // deliberately NOT staging_path. Clients are not allowed to learn
    // server-side paths (the architecture commitment); the layout is
    // derivable from root + upload_idx by parties that legitimately
    // need it (CP, DP), but the client is not one of those.
    let body: serde_json::Value = serde_json::from_slice(&result.app_metadata).unwrap();
    assert_eq!(body["upload_idx"], 42);
    assert_eq!(body["row_count"], 3);
    assert!(
        body.get("staging_path").is_none(),
        "staging_path must not leak to the client"
    );
    let claimed_sha = body["sha256"].as_str().unwrap();
    let claimed_bytes = body["bytes_received"].as_u64().unwrap();

    // Recompute sha256 + size of the actual file, verify the PutResult
    // claim matches byte-for-byte.
    let actual_bytes = std::fs::metadata(&staged).unwrap().len();
    assert_eq!(claimed_bytes, actual_bytes);
    let file_bytes = std::fs::read(&staged).unwrap();
    let mut hasher = Sha256::new();
    hasher.update(&file_bytes);
    let actual_sha: String = hasher
        .finalize()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    assert_eq!(claimed_sha, actual_sha);
}

#[tokio::test]
async fn do_put_rejects_expired_ticket() {
    let tmp = tempfile::tempdir().unwrap();
    let service = make_service(tmp.path().to_path_buf());
    let expired = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        - 1000;
    let ticket = sign_doput_for_test(1, &TEST_SEED, expired);
    let messages = flight_stream_with_ticket(vec![sample_batch()], ticket).await;

    let err = service
        .do_put_inner(stream::iter(messages))
        .await
        .expect_err("expired ticket must be rejected");
    assert_eq!(err.code(), tonic::Code::Unauthenticated);
}

#[tokio::test]
async fn do_put_rejects_bad_signature() {
    let tmp = tempfile::tempdir().unwrap();
    let service = make_service(tmp.path().to_path_buf());
    // Sign with a different secret than the service holds.
    let ticket = sign_doput_for_test(1, &WRONG_SEED, future_expiry_secs(300));
    let messages = flight_stream_with_ticket(vec![sample_batch()], ticket).await;

    let err = service
        .do_put_inner(stream::iter(messages))
        .await
        .expect_err("bad signature must be rejected");
    assert_eq!(err.code(), tonic::Code::Unauthenticated);
}

#[tokio::test]
async fn do_put_rejects_missing_descriptor() {
    let tmp = tempfile::tempdir().unwrap();
    let service = make_service(tmp.path().to_path_buf());
    // A stream whose first message has no descriptor at all.
    let messages: Vec<Result<FlightData, Status>> = vec![Ok(FlightData::default())];

    let err = service
        .do_put_inner(stream::iter(messages))
        .await
        .expect_err("missing descriptor must be rejected");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn do_put_rejects_empty_cmd() {
    let tmp = tempfile::tempdir().unwrap();
    let service = make_service(tmp.path().to_path_buf());
    let fd = FlightData {
        flight_descriptor: Some(FlightDescriptor::new_cmd(Vec::<u8>::new())),
        ..Default::default()
    };
    let messages: Vec<Result<FlightData, Status>> = vec![Ok(fd)];

    let err = service
        .do_put_inner(stream::iter(messages))
        .await
        .expect_err("empty cmd must be rejected");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn do_put_interrupted_stream_leaves_no_parquet() {
    let tmp = tempfile::tempdir().unwrap();
    let service = make_service(tmp.path().to_path_buf());
    let ticket = sign_doput_for_test(99, &TEST_SEED, future_expiry_secs(300));

    // Build a valid first message (descriptor + schema), then yield an
    // Err mid-stream before any batch lands. The handler should
    // surface the error AND leave nothing in the staging directory.
    let mut messages = flight_stream_with_ticket(vec![sample_batch()], ticket).await;
    // Truncate to schema only, then inject an error.
    messages.truncate(1);
    messages.push(Err(Status::internal("simulated mid-stream drop")));

    let err = service
        .do_put_inner(stream::iter(messages))
        .await
        .expect_err("interrupted stream must surface an error");
    assert_eq!(err.code(), tonic::Code::Internal);

    let staged = tmp.path().join("uploads/99/upload.parquet");
    assert!(
        !staged.exists(),
        "partial parquet must be deleted on interrupt; found {}",
        staged.display()
    );
}

#[tokio::test]
async fn do_put_same_upload_idx_second_attempt_rejected() {
    // After a successful DoPut to upload_idx=N, a second DoPut to the
    // same N must fail with AlreadyExists rather than silently
    // clobbering the staged file. The CP doesn't reissue tickets, but
    // a malicious / buggy client could replay a still-valid one.
    let tmp = tempfile::tempdir().unwrap();
    let service = make_service(tmp.path().to_path_buf());

    let ticket = sign_doput_for_test(7, &TEST_SEED, future_expiry_secs(300));
    let m1 = flight_stream_with_ticket(vec![sample_batch()], ticket.clone()).await;
    service
        .do_put_inner(stream::iter(m1))
        .await
        .expect("first DoPut should succeed");

    let m2 = flight_stream_with_ticket(vec![sample_batch()], ticket).await;
    let err = service
        .do_put_inner(stream::iter(m2))
        .await
        .expect_err("second DoPut to the same upload_idx must be rejected");
    assert_eq!(err.code(), tonic::Code::AlreadyExists);

    // The first DoPut's file survives (still mode 440); it was not
    // clobbered by the failed second attempt.
    let staged = tmp.path().join("uploads/7/upload.parquet");
    let perms = std::fs::metadata(&staged).unwrap().permissions();
    assert_eq!(perms.mode() & 0o777, 0o440);
}

#[tokio::test]
async fn do_put_alreadyexists_wins_over_mid_stream_decode_error() {
    // Regression: with the async-decoder/blocking-writer bridge, a second
    // DoPut to an occupied upload_idx whose stream ALSO errors mid-flight
    // must still surface AlreadyExists — not the decode error. do_put_inner
    // skips its partial-file cleanup only for AlreadyExists, and that staged
    // file belongs to the first, legitimate upload; masking it as the decode
    // error would unlink their file. Reproduces the writer-task error vs
    // decode-error precedence.
    let tmp = tempfile::tempdir().unwrap();
    let service = make_service(tmp.path().to_path_buf());

    // First upload occupies upload_idx=88.
    let t1 = sign_doput_for_test(88, &TEST_SEED, future_expiry_secs(300));
    let m1 = flight_stream_with_ticket(vec![sample_batch()], t1).await;
    service
        .do_put_inner(stream::iter(m1))
        .await
        .expect("first DoPut should succeed");
    let staged = tmp.path().join("uploads/88/upload.parquet");
    let bytes_before = std::fs::read(&staged).unwrap();

    // Second DoPut to the same idx: keep only the schema frame, then inject a
    // mid-stream error. The writer hits AlreadyExists on create_new while the
    // decoder surfaces the error, exercising the precedence.
    let t2 = sign_doput_for_test(88, &TEST_SEED, future_expiry_secs(300));
    let mut m2 = flight_stream_with_ticket(vec![sample_batch()], t2).await;
    m2.truncate(1);
    m2.push(Err(Status::internal("simulated mid-stream drop")));

    let err = service
        .do_put_inner(stream::iter(m2))
        .await
        .expect_err("second DoPut must fail");
    assert_eq!(
        err.code(),
        tonic::Code::AlreadyExists,
        "AlreadyExists must win over the decode error so the first file is preserved"
    );

    // The first upload's file is untouched — not unlinked by cleanup.
    assert!(staged.exists(), "the first upload's file must survive");
    assert_eq!(
        std::fs::read(&staged).unwrap(),
        bytes_before,
        "first upload bytes unchanged"
    );
    assert_eq!(
        std::fs::metadata(&staged).unwrap().permissions().mode() & 0o777,
        0o440
    );
}

#[tokio::test]
async fn do_put_concurrent_uploads_are_isolated() {
    // Two uploads to different upload_idx values land at different
    // staging paths and don't trample each other. Smoke test that the
    // QiitaFlightService has no shared mutable state.
    let tmp = tempfile::tempdir().unwrap();
    let service = make_service(tmp.path().to_path_buf());

    let t1 = sign_doput_for_test(1, &TEST_SEED, future_expiry_secs(300));
    let t2 = sign_doput_for_test(2, &TEST_SEED, future_expiry_secs(300));
    let m1 = flight_stream_with_ticket(vec![sample_batch()], t1).await;
    let m2 = flight_stream_with_ticket(vec![sample_batch()], t2).await;

    let (r1, r2) = futures::join!(
        service.do_put_inner(stream::iter(m1)),
        service.do_put_inner(stream::iter(m2)),
    );
    r1.unwrap();
    r2.unwrap();

    assert!(tmp.path().join("uploads/1/upload.parquet").exists());
    assert!(tmp.path().join("uploads/2/upload.parquet").exists());
}

#[tokio::test]
async fn do_put_writes_multi_batch_stream() {
    // Exercise the async-decoder → blocking-writer channel bridge with more
    // than one RecordBatch: every batch must flow through the mpsc channel,
    // be written, and be counted. Three 3-row batches => 9 rows, and the
    // PutResult sha256/bytes must match the file on disk byte-for-byte.
    let tmp = tempfile::tempdir().unwrap();
    let service = make_service(tmp.path().to_path_buf());

    let ticket = sign_doput_for_test(55, &TEST_SEED, future_expiry_secs(300));
    let batches = vec![sample_batch(), sample_batch(), sample_batch()];
    let messages = flight_stream_with_ticket(batches, ticket).await;

    let result = service
        .do_put_inner(stream::iter(messages))
        .await
        .expect("multi-batch do_put should succeed");

    let body: serde_json::Value = serde_json::from_slice(&result.app_metadata).unwrap();
    assert_eq!(body["upload_idx"], 55);
    assert_eq!(body["row_count"], 9, "three 3-row batches stream through");

    let staged = tmp.path().join("uploads/55/upload.parquet");
    let actual_bytes = std::fs::metadata(&staged).unwrap().len();
    assert_eq!(body["bytes_received"].as_u64().unwrap(), actual_bytes);
    let mut hasher = Sha256::new();
    hasher.update(std::fs::read(&staged).unwrap());
    let actual_sha: String = hasher
        .finalize()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    assert_eq!(body["sha256"].as_str().unwrap(), actual_sha);

    // Round-trips as a 9-row Parquet.
    let reader = Connection::open_in_memory().unwrap();
    let n: i64 = reader
        .query_row(
            &format!(
                "SELECT count(*) FROM read_parquet('{}')",
                staged.to_str().unwrap()
            ),
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(n, 9);
}

#[test]
fn staging_path_for_layout() {
    let root = Path::new("/scratch/ephemeral/staging");
    assert_eq!(
        staging_path_for(root, 42),
        Path::new("/scratch/ephemeral/staging/uploads/42/upload.parquet")
    );
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
    let connstr = delete_test_catalog_connstr();
    let data_path = delete_test_data_path();
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
    let connstr = delete_test_catalog_connstr();
    let data_path = delete_test_data_path();
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
