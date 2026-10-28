//! Unit tests for [`super`].

use super::*;
use crate::flight_service::test_support::unattached_writer;

#[cfg(feature = "integration")]
use duckdb::Connection;

#[cfg(feature = "integration")]
use crate::ducklake;
#[cfg(feature = "integration")]
use crate::flight_service::test_support::{
    attached_writer, catalog_that_loses_a_commit_answer, setup_full_lake,
};

/// Orphan-only sequence deletion: a feature owned by another reference
/// keeps its sequence; a feature owned only by the deleted reference loses
/// it. Reference-scoped tables (membership, taxonomy) drop fully.
#[test]
#[serial_test::serial]
#[cfg(feature = "integration")]
fn delete_reference_drops_orphans_keeps_shared() {
    let connstr = crate::test_catalog::connstr();
    let data_path = crate::test_catalog::data_path();
    let conn = Connection::open_in_memory().unwrap();
    ducklake::connect_ducklake(&conn, &connstr, &data_path).unwrap();
    // delete_reference takes the registration lock, so the lake needs more
    // than the reference tables.
    setup_full_lake(&conn);

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

    let counts = delete_reference(&attached_writer(&connstr, &data_path), ref_a)
        .expect("delete_reference failed");
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

/// A reference delete whose COMMIT answer is lost succeeds and reports the rows
/// it deleted, rather than the zeros a re-run of the deletes would count.
#[test]
#[serial_test::serial]
#[cfg(feature = "integration")]
fn delete_reference_whose_commit_answer_is_lost_reports_what_it_deleted() {
    let connstr = crate::test_catalog::connstr();
    let data_path = crate::test_catalog::data_path();
    let conn = Connection::open_in_memory().unwrap();
    ducklake::connect_ducklake(&conn, &connstr, &data_path).unwrap();
    setup_full_lake(&conn);
    let ref_idx: i64 = 911_000;
    conn.execute_batch(&format!(
        "INSERT INTO qiita_lake.reference_membership VALUES ({ref_idx}, 911010), ({ref_idx}, 911011);"
    ))
    .unwrap();
    let (proxied, cut) = catalog_that_loses_a_commit_answer(&connstr);

    let counts = delete_reference(&attached_writer(&proxied, &data_path), ref_idx)
        .expect("a reference delete whose COMMIT answer was lost failed");

    assert!(cut.load(std::sync::atomic::Ordering::SeqCst));
    assert_eq!(counts["membership_deleted"], 2);
    let left: i64 = conn
        .query_row(
            &format!(
                "SELECT count(*) FROM qiita_lake.reference_membership WHERE reference_idx = {ref_idx}"
            ),
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(left, 0);
}

/// `delete_mask` drops exactly the target mask's `read_mask` rows, leaves a
/// different mask untouched, and is idempotent: a second delete of the same
/// mask_idx succeeds and reports `rows_deleted: 0`.
#[test]
#[serial_test::serial]
#[cfg(feature = "integration")]
fn delete_mask_drops_target_idempotently() {
    let connstr = crate::test_catalog::connstr();
    let data_path = crate::test_catalog::data_path();
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

    let first =
        delete_mask(&attached_writer(&connstr, &data_path), mask_a).expect("delete_mask failed");
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
    let second = delete_mask(&attached_writer(&connstr, &data_path), mask_a)
        .expect("idempotent re-delete failed");
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
    let connstr = crate::test_catalog::connstr();
    let data_path = crate::test_catalog::data_path();
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

    let first = delete_read_mask_block(&attached_writer(&connstr, &data_path), mask_a, &members)
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
    let second = delete_read_mask_block(&attached_writer(&connstr, &data_path), mask_a, &members)
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
    let connstr = crate::test_catalog::connstr();
    let data_path = crate::test_catalog::data_path();
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

    let first = delete_alignment(&attached_writer(&connstr, &data_path), align_a)
        .expect("delete_alignment failed");
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
    let second = delete_alignment(&attached_writer(&connstr, &data_path), align_a)
        .expect("idempotent re-delete failed");
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
    let connstr = crate::test_catalog::connstr();
    let data_path = crate::test_catalog::data_path();
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

    let first = delete_alignment_block(&attached_writer(&connstr, &data_path), align_a, &members)
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
    let second = delete_alignment_block(&attached_writer(&connstr, &data_path), align_a, &members)
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
    let connstr = crate::test_catalog::connstr();
    let data_path = crate::test_catalog::data_path();
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

    let first = delete_alignment_sample(&attached_writer(&connstr, &data_path), align_a, prep_a)
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
    let second = delete_alignment_sample(&attached_writer(&connstr, &data_path), align_a, prep_a)
        .expect("idempotent re-delete failed");
    assert_eq!(second["rows_deleted"], 0, "second delete removes nothing");

    // A prep_sample that never registered rows is the same 0-row success.
    let never =
        delete_alignment_sample(&attached_writer(&connstr, &data_path), align_a, prep_empty)
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

/// An empty prep_sample set short-circuits: zero counts, no catalog touched
/// (so this needs no DuckLake — runs in the pure-unit tier). Guards against
/// emitting an invalid `IN ()` clause.
#[test]
fn delete_pool_reads_empty_set_is_zero_count_noop() {
    let counts = delete_pool_reads(&unattached_writer(), &[])
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
    let connstr = crate::test_catalog::connstr();
    let data_path = crate::test_catalog::data_path();
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

    let first = delete_pool_reads(&attached_writer(&connstr, &data_path), &[prep_a, prep_b])
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
    let second = delete_pool_reads(&attached_writer(&connstr, &data_path), &[prep_a, prep_b])
        .expect("idempotent re-delete failed");
    assert_eq!(second["read_rows_deleted"], 0);
    assert_eq!(second["read_mask_rows_deleted"], 0);

    // Best-effort cleanup of the surviving other-pool rows.
    let _ = conn.execute_batch(&format!(
        "DELETE FROM qiita_lake.read WHERE prep_sample_idx = {prep_other};
         DELETE FROM qiita_lake.read_mask WHERE prep_sample_idx = {prep_other};"
    ));
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
    let connstr = crate::test_catalog::connstr();
    let data_path = crate::test_catalog::data_path();
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
    let connstr = crate::test_catalog::connstr();
    let data_path = crate::test_catalog::data_path();
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

    let first = sync_reference_exclusion(&attached_writer(&connstr, &data_path), &src, root)
        .expect("sync failed");
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
    let second = sync_reference_exclusion(&attached_writer(&connstr, &data_path), &src, root)
        .expect("re-sync failed");
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
    let cleared = sync_reference_exclusion(&attached_writer(&connstr, &data_path), &empty, root)
        .expect("clear sync failed");
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
    let escaped = sync_reference_exclusion(&attached_writer(&connstr, &data_path), &planted, root);
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

/// The mint against a real DuckLake catalog: what `execute` reports as
/// `minted_rows`, the all-or-nothing gate, replay, and isolation between
/// references. Pinned here so a DuckDB or ducklake bump re-runs them.
#[test]
#[serial_test::serial]
#[cfg(feature = "integration")]
fn mint_phylogeny_edge_id_is_all_or_nothing_and_replay_safe() {
    let connstr = crate::test_catalog::connstr();
    let data_path = crate::test_catalog::data_path();
    let conn = Connection::open_in_memory().unwrap();
    ducklake::connect_ducklake(&conn, &connstr, &data_path).unwrap();
    ducklake::ensure_reference_tables(&conn).unwrap();

    // Unique ids so leftover rows never collide with other serial tests.
    let all_null: i64 = 974_210;
    let partial: i64 = 974_211;
    let neighbour: i64 = 974_212;
    // Never inserted under: the "reference exists but has no tree" case.
    let no_rows: i64 = 974_213;

    for id in [all_null, partial, neighbour] {
        conn.execute_batch(&format!(
            "DELETE FROM qiita_lake.reference_phylogeny WHERE reference_idx = {id};"
        ))
        .unwrap();
    }

    // Three nodes, no numbering at all — the shape the mint exists for.
    // Columns named rather than positional: `edge_id` precedes `parent_index` in
    // the table, and a positional VALUES list silently swaps them.
    conn.execute_batch(&format!(
        "INSERT INTO qiita_lake.reference_phylogeny
           (reference_idx, node_index, name, branch_length, edge_id, parent_index, is_tip)
         VALUES
           ({all_null}, 11, 'root', 0.0, NULL, NULL, false),
           ({all_null}, 4, 'tipA', 0.5, NULL, 11, true),
           ({all_null}, 7, 'tipB', 0.5, NULL, 11, true);"
    ))
    .unwrap();

    // Three nodes where one already carries a number the mint must not join.
    conn.execute_batch(&format!(
        "INSERT INTO qiita_lake.reference_phylogeny
           (reference_idx, node_index, name, branch_length, edge_id, parent_index, is_tip)
         VALUES
           ({partial}, 0, 'root', 0.0, 77, NULL, false),
           ({partial}, 1, 'tipA', 0.5, NULL, 0, true),
           ({partial}, 2, 'tipB', 0.5, NULL, 0, true);"
    ))
    .unwrap();

    // A second all-NULL reference that must be left alone throughout.
    conn.execute_batch(&format!(
        "INSERT INTO qiita_lake.reference_phylogeny
           (reference_idx, node_index, name, branch_length, edge_id, parent_index, is_tip)
         VALUES ({neighbour}, 0, 'root', 0.0, NULL, NULL, false);"
    ))
    .unwrap();

    let edge_ids = |id: i64| -> Vec<Option<i64>> {
        let mut stmt = conn
            .prepare(&format!(
                "SELECT edge_id FROM qiita_lake.reference_phylogeny \
                 WHERE reference_idx = {id} ORDER BY node_index"
            ))
            .unwrap();
        let rows = stmt
            .query_map([], |r| r.get::<_, Option<i64>>(0))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        rows
    };

    // An entirely NULL tree is numbered, and `execute` reports the true count —
    // which is what the operator reads back as `minted_rows`.
    let first = mint_phylogeny_edge_id(&attached_writer(&connstr, &data_path), all_null)
        .expect("mint failed");
    assert_eq!(first["phylogeny_rows"], 3);
    assert_eq!(first["already_numbered_rows"], 0);
    assert_eq!(
        first["minted_rows"], 3,
        "execute must report rows changed, not 0 or a file count"
    );
    assert_eq!(
        first["reference_idx"], all_null,
        "the body echoes the reference asked for"
    );
    // node_index is deliberately non-contiguous and out of insertion order, so this
    // fails any dense renumbering (a row_number()) that a contiguous 0,1,2 fixture
    // would accept as indistinguishable from node_index.
    assert_eq!(edge_ids(all_null), vec![Some(4), Some(7), Some(11)]);

    // Replay: the counts are re-read, the tree is already numbered, nothing moves.
    let replay = mint_phylogeny_edge_id(&attached_writer(&connstr, &data_path), all_null)
        .expect("replay failed");
    assert_eq!(replay["already_numbered_rows"], 3);
    assert_eq!(replay["minted_rows"], 0, "a replay writes nothing");
    assert_eq!(edge_ids(all_null), vec![Some(4), Some(7), Some(11)]);

    // The all-or-nothing gate: one numbered node is enough to stop the whole tree,
    // so the two NULLs stay NULL rather than being filled with a second numbering.
    let mixed = mint_phylogeny_edge_id(&attached_writer(&connstr, &data_path), partial)
        .expect("gate call failed");
    assert_eq!(mixed["phylogeny_rows"], 3);
    assert_eq!(mixed["already_numbered_rows"], 1);
    assert_eq!(
        mixed["minted_rows"], 0,
        "a partly numbered tree is not completed"
    );
    assert_eq!(
        edge_ids(partial),
        vec![Some(77), None, None],
        "the carried number survives and no node beside it was minted"
    );

    // Scoping: none of the above touched another reference.
    assert_eq!(edge_ids(neighbour), vec![None]);

    // A reference with no rows at all is reported, not mistaken for a done tree.
    let empty = mint_phylogeny_edge_id(&attached_writer(&connstr, &data_path), no_rows)
        .expect("empty call failed");
    assert_eq!(empty["phylogeny_rows"], 0);
    assert_eq!(empty["minted_rows"], 0);

    for id in [all_null, partial, neighbour] {
        conn.execute_batch(&format!(
            "DELETE FROM qiita_lake.reference_phylogeny WHERE reference_idx = {id};"
        ))
        .unwrap();
    }
}
