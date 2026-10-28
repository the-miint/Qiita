//! Unit tests for [`super`].

use super::*;
use crate::flight_service::test_support::{columns, filter_of};

#[cfg(feature = "integration")]
use arrow_flight::flight_service_server::FlightService;
#[cfg(feature = "integration")]
use arrow_flight::{FlightData, Ticket};
#[cfg(feature = "integration")]
use duckdb::Connection;
#[cfg(feature = "integration")]
use futures::stream::StreamExt;
#[cfg(feature = "integration")]
use tonic::Request;

#[cfg(feature = "integration")]
use crate::ducklake;
#[cfg(feature = "integration")]
use crate::flight_service::doget::stream_ducklake_batches;
#[cfg(feature = "integration")]
use crate::flight_service::test_support::{
    doget_fixture, future_expiry_secs, pooled_conn, sign_raw, FEATURE_TABLE_COLUMNS, TEST_SEED,
};
#[cfg(feature = "integration")]
use crate::flight_service::QiitaFlightService;

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
    let connstr = crate::test_catalog::connstr();
    let data_path = crate::test_catalog::data_path();
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
    let connstr = crate::test_catalog::connstr();
    let data_path = crate::test_catalog::data_path();
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
    let connstr = crate::test_catalog::connstr();
    let data_path = crate::test_catalog::data_path();
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
    const DUCKLAKE_SRC: &str = include_str!("../ducklake.rs");

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
    let connstr = crate::test_catalog::connstr();
    let data_path = crate::test_catalog::data_path();

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
        stream_ducklake_batches(pooled_conn(&connstr, &data_path), sql, table)
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
    let connstr = crate::test_catalog::connstr();
    let data_path = crate::test_catalog::data_path();

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
        stream_ducklake_batches(pooled_conn(&connstr, &data_path), sql, table)
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
        &crate::test_catalog::connstr(),
        &crate::test_catalog::data_path(),
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
