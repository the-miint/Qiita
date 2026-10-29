//! Unit tests for [`super`].

use duckdb::Connection;

use super::*;
use crate::flight_service::test_support::unattached_writer;

#[cfg(feature = "integration")]
use crate::ducklake;
#[cfg(feature = "integration")]
use crate::flight_service::test_support::{
    attached_conn, attached_writer, catalog_that_loses_a_commit_answer, setup_full_lake,
};

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

/// Why `move_file` cannot refuse by checking first: a plain rename replaces an
/// existing destination.
#[test]
fn rename_replaces_an_existing_destination() {
    let tmp = tempfile::tempdir().unwrap();
    let (src, dest) = (tmp.path().join("src"), tmp.path().join("dest"));
    std::fs::write(&src, b"new").unwrap();
    std::fs::write(&dest, b"old").unwrap();
    std::fs::rename(&src, &dest).unwrap();
    assert_eq!(std::fs::read(&dest).unwrap(), b"new");
}

/// Each way `move_file` places a file is refused by the call itself when the
/// destination exists, on the filesystem the tests run on.
#[test]
fn every_placement_refuses_an_existing_destination() {
    let tmp = tempfile::tempdir().unwrap();
    let (src, dest) = (tmp.path().join("src"), tmp.path().join("dest"));
    std::fs::write(&src, b"new").unwrap();
    std::fs::write(&dest, b"old").unwrap();

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    assert_eq!(
        rename_no_replace(&src, &dest).unwrap_err().kind(),
        std::io::ErrorKind::AlreadyExists
    );
    assert_eq!(
        std::fs::hard_link(&src, &dest).unwrap_err().kind(),
        std::io::ErrorKind::AlreadyExists
    );
    assert_eq!(
        copy_no_replace(&src, &dest).unwrap_err().kind(),
        std::io::ErrorKind::AlreadyExists
    );
    assert_eq!(std::fs::read(&dest).unwrap(), b"old");
    assert_eq!(std::fs::read(&src).unwrap(), b"new");
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn rename_no_replace_moves_to_a_free_destination() {
    let tmp = tempfile::tempdir().unwrap();
    let (src, dest) = (tmp.path().join("src"), tmp.path().join("dest"));
    std::fs::write(&src, b"new").unwrap();
    assert!(rename_no_replace(&src, &dest).unwrap());
    assert_eq!(std::fs::read(&dest).unwrap(), b"new");
    assert!(!src.exists());
}

/// A copy that fails partway leaves no destination: here the source is a
/// directory, which opens but cannot be read.
#[test]
fn copy_no_replace_removes_the_destination_of_a_failed_copy() {
    let tmp = tempfile::tempdir().unwrap();
    let (src, dest) = (tmp.path().join("src"), tmp.path().join("dest"));
    std::fs::create_dir(&src).unwrap();
    copy_no_replace(&src, &dest).expect_err("copying a directory succeeded");
    assert!(!dest.exists());
}

#[test]
fn copy_no_replace_keeps_the_source_mode() {
    use std::os::unix::fs::PermissionsExt;
    let tmp = tempfile::tempdir().unwrap();
    let (src, dest) = (tmp.path().join("src"), tmp.path().join("dest"));
    std::fs::write(&src, b"new").unwrap();
    std::fs::set_permissions(&src, std::fs::Permissions::from_mode(0o440)).unwrap();
    copy_no_replace(&src, &dest).unwrap();
    assert_eq!(std::fs::read(&dest).unwrap(), b"new");
    assert_eq!(
        std::fs::metadata(&dest).unwrap().permissions().mode() & 0o777,
        0o440
    );
}

/// Calls racing to place one destination: exactly one succeeds, every other is
/// refused with AlreadyExists, and the destination holds the winner's bytes.
#[test]
fn move_file_lets_one_of_racing_placements_through() {
    const RACERS: usize = 16;
    for _ in 0..20 {
        let tmp = tempfile::tempdir().unwrap();
        let dest = tmp.path().join("dest");
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(RACERS));
        let handles: Vec<_> = (0..RACERS)
            .map(|n| {
                let src = tmp.path().join(format!("src-{n}"));
                std::fs::write(&src, format!("racer {n}")).unwrap();
                let (dest, barrier) = (dest.clone(), barrier.clone());
                std::thread::spawn(move || {
                    barrier.wait();
                    (n, src.clone(), move_file(&src, &dest))
                })
            })
            .collect();
        let outcomes: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        let winners: Vec<_> = outcomes.iter().filter(|(_, _, r)| r.is_ok()).collect();
        assert_eq!(winners.len(), 1, "{} placements succeeded", winners.len());
        for (n, src, result) in &outcomes {
            match result {
                Ok(()) => assert_eq!(
                    std::fs::read(&dest).unwrap(),
                    format!("racer {n}").as_bytes()
                ),
                Err(e) => {
                    assert_eq!(e.code(), tonic::Code::AlreadyExists, "{}", e.message());
                    assert!(src.exists(), "a refused move removed its source");
                }
            }
        }
    }
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

// `register_files` reads a lake file's ticket back out of the name
// `lake_dest_filename` gave it, so the two have to agree on the format — for the
// current name and for the pre-digest `wt<ticket>-<basename>` that files
// registered earlier still carry.
#[test]
fn lake_file_ticket_reads_back_the_ticket_lake_dest_filename_wrote() {
    let name = lake_dest_filename(
        6939,
        "ticket/6939/ingest/attempt-1/output",
        "part_00000.parquet",
    );
    assert_eq!(lake_file_ticket(&format!("/lake/read/{name}")), Some(6939));
    assert_eq!(
        lake_file_ticket("/lake/read/wt6939-part_00000.parquet"),
        Some(6939),
        "pre-digest name"
    );
    for unnamed in [
        "/lake/read/ducklake-0193a1.parquet",
        "/lake/read/wt-part_00000.parquet",
        "/lake/read/wtx1-part_00000.parquet",
        "/lake/read/part_00000.parquet",
    ] {
        assert_eq!(lake_file_ticket(unnamed), None, "{unnamed}");
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
            &unattached_writer(),
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

/// `read` is NOT replace-keyed, so a registration of a prep_sample's reads by a
/// different ticket APPENDS rather than superseding. (A ticket's own earlier
/// `read` rows are replaced, through `ticket_read_files_in_lake`, not through
/// this table; `register_files_leaves_reads_another_ticket_registered` pins the
/// end-to-end behaviour against a real catalog.)
///
/// Pinned because `FORCE_RESUBMIT_EXPLANATION` (qiita-common
/// `work_ticket_constants.py`) tells users a forced re-run stores the pool's
/// reads a second time; adding `read` to `REPLACE_KEY_TABLES` would make that
/// text wrong.
#[test]
fn read_is_not_replace_keyed_so_a_forced_rerun_appends() {
    assert!(
        !REPLACE_KEY_TABLES
            .iter()
            .any(|entry| entry.table == "read" || entry.key_source == "read"),
        "`read` became replace-keyed — FORCE_RESUBMIT_EXPLANATION and the 409 that \
         carries it now overstate the damage; update them together"
    );
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

/// End-to-end `register_files`: seed a Parquet in a staging dir, register it
/// into DuckLake, and assert the file was moved to the lake path
/// and its rows are queryable through the catalog. Exercises the
/// move-then-register path and its wrapping transaction against a real
/// DuckLake catalog.
#[test]
#[serial_test::serial]
#[cfg(feature = "integration")]
fn register_files_moves_and_registers_end_to_end() {
    let connstr = crate::test_catalog::connstr();
    let data_path = crate::test_catalog::data_path();

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

    let outcome = register_files(
        &attached_writer(&connstr, &data_path),
        &data_path,
        std::path::Path::new("/"),
        &payload,
    )
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

/// Seed one `reference_membership` staging Parquet for `register_files` replay
/// tests, returning (staging dir guard, payload).
#[cfg(feature = "integration")]
fn seed_membership_registration(
    ref_idx: i64,
    ticket: i64,
) -> (tempfile::TempDir, auth::ActionPayload) {
    let staging = tempfile::tempdir().unwrap();
    let src = staging.path().join("reference_membership.parquet");
    let src_str = src.to_str().unwrap();
    Connection::open_in_memory()
        .unwrap()
        .execute_batch(&format!(
            "COPY (SELECT * FROM (VALUES \
                 ({ref_idx}::BIGINT, 1::BIGINT), ({ref_idx}::BIGINT, 2::BIGINT)) \
                 t(reference_idx, feature_idx)) \
             TO '{src_str}' (FORMAT PARQUET)"
        ))
        .unwrap();
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
    (staging, payload)
}

#[cfg(feature = "integration")]
fn membership_rows(connstr: &str, data_path: &str, ref_idx: i64) -> i64 {
    let reader = Connection::open_in_memory().unwrap();
    ducklake::connect_ducklake(&reader, connstr, data_path).unwrap();
    reader
        .query_row(
            &format!(
                "SELECT count(*) FROM qiita_lake.reference_membership \
                 WHERE reference_idx = {ref_idx}"
            ),
            [],
            |r| r.get(0),
        )
        .unwrap()
}

#[cfg(feature = "integration")]
fn prepare_membership_table(connstr: &str, data_path: &str, ref_idx: i64) {
    let conn = Connection::open_in_memory().unwrap();
    ducklake::connect_ducklake(&conn, connstr, data_path).unwrap();
    ducklake::ensure_reference_tables(&conn).unwrap();
    ducklake::ensure_registration_lock(&conn).unwrap();
    conn.execute_batch(&format!(
        "DELETE FROM qiita_lake.reference_membership WHERE reference_idx = {ref_idx};"
    ))
    .unwrap();
}

/// A registration replayed after it completed — the caller lost the reply, or
/// died before recording it — returns the files the first call registered and
/// registers nothing again. `reference_membership` is not replace-keyed, so a
/// second registration of its file would double its rows.
#[test]
#[serial_test::serial]
#[cfg(feature = "integration")]
fn register_files_replayed_after_it_completed_registers_nothing_again() {
    let connstr = crate::test_catalog::connstr();
    let data_path = crate::test_catalog::data_path();
    let ref_idx: i64 = 971_000;
    let ticket: i64 = 971_000_000 + std::process::id() as i64;
    prepare_membership_table(&connstr, &data_path, ref_idx);
    let (_staging, payload) = seed_membership_registration(ref_idx, ticket);

    let first = register_files(
        &attached_writer(&connstr, &data_path),
        &data_path,
        std::path::Path::new("/"),
        &payload,
    )
    .expect("first register_files failed");
    let replay = register_files(
        &attached_writer(&connstr, &data_path),
        &data_path,
        std::path::Path::new("/"),
        &payload,
    )
    .expect("replayed register_files failed");

    assert_eq!(replay.registered, first.registered);
    assert!(replay.replaced.is_empty());
    assert_eq!(membership_rows(&connstr, &data_path, ref_idx), 2);
}

/// A replay that finds the files moved but not registered fails closed: the
/// earlier call may still be registering them, and a second
/// `ducklake_add_data_files` of a path adds its rows again
/// (`ducklake_add_data_files_accepts_a_path_it_already_holds`).
#[test]
#[serial_test::serial]
#[cfg(feature = "integration")]
fn register_files_replayed_before_the_earlier_call_committed_fails_closed() {
    let connstr = crate::test_catalog::connstr();
    let data_path = crate::test_catalog::data_path();
    let ref_idx: i64 = 972_000;
    let ticket: i64 = 972_000_000 + std::process::id() as i64;
    prepare_membership_table(&connstr, &data_path, ref_idx);
    let (staging, payload) = seed_membership_registration(ref_idx, ticket);

    // The move half of a registration, as `register_files` places the file.
    let scope = staging_scope(&payload.staging_dir, std::path::Path::new("/"));
    let dest_dir = std::path::Path::new(&data_path).join("reference_membership");
    std::fs::create_dir_all(&dest_dir).unwrap();
    let dest = dest_dir.join(lake_dest_filename(
        ticket,
        &scope,
        "reference_membership.parquet",
    ));
    move_file(&staging.path().join("reference_membership.parquet"), &dest).unwrap();

    let err = register_files(
        &attached_writer(&connstr, &data_path),
        &data_path,
        std::path::Path::new("/"),
        &payload,
    )
    .expect_err("a replay of an uncommitted registration is refused");

    assert_eq!(err.code(), tonic::Code::FailedPrecondition, "{err}");
    assert_eq!(membership_rows(&connstr, &data_path, ref_idx), 0);
}

/// A replay that lands after the earlier registration's rows were deleted
/// (all of them, or some) still registers nothing: DuckLake keeps listing a
/// data file whose rows are deleted.
#[test]
#[serial_test::serial]
#[cfg(feature = "integration")]
fn register_files_replayed_after_its_rows_were_deleted_registers_nothing() {
    let connstr = crate::test_catalog::connstr();
    let data_path = crate::test_catalog::data_path();
    for (ref_idx, filter, rows_left) in [
        (973_000_i64, "", 0_i64),
        (974_000_i64, " AND feature_idx = 1", 1_i64),
    ] {
        let ticket: i64 = ref_idx * 1000 + std::process::id() as i64;
        prepare_membership_table(&connstr, &data_path, ref_idx);
        let (_staging, payload) = seed_membership_registration(ref_idx, ticket);
        let first = register_files(
            &attached_writer(&connstr, &data_path),
            &data_path,
            std::path::Path::new("/"),
            &payload,
        )
        .expect("first register_files failed");
        let conn = Connection::open_in_memory().unwrap();
        ducklake::connect_ducklake(&conn, &connstr, &data_path).unwrap();
        conn.execute_batch(&format!(
            "DELETE FROM qiita_lake.reference_membership WHERE reference_idx = {ref_idx}{filter};"
        ))
        .unwrap();

        let replay = register_files(
            &attached_writer(&connstr, &data_path),
            &data_path,
            std::path::Path::new("/"),
            &payload,
        )
        .expect("replayed register_files failed");

        assert_eq!(replay.registered, first.registered);
        assert_eq!(membership_rows(&connstr, &data_path, ref_idx), rows_left);
    }
}

/// A registration that fails after its moves — here `ducklake_add_data_files`
/// refuses a file whose columns do not match the table — returns the files to
/// staging, so a call with the same payload starts over instead of failing
/// closed on files left at their lake paths unregistered.
#[test]
#[serial_test::serial]
#[cfg(feature = "integration")]
fn register_files_that_fails_after_its_moves_returns_the_files_to_staging() {
    let connstr = crate::test_catalog::connstr();
    let data_path = crate::test_catalog::data_path();
    let ref_idx: i64 = 980_000_000 + std::process::id() as i64;
    prepare_membership_table(&connstr, &data_path, ref_idx);
    let (staging, payload) = seed_membership_registration(ref_idx, ref_idx);
    let good = staging.path().join("reference_membership.parquet");
    let good_bytes = std::fs::read(&good).unwrap();
    Connection::open_in_memory()
        .unwrap()
        .execute_batch(&format!(
            "COPY (SELECT 'not a number' AS reference_idx) TO '{}' (FORMAT PARQUET)",
            good.to_str().unwrap()
        ))
        .unwrap();

    let err = register_files(
        &attached_writer(&connstr, &data_path),
        &data_path,
        std::path::Path::new("/"),
        &payload,
    )
    .expect_err("a file that does not match its table registered");

    assert!(
        err.message().contains("moved back to staging"),
        "{}",
        err.message()
    );
    assert!(good.exists(), "the staged file was not returned");
    assert_eq!(membership_rows(&connstr, &data_path, ref_idx), 0);

    // The same payload, with the file fixed in staging, registers.
    std::fs::write(&good, good_bytes).unwrap();
    register_files(
        &attached_writer(&connstr, &data_path),
        &data_path,
        std::path::Path::new("/"),
        &payload,
    )
    .expect("the retried registration failed");
    assert_eq!(membership_rows(&connstr, &data_path, ref_idx), 2);
}

/// A destination already there when the call starts is read as an earlier call's
/// move, so the call fails closed before moving anything, and leaves that file
/// as it was. (A destination that appears between that check and the move is
/// refused by `move_file`, and `register_files` leaves it out of the move back.)
#[test]
#[serial_test::serial]
#[cfg(feature = "integration")]
fn register_files_fails_closed_on_a_destination_there_before_it_moves() {
    let connstr = crate::test_catalog::connstr();
    let data_path = crate::test_catalog::data_path();
    let ref_idx: i64 = 982_000_000 + std::process::id() as i64;
    prepare_membership_table(&connstr, &data_path, ref_idx);
    let (staging, mut payload) = seed_membership_registration(ref_idx, ref_idx);
    std::fs::copy(
        staging.path().join("reference_membership.parquet"),
        staging.path().join("second.parquet"),
    )
    .unwrap();
    payload.files.insert(
        "second.parquet".to_string(),
        "reference_membership".to_string(),
    );
    let scope = staging_scope(&payload.staging_dir, std::path::Path::new("/"));
    let taken = std::path::Path::new(&data_path)
        .join("reference_membership")
        .join(lake_dest_filename(ref_idx, &scope, "second.parquet"));
    std::fs::create_dir_all(taken.parent().unwrap()).unwrap();
    std::fs::write(&taken, b"not ours").unwrap();

    let err = register_files(
        &attached_writer(&connstr, &data_path),
        &data_path,
        std::path::Path::new("/"),
        &payload,
    )
    .expect_err("a registration whose destination was taken succeeded");

    assert_eq!(
        err.code(),
        tonic::Code::FailedPrecondition,
        "{}",
        err.message()
    );
    assert_eq!(std::fs::read(&taken).unwrap(), b"not ours");
    assert!(staging.path().join("reference_membership.parquet").exists());
    assert!(staging.path().join("second.parquet").exists());
    std::fs::remove_file(&taken).unwrap();
}

/// Why `transact_with_retry` reads the catalog before re-running: a DuckLake
/// COMMIT whose answer from the catalog is lost reports an error, and the rows
/// it added are there.
#[test]
#[serial_test::serial]
#[cfg(feature = "integration")]
fn ducklake_commit_error_after_the_catalog_committed() {
    let connstr = crate::test_catalog::connstr();
    let data_path = crate::test_catalog::data_path();
    let ref_idx: i64 = 983_000;
    prepare_membership_table(&connstr, &data_path, ref_idx);
    let (staging, _payload) = seed_membership_registration(ref_idx, ref_idx);
    // In the lake's data path, where it outlives the test as a registered file must.
    let lake_file = std::path::Path::new(&data_path)
        .join("reference_membership")
        .join(format!("commit-answer-lost-{}.parquet", std::process::id()));
    std::fs::create_dir_all(lake_file.parent().unwrap()).unwrap();
    move_file(
        &staging.path().join("reference_membership.parquet"),
        &lake_file,
    )
    .unwrap();
    let (proxied, cut) = catalog_that_loses_a_commit_answer(&connstr);
    let conn = attached_conn(&proxied, &data_path);

    conn.execute_batch("BEGIN TRANSACTION").unwrap();
    conn.execute(
        "CALL ducklake_add_data_files('qiita_lake', 'reference_membership', ?)",
        [lake_file.to_str().unwrap()],
    )
    .unwrap();
    conn.execute_batch("COMMIT")
        .expect_err("a COMMIT whose answer was lost succeeded");

    assert!(cut.load(std::sync::atomic::Ordering::SeqCst));
    assert_eq!(membership_rows(&connstr, &data_path, ref_idx), 2);
}

/// A registration whose COMMIT answer is lost succeeds and registers its file
/// once, rather than adding it again on a re-run.
#[test]
#[serial_test::serial]
#[cfg(feature = "integration")]
fn register_files_whose_commit_answer_is_lost_registers_once() {
    let connstr = crate::test_catalog::connstr();
    let data_path = crate::test_catalog::data_path();
    let ref_idx: i64 = 984_000;
    let ticket: i64 = 984_000_000 + std::process::id() as i64;
    prepare_membership_table(&connstr, &data_path, ref_idx);
    let (_staging, payload) = seed_membership_registration(ref_idx, ticket);
    let (proxied, cut) = catalog_that_loses_a_commit_answer(&connstr);

    let registration = register_files(
        &attached_writer(&proxied, &data_path),
        &data_path,
        std::path::Path::new("/"),
        &payload,
    )
    .expect("a registration whose COMMIT answer was lost failed");

    assert!(cut.load(std::sync::atomic::Ordering::SeqCst));
    assert_eq!(registration.registered.len(), 1);
    assert_eq!(membership_rows(&connstr, &data_path, ref_idx), 2);
}

/// The reason a replay checks the catalog before registering: in the DuckLake
/// we ship, adding a path the table already holds succeeds and counts its rows
/// twice.
#[test]
#[serial_test::serial]
#[cfg(feature = "integration")]
fn ducklake_add_data_files_accepts_a_path_it_already_holds() {
    let connstr = crate::test_catalog::connstr();
    let data_path = crate::test_catalog::data_path();
    // Unique per run: rows a prior local run added twice outlive the DELETE in
    // `prepare_membership_table`, so a fixed reference_idx would not start at 0.
    let ref_idx: i64 = 975_000_000 + std::process::id() as i64;
    let ticket: i64 = ref_idx;
    prepare_membership_table(&connstr, &data_path, ref_idx);
    let (_staging, payload) = seed_membership_registration(ref_idx, ticket);
    let first = register_files(
        &attached_writer(&connstr, &data_path),
        &data_path,
        std::path::Path::new("/"),
        &payload,
    )
    .expect("register_files failed");
    assert_eq!(membership_rows(&connstr, &data_path, ref_idx), 2);
    let conn = Connection::open_in_memory().unwrap();
    ducklake::connect_ducklake(&conn, &connstr, &data_path).unwrap();

    conn.execute(
        "CALL ducklake_add_data_files('qiita_lake', ?, ?)",
        duckdb::params!["reference_membership", first.registered[0]],
    )
    .expect("a second add of the same path is accepted");

    assert_eq!(membership_rows(&connstr, &data_path, ref_idx), 4);
}

/// One `reference_idx`'s rows registered once, then a second registration of
/// the same rows from a new staging dir, for the overlapping-delete tests: the
/// shape of `delete-alignment-sample` followed by `register-files`.
#[cfg(feature = "integration")]
fn delete_then_reregister(
    connstr: &str,
    data_path: &str,
    ref_idx: i64,
    between: impl FnOnce(),
) -> Vec<tempfile::TempDir> {
    prepare_membership_table(connstr, data_path, ref_idx);
    let (first_dir, first) = seed_membership_registration(ref_idx, ref_idx * 10);
    register_files(
        &attached_writer(connstr, data_path),
        data_path,
        std::path::Path::new("/"),
        &first,
    )
    .unwrap();
    between();
    let rerun = attached_conn(connstr, data_path);
    rerun
        .execute(
            "DELETE FROM qiita_lake.reference_membership WHERE reference_idx = ?",
            [ref_idx],
        )
        .unwrap();
    let (second_dir, second) = seed_membership_registration(ref_idx, ref_idx * 10 + 1);
    register_files(
        &attached_writer(connstr, data_path),
        data_path,
        std::path::Path::new("/"),
        &second,
    )
    .unwrap();
    assert_eq!(membership_rows(connstr, data_path, ref_idx), 2);
    vec![first_dir, second_dir]
}

/// A delete the caller abandoned, whose DELETE ran before the rerun's delete and
/// registration and whose COMMIT comes after them, fails to commit, and the
/// rerun's rows stay.
#[test]
#[serial_test::serial]
#[cfg(feature = "integration")]
fn ducklake_abandoned_delete_that_read_first_fails_to_commit() {
    let connstr = crate::test_catalog::connstr();
    let data_path = crate::test_catalog::data_path();
    let ref_idx: i64 = 976_000_000 + std::process::id() as i64;
    let abandoned = attached_conn(&connstr, &data_path);
    let _dirs = delete_then_reregister(&connstr, &data_path, ref_idx, || {
        abandoned.execute_batch("BEGIN TRANSACTION").unwrap();
        let n = abandoned
            .execute(
                "DELETE FROM qiita_lake.reference_membership WHERE reference_idx = ?",
                [ref_idx],
            )
            .unwrap();
        assert_eq!(n, 2);
    });

    let err = abandoned
        .execute_batch("COMMIT")
        .expect_err("the abandoned delete committed over the rerun");
    assert!(err.to_string().contains("Transaction conflict"), "{err}");
    assert_eq!(membership_rows(&connstr, &data_path, ref_idx), 2);
}

/// A DuckLake transaction reads the snapshot current at its first statement, not
/// at its BEGIN: a delete whose DELETE runs after the rerun's registration
/// commits, and deletes the rerun's rows.
#[test]
#[serial_test::serial]
#[cfg(feature = "integration")]
fn ducklake_transaction_reads_the_snapshot_of_its_first_statement() {
    let connstr = crate::test_catalog::connstr();
    let data_path = crate::test_catalog::data_path();
    let ref_idx: i64 = 977_000_000 + std::process::id() as i64;
    let late = attached_conn(&connstr, &data_path);
    let _dirs = delete_then_reregister(&connstr, &data_path, ref_idx, || {
        late.execute_batch("BEGIN TRANSACTION").unwrap();
    });

    let n = late
        .execute(
            "DELETE FROM qiita_lake.reference_membership WHERE reference_idx = ?",
            [ref_idx],
        )
        .unwrap();
    late.execute_batch("COMMIT").unwrap();

    assert_eq!(n, 2);
    assert_eq!(membership_rows(&connstr, &data_path, ref_idx), 0);
}

/// What `return_to_staging`'s catalog check rests on: after a COMMIT that fails,
/// the connection is out of its transaction, and a read on it sees the catalog as
/// it now is — here, a registration another connection committed meanwhile.
#[test]
#[serial_test::serial]
#[cfg(feature = "integration")]
fn ducklake_read_after_a_failed_commit_sees_the_current_catalog() {
    let connstr = crate::test_catalog::connstr();
    let data_path = crate::test_catalog::data_path();
    let ref_idx: i64 = 981_000_000 + std::process::id() as i64;
    let delete = "DELETE FROM qiita_lake.reference_membership WHERE reference_idx = ?";
    prepare_membership_table(&connstr, &data_path, ref_idx);
    let (_first_dir, first) = seed_membership_registration(ref_idx, ref_idx * 10);
    register_files(
        &attached_writer(&connstr, &data_path),
        &data_path,
        std::path::Path::new("/"),
        &first,
    )
    .unwrap();

    let failed = attached_conn(&connstr, &data_path);
    failed.execute_batch("BEGIN TRANSACTION").unwrap();
    failed.execute(delete, [ref_idx]).unwrap();
    attached_conn(&connstr, &data_path)
        .execute(delete, [ref_idx])
        .unwrap();
    let (_second_dir, second) = seed_membership_registration(ref_idx, ref_idx * 10 + 1);
    let committed = register_files(
        &attached_writer(&connstr, &data_path),
        &data_path,
        std::path::Path::new("/"),
        &second,
    )
    .unwrap();
    failed
        .execute_batch("COMMIT")
        .expect_err("the conflicting delete committed");

    assert!(failed.is_autocommit());
    let listed: Vec<String> = failed
        .prepare("SELECT data_file FROM ducklake_list_files('qiita_lake', 'reference_membership')")
        .unwrap()
        .query_map([], |r| r.get::<_, String>(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert!(
        listed.contains(&committed.registered[0]),
        "the read after the failed commit missed the committed registration"
    );
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
    let connstr = crate::test_catalog::connstr();
    let data_path = crate::test_catalog::data_path();

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
        &attached_writer(&connstr, &data_path),
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
        &attached_writer(&connstr, &data_path),
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
    let connstr = crate::test_catalog::connstr();
    let data_path = crate::test_catalog::data_path();

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
            &attached_writer(&connstr, &data_path),
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
    register_files(
        &attached_writer(connstr, data_path),
        data_path,
        std::path::Path::new("/"),
        &payload,
    )
    .unwrap_or_else(|e| {
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

/// One ticket registering the same reads twice, from two staging dirs, leaves
/// one copy in `read`: the second load replaces the first.
///
/// `register_one_parquet` stages into a fresh tempdir on every call, which is
/// what a ticket's re-run hands `register_files`: a different staging dir under
/// the same ticket.
#[test]
#[serial_test::serial]
#[cfg(feature = "integration")]
fn register_files_re_registering_a_tickets_reads_leaves_one_copy() {
    let connstr = crate::test_catalog::connstr();
    let data_path = crate::test_catalog::data_path();

    let prep_sample: i64 = 972_556;
    let ticket: i64 = 972_100_000 + std::process::id() as i64;
    let clear = format!("DELETE FROM qiita_lake.read WHERE prep_sample_idx = {prep_sample}");
    let count =
        format!("SELECT count(*) FROM qiita_lake.read WHERE prep_sample_idx = {prep_sample}");
    let live_files = format!(
        "SELECT count(DISTINCT filename) FROM qiita_lake.read WHERE prep_sample_idx = {prep_sample}"
    );

    {
        let conn = Connection::open_in_memory().unwrap();
        ducklake::connect_ducklake(&conn, &connstr, &data_path).unwrap();
        ducklake::ensure_read_tables(&conn).unwrap();
        conn.execute_batch(&clear).unwrap();
    }

    let reads = read_rows_sql(prep_sample, 1000, 5);
    for load in 0..2 {
        let registration = register_one_parquet(&connstr, &data_path, "read", &reads, ticket);
        let reader = Connection::open_in_memory().unwrap();
        ducklake::connect_ducklake(&reader, &connstr, &data_path).unwrap();
        assert_eq!(lake_count(&reader, &count), 5, "rows after load {load}");
        assert_eq!(
            lake_count(&reader, &live_files),
            1,
            "files holding the prep_sample after load {load}"
        );
        assert_eq!(
            registration.registered.len(),
            1,
            "load {load} registers its file"
        );
        assert_eq!(
            registration.replaced.get("read").copied(),
            if load == 0 { None } else { Some(5) },
            "rows load {load} replaced"
        );
    }

    let reader = Connection::open_in_memory().unwrap();
    ducklake::connect_ducklake(&reader, &connstr, &data_path).unwrap();
    let _ = reader.execute_batch(&clear);
}

/// `n` single-end reads for `prep_sample`, with contiguous `sequence_idx`
/// starting at `first`, in the `read` table's column order.
fn read_rows_sql(prep_sample: i64, first: i64, n: i64) -> String {
    format!(
        "SELECT {prep_sample}::BIGINT AS prep_sample_idx, i::BIGINT AS sequence_idx, \
         'read' || i AS read_id, 'ACGT' AS sequence1, [30, 30, 30, 30]::UTINYINT[] AS qual1, \
         NULL::VARCHAR AS sequence2, NULL::UTINYINT[] AS qual2 \
         FROM range({first}, {first} + {n}) t(i)"
    )
}

/// Stage each `(name, SELECT)` as `read/<name>.parquet`, the per-prep_sample layout
/// `ingest_reads` produces. Returns the staging dir and a payload's `files`.
fn stage_read_files(
    parts: &[(&str, String)],
) -> (tempfile::TempDir, std::collections::HashMap<String, String>) {
    let staging = tempfile::tempdir().unwrap();
    std::fs::create_dir(staging.path().join("read")).unwrap();
    let writer = Connection::open_in_memory().unwrap();
    let mut files = std::collections::HashMap::new();
    for (name, values_sql) in parts {
        let rel = format!("read/{name}.parquet");
        writer
            .execute_batch(&format!(
                "COPY ({values_sql}) TO '{}' (FORMAT PARQUET)",
                staging.path().join(&rel).to_str().unwrap()
            ))
            .unwrap();
        files.insert(rel, "read".to_string());
    }
    (staging, files)
}

#[test]
fn staged_read_prep_samples_collects_every_staged_read_prep_sample() {
    let spanning = format!(
        "{} UNION ALL {}",
        read_rows_sql(33, 300, 2),
        read_rows_sql(44, 400, 2)
    );
    let (staging, mut files) = stage_read_files(&[
        ("a", read_rows_sql(11, 100, 3)),
        ("b", read_rows_sql(22, 200, 2)),
        ("spanning", spanning),
        ("empty", read_rows_sql(55, 500, 0)),
    ]);
    // Other tables in the same registration are not read.
    files.insert("read_mask.parquet".to_string(), "read_mask".to_string());
    let conn = Connection::open_in_memory().unwrap();
    let prep_samples = staged_read_prep_samples(&conn, staging.path(), &files).unwrap();
    assert_eq!(
        prep_samples,
        std::collections::BTreeSet::from([11, 22, 33, 44])
    );
}

#[test]
fn staged_read_prep_samples_refuses_a_null_prep_sample_idx() {
    let null_row = read_rows_sql(0, 1, 1).replacen(
        "0::BIGINT AS prep_sample_idx",
        "NULL::BIGINT AS prep_sample_idx",
        1,
    );
    assert!(
        null_row.contains("NULL::BIGINT AS prep_sample_idx"),
        "{null_row}"
    );
    let (staging, files) = stage_read_files(&[("null", null_row)]);
    let conn = Connection::open_in_memory().unwrap();
    let err = staged_read_prep_samples(&conn, staging.path(), &files)
        .expect_err("a NULL prep_sample_idx is refused");
    assert_eq!(err.code(), tonic::Code::InvalidArgument, "{err}");
    assert!(err.message().contains("read/null.parquet"), "{err}");
}

/// Register `stage_read_files`' output as ONE call.
#[cfg(feature = "integration")]
fn register_read_files(
    connstr: &str,
    data_path: &str,
    parts: &[(&str, String)],
    ticket: i64,
) -> Result<Registration, Status> {
    let (staging, files) = stage_read_files(parts);
    let payload = auth::ActionPayload {
        action: "register_files".to_string(),
        staging_dir: staging.path().to_str().unwrap().to_string(),
        files,
        work_ticket_idx: ticket,
    };
    register_files(
        &attached_writer(connstr, data_path),
        data_path,
        std::path::Path::new("/"),
        &payload,
    )
}

/// A registration carrying one prep_sample this ticket registered before and one
/// new prep_sample replaces the first and adds the second.
#[test]
#[serial_test::serial]
#[cfg(feature = "integration")]
fn register_files_decides_per_read_prep_sample() {
    let connstr = crate::test_catalog::connstr();
    let data_path = crate::test_catalog::data_path();

    let held: i64 = 972_557;
    let new: i64 = 972_558;
    let ticket: i64 = 972_200_000 + std::process::id() as i64;
    let clear = format!("DELETE FROM qiita_lake.read WHERE prep_sample_idx IN ({held}, {new})");
    let count = |prep_sample: i64| {
        format!("SELECT count(*) FROM qiita_lake.read WHERE prep_sample_idx = {prep_sample}")
    };

    {
        let conn = Connection::open_in_memory().unwrap();
        ducklake::connect_ducklake(&conn, &connstr, &data_path).unwrap();
        ducklake::ensure_read_tables(&conn).unwrap();
        conn.execute_batch(&clear).unwrap();
    }

    register_read_files(
        &connstr,
        &data_path,
        &[("held", read_rows_sql(held, 2000, 3))],
        ticket,
    )
    .unwrap();
    let registration = register_read_files(
        &connstr,
        &data_path,
        &[
            ("held", read_rows_sql(held, 2000, 3)),
            ("new", read_rows_sql(new, 3000, 4)),
        ],
        ticket,
    )
    .unwrap();

    assert_eq!(
        registration.registered.len(),
        2,
        "both files are registered"
    );
    assert_eq!(
        registration.replaced.get("read"),
        Some(&3),
        "the held prep_sample's rows are replaced"
    );
    let reader = Connection::open_in_memory().unwrap();
    ducklake::connect_ducklake(&reader, &connstr, &data_path).unwrap();
    assert_eq!(
        lake_count(&reader, &count(held)),
        3,
        "the held prep_sample keeps one copy"
    );
    assert_eq!(
        lake_count(&reader, &count(new)),
        4,
        "the new prep_sample is registered"
    );

    let _ = reader.execute_batch(&clear);
}

/// Reads another ticket registered are left as they are: a registration of the
/// same prep_sample by a different ticket adds its rows and replaces none.
#[test]
#[serial_test::serial]
#[cfg(feature = "integration")]
fn register_files_leaves_reads_another_ticket_registered() {
    let connstr = crate::test_catalog::connstr();
    let data_path = crate::test_catalog::data_path();

    let held: i64 = 972_561;
    let owner: i64 = 972_400_000 + std::process::id() as i64;
    let other = owner + 1;
    let clear = format!("DELETE FROM qiita_lake.read WHERE prep_sample_idx = {held}");
    let rows_of = |ticket: i64| {
        format!(
            "SELECT count(*) FROM qiita_lake.read WHERE prep_sample_idx = {held} \
             AND filename LIKE '%{LAKE_FILE_TICKET_PREFIX}{ticket}-%'"
        )
    };

    {
        let conn = Connection::open_in_memory().unwrap();
        ducklake::connect_ducklake(&conn, &connstr, &data_path).unwrap();
        ducklake::ensure_read_tables(&conn).unwrap();
        conn.execute_batch(&clear).unwrap();
    }

    register_read_files(
        &connstr,
        &data_path,
        &[("held", read_rows_sql(held, 6000, 3))],
        owner,
    )
    .unwrap();
    let registration = register_read_files(
        &connstr,
        &data_path,
        &[("held", read_rows_sql(held, 6000, 3))],
        other,
    )
    .unwrap();

    assert_eq!(
        registration.replaced.get("read"),
        None,
        "another ticket's rows are not replaced"
    );
    let reader = Connection::open_in_memory().unwrap();
    ducklake::connect_ducklake(&reader, &connstr, &data_path).unwrap();
    assert_eq!(
        lake_count(&reader, &rows_of(owner)),
        3,
        "the owner's rows are untouched"
    );
    assert_eq!(
        lake_count(&reader, &rows_of(other)),
        3,
        "the other ticket's rows are registered"
    );

    let _ = reader.execute_batch(&clear);
}

/// A `read` file may hold more than one prep_sample. The same ticket
/// re-registering it replaces the rows of each; re-registering one of them
/// replaces only that one's rows.
#[test]
#[serial_test::serial]
#[cfg(feature = "integration")]
fn register_files_replaces_a_tickets_rows_from_a_file_spanning_prep_samples() {
    let connstr = crate::test_catalog::connstr();
    let data_path = crate::test_catalog::data_path();

    let first: i64 = 972_565;
    let second: i64 = 972_566;
    let ticket: i64 = 972_600_000 + std::process::id() as i64;
    let clear = format!("DELETE FROM qiita_lake.read WHERE prep_sample_idx IN ({first}, {second})");
    let count = |prep_sample: i64| {
        format!("SELECT count(*) FROM qiita_lake.read WHERE prep_sample_idx = {prep_sample}")
    };
    let spanning = format!(
        "{} UNION ALL {}",
        read_rows_sql(first, 8000, 2),
        read_rows_sql(second, 9000, 3)
    );

    {
        let conn = Connection::open_in_memory().unwrap();
        ducklake::connect_ducklake(&conn, &connstr, &data_path).unwrap();
        ducklake::ensure_read_tables(&conn).unwrap();
        conn.execute_batch(&clear).unwrap();
    }

    register_read_files(
        &connstr,
        &data_path,
        &[("spanning", spanning.clone())],
        ticket,
    )
    .unwrap();
    let registration =
        register_read_files(&connstr, &data_path, &[("spanning", spanning)], ticket).unwrap();
    assert_eq!(
        registration.replaced.get("read"),
        Some(&5),
        "both prep_samples' rows are replaced"
    );

    // Re-registering only the first leaves the second's rows in the file they share.
    let registration = register_read_files(
        &connstr,
        &data_path,
        &[("first", read_rows_sql(first, 8000, 2))],
        ticket,
    )
    .unwrap();
    assert_eq!(
        registration.replaced.get("read"),
        Some(&2),
        "only the first prep_sample's rows are replaced"
    );

    let reader = Connection::open_in_memory().unwrap();
    ducklake::connect_ducklake(&reader, &connstr, &data_path).unwrap();
    assert_eq!(
        lake_count(&reader, &count(first)),
        2,
        "one copy of the first"
    );
    assert_eq!(
        lake_count(&reader, &count(second)),
        3,
        "the second keeps its rows"
    );

    let _ = reader.execute_batch(&clear);
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
    let connstr = crate::test_catalog::connstr();
    let data_path = crate::test_catalog::data_path();

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
    let connstr = crate::test_catalog::connstr();
    let data_path = crate::test_catalog::data_path();

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
    let connstr = crate::test_catalog::connstr();
    let data_path = crate::test_catalog::data_path();
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
    let connstr = crate::test_catalog::connstr();
    let data_path = crate::test_catalog::data_path();
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
    let connstr = crate::test_catalog::connstr();
    let data_path = crate::test_catalog::data_path();

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
                    // Each writer attaches for itself, as each concurrent
                    // request borrows its own instance in production.
                    register_files(
                        &attached_writer(connstr, data_path),
                        data_path,
                        std::path::Path::new("/"),
                        payload,
                    )
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
    let connstr = crate::test_catalog::connstr();
    let data_path = crate::test_catalog::data_path();

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
        let outcome = register_files(
            &attached_writer(&connstr, &data_path),
            &data_path,
            std::path::Path::new("/"),
            &payload,
        )
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
    let connstr = crate::test_catalog::connstr();
    let data_path = crate::test_catalog::data_path();

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
