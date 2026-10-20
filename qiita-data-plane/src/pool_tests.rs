//! Pool behaviour that needs no catalog: the in-flight bound, what a returned
//! connection carries, and that the settings reach DuckDB. The real ATTACH is
//! exercised by the integration tier.

use super::*;

fn pool_without_catalog(size: usize, temp_directory: PathBuf) -> Arc<LakePool> {
    LakePool::unattached_for_tests(PoolSettings::for_tests(size, temp_directory))
}

#[test]
fn a_pool_of_zero_is_rejected() {
    let err = LakePool::open(
        "unused".to_string(),
        "unused".to_string(),
        PoolSettings::for_tests(0, std::env::temp_dir().join("qiita-pool-zero")),
    )
    .expect_err("size 0 must not build");
    assert!(
        err.to_string().contains("at least 1"),
        "unexpected error: {err}"
    );
}

/// The premise `pool`'s module docs rest on, pinned so a version bump that changes
/// it fails a test rather than quietly invalidating the design.
#[test]
fn duckdb_resource_settings_belong_to_the_database_not_the_connection() {
    let first = Connection::open_in_memory().expect("open");
    let sibling = first
        .try_clone()
        .expect("a second connection to the same database");
    first
        .execute_batch("SET memory_limit='512MiB';")
        .expect("set on the first connection");

    let seen: String = sibling
        .query_row("SELECT current_setting('memory_limit')", [], |row| {
            row.get(0)
        })
        .expect("read on the sibling");
    assert_eq!(
        seen, "512.0 MiB",
        "a sibling connection to the same database sees the setting"
    );

    // Control: a separately opened database does not.
    let other = Connection::open_in_memory().expect("open a second database");
    let elsewhere: String = other
        .query_row("SELECT current_setting('memory_limit')", [], |row| {
            row.get(0)
        })
        .expect("read on the other database");
    assert_ne!(
        elsewhere, "512.0 MiB",
        "a separate database must keep its own limit"
    );
}

#[test]
fn checkout_refuses_once_every_connection_is_in_use() {
    let tmp = tempfile::tempdir().unwrap();
    let pool = pool_without_catalog(2, tmp.path().to_path_buf());

    let first = pool.checkout().expect("first checkout");
    let second = pool.checkout().expect("second checkout");
    let err = pool
        .checkout()
        .expect_err("a third checkout has nothing left to hand out");

    assert_eq!(err.code(), tonic::Code::ResourceExhausted);
    assert!(
        err.message().contains("busy") && err.message().contains('2'),
        "the refusal names the bound it hit: {}",
        err.message()
    );
    drop(first);
    drop(second);
    assert_eq!(pool.idle_len(), 2, "both came back");
    pool.checkout().expect("a returned connection is reusable");
}

#[test]
fn an_unfinished_transaction_does_not_outlive_the_borrow() {
    let tmp = tempfile::tempdir().unwrap();
    let pool = pool_without_catalog(1, tmp.path().to_path_buf());

    {
        let conn = pool.checkout().expect("checkout");
        conn.execute_batch("CREATE TABLE t (i INTEGER);")
            .expect("create");
        conn.execute_batch("BEGIN TRANSACTION; INSERT INTO t VALUES (1);")
            .expect("begin and insert");
        // The borrow ends here with the transaction still open, which is what a
        // request that failed mid-transaction leaves behind.
    }

    let conn = pool.checkout().expect("checkout the same connection again");
    let rows: i64 = conn
        .query_row("SELECT count(*) FROM t", [], |row| row.get(0))
        .expect("count");
    assert_eq!(
        rows, 0,
        "the previous borrow's uncommitted row must not be visible"
    );
    // And it cannot be committed by this borrower either: with the transaction
    // rolled back there is none to commit. Measured, an inherited open
    // transaction would have let this succeed and keep the row.
    let err = conn
        .execute_batch("COMMIT;")
        .expect_err("no transaction may be left open for the next borrower");
    assert!(
        err.to_string().contains(NO_TRANSACTION_TO_ROLL_BACK),
        "unexpected error: {err}"
    );
}

/// The other half of the reset: a borrow that ends with nothing open comes back
/// without being treated as broken. This is what pins the one error string
/// `reset` tolerates — if DuckDB ever words it differently, this fails rather
/// than the pool quietly discarding and rebuilding every connection it lends.
#[test]
fn a_clean_borrow_comes_back_without_error() {
    let tmp = tempfile::tempdir().unwrap();
    let pool = pool_without_catalog(1, tmp.path().to_path_buf());
    {
        let conn = pool.checkout().expect("checkout");
        conn.execute_batch("CREATE TABLE t (i INTEGER); INSERT INTO t VALUES (1);")
            .expect("plain statements, no explicit transaction");
    }
    assert_eq!(
        pool.idle_len(),
        1,
        "the connection was returned, not discarded"
    );
    let conn = pool.checkout().expect("checkout again");
    let rows: i64 = conn
        .query_row("SELECT count(*) FROM t", [], |row| row.get(0))
        .expect("count");
    assert_eq!(rows, 1, "a committed write survives the reset");
}

#[test]
fn a_setting_the_borrower_changed_is_restored() {
    let tmp = tempfile::tempdir().unwrap();
    let pool = pool_without_catalog(1, tmp.path().to_path_buf());

    {
        let conn = pool.checkout().expect("checkout");
        conn.execute_batch("SET preserve_insertion_order=true;")
            .expect("set");
    }

    let conn = pool.checkout().expect("checkout again");
    // BOOLEAN, not text — `current_setting` is typed per setting (measured).
    let value: bool = conn
        .query_row(
            "SELECT current_setting('preserve_insertion_order')",
            [],
            |row| row.get(0),
        )
        .expect("read back");
    assert!(!value, "the pool re-applied its baseline");
}

/// A connection whose database has spilled comes back to the pool rather than
/// being discarded: DuckDB refuses any `SET temp_directory` once the directory
/// has been used, the same path included, so a reset that re-set it would fail
/// for every borrow that spilled.
#[test]
fn a_connection_that_spilled_comes_back() {
    let tmp = tempfile::tempdir().unwrap();
    let mut settings = PoolSettings::for_tests(1, tmp.path().to_path_buf());
    settings.memory_limit = "32MiB".to_string();
    settings.threads = 1;
    let pool = LakePool::unattached_for_tests(settings);

    let spill = {
        let conn = pool.checkout().expect("checkout");
        let spill: String = conn
            .query_row("SELECT current_setting('temp_directory')", [], |row| {
                row.get(0)
            })
            .expect("read temp_directory");
        conn.execute_batch(
            "CREATE TABLE t AS SELECT hash(range) AS x, repeat('y', 64) AS pad
                 FROM range(1500000);",
        )
        .expect("set up a table larger than the limit");
        let mut stmt = conn
            .prepare("SELECT x, pad FROM t ORDER BY x, pad")
            .expect("prepare");
        let mut rows = stmt.query([]).expect("a sort that spills");
        rows.next().expect("first row");
        let spilled = std::fs::read_dir(&spill).unwrap().count();
        assert!(spilled > 0, "the sort must spill for this to say anything");
        spill
    };

    assert_eq!(
        pool.idle_len(),
        1,
        "the connection was returned, not discarded"
    );
    let conn = pool.checkout().expect("checkout again");
    let again: String = conn
        .query_row("SELECT current_setting('temp_directory')", [], |row| {
            row.get(0)
        })
        .expect("read temp_directory");
    assert_eq!(again, spill, "the same connection, with its own directory");
}

#[test]
fn the_pool_settings_reach_duckdb() {
    let tmp = tempfile::tempdir().unwrap();
    let pool = pool_without_catalog(1, tmp.path().to_path_buf());
    let conn = pool.checkout().expect("checkout");

    // `current_setting` is typed per setting: `threads` comes back BIGINT, the
    // rest here VARCHAR, and DuckDB renders a byte size in binary units
    // (measured). The expected values are exact so a changed rendering fails
    // here rather than passing silently.
    let read_text = |name: &str| -> String {
        conn.query_row(&format!("SELECT current_setting('{name}')"), [], |row| {
            row.get(0)
        })
        .unwrap_or_else(|e| panic!("read {name}: {e}"))
    };
    let threads: i64 = conn
        .query_row("SELECT current_setting('threads')", [], |row| row.get(0))
        .expect("read threads");

    assert_eq!(threads, 2);
    let spill = PathBuf::from(read_text("temp_directory"));
    assert_eq!(
        spill.parent(),
        Some(tmp.path()),
        "a connection spills into a directory of its own under the root, got {}",
        spill.display()
    );
    assert!(
        spill
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|n| n.starts_with(&pool.spill_prefix)),
        "the directory is named for the host and process that own it: {}",
        spill.display()
    );
    assert_eq!(read_text("memory_limit"), "512.0 MiB");
    assert_eq!(
        read_text("max_temp_directory_size"),
        "256.0 MiB",
        "the spill cap must be ours, not DuckDB's filesystem-derived default"
    );
    let ordered: bool = conn
        .query_row(
            "SELECT current_setting('preserve_insertion_order')",
            [],
            |row| row.get(0),
        )
        .expect("read preserve_insertion_order");
    assert!(!ordered, "order is off unless a handler asks for it");
}

#[test]
fn every_connection_spills_into_a_directory_of_its_own_and_takes_it_when_dropped() {
    let tmp = tempfile::tempdir().unwrap();
    let pool = pool_without_catalog(3, tmp.path().to_path_buf());
    let spill_of = |conn: &Connection| -> PathBuf {
        PathBuf::from(
            conn.query_row("SELECT current_setting('temp_directory')", [], |row| {
                row.get::<_, String>(0)
            })
            .expect("read temp_directory"),
        )
    };

    let held: Vec<_> = (0..3).map(|_| pool.checkout().expect("checkout")).collect();
    let dirs: std::collections::BTreeSet<PathBuf> = held.iter().map(|c| spill_of(c)).collect();
    assert_eq!(
        dirs.len(),
        3,
        "three connections, three directories: {dirs:?}"
    );
    for dir in &dirs {
        assert!(
            dir.is_dir(),
            "{} exists while its connection lives",
            dir.display()
        );
    }

    // A connection keeps its directory across a borrow: the reset re-applies the
    // same one rather than minting another.
    let first = spill_of(&held[0]);
    drop(held);
    // All three held at once, so each is a different connection.
    let held_again: Vec<_> = (0..3)
        .map(|_| pool.checkout().expect("checkout again"))
        .collect();
    let again: std::collections::BTreeSet<PathBuf> =
        held_again.iter().map(|c| spill_of(c)).collect();
    assert_eq!(again, dirs, "returned connections kept their directories");
    assert!(again.contains(&first));
    drop(held_again);

    drop(pool);
    for dir in &dirs {
        assert!(
            !dir.exists(),
            "{} is removed with its connection",
            dir.display()
        );
    }
}

/// The premise `Member` rests on, pinned so a DuckDB bump that changes it fails
/// here: two databases spilling into directories of their own write files of the
/// same names, so sharing one directory would have them write the same files.
/// What sharing does instead — the crash — is measured, not tested, since a
/// segfault takes the test binary with it.
#[test]
fn duckdb_names_its_spill_files_the_same_in_every_database() {
    let tmp = tempfile::tempdir().unwrap();
    let spilled_names = |dir: &std::path::Path| -> std::collections::BTreeSet<String> {
        std::fs::create_dir_all(dir).unwrap();
        let conn = Connection::open_in_memory().expect("open");
        conn.execute_batch(&format!(
            "SET temp_directory='{}'; SET memory_limit='32MiB'; SET threads=1;
             SET preserve_insertion_order=false;
             CREATE TABLE t AS SELECT hash(range) AS x, repeat('y', 64) AS pad
                 FROM range(1500000);",
            dir.display()
        ))
        .expect("set up a table larger than the limit");
        // Hold the result open so the spill files are still on disk to list.
        let mut stmt = conn
            .prepare("SELECT x, pad FROM t ORDER BY x, pad")
            .expect("prepare");
        let mut rows = stmt.query([]).expect("a sort that spills");
        rows.next().expect("first row");
        std::fs::read_dir(dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect()
    };

    let a = spilled_names(&tmp.path().join("a"));
    let b = spilled_names(&tmp.path().join("b"));
    assert!(
        !a.is_empty(),
        "the sort must spill for this to say anything"
    );
    assert!(
        !a.is_disjoint(&b),
        "DuckDB now names spill files uniquely per database ({a:?} vs {b:?}) — the \
         per-connection directories in `Member` may no longer be needed"
    );
}

#[test]
fn the_sweep_removes_only_this_hosts_directories_whose_process_is_gone() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    // A pid that was running a moment ago and is not now.
    let mut child = std::process::Command::new("true").spawn().expect("spawn");
    let gone = child.id();
    child.wait().expect("reap");
    let me = std::process::id();
    let mkdir = |name: &str| {
        std::fs::create_dir(root.join(name)).unwrap();
        name.to_string()
    };

    let abandoned = mkdir(&format!("qiita-{gone}-Ab3dE9"));
    let kept = [
        mkdir(&format!("qiita-{me}-Ab3dE9")),         // this process
        mkdir(&format!("qiita-{}-Ab3dE9", 1)),        // a running process
        mkdir(&format!("other-{gone}-Ab3dE9")),       // another host
        mkdir(&format!("qiita-2-{gone}-Ab3dE9")),     // a host named qiita-2
        mkdir(&format!("qiita-{gone}")),              // no suffix
        mkdir("qiita-0-Ab3dE9"),                      // pid 0
        mkdir(&format!("qiita-{}-Ab3dE9", u32::MAX)), // past pid_t
    ];
    let file = format!("qiita-{gone}-AbFile");
    std::fs::write(root.join(&file), b"").unwrap();

    let removed = sweep_abandoned_spill_directories(root, "qiita").expect("sweep");

    assert_eq!(removed, vec![root.join(&abandoned)]);
    assert!(!root.join(&abandoned).exists());
    for name in kept.iter().chain([&file]) {
        assert!(root.join(name).exists(), "{name} must be left alone");
    }
}

/// Two instances booting together can both list one abandoned directory; the
/// second to remove it finds it gone, which is not a reason to stop boot.
#[test]
fn a_spill_directory_already_removed_is_not_a_boot_failure() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("qiita-1-Ab3dE9");
    std::fs::create_dir(&dir).unwrap();

    assert_eq!(remove_abandoned_spill_directory(&dir, 1), Ok(true));
    assert!(!dir.exists());
    assert_eq!(
        remove_abandoned_spill_directory(&dir, 1),
        Ok(false),
        "the second removal finds it gone"
    );
}

#[test]
fn every_setting_that_could_end_a_sql_literal_is_rejected_at_construction() {
    /// The env var a case poisons, and how.
    type PoisonCase = (&'static str, fn(&mut PoolSettings));

    let tmp = tempfile::tempdir().unwrap();
    // One case per value that reaches `SET` as a literal; deleting any of the
    // three checks fails here rather than shipping an interpolation.
    let cases: [PoisonCase; 3] = [
        ("DUCKDB_MEMORY_LIMIT", |s| {
            s.memory_limit = "512MiB'; ATTACH 'evil".to_string()
        }),
        ("DUCKDB_MAX_TEMP_DIRECTORY_SIZE", |s| {
            s.max_temp_directory_size = "256MiB'; ATTACH 'evil".to_string()
        }),
        ("DUCKDB_TEMP_DIRECTORY", |s| {
            s.temp_directory = PathBuf::from("/tmp/x'; ATTACH 'evil")
        }),
    ];
    for (var, poison) in cases {
        let mut settings = PoolSettings::for_tests(1, tmp.path().to_path_buf());
        poison(&mut settings);
        let err = LakePool::open("unused".to_string(), "unused".to_string(), settings)
            .expect_err("a quote in a setting must not reach DuckDB's SQL");
        assert!(
            err.to_string().contains(var),
            "the error names the variable that carried it, got {err}"
        );
    }
}

/// The lazy half of the bound, against a real catalog: `open` attaches one
/// connection so a bad setting or an unwritable spill root fails the unit at boot,
/// and the remaining slots are attached only when a request needs them —
/// each one costs a lake-Postgres backend for as long as it lives.
#[test]
#[cfg(feature = "integration")]
#[serial_test::serial]
fn open_attaches_one_connection_and_the_rest_on_demand() {
    let connstr = crate::test_catalog::connstr();
    let data_path = crate::test_catalog::data_path();
    let tmp = tempfile::tempdir().unwrap();

    let pool = LakePool::open(
        connstr,
        data_path,
        PoolSettings::for_tests(3, tmp.path().to_path_buf()),
    )
    .expect("open a pool against the test catalog");
    assert_eq!(
        pool.idle_len(),
        1,
        "exactly one connection is attached at open"
    );
    assert_eq!(pool.unbuilt_len(), 2, "the other two are not attached yet");

    let first = pool.checkout().expect("the attached connection");
    let second = pool.checkout().expect("a connection attached on demand");
    assert_eq!(pool.unbuilt_len(), 1);
    let _third = pool.checkout().expect("the last slot");
    assert_eq!(pool.unbuilt_len(), 0);
    assert!(
        pool.checkout().is_err(),
        "a fourth request has nothing left to borrow"
    );

    // What a reused attach is for: the connection comes back and answers again.
    // `idle_len` is the half a query cannot show — a reset that failed on an
    // attached connection would discard it, credit the slot and rebuild, and the
    // query below would still succeed.
    drop(first);
    drop(second);
    assert_eq!(
        pool.idle_len(),
        2,
        "both were returned and reset, not discarded and rebuilt"
    );
    let reused = pool.checkout().expect("a returned connection");
    let one: i64 = reused
        .query_row("SELECT 1", [], |row| row.get(0))
        .expect("a returned connection still answers");
    assert_eq!(one, 1);
}

#[test]
fn a_lake_write_runs_ordered_on_one_thread_and_the_baseline_comes_back() {
    let tmp = tempfile::tempdir().unwrap();
    let pool = pool_without_catalog(1, tmp.path().to_path_buf());
    let threads = |conn: &Connection| -> i64 {
        conn.query_row("SELECT current_setting('threads')", [], |row| row.get(0))
            .expect("read threads")
    };
    let ordered = |conn: &Connection| -> bool {
        conn.query_row(
            "SELECT current_setting('preserve_insertion_order')",
            [],
            |row| row.get(0),
        )
        .expect("read preserve_insertion_order")
    };

    {
        let writer = pool.checkout_for_write().expect("checkout for write");
        assert_eq!(threads(&writer), 1);
        assert!(ordered(&writer));
    }
    assert_eq!(pool.idle_len(), 1, "the writer's connection came back");
    let conn = pool.checkout().expect("checkout for read");
    assert_eq!(threads(&conn), 2, "a read gets the pool's threads back");
    assert!(!ordered(&conn), "and the pool's unordered baseline");
}

/// What `LAKE_WRITE_SETTINGS` rests on, against the test catalog: a sorted
/// source inserted into a lake table at several threads comes back out of order,
/// and at the writer's settings it does not. When DuckLake keeps the order of a
/// parallel insert (duckdb/ducklake#1356), the first assertion fails and the
/// one-thread setting can be reconsidered. The parallel insert is tried up to five
/// times, because whether it reorders depends on scheduling: measured with this
/// source, 20 of 20 inserts at eight threads reordered, and 16 of 20 at two.
#[test]
#[cfg(feature = "integration")]
#[serial_test::serial]
fn a_parallel_insert_into_the_lake_loses_order_and_the_writer_keeps_it() {
    let connstr = crate::test_catalog::connstr();
    let data_path = crate::test_catalog::data_path();
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("sorted.parquet");
    let conn = Connection::open_in_memory().expect("open");
    crate::ducklake::connect_ducklake(&conn, &connstr, &data_path).expect("attach DuckLake");
    conn.execute_batch(&format!(
        "COPY (SELECT range AS k, md5(range::VARCHAR) AS pad FROM range(4000000) ORDER BY k)
             TO '{}' (FORMAT PARQUET);",
        source.display()
    ))
    .expect("write a sorted source");
    let rows_out_of_order = |conn: &Connection, table: &str| -> i64 {
        conn.execute_batch(&format!(
            "DROP TABLE IF EXISTS qiita_lake.{table};
             CREATE TABLE qiita_lake.{table} (k BIGINT, pad VARCHAR);
             INSERT INTO qiita_lake.{table} SELECT * FROM read_parquet('{}');",
            source.display()
        ))
        .expect("insert the sorted source");
        let n = conn
            .query_row(
                &format!(
                    "SELECT count(*) FROM (SELECT k < lag(k) OVER (ORDER BY rowid) AS back
                     FROM qiita_lake.{table}) WHERE back"
                ),
                [],
                |row| row.get(0),
            )
            .expect("count rows out of order");
        conn.execute_batch(&format!("DROP TABLE qiita_lake.{table};"))
            .expect("drop the probe table");
        n
    };

    conn.execute_batch("SET threads=8; SET preserve_insertion_order=true;")
        .expect("set a parallel insert");
    assert!(
        (0..5).any(|_| rows_out_of_order(&conn, "order_probe_parallel") > 0),
        "five parallel inserts kept their order: DuckLake may have fixed duckdb/ducklake#1356"
    );

    let writer = LakeWriter::for_tests(conn);
    assert_eq!(
        rows_out_of_order(&writer, "order_probe_writer"),
        0,
        "a lake write must keep its source's order"
    );
}
