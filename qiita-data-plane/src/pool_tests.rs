//! Pool behaviour that needs no catalog: the in-flight bound, what a returned
//! instance carries, and that the settings reach DuckDB. The real ATTACH is
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

/// The premise the pool's shape rests on: DuckDB's resource settings belong to a
/// *database*, not to a connection. If they were per connection, one database with
/// N connections would give the same bound at a fraction of the catalog-backend
/// cost, and the pool would be the wrong structure. Pinned here so a version bump
/// that changes it fails a test rather than quietly invalidating the design.
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
        "a sibling connection to the same database sees the setting, so settings are \
         database-scoped"
    );

    // Control: a separately opened database does not see it, which is what makes a
    // pool of databases the thing that bounds per-request cost.
    let other = Connection::open_in_memory().expect("open a second database");
    let elsewhere: String = other
        .query_row("SELECT current_setting('memory_limit')", [], |row| {
            row.get(0)
        })
        .expect("read on the other database");
    assert_ne!(
        elsewhere, "512.0 MiB",
        "a separate database must keep its own limit, or pooling instances buys nothing"
    );
}

#[test]
fn checkout_refuses_once_every_instance_is_in_use() {
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
    pool.checkout().expect("a returned instance is reusable");
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

    let conn = pool.checkout().expect("checkout the same instance again");
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
/// than the pool quietly discarding and rebuilding every instance it lends.
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
        "the instance was returned, not discarded"
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
        conn.execute_batch("SET preserve_insertion_order=false;")
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
    assert!(value, "the pool re-applied its baseline");
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
    assert_eq!(
        read_text("temp_directory"),
        tmp.path().display().to_string()
    );
    assert_eq!(read_text("memory_limit"), "512.0 MiB");
    assert_eq!(
        read_text("max_temp_directory_size"),
        "256.0 MiB",
        "the spill cap must be ours, not DuckDB's filesystem-derived default"
    );
}

#[test]
fn every_setting_that_could_end_a_sql_literal_is_rejected_at_construction() {
    let tmp = tempfile::tempdir().unwrap();
    // One case per value that reaches `SET` as a literal; deleting any of the
    // three checks fails here rather than shipping an interpolation.
    let cases: [(&str, fn(&mut PoolSettings)); 3] = [
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
/// instance so a bad setting or an unwritable spill directory fails the unit at
/// boot, and the remaining slots are attached only when a request needs them —
/// each one costs a lake-Postgres backend for as long as it lives.
#[test]
#[cfg(feature = "integration")]
#[serial_test::serial]
fn open_attaches_one_instance_and_the_rest_on_demand() {
    let connstr = crate::flight_service::tests::delete_test_catalog_connstr();
    let data_path = crate::flight_service::tests::delete_test_data_path();
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
        "exactly one instance is attached at open"
    );
    assert_eq!(pool.unbuilt_len(), 2, "the other two are not attached yet");

    let first = pool.checkout().expect("the attached instance");
    let second = pool.checkout().expect("an instance attached on demand");
    assert_eq!(pool.unbuilt_len(), 1);
    let _third = pool.checkout().expect("the last slot");
    assert_eq!(pool.unbuilt_len(), 0);
    assert!(
        pool.checkout().is_err(),
        "a fourth request has nothing left to borrow"
    );

    // What a reused attach is for: the instance comes back and answers again.
    // `idle_len` is the half a query cannot show — a reset that failed on an
    // attached instance would discard it, credit the slot and rebuild, and the
    // query below would still succeed.
    drop(first);
    drop(second);
    assert_eq!(
        pool.idle_len(),
        2,
        "both were returned and reset, not discarded and rebuilt"
    );
    let reused = pool.checkout().expect("a returned instance");
    let one: i64 = reused
        .query_row("SELECT 1", [], |row| row.get(0))
        .expect("a returned instance still answers");
    assert_eq!(one, 1);
}
