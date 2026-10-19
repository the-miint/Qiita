//! Where the integration tier finds the test DuckLake catalog.
//!
//! One home for the two values every catalog-backed test needs: a divergence
//! between copies reads as a test failure in whichever one is stale.

/// The test catalog's libpq connection string.
///
/// The fallback matches the Docker-compose Postgres at `:5433`, so a local
/// Docker-mode run works without setting the env var. The Makefile's
/// `test-integration` recipe in host-Postgres mode (CI macOS, dev macOS without
/// Docker) sets the var explicitly to `:5432`. A test that mutates it must use
/// `EnvSnapshot` (see `main`'s test module) so it does not leak the default-only
/// state into another `#[serial]` test.
pub(crate) fn connstr() -> String {
    std::env::var("DUCKLAKE_CATALOG_CONNSTR").unwrap_or_else(|_| {
        "dbname=qiita_ducklake host=localhost port=5433 user=qiita password=qiita".to_string()
    })
}

/// Where DuckLake writes its Parquet for the test catalog, created if absent.
/// The `/ducklake` suffix must match `config`'s `PATH_PERSISTENT/ducklake`
/// derivation.
pub(crate) fn data_path() -> String {
    let data_path = std::env::var("PATH_PERSISTENT")
        .map(|base| format!("{base}/ducklake"))
        .unwrap_or_else(|_| "/tmp/qiita-integration-ducklake-data".to_string());
    std::fs::create_dir_all(&data_path).expect("create the test catalog's data path");
    data_path
}
