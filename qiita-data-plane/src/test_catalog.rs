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

/// Counts the test catalog server's backends open under one instance's session
/// names, `ducklake::catalog_application_name(port, pool_size)`, through an
/// observer attach of its own.
pub(crate) struct CatalogSessions {
    observer: duckdb::Connection,
    /// Derived from the process id, so concurrent test processes against one
    /// catalog are unlikely to share a session name.
    pub(crate) port: u16,
}

impl CatalogSessions {
    pub(crate) fn new() -> Self {
        let observer = duckdb::Connection::open_in_memory().unwrap();
        observer
            .execute_batch(&format!(
                "LOAD postgres; ATTACH '{}' AS observer (TYPE postgres);",
                connstr()
            ))
            .unwrap();
        Self {
            observer,
            port: 1 + (std::process::id() % 60000) as u16,
        }
    }

    /// Backends open now under the name `pool_size` gives.
    pub(crate) fn count(&self, pool_size: usize) -> i64 {
        let name = crate::ducklake::catalog_application_name(self.port, pool_size);
        self.observer
            .query_row(
                &format!(
                    "SELECT n FROM postgres_query('observer', \
                     'SELECT count(*) AS n FROM pg_stat_activity \
                      WHERE application_name = ''{name}''')"
                ),
                [],
                |r| r.get(0),
            )
            .unwrap()
    }
}
