use arrow_flight::flight_service_server::FlightServiceServer;
use duckdb::Connection;
use tonic::transport::Server;
use tonic_health::ServingStatus;

mod auth;
mod config;
mod ducklake;
mod flight_service;
mod miint;
mod pool;
mod resources;
// Gated with its consumers, every one of which is an `integration` test: under
// `#[cfg(test)]` alone its two functions are dead code on a plain `cargo test`.
#[cfg(all(test, feature = "integration"))]
mod test_catalog;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = config::Settings::from_env().map_err(|e| {
        eprintln!("Configuration error: {e}");
        e
    })?;

    // Ensure DuckLake tables exist (one-time setup connection)
    let setup_conn = Connection::open_in_memory()?;
    ducklake::connect_ducklake(
        &setup_conn,
        &cfg.ducklake_catalog_connstr,
        &cfg.path_persistent_ducklake,
    )?;
    // Catalog-global Parquet defaults (zstd + v2). Set ONCE here at boot, NOT on
    // every attach the pool makes: a per-attach write races on ducklake_metadata
    // under concurrent Flight load and fails with SQLSTATE 40001. See
    // set_catalog_options.
    ducklake::set_catalog_options(&setup_conn)?;
    ducklake::ensure_lake_tables(&setup_conn)?;
    drop(setup_conn);

    // One bounded pool of attached connections serves every request; its size is
    // the number in flight. Logged after it opens, so the line is a statement
    // that these settings applied rather than that they were intended.
    let pool_log = format!(
        "qiita-data-plane pool: {} connections x {} threads, memory_limit {} \
         (from a {:.1} GiB ceiling read via {}, CPU budget {}), \
         spill under {} capped at {}; {} concurrent uploads",
        cfg.pool.size,
        cfg.pool.threads,
        cfg.pool.memory_limit,
        cfg.memory_ceiling.bytes as f64 / (1024.0 * 1024.0 * 1024.0),
        cfg.memory_ceiling.source.as_str(),
        cfg.thread_budget,
        cfg.pool.temp_directory.display(),
        cfg.pool.max_temp_directory_size,
        cfg.max_concurrent_doput,
    );
    let lake_pool = pool::LakePool::open(
        cfg.ducklake_catalog_connstr,
        cfg.path_persistent_ducklake,
        cfg.pool,
    )?;
    eprintln!("{pool_log}");

    let flight_svc = flight_service::QiitaFlightService::new(
        cfg.flight_public_key,
        lake_pool,
        cfg.max_concurrent_doput,
        cfg.path_scratch_staging,
        cfg.path_scratch,
    );

    let (health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter
        .set_service_status("", ServingStatus::Serving)
        .await;

    // gRPC reflection lets `grpcurl` introspect the server (used
    // by `make verify-health` from the deploy host). Both v1 and
    // v1alpha are registered: grpcurl 1.9.3 (the version pinned in
    // the Makefile today) tries v1alpha first by default, and v1
    // is forward-compat for when grpcurl drops v1alpha. Reflection
    // only exposes the tonic-health descriptor here — the Flight
    // service uses arrow-flight's prebuilt bindings, which don't
    // ship a public descriptor set, so it's intentionally not
    // reflected.
    let reflection_v1 = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(tonic_health::pb::FILE_DESCRIPTOR_SET)
        .build_v1()?;
    let reflection_v1alpha = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(tonic_health::pb::FILE_DESCRIPTOR_SET)
        .build_v1alpha()?;

    println!("qiita-data-plane listening on {}", cfg.listen_addr);

    // Arrow Flight DoPut batches for chunked uploads run up to ~1 GiB
    // on dense GG2-scale data (see _CHUNK_ROWS_PER_BATCH × _CHUNK_SIZE
    // in the CLI's reference_load.py — 16384 rows × 64 KB chunks).
    // tonic's default 4 MiB max-decoding-message-size rejects these
    // outright with "decoded message length too large". Bump to 1 GiB
    // so the server accepts the batch sizes the chunked-upload design
    // actually produces.
    const FLIGHT_MAX_DECODING_BYTES: usize = 1024 * 1024 * 1024;

    Server::builder()
        .add_service(health_service)
        .add_service(reflection_v1)
        .add_service(reflection_v1alpha)
        .add_service(
            FlightServiceServer::new(flight_svc)
                .max_decoding_message_size(FLIGHT_MAX_DECODING_BYTES),
        )
        .serve(cfg.listen_addr)
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::config::Settings;
    use super::miint;
    use base64::Engine;
    use duckdb::Connection;
    use serial_test::serial;

    /// RAII guard that snapshots the current values of named env vars on
    /// construction and restores them on drop. Tests that mutate global
    /// process env (Settings::from_env() reads `std::env`) MUST use this
    /// — otherwise an `env::remove_var` in one test races against
    /// `std::env::var` reads in another `#[serial]` test that runs in the
    /// same cargo-test process. Concretely, the host-Postgres CI leg
    /// flaked before this guard existed: the ducklake tests share
    /// `#[serial]` with these config tests and saw a stale absence of
    /// `DUCKLAKE_CATALOG_CONNSTR` left behind by a config test that
    /// scheduled earlier in the same run.
    struct EnvSnapshot {
        original: Vec<(&'static str, Option<String>)>,
    }

    impl EnvSnapshot {
        fn capture(names: &[&'static str]) -> Self {
            let original = names.iter().map(|&n| (n, std::env::var(n).ok())).collect();
            Self { original }
        }
    }

    impl Drop for EnvSnapshot {
        fn drop(&mut self) {
            for (name, value) in &self.original {
                match value {
                    Some(v) => std::env::set_var(name, v),
                    None => std::env::remove_var(name),
                }
            }
        }
    }

    /// An absolute temp path under `TMPDIR` (via `std::env::temp_dir()`), never a
    /// bare `/tmp` — that isn't assured present/writable on every platform
    /// (macOS and some CI sandboxes use a per-user `TMPDIR`). The config tests
    /// only need an absolute string; the path is never created or written.
    fn tmp_abs(name: &str) -> String {
        std::env::temp_dir()
            .join(name)
            .to_str()
            .expect("temp_dir path is valid UTF-8")
            .to_string()
    }

    #[test]
    #[serial]
    fn config_with_valid_env() {
        let _snapshot = EnvSnapshot::capture(&[
            "LISTEN_ADDR",
            "FLIGHT_TICKET_PUBLIC_KEY",
            "DUCKLAKE_CATALOG_CONNSTR",
            "PATH_SCRATCH",
            "PATH_PERSISTENT",
            "DUCKDB_MEMORY_LIMIT",
        ]);
        std::env::remove_var("LISTEN_ADDR");
        std::env::set_var("DUCKDB_MEMORY_LIMIT", TEST_MEMORY_LIMIT);
        // Ed25519 PUBLIC key (base64) — the verification half of the fixed test
        // seed [7u8; 32] used across the suite. A public key is not secret
        // (it only verifies; it cannot sign), so hardcoding it here is safe.
        let pubkey = "6kpsY+KcUgq+9VB7Ey7F+ZVHdq6+vnuSQh7qaRRG0iw=".to_string();
        std::env::set_var("FLIGHT_TICKET_PUBLIC_KEY", &pubkey);
        std::env::set_var("DUCKLAKE_CATALOG_CONNSTR", "dbname=test host=localhost");
        let scratch = tmp_abs("qiita-test-scratch");
        let persistent = tmp_abs("qiita-test-persistent");
        std::env::set_var("PATH_SCRATCH", &scratch);
        std::env::set_var("PATH_PERSISTENT", &persistent);
        let cfg = Settings::from_env().expect("Settings::from_env() failed with valid config");
        assert_eq!(cfg.listen_addr.to_string(), "0.0.0.0:50051");
        // The decoded Ed25519 public key round-trips to the env value's bytes.
        assert_eq!(
            cfg.flight_public_key.to_bytes().to_vec(),
            base64::engine::general_purpose::STANDARD
                .decode(&pubkey)
                .unwrap()
        );
        assert_eq!(cfg.ducklake_catalog_connstr, "dbname=test host=localhost");
        // Leaf paths are derived from the base roots with fixed suffixes.
        assert_eq!(
            cfg.path_scratch_staging,
            std::path::PathBuf::from(&scratch).join("staging")
        );
        assert_eq!(
            cfg.path_persistent_ducklake,
            format!("{persistent}/ducklake")
        );
    }

    /// Every required var, so a test can get to the pool settings. Returns the
    /// scratch and persistent roots it chose.
    fn set_required_env() -> (String, String) {
        std::env::set_var(
            "FLIGHT_TICKET_PUBLIC_KEY",
            "6kpsY+KcUgq+9VB7Ey7F+ZVHdq6+vnuSQh7qaRRG0iw=",
        );
        std::env::set_var("DUCKLAKE_CATALOG_CONNSTR", "dbname=test host=localhost");
        let scratch = tmp_abs("qiita-test-scratch");
        let persistent = tmp_abs("qiita-test-persistent");
        std::env::set_var("PATH_SCRATCH", &scratch);
        std::env::set_var("PATH_PERSISTENT", &persistent);
        (scratch, persistent)
    }

    /// What `set_required_env` sets, plus the `HOME` the spill directory derives
    /// from — snapshotted separately from the pool's own vars so neither list is
    /// a copy of the other.
    const REQUIRED_VARS: &[&str] = &[
        "FLIGHT_TICKET_PUBLIC_KEY",
        "DUCKLAKE_CATALOG_CONNSTR",
        "PATH_SCRATCH",
        "PATH_PERSISTENT",
        "HOME",
    ];

    /// The optional vars the pool and the upload bound read.
    const POOL_VARS: &[&str] = &[
        "DUCKLAKE_POOL_SIZE",
        "DUCKDB_THREADS",
        "DUCKDB_MEMORY_LIMIT",
        "DUCKDB_TEMP_DIRECTORY",
        "DUCKDB_MAX_TEMP_DIRECTORY_SIZE",
        "FLIGHT_MAX_CONCURRENT_DOPUT",
    ];

    /// A memory limit for the tests that are not about deriving one, so they do
    /// not depend on the memory of the machine running them.
    const TEST_MEMORY_LIMIT: &str = "1GiB";

    /// The snapshot guards plus a known starting point: every required var set,
    /// every optional pool var cleared. The caller holds the guards, so the
    /// snapshot is restored when its test ends.
    #[must_use]
    fn pool_test_env() -> (EnvSnapshot, EnvSnapshot) {
        let guards = (
            EnvSnapshot::capture(REQUIRED_VARS),
            EnvSnapshot::capture(POOL_VARS),
        );
        set_required_env();
        for var in POOL_VARS {
            std::env::remove_var(var);
        }
        guards
    }

    /// With nothing configured, the pool still has a bound and every connection
    /// still has limits.
    #[test]
    #[serial]
    fn pool_settings_are_derived_when_unset() {
        let _env = pool_test_env();
        // Where this machine's memory gives the default pool a share below
        // `MIN_MEMORY_LIMIT_MIB`, boot refuses the pool, so there are no derived
        // settings to check.
        let ceiling = super::resources::detect_memory_ceiling();
        let share_mib =
            super::config::derived_share_mib(ceiling.bytes, super::config::DEFAULT_POOL_SIZE);
        if share_mib < super::config::MIN_MEMORY_LIMIT_MIB {
            eprintln!(
                "skipped: the {} MiB ceiling detected via {} gives the default pool \
                 {share_mib} MiB per connection, below the {} MiB floor",
                ceiling.bytes / (1024 * 1024),
                ceiling.source.as_str(),
                super::config::MIN_MEMORY_LIMIT_MIB,
            );
            return;
        }
        // Set, so the assertion below shows the spill root does not follow it.
        std::env::set_var("HOME", tmp_abs("qiita-test-home"));

        let cfg = Settings::from_env().expect("from_env with no pool vars set");

        assert_eq!(
            cfg.pool.size,
            super::config::DEFAULT_POOL_SIZE,
            "the default in-flight bound"
        );
        assert!(cfg.pool.threads >= 1, "a request always gets a thread");
        assert_eq!(
            cfg.pool.threads,
            std::cmp::max(1, (cfg.thread_budget / cfg.pool.size) as u32),
            "threads is the CPU budget divided by the bound"
        );
        assert!(
            cfg.pool.memory_limit.ends_with("MiB"),
            "a derived memory_limit is a binary size, got {}",
            cfg.pool.memory_limit
        );
        assert!(
            cfg.memory_ceiling.bytes > 0 && !cfg.memory_ceiling.source.as_str().is_empty(),
            "the ceiling it was divided from is reported with its source"
        );
        assert_eq!(
            cfg.pool.temp_directory,
            std::path::PathBuf::from(tmp_abs("qiita-test-scratch")).join("duckdb-spill"),
            "the spill root is on PATH_SCRATCH, not under HOME"
        );
        assert_eq!(cfg.pool.max_temp_directory_size, "4GiB");
        assert_eq!(
            cfg.max_concurrent_doput, 8,
            "uploads are bounded too, though they borrow no connection"
        );
    }

    #[test]
    #[serial]
    fn pool_settings_are_taken_from_the_environment() {
        let _env = pool_test_env();
        let spill = tmp_abs("qiita-test-spill");
        std::env::set_var("DUCKLAKE_POOL_SIZE", "3");
        std::env::set_var("DUCKDB_THREADS", "5");
        std::env::set_var("DUCKDB_MEMORY_LIMIT", "7GiB");
        std::env::set_var("DUCKDB_TEMP_DIRECTORY", &spill);
        std::env::set_var("DUCKDB_MAX_TEMP_DIRECTORY_SIZE", "11GiB");
        std::env::set_var("FLIGHT_MAX_CONCURRENT_DOPUT", "2");

        let cfg = Settings::from_env().expect("from_env with explicit pool vars");

        assert_eq!(cfg.pool.size, 3);
        assert_eq!(cfg.pool.threads, 5);
        assert_eq!(cfg.pool.memory_limit, "7GiB");
        assert_eq!(cfg.pool.temp_directory, std::path::PathBuf::from(spill));
        assert_eq!(cfg.pool.max_temp_directory_size, "11GiB");
        assert_eq!(cfg.max_concurrent_doput, 2);
    }

    #[test]
    #[serial]
    fn config_rejects_a_pool_that_serves_nothing() {
        let _env = pool_test_env();
        std::env::set_var("DUCKDB_MEMORY_LIMIT", TEST_MEMORY_LIMIT);
        std::env::set_var("DUCKLAKE_POOL_SIZE", "0");
        let err = Settings::from_env().unwrap_err();
        assert!(
            err.contains("DUCKLAKE_POOL_SIZE") && err.contains("at least 1"),
            "unexpected error: {err}"
        );

        std::env::set_var("DUCKLAKE_POOL_SIZE", "8");
        std::env::set_var("DUCKDB_THREADS", "0");
        let err = Settings::from_env().unwrap_err();
        assert!(
            err.contains("DUCKDB_THREADS") && err.contains("at least 1"),
            "unexpected error: {err}"
        );

        std::env::set_var("DUCKDB_THREADS", "4");
        std::env::set_var("FLIGHT_MAX_CONCURRENT_DOPUT", "0");
        let err = Settings::from_env().unwrap_err();
        assert!(
            err.contains("FLIGHT_MAX_CONCURRENT_DOPUT") && err.contains("at least 1"),
            "unexpected error: {err}"
        );
    }

    #[test]
    #[serial]
    fn config_rejects_a_relative_spill_directory() {
        let _env = pool_test_env();
        std::env::set_var("DUCKDB_MEMORY_LIMIT", TEST_MEMORY_LIMIT);
        std::env::set_var("DUCKDB_TEMP_DIRECTORY", "relative/spill");
        let err = Settings::from_env().unwrap_err();
        assert!(
            err.contains("DUCKDB_TEMP_DIRECTORY") && err.contains("absolute"),
            "unexpected error: {err}"
        );
    }

    /// A share below the floor means the pool is too big for the host, and
    /// granting the floor anyway would over-commit the ceiling it was divided
    /// from. The size is computed from whatever this machine detects, so the
    /// test fails for the stated reason on a laptop and on the deploy host alike.
    #[test]
    #[serial]
    fn a_pool_too_large_for_the_detected_ceiling_fails_at_boot() {
        let _env = pool_test_env();
        // From the constants that do the dividing, so the test cannot drift from
        // them, and from this machine's own ceiling so it holds on any host.
        let ceiling_mib = super::resources::detect_memory_ceiling().bytes / (1024 * 1024);
        let size =
            (ceiling_mib / super::config::CEILING_DIVISOR / super::config::MIN_MEMORY_LIMIT_MIB)
                + 1;
        std::env::set_var("DUCKLAKE_POOL_SIZE", size.to_string());

        let err = Settings::from_env().unwrap_err();

        assert!(
            err.contains("DUCKLAKE_POOL_SIZE") && err.contains("DUCKDB_MEMORY_LIMIT"),
            "the error names both ways out: {err}"
        );
    }

    /// The spill root needs nothing beyond PATH_SCRATCH, which boot already
    /// requires (`config_rejects_missing_path_scratch`).
    #[test]
    #[serial]
    fn the_spill_root_does_not_need_home() {
        let _env = pool_test_env();
        std::env::set_var("DUCKDB_MEMORY_LIMIT", TEST_MEMORY_LIMIT);
        let home = EnvSnapshot::capture(&["HOME"]);
        std::env::remove_var("HOME");
        let cfg = Settings::from_env().expect("from_env with HOME unset");
        drop(home);
        assert_eq!(
            cfg.pool.temp_directory,
            std::path::PathBuf::from(tmp_abs("qiita-test-scratch")).join("duckdb-spill")
        );
    }

    #[test]
    #[serial]
    fn config_rejects_missing_flight_public_key() {
        let _snapshot =
            EnvSnapshot::capture(&["FLIGHT_TICKET_PUBLIC_KEY", "DUCKLAKE_CATALOG_CONNSTR"]);
        std::env::remove_var("FLIGHT_TICKET_PUBLIC_KEY");
        std::env::set_var("DUCKLAKE_CATALOG_CONNSTR", "dbname=test");
        let err = Settings::from_env().unwrap_err();
        assert!(
            err.contains("FLIGHT_TICKET_PUBLIC_KEY"),
            "error should mention FLIGHT_TICKET_PUBLIC_KEY: {err}"
        );
    }

    #[test]
    #[serial]
    fn config_rejects_missing_path_scratch() {
        let _snapshot = EnvSnapshot::capture(&[
            "FLIGHT_TICKET_PUBLIC_KEY",
            "DUCKLAKE_CATALOG_CONNSTR",
            "PATH_SCRATCH",
            "PATH_PERSISTENT",
        ]);
        let pubkey = "6kpsY+KcUgq+9VB7Ey7F+ZVHdq6+vnuSQh7qaRRG0iw=".to_string();
        std::env::set_var("FLIGHT_TICKET_PUBLIC_KEY", &pubkey);
        std::env::set_var("DUCKLAKE_CATALOG_CONNSTR", "dbname=test");
        std::env::set_var("PATH_PERSISTENT", tmp_abs("qiita-test-persistent"));
        std::env::remove_var("PATH_SCRATCH");
        let err = Settings::from_env().unwrap_err();
        assert!(
            err.contains("PATH_SCRATCH"),
            "error should mention PATH_SCRATCH: {err}"
        );
    }

    #[test]
    #[serial]
    fn config_rejects_missing_path_persistent() {
        // PATH_PERSISTENT is the system-of-record store — a missing one must
        // fail fast, not fall back to a tmp-rooted default that loses durable
        // lake data on reboot.
        let _snapshot = EnvSnapshot::capture(&[
            "FLIGHT_TICKET_PUBLIC_KEY",
            "DUCKLAKE_CATALOG_CONNSTR",
            "PATH_SCRATCH",
            "PATH_PERSISTENT",
        ]);
        let pubkey = "6kpsY+KcUgq+9VB7Ey7F+ZVHdq6+vnuSQh7qaRRG0iw=".to_string();
        std::env::set_var("FLIGHT_TICKET_PUBLIC_KEY", &pubkey);
        std::env::set_var("DUCKLAKE_CATALOG_CONNSTR", "dbname=test");
        std::env::set_var("PATH_SCRATCH", tmp_abs("qiita-test-scratch"));
        std::env::remove_var("PATH_PERSISTENT");
        let err = Settings::from_env().unwrap_err();
        assert!(
            err.contains("PATH_PERSISTENT"),
            "error should mention PATH_PERSISTENT: {err}"
        );
    }

    #[test]
    #[serial]
    fn config_rejects_relative_path_persistent() {
        let _snapshot = EnvSnapshot::capture(&[
            "FLIGHT_TICKET_PUBLIC_KEY",
            "DUCKLAKE_CATALOG_CONNSTR",
            "PATH_SCRATCH",
            "PATH_PERSISTENT",
        ]);
        let pubkey = "6kpsY+KcUgq+9VB7Ey7F+ZVHdq6+vnuSQh7qaRRG0iw=".to_string();
        std::env::set_var("FLIGHT_TICKET_PUBLIC_KEY", &pubkey);
        std::env::set_var("DUCKLAKE_CATALOG_CONNSTR", "dbname=test");
        std::env::set_var("PATH_SCRATCH", tmp_abs("qiita-test-scratch"));
        std::env::set_var("PATH_PERSISTENT", "relative/persistent");
        let err = Settings::from_env().unwrap_err();
        assert!(
            err.contains("must be an absolute path"),
            "error should mention absolute path requirement: {err}"
        );
    }

    #[test]
    #[serial]
    fn config_rejects_relative_path_scratch() {
        let _snapshot = EnvSnapshot::capture(&[
            "FLIGHT_TICKET_PUBLIC_KEY",
            "DUCKLAKE_CATALOG_CONNSTR",
            "PATH_SCRATCH",
            "PATH_PERSISTENT",
        ]);
        let pubkey = "6kpsY+KcUgq+9VB7Ey7F+ZVHdq6+vnuSQh7qaRRG0iw=".to_string();
        std::env::set_var("FLIGHT_TICKET_PUBLIC_KEY", &pubkey);
        std::env::set_var("DUCKLAKE_CATALOG_CONNSTR", "dbname=test");
        std::env::set_var("PATH_PERSISTENT", tmp_abs("qiita-test-persistent"));
        std::env::set_var("PATH_SCRATCH", "relative/scratch");
        let err = Settings::from_env().unwrap_err();
        assert!(
            err.contains("must be an absolute path"),
            "error should mention absolute path requirement: {err}"
        );
    }

    #[test]
    #[serial]
    fn miint_extension_smoke() {
        // The data plane uses a SINGLE deploy-staged miint build: production
        // LOADs it from MIINT_EXTENSION_DIRECTORY and never installs its own
        // (two drifting builds is the nightmare we avoid). `cargo test` has no
        // deploy stage, so this test plays both roles against a stable per-suite
        // dir: it STAGES once (the deploy's INSTALL from the team mirror), then
        // opens a FRESH connection through the production helpers
        // (miint::miint_config + miint::load_miint) and LOAD-only's — exercising
        // exactly the path future DP code will use. The dir is stable (not a
        // fresh tempdir) so the install caches across runs, mirroring the Python
        // suites' setup_miint_test_env.
        let _snapshot = EnvSnapshot::capture(&["MIINT_EXTENSION_DIRECTORY"]);
        let ext_dir = std::env::temp_dir().join("qiita-data-plane-duckdb-ext");
        std::fs::create_dir_all(&ext_dir).expect("create staged extension dir");
        std::env::set_var("MIINT_EXTENSION_DIRECTORY", &ext_dir);

        // Stage (the deploy's role): INSTALL the team-mirror build into the dir.
        {
            let staging =
                Connection::open_in_memory_with_flags(miint::miint_config().expect("miint config"))
                    .expect("open staging connection");
            staging
                .execute_batch(&format!("INSTALL miint FROM '{}';", miint::miint_repo()))
                .expect("stage miint from the team mirror");
        }

        // Runtime (production's role): open through the same config and LOAD-only.
        let conn =
            Connection::open_in_memory_with_flags(miint::miint_config().expect("miint config"))
                .expect("open runtime connection");
        miint::load_miint(&conn).expect("LOAD the staged miint");
        let mut stmt = conn
            .prepare("SELECT count(*) FROM miint_versions()")
            .expect("failed to prepare miint_versions() query");
        let count: i64 = stmt
            .query_row([], |row| row.get(0))
            .expect("miint_versions() query failed");
        assert!(
            count > 0,
            "miint_versions() returned no rows — extension may be broken"
        );
    }
}
