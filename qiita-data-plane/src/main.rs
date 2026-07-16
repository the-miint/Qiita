use arrow_flight::flight_service_server::FlightServiceServer;
use duckdb::Connection;
use tonic::transport::Server;
use tonic_health::ServingStatus;

mod auth;
mod config;
mod ducklake;
mod flight_service;
mod miint;

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
    // every per-request attach: a per-attach write races on ducklake_metadata
    // under concurrent Flight load and fails with SQLSTATE 40001. See
    // set_catalog_options.
    ducklake::set_catalog_options(&setup_conn)?;
    ducklake::ensure_reference_tables(&setup_conn)?;
    ducklake::ensure_read_tables(&setup_conn)?;
    ducklake::ensure_alignment_tables(&setup_conn)?;
    ducklake::ensure_assembly_tables(&setup_conn)?;
    drop(setup_conn);

    // Build Flight service — each request opens its own DuckDB connection
    let flight_svc = flight_service::QiitaFlightService::new(
        cfg.flight_public_key,
        cfg.ducklake_catalog_connstr,
        cfg.path_persistent_ducklake,
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
        ]);
        std::env::remove_var("LISTEN_ADDR");
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
