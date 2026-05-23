use arrow_flight::flight_service_server::FlightServiceServer;
use duckdb::Connection;
use tonic::transport::Server;
use tonic_health::ServingStatus;

mod auth;
mod config;
mod ducklake;
mod flight_service;

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
        &cfg.ducklake_data_path,
    )?;
    ducklake::ensure_reference_tables(&setup_conn)?;
    drop(setup_conn);

    // Build Flight service — each request opens its own DuckDB connection
    let flight_svc = flight_service::QiitaFlightService::new(
        cfg.hmac_secret_key,
        cfg.ducklake_catalog_connstr,
        cfg.ducklake_data_path,
        cfg.upload_staging_root,
    );

    let (health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter
        .set_service_status("", ServingStatus::Serving)
        .await;

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

    #[test]
    #[serial]
    fn config_with_valid_env() {
        let _snapshot =
            EnvSnapshot::capture(&["LISTEN_ADDR", "HMAC_SECRET_KEY", "DUCKLAKE_CATALOG_CONNSTR"]);
        std::env::remove_var("LISTEN_ADDR");
        let secret = base64::engine::general_purpose::STANDARD.encode(vec![0xABu8; 32]);
        std::env::set_var("HMAC_SECRET_KEY", &secret);
        std::env::set_var("DUCKLAKE_CATALOG_CONNSTR", "dbname=test host=localhost");
        let cfg = Settings::from_env().expect("Settings::from_env() failed with valid config");
        assert_eq!(cfg.listen_addr.to_string(), "0.0.0.0:50051");
        assert_eq!(cfg.hmac_secret_key.len(), 32);
        assert_eq!(cfg.ducklake_catalog_connstr, "dbname=test host=localhost");
    }

    #[test]
    #[serial]
    fn config_rejects_missing_hmac() {
        let _snapshot = EnvSnapshot::capture(&["HMAC_SECRET_KEY", "DUCKLAKE_CATALOG_CONNSTR"]);
        std::env::remove_var("HMAC_SECRET_KEY");
        std::env::set_var("DUCKLAKE_CATALOG_CONNSTR", "dbname=test");
        let err = Settings::from_env().unwrap_err();
        assert!(
            err.contains("HMAC_SECRET_KEY"),
            "error should mention HMAC_SECRET_KEY: {err}"
        );
    }

    #[test]
    fn miint_extension_smoke() {
        let conn = Connection::open_in_memory().expect("open in-memory DuckDB");
        conn.execute_batch("INSTALL miint FROM community; LOAD miint;")
            .expect("failed to install/load miint extension");
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
