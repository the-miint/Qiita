use tonic::transport::Server;
use tonic_health::ServingStatus;

#[allow(dead_code)] // Used by Flight service in Phase 8; currently tested only.
mod auth;
mod config;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = config::Settings::from_env().map_err(|e| {
        eprintln!("Configuration error: {e}");
        e
    })?;
    if cfg.jwks_url.is_none() {
        eprintln!("Warning: JWKS_URL not set — JWT verification is not active");
    }

    let (health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter
        .set_service_status("", ServingStatus::Serving)
        .await;

    println!("qiita-data-plane listening on {}", cfg.listen_addr);

    Server::builder()
        .add_service(health_service)
        .serve(cfg.listen_addr)
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::config::Settings;
    use base64::Engine;
    use duckdb::Connection;

    #[test]
    fn config_with_valid_hmac() {
        std::env::remove_var("LISTEN_ADDR");
        // Provide a valid base64-encoded 32-byte secret
        let secret = base64::engine::general_purpose::STANDARD.encode(vec![0xABu8; 32]);
        std::env::set_var("HMAC_SECRET_KEY", &secret);
        let cfg = Settings::from_env().expect("Settings::from_env() failed with valid config");
        assert_eq!(cfg.listen_addr.to_string(), "0.0.0.0:50051");
        assert_eq!(cfg.hmac_secret_key.len(), 32);
        std::env::remove_var("HMAC_SECRET_KEY");
    }

    #[test]
    fn config_rejects_missing_hmac() {
        std::env::remove_var("HMAC_SECRET_KEY");
        let err = Settings::from_env().unwrap_err();
        assert!(
            err.contains("HMAC_SECRET_KEY"),
            "error should mention HMAC_SECRET_KEY: {err}"
        );
    }

    #[test]
    fn miint_extension_smoke() {
        // "miint" is the DuckDB community extension for minimizer index tables.
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
