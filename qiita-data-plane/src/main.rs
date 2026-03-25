use tonic::transport::Server;
use tonic_health::ServingStatus;

mod config;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = config::Settings::from_env().map_err(|e| {
        eprintln!("Configuration error: {e}");
        e
    })?;
    if cfg.hmac_secret_key.is_none() {
        eprintln!("Warning: HMAC_SECRET_KEY not set — Flight ticket signing is not active");
    }
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
    use duckdb::Connection;

    #[test]
    fn config_defaults() {
        // Ensure Settings::from_env() produces expected defaults when no env vars are set.
        // Unset LISTEN_ADDR to guarantee default behaviour.
        std::env::remove_var("LISTEN_ADDR");
        let cfg = Settings::from_env().expect("Settings::from_env() failed with valid defaults");
        assert_eq!(cfg.listen_addr.to_string(), "0.0.0.0:50051");
    }

    #[test]
    fn miint_extension_smoke() {
        // "miint" is the DuckDB community extension for minimizer index tables.
        let conn = Connection::open_in_memory().expect("open in-memory DuckDB");
        conn.execute_batch("INSTALL miint FROM community; LOAD miint;")
            .expect("failed to install/load miint extension");
        // Verify the extension is functional: miint_versions() must return at least one row.
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
