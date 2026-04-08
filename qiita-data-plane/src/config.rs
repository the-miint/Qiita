use base64::Engine;
use std::net::SocketAddr;

/// Runtime configuration for qiita-data-plane.
/// All fields read from environment variables.
#[derive(Debug)]
pub struct Settings {
    /// Address to bind the gRPC server (e.g. "0.0.0.0:50051")
    pub listen_addr: SocketAddr,
    /// HMAC-SHA256 key for Flight ticket verification (decoded from base64 env var).
    /// Used by the Flight service in Phase 8; currently validated at startup only.
    #[allow(dead_code)]
    pub hmac_secret_key: Vec<u8>,
    /// JWKS endpoint URL for JWT public key retrieval and verification.
    /// TODO: make required before any authenticated endpoint is added.
    pub jwks_url: Option<String>,
    /// DuckLake catalog connection string (libpq format).
    /// E.g., "dbname=qiita_ducklake host=localhost port=5432 user=qiita password=qiita"
    #[allow(dead_code)]
    pub ducklake_catalog_connstr: String,
    /// Directory where DuckLake stores Parquet data files.
    #[allow(dead_code)]
    pub ducklake_data_path: String,
}

impl Settings {
    pub fn from_env() -> Result<Self, String> {
        let listen_addr = std::env::var("LISTEN_ADDR")
            .unwrap_or_else(|_| "0.0.0.0:50051".to_string())
            .parse::<SocketAddr>()
            .map_err(|e| format!("invalid LISTEN_ADDR: {e}"))?;

        let hmac_b64 = std::env::var("HMAC_SECRET_KEY")
            .map_err(|_| "HMAC_SECRET_KEY is required but not set".to_string())?;
        let hmac_secret_key = base64::engine::general_purpose::STANDARD
            .decode(&hmac_b64)
            .map_err(|e| format!("HMAC_SECRET_KEY is not valid base64: {e}"))?;
        if hmac_secret_key.len() < 16 {
            return Err(format!(
                "HMAC_SECRET_KEY must decode to at least 16 bytes, got {}",
                hmac_secret_key.len()
            ));
        }

        let ducklake_catalog_connstr = std::env::var("DUCKLAKE_CATALOG_CONNSTR")
            .map_err(|_| "DUCKLAKE_CATALOG_CONNSTR is required but not set".to_string())?;
        let ducklake_data_path = std::env::var("DUCKLAKE_DATA_PATH").unwrap_or_else(|_| {
            let base = std::env::var("TMPDIR").unwrap_or_else(|_| "/tmp".to_string());
            format!("{base}/qiita/ducklake")
        });

        Ok(Self {
            listen_addr,
            hmac_secret_key,
            jwks_url: std::env::var("JWKS_URL").ok(),
            ducklake_catalog_connstr,
            ducklake_data_path,
        })
    }
}
