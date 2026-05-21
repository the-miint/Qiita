use base64::Engine;
use std::net::SocketAddr;
use std::path::PathBuf;

/// Runtime configuration for qiita-data-plane.
/// All fields read from environment variables.
#[derive(Debug)]
pub struct Settings {
    /// Address to bind the gRPC server (e.g. "0.0.0.0:50051")
    pub listen_addr: SocketAddr,
    /// HMAC-SHA256 key for Flight ticket verification (decoded from base64 env var).
    pub hmac_secret_key: Vec<u8>,
    /// DuckLake catalog connection string (libpq format).
    /// E.g., "dbname=qiita_ducklake host=localhost port=5432 user=qiita password=qiita"
    pub ducklake_catalog_connstr: String,
    /// Directory where DuckLake stores Parquet data files.
    pub ducklake_data_path: String,
    /// Root directory under which DoPut writes staged Parquet uploads.
    /// Each upload lands at `{root}/uploads/{upload_idx}/upload.parquet`.
    pub upload_staging_root: PathBuf,
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
        let upload_staging_root = std::env::var("UPLOAD_STAGING_ROOT")
            .unwrap_or_else(|_| "/scratch/ephemeral/staging".to_string())
            .into();

        Ok(Self {
            listen_addr,
            hmac_secret_key,
            ducklake_catalog_connstr,
            ducklake_data_path,
            upload_staging_root,
        })
    }
}
