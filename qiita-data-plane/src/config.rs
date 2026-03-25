use std::net::SocketAddr;

/// Runtime configuration for qiita-data-plane.
/// All fields read from environment variables with sensible defaults for local dev.
pub struct Settings {
    /// Address to bind the gRPC server (e.g. "0.0.0.0:50051")
    pub listen_addr: SocketAddr,
    /// HMAC-SHA256 key for Flight ticket signing/verification (base64-encoded).
    /// TODO: make required before the first DoGet/DoPut is implemented.
    pub hmac_secret_key: Option<String>,
    /// JWKS endpoint URL for JWT public key retrieval and verification.
    /// TODO: make required before any authenticated endpoint is added.
    pub jwks_url: Option<String>,
}

impl Settings {
    pub fn from_env() -> Result<Self, String> {
        let listen_addr = std::env::var("LISTEN_ADDR")
            .unwrap_or_else(|_| "0.0.0.0:50051".to_string())
            .parse::<SocketAddr>()
            .map_err(|e| format!("invalid LISTEN_ADDR: {e}"))?;

        Ok(Self {
            listen_addr,
            hmac_secret_key: std::env::var("HMAC_SECRET_KEY").ok(),
            jwks_url: std::env::var("JWKS_URL").ok(),
        })
    }
}
