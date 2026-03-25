/// Runtime configuration for qiita-data-plane.
/// All fields read from environment variables with sensible defaults for local dev.
pub struct Settings {
    /// Address to bind the gRPC server (e.g. "0.0.0.0:50051")
    pub listen_addr: String,
}

impl Settings {
    pub fn from_env() -> Self {
        Self {
            listen_addr: std::env::var("LISTEN_ADDR")
                .unwrap_or_else(|_| "0.0.0.0:50051".to_string()),
        }
    }
}
