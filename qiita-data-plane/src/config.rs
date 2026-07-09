use base64::Engine;
use std::net::SocketAddr;
use std::path::PathBuf;

/// Runtime configuration for qiita-data-plane.
/// All fields read from environment variables.
#[derive(Debug)]
pub struct Settings {
    /// Address to bind the gRPC server (e.g. "0.0.0.0:50051")
    pub listen_addr: SocketAddr,
    /// Ed25519 PUBLIC key for Flight ticket verification (decoded from a base64
    /// env var). The control plane holds the matching private seed and signs;
    /// this (publicly reachable) data plane only verifies, so it can never forge
    /// a ticket.
    pub flight_public_key: ed25519_dalek::VerifyingKey,
    /// DuckLake catalog connection string (libpq format).
    /// E.g., "dbname=qiita_ducklake host=localhost port=5432 user=qiita password=qiita"
    pub ducklake_catalog_connstr: String,
    /// Directory where DuckLake stores Parquet data files (system-of-record
    /// state). Derived as `PATH_PERSISTENT/ducklake`; `PATH_PERSISTENT` is a
    /// required, absolute env var (no dev fallback — a missing one must fail
    /// fast rather than silently rooting durable data under `/tmp`).
    pub path_persistent_ducklake: String,
    /// Root directory under which DoPut writes staged Parquet uploads.
    /// Derived as `PATH_SCRATCH/staging`; each upload lands at
    /// `{root}/uploads/{upload_idx}/upload.parquet`. Must match the
    /// control plane's PATH_SCRATCH/staging — set PATH_SCRATCH to the same
    /// value in both env files.
    pub path_scratch_staging: PathBuf,
    /// The `PATH_SCRATCH` base root itself (parent of `path_scratch_staging`).
    /// The `export_read` DoAction writes a sample's reads into a control-plane
    /// ticket workspace under `{PATH_SCRATCH}/ticket/...`, so the handler
    /// validates the requested destination resolves under this root.
    pub path_scratch: PathBuf,
}

impl Settings {
    pub fn from_env() -> Result<Self, String> {
        let listen_addr = std::env::var("LISTEN_ADDR")
            .unwrap_or_else(|_| "0.0.0.0:50051".to_string())
            .parse::<SocketAddr>()
            .map_err(|e| format!("invalid LISTEN_ADDR: {e}"))?;

        let pubkey_b64 = std::env::var("FLIGHT_TICKET_PUBLIC_KEY")
            .map_err(|_| "FLIGHT_TICKET_PUBLIC_KEY is required but not set".to_string())?;
        let pubkey_bytes = base64::engine::general_purpose::STANDARD
            .decode(&pubkey_b64)
            .map_err(|e| format!("FLIGHT_TICKET_PUBLIC_KEY is not valid base64: {e}"))?;
        let pubkey_array: [u8; 32] = pubkey_bytes.as_slice().try_into().map_err(|_| {
            format!(
                "FLIGHT_TICKET_PUBLIC_KEY must decode to exactly 32 bytes \
                 (an Ed25519 public key), got {}",
                pubkey_bytes.len()
            )
        })?;
        let flight_public_key =
            ed25519_dalek::VerifyingKey::from_bytes(&pubkey_array).map_err(|e| {
                format!("FLIGHT_TICKET_PUBLIC_KEY is not a valid Ed25519 public key: {e}")
            })?;

        let ducklake_catalog_connstr = std::env::var("DUCKLAKE_CATALOG_CONNSTR")
            .map_err(|_| "DUCKLAKE_CATALOG_CONNSTR is required but not set".to_string())?;
        // DuckLake parquet lives at PATH_PERSISTENT/ducklake — this is the
        // system-of-record store, so PATH_PERSISTENT is required + absolute
        // (same fail-fast posture as FLIGHT_TICKET_PUBLIC_KEY / DUCKLAKE_CATALOG_CONNSTR /
        // PATH_SCRATCH). It previously fell back to $TMPDIR/qiita, which meant a
        // forgotten env var in production silently landed durable lake data in
        // /tmp — lost on reboot, never backed up. Fail loudly instead.
        let path_persistent_raw = std::env::var("PATH_PERSISTENT")
            .map_err(|_| "PATH_PERSISTENT is required but not set".to_string())?;
        if !std::path::Path::new(&path_persistent_raw).is_absolute() {
            return Err(format!(
                "PATH_PERSISTENT must be an absolute path, got {path_persistent_raw:?}"
            ));
        }
        let path_persistent_ducklake = format!("{path_persistent_raw}/ducklake");

        // DoPut uploads stage under PATH_SCRATCH/staging. PATH_SCRATCH is
        // required + absolute (same posture as the control plane); the
        // /staging subdir must match the CP's PATH_SCRATCH/staging.
        let path_scratch_raw = std::env::var("PATH_SCRATCH")
            .map_err(|_| "PATH_SCRATCH is required but not set".to_string())?;
        let path_scratch: PathBuf = path_scratch_raw.clone().into();
        if !path_scratch.is_absolute() {
            return Err(format!(
                "PATH_SCRATCH must be an absolute path, got {path_scratch_raw:?}"
            ));
        }
        let path_scratch_staging: PathBuf = path_scratch.join("staging");

        Ok(Self {
            listen_addr,
            flight_public_key,
            ducklake_catalog_connstr,
            path_persistent_ducklake,
            path_scratch_staging,
            path_scratch,
        })
    }
}
