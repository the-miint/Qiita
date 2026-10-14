use base64::Engine;
use std::net::SocketAddr;
use std::path::PathBuf;

use crate::pool::PoolSettings;
use crate::resources::{self, MemoryCeiling};

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
    /// The `export_read` DoAction writes a prep_sample's reads into a control-plane
    /// ticket workspace under `{PATH_SCRATCH}/ticket/...`, so the handler
    /// validates the requested destination resolves under this root.
    pub path_scratch: PathBuf,
    /// Size of the DuckLake connection pool and the resource settings each of
    /// its instances carries. The size is also the in-flight request bound.
    pub pool: PoolSettings,
    /// The ceiling the pool's memory share was divided from, and which detector
    /// answered. Carried so boot can log both — a number alone does not say
    /// whether it came from a cgroup or from the fallback.
    pub memory_ceiling: MemoryCeiling,
    /// CPU allowance the per-instance `threads` was divided from.
    pub thread_budget: usize,
}

/// Instances one data plane may hold, and so requests it may serve at once.
/// Each attached instance holds one lake-Postgres backend for as long as it
/// lives (measured: 8 attached connections held 8 idle backends), against a
/// `max_connections` of 100 with 3 superuser-reserved that the control plane
/// draws on as well. Raise it with `DUCKLAKE_POOL_SIZE` where that budget
/// allows; every instance on every host is spent against the same 97.
const DEFAULT_POOL_SIZE: usize = 8;

/// The pool divides half the detected ceiling, not all of it. `memory_limit`
/// bounds what DuckDB accounts for rather than what the process holds:
/// measured on the deploy host, one request under a 3.7 GiB limit peaked at
/// 5,021 MiB RSS and finished in the same 100.5 s as the same request under a
/// 29.8 GiB limit. The unspent half covers what that accounting does not see —
/// the Arrow batches each stream holds in flight — and the host's other work.
const CEILING_DIVISOR: u64 = 2;

/// Floor for a derived per-instance `memory_limit`, so a small ceiling divided
/// by a large pool cannot round down to nothing.
const MIN_MEMORY_LIMIT_MIB: u64 = 256;

/// Per-instance ceiling on spill. DuckDB's own default is 90% of the
/// filesystem holding `temp_directory` — which every instance would claim in
/// full — so an explicit value is required rather than optional. This one is a
/// bound, not a sizing: no spill size for a real request has been measured, and
/// the measured behaviour when the cap is reached is a clean error naming the
/// setting, with nothing left behind.
const DEFAULT_MAX_TEMP_DIRECTORY_SIZE: &str = "4GiB";

/// Leaf name of the derived spill directory, under the per-instance `HOME` the
/// systemd unit sets (its `StateDirectory`, on local disk). Spill must not land
/// on the shared scratch filesystem that every compute node mounts, and two
/// instances must not share one directory.
const DERIVED_TEMP_DIRECTORY_LEAF: &str = "duckdb-spill";

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
        // Reject a small-order (weak) key at boot. `verify_strict` would reject
        // every ticket signed against it anyway, but that surfaces as a confusing
        // per-request "invalid signature"; fail loudly here with a clear cause.
        if flight_public_key.is_weak() {
            return Err("FLIGHT_TICKET_PUBLIC_KEY is a weak (small-order) Ed25519 key".to_string());
        }

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

        // --- what one request may spend -------------------------------------
        // Absent these, every DuckDB database this process opens sizes itself
        // from the process's own cgroup — so each concurrent request
        // authorises itself against the whole host, and N of them authorise N
        // times the host. Each env var is named after the DuckDB setting it
        // feeds; the defaults are derived here because this is the site that
        // divides the ceiling.
        let memory_ceiling = resources::detect_memory_ceiling();
        let thread_budget = resources::detect_thread_budget();

        let size = parse_positive_usize("DUCKLAKE_POOL_SIZE", DEFAULT_POOL_SIZE)?;

        // The CPU allowance divided by the in-flight bound, so a saturated
        // pool asks for the budget once rather than once per request. Measured
        // on the deploy host (48 cores), one masked-read request fitted
        // `25.3 s + 611.8 s / threads` over 1…48 threads while peak RSS rose
        // about 509 MiB per thread — an upper bound, since the probe's own
        // expression inflated both axes — so this is the setting that prices
        // concurrency.
        let threads = match std::env::var("DUCKDB_THREADS") {
            Ok(raw) => parse_positive_u32("DUCKDB_THREADS", &raw)?,
            Err(_) => std::cmp::max(1, (thread_budget / size) as u32),
        };

        let memory_limit = match std::env::var("DUCKDB_MEMORY_LIMIT") {
            Ok(raw) if !raw.trim().is_empty() => raw,
            _ => {
                let share_mib =
                    memory_ceiling.bytes / CEILING_DIVISOR / size as u64 / (1024 * 1024);
                format!("{}MiB", std::cmp::max(MIN_MEMORY_LIMIT_MIB, share_mib))
            }
        };

        let temp_directory = match std::env::var("DUCKDB_TEMP_DIRECTORY") {
            Ok(raw) => {
                let path = PathBuf::from(&raw);
                if !path.is_absolute() {
                    return Err(format!(
                        "DUCKDB_TEMP_DIRECTORY must be an absolute path, got {raw:?}"
                    ));
                }
                path
            }
            Err(_) => {
                let home = std::env::var("HOME").map_err(|_| {
                    "DUCKDB_TEMP_DIRECTORY is not set and neither is HOME, so there is                      nowhere to derive a spill directory from — set DUCKDB_TEMP_DIRECTORY                      to an absolute path on local disk"
                        .to_string()
                })?;
                let home = PathBuf::from(home);
                if !home.is_absolute() {
                    return Err(format!(
                        "HOME must be absolute to derive DUCKDB_TEMP_DIRECTORY from it, got {}",
                        home.display()
                    ));
                }
                home.join(DERIVED_TEMP_DIRECTORY_LEAF)
            }
        };

        let max_temp_directory_size = match std::env::var("DUCKDB_MAX_TEMP_DIRECTORY_SIZE") {
            Ok(raw) if !raw.trim().is_empty() => raw,
            _ => DEFAULT_MAX_TEMP_DIRECTORY_SIZE.to_string(),
        };

        Ok(Self {
            listen_addr,
            flight_public_key,
            ducklake_catalog_connstr,
            path_persistent_ducklake,
            path_scratch_staging,
            path_scratch,
            pool: PoolSettings {
                size,
                threads,
                memory_limit,
                temp_directory,
                max_temp_directory_size,
            },
            memory_ceiling,
            thread_budget,
        })
    }
}

fn parse_positive_usize(var: &str, default: usize) -> Result<usize, String> {
    match std::env::var(var) {
        Ok(raw) => {
            let value: usize = raw
                .trim()
                .parse()
                .map_err(|e| format!("{var} must be a positive integer, got {raw:?}: {e}"))?;
            if value == 0 {
                return Err(format!("{var} must be at least 1, got 0"));
            }
            Ok(value)
        }
        Err(_) => Ok(default),
    }
}

fn parse_positive_u32(var: &str, raw: &str) -> Result<u32, String> {
    let value: u32 = raw
        .trim()
        .parse()
        .map_err(|e| format!("{var} must be a positive integer, got {raw:?}: {e}"))?;
    if value == 0 {
        return Err(format!("{var} must be at least 1, got 0"));
    }
    Ok(value)
}
