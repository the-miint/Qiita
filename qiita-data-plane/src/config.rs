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
    /// its connections carries. The size is also the in-flight request bound.
    pub pool: PoolSettings,
    /// The ceiling the pool's memory share was divided from, and which detector
    /// answered. Carried so boot can log both — a number alone does not say
    /// whether it came from a cgroup or from the fallback.
    pub memory_ceiling: MemoryCeiling,
    /// CPU allowance each connection's `threads` was divided from.
    pub thread_budget: usize,
    /// DoPuts the data plane will serve at once. Not part of the pool: an upload
    /// borrows no connection, so the pool does not bound it.
    pub max_concurrent_doput: usize,
}

/// Connections one data plane may hold, and so requests it may serve at once.
/// Chosen, not measured: what is measured is the budget it is chosen against —
/// each attached connection holds one lake-Postgres backend for as long as it
/// lives (8 attached connections held 8 idle backends), against a
/// `max_connections` of 100 with 3 superuser-reserved that the control plane
/// draws on as well. Eight leaves that budget room for other data-plane
/// processes and for the control plane; raise it with `DUCKLAKE_POOL_SIZE` where
/// the budget allows, counting every process on every host against the same 97.
const DEFAULT_POOL_SIZE: usize = 8;

/// The pool divides half the detected ceiling, not all of it. Half is a choice;
/// what is measured is that it has to be less than all of it, because
/// `memory_limit` bounds what DuckDB accounts for rather than what the process
/// holds — on the deploy host one request under a 3.7 GiB limit peaked at
/// 5,021 MiB RSS, an overshoot of 1.33×, and finished in the same 100.5 s as the
/// same request under a 29.8 GiB limit. Half leaves room for that overshoot on
/// every connection at once, plus what the accounting never sees (the Arrow
/// batches each stream holds in flight) and the host's other work.
pub(crate) const CEILING_DIVISOR: u64 = 2;

/// Smallest per-connection `memory_limit` worth deriving — the point below which
/// a request shape the data plane serves cannot run at all, rather than a size
/// any particular request needs.
///
/// Measured on the four shapes this process runs (the `read_masked` stream, the
/// export's `COPY … TO` Parquet, the mask-metrics aggregate, the pool-reads
/// DELETE) at a 209 MiB and a 2,086 MiB payload: the export fails at 256 MiB for
/// the larger payload and at 128 MiB for the smaller, with a single
/// `failed to allocate data of size 128.0 MiB` — its Parquet row-group buffer,
/// which a spill directory does not rescue. Ten times the payload moved each
/// shape that has a floor up one step, the streamed read 128 → 256 MiB and the
/// export 256 → 512 MiB, so no fixed value serves every request; this is the
/// floor, and what a request actually gets comes from dividing a real ceiling.
///
/// A share below it means the pool is too big for the host rather than that the
/// share should be rounded up — granting the floor anyway would hand out
/// `size × this` from a ceiling smaller than that. Boot fails instead, naming
/// both ways out.
pub(crate) const MIN_MEMORY_LIMIT_MIB: u64 = 512;

/// Per-connection ceiling on spill. DuckDB's own default is 90% of the
/// filesystem holding `temp_directory` — which every connection would claim in
/// full — so an explicit value is required rather than optional.
///
/// It is a backstop, and measurement is why: none of the four shapes this process
/// runs spills anything at all when it completes, at either payload tested, and
/// the largest spill seen anywhere was 58 MiB written by an export that then
/// failed its allocation. So this does not size expected spill; it bounds a shape
/// nobody has measured. At the default pool size that is 8 × 4 GiB against the
/// deploy host's 378 G of free root, and the measured behaviour when a connection
/// reaches it is a clean error naming the setting, with nothing left behind.
const DEFAULT_MAX_TEMP_DIRECTORY_SIZE: &str = "4GiB";

/// Uploads one data plane serves at once. Chosen, not measured: what is measured
/// is the cost curve it bounds — against a real data plane, peak RSS was 165 MiB
/// for one upload and 1,103 MiB for sixteen at 8 MiB batches, 3,014 MiB for
/// sixteen at 64 MiB, and 2,787 MiB for one and 8,834 MiB for four at 896 MiB.
/// So the cost is linear in concurrency and tracks the batch size, which makes
/// this a bound on uploads and not on memory.
///
/// What a payload may be is bounded elsewhere and far higher: one client
/// RecordBatch, which `main`'s `FLIGHT_MAX_DECODING_BYTES` admits at up to 1 GiB
/// and `flight_service::DOPUT_WRITER_CHANNEL_DEPTH` lets that many again queue
/// for behind. Today's uploaders are the two user-facing CLIs, which send
/// `qiita_common.chunking.CHUNK_ROW_GROUP_SIZE` rows per batch — stated there as
/// ~1 GB on dense data — so eight of them is ~18 GiB of decode buffers by the
/// measured linearity. That is a share of the deploy host and the whole of the
/// 8 GiB fallback ceiling, and unlike the pool's memory share it is not derived
/// from a ceiling and cannot fail boot: a host smaller than this one needs the
/// value lowered by hand. Bounding bytes rather than count is the mechanism that
/// would make that automatic, and nothing here measures what it should be.
///
/// Eight matches the connection pool's default because it is the same question
/// asked of a different resource, not because the two share anything — an upload
/// takes no connection, and this permit is not one.
const DEFAULT_MAX_CONCURRENT_DOPUT: usize = 8;

/// Leaf name of the derived spill root, under `PATH_SCRATCH`. Spill is high IO, so
/// it belongs on the scratch filesystem. This is a root, not a spill directory;
/// why is `pool::Member`.
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

        let size = parse_positive::<usize>("DUCKLAKE_POOL_SIZE")?.unwrap_or(DEFAULT_POOL_SIZE);

        // The CPU allowance divided by the in-flight bound, so a saturated
        // pool asks for the budget once rather than once per request. Measured
        // on the deploy host (48 cores), one masked-read request fitted
        // `25.3 s + 611.8 s / threads` over 1…48 threads while peak RSS rose
        // about 509 MiB per thread — an upper bound, since the probe's own
        // expression inflated both axes — so this is the setting that prices
        // concurrency.
        let threads = parse_positive::<u32>("DUCKDB_THREADS")?
            .unwrap_or_else(|| std::cmp::max(1, (thread_budget / size) as u32));

        let memory_limit = match env_value("DUCKDB_MEMORY_LIMIT") {
            Some(raw) => raw,
            None => {
                let share_mib =
                    memory_ceiling.bytes / CEILING_DIVISOR / size as u64 / (1024 * 1024);
                if share_mib < MIN_MEMORY_LIMIT_MIB {
                    return Err(format!(
                        concat!(
                            "a pool of {} would get {} MiB each from the {:.1} GiB ceiling ",
                            "detected via {} — below the {} MiB floor. Lower ",
                            "DUCKLAKE_POOL_SIZE, or set DUCKDB_MEMORY_LIMIT explicitly so ",
                            "the pool does not depend on what was free at boot.",
                        ),
                        size,
                        share_mib,
                        memory_ceiling.bytes as f64 / (1024.0 * 1024.0 * 1024.0),
                        memory_ceiling.source.as_str(),
                        MIN_MEMORY_LIMIT_MIB,
                    ));
                }
                format!("{share_mib}MiB")
            }
        };

        let temp_directory = match env_value("DUCKDB_TEMP_DIRECTORY") {
            Some(raw) => {
                let path = PathBuf::from(&raw);
                if !path.is_absolute() {
                    return Err(format!(
                        "DUCKDB_TEMP_DIRECTORY must be an absolute path, got {raw:?}"
                    ));
                }
                path
            }
            None => path_scratch.join(DERIVED_TEMP_DIRECTORY_LEAF),
        };

        // Only `> 0` is checked, where DUCKLAKE_POOL_SIZE also fails boot when the
        // ceiling cannot give each connection MIN_MEMORY_LIMIT_MIB. There is no
        // equivalent arithmetic here: DuckDB accounts the pool's memory against a
        // limit this process sets, and nothing accounts an upload's, so there is
        // no per-upload share to check a ceiling against.
        let max_concurrent_doput = parse_positive::<usize>("FLIGHT_MAX_CONCURRENT_DOPUT")?
            .unwrap_or(DEFAULT_MAX_CONCURRENT_DOPUT);

        let max_temp_directory_size = env_value("DUCKDB_MAX_TEMP_DIRECTORY_SIZE")
            .unwrap_or_else(|| DEFAULT_MAX_TEMP_DIRECTORY_SIZE.to_string());

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
            max_concurrent_doput,
        })
    }
}

/// A set, non-empty environment value. `None` covers unset, empty, and
/// non-unicode alike, so each caller states its own default beside the reasoning
/// for it.
fn env_value(var: &str) -> Option<String> {
    std::env::var(var).ok().filter(|v| !v.trim().is_empty())
}

/// `None` when the variable is unset or empty, so each caller states its own
/// default beside the reasoning for it.
fn parse_positive<T>(var: &str) -> Result<Option<T>, String>
where
    T: std::str::FromStr + PartialEq + From<u8>,
    T::Err: std::fmt::Display,
{
    // An empty value reads as unset, for every var in this group: an operator who
    // comments a value out but leaves the `KEY=` gets the derived default rather
    // than a unit that will not boot.
    let Some(raw) = env_value(var) else {
        return Ok(None);
    };
    let value: T = raw
        .trim()
        .parse()
        .map_err(|e| format!("{var} must be a positive integer, got {raw:?}: {e}"))?;
    if value == T::from(0u8) {
        return Err(format!("{var} must be at least 1, got 0"));
    }
    Ok(Some(value))
}
