//! A bounded pool of attached DuckLake connections. Its size is the in-flight bound.
//!
//! Two measurements shape it. DuckDB's resource settings belong to a *database*,
//! not to a connection, so each pool member is its own in-memory DuckDB with its
//! own attach — N connections against one database would all share one set of
//! limits, which bounds nothing per request. And holding the
//! ATTACH is most of what a small request pays: a fresh connect + attach + query
//! measured 84–107 ms and about 16 catalog transactions, against 3.0–3.4 ms and
//! about 3.7 once the attach is reused.
//!
//! A checkout that finds nothing idle and nothing left to build refuses with
//! `RESOURCE_EXHAUSTED` instead of waiting, because waiting here is not shorter
//! than waiting at the edge: the `limit_conn` and `grpc_read_timeout` in
//! `deploy/nginx/qiita.conf` already admit and hold far more concurrent requests
//! per client address than this pool serves, so a queue inside the process would
//! fill with requests the edge is holding open anyway. No client retries the call
//! itself; what a refusal becomes instead differs per caller, and
//! `docs/architecture/flight.md` ("Concurrency bound") is where that lives.
//!
//! Connections are built as they are first needed rather than all at boot, because
//! each one costs a lake-Postgres backend for as long as it lives — the budget
//! `config::DEFAULT_POOL_SIZE` is chosen against. One is built at `open` so a bad
//! setting or an unwritable spill root fails the unit at boot rather than on first
//! request.

use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use duckdb::Connection;
use tonic::Status;

use crate::ducklake;

/// What every pooled connection is set to. Per connection, not per pool: DuckDB
/// applies each of these to the database it is set on, and each connection has
/// its own.
#[derive(Debug, Clone)]
pub struct PoolSettings {
    /// Connections the pool may hold, and so requests that may be in flight.
    pub size: usize,
    pub threads: u32,
    /// In DuckDB's own spelling, e.g. `23552MiB` — note DuckDB reads `GB` as
    /// decimal and `GiB` as binary.
    pub memory_limit: String,
    /// The root each connection's spill directory is created under. Spill is the
    /// difference between a request that fails and one that completes: measured,
    /// the same over-limit sort raised `OutOfMemoryException` after 2.41 s with no
    /// spill directory and completed in 22.62 s with one. Which filesystem, and
    /// why, is `config::DERIVED_TEMP_DIRECTORY_LEAF`; why it is a root rather than
    /// the directory itself is `Member`.
    pub temp_directory: PathBuf,
    /// Ceiling on that spill, per connection. Measured: it is enforced mid-query,
    /// names itself in the error (`This limit was set by the
    /// 'max_temp_directory_size' setting`) and leaves no files behind. Why it is
    /// set at all rather than left to DuckDB is
    /// `config::DEFAULT_MAX_TEMP_DIRECTORY_SIZE`.
    pub max_temp_directory_size: String,
}

impl PoolSettings {
    /// Checked by every constructor, over everything in these settings that
    /// comes from the environment: the size, which bounds what the pool can
    /// serve, and the three values that reach DuckDB inside quoted SQL literals
    /// — two in `apply_settings`, and the spill root as the start of each
    /// member's path in `settle`, which `new_spill_directory` checks whole — the
    /// same way the connection string and data path do in
    /// `ducklake::connect_ducklake`. What a valid value looks like is DuckDB's
    /// business, and `ducklake::validate_sql_literal` says what happens to one it
    /// rejects.
    fn validate(&self) -> Result<(), String> {
        if self.size == 0 {
            return Err("pool size must be at least 1 — a size of 0 serves no request".into());
        }
        ducklake::validate_sql_literal(&self.memory_limit, "DUCKDB_MEMORY_LIMIT")?;
        ducklake::validate_sql_literal(
            &self.max_temp_directory_size,
            "DUCKDB_MAX_TEMP_DIRECTORY_SIZE",
        )?;
        ducklake::validate_sql_literal(
            &self.temp_directory.display().to_string(),
            "DUCKDB_TEMP_DIRECTORY",
        )
    }
}

#[cfg(test)]
impl PoolSettings {
    /// Settings for a test pool: small, and spilling somewhere harmless.
    pub(crate) fn for_tests(size: usize, temp_directory: PathBuf) -> Self {
        Self {
            size,
            threads: 2,
            memory_limit: "512MiB".to_string(),
            temp_directory,
            max_temp_directory_size: "256MiB".to_string(),
        }
    }
}

/// What DuckDB says when asked to roll back with no transaction open — the
/// expected answer for a borrow that ended cleanly, not a failure.
const NO_TRANSACTION_TO_ROLL_BACK: &str = "no transaction is active";

/// One pooled database and the directory it spills into.
///
/// Every database gets a directory of its own because DuckDB names its spill
/// files the same in every database (`duckdb_temp_storage_DEFAULT-0.tmp` and
/// siblings, measured on DuckDB 1.5.5), and does not make the directory unique
/// itself. Measured with two databases in one process spilling into one shared
/// directory: all four runs failed — three segfaults and one `INTERNAL Error:
/// Cannot perform IO in in-memory database - CreateBlock!` — where the same
/// workload with a directory each returned correct results in four of four.
///
/// `conn` is declared before `spill` so it drops first: DuckDB closes and
/// releases its spill files, then the directory is removed. The data plane has
/// no shutdown path that drops the pool, so every stop — `systemctl stop` and
/// `restart` included, measured with SIGTERM — leaves the directories behind;
/// `sweep_abandoned_spill_directories` is what removes them.
struct Member {
    conn: Connection,
    spill: tempfile::TempDir,
}

struct PoolState {
    idle: Vec<Member>,
    /// Connections the pool is entitled to but has not built, or has discarded.
    /// `idle.len() + checked_out + unbuilt == size` always.
    unbuilt: usize,
}

pub struct LakePool {
    catalog_connstr: String,
    data_path: String,
    settings: PoolSettings,
    /// `<host>-<pid>-`, which every spill directory this process creates starts
    /// with; see `sweep_abandoned_spill_directories`.
    spill_prefix: String,
    state: Mutex<PoolState>,
}

impl LakePool {
    /// Build the pool and its first connection.
    pub fn open(
        catalog_connstr: String,
        data_path: String,
        settings: PoolSettings,
    ) -> Result<Arc<Self>, Box<dyn std::error::Error>> {
        settings.validate()?;
        // An unwritable root must stop the unit here rather than surface as a
        // failed request under load; building the first member below creates a
        // directory in it, so it is checked for writing as well as existence.
        std::fs::create_dir_all(&settings.temp_directory).map_err(|e| {
            format!(
                "cannot create the DuckDB spill root {}: {e}",
                settings.temp_directory.display()
            )
        })?;
        let host = host_name()?;
        for removed in sweep_abandoned_spill_directories(&settings.temp_directory, &host)? {
            eprintln!(
                "removed spill directory {} left by a process that is gone",
                removed.display()
            );
        }
        let pool = Arc::new(Self {
            catalog_connstr,
            data_path,
            settings,
            spill_prefix: spill_prefix(&host),
            state: Mutex::new(PoolState {
                idle: Vec::new(),
                unbuilt: 0,
            }),
        });
        let first = pool.build_connection()?;
        {
            let mut state = pool.lock_state();
            state.idle.push(first);
            state.unbuilt = pool.settings.size - 1;
        }
        Ok(pool)
    }

    /// Where DuckLake keeps its Parquet files, for a handler that moves files into
    /// it as well as reading through the connection.
    pub fn data_path(&self) -> &str {
        &self.data_path
    }

    /// Take a connection for writing to the lake, or refuse — the same bound as
    /// `checkout`. See `LakeWriter`.
    pub fn checkout_for_write(self: &Arc<Self>) -> Result<LakeWriter, Status> {
        let pooled = self.checkout()?;
        pooled
            .execute_batch(LAKE_WRITE_SETTINGS)
            .map_err(|e| Status::internal(format!("failed to set up a lake write: {e}")))?;
        Ok(LakeWriter {
            conn: WriterConnection::Pooled(pooled),
        })
    }

    /// Take a connection, or refuse. Never waits — the refusal is the in-flight
    /// bound; see the module docs for why it is not a queue.
    pub fn checkout(self: &Arc<Self>) -> Result<PooledConnection, Status> {
        {
            let mut state = self.lock_state();
            if let Some(member) = state.idle.pop() {
                return Ok(PooledConnection {
                    pool: Arc::clone(self),
                    member: Some(member),
                });
            }
            if state.unbuilt == 0 {
                return Err(Status::resource_exhausted(format!(
                    "data plane busy: all {} DuckLake connections are in use; retry",
                    self.settings.size
                )));
            }
            // Reserved before the build so two callers cannot both spend the
            // same slot; handed back below if the build fails.
            state.unbuilt -= 1;
        }
        match self.build_connection() {
            Ok(member) => Ok(PooledConnection {
                pool: Arc::clone(self),
                member: Some(member),
            }),
            Err(e) => {
                self.lock_state().unbuilt += 1;
                // `unavailable`, not `internal`: by the time the pool is serving,
                // a failed build is the catalog being unreachable or at its
                // connection limit, or the spill root refusing a new directory
                // (scratch full or remounted), each of which can clear. A setting
                // DuckDB rejects cannot arrive here — `open` builds one at boot,
                // so the unit never starts with a setting it will not take.
                Err(Status::unavailable(format!(
                    "failed to open a DuckLake connection: {e}"
                )))
            }
        }
    }

    fn build_connection(&self) -> Result<Member, Box<dyn std::error::Error>> {
        let spill = self.new_spill_directory()?;
        let conn = Connection::open_in_memory()?;
        ducklake::connect_ducklake(&conn, &self.catalog_connstr, &self.data_path)?;
        let member = Member { conn, spill };
        self.settle(&member)?;
        Ok(member)
    }

    /// A directory of this member's own under the spill root; see `Member`.
    fn new_spill_directory(&self) -> Result<tempfile::TempDir, String> {
        let root = &self.settings.temp_directory;
        let dir = tempfile::Builder::new()
            .prefix(&self.spill_prefix)
            .tempdir_in(root)
            .map_err(|e| {
                format!(
                    "cannot create a spill directory under {}: {e}",
                    root.display()
                )
            })?;
        // The root passed `validate`; the name `tempfile` appended is checked too,
        // since the whole path reaches DuckDB inside a quoted SQL literal.
        ducklake::validate_sql_literal(&dir.path().display().to_string(), "DUCKDB_TEMP_DIRECTORY")?;
        Ok(dir)
    }

    /// A new member's settings: its spill directory, then `apply_settings`. The
    /// directory is set here and never again, because DuckDB refuses to change
    /// `temp_directory` once the database has used it — measured on DuckDB 1.5.5,
    /// `Cannot switch temporary directory after the current one has been used`
    /// for the same path, after the spill files were gone, and for `RESET` too.
    fn settle(&self, member: &Member) -> Result<(), duckdb::Error> {
        member.conn.execute_batch(&format!(
            "SET temp_directory='{}';",
            member.spill.path().display()
        ))?;
        self.apply_settings(member)
    }

    /// The settings a connection carries, applied at build and re-applied when it
    /// comes back, because a borrower's `SET` outlives its borrow: measured, a
    /// connection kept a changed `preserve_insertion_order` after the request that
    /// set it was done.
    ///
    /// `preserve_insertion_order` is off for reads: no DoGet query has an `ORDER
    /// BY`, chunks are reassembled by `chunk_index`, a pair's mates share one row,
    /// no query takes a `LIMIT` or a first row without ordering, and the
    /// masked-read DoGet is a join that carries no order either way. A write to
    /// the lake runs on a `LakeWriter`, which turns order on for its borrow.
    ///
    /// Two obligations follow. A handler that `SET`s anything not listed here
    /// leaks it to the next borrower, so a new `SET` in a request path belongs in
    /// this list — except `temp_directory`, which cannot be re-applied (see
    /// `settle`), so no handler may set it. And this is the only place limits
    /// are applied, apart from `LAKE_WRITE_SETTINGS` for a write's borrow, so a
    /// `Connection` built outside the pool — the boot connection in `main`, a
    /// test's own — runs at DuckDB's defaults.
    fn apply_settings(&self, member: &Member) -> Result<(), duckdb::Error> {
        member.conn.execute_batch(&format!(
            "SET threads={};
             SET memory_limit='{}';
             SET max_temp_directory_size='{}';
             SET preserve_insertion_order=false;",
            self.settings.threads,
            self.settings.memory_limit,
            self.settings.max_temp_directory_size,
        ))
    }

    /// Put a connection back, or discard it. A borrow that failed inside an
    /// explicit transaction leaves the connection *in* that transaction —
    /// measured: a later statement on it saw the uncommitted row and a later
    /// `COMMIT` succeeded, committing writes the failed request had made. So a
    /// returning connection is rolled back and re-set, and one that will not reset
    /// is dropped; its slot goes back to `unbuilt` so the next checkout rebuilds
    /// it and the bound does not quietly shrink.
    fn give_back(&self, member: Member) {
        match self.reset(&member) {
            Ok(()) => self.lock_state().idle.push(member),
            Err(e) => {
                eprintln!("discarding a pooled DuckLake connection that failed to reset: {e}");
                drop(member);
                self.lock_state().unbuilt += 1;
            }
        }
    }

    fn reset(&self, member: &Member) -> Result<(), duckdb::Error> {
        // Rolled back unconditionally because there is nothing to ask: this
        // binding's `is_autocommit()` reports true even inside an explicit
        // `BEGIN TRANSACTION` (measured on duckdb 1.10505.0), so branching on it
        // would skip every rollback that matters. A connection with no
        // transaction open answers with exactly one error, which is the answer
        // that there was nothing to roll back — a test drives both halves, so a
        // reworded error fails there rather than making every return a discard.
        if let Err(e) = member.conn.execute_batch("ROLLBACK;") {
            if !e.to_string().contains(NO_TRANSACTION_TO_ROLL_BACK) {
                return Err(e);
            }
        }
        self.apply_settings(member)
    }

    fn lock_state(&self) -> std::sync::MutexGuard<'_, PoolState> {
        // Nothing under this guard can panic: the work is a `Vec` pop or push and
        // a counter, with every build and reset done outside the lock. The
        // recovery is here because honouring a poison flag we cannot explain
        // would turn one unexplained panic into a data plane that refuses every
        // request for the life of the process.
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// A pool over plain in-memory DuckDB databases, with no DuckLake attached:
    /// for tests that need a pool but never reach a catalog. The bound, the
    /// refusal and the reset on return are the production path; the lazy build —
    /// reserving an unbuilt slot, crediting it back when a build fails — is only
    /// exercised against a real catalog, in the integration test.
    #[cfg(test)]
    pub(crate) fn unattached_for_tests(settings: PoolSettings) -> Arc<Self> {
        settings
            .validate()
            .expect("test pool settings must pass the same check production settings do");
        let pool = Arc::new(Self {
            catalog_connstr: String::new(),
            data_path: String::new(),
            settings,
            spill_prefix: spill_prefix(&host_name().expect("read the host name")),
            state: Mutex::new(PoolState {
                idle: Vec::new(),
                unbuilt: 0,
            }),
        });
        std::fs::create_dir_all(&pool.settings.temp_directory).expect("create the spill root");
        let mut idle = Vec::new();
        for _ in 0..pool.settings.size {
            let member = Member {
                conn: Connection::open_in_memory().expect("open in-memory DuckDB"),
                spill: pool
                    .new_spill_directory()
                    .expect("create a spill directory"),
            };
            pool.settle(&member).expect("apply pool settings");
            idle.push(member);
        }
        pool.lock_state().idle = idle;
        pool
    }

    #[cfg(test)]
    fn idle_len(&self) -> usize {
        self.lock_state().idle.len()
    }

    #[cfg(all(test, feature = "integration"))]
    fn unbuilt_len(&self) -> usize {
        self.lock_state().unbuilt
    }
}

/// The name every spill directory this process creates starts with. The host is
/// in it because the spill root is on `PATH_SCRATCH`, which every host mounts, so
/// a pid alone does not say whose a directory is.
fn spill_prefix(host: &str) -> String {
    format!("{host}-{}-", std::process::id())
}

fn host_name() -> Result<String, String> {
    let mut buf = [0u8; 256];
    // SAFETY: `buf` is writable for its whole length, which is what is passed.
    if unsafe { libc::gethostname(buf.as_mut_ptr().cast(), buf.len()) } != 0 {
        return Err(format!(
            "cannot read the host name for the spill directories: {}",
            std::io::Error::last_os_error()
        ));
    }
    let len = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    let host = String::from_utf8(buf[..len].to_vec())
        .map_err(|_| "the host name is not UTF-8".to_string())?;
    if host.is_empty() || host.contains('/') {
        return Err(format!(
            "the host name {host:?} cannot start a spill directory's name"
        ));
    }
    Ok(host)
}

/// Whether `pid` names a running process. A pid that cannot be asked about —
/// 0, or one past `i32`, which `kill` would read as a process group — counts as
/// running, so the sweep leaves its directory alone.
fn process_is_running(pid: u32) -> bool {
    let Ok(pid) = libc::pid_t::try_from(pid) else {
        return true;
    };
    if pid == 0 {
        return true;
    }
    // SAFETY: signal 0 delivers nothing; it only checks that the pid exists.
    if unsafe { libc::kill(pid, 0) } == 0 {
        return true;
    }
    // EPERM: it exists, under another user.
    std::io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH)
}

/// Remove the spill directories under `root` that a process on this host left
/// when it stopped, and return them. A directory is this host's when its name is
/// `<host>-<pid>-<suffix>` with an alphanumeric suffix — `tempfile`'s — so
/// another host whose name extends this one (`qiita` and `qiita-2`) does not
/// match. It is left when its pid is this process or a running one, which keeps
/// the directories of the other instances on this host; a pid reused by an
/// unrelated process only postpones the removal to a later boot.
fn sweep_abandoned_spill_directories(root: &Path, host: &str) -> Result<Vec<PathBuf>, String> {
    let entries = std::fs::read_dir(root)
        .map_err(|e| format!("cannot list the spill root {}: {e}", root.display()))?;
    let own = format!("{host}-");
    let mut removed = Vec::new();
    for entry in entries {
        let entry =
            entry.map_err(|e| format!("cannot list the spill root {}: {e}", root.display()))?;
        let name = entry.file_name();
        let Some(rest) = name.to_str().and_then(|n| n.strip_prefix(&own)) else {
            continue;
        };
        let Some((pid, suffix)) = rest.split_once('-') else {
            continue;
        };
        let Ok(pid) = pid.parse::<u32>() else {
            continue;
        };
        if suffix.is_empty() || !suffix.chars().all(|c| c.is_ascii_alphanumeric()) {
            continue;
        }
        if pid == std::process::id() || process_is_running(pid) {
            continue;
        }
        let path = entry.path();
        if !entry.file_type().is_ok_and(|t| t.is_dir()) {
            continue;
        }
        if remove_abandoned_spill_directory(&path, pid)? {
            removed.push(path);
        }
    }
    Ok(removed)
}

/// Remove one abandoned spill directory; `Ok(false)` when it was already gone.
/// That is another instance on this host booting at the same moment and removing
/// it first (measured: `remove_dir_all` on a listed path removed since returns
/// NotFound). Any other failure stops boot.
fn remove_abandoned_spill_directory(path: &Path, pid: u32) -> Result<bool, String> {
    match std::fs::remove_dir_all(path) {
        Ok(()) => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(format!(
            "cannot remove the spill directory {} left by pid {pid}: {e}",
            path.display()
        )),
    }
}

impl std::fmt::Debug for LakePool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // try_lock, not lock: formatting must not block, least of all on a lock
        // the formatting caller may already hold.
        let idle = self.state.try_lock().map(|s| s.idle.len()).ok();
        f.debug_struct("LakePool")
            .field("size", &self.settings.size)
            .field("threads", &self.settings.threads)
            .field("memory_limit", &self.settings.memory_limit)
            .field("idle", &idle)
            .finish()
    }
}

/// A connection on loan. Returns itself to the pool when dropped, so a request
/// that fails, panics or has its stream dropped by the client does not leak the
/// slot.
pub struct PooledConnection {
    pool: Arc<LakePool>,
    member: Option<Member>,
}

impl std::fmt::Debug for PooledConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PooledConnection")
            .field("held", &self.member.is_some())
            .finish()
    }
}

impl Deref for PooledConnection {
    type Target = Connection;

    fn deref(&self) -> &Connection {
        &self
            .member
            .as_ref()
            .expect("a PooledConnection holds its connection until it is dropped")
            .conn
    }
}

impl Drop for PooledConnection {
    fn drop(&mut self) {
        if let Some(member) = self.member.take() {
            self.pool.give_back(member);
        }
    }
}

/// What a lake write runs at, for the length of its borrow; the reset on return
/// puts back the pool's baseline.
///
/// One thread, because on DuckLake `d8a1881e` a multi-threaded `INSERT` into a
/// lake table loses its rows' order whatever `preserve_insertion_order` or an
/// explicit `ORDER BY` say — measured, a 20M-row sorted source inserted at
/// `threads=8` came back with 21–44 rows out of order per insert across all four
/// combinations, and 0 at `threads=1`, while a plain Parquet `COPY` of the same
/// source at `threads=8` kept it. Upstream, duckdb/ducklake#1356: the insert's
/// copy is always planned parallel. `preserve_insertion_order` is on as well, for
/// a write whose source order is the order wanted and that has no `ORDER BY`.
const LAKE_WRITE_SETTINGS: &str = "SET threads=1; SET preserve_insertion_order=true;";

/// A connection lent for writing to the lake. Data at rest is kept in its sort
/// order, so every function that changes the lake takes this rather than a
/// `Connection`, and the only way to get one is `LakePool::checkout_for_write`,
/// which applies `LAKE_WRITE_SETTINGS`. The type is what stops a write being
/// handed a read's connection; it does not stop SQL that writes being run on a
/// `PooledConnection` directly.
///
/// Deletes and registration are on it too, though neither writes rows — measured,
/// a DuckLake `DELETE` adds delete files and leaves data files as they are, and
/// `ducklake_add_data_files` leaves a registered file byte-identical and reads it
/// back in order at eight threads — so that a
/// row write added to either later is ordered without anyone choosing it. The
/// cost is that they run on one thread as well.
pub struct LakeWriter {
    conn: WriterConnection,
}

enum WriterConnection {
    Pooled(PooledConnection),
    #[cfg(test)]
    Owned(Connection),
}

impl LakeWriter {
    /// A test's own connection, set up the way `checkout_for_write` sets up a
    /// pooled one.
    #[cfg(test)]
    pub(crate) fn for_tests(conn: Connection) -> Self {
        conn.execute_batch(LAKE_WRITE_SETTINGS)
            .expect("apply the lake write settings");
        Self {
            conn: WriterConnection::Owned(conn),
        }
    }
}

impl std::fmt::Debug for LakeWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LakeWriter").finish_non_exhaustive()
    }
}

impl Deref for LakeWriter {
    type Target = Connection;

    fn deref(&self) -> &Connection {
        match &self.conn {
            WriterConnection::Pooled(pooled) => pooled,
            #[cfg(test)]
            WriterConnection::Owned(conn) => conn,
        }
    }
}

#[cfg(test)]
#[path = "pool_tests.rs"]
mod pool_tests;
