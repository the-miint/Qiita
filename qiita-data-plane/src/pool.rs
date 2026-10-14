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
//! setting or an unwritable spill directory fails the unit at boot rather than on
//! first request.

use std::ops::Deref;
use std::path::PathBuf;
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
    /// Where an over-limit request spills, which is the difference between a
    /// request that fails and one that completes: measured, the same over-limit
    /// sort raised `OutOfMemoryException` after 2.41 s with no spill directory
    /// and completed in 22.62 s with one. Which filesystem, and why, is
    /// `config::DERIVED_TEMP_DIRECTORY_LEAF`.
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
    /// in `apply_settings` — the same way the connection string and data path do
    /// in `ducklake::connect_ducklake`. What a valid value looks like is DuckDB's
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

struct PoolState {
    idle: Vec<Connection>,
    /// Connections the pool is entitled to but has not built, or has discarded.
    /// `idle.len() + checked_out + unbuilt == size` always.
    unbuilt: usize,
}

pub struct LakePool {
    catalog_connstr: String,
    data_path: String,
    settings: PoolSettings,
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
        // DuckDB does not create this itself; an unwritable path must stop the
        // unit here rather than surface as a failed request under load.
        std::fs::create_dir_all(&settings.temp_directory).map_err(|e| {
            format!(
                "cannot create DuckDB temp directory {}: {e}",
                settings.temp_directory.display()
            )
        })?;
        let pool = Arc::new(Self {
            catalog_connstr,
            data_path,
            settings,
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

    /// Take a connection, or refuse. Never waits — the refusal is the in-flight
    /// bound; see the module docs for why it is not a queue.
    pub fn checkout(self: &Arc<Self>) -> Result<PooledConnection, Status> {
        {
            let mut state = self.lock_state();
            if let Some(conn) = state.idle.pop() {
                return Ok(PooledConnection {
                    pool: Arc::clone(self),
                    conn: Some(conn),
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
            Ok(conn) => Ok(PooledConnection {
                pool: Arc::clone(self),
                conn: Some(conn),
            }),
            Err(e) => {
                self.lock_state().unbuilt += 1;
                // `unavailable`, not `internal`: by the time the pool is serving,
                // a failed build is the catalog being unreachable or at its
                // connection limit, which the next attempt can clear. A setting
                // DuckDB rejects cannot arrive here — `open` builds one at boot,
                // so the unit never starts with a setting it will not take.
                Err(Status::unavailable(format!(
                    "failed to open a DuckLake connection: {e}"
                )))
            }
        }
    }

    fn build_connection(&self) -> Result<Connection, Box<dyn std::error::Error>> {
        let conn = Connection::open_in_memory()?;
        ducklake::connect_ducklake(&conn, &self.catalog_connstr, &self.data_path)?;
        self.apply_settings(&conn)?;
        Ok(conn)
    }

    /// The settings a connection carries, applied at build and re-applied when it
    /// comes back. `preserve_insertion_order` is in the list because a borrower
    /// may change it (the Parquet export does) and the change outlives the
    /// borrow: measured, a connection kept `preserve_insertion_order=false`
    /// after the request that set it was done.
    ///
    /// Two obligations follow. A handler that `SET`s anything not listed here
    /// leaks it to the next borrower, so a new `SET` in a request path belongs in
    /// this list. And this is the only place limits are applied, so a
    /// `Connection` built outside the pool — the boot connection in `main`, a
    /// test's own — runs at DuckDB's defaults.
    fn apply_settings(&self, conn: &Connection) -> Result<(), duckdb::Error> {
        conn.execute_batch(&format!(
            "SET threads={};
             SET memory_limit='{}';
             SET temp_directory='{}';
             SET max_temp_directory_size='{}';
             SET preserve_insertion_order=true;",
            self.settings.threads,
            self.settings.memory_limit,
            self.settings.temp_directory.display(),
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
    fn give_back(&self, conn: Connection) {
        match self.reset(&conn) {
            Ok(()) => self.lock_state().idle.push(conn),
            Err(e) => {
                eprintln!("discarding a pooled DuckLake connection that failed to reset: {e}");
                drop(conn);
                self.lock_state().unbuilt += 1;
            }
        }
    }

    fn reset(&self, conn: &Connection) -> Result<(), duckdb::Error> {
        // Rolled back unconditionally because there is nothing to ask: this
        // binding's `is_autocommit()` reports true even inside an explicit
        // `BEGIN TRANSACTION` (measured on duckdb 1.10505.0), so branching on it
        // would skip every rollback that matters. A connection with no
        // transaction open answers with exactly one error, which is the answer
        // that there was nothing to roll back — a test drives both halves, so a
        // reworded error fails there rather than making every return a discard.
        if let Err(e) = conn.execute_batch("ROLLBACK;") {
            if !e.to_string().contains(NO_TRANSACTION_TO_ROLL_BACK) {
                return Err(e);
            }
        }
        self.apply_settings(conn)
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
            state: Mutex::new(PoolState {
                idle: Vec::new(),
                unbuilt: 0,
            }),
        });
        let mut idle = Vec::new();
        for _ in 0..pool.settings.size {
            let conn = Connection::open_in_memory().expect("open in-memory DuckDB");
            pool.apply_settings(&conn).expect("apply pool settings");
            idle.push(conn);
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
    conn: Option<Connection>,
}

impl std::fmt::Debug for PooledConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PooledConnection")
            .field("held", &self.conn.is_some())
            .finish()
    }
}

impl Deref for PooledConnection {
    type Target = Connection;

    fn deref(&self) -> &Connection {
        self.conn
            .as_ref()
            .expect("a PooledConnection holds its connection until it is dropped")
    }
}

impl Drop for PooledConnection {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            self.pool.give_back(conn);
        }
    }
}

#[cfg(test)]
#[path = "pool_tests.rs"]
mod pool_tests;
