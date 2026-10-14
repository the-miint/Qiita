//! A bounded pool of attached DuckLake instances. Its size is the in-flight bound.
//!
//! Two measurements shape this. First, DuckDB's resource settings belong to a
//! *database*, not to a connection, so bounding what one request may spend means
//! pooling instances rather than handing out connections to one instance.
//! Second, holding the ATTACH is most of what a request pays: a fresh
//! connect + attach + query measured 84–107 ms and about 16 catalog
//! transactions, against 3.0–3.4 ms and about 3.7 transactions once the attach
//! is reused.
//!
//! A checkout that finds nothing idle and nothing left to build refuses with
//! `RESOURCE_EXHAUSTED` instead of waiting. Waiting would hold the caller's
//! stream open against the edge's `grpc_read_timeout 3600s` while the edge keeps
//! admitting more (64 concurrent requests per client address), which is how the
//! unbounded version failed: with every request sizing its own DuckDB against
//! the whole host, two instances under 16 concurrent streams were both
//! OOM-killed and the edge answered 888 of 896 requests with 502.
//!
//! Instances are built as they are first needed rather than all at boot, because
//! each attached instance holds one lake-Postgres backend for as long as it
//! lives — against a `max_connections` of 100 with 3 superuser-reserved, shared
//! with the control plane. One is built at `open` so a bad setting or an
//! unwritable spill directory fails the unit at boot rather than on first
//! request.

use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use duckdb::Connection;
use tonic::Status;

use crate::ducklake;

/// What every pooled instance is set to. Per instance, not per pool: DuckDB
/// applies each of these to the database it is set on.
#[derive(Debug, Clone)]
pub struct PoolSettings {
    /// Instances the pool may hold, and so requests that may be in flight.
    pub size: usize,
    pub threads: u32,
    /// In DuckDB's own spelling, e.g. `23552MiB` — note DuckDB reads `GB` as
    /// decimal and `GiB` as binary.
    pub memory_limit: String,
    /// Where an over-limit request spills. Local disk, never the shared scratch
    /// filesystem: spilling is the difference between a request that fails and
    /// one that completes (measured: the same over-limit sort raised
    /// `OutOfMemoryException` after 2.41 s with no spill directory and completed
    /// in 22.62 s with one), and that traffic does not belong on a cluster
    /// filesystem every compute node shares.
    pub temp_directory: PathBuf,
    /// Ceiling on that spill, per instance. Needed explicitly because DuckDB's
    /// own default is 90% of the *filesystem*, which N instances would each
    /// claim in full. Measured: the cap is enforced mid-query, names itself in
    /// the error (`This limit was set by the 'max_temp_directory_size' setting`)
    /// and leaves no files behind.
    pub max_temp_directory_size: String,
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
    /// Instances the pool is entitled to but has not built, or has discarded.
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
    /// Build the pool and its first instance.
    pub fn open(
        catalog_connstr: String,
        data_path: String,
        settings: PoolSettings,
    ) -> Result<Arc<Self>, Box<dyn std::error::Error>> {
        if settings.size == 0 {
            return Err("pool size must be at least 1 — a size of 0 serves no request".into());
        }
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
        let first = pool.build_instance()?;
        {
            let mut state = pool.lock_state();
            state.idle.push(first);
            state.unbuilt = pool.settings.size - 1;
        }
        Ok(pool)
    }

    /// Where DuckLake keeps its Parquet files. `register_files` moves staged
    /// files under it, so it needs the path as well as a connection.
    pub fn data_path(&self) -> &str {
        &self.data_path
    }

    /// Take an instance, or refuse. Never waits: the refusal is the in-flight
    /// bound, and the caller retries at stream open.
    pub fn checkout(self: &Arc<Self>) -> Result<PooledConnection, Status> {
        let reserved = {
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
            true
        };
        debug_assert!(reserved);
        match self.build_instance() {
            Ok(conn) => Ok(PooledConnection {
                pool: Arc::clone(self),
                conn: Some(conn),
            }),
            Err(e) => {
                self.lock_state().unbuilt += 1;
                Err(Status::internal(format!(
                    "failed to open a DuckLake connection: {e}"
                )))
            }
        }
    }

    fn build_instance(&self) -> Result<Connection, Box<dyn std::error::Error>> {
        let conn = Connection::open_in_memory()?;
        ducklake::connect_ducklake(&conn, &self.catalog_connstr, &self.data_path)?;
        self.apply_settings(&conn)?;
        Ok(conn)
    }

    /// The settings an instance carries, applied at build and re-applied when it
    /// comes back. `preserve_insertion_order` is in the list because a borrower
    /// may change it (the Parquet export does) and the change outlives the
    /// borrow: measured, a connection kept `preserve_insertion_order=false`
    /// after the request that set it was done.
    fn apply_settings(&self, conn: &Connection) -> Result<(), duckdb::Error> {
        let temp_dir = sql_quoted(&self.settings.temp_directory);
        conn.execute_batch(&format!(
            "SET threads={};
             SET memory_limit='{}';
             SET temp_directory='{}';
             SET max_temp_directory_size='{}';
             SET preserve_insertion_order=true;",
            self.settings.threads,
            self.settings.memory_limit,
            temp_dir,
            self.settings.max_temp_directory_size,
        ))
    }

    /// Put an instance back, or discard it. A borrow that failed inside an
    /// explicit transaction leaves the connection *in* that transaction —
    /// measured: a later statement on it saw the uncommitted row and a later
    /// `COMMIT` succeeded, committing writes the failed request had made. So a
    /// returning instance is rolled back and re-set, and one that will not reset
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
        // `BEGIN TRANSACTION` (measured on duckdb 1.10505.0), so branching on
        // it would skip every rollback that matters. A connection with no
        // transaction open answers with exactly one error, which is the
        // answer that there was nothing to roll back;
        // `a_clean_borrow_comes_back_without_error` pins that.
        if let Err(e) = conn.execute_batch("ROLLBACK;") {
            if !e.to_string().contains(NO_TRANSACTION_TO_ROLL_BACK) {
                return Err(e);
            }
        }
        self.apply_settings(conn)
    }

    fn lock_state(&self) -> std::sync::MutexGuard<'_, PoolState> {
        // Nothing between lock and unlock can panic, so a poisoned lock means
        // the process is already unwinding; recover the state rather than make
        // every later checkout fail.
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// A pool over plain in-memory DuckDB instances, with no DuckLake attached:
    /// for tests that need a pool but never reach a catalog. Everything else —
    /// the bound, the refusal, the reset on return — is the production path.
    #[cfg(test)]
    pub(crate) fn unattached_for_tests(settings: PoolSettings) -> Arc<Self> {
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

/// An instance on loan. Returns itself to the pool when dropped, so a request
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

/// `temp_directory` reaches DuckDB inside a quoted SQL literal, the same way the
/// catalog connection string and data path do in `ducklake::connect_ducklake`.
/// A path is not operator-free text — it comes from the environment — so a
/// single quote in it is doubled rather than left to end the literal early.
fn sql_quoted(path: &Path) -> String {
    path.display().to_string().replace('\'', "''")
}

#[cfg(test)]
#[path = "pool_tests.rs"]
mod pool_tests;
