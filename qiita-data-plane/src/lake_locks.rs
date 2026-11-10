//! Postgres advisory locks that order writers of the lake's replace-keyed tables,
//! and the in-process gates that keep their waiting off the connection pool.
//!
//! DuckLake detects conflicts per table: a `DELETE` conflicts with any concurrent
//! insert into the same table, whatever keys either touches, and a writer that
//! loses re-runs its whole transaction. Measured (DuckDB 2.0.0, ducklake
//! 417a9117, Postgres 17, writers released together):
//! - 8 writers, 10 rounds of replace-by-key `DELETE` + `ducklake_add_data_files`
//!   on six tables: 280 lost COMMITs whether the writers' keys were disjoint or
//!   shared, and 0 when the writers only inserted.
//! - 1 writer replacing a run beside 8 writers inserting new runs into the same
//!   table, 10 rounds: the replacing writer lost 10 COMMITs and the inserting ones
//!   33, so an insert also loses to a concurrent delete.
//!
//! Under steady overlap one writer can lose every attempt until
//! `registration::LAKE_COMMIT_BUDGET` runs out: on qiita-miint, 55 of 1,062
//! registrations submitted 10 s apart failed that way. So `GLOBAL_KEY` is a
//! shared/exclusive lock: a writer that deletes holds it exclusively through its
//! COMMIT, and one that only inserts holds it shared, so inserts overlap each
//! other and never a delete. Postgres queues a shared request behind a waiting
//! exclusive one (measured through DuckDB 2.0.0's postgres extension against
//! Postgres 17: a shared request made while an exclusive one waited was granted
//! only after the exclusive holder committed), so inserts arriving steadily do
//! not starve a delete. `flight_service::registration` decides which a
//! registration takes.
//!
//! The waits of one session, together, end after `LakeLocks::budget`, and none
//! holds a pooled connection: a lock session is a connection of its own to the
//! catalog's Postgres, outside the pool, and at most `LOCK_SESSIONS` are open per
//! process.

use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

use duckdb::Connection;
use sha2::{Digest, Sha256};
use tonic::Status;

/// Lock sessions one data plane may hold open at once, each one catalog backend.
/// Chosen, not measured: a session is held from a registration's key check
/// through its COMMIT, a part of the 0.3 s median from step start to commit on
/// qiita-miint (2026-11-09), which also includes the file moves.
/// `ducklake::tag_catalog_connstr` declares them with the pool, so the deploy's
/// connection budget counts them.
pub(crate) const LOCK_SESSIONS: usize = 2;

/// How long one session's waits may take in all: for the session, its keys, the
/// queue, and `GLOBAL_KEY`. A backstop for a holder that does not finish.
pub(crate) const LOCK_BUDGET: Duration = Duration::from_secs(120);

/// Advisory-lock key every writer of a replace-keyed table holds through its
/// COMMIT: exclusively when it deletes, shared when it only inserts. Distinct
/// from `ducklake::CATALOG_SETUP_LOCK_KEY`.
const GLOBAL_KEY: i64 = 0x7169_6974_615f_7772;

/// Open a connection to the catalog's Postgres for taking advisory locks, inside an
/// open transaction whose lock waits end after `budget`.
///
/// The locks are `pg_advisory_xact_lock`, held until this transaction ends: on
/// COMMIT, ROLLBACK, or the connection closing, which is also what happens when
/// the process dies. Measured through DuckDB 2.0.0's postgres extension against
/// Postgres 17: a second taker waited until the first committed; `lock_timeout`
/// ended a wait at its limit with "canceling statement due to lock timeout"; the
/// lock was released on each of the three; open, attach, lock and release took
/// 46–56 ms; the open session was one more client backend. The `SET LOCAL` and the
/// locks share one Postgres transaction because the extension keeps one Postgres
/// connection for the length of a DuckDB transaction.
pub(crate) fn open_lock_connection(
    catalog_connstr: &str,
    budget: Duration,
) -> Result<Connection, String> {
    crate::ducklake::validate_sql_literal(catalog_connstr, "catalog_connstr")?;
    let conn = Connection::open_in_memory().map_err(|e| format!("open: {e}"))?;
    conn.execute_batch(&format!(
        "LOAD postgres;
         ATTACH '{catalog_connstr}' AS lock_session (TYPE postgres);
         BEGIN;"
    ))
    .map_err(|e| format!("attach: {e}"))?;
    set_lock_timeout(&conn, budget)?;
    Ok(conn)
}

/// End the lock waits that follow on `conn` after `limit`.
fn set_lock_timeout(conn: &Connection, limit: Duration) -> Result<(), String> {
    // 0 would mean no limit; a spent budget waits 1 ms instead.
    let ms = limit.as_millis().max(1);
    conn.execute_batch(&format!(
        "CALL postgres_execute('lock_session', 'SET LOCAL lock_timeout = {ms}');"
    ))
    .map_err(|e| format!("set lock_timeout: {e}"))
}

/// Wait for advisory lock `key` on a connection from `open_lock_connection`,
/// exclusively or shared.
pub(crate) fn advisory_lock(conn: &Connection, key: i64, shared: bool) -> Result<(), String> {
    let function = if shared {
        "pg_advisory_xact_lock_shared"
    } else {
        "pg_advisory_xact_lock"
    };
    conn.query_row(
        &format!(
            "SELECT taken FROM postgres_query('lock_session', \
             'SELECT 1 AS taken FROM (SELECT {function}({key})) s')"
        ),
        [],
        |row| row.get::<_, i32>(0),
    )
    .map(|_| ())
    .map_err(|e| e.to_string())
}

/// The advisory-lock key for one run of a run-keyed table family, named by the
/// table its keys come from. Two runs whose hashes collide only wait for each other.
pub(crate) fn run_key(key_source: &str, prep_sample_idx: i64, processing_idx: i64) -> i64 {
    let digest = Sha256::digest(format!("{key_source}|{prep_sample_idx}|{processing_idx}"));
    i64::from_be_bytes(
        digest[..8]
            .try_into()
            .expect("a SHA-256 digest is 32 bytes"),
    )
}

/// A counting gate whose waits end at a deadline.
struct Gate {
    free: Mutex<usize>,
    freed: Condvar,
}

impl Gate {
    fn new(capacity: usize) -> Self {
        Self {
            free: Mutex::new(capacity),
            freed: Condvar::new(),
        }
    }

    fn enter(&self, deadline: Instant) -> Option<GatePass<'_>> {
        let mut free = self.free.lock().unwrap_or_else(|e| e.into_inner());
        while *free == 0 {
            let left = deadline.checked_duration_since(Instant::now())?;
            free = self
                .freed
                .wait_timeout(free, left)
                .unwrap_or_else(|e| e.into_inner())
                .0;
        }
        *free -= 1;
        Some(GatePass { gate: self })
    }
}

struct GatePass<'a> {
    gate: &'a Gate,
}

impl Drop for GatePass<'_> {
    fn drop(&mut self) {
        *self.gate.free.lock().unwrap_or_else(|e| e.into_inner()) += 1;
        self.gate.freed.notify_one();
    }
}

/// One process's lock sessions and its queue for holding `GLOBAL_KEY`
/// exclusively.
pub(crate) struct LakeLocks {
    catalog_connstr: String,
    budget: Duration,
    sessions: Gate,
    /// One exclusive taker of `GLOBAL_KEY` per process at a time, so the others
    /// wait here rather than each holding a session and a backend in Postgres's
    /// queue.
    queue: Gate,
    global: bool,
}

impl LakeLocks {
    pub(crate) fn new(catalog_connstr: String) -> Self {
        Self {
            catalog_connstr,
            budget: LOCK_BUDGET,
            sessions: Gate::new(LOCK_SESSIONS),
            queue: Gate::new(1),
            global: true,
        }
    }

    /// `new` with every session's waits ending after `budget`, for a test that
    /// times a wait out.
    #[cfg(test)]
    pub(crate) fn with_budget(catalog_connstr: String, budget: Duration) -> Self {
        Self {
            budget,
            ..Self::new(catalog_connstr)
        }
    }

    /// `new` with `GLOBAL_KEY` never taken, for a test that overlaps writers that
    /// delete.
    #[cfg(test)]
    pub(crate) fn without_global_lock(catalog_connstr: String) -> Self {
        Self {
            global: false,
            ..Self::new(catalog_connstr)
        }
    }

    /// Wait for a session and open it. `what` names the waiter in errors.
    pub(crate) fn session(&self, what: &str) -> Result<LockSession<'_>, Status> {
        let started = Instant::now();
        let deadline = started + self.budget;
        let pass = self.sessions.enter(deadline).ok_or_else(|| {
            Status::unavailable(format!(
                "{what}: no lake lock session came free within {} s",
                self.budget.as_secs()
            ))
        })?;
        let left = deadline.saturating_duration_since(Instant::now());
        // The pool answers a catalog it cannot reach the same way.
        let conn = open_lock_connection(&self.catalog_connstr, left).map_err(|e| {
            Status::unavailable(format!("{what}: failed to open a lake lock session ({e})"))
        })?;
        Ok(LockSession {
            locks: self,
            what: what.to_string(),
            conn,
            deadline,
            holds_global: false,
            waited: vec![("session", started.elapsed())],
            _session: pass,
            queued: None,
        })
    }
}

/// An open lock session. Its locks are released when it is dropped.
pub(crate) struct LockSession<'a> {
    locks: &'a LakeLocks,
    what: String,
    conn: Connection,
    deadline: Instant,
    holds_global: bool,
    waited: Vec<(&'static str, Duration)>,
    // Field order is drop order: the connection's transaction ends (in `drop`)
    // before these passes let the next waiter in.
    queued: Option<GatePass<'a>>,
    _session: GatePass<'a>,
}

impl LockSession<'_> {
    /// Wait for every key in `keys`, in ascending order so two sessions that share
    /// keys cannot each hold one the other waits for.
    ///
    /// Refused once this session holds `GLOBAL_KEY`: a holder of it waiting for a
    /// key, while that key's holder waits for `GLOBAL_KEY`, would wait forever.
    pub(crate) fn lock_keys(&mut self, keys: &[i64]) -> Result<(), Status> {
        if self.holds_global {
            return Err(Status::internal(format!(
                "{}: asked for run locks after the global lock",
                self.what
            )));
        }
        let started = Instant::now();
        let mut sorted = keys.to_vec();
        sorted.sort_unstable();
        sorted.dedup();
        for key in sorted {
            self.wait_for(key, false)
                .map_err(|e| self.failed("its keys' locks", started, e))?;
        }
        self.waited.push(("keys", started.elapsed()));
        Ok(())
    }

    /// Wait for this process's queue, then hold `GLOBAL_KEY` exclusively: for a
    /// writer that deletes from a replace-keyed table.
    pub(crate) fn lock_global(&mut self) -> Result<(), Status> {
        if !self.locks.global {
            return Ok(());
        }
        let started = Instant::now();
        let pass = self.locks.queue.enter(self.deadline).ok_or_else(|| {
            Status::unavailable(format!(
                "{}: the lake writer queue did not come free within {} s",
                self.what,
                self.locks.budget.as_secs()
            ))
        })?;
        self.queued = Some(pass);
        self.waited.push(("queue", started.elapsed()));
        self.hold_global(false)
    }

    /// Hold `GLOBAL_KEY` shared: for a writer that only inserts into a
    /// replace-keyed table.
    pub(crate) fn lock_global_shared(&mut self) -> Result<(), Status> {
        if !self.locks.global {
            return Ok(());
        }
        self.hold_global(true)
    }

    fn hold_global(&mut self, shared: bool) -> Result<(), Status> {
        let started = Instant::now();
        self.holds_global = true;
        self.wait_for(GLOBAL_KEY, shared)
            .map_err(|e| self.failed("the global lock", started, e))?;
        self.waited.push(("global", started.elapsed()));
        Ok(())
    }

    /// Wait for `key` until this session's deadline.
    fn wait_for(&self, key: i64, shared: bool) -> Result<(), String> {
        set_lock_timeout(
            &self.conn,
            self.deadline.saturating_duration_since(Instant::now()),
        )?;
        advisory_lock(&self.conn, key, shared)
    }

    /// The waits so far, as `name_wait_ms=N` pairs for a log line.
    pub(crate) fn waits(&self) -> String {
        self.waited
            .iter()
            .map(|(name, d)| format!("{name}_wait_ms={}", d.as_millis()))
            .collect::<Vec<_>>()
            .join(" ")
    }

    fn failed(&self, which: &str, started: Instant, e: String) -> Status {
        Status::unavailable(format!(
            "{}: waited {} ms for {which} and gave up: {e}",
            self.what,
            started.elapsed().as_millis()
        ))
    }
}

impl Drop for LockSession<'_> {
    fn drop(&mut self) {
        if let Err(e) = self.conn.execute_batch("ROLLBACK") {
            // The connection closes right after, which releases the locks anyway.
            eprintln!(
                "warning: {}: ending the lake lock session failed: {e}",
                self.what
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SOON: Duration = Duration::from_millis(50);

    #[test]
    fn a_gate_admits_up_to_its_capacity_and_then_times_out() {
        let gate = Gate::new(2);
        let first = gate.enter(Instant::now() + SOON);
        let second = gate.enter(Instant::now() + SOON);
        assert!(first.is_some() && second.is_some());
        let started = Instant::now();
        assert!(gate.enter(Instant::now() + SOON).is_none());
        assert!(
            started.elapsed() >= SOON,
            "gave up after {:?}",
            started.elapsed()
        );
    }

    #[test]
    fn a_gate_lets_a_waiter_in_when_a_pass_is_dropped() {
        let gate = Gate::new(1);
        let held = gate.enter(Instant::now() + SOON).unwrap();
        std::thread::scope(|scope| {
            let waiter = scope.spawn(|| {
                gate.enter(Instant::now() + Duration::from_secs(5))
                    .is_some()
            });
            std::thread::sleep(SOON);
            assert!(
                !waiter.is_finished(),
                "a waiter got in while the gate was full"
            );
            drop(held);
            assert!(waiter.join().unwrap(), "the waiter did not get in");
        });
    }

    #[test]
    fn a_gate_waits_no_longer_than_a_past_deadline() {
        let gate = Gate::new(0);
        let started = Instant::now();
        assert!(gate.enter(started).is_none());
        assert!(started.elapsed() < SOON, "waited {:?}", started.elapsed());
    }
}
