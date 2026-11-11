//! Postgres advisory locks that order writers of the lake's replace-keyed tables,
//! and the in-process gate that bounds how many lock sessions a process opens.
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
//! registrations submitted 10 s apart failed that way. So every writer of a
//! replace-keyed table holds `GLOBAL_KEY` exclusively through its COMMIT, and the
//! writers reach COMMIT one at a time. `flight_service::registration` says which
//! registrations take it.
//!
//! The waits of one session, together, end after `LakeLocks::budget`, and none
//! holds a pooled connection: a lock session is a connection of its own to the
//! catalog's Postgres, outside the pool, and at most `LOCK_SESSIONS` are open per
//! process.

use std::collections::VecDeque;
use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

use duckdb::Connection;
use tonic::Status;

/// Lock sessions one data plane may hold open at once, each one catalog backend.
/// Chosen, not measured: a session is held through a registration's lock wait
/// and its COMMIT. With no wait, a session is held for part of the 0.3 s median
/// from step start to commit on qiita-miint (2026-11-09), a span that also
/// includes the file moves; a session waiting behind another writer's lock is
/// held for that wait too.
/// `ducklake::tag_catalog_connstr` declares them with the pool, so the deploy's
/// connection budget counts them.
pub(crate) const LOCK_SESSIONS: usize = 2;

/// How long one session's waits may take in all: for the session and for
/// `GLOBAL_KEY`. A backstop for a holder that does not finish.
pub(crate) const LOCK_BUDGET: Duration = Duration::from_secs(120);

/// Advisory-lock key every writer of a replace-keyed table holds exclusively
/// through its COMMIT. Distinct from `ducklake::CATALOG_SETUP_LOCK_KEY`.
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
    // The session sits idle in its transaction while its writer works on a pooled
    // connection, so a server, database or role `idle_in_transaction_session_timeout`
    // shorter than that work would end it and release the lock (a 1 s setting
    // ended a session held 2.5 s, Postgres 17); 0 turns it off for this session.
    for (setting, value) in [
        ("tcp_keepalives_idle", LOCK_KEEPALIVE_IDLE.as_secs()),
        ("tcp_keepalives_interval", LOCK_KEEPALIVE_INTERVAL.as_secs()),
        ("tcp_keepalives_count", LOCK_KEEPALIVE_COUNT),
        ("idle_in_transaction_session_timeout", 0),
    ] {
        conn.execute_batch(&format!(
            "CALL postgres_execute('lock_session', 'SET LOCAL {setting} = {value}');"
        ))
        .map_err(|e| format!("set {setting}: {e}"))?;
    }
    set_lock_timeout(&conn, budget)?;
    Ok(conn)
}

/// TCP keepalives on the catalog server's end of a lock session, so the server
/// ends a session whose data plane it can no longer reach, releasing its locks,
/// rather than after the server's own keepalive settings (`tcp_keepalives_idle`
/// was 7200 s on qiita-miint's catalog, 2026-11-11). A session that exits closes
/// its connection, which releases its locks without this. The values are chosen.
/// Measured (Postgres 17, client's network removed while it held a lock idle in a
/// transaction): released 51–56 s after the cut with these values, still held at
/// 185 s with the server's defaults. Over a Unix socket Postgres ignores them.
const LOCK_KEEPALIVE_IDLE: Duration = Duration::from_secs(30);
const LOCK_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(10);
const LOCK_KEEPALIVE_COUNT: u64 = 3;

/// End the lock waits that follow on `conn` after `limit`.
fn set_lock_timeout(conn: &Connection, limit: Duration) -> Result<(), String> {
    // 0 would mean no limit; a spent budget waits 1 ms instead.
    let ms = limit.as_millis().max(1);
    conn.execute_batch(&format!(
        "CALL postgres_execute('lock_session', 'SET LOCAL lock_timeout = {ms}');"
    ))
    .map_err(|e| format!("set lock_timeout: {e}"))
}

/// Wait for advisory lock `key`, exclusively, on a connection from
/// `open_lock_connection`.
pub(crate) fn advisory_lock(conn: &Connection, key: i64) -> Result<(), String> {
    conn.query_row(
        &format!(
            "SELECT taken FROM postgres_query('lock_session', \
             'SELECT 1 AS taken FROM (SELECT pg_advisory_xact_lock({key})) s')"
        ),
        [],
        |row| row.get::<_, i32>(0),
    )
    .map(|_| ())
    .map_err(|e| e.to_string())
}

/// A counting gate whose waits end at a deadline. Waiters get in in the order they
/// arrived, and a new arrival queues behind them even when a pass is free.
struct Gate {
    state: Mutex<GateState>,
    changed: Condvar,
}

struct GateState {
    free: usize,
    waiting: VecDeque<u64>,
    next_ticket: u64,
}

impl Gate {
    fn new(capacity: usize) -> Self {
        Self {
            state: Mutex::new(GateState {
                free: capacity,
                waiting: VecDeque::new(),
                next_ticket: 0,
            }),
            changed: Condvar::new(),
        }
    }

    fn enter(&self, deadline: Instant) -> Option<GatePass<'_>> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let ticket = state.next_ticket;
        state.next_ticket += 1;
        state.waiting.push_back(ticket);
        loop {
            if state.free > 0 && state.waiting.front() == Some(&ticket) {
                state.waiting.pop_front();
                state.free -= 1;
                // The next in line may fit in a pass that is still free.
                self.changed.notify_all();
                return Some(GatePass { gate: self });
            }
            let Some(left) = deadline.checked_duration_since(Instant::now()) else {
                state.waiting.retain(|&t| t != ticket);
                self.changed.notify_all();
                return None;
            };
            state = self
                .changed
                .wait_timeout(state, left)
                .unwrap_or_else(|e| e.into_inner())
                .0;
        }
    }
}

struct GatePass<'a> {
    gate: &'a Gate,
}

impl Drop for GatePass<'_> {
    fn drop(&mut self) {
        let mut state = self.gate.state.lock().unwrap_or_else(|e| e.into_inner());
        state.free += 1;
        self.gate.changed.notify_all();
    }
}

/// One process's lock sessions.
pub(crate) struct LakeLocks {
    catalog_connstr: String,
    budget: Duration,
    sessions: Gate,
    global: bool,
}

impl LakeLocks {
    pub(crate) fn new(catalog_connstr: String) -> Self {
        Self {
            catalog_connstr,
            budget: LOCK_BUDGET,
            sessions: Gate::new(LOCK_SESSIONS),
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
                "{what}: waited {} ms for a lake lock session and gave up",
                started.elapsed().as_millis()
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
            waited: vec![("session", started.elapsed())],
            _session: pass,
        })
    }
}

/// An open lock session. Its locks are released when it is dropped.
pub(crate) struct LockSession<'a> {
    locks: &'a LakeLocks,
    what: String,
    conn: Connection,
    deadline: Instant,
    waited: Vec<(&'static str, Duration)>,
    // Dropped after `drop` ends the connection's transaction, so the next waiter
    // gets the session once its locks are released.
    _session: GatePass<'a>,
}

impl LockSession<'_> {
    /// Hold `GLOBAL_KEY` exclusively, waiting until this session's deadline.
    pub(crate) fn lock_global(&mut self) -> Result<(), Status> {
        if !self.locks.global {
            return Ok(());
        }
        let started = Instant::now();
        set_lock_timeout(
            &self.conn,
            self.deadline.saturating_duration_since(Instant::now()),
        )
        .and_then(|()| advisory_lock(&self.conn, GLOBAL_KEY))
        .map_err(|e| {
            Status::unavailable(format!(
                "{}: waited {} ms for the global lock and gave up: {e}",
                self.what,
                started.elapsed().as_millis()
            ))
        })?;
        self.waited.push(("global", started.elapsed()));
        Ok(())
    }

    /// This session's backend, for a test that ends it.
    #[cfg(test)]
    pub(crate) fn backend_pid(&self) -> i32 {
        self.conn
            .query_row(
                "SELECT pid FROM postgres_query('lock_session', \
                 'SELECT pg_backend_pid() AS pid')",
                [],
                |row| row.get(0),
            )
            .unwrap()
    }

    /// The waits so far, as `name_wait_ms=N` pairs for a log line.
    pub(crate) fn waits(&self) -> String {
        self.waited
            .iter()
            .map(|(name, d)| format!("{name}_wait_ms={}", d.as_millis()))
            .collect::<Vec<_>>()
            .join(" ")
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
    fn a_new_arrival_queues_behind_a_waiter_for_a_freed_pass() {
        let gate = Gate::new(1);
        let held = gate.enter(Instant::now() + SOON).unwrap();
        let (entered, got_in) = std::sync::mpsc::channel();
        let (release, released) = std::sync::mpsc::channel::<()>();
        std::thread::scope(|scope| {
            // Dropped if an assertion below fails, which ends the waiter's hold.
            let release = release;
            let gate = &gate;
            scope.spawn(move || {
                let pass = gate.enter(Instant::now() + Duration::from_secs(5));
                entered.send(pass.is_some()).unwrap();
                let _ = released.recv();
            });
            std::thread::sleep(SOON);
            drop(held);
            assert!(
                gate.enter(Instant::now() + SOON).is_none(),
                "a new arrival took the pass ahead of the waiter"
            );
            assert!(got_in.recv().unwrap(), "the waiter did not get in");
            release.send(()).unwrap();
        });
    }

    #[test]
    fn a_waiter_that_gives_up_leaves_the_queue() {
        let gate = Gate::new(1);
        let held = gate.enter(Instant::now() + SOON).unwrap();
        assert!(gate.enter(Instant::now() + SOON).is_none());
        drop(held);
        assert!(
            gate.enter(Instant::now() + SOON).is_some(),
            "a waiter that gave up still holds a place in line"
        );
    }

    #[test]
    fn a_gate_waits_no_longer_than_a_past_deadline() {
        let gate = Gate::new(0);
        let started = Instant::now();
        assert!(gate.enter(started).is_none());
        assert!(started.elapsed() < SOON, "waited {:?}", started.elapsed());
    }
}
