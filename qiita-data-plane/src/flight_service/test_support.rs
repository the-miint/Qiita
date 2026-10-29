//! Fixtures and helpers shared by the `flight_service` test modules.

use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use ed25519_dalek::{Signer, SigningKey, VerifyingKey};

use super::*;

// The pool owns attaching DuckLake, so the table-creation helpers these tests
// call are imported here rather than inherited through `flight_service`; only the
// catalog-backed tests use them.
#[cfg(feature = "integration")]
use crate::ducklake;

pub(super) fn filter_of(pairs: &[(&str, Vec<serde_json::Value>)]) -> auth::TicketFilter {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect()
}

// --- delete_reference integration harness ---

/// Lay down every lake table the data plane creates at boot, through the same
/// `ducklake::ensure_lake_tables` boot runs.
///
/// The catalog-shape tests below decide what belongs in a registry by QUERYING
/// the catalog, so each one is only as complete as the tables its connection
/// happens to hold: a table that function does not create is invisible to the
/// shape query and joins no registry, silently.
#[cfg(feature = "integration")]
pub(super) fn setup_full_lake(conn: &duckdb::Connection) {
    ducklake::ensure_lake_tables(conn).unwrap();
}

/// A TCP proxy in front of the test catalog's Postgres. Once a client message on
/// a connection names `ducklake_snapshot` (DuckLake's commit writes a snapshot
/// row), the proxy drops Postgres's answer to the next COMMIT on it and closes
/// that connection, so
/// the commit is in the catalog and the client is told it failed. Returns the
/// connection string through the proxy, and whether it has cut a connection.
/// Only the first such COMMIT is cut.
#[cfg(feature = "integration")]
pub(super) fn catalog_that_loses_a_commit_answer(
    connstr: &str,
) -> (String, std::sync::Arc<std::sync::atomic::AtomicBool>) {
    use std::io::{Read, Write as _};
    use std::sync::atomic::{AtomicBool, Ordering};

    let field = |key: &str| {
        connstr
            .split_whitespace()
            .find_map(|kv| kv.strip_prefix(&format!("{key}=")))
            .map(str::to_string)
    };
    let upstream = format!(
        "{}:{}",
        field("host").unwrap_or_else(|| "localhost".into()),
        field("port").unwrap_or_else(|| "5432".into())
    );
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let proxied = connstr
        .split_whitespace()
        .filter(|kv| {
            !["host=", "port=", "sslmode=", "gssencmode="]
                .iter()
                .any(|key| kv.starts_with(key))
        })
        .chain(["host=127.0.0.1", "sslmode=disable", "gssencmode=disable"])
        .collect::<Vec<_>>()
        .join(" ")
        + &format!(" port={port}");
    let cut = Arc::new(AtomicBool::new(false));

    // Complete protocol messages off the front of `buf`. Every message is typed
    // except the client's first (startup), which `untyped` names.
    fn split(buf: &mut Vec<u8>, untyped: &mut bool) -> Vec<Vec<u8>> {
        let mut out = Vec::new();
        loop {
            let (start, len) = if *untyped { (0, 4) } else { (1, 5) };
            if buf.len() < len {
                break;
            }
            let n = u32::from_be_bytes(buf[start..start + 4].try_into().unwrap()) as usize + start;
            if buf.len() < n {
                break;
            }
            out.push(buf.drain(..n).collect());
            *untyped = false;
        }
        out
    }

    let cut_flag = cut.clone();
    std::thread::spawn(move || {
        for client in listener.incoming() {
            let Ok(client) = client else { return };
            let Ok(server) = std::net::TcpStream::connect(&upstream) else {
                return;
            };
            let armed = Arc::new(AtomicBool::new(false));
            let (mut c_read, mut c_write) = (client.try_clone().unwrap(), client);
            let (mut s_read, mut s_write) = (server.try_clone().unwrap(), server);
            let client_armed = armed.clone();
            std::thread::spawn(move || {
                let (mut buf, mut untyped, mut chunk) = (Vec::new(), true, [0u8; 65536]);
                while let Ok(n @ 1..) = c_read.read(&mut chunk) {
                    buf.extend_from_slice(&chunk[..n]);
                    for m in split(&mut buf, &mut untyped) {
                        if m.windows(17).any(|w| w == b"ducklake_snapshot") {
                            client_armed.store(true, Ordering::SeqCst);
                        }
                        if s_write.write_all(&m).is_err() {
                            return;
                        }
                    }
                }
                let _ = s_write.shutdown(std::net::Shutdown::Both);
            });
            let cut_flag = cut_flag.clone();
            std::thread::spawn(move || {
                let (mut buf, mut untyped, mut chunk) = (Vec::new(), false, [0u8; 65536]);
                while let Ok(n @ 1..) = s_read.read(&mut chunk) {
                    buf.extend_from_slice(&chunk[..n]);
                    for m in split(&mut buf, &mut untyped) {
                        if armed.load(Ordering::SeqCst)
                            && m[0] == b'C'
                            && &m[5..] == b"COMMIT\0"
                            && !cut_flag.swap(true, Ordering::SeqCst)
                        {
                            let _ = c_write.shutdown(std::net::Shutdown::Both);
                            let _ = s_read.shutdown(std::net::Shutdown::Both);
                            return;
                        }
                        if c_write.write_all(&m).is_err() {
                            return;
                        }
                    }
                }
                let _ = c_write.shutdown(std::net::Shutdown::Both);
            });
        }
    });
    (proxied, cut)
}

pub(super) fn columns(names: &[&str]) -> Vec<String> {
    names.iter().map(|s| s.to_string()).collect()
}

// Fixed test keypair; WRONG_SEED signs tickets that must NOT verify.
pub(super) const TEST_SEED: [u8; 32] = [7u8; 32];
pub(super) const WRONG_SEED: [u8; 32] = [9u8; 32];

fn test_vk() -> VerifyingKey {
    SigningKey::from_bytes(&TEST_SEED).verifying_key()
}

pub(super) fn sign_doput_for_test(upload_idx: i64, seed: &[u8; 32], expiry: u64) -> Vec<u8> {
    let payload = format!(r#"{{"action":"doput","upload_idx":{upload_idx}}}"#);
    sign_raw(payload.as_bytes(), seed, expiry)
}

pub(super) fn sign_raw(payload: &[u8], seed: &[u8; 32], expiry: u64) -> Vec<u8> {
    let version: u8 = 2;
    let payload_len = (payload.len() as u32).to_be_bytes();
    let expiry_bytes = expiry.to_be_bytes();
    let signed_input = [&[version][..], &payload_len[..], payload, &expiry_bytes[..]].concat();
    let sig = SigningKey::from_bytes(seed).sign(&signed_input).to_bytes();
    let mut ticket = Vec::new();
    ticket.push(version);
    ticket.extend_from_slice(&payload_len);
    ticket.extend_from_slice(payload);
    ticket.extend_from_slice(&sig);
    ticket.extend_from_slice(&expiry_bytes);
    ticket
}

pub(super) fn future_expiry_secs(secs: u64) -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + secs
}

/// Build a tiny test RecordBatch — schema is arbitrary, DoPut is content-agnostic.
pub(super) fn sample_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("read_id", DataType::Utf8, false),
        Field::new("seq_length", DataType::Int64, false),
    ]));
    let read_ids = Arc::new(StringArray::from(vec!["r1", "r2", "r3"]));
    let lengths = Arc::new(Int64Array::from(vec![12i64, 34, 56]));
    RecordBatch::try_new(schema, vec![read_ids, lengths]).unwrap()
}

/// Convert one or more RecordBatches into a Flight stream stamped with
/// the supplied ticket on the first message's FlightDescriptor.cmd.
pub(super) async fn flight_stream_with_ticket(
    batches: Vec<RecordBatch>,
    ticket: Vec<u8>,
) -> Vec<Result<FlightData, Status>> {
    let batch_stream = stream::iter(
        batches
            .into_iter()
            .map(Ok::<_, arrow_flight::error::FlightError>),
    );
    let mut flight_data: Vec<FlightData> = FlightDataEncoderBuilder::new()
        .build(batch_stream)
        .filter_map(|r| async move { r.ok() })
        .collect()
        .await;
    // Stamp the ticket onto the first message's descriptor — pyarrow's
    // client does the equivalent via FlightDescriptor.for_command.
    let mut first = flight_data.remove(0);
    first.flight_descriptor = Some(FlightDescriptor::new_cmd(ticket));
    let mut out = vec![Ok(first)];
    out.extend(flight_data.into_iter().map(Ok));
    out
}

pub(super) fn make_service(staging_root: PathBuf) -> QiitaFlightService {
    // DoPut tests don't exercise export_read; any scratch root works here.
    let scratch_root = staging_root
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| staging_root.clone());
    QiitaFlightService::new(
        test_vk(),
        // The DoPut path borrows no instance, so a pool with nothing attached
        // is enough here; a test that reaches the catalog builds a real one.
        crate::pool::LakePool::unattached_for_tests(crate::pool::PoolSettings::for_tests(
            1,
            std::env::temp_dir().join("qiita-dp-test-spill"),
        )),
        // Two upload permits: enough that a DoPut test is never refused, small
        // enough that `do_put_past_its_bound_is_refused_with_a_retriable_status`
        // can exhaust them.
        2,
        staging_root,
        scratch_root,
    )
}

/// A DuckDB connection with no DuckLake attached, for the validation paths that
/// must reject their input before any catalog is touched.
fn unattached_conn() -> duckdb::Connection {
    duckdb::Connection::open_in_memory().expect("open in-memory DuckDB")
}

/// A real pool over the test catalog. Four instances rather than one so a caller
/// that borrows more than once from the same pool is not refused by the bound.
#[cfg(feature = "integration")]
fn test_pool(connstr: &str, data_path: &str) -> std::sync::Arc<crate::pool::LakePool> {
    crate::pool::LakePool::open(
        connstr.to_string(),
        data_path.to_string(),
        crate::pool::PoolSettings::for_tests(4, std::env::temp_dir().join("qiita-dp-test-spill")),
    )
    .expect("open a test pool")
}

/// An instance borrowed from such a pool, for the paths that take what
/// `do_get` takes.
#[cfg(feature = "integration")]
pub(super) fn pooled_conn(connstr: &str, data_path: &str) -> crate::pool::PooledConnection {
    test_pool(connstr, data_path)
        .checkout()
        .expect("a fresh pool lends its first instance")
}

/// A connection with the test catalog attached, built directly so a test needs
/// no pool. It carries none of the pool's resource settings — only `LakePool`
/// applies those — so it stands in for the attach, not for a pooled instance.
#[cfg(feature = "integration")]
pub(super) fn attached_conn(connstr: &str, data_path: &str) -> duckdb::Connection {
    let conn = unattached_conn();
    ducklake::connect_ducklake(&conn, connstr, data_path).expect("attach DuckLake");
    conn
}

/// `attached_conn`, set up the way the pool lends a writing DoAction its
/// connection.
#[cfg(feature = "integration")]
pub(super) fn attached_writer(connstr: &str, data_path: &str) -> crate::pool::LakeWriter {
    crate::pool::LakeWriter::for_tests(attached_conn(connstr, data_path))
}

pub(super) fn unattached_writer() -> crate::pool::LakeWriter {
    crate::pool::LakeWriter::for_tests(unattached_conn())
}

/// Seed alignment rows and return a signed `alignment_visible` ticket for
/// them projecting `cols`, plus a service wired to the same catalog.
///
/// Every row carries a `cigar` — the wide column the projection exists to
/// keep off the wire — so a test can prove both that asking for it delivers
/// it and that not asking for it costs nothing.
///
/// The returned `TempDir` owns the service's staging and scratch roots and
/// must stay bound for the test's lifetime — dropping it removes the
/// directory. `do_get` reaches neither root today (staging belongs to DoPut,
/// scratch to the export DoActions), but they are real, TMPDIR-resident and
/// self-cleaning rather than `/tmp` literals, so a DoGet that later does
/// write cannot litter a shared directory.
#[cfg(feature = "integration")]
pub(super) fn doget_fixture(
    align: i64,
    prep: i64,
    cols: &[&str],
) -> (QiitaFlightService, Vec<u8>, tempfile::TempDir) {
    let connstr = crate::test_catalog::connstr();
    let data_path = crate::test_catalog::data_path();
    {
        let conn = Connection::open_in_memory().unwrap();
        ducklake::connect_ducklake(&conn, &connstr, &data_path).unwrap();
        ducklake::ensure_reference_tables(&conn).unwrap();
        ducklake::ensure_alignment_tables(&conn).unwrap();
        ducklake::ensure_exclusion_tables(&conn).unwrap();
        // Enough rows that a compressed body is unambiguously smaller than a
        // raw one; a handful would be dominated by framing.
        conn.execute_batch(&format!(
            "DELETE FROM qiita_lake.alignment WHERE alignment_idx = {align};
             INSERT INTO qiita_lake.alignment \
                 (alignment_idx, prep_sample_idx, sequence_idx, feature_idx, \
                  flags, position, stop_position, cigar) \
             SELECT {align}, {prep}, i, 1, 0, 100, 200, '100M' FROM range(20000) t(i);"
        ))
        .unwrap();
    }
    let quoted = cols
        .iter()
        .map(|c| format!("\"{c}\""))
        .collect::<Vec<_>>()
        .join(",");
    let payload = format!(
        r#"{{"table":"alignment_visible","filter":{{"alignment_idx":[{align}],"prep_sample_idx":[{prep}]}},"columns":[{quoted}]}}"#
    );
    let ticket = sign_raw(payload.as_bytes(), &TEST_SEED, future_expiry_secs(300));
    let tmp = tempfile::tempdir().unwrap();
    let staging = tmp.path().join("staging");
    std::fs::create_dir_all(&staging).unwrap();
    let service = QiitaFlightService::new(
        test_vk(),
        test_pool(&connstr, &data_path),
        2,
        staging,
        tmp.path().to_path_buf(),
    );
    (service, ticket, tmp)
}

/// The projection the feature-table consumer signs. Mirrors
/// `_ALIGNMENT_COLUMNS` in `estimate_feature_table.py`; used by the
/// compression tests, which care about the stream, not the column set.
#[cfg(feature = "integration")]
pub(super) const FEATURE_TABLE_COLUMNS: &[&str] = &[
    "prep_sample_idx",
    "sequence_idx",
    "feature_idx",
    "flags",
    "position",
    "stop_position",
];
