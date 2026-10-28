//! Unit tests for [`super`].

use std::sync::Arc;

use arrow_flight::decode::{DecodedPayload, FlightDataDecoder};
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_ipc::writer::IpcWriteOptions;
use futures::stream::{self, StreamExt};

use super::*;

#[cfg(feature = "integration")]
use arrow_flight::flight_service_server::FlightService;
#[cfg(feature = "integration")]
use arrow_flight::{FlightData, Ticket};
#[cfg(feature = "integration")]
use tonic::Request;

#[cfg(feature = "integration")]
use crate::flight_service::query::build_query;
#[cfg(feature = "integration")]
use crate::flight_service::test_support::{doget_fixture, pooled_conn, FEATURE_TABLE_COLUMNS};
#[cfg(feature = "integration")]
use crate::flight_service::QiitaFlightService;
#[cfg(feature = "integration")]
use crate::{auth, ducklake};

// --- requested_ipc_codec (pure; no DuckDB) ---

fn metadata_with(value: &[u8]) -> tonic::metadata::MetadataMap {
    let mut map = tonic::metadata::MetadataMap::new();
    map.insert(
        IPC_COMPRESSION_HEADER,
        tonic::metadata::MetadataValue::try_from(value).expect("valid metadata value"),
    );
    map
}

#[test]
fn absent_compression_header_means_no_codec() {
    let map = tonic::metadata::MetadataMap::new();
    assert_eq!(requested_ipc_codec(&map).expect("absent is valid"), None);
}

#[test]
fn zstd_compression_header_selects_zstd() {
    let map = metadata_with(b"zstd");
    assert_eq!(
        requested_ipc_codec(&map).expect("zstd is accepted"),
        Some(CompressionType::ZSTD)
    );
}

#[test]
fn explicit_none_compression_header_means_no_codec() {
    let map = metadata_with(b"none");
    assert_eq!(requested_ipc_codec(&map).expect("none is accepted"), None);
}

/// `lz4` is in this list on purpose: it is rejected by decision, not by
/// omission, so a future reader does not "fix" the gap by accepting it.
#[test]
fn unknown_compression_header_is_rejected() {
    for value in [b"lz4".as_slice(), b"gzip", b"ZSTD", b"", b"zstd,none"] {
        let err = requested_ipc_codec(&metadata_with(value))
            .expect_err("unknown codec must be rejected, not ignored");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(
            err.message().contains("zstd") && err.message().contains("none"),
            "error should name the accepted values, got: {}",
            err.message()
        );
    }
}

/// A repeated header must be refused, not resolved to its first value.
///
/// `MetadataMap::get` returns only the first value of a repeated key, so
/// reading with it would apply `zstd` and silently ignore the `lz4` the client
/// also asked for — a client that requested an unsupported codec and got a
/// working stream anyway learns nothing. Both orders are exercised because
/// only one of them looks wrong under `get`.
#[test]
fn repeated_compression_header_is_rejected() {
    for pair in [["zstd", "lz4"], ["lz4", "zstd"], ["zstd", "zstd"]] {
        let mut map = tonic::metadata::MetadataMap::new();
        for value in pair {
            map.append(
                IPC_COMPRESSION_HEADER,
                tonic::metadata::MetadataValue::try_from(value).expect("valid value"),
            );
        }
        let err = requested_ipc_codec(&map)
            .expect_err("a repeated codec header is ambiguous and must be rejected");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(
            err.message().contains("more than once"),
            "error should say the header repeated, got: {}",
            err.message()
        );
    }
}

/// No codec must leave the encoder options structurally identical to the
/// default the uncompressed encoder used before this existed.
///
/// This is the mechanism behind "no header means today's behaviour byte for
/// byte": `do_get` always routes through `try_with_compression`, so the claim
/// rests on that call being a no-op when the codec is `None`. Compared through
/// `Debug` because `IpcWriteOptions` implements neither `PartialEq` nor field
/// accessors — the derived formatting covers every field, which is the
/// property being pinned.
#[test]
fn no_codec_write_options_match_the_encoder_default() {
    let with_none = IpcWriteOptions::default()
        .try_with_compression(None)
        .expect("None is always a valid codec");
    assert_eq!(
        format!("{with_none:?}"),
        format!("{:?}", IpcWriteOptions::default()),
        "an unrequested codec changed the write options"
    );
}

/// A value the transport accepts but that is not UTF-8 must be an error
/// rather than a panic or a silent fall-through to uncompressed.
#[test]
fn non_utf8_compression_header_is_rejected() {
    let mut map = tonic::metadata::MetadataMap::new();
    map.insert(
        IPC_COMPRESSION_HEADER,
        tonic::metadata::MetadataValue::try_from(b"\xff\xfe".as_slice())
            .expect("bytes are valid as an ASCII metadata value"),
    );
    let err = requested_ipc_codec(&map).expect_err("non-UTF-8 must be rejected");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

// The streaming DoGet helper: runs the query in streaming mode inside a
// blocking task and hands batches back over a bounded channel (do_get's body).
// Pins that it streams every row, preserves the UTINYINT[] -> List<UInt8>
// shape, and emits one empty schema batch for a zero-row result.
#[tokio::test]
#[serial_test::serial]
#[cfg(feature = "integration")]
async fn stream_ducklake_batches_streams_rows_and_empty_schema_branch() {
    use arrow_schema::DataType;

    let connstr = crate::test_catalog::connstr();
    let data_path = crate::test_catalog::data_path();

    let prep: i64 = 921_000;
    let mask: i64 = 921_001;
    let (s0, s1, s2) = (prep + 1, prep + 2, prep + 3);
    {
        let conn = Connection::open_in_memory().unwrap();
        ducklake::connect_ducklake(&conn, &connstr, &data_path).unwrap();
        ducklake::ensure_read_tables(&conn).unwrap();
        conn.execute_batch(&format!(
            "DELETE FROM qiita_lake.read WHERE prep_sample_idx = {prep};
             DELETE FROM qiita_lake.read_mask WHERE prep_sample_idx = {prep};
             INSERT INTO qiita_lake.read \
                 (prep_sample_idx, sequence_idx, read_id, sequence1, qual1, sequence2, qual2) VALUES \
                 ({prep}, {s0}, 'r0', 'ACGT', [5,6,7,8]::UTINYINT[], NULL, NULL), \
                 ({prep}, {s1}, 'r1', 'TTGG', [9,9,9,9]::UTINYINT[], NULL, NULL), \
                 ({prep}, {s2}, 'r2', 'CCAA', [3,3,3,3]::UTINYINT[], NULL, NULL);
             INSERT INTO qiita_lake.read_mask \
                 (mask_idx, prep_sample_idx, sequence_idx, reason, left_trim1, right_trim1) VALUES \
                 ({mask}, {prep}, {s0}, 'pass', 0, 0), \
                 ({mask}, {prep}, {s1}, 'pass', 0, 0), \
                 ({mask}, {prep}, {s2}, 'pass', 0, 0);"
        ))
        .unwrap();
    }

    let mut filter = auth::TicketFilter::new();
    filter.insert("mask_idx".to_string(), vec![serde_json::Value::from(mask)]);
    filter.insert(
        "prep_sample_idx".to_string(),
        vec![serde_json::Value::from(prep)],
    );
    let (sql, table) = build_query("read_masked", &filter, &[], &[]).unwrap();
    let batches: Vec<arrow_array::RecordBatch> =
        stream_ducklake_batches(pooled_conn(&connstr, &data_path), sql, table)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(|r| r.expect("stream item should be Ok"))
            .collect();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3, "all three pass reads should stream through");
    let qual1 = batches[0]
        .schema()
        .field_with_name("qual1")
        .unwrap()
        .data_type()
        .clone();
    match qual1 {
        DataType::List(item) | DataType::LargeList(item) => {
            assert_eq!(
                item.data_type(),
                &DataType::UInt8,
                "qual1 must be List<UInt8>"
            )
        }
        other => panic!("qual1 should be an Arrow List, got: {other:?}"),
    }

    // Empty-result branch: one zero-row batch carrying the schema.
    let mut empty_filter = auth::TicketFilter::new();
    empty_filter.insert(
        "mask_idx".to_string(),
        vec![serde_json::Value::from(mask + 999_999)],
    );
    empty_filter.insert(
        "prep_sample_idx".to_string(),
        vec![serde_json::Value::from(prep)],
    );
    let (esql, etable) = build_query("read_masked", &empty_filter, &[], &[]).unwrap();
    let empty: Vec<arrow_array::RecordBatch> =
        stream_ducklake_batches(pooled_conn(&connstr, &data_path), esql, etable)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(|r| r.expect("empty stream item should be Ok"))
            .collect();
    assert_eq!(empty.len(), 1, "empty result still yields one schema batch");
    assert_eq!(empty[0].num_rows(), 0, "the schema batch has no rows");
    assert!(
        empty[0].schema().field_with_name("qual1").is_ok(),
        "empty batch carries the full read_masked schema"
    );

    let conn = Connection::open_in_memory().unwrap();
    ducklake::connect_ducklake(&conn, &connstr, &data_path).unwrap();
    let _ = conn.execute_batch(&format!(
        "DELETE FROM qiita_lake.read WHERE prep_sample_idx = {prep};
         DELETE FROM qiita_lake.read_mask WHERE prep_sample_idx = {prep};"
    ));
}

// A producer-side error (here, a query against a missing table) must surface
// as a single Err stream item — never a silently-truncated empty stream.
#[tokio::test]
#[serial_test::serial]
#[cfg(feature = "integration")]
async fn stream_ducklake_batches_propagates_query_error() {
    let connstr = crate::test_catalog::connstr();
    let data_path = crate::test_catalog::data_path();
    let items: Vec<_> = stream_ducklake_batches(
        pooled_conn(&connstr, &data_path),
        "SELECT * FROM qiita_lake.does_not_exist_table".to_string(),
        "qiita_lake.does_not_exist_table".to_string(),
    )
    .collect::<Vec<_>>()
    .await;
    assert_eq!(items.len(), 1, "a producer error yields exactly one item");
    assert!(
        items[0].is_err(),
        "the item must be an Err, not a silent empty stream"
    );
}

/// Run `send_query_batches` on a fresh single-threaded in-memory connection,
/// draining its channel as a client would. Returns the batches received and the
/// helper's own result.
fn drain_send_query_batches(sql: &'static str) -> (Vec<RecordBatch>, Result<(), Status>) {
    let (tx, mut rx) = tokio::sync::mpsc::channel(DOGET_BATCH_CHANNEL_DEPTH);
    let producer = std::thread::spawn(move || {
        let conn = Connection::open_in_memory().unwrap();
        // One thread, so chunks are produced in row order and a late row's
        // failure cannot surface before the earlier chunks are fetched.
        conn.execute_batch("SET threads = 1").unwrap();
        send_query_batches(&conn, sql, "probe", &tx)
    });
    let mut batches = Vec::new();
    while let Some(item) = rx.blocking_recv() {
        batches.push(item.expect("the helper sends only Ok items; errors are its return value"));
    }
    (
        batches,
        producer.join().expect("the producer must not panic"),
    )
}

// A chunk that fails to fetch after batches have gone out must come back as an
// error, so the client never mistakes a truncated result for a whole one. The
// failing row is the last of 2M: DuckDB buffers only ~1 MiB of a streaming
// result ahead of the consumer, so the earlier chunks are fetched first.
#[test]
fn send_query_batches_surfaces_a_mid_stream_failure() {
    let (batches, result) = drain_send_query_batches(
        "SELECT CASE WHEN i < 1999999 THEN i ELSE error('mid-stream boom') END AS v \
         FROM range(2000000) t(i)",
    );
    assert!(
        !batches.is_empty(),
        "the failure must come after some batches, or this does not test mid-stream"
    );
    let status = result.expect_err("a mid-stream failure must be an error, not a clean end");
    assert!(
        status.message().contains("mid-stream boom"),
        "the error names the cause: {}",
        status.message()
    );
    assert!(
        status.message().contains("query failed mid-stream"),
        "the error says the failure came after batches went out: {}",
        status.message()
    );
}

// A panic in the producer must not end the stream as if the result were whole:
// the batches already sent stay sent, and an error follows them.
#[test]
fn run_producer_turns_a_panic_into_a_trailing_error() {
    let (tx, mut rx) = tokio::sync::mpsc::channel(DOGET_BATCH_CHANNEL_DEPTH);
    let batch = RecordBatch::try_from_iter([(
        "v",
        Arc::new(arrow_array::Int64Array::from(vec![1])) as arrow_array::ArrayRef,
    )])
    .unwrap();
    run_producer(&tx, || {
        tx.blocking_send(Ok(batch)).unwrap();
        panic!("producer boom");
    });
    drop(tx);
    assert!(rx.blocking_recv().expect("the batch").is_ok());
    let err = rx
        .blocking_recv()
        .expect("a trailing item, not a clean end")
        .expect_err("the trailing item is an error");
    assert!(err.to_string().contains("producer boom"), "{err}");
    assert!(rx.blocking_recv().is_none());
}

// The trailing `Err` that `stream_ducklake_batches` sends after a mid-stream
// failure must reach the client after the batches before it, not be dropped by the
// Flight encoder, and not cost the client those batches either.
#[tokio::test]
async fn flight_encoder_delivers_batches_then_a_trailing_error() {
    let batch = RecordBatch::try_from_iter([(
        "v",
        Arc::new(arrow_array::Int64Array::from(vec![1, 2])) as arrow_array::ArrayRef,
    )])
    .unwrap();
    let items: Vec<Result<RecordBatch, FlightError>> = vec![
        Ok(batch),
        Err(FlightError::ExternalError(Box::new(std::io::Error::other(
            "mid-stream boom",
        )))),
    ];
    let mut decoded =
        FlightDataDecoder::new(FlightDataEncoderBuilder::new().build(stream::iter(items)));
    let mut rows = 0;
    let err = loop {
        match decoded.next().await {
            Some(Ok(msg)) => {
                if let DecodedPayload::RecordBatch(b) = msg.payload {
                    rows += b.num_rows();
                }
            }
            Some(Err(e)) => break e,
            None => panic!("the stream ended cleanly; the trailing error was lost"),
        }
    };
    assert_eq!(rows, 2, "the batches before the error reach the client");
    assert!(err.to_string().contains("mid-stream boom"), "{err}");
}

// A zero-row result still yields exactly one empty batch carrying the schema.
#[test]
fn send_query_batches_emits_one_schema_batch_for_an_empty_result() {
    let (batches, result) = drain_send_query_batches("SELECT 1::BIGINT AS a, 'x' AS b WHERE false");
    result.expect("an empty result is not an error");
    assert_eq!(batches.len(), 1, "one schema batch");
    assert_eq!(batches[0].num_rows(), 0);
    let names: Vec<_> = batches[0]
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    assert_eq!(names, ["a", "b"]);
}

// ------------------------------------------------------------------
// DoGet IPC compression — the parser being right does not prove the encoder
// honoured it, so these drive the real `do_get` end to end and assert on
// what the encoder stamped into each record-batch message.
// ------------------------------------------------------------------

/// Collect a DoGet response, returning the codec stamped into each
/// record-batch message and the total payload size.
///
/// Reads the codec back out of each message's IPC header rather than
/// trusting that the request was honoured — a codec the encoder silently
/// declined to apply would otherwise pass every test below.
/// Returns the per-message codec stamps, the total body size, and every
/// message's bytes (header ‖ body, concatenated) so a caller can compare two
/// streams for real byte-identity rather than for matching stamps.
#[cfg(feature = "integration")]
async fn doget_codecs(
    service: &QiitaFlightService,
    ticket: Vec<u8>,
    header: Option<&str>,
) -> Result<(Vec<Option<CompressionType>>, usize, Vec<u8>), Status> {
    let mut request = Request::new(Ticket {
        ticket: ticket.into(),
    });
    if let Some(value) = header {
        request.metadata_mut().insert(
            IPC_COMPRESSION_HEADER,
            tonic::metadata::MetadataValue::try_from(value).unwrap(),
        );
    }
    let response = service.do_get(request).await?;
    let messages: Vec<FlightData> = response
        .into_inner()
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<_, Status>>()?;
    let bytes = messages.iter().map(|m| m.data_body.len()).sum();
    let codecs = messages
        .iter()
        .filter_map(|m| {
            Some(
                arrow_ipc::root_as_message(&m.data_header)
                    .ok()?
                    .header_as_record_batch()?
                    .compression()
                    .map(|c| c.codec()),
            )
        })
        .collect();
    let raw = messages
        .iter()
        .flat_map(|m| m.data_header.iter().chain(m.data_body.iter()).copied())
        .collect();
    Ok((codecs, bytes, raw))
}

#[tokio::test]
#[serial_test::serial]
#[cfg(feature = "integration")]
async fn doget_with_zstd_header_stamps_the_codec_into_every_batch_message() {
    let (service, ticket, _tmp) = doget_fixture(977_000, 977_010, FEATURE_TABLE_COLUMNS);
    let (codecs, compressed, _) = doget_codecs(&service, ticket.clone(), Some("zstd"))
        .await
        .expect("zstd DoGet should succeed");

    assert!(!codecs.is_empty(), "no record-batch messages in the stream");
    assert!(
        codecs.iter().all(|c| *c == Some(CompressionType::ZSTD)),
        "a batch message shipped uncompressed despite the header: {codecs:?}"
    );

    // The codec stamp alone would be satisfied by a codec that ran and
    // achieved nothing; these rows are highly repetitive, so real
    // compression must show up in the body.
    let (_, raw, _) = doget_codecs(&service, ticket, None)
        .await
        .expect("uncompressed DoGet should succeed");
    assert!(
        compressed * 2 < raw,
        "expected zstd to at least halve the body, got {compressed} vs {raw} bytes"
    );
}

/// The regression guard for every existing client: neither way of asking for
/// no compression may change the stream.
///
/// Asserts two things the codec stamp alone does not. Every message comes back
/// unstamped, AND the two streams are byte-for-byte equal — an absent header
/// and an explicit `none` are the same stream, not merely two streams that
/// both claim to be uncompressed. What neither can observe is equality with
/// the encoder that preceded this change, which is not reachable from here;
/// `no_codec_write_options_match_the_encoder_default` pins that instead.
#[tokio::test]
#[serial_test::serial]
#[cfg(feature = "integration")]
async fn doget_without_the_header_streams_exactly_what_an_explicit_none_does() {
    let (service, ticket, _tmp) = doget_fixture(977_100, 977_110, FEATURE_TABLE_COLUMNS);
    let mut streams = Vec::new();
    for header in [None, Some("none")] {
        let (codecs, _, raw) = doget_codecs(&service, ticket.clone(), header)
            .await
            .unwrap_or_else(|e| panic!("DoGet with header {header:?} failed: {e}"));
        assert!(!codecs.is_empty(), "{header:?}: no record-batch messages");
        assert!(
            codecs.iter().all(Option::is_none),
            "{header:?} must leave the body uncompressed, got {codecs:?}"
        );
        streams.push(raw);
    }
    assert_eq!(
        streams[0], streams[1],
        "an absent header and an explicit `none` produced different bytes"
    );
}

#[tokio::test]
#[serial_test::serial]
#[cfg(feature = "integration")]
async fn doget_with_an_unsupported_codec_is_rejected_before_streaming() {
    let (service, ticket, _tmp) = doget_fixture(977_200, 977_210, FEATURE_TABLE_COLUMNS);
    let err = doget_codecs(&service, ticket, Some("lz4"))
        .await
        .expect_err("lz4 must be rejected, not silently ignored");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}
