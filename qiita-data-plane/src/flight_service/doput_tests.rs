//! Unit tests for [`super`].

use std::os::unix::fs::PermissionsExt;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow_flight::FlightDescriptor;
use duckdb::Connection;

use super::*;
use crate::flight_service::test_support::{
    flight_stream_with_ticket, future_expiry_secs, make_service, sample_batch, sign_doput_for_test,
    TEST_SEED, WRONG_SEED,
};

// ------------------------------------------------------------------
// DoPut handler tests
// ------------------------------------------------------------------

/// A DoPut borrows no pooled connection, so the pool does not bound it; its own
/// permit does. Measured against a real data plane, an upload's cost is linear in
/// concurrency and grows with batch size, so without this the ceiling was whatever
/// the edge admitted per client address.
///
/// The refusal carries the same "data plane busy" prefix as the pool's, which is
/// what the shared classification in `qiita_common.data_plane_errors` keys on —
/// asserted here so the two surfaces cannot drift into wordings only one of them
/// classifies.
#[tokio::test]
async fn do_put_past_its_bound_is_refused_with_a_retriable_status() {
    let tmp = tempfile::tempdir().unwrap();
    // `make_service` grants two permits.
    let service = Arc::new(make_service(tmp.path().to_path_buf()));

    // Two uploads that never finish: each sends its descriptor-bearing first
    // message and then holds its stream open, so it holds its permit while the
    // third upload arrives.
    let mut senders = Vec::new();
    let mut running = Vec::new();
    for i in 0..2 {
        let ticket = sign_doput_for_test(900 + i, &TEST_SEED, future_expiry_secs(300));
        let first = flight_stream_with_ticket(vec![sample_batch()], ticket).await;
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<FlightData, Status>>(1);
        tx.send(first[0].clone()).await.unwrap();
        let svc = Arc::clone(&service);
        running.push(tokio::spawn(async move {
            svc.do_put_inner(tokio_stream::wrappers::ReceiverStream::new(rx))
                .await
        }));
        senders.push(tx);
    }
    // Both permits taken before the third attempt. Waiting on the count rather
    // than on a duration: a sleep that ends early admits the third upload and
    // fails the assertion below for a reason that is not the bound.
    for _ in 0..1_000 {
        if service.doput_permits.available_permits() == 0 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert_eq!(
        service.doput_permits.available_permits(),
        0,
        "both held uploads must hold their permits before the third arrives"
    );

    let ticket = sign_doput_for_test(999, &TEST_SEED, future_expiry_secs(300));
    let messages = flight_stream_with_ticket(vec![sample_batch()], ticket).await;
    let err = service
        .do_put_inner(stream::iter(messages))
        .await
        .expect_err("a third upload must be refused while two hold the permits");

    assert_eq!(err.code(), tonic::Code::ResourceExhausted);
    assert!(
        err.message().contains("data plane busy"),
        "the refusal must carry the prefix both services classify on: {}",
        err.message()
    );
    assert!(
        err.message().contains("all 2 concurrent uploads"),
        "the refusal names the bound it hit, not what is left: {}",
        err.message()
    );

    // Closing the senders ends the held uploads, which returns their permits.
    drop(senders);
    for task in running {
        let _ = task.await;
    }
    let ticket = sign_doput_for_test(1000, &TEST_SEED, future_expiry_secs(300));
    let messages = flight_stream_with_ticket(vec![sample_batch()], ticket).await;
    service
        .do_put_inner(stream::iter(messages))
        .await
        .expect("a permit returns when its upload ends");
}

#[tokio::test]
async fn do_put_writes_arrow_stream_to_parquet() {
    let tmp = tempfile::tempdir().unwrap();
    let service = make_service(tmp.path().to_path_buf());

    let ticket = sign_doput_for_test(42, &TEST_SEED, future_expiry_secs(300));
    let messages = flight_stream_with_ticket(vec![sample_batch()], ticket).await;

    let result = service
        .do_put_inner(stream::iter(messages))
        .await
        .expect("do_put should succeed on a well-formed stream");

    let staged = tmp.path().join("uploads/42/upload.parquet");
    assert!(staged.exists(), "staging file not written");

    // File mode is 440 (owner+group read, no write, no world)
    let perms = std::fs::metadata(&staged).unwrap().permissions();
    assert_eq!(perms.mode() & 0o777, 0o440);

    // PutResult body carries sha256/row_count/bytes/upload_idx — and
    // deliberately NOT staging_path. Clients are not allowed to learn
    // server-side paths (the architecture commitment); the layout is
    // derivable from root + upload_idx by parties that legitimately
    // need it (CP, DP), but the client is not one of those.
    let body: serde_json::Value = serde_json::from_slice(&result.app_metadata).unwrap();
    assert_eq!(body["upload_idx"], 42);
    assert_eq!(body["row_count"], 3);
    assert!(
        body.get("staging_path").is_none(),
        "staging_path must not leak to the client"
    );
    let claimed_sha = body["sha256"].as_str().unwrap();
    let claimed_bytes = body["bytes_received"].as_u64().unwrap();

    // Recompute sha256 + size of the actual file, verify the PutResult
    // claim matches byte-for-byte.
    let actual_bytes = std::fs::metadata(&staged).unwrap().len();
    assert_eq!(claimed_bytes, actual_bytes);
    let file_bytes = std::fs::read(&staged).unwrap();
    let mut hasher = Sha256::new();
    hasher.update(&file_bytes);
    let actual_sha: String = hasher
        .finalize()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    assert_eq!(claimed_sha, actual_sha);
}

#[tokio::test]
async fn do_put_rejects_expired_ticket() {
    let tmp = tempfile::tempdir().unwrap();
    let service = make_service(tmp.path().to_path_buf());
    let expired = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        - 1000;
    let ticket = sign_doput_for_test(1, &TEST_SEED, expired);
    let messages = flight_stream_with_ticket(vec![sample_batch()], ticket).await;

    let err = service
        .do_put_inner(stream::iter(messages))
        .await
        .expect_err("expired ticket must be rejected");
    assert_eq!(err.code(), tonic::Code::Unauthenticated);
}

#[tokio::test]
async fn do_put_rejects_bad_signature() {
    let tmp = tempfile::tempdir().unwrap();
    let service = make_service(tmp.path().to_path_buf());
    // Sign with a different secret than the service holds.
    let ticket = sign_doput_for_test(1, &WRONG_SEED, future_expiry_secs(300));
    let messages = flight_stream_with_ticket(vec![sample_batch()], ticket).await;

    let err = service
        .do_put_inner(stream::iter(messages))
        .await
        .expect_err("bad signature must be rejected");
    assert_eq!(err.code(), tonic::Code::Unauthenticated);
}

#[tokio::test]
async fn do_put_rejects_missing_descriptor() {
    let tmp = tempfile::tempdir().unwrap();
    let service = make_service(tmp.path().to_path_buf());
    // A stream whose first message has no descriptor at all.
    let messages: Vec<Result<FlightData, Status>> = vec![Ok(FlightData::default())];

    let err = service
        .do_put_inner(stream::iter(messages))
        .await
        .expect_err("missing descriptor must be rejected");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn do_put_rejects_empty_cmd() {
    let tmp = tempfile::tempdir().unwrap();
    let service = make_service(tmp.path().to_path_buf());
    let fd = FlightData {
        flight_descriptor: Some(FlightDescriptor::new_cmd(Vec::<u8>::new())),
        ..Default::default()
    };
    let messages: Vec<Result<FlightData, Status>> = vec![Ok(fd)];

    let err = service
        .do_put_inner(stream::iter(messages))
        .await
        .expect_err("empty cmd must be rejected");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn do_put_interrupted_stream_leaves_no_parquet() {
    let tmp = tempfile::tempdir().unwrap();
    let service = make_service(tmp.path().to_path_buf());
    let ticket = sign_doput_for_test(99, &TEST_SEED, future_expiry_secs(300));

    // Build a valid first message (descriptor + schema), then yield an
    // Err mid-stream before any batch lands. The handler should
    // surface the error AND leave nothing in the staging directory.
    let mut messages = flight_stream_with_ticket(vec![sample_batch()], ticket).await;
    // Truncate to schema only, then inject an error.
    messages.truncate(1);
    messages.push(Err(Status::internal("simulated mid-stream drop")));

    let err = service
        .do_put_inner(stream::iter(messages))
        .await
        .expect_err("interrupted stream must surface an error");
    assert_eq!(err.code(), tonic::Code::Internal);

    for leftover in [
        "uploads/99/upload.parquet",
        "uploads/99/upload.parquet.partial",
    ] {
        let staged = tmp.path().join(leftover);
        assert!(
            !staged.exists(),
            "an interrupted upload must leave nothing; found {}",
            staged.display()
        );
    }
}

#[tokio::test]
async fn do_put_same_upload_idx_second_attempt_rejected() {
    // After a successful DoPut to upload_idx=N, a second DoPut to the
    // same N must fail with AlreadyExists rather than silently
    // clobbering the staged file. The CP doesn't reissue tickets, but
    // a malicious / buggy client could replay a still-valid one.
    let tmp = tempfile::tempdir().unwrap();
    let service = make_service(tmp.path().to_path_buf());

    let ticket = sign_doput_for_test(7, &TEST_SEED, future_expiry_secs(300));
    let m1 = flight_stream_with_ticket(vec![sample_batch()], ticket.clone()).await;
    service
        .do_put_inner(stream::iter(m1))
        .await
        .expect("first DoPut should succeed");

    let m2 = flight_stream_with_ticket(vec![sample_batch()], ticket).await;
    let err = service
        .do_put_inner(stream::iter(m2))
        .await
        .expect_err("second DoPut to the same upload_idx must be rejected");
    assert_eq!(err.code(), tonic::Code::AlreadyExists);

    // The first DoPut's file survives (still mode 440); it was not
    // clobbered by the failed second attempt.
    let staged = tmp.path().join("uploads/7/upload.parquet");
    let perms = std::fs::metadata(&staged).unwrap().permissions();
    assert_eq!(perms.mode() & 0o777, 0o440);
}

#[tokio::test]
async fn do_put_alreadyexists_wins_over_mid_stream_decode_error() {
    // Regression: with the async-decoder/blocking-writer bridge, a second
    // DoPut to an upload_idx another DoPut is still writing, whose stream ALSO
    // errors mid-flight, must still surface AlreadyExists — not the decode
    // error. do_put_inner skips its partial-file cleanup only for
    // AlreadyExists, and that `.partial` belongs to the first upload; masking
    // it as the decode error would unlink it. Reproduces the writer-task error
    // vs decode-error precedence.
    let tmp = tempfile::tempdir().unwrap();
    let service = make_service(tmp.path().to_path_buf());

    // An upload in progress occupies upload_idx=88.
    let staged = tmp.path().join("uploads/88/upload.parquet.partial");
    std::fs::create_dir_all(staged.parent().unwrap()).unwrap();
    std::fs::write(&staged, b"an upload in progress").unwrap();
    std::fs::set_permissions(&staged, std::fs::Permissions::from_mode(0o440)).unwrap();
    let bytes_before = std::fs::read(&staged).unwrap();

    // Second DoPut to the same idx: keep only the schema frame, then inject a
    // mid-stream error. The writer hits AlreadyExists on create_new while the
    // decoder surfaces the error, exercising the precedence.
    let t2 = sign_doput_for_test(88, &TEST_SEED, future_expiry_secs(300));
    let mut m2 = flight_stream_with_ticket(vec![sample_batch()], t2).await;
    m2.truncate(1);
    m2.push(Err(Status::internal("simulated mid-stream drop")));

    let err = service
        .do_put_inner(stream::iter(m2))
        .await
        .expect_err("second DoPut must fail");
    assert_eq!(
        err.code(),
        tonic::Code::AlreadyExists,
        "AlreadyExists must win over the decode error so the first file is preserved"
    );

    // The first upload's file is untouched — not unlinked by cleanup.
    assert!(staged.exists(), "the first upload's file must survive");
    assert!(
        !tmp.path().join("uploads/88/upload.parquet").exists(),
        "nothing is published"
    );
    assert_eq!(
        std::fs::read(&staged).unwrap(),
        bytes_before,
        "first upload bytes unchanged"
    );
    assert_eq!(
        std::fs::metadata(&staged).unwrap().permissions().mode() & 0o777,
        0o440
    );
}

#[tokio::test]
async fn do_put_publishes_upload_parquet_and_leaves_no_partial() {
    let tmp = tempfile::tempdir().unwrap();
    let service = make_service(tmp.path().to_path_buf());
    let ticket = sign_doput_for_test(31, &TEST_SEED, future_expiry_secs(300));
    let messages = flight_stream_with_ticket(vec![sample_batch()], ticket).await;
    service
        .do_put_inner(stream::iter(messages))
        .await
        .expect("DoPut should succeed");
    let staged = tmp.path().join("uploads/31/upload.parquet");
    assert!(staged.exists());
    assert_eq!(
        std::fs::metadata(&staged).unwrap().permissions().mode() & 0o777,
        0o440
    );
    assert!(!tmp
        .path()
        .join("uploads/31/upload.parquet.partial")
        .exists());
}

#[tokio::test]
async fn do_put_concurrent_uploads_are_isolated() {
    // Two uploads to different upload_idx values land at different
    // staging paths and don't trample each other. Smoke test that the
    // QiitaFlightService has no shared mutable state.
    let tmp = tempfile::tempdir().unwrap();
    let service = make_service(tmp.path().to_path_buf());

    let t1 = sign_doput_for_test(1, &TEST_SEED, future_expiry_secs(300));
    let t2 = sign_doput_for_test(2, &TEST_SEED, future_expiry_secs(300));
    let m1 = flight_stream_with_ticket(vec![sample_batch()], t1).await;
    let m2 = flight_stream_with_ticket(vec![sample_batch()], t2).await;

    let (r1, r2) = futures::join!(
        service.do_put_inner(stream::iter(m1)),
        service.do_put_inner(stream::iter(m2)),
    );
    r1.unwrap();
    r2.unwrap();

    assert!(tmp.path().join("uploads/1/upload.parquet").exists());
    assert!(tmp.path().join("uploads/2/upload.parquet").exists());
}

#[tokio::test]
async fn do_put_writes_multi_batch_stream() {
    // Exercise the async-decoder → blocking-writer channel bridge with more
    // than one RecordBatch: every batch must flow through the mpsc channel,
    // be written, and be counted. Three 3-row batches => 9 rows, and the
    // PutResult sha256/bytes must match the file on disk byte-for-byte.
    let tmp = tempfile::tempdir().unwrap();
    let service = make_service(tmp.path().to_path_buf());

    let ticket = sign_doput_for_test(55, &TEST_SEED, future_expiry_secs(300));
    let batches = vec![sample_batch(), sample_batch(), sample_batch()];
    let messages = flight_stream_with_ticket(batches, ticket).await;

    let result = service
        .do_put_inner(stream::iter(messages))
        .await
        .expect("multi-batch do_put should succeed");

    let body: serde_json::Value = serde_json::from_slice(&result.app_metadata).unwrap();
    assert_eq!(body["upload_idx"], 55);
    assert_eq!(body["row_count"], 9, "three 3-row batches stream through");

    let staged = tmp.path().join("uploads/55/upload.parquet");
    let actual_bytes = std::fs::metadata(&staged).unwrap().len();
    assert_eq!(body["bytes_received"].as_u64().unwrap(), actual_bytes);
    let mut hasher = Sha256::new();
    hasher.update(std::fs::read(&staged).unwrap());
    let actual_sha: String = hasher
        .finalize()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    assert_eq!(body["sha256"].as_str().unwrap(), actual_sha);

    // Round-trips as a 9-row Parquet.
    let reader = Connection::open_in_memory().unwrap();
    let n: i64 = reader
        .query_row(
            &format!(
                "SELECT count(*) FROM read_parquet('{}')",
                staged.to_str().unwrap()
            ),
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(n, 9);
}

#[test]
fn staging_path_for_layout() {
    let root = Path::new("/scratch/staging");
    assert_eq!(
        staging_path_for(root, 42),
        Path::new("/scratch/staging/uploads/42/upload.parquet")
    );
}
