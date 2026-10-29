//! DoPut: the staging path an upload lands at, and staging an uploaded Arrow
//! stream as Parquet.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use arrow_array::RecordBatch;
use arrow_flight::decode::{DecodedPayload, FlightDataDecoder};
use arrow_flight::{FlightData, PutResult};
use futures::stream::{self, Stream, StreamExt};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::{WriterProperties, WriterVersion};
use sha2::{Digest, Sha256};
use tonic::Status;

use crate::auth;

use super::QiitaFlightService;

/// Canonical staging path for an upload — single source of truth shared by
/// the DoPut handler (writes here) and the control plane (reads here).
pub fn staging_path_for(root: &Path, upload_idx: i64) -> PathBuf {
    root.join("uploads")
        .join(upload_idx.to_string())
        .join("upload.parquet")
}

// ---------------------------------------------------------------------------
// DoPut — generic Arrow-data staging
// ---------------------------------------------------------------------------
//
// Receives an Arrow Flight stream with a signed DoPut ticket on the first
// message's FlightDescriptor.cmd. The ticket payload is exactly
// `{"action": "doput", "upload_idx": N}` — no consumer-specific fields. The
// handler is content-agnostic: whatever schema the client streams is what
// lands on disk as Parquet, set mode 440 on close. The consuming workflow
// (an orchestrator native module) reads `upload.parquet` and interprets it.
//
// The upload is written to a sibling `upload.parquet.partial` and linked to
// `upload.parquet` only once it is closed, fsynced and locked 440, so
// `upload.parquet` only ever appears complete. A data plane killed mid-upload
// leaves the `.partial`, never a truncated `upload.parquet`.
//
// Failure policy: any error mid-stream deletes the partial file and returns
// a Status to the client. The upload row in `qiita.upload` stays at
// `pending`; the client mints a fresh slot to retry. Partial-write
// failures aren't resumable.

impl QiitaFlightService {
    /// Generic over the input stream so unit tests can drive it with an
    /// in-memory `stream::iter([...])` instead of needing a real
    /// `Streaming<FlightData>` (which only the tonic transport can build).
    pub(crate) async fn do_put_inner<S>(&self, mut stream: S) -> Result<PutResult, Status>
    where
        S: Stream<Item = Result<FlightData, Status>> + Send + Unpin + 'static,
    {
        // Peel the first message, extract + verify the ticket.
        let first = stream
            .next()
            .await
            .ok_or_else(|| Status::invalid_argument("empty DoPut stream"))?
            .map_err(|e| Status::internal(format!("recv error: {e}")))?;
        let descriptor = first.flight_descriptor.as_ref().ok_or_else(|| {
            Status::invalid_argument("first DoPut message lacks FlightDescriptor")
        })?;
        if descriptor.cmd.is_empty() {
            return Err(Status::invalid_argument(
                "FlightDescriptor.cmd is empty (expected signed DoPut ticket)",
            ));
        }
        let payload = auth::verify_doput(&descriptor.cmd, &self.flight_public_key)
            .map_err(|e| Status::unauthenticated(e.to_string()))?;
        if payload.action != "doput" {
            return Err(Status::invalid_argument(format!(
                "action mismatch: ticket says {:?}, expected \"doput\"",
                payload.action
            )));
        }

        // One permit per upload in flight, taken after the ticket verifies so an
        // unauthenticated caller cannot spend one, and held until the staged file
        // is written. `try_acquire`, not `acquire`, for the reason `pool.rs` gives
        // for refusing rather than queueing. The refusal carries the same
        // "data plane busy" prefix as the pool's, so one signature covers both
        // surfaces; today both DoPut clients are user-facing CLIs that classify
        // nothing, so what the prefix reaches is a person reading the message.
        let _upload_permit = self.doput_permits.try_acquire().map_err(|_| {
            Status::resource_exhausted(format!(
                "data plane busy: all {} concurrent uploads are in use; retry",
                self.max_concurrent_doput
            ))
        })?;

        // Resolve staging path, create parent dir.
        let staging_path = staging_path_for(&self.upload_staging_root, payload.upload_idx);
        let parent = staging_path
            .parent()
            .ok_or_else(|| Status::internal("staging path has no parent"))?;
        std::fs::create_dir_all(parent)
            .map_err(|e| Status::internal(format!("mkdir {}: {e}", parent.display())))?;
        // Refused before the stream is read, so an upload to a finished slot is
        // not received in full only to be refused at the link below.
        if staging_path.exists() {
            return Err(already_uploaded(&staging_path));
        }
        let partial_path = {
            let mut s = staging_path.clone().into_os_string();
            s.push(".partial");
            PathBuf::from(s)
        };

        // Write the parquet + chmod + body-encode under a single
        // error-guarded scope. Any Err return below cleans up the partial
        // staging file via the trailing `if result.is_err()` block — this is
        // the single cleanup site so a new fallible operation in this scope
        // can't accidentally bypass it.
        let path_for_cleanup = partial_path.clone();
        let result: Result<PutResult, Status> = async {
            let (sha256, row_count, bytes_received) =
                write_doput_parquet(partial_path.clone(), first, stream).await?;

            // Lock the file 440 — owner+group read, no write, no world.
            // After this the data plane itself can't modify it; matches
            // the immutability assumption the consuming workflow makes.
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&partial_path, std::fs::Permissions::from_mode(0o440))
                .map_err(|e| Status::internal(format!("chmod 440: {e}")))?;

            // Publish. A hard link, not a rename, because it refuses an existing
            // `upload.parquet` (EEXIST) where a rename would replace it.
            std::fs::hard_link(&partial_path, &staging_path).map_err(|e| match e.kind() {
                std::io::ErrorKind::AlreadyExists => Status::internal(format!(
                    "{} appeared while this upload was written; this upload is discarded",
                    staging_path.display()
                )),
                _ => Status::internal(format!(
                    "link {} → {}: {e}",
                    partial_path.display(),
                    staging_path.display()
                )),
            })?;
            if let Err(e) = std::fs::remove_file(&partial_path) {
                eprintln!(
                    "warning: published {} but failed to remove {}: {e}",
                    staging_path.display(),
                    partial_path.display()
                );
            }

            let body = serde_json::to_vec(&serde_json::json!({
                "sha256": sha256,
                "row_count": row_count,
                "bytes_received": bytes_received,
                "upload_idx": payload.upload_idx,
            }))
            .map_err(|e| Status::internal(format!("json: {e}")))?;
            Ok(PutResult {
                app_metadata: body.into(),
            })
        }
        .await;

        // Clean up the `.partial` on any error path EXCEPT AlreadyExists.
        // AlreadyExists means we never opened it (a concurrent DoPut owns it
        // via create_new's atomic guard); deleting it would wipe that call's
        // upload. `upload.parquet` itself is never removed here.
        if let Err(ref e) = result {
            if e.code() != tonic::Code::AlreadyExists {
                if let Err(cleanup_err) = std::fs::remove_file(&path_for_cleanup) {
                    if cleanup_err.kind() != std::io::ErrorKind::NotFound {
                        eprintln!(
                            "warning: failed to clean up partial DoPut at {}: {cleanup_err}",
                            path_for_cleanup.display()
                        );
                    }
                }
            }
        }
        result
    }
}

/// The refusal of an upload to a slot whose `upload.parquet` is already published.
fn already_uploaded(staging_path: &Path) -> Status {
    Status::already_exists(format!(
        "staging file already exists — concurrent DoPut?: {}",
        staging_path.display()
    ))
}

/// `std::io::Write` adapter that incrementally feeds every byte the inner
/// writer accepts into a shared Sha256 + byte counter. Wrapping the staging
/// `File` in this lets ArrowWriter's normal write path also drive the digest,
/// removing the second full-file read `sha256_and_size` used to do.
///
/// State lives in an `Arc<Mutex<...>>` so the outer scope can extract the
/// final hash + byte count after `ArrowWriter::close()` consumes (and drops)
/// the wrapped writer. Mutex is uncontended in practice — parquet-rs writes
/// from the single async task that owns this writer.
struct HashingWriter<W: Write> {
    inner: W,
    state: Arc<Mutex<(Sha256, u64)>>,
}

impl<W: Write> Write for HashingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.inner.write(buf)?;
        let mut state = self
            .state
            .lock()
            .expect("HashingWriter mutex never poisoned");
        state.0.update(&buf[..n]);
        state.1 += n as u64;
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

/// Bounded depth of the DoPut decoder→writer channel. The blocking writer task
/// parks the async decoder on `send` when the channel is full, so peak memory is
/// ~this many decoded payloads in flight rather than the whole upload. A payload
/// is one client RecordBatch, which the chunked-upload path sizes up to ~1 GiB
/// (see `FLIGHT_MAX_DECODING_BYTES` in main.rs), so this is deliberately small —
/// enough to overlap decode and write without buffering many large batches.
/// Mirrors `DOGET_BATCH_CHANNEL_DEPTH`.
const DOPUT_WRITER_CHANNEL_DEPTH: usize = 4;

/// One decoded payload forwarded from the async decoder to the blocking writer.
enum DoPutWriterMsg {
    Schema(arrow_schema::SchemaRef),
    Batch(RecordBatch),
}

/// Drive the Flight stream through a Parquet writer; return
/// `(sha256_hex, row_count, bytes_received)`. The caller owns staging-path
/// cleanup on Err.
///
/// The Parquet write, `fsync`, and hashing are all blocking, but they must be
/// interleaved with `decoder.next().await` on the live tonic stream — so this
/// can't be a single `spawn_blocking`. Instead it bridges the two: an async loop
/// pulls decoded payloads off the stream and forwards each to a `spawn_blocking`
/// writer task over a bounded mpsc channel; the writer task owns the file and
/// does all blocking I/O off the async runtime. The bounded channel
/// backpressures the decoder (and thus the network) when the writer falls
/// behind, so peak memory stays bounded (`DOPUT_WRITER_CHANNEL_DEPTH`) — the
/// same posture as the DoGet streaming path.
async fn write_doput_parquet<S>(
    staging_path: PathBuf,
    first: FlightData,
    stream: S,
) -> Result<(String, u64, u64), Status>
where
    S: Stream<Item = Result<FlightData, Status>> + Send + Unpin + 'static,
{
    let (tx, mut rx) = tokio::sync::mpsc::channel::<DoPutWriterMsg>(DOPUT_WRITER_CHANNEL_DEPTH);

    // Blocking writer task: owns the file + ArrowWriter, consumes payloads until
    // the channel closes, then closes + fsyncs and returns the digest. All file
    // I/O lives here, off the tonic async worker.
    let writer_task = tokio::task::spawn_blocking(move || -> Result<(String, u64, u64), Status> {
        // sync_handle is a dup of the same file ArrowWriter owns. After
        // ArrowWriter::close() (which only flushes the writer's user-space
        // buffer plus the OS write buffer), sync_all() on the dup forces a
        // disk-level flush. Without it, a power loss / OOM kill between close
        // and /done can leave the client thinking the upload succeeded while
        // the bytes were never durable.
        let mut writer: Option<ArrowWriter<HashingWriter<std::fs::File>>> = None;
        let mut sync_handle: Option<std::fs::File> = None;
        let mut row_count: u64 = 0;
        // Outer half of the shared state. The HashingWriter held inside
        // ArrowWriter holds a clone; on ArrowWriter::close() that clone is
        // dropped and we can `try_unwrap` to extract the final digest and
        // byte count without a second read of the file.
        let hash_state: Arc<Mutex<(Sha256, u64)>> = Arc::new(Mutex::new((Sha256::new(), 0)));

        while let Some(msg) = rx.blocking_recv() {
            match msg {
                DoPutWriterMsg::Schema(schema) => {
                    if writer.is_some() {
                        return Err(Status::invalid_argument(
                            "DoPut stream carried multiple schemas",
                        ));
                    }
                    // `create_new` fails atomically (EEXIST) if the file already
                    // exists. Guards against two concurrent DoPuts with the same
                    // upload_idx silently clobbering each other's bytes via
                    // separate write() calls on the same path. The CP doesn't
                    // reissue tickets for a given slot, so this is the
                    // contract-violation surface. A `.partial` a killed data
                    // plane left behind is refused the same way.
                    let file = std::fs::OpenOptions::new()
                        .write(true)
                        .create_new(true)
                        .open(&staging_path)
                        .map_err(|e| match e.kind() {
                            std::io::ErrorKind::AlreadyExists => Status::already_exists(format!(
                                "partial upload already exists — concurrent DoPut, or one a \
                                 restart interrupted?: {}",
                                staging_path.display()
                            )),
                            _ => {
                                Status::internal(format!("create {}: {e}", staging_path.display()))
                            }
                        })?;
                    sync_handle = Some(
                        file.try_clone()
                            .map_err(|e| Status::internal(format!("dup file handle: {e}")))?,
                    );
                    // Parquet v2 + zstd. `qiita_common.parquet` names its option
                    // sets by where a file ends up: AT_REST (registered into
                    // DuckLake), TRANSFER (leaves the job, but not into the lake)
                    // and INTERMEDIATE (snappy, read once then deleted in the same
                    // job). A DoPut upload is consumed once, like an intermediate,
                    // but its disk-residency is "from /done to (eventual)
                    // cleanup," not "until the next phase in the same job." That
                    // can be minutes or much longer, so it is a transfer file: the disk-footprint
                    // tradeoff outweighs the snappy fast-decode win at GG2 scale.
                    // Backbone FASTA blew up to ~3.5× the source on disk under
                    // uncompressed v1 (DNA-chunk VARCHAR columns don't
                    // dictionary-encode), and zstd at parquet-rs's default level
                    // (1) gives ~4× compression.
                    let props = WriterProperties::builder()
                        .set_writer_version(WriterVersion::PARQUET_2_0)
                        .set_compression(Compression::ZSTD(ZstdLevel::default()))
                        .build();
                    let hashing_writer = HashingWriter {
                        inner: file,
                        state: hash_state.clone(),
                    };
                    writer = Some(
                        ArrowWriter::try_new(hashing_writer, schema, Some(props))
                            .map_err(|e| Status::internal(format!("parquet writer init: {e}")))?,
                    );
                }
                DoPutWriterMsg::Batch(batch) => {
                    let w = writer.as_mut().ok_or_else(|| {
                        Status::invalid_argument("RecordBatch arrived before Schema")
                    })?;
                    row_count += batch.num_rows() as u64;
                    w.write(&batch)
                        .map_err(|e| Status::internal(format!("parquet write: {e}")))?;
                }
            }
        }

        let w = writer.ok_or_else(|| Status::invalid_argument("DoPut stream had no Schema"))?;
        w.close()
            .map_err(|e| Status::internal(format!("parquet close: {e}")))?;

        // Force disk-level flush via the dup'd handle. Unwrap is safe —
        // sync_handle is set in lockstep with `writer`, which we just confirmed
        // resolved Some via the line above.
        sync_handle
            .expect("sync_handle set in lockstep with writer")
            .sync_all()
            .map_err(|e| Status::internal(format!("fsync: {e}")))?;

        // ArrowWriter::close() above dropped the HashingWriter (and with it
        // the inner Arc clone of hash_state); the outer Arc is now the sole
        // owner, so try_unwrap succeeds.
        let (hasher, bytes_received) = Arc::try_unwrap(hash_state)
            .expect("HashingWriter dropped its Arc clone via ArrowWriter::close")
            .into_inner()
            .expect("hash_state mutex never poisoned");
        let digest = hasher.finalize();
        let mut sha256 = String::with_capacity(64);
        for b in digest {
            use std::fmt::Write;
            write!(&mut sha256, "{b:02x}").expect("write to String never fails");
        }
        Ok((sha256, row_count, bytes_received))
    });

    // Async side: re-prepend the first message and map Status → FlightError so
    // the arrow-flight decoder can consume it, then forward each decoded payload
    // to the writer task.
    let combined = stream::once(async move { Ok::<_, Status>(first) })
        .chain(stream)
        .map(|r| {
            r.map_err(|s| {
                arrow_flight::error::FlightError::ExternalError(Box::new(std::io::Error::other(
                    s.to_string(),
                )))
            })
        });
    let mut decoder = FlightDataDecoder::new(combined);

    // A mid-stream decode error must win over whatever the writer task reports:
    // once we drop `tx`, the task finishes "normally" on a partial file. Capture
    // the decode error and return it after joining the task (the caller then
    // cleans up the partial file).
    let mut decode_err: Option<Status> = None;
    while let Some(item) = decoder.next().await {
        let decoded = match item {
            Ok(d) => d,
            Err(e) => {
                decode_err = Some(Status::internal(format!("flight decode: {e}")));
                break;
            }
        };
        let msg = match decoded.payload {
            DecodedPayload::Schema(schema) => DoPutWriterMsg::Schema(schema),
            DecodedPayload::RecordBatch(batch) => DoPutWriterMsg::Batch(batch),
            DecodedPayload::None => continue,
        };
        // A send error means the writer task already returned (an internal
        // error, e.g. AlreadyExists on create_new). Stop forwarding; the task's
        // Result carries the real cause.
        if tx.send(msg).await.is_err() {
            break;
        }
    }
    // Close the channel so the writer task can finish (drain buffered payloads,
    // then close + fsync).
    drop(tx);

    // Fold a task-panic join error into the same Result shape as the task body.
    let task_result: Result<(String, u64, u64), Status> = writer_task.await.unwrap_or_else(|e| {
        Err(Status::internal(format!(
            "doput writer task join failed: {e}"
        )))
    });

    // Error precedence. An `AlreadyExists` from the writer (it lost the
    // create_new race for this upload_idx) MUST win over a mid-stream decode
    // error: do_put_inner skips its partial-file cleanup ONLY for AlreadyExists,
    // and that staged file belongs to the concurrent, legitimate DoPut —
    // surfacing the decode error instead would let the cleanup unlink their
    // file. For every other outcome the decode error wins, because the writer
    // may have "succeeded" on a truncated file that the caller must clean up.
    if let Err(ref e) = task_result {
        if e.code() == tonic::Code::AlreadyExists {
            return task_result;
        }
    }
    if let Some(e) = decode_err {
        return Err(e);
    }
    task_result
}

#[cfg(test)]
#[path = "doput_tests.rs"]
mod tests;
