//! Calibration for the M0 compression harness.
//!
//! A measuring instrument is checked against inputs whose answer is known
//! before the run: bytes that cannot compress, bytes that must. These are
//! synthetic and hermetic, so they belong in the pure-unit tier — the real
//! fixtures are the slow path and are measured separately.

// A directory under `tests/` is not a test target, so this is how the
// instrument is shared: Phase 3's fixture target declares the same module.
mod compression_harness;
use compression_harness as harness;

use std::sync::Arc;
use std::time::Duration;

use arrow_array::builder::StringDictionaryBuilder;
use arrow_array::types::UInt32Type;
use arrow_array::{ArrayRef, BinaryArray, Int64Array, RecordBatch, StringArray};
use arrow_ipc::CompressionType;
use arrow_schema::{DataType, Field, Schema};

use harness::{
    record_batch_codecs, verify_codec, Dictionaries, IpcCodec, IpcSetting, Measurement, Transport,
};

/// xorshift64*, so "incompressible" means the same bytes on every run. Four
/// lines beats a `rand` dependency the crate does not otherwise need.
fn pseudo_random_bytes(len: usize, seed: u64) -> Vec<u8> {
    let mut state = seed;
    let mut out = Vec::with_capacity(len + 8);
    while out.len() < len {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        out.extend_from_slice(&state.to_le_bytes());
    }
    out.truncate(len);
    out
}

/// One binary column of `rows` × `width` bytes with no exploitable structure.
/// Values are wide so the offsets buffer — which *is* compressible — stays a
/// rounding error against the data buffer.
fn incompressible_batch(rows: usize, width: usize) -> RecordBatch {
    let values: Vec<Vec<u8>> = (0..rows)
        .map(|i| pseudo_random_bytes(width, 0x9E3779B97F4A7C15 ^ i as u64))
        .collect();
    let array = BinaryArray::from_iter_values(values.iter().map(|v| v.as_slice()));
    single_column("payload", Arc::new(array))
}

/// One string column that is the same value `rows` times — the degenerate
/// shape a short-read eqx CIGAR approaches.
///
/// Note this is *not* the unambiguous known-answer input: a `StringArray`'s
/// offsets buffer is a monotonic i32 counter, which LZ4 cannot exploit at all,
/// so the achievable ratio is floored near 2x however trivial the values are.
/// Use [`constant_batch`] where the expected answer must be codec-independent.
fn degenerate_batch(rows: usize) -> RecordBatch {
    let array = StringArray::from_iter_values(std::iter::repeat_n("150=", rows));
    single_column("cigar", Arc::new(array))
}

/// One fixed-width column holding the same value `rows` times: a single buffer,
/// no offsets, nothing but repetition for a codec to find.
fn constant_batch(rows: usize) -> RecordBatch {
    let array = Int64Array::from_iter_values(std::iter::repeat_n(4_218_907_651, rows));
    single_column("prep_sample_idx", Arc::new(array))
}

/// One dictionary-encoded string column, low cardinality — what a taxonomy
/// rank looks like if DuckDB hands us one.
fn dictionary_batch(rows: usize) -> RecordBatch {
    const RANKS: [&str; 4] = ["Bacteria", "Archaea", "Eukaryota", "Viruses"];
    let mut builder = StringDictionaryBuilder::<UInt32Type>::new();
    for i in 0..rows {
        builder.append_value(RANKS[i % RANKS.len()]);
    }
    single_column("kingdom", Arc::new(builder.finish()))
}

fn single_column(name: &str, array: ArrayRef) -> RecordBatch {
    let schema = Schema::new(vec![Field::new(name, array.data_type().clone(), false)]);
    RecordBatch::try_new(Arc::new(schema), vec![array]).expect("build single-column batch")
}

async fn measure(batches: &[RecordBatch], setting: IpcSetting) -> Measurement {
    harness::measure_ipc(batches, setting)
        .await
        .unwrap_or_else(|e| panic!("measure_ipc({setting:?}) failed: {e}"))
}

// --- The codec is provably active ---------------------------------------

/// The load-bearing test: a codec that silently degraded to raw buffers would
/// make every downstream number a lie. Assert on what the encoder stamped into
/// the message header, not on the byte count — a small stream could plausibly
/// shrink for other reasons, but only a real codec sets `BodyCompression`.
#[tokio::test]
async fn requested_codec_is_stamped_into_every_batch_message() {
    let batches = [degenerate_batch(10_000)];
    for (codec, expected) in [
        (IpcCodec::None, None),
        (IpcCodec::Lz4Frame, Some(CompressionType::LZ4_FRAME)),
        (IpcCodec::Zstd, Some(CompressionType::ZSTD)),
    ] {
        let (messages, _) = harness::encode(&batches, IpcSetting::with_codec(codec))
            .await
            .expect("encode");
        let stamped = record_batch_codecs(&messages);
        assert!(!stamped.is_empty(), "{codec:?}: no record-batch messages");
        assert!(
            stamped.iter().all(|c| *c == expected),
            "{codec:?}: expected every batch stamped {expected:?}, got {stamped:?}"
        );
    }
}

/// The guard the instrument carries so no later measurement can be reported
/// from a stream that did not actually use the codec it claims.
#[tokio::test]
async fn verify_codec_rejects_a_stream_that_did_not_use_it() {
    let batches = [degenerate_batch(1_000)];
    let (uncompressed, _) = harness::encode(&batches, IpcSetting::with_codec(IpcCodec::None))
        .await
        .expect("encode");

    verify_codec(&uncompressed, IpcCodec::None).expect("none matches none");
    let err = verify_codec(&uncompressed, IpcCodec::Zstd)
        .expect_err("claiming zstd over an uncompressed stream must fail");
    assert!(
        err.to_string().contains("Zstd"),
        "error should name the codec asked for: {err}"
    );

    let empty: Vec<arrow_flight::FlightData> = Vec::new();
    verify_codec(&empty, IpcCodec::None)
        .expect_err("a stream with no record batches is not a measurement");
}

// --- Known-answer inputs -------------------------------------------------

#[tokio::test]
async fn incompressible_input_gains_nothing() {
    let batches = [incompressible_batch(512, 1024)];
    let baseline = measure(&batches, IpcSetting::with_codec(IpcCodec::None)).await;

    for codec in [IpcCodec::Lz4Frame, IpcCodec::Zstd] {
        let ratio = measure(&batches, IpcSetting::with_codec(codec))
            .await
            .ratio_over(&baseline);
        assert!(
            (0.85..1.05).contains(&ratio),
            "{codec:?} on random bytes should be ~1.0, got {ratio:.3}"
        );
    }
}

#[tokio::test]
async fn degenerate_input_compresses_substantially() {
    let batches = [constant_batch(100_000)];
    let baseline = measure(&batches, IpcSetting::with_codec(IpcCodec::None)).await;

    for codec in [IpcCodec::Lz4Frame, IpcCodec::Zstd] {
        let measured = measure(&batches, IpcSetting::with_codec(codec)).await;
        let ratio = measured.ratio_over(&baseline);
        assert!(
            ratio > 10.0,
            "{codec:?} on one repeated value should be >10x, got {ratio:.1}"
        );
        // The timings are reported, not left at their defaults.
        assert!(measured.encode > Duration::ZERO && measured.decode > Duration::ZERO);
    }
}

// --- The numbers are what was produced, not an estimate ------------------

#[tokio::test]
async fn encoded_bytes_equal_the_serialized_messages() {
    use prost::Message as _;

    let batches = [incompressible_batch(64, 512), incompressible_batch(31, 512)];
    let setting = IpcSetting::with_codec(IpcCodec::Zstd);
    let (messages, _) = harness::encode(&batches, setting).await.expect("encode");
    let serialized: u64 = messages
        .iter()
        .map(|m| m.encode_to_vec().len() as u64)
        .sum();

    let measured = measure(&batches, setting).await;
    assert_eq!(measured.encoded_bytes, serialized);
    assert_eq!(measured.messages, messages.len());
    assert_eq!(
        measured.rows,
        batches.iter().map(|b| b.num_rows()).sum::<usize>()
    );
}

#[tokio::test]
async fn every_codec_round_trips_the_input_exactly() {
    let batches = [degenerate_batch(2_000), degenerate_batch(37)];
    for codec in [IpcCodec::None, IpcCodec::Lz4Frame, IpcCodec::Zstd] {
        let (messages, _) = harness::encode(&batches, IpcSetting::with_codec(codec))
            .await
            .expect("encode");
        let (decoded, _) = harness::decode(messages).await.expect("decode");
        let rows: usize = decoded.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 2_037, "{codec:?} lost rows");
        assert_eq!(
            decoded[0].schema().field(0).data_type(),
            &DataType::Utf8,
            "{codec:?} changed the column type"
        );
    }
}

// --- The representation knobs are live -----------------------------------

#[tokio::test]
async fn smaller_max_flight_data_size_splits_into_more_messages() {
    let batches = [incompressible_batch(4_096, 1_024)];
    let big = measure(&batches, IpcSetting::default()).await;
    let small = measure(
        &batches,
        IpcSetting {
            max_flight_data_size: 256 * 1024,
            ..IpcSetting::default()
        },
    )
    .await;

    assert!(
        small.messages > big.messages,
        "geometry knob is inert: {} messages at 256 KiB vs {} at the 2 MiB default",
        small.messages,
        big.messages
    );
    assert_eq!(small.rows, big.rows);
}

/// `Hydrate` (the default) expands a dictionary to its value type before
/// encoding; `Resend` keeps the keys and ships the dictionary. On a
/// low-cardinality column that is a large byte difference, which is what makes
/// the knob worth measuring in Phase 4.
#[tokio::test]
async fn resend_keeps_the_dictionary_that_hydrate_expands() {
    let batches = [dictionary_batch(50_000)];
    let hydrate = measure(&batches, IpcSetting::default()).await;
    let resend = measure(
        &batches,
        IpcSetting {
            dictionaries: Dictionaries::Resend,
            ..IpcSetting::default()
        },
    )
    .await;

    assert!(
        resend.encoded_bytes < hydrate.encoded_bytes,
        "dictionary knob is inert: resend {} bytes vs hydrate {} bytes",
        resend.encoded_bytes,
        hydrate.encoded_bytes
    );
    assert_eq!(resend.rows, hydrate.rows);
}

// --- The transport layer is measurable -----------------------------------

#[tokio::test]
async fn grpc_compression_shrinks_the_wire_for_the_same_messages() {
    let batches = [constant_batch(50_000)];
    let (messages, _) = harness::encode(&batches, IpcSetting::with_codec(IpcCodec::None))
        .await
        .expect("encode");

    let plain = harness::measure_transport(&messages, Transport::None)
        .await
        .expect("uncompressed transport");
    assert_eq!(plain.rows, 50_000);
    assert!(plain.wire_bytes > 0, "the relay counted no server bytes");
    assert!(
        plain.elapsed > Duration::ZERO,
        "no round-trip time reported"
    );

    for transport in [Transport::Gzip, Transport::Zstd] {
        let compressed = harness::measure_transport(&messages, transport)
            .await
            .unwrap_or_else(|e| panic!("{transport:?} transport: {e}"));
        assert_eq!(compressed.rows, plain.rows);
        assert!(
            compressed.wire_bytes * 10 < plain.wire_bytes,
            "{transport:?} on one repeated value should be >10x, got {} vs {} bytes",
            compressed.wire_bytes,
            plain.wire_bytes
        );
    }
}

// --- Fixture loading and per-column decomposition ------------------------

/// Write `batches` to a Parquet file the way the fixtures were produced, so the
/// loader is exercised against a file DuckDB wrote, not one this test invented.
fn write_temp_parquet(dir: &tempfile::TempDir, name: &str, rows: usize) -> std::path::PathBuf {
    let path = dir.path().join(name);
    let conn = duckdb::Connection::open_in_memory().expect("duckdb in-memory");
    conn.execute_batch(&format!(
        "COPY (SELECT i AS sequence_idx, 'read_' || i AS read_id, 'ACGT' AS sequence1
                 FROM range({rows}) t(i))
           TO '{}' (FORMAT parquet)",
        path.display()
    ))
    .expect("write parquet");
    path
}

#[test]
fn loading_a_fixture_yields_the_rows_written() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = write_temp_parquet(&dir, "read.parquet", 5_000);

    let batches = harness::load_fixture(&path).expect("load");
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 5_000);

    let schema = batches[0].schema();
    assert_eq!(
        schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>(),
        ["sequence_idx", "read_id", "sequence1"]
    );
    assert_eq!(schema.field(0).data_type(), &DataType::Int64);
}

/// An absent fixture must be an error, not an empty batch set — the whole point
/// of the fixture target is that opting in asserts the data is there, and a
/// silent skip would report a shape as measured when nothing was.
#[test]
fn loading_a_missing_fixture_fails_loudly() {
    let dir = tempfile::tempdir().expect("tempdir");
    let err = harness::load_fixture(&dir.path().join("absent.parquet"))
        .expect_err("a missing fixture must not load");
    assert!(
        err.to_string().contains("fixture not found"),
        "unhelpful error: {err}"
    );
}

/// Pins the production-fidelity claim: `stream_arrow` yields DuckDB's chunk
/// geometry, so an input well past the vector size arrives as several batches.
/// If this ever collapses to one batch the loader has fallen back to a
/// materialising path and every geometry-sensitive number is measuring a shape
/// production does not emit.
#[test]
fn loading_a_fixture_streams_rather_than_materialising() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = write_temp_parquet(&dir, "big.parquet", 100_000);

    let batches = harness::load_fixture(&path).expect("load");
    assert!(
        batches.len() > 1,
        "100,000 rows arrived as {} batch(es) — not streaming",
        batches.len()
    );
    assert!(
        batches.iter().all(|b| b.num_rows() > 0),
        "an empty batch in the stream"
    );
}

#[test]
fn projecting_a_column_preserves_rows_and_values() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = write_temp_parquet(&dir, "read.parquet", 3_000);
    let batches = harness::load_fixture(&path).expect("load");

    let projected = harness::project(&batches, "read_id").expect("project");
    assert_eq!(projected.len(), batches.len());
    assert_eq!(projected[0].num_columns(), 1);
    assert_eq!(projected[0].schema().field(0).name(), "read_id");

    let rows: usize = projected.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 3_000);
    let whole = batches[0].column_by_name("read_id").expect("column");
    assert_eq!(&projected[0].column(0), &whole);

    let missing = harness::project(&batches, "no_such_column").expect_err("unknown column");
    assert!(missing.to_string().contains("no_such_column"));
}

/// The load-bearing test for every per-column number Phase 3 reports: IPC
/// compresses each buffer independently, so a column measured alone must
/// compress as it does inside the table. If that holds, the per-column figures
/// are a decomposition of the shape figure and Phase 5 may generalise from
/// them; if it does not, they are a different experiment wearing the same name.
///
/// The tolerance covers per-message framing only — each projected stream repeats
/// the schema and flatbuffer headers the whole-table stream pays once.
#[tokio::test]
async fn per_column_encoded_bytes_sum_to_the_whole_table() {
    let rows = 20_000;
    let batch = RecordBatch::try_from_iter(vec![
        (
            "prep_sample_idx",
            Arc::new(Int64Array::from_iter_values(std::iter::repeat_n(
                7_000_001, rows,
            ))) as ArrayRef,
        ),
        (
            "cigar",
            Arc::new(StringArray::from_iter_values(
                (0..rows).map(|i| format!("{}M{}S", 150 - i % 7, i % 7)),
            )) as ArrayRef,
        ),
        (
            "payload",
            Arc::new(BinaryArray::from_iter_values(
                (0..rows).map(|i| pseudo_random_bytes(64, i as u64)),
            )) as ArrayRef,
        ),
    ])
    .expect("build batch");
    let batches = [batch];

    for codec in [IpcCodec::None, IpcCodec::Lz4Frame, IpcCodec::Zstd] {
        let whole = measure(&batches, IpcSetting::with_codec(codec)).await;
        let mut summed = 0u64;
        for name in ["prep_sample_idx", "cigar", "payload"] {
            let column = harness::project(&batches, name).expect("project");
            summed += measure(&column, IpcSetting::with_codec(codec))
                .await
                .encoded_bytes;
        }
        let drift = (summed as f64 - whole.encoded_bytes as f64).abs() / whole.encoded_bytes as f64;
        assert!(
            drift < 0.02,
            "{codec:?}: per-column sum {summed} vs whole table {} — {:.1}% drift, \
             so per-column measurement is not a decomposition",
            whole.encoded_bytes,
            drift * 100.0
        );
    }
}

// --- Representation transforms preserve the data (Phase 4) ---------------

/// Every byte count Phase 4 reports is a comparison between two arrays that must
/// hold the *same* values. A transform that silently dropped, reordered, or
/// truncated a value would produce a smaller array and read as a win.
fn assert_same_values(original: &RecordBatch, transformed: &RecordBatch, index: usize) {
    assert_eq!(transformed.num_rows(), original.num_rows(), "row count");
    assert_eq!(transformed.num_columns(), original.num_columns(), "columns");
    // Cast back to the original type: equality across representations is exactly
    // the property under test, and `cast` is the arrow-blessed way to state it.
    let back = arrow_cast::cast(
        transformed.column(index),
        original.column(index).data_type(),
    )
    .expect("cast back to the original type");
    assert_eq!(
        &back,
        original.column(index),
        "values differ after the round trip"
    );
}

/// A column with long runs — what a sorted identifier column looks like.
fn runny_batch(rows: usize, run: usize) -> RecordBatch {
    let array = Int64Array::from_iter_values((0..rows).map(|i| (i / run) as i64 + 7_000_001));
    single_column("prep_sample_idx", Arc::new(array))
}

#[test]
fn run_end_encoding_preserves_every_value() {
    let batch = runny_batch(10_000, 500);
    let encoded = harness::to_run_end_encoded(&batch, 0).expect("Int64 column");
    assert!(matches!(
        encoded.schema().field(0).data_type(),
        DataType::RunEndEncoded(..)
    ));
    assert_same_values(&batch, &encoded, 0);

    // A column with no runs at all must still round-trip — the degenerate case
    // where run-end encoding is pure overhead is exactly one we need to measure.
    let distinct = single_column(
        "sequence_idx",
        Arc::new(Int64Array::from_iter_values(0..5_000i64)),
    );
    assert_same_values(
        &distinct,
        &harness::to_run_end_encoded(&distinct, 0).expect("Int64 column"),
        0,
    );

    assert!(
        harness::to_run_end_encoded(&degenerate_batch(10), 0).is_none(),
        "a Utf8 column is not an Int64 column"
    );
}

#[test]
fn string_view_conversion_preserves_every_value() {
    let batch = degenerate_batch(5_000);
    let view = harness::to_string_view(&batch, 0).expect("Utf8 column");
    assert_eq!(view.schema().field(0).data_type(), &DataType::Utf8View);
    assert_same_values(&batch, &view, 0);
    assert!(harness::to_string_view(&constant_batch(10), 0).is_none());
}

#[test]
fn dictionary_encoding_preserves_every_value() {
    let batch = degenerate_batch(5_000);
    let dict = harness::to_dictionary(&batch, 0).expect("Utf8 column");
    assert!(matches!(
        dict.schema().field(0).data_type(),
        DataType::Dictionary(..)
    ));
    assert_same_values(&batch, &dict, 0);
    assert!(harness::to_dictionary(&constant_batch(10), 0).is_none());
}

/// The refusal is the point. Item 5's hazard is that a narrowed identifier which
/// later exceeds its range corrupts silently, so the instrument must fail loudly
/// rather than wrap — otherwise the measurement would recommend a change whose
/// only real cost it had hidden.
#[test]
fn narrowing_preserves_every_value_and_refuses_out_of_range() {
    let batch = single_column(
        "prep_sample_idx",
        Arc::new(Int64Array::from_iter_values((0..5_000i64).map(|i| i * 7))),
    );
    let narrowed = harness::to_narrowed_i32(&batch, 0)
        .expect("in range")
        .expect("Int64 column");
    assert_eq!(narrowed.schema().field(0).data_type(), &DataType::Int32);
    assert_same_values(&batch, &narrowed, 0);

    let overflow = single_column(
        "prep_sample_idx",
        Arc::new(Int64Array::from_iter_values([1i64, i32::MAX as i64 + 1])),
    );
    let err = harness::to_narrowed_i32(&overflow, 0).expect_err("must refuse to truncate");
    assert!(
        err.to_string().contains("silently truncate"),
        "unhelpful error: {err}"
    );

    assert!(harness::to_narrowed_i32(&degenerate_batch(10), 0)
        .expect("not an error")
        .is_none());
}

/// Value preservation is not enough for a representation change: a transform can
/// keep every value and still explode the *layout*, and `assert_same_values` is
/// blind to that. Unsliced, a `Utf8View` column is 16 bytes/row of views plus the
/// value bytes against `Utf8`'s 4 bytes/row of offsets plus the same values — a
/// few percent, not a multiple.
#[tokio::test]
async fn string_view_conversion_does_not_inflate_the_layout() {
    // Long values, so any per-block slack dominates: 4 KiB each.
    let rows = 2_000;
    let batch = single_column(
        "sequence1",
        Arc::new(StringArray::from_iter_values(
            (0..rows).map(|i| "ACGT".repeat(1024 - i % 8)),
        )),
    );
    let view = harness::to_string_view(&batch, 0).expect("Utf8 column");
    assert_same_values(&batch, &view, 0);

    // Large enough that the encoder does not split: splitting is the separate
    // pathology pinned by the next test, and it would swamp this one.
    let unsplit = IpcSetting {
        codec: IpcCodec::None,
        max_flight_data_size: 64 * 1024 * 1024,
        ..IpcSetting::default()
    };
    let plain_raw = measure(&[batch], unsplit).await.encoded_bytes;
    let view_raw = measure(&[view], unsplit).await.encoded_bytes;
    assert!(
        view_raw < plain_raw + 16 * rows as u64 + plain_raw / 20,
        "Utf8View layout inflated: {view_raw} vs {plain_raw} uncompressed bytes \
         ({:.1}x) — the data buffers are not compact",
        view_raw as f64 / plain_raw as f64
    );
}

/// The pathology that made the first Phase 4 run report `Utf8View` as an 11x
/// loss. Views reference shared data buffers, so slicing keeps *every* buffer of
/// the original — and `FlightDataEncoderBuilder` slices to honour
/// `max_flight_data_size`, re-serializing the whole buffer per slice.
///
/// Pinned rather than merely noted: it is invisible in the array itself (which
/// stays compact), it only appears once the encoder splits, and it is the reason
/// `Utf8View` cannot be evaluated independently of batch geometry.
#[tokio::test]
async fn slicing_a_string_view_array_duplicates_its_data_buffers() {
    let rows = 2_000;
    let batch = single_column(
        "sequence1",
        Arc::new(StringArray::from_iter_values(
            (0..rows).map(|i| "ACGT".repeat(1024 - i % 8)),
        )),
    );
    let view = harness::to_string_view(&batch, 0).expect("Utf8 column");

    let split = |size| IpcSetting {
        codec: IpcCodec::None,
        max_flight_data_size: size,
        ..IpcSetting::default()
    };
    let unsplit = measure(std::slice::from_ref(&view), split(64 * 1024 * 1024)).await;
    let sliced = measure(&[view], split(2 * 1024 * 1024)).await;

    assert!(
        sliced.messages > unsplit.messages,
        "the encoder did not split"
    );
    assert!(
        sliced.encoded_bytes > unsplit.encoded_bytes * 3,
        "expected splitting to duplicate the data buffers, got {} vs {} bytes — \
         if this now passes cheaply, arrow-rs has started compacting slices and \
         the Phase 4 finding needs revisiting",
        sliced.encoded_bytes,
        unsplit.encoded_bytes
    );
}
