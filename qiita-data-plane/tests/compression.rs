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

fn with_codec(codec: IpcCodec) -> IpcSetting {
    IpcSetting {
        codec,
        ..IpcSetting::default()
    }
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
        let (messages, _) = harness::encode(&batches, with_codec(codec))
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
    let (uncompressed, _) = harness::encode(&batches, with_codec(IpcCodec::None))
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
    let baseline = measure(&batches, with_codec(IpcCodec::None)).await;

    for codec in [IpcCodec::Lz4Frame, IpcCodec::Zstd] {
        let ratio = measure(&batches, with_codec(codec))
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
    let baseline = measure(&batches, with_codec(IpcCodec::None)).await;

    for codec in [IpcCodec::Lz4Frame, IpcCodec::Zstd] {
        let measured = measure(&batches, with_codec(codec)).await;
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
    let setting = with_codec(IpcCodec::Zstd);
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
        let (messages, _) = harness::encode(&batches, with_codec(codec))
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
    let (messages, _) = harness::encode(&batches, with_codec(IpcCodec::None))
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
