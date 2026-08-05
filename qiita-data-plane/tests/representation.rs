//! Phase 4 of the M0 compression evaluation: the representation axis.
//!
//! Arrow IPC has no encoding layer, so every option here is a choice of array
//! type or of batch geometry. This measures items 2, 3, 4, 5 and 7; items 1 and
//! 6 are structural questions about DuckDB and live with the DuckLake
//! integration tests in `src/ducklake.rs`.
//!
//! ```sh
//! DUCKDB_DOWNLOAD_LIB=1 cargo test --release --features fixtures \
//!     --test representation -- --nocapture
//! ```
#![cfg(feature = "fixtures")]

mod compression_harness;
use compression_harness as harness;

use std::collections::HashSet;

use arrow_array::{Array, RecordBatch, StringArray};
use arrow_schema::DataType;
use harness::{mib, Dictionaries, IpcCodec, IpcSetting, Transport};

const SHAPES: &[(u8, &str, &str)] = &[
    (1, "read, short", "read-short.parquet"),
    (2, "read, long", "read-long.parquet"),
    (4, "alignment, long", "alignment-long.parquet"),
    (5, "phylogeny", "phylogeny-wol3.parquet"),
    (6, "taxonomy", "taxonomy-wol3.parquet"),
];

/// Geometry sweep. The default is `GRPC_TARGET_MAX_FLIGHT_SIZE_BYTES` (2 MiB),
/// which is the middle point, so the sweep brackets what production does today.
const GEOMETRIES: [usize; 5] = [
    256 * 1024,
    1024 * 1024,
    2 * 1024 * 1024,
    8 * 1024 * 1024,
    32 * 1024 * 1024,
];

/// Large enough that `FlightDataEncoderBuilder` never splits a batch. Slicing a
/// `Utf8View` array copies every data buffer into every slice, so a view column
/// cannot be evaluated at all without holding geometry fixed.
const NO_SPLIT: usize = 512 * 1024 * 1024;

fn setting(codec: IpcCodec, size: usize, dictionaries: Dictionaries) -> IpcSetting {
    IpcSetting {
        dictionaries,
        max_flight_data_size: size,
        ..IpcSetting::with_codec(codec)
    }
}

/// Encoded bytes under one setting, with the round-trip and codec checks
/// `measure_ipc` already enforces.
async fn bytes(batches: &[RecordBatch], s: IpcSetting) -> u64 {
    harness::measure_ipc(batches, s)
        .await
        .unwrap_or_else(|e| panic!("measure_ipc({s:?}): {e}"))
        .encoded_bytes
}

async fn zstd_bytes(batches: &[RecordBatch]) -> u64 {
    bytes(
        batches,
        setting(
            IpcCodec::Zstd,
            IpcSetting::default().max_flight_data_size,
            Dictionaries::Hydrate,
        ),
    )
    .await
}

fn distinct_strings(batches: &[RecordBatch], index: usize) -> Option<usize> {
    let mut seen: HashSet<&str> = HashSet::new();
    for batch in batches {
        let column = batch.column(index).as_any().downcast_ref::<StringArray>()?;
        for row in 0..column.len() {
            if column.is_valid(row) {
                seen.insert(column.value(row));
            }
        }
    }
    Some(seen.len())
}

#[tokio::test(flavor = "multi_thread")]
async fn representation_axis_over_production_fixtures() {
    if cfg!(debug_assertions) {
        panic!("run with --release: debug-build throughput is not a measurement");
    }
    let dir = harness::fixture_dir();

    let mut geometry = Vec::new();
    let mut identifiers = Vec::new();
    let mut strings = Vec::new();

    for (number, label, file) in SHAPES {
        let batches = harness::load_fixture(&dir.join(file))
            .unwrap_or_else(|e| panic!("shape {number} ({file}): {e}"));
        let schema = batches[0].schema();

        // --- item 7: batch geometry -------------------------------------
        for size in GEOMETRIES {
            let raw = harness::measure_ipc(
                &batches,
                setting(IpcCodec::None, size, Dictionaries::Hydrate),
            )
            .await
            .expect("uncompressed");
            let compressed = harness::measure_ipc(
                &batches,
                setting(IpcCodec::Zstd, size, Dictionaries::Hydrate),
            )
            .await
            .expect("zstd");
            geometry.push(format!(
                "| {number} | {label} | {} KiB | {} | {:.1} MiB | {:.2}x |",
                size / 1024,
                raw.messages,
                mib(compressed.encoded_bytes),
                compressed.ratio_over(&raw),
            ));
        }

        // --- items 3 and 5: identifier columns ---------------------------
        for field in schema.fields() {
            if field.data_type() != &DataType::Int64 {
                continue;
            }
            let column = harness::project(&batches, field.name()).expect("project");
            let plain = zstd_bytes(&column).await;

            let ree: Vec<RecordBatch> = column
                .iter()
                .filter_map(|b| harness::to_run_end_encoded(b, 0))
                .collect();
            assert_eq!(ree.len(), column.len(), "run-end encoding dropped a batch");
            let ree_bytes = zstd_bytes(&ree).await;

            // Narrowing refuses out-of-range rather than truncating; a refusal
            // is the answer for that column, not a failure of the run.
            let narrowed = column
                .iter()
                .map(|b| harness::to_narrowed_i32(b, 0))
                .collect::<Result<Vec<_>, _>>();
            let narrow_cell = match narrowed {
                Err(_) => "**exceeds i32**".to_string(),
                Ok(batches) => {
                    let batches: Vec<RecordBatch> = batches.into_iter().flatten().collect();
                    let narrow_bytes = zstd_bytes(&batches).await;
                    format!("{:.2}x", plain as f64 / narrow_bytes as f64)
                }
            };

            identifiers.push(format!(
                "| {number} | `{}` | {:.2} MiB | {:.2}x | {} |",
                field.name(),
                mib(plain),
                plain as f64 / ree_bytes as f64,
                narrow_cell,
            ));
        }

        // --- items 2 and 4: string columns -------------------------------
        for field in schema.fields() {
            if field.data_type() != &DataType::Utf8 {
                continue;
            }
            let column = harness::project(&batches, field.name()).expect("project");
            let plain = zstd_bytes(&column).await;
            let distinct = distinct_strings(&column, 0).expect("Utf8 column");

            let view: Vec<RecordBatch> = column
                .iter()
                .filter_map(|b| harness::to_string_view(b, 0))
                .collect();
            // Both geometries, because they are not the same experiment: the
            // encoder slices to honour `max_flight_data_size`, and slicing a view
            // array re-serializes every data buffer into every slice. The default
            // column is what production would actually get today.
            let view_bytes = zstd_bytes(&view).await;
            let view_unsplit = bytes(
                &view,
                setting(IpcCodec::Zstd, NO_SPLIT, Dictionaries::Hydrate),
            )
            .await;
            let plain_unsplit = bytes(
                &column,
                setting(IpcCodec::Zstd, NO_SPLIT, Dictionaries::Hydrate),
            )
            .await;

            let dict: Vec<RecordBatch> = column
                .iter()
                .filter_map(|b| harness::to_dictionary(b, 0))
                .collect();
            let default_size = IpcSetting::default().max_flight_data_size;
            let hydrate = bytes(
                &dict,
                setting(IpcCodec::Zstd, default_size, Dictionaries::Hydrate),
            )
            .await;
            let resend = bytes(
                &dict,
                setting(IpcCodec::Zstd, default_size, Dictionaries::Resend),
            )
            .await;

            strings.push(format!(
                "| {number} | `{}` | {} | {:.2} MiB | {:.2}x | {:.2}x | {:.2}x | {:.2}x |",
                field.name(),
                distinct,
                mib(plain),
                plain as f64 / view_bytes as f64,
                plain_unsplit as f64 / view_unsplit as f64,
                plain as f64 / hydrate as f64,
                plain as f64 / resend as f64,
            ));
        }
        drop(batches);
    }

    // --- item 7b: does IPC's deficit against gRPC grow with column count? --
    // The Phase 3 hypothesis, on one shape so payload is the only thing held
    // constant that matters: IPC compresses each buffer independently, gRPC
    // compresses the whole message, so more columns should favour gRPC.
    let taxonomy = harness::load_fixture(&dir.join("taxonomy-wol3.parquet")).expect("taxonomy");
    let names: Vec<String> = taxonomy[0]
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    let mut column_count = Vec::new();
    for take in [1usize, 3, 6, names.len()] {
        let projected: Vec<RecordBatch> = taxonomy
            .iter()
            .map(|b| b.project(&(0..take).collect::<Vec<_>>()).expect("project"))
            .collect();
        let default_size = IpcSetting::default().max_flight_data_size;

        let (plain_msgs, _) = harness::encode(
            &projected,
            setting(IpcCodec::None, default_size, Dictionaries::Hydrate),
        )
        .await
        .expect("encode plain");
        let (zstd_msgs, _) = harness::encode(
            &projected,
            setting(IpcCodec::Zstd, default_size, Dictionaries::Hydrate),
        )
        .await
        .expect("encode zstd");

        let wire_plain = harness::measure_transport(&plain_msgs, Transport::None)
            .await
            .expect("wire plain")
            .wire_bytes;
        let wire_ipc = harness::measure_transport(&zstd_msgs, Transport::None)
            .await
            .expect("wire ipc")
            .wire_bytes;
        let wire_grpc = harness::measure_transport(&plain_msgs, Transport::Zstd)
            .await
            .expect("wire grpc")
            .wire_bytes;

        let ipc_ratio = wire_plain as f64 / wire_ipc as f64;
        let grpc_ratio = wire_plain as f64 / wire_grpc as f64;
        column_count.push(format!(
            "| {take} | {:.1} MiB | {ipc_ratio:.2}x | {grpc_ratio:.2}x | {:+.2}x |",
            mib(wire_plain),
            grpc_ratio - ipc_ratio,
        ));
    }

    println!("\n### Item 7 — batch geometry\n");
    println!("| # | shape | max_flight_data_size | messages | zstd bytes | ratio |");
    println!("|---|---|---|---|---|---|");
    for row in geometry {
        println!("{row}");
    }

    println!("\n### Item 7b — IPC vs gRPC as column count grows (shape 6, real socket)\n");
    println!("| columns | wire uncompressed | IPC zstd | gRPC zstd | gRPC advantage |");
    println!("|---|---|---|---|---|");
    for row in column_count {
        println!("{row}");
    }

    println!("\n### Items 3 and 5 — identifier columns (all vs zstd-compressed plain)\n");
    println!("| # | column | zstd plain | + run-end encoding | + narrow to i32 |");
    println!("|---|---|---|---|---|");
    for row in identifiers {
        println!("{row}");
    }

    println!("\n### Items 2 and 4 — string columns (all vs zstd-compressed plain)\n");
    println!(
        "| # | column | distinct | zstd plain | + Utf8View (split) | + Utf8View (unsplit) \
| + dict/Hydrate | + dict/Resend |"
    );
    println!("|---|---|---|---|---|---|---|---|");
    for row in strings {
        println!("{row}");
    }
    println!();
}
