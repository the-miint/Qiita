//! Phase 3 of the M0 compression evaluation: the codec axis over the Phase 2
//! production fixtures.
//!
//! This is a measurement run, not a correctness test. What it asserts is that
//! the run itself was sound — every codec was provably applied, every round trip
//! returned every row, no shape silently went missing. The numbers are printed
//! as markdown for [`docs/plans/m0-compression-evaluation.md`].
//!
//! ```sh
//! DUCKDB_DOWNLOAD_LIB=1 cargo test --release --features fixtures -- --nocapture
//! ```
//!
//! Feature-gated because the fixtures are local-only production data that is not
//! in the repo; `--features fixtures` asserts you have it, and a missing file is
//! an error rather than a skip.
#![cfg(feature = "fixtures")]

mod compression_harness;
use compression_harness as harness;

use std::time::Duration;

use arrow_array::RecordBatch;
use harness::{mib, IpcCodec, IpcSetting, Measurement, Transport};

/// The six shapes of the plan. Shape 3 is absent from production — no short-read
/// alignment has been run — and is carried here as a hole rather than dropped,
/// so the matrix shows what was not measured.
const SHAPES: &[(u8, &str, Option<&str>)] = &[
    (1, "read, short", Some("read-short.parquet")),
    (2, "read, long (HiFi)", Some("read-long.parquet")),
    (3, "alignment, short", None),
    (4, "alignment, long", Some("alignment-long.parquet")),
    (5, "phylogeny (WOL3)", Some("phylogeny-wol3.parquet")),
    (6, "taxonomy (WOL3)", Some("taxonomy-wol3.parquet")),
];

const CODECS: [IpcCodec; 3] = [IpcCodec::None, IpcCodec::Lz4Frame, IpcCodec::Zstd];

/// MiB/s over the *uncompressed* payload, so encode and decode of different
/// codecs are quoted against the same denominator and stay comparable.
fn throughput(uncompressed: u64, elapsed: Duration) -> f64 {
    if elapsed.is_zero() {
        return f64::INFINITY;
    }
    mib(uncompressed) / elapsed.as_secs_f64()
}

async fn measure(batches: &[RecordBatch], codec: IpcCodec, what: &str) -> Measurement {
    harness::measure_ipc(batches, IpcSetting::with_codec(codec))
        .await
        .unwrap_or_else(|e| panic!("{what}: {codec:?} failed: {e}"))
}

#[tokio::test(flavor = "multi_thread")]
async fn codec_axis_over_production_fixtures() {
    // A timing from an unoptimised build measures the build, not the codec, and
    // the whole deliverable here is throughput alongside ratio. Refuse rather
    // than publish a number that would be wrong by an order of magnitude.
    if cfg!(debug_assertions) {
        panic!("run with --release: debug-build throughput is not a measurement");
    }

    let dir = harness::fixture_dir();
    println!("\n<!-- fixtures: {} -->", dir.display());

    println!("\n### Whole shape\n");
    println!("| # | shape | rows | uncompressed | lz4 | zstd | lz4 enc/dec | zstd enc/dec |");
    println!("|---|---|---|---|---|---|---|---|");

    let mut per_column_rows: Vec<String> = Vec::new();
    let mut transport_rows: Vec<String> = Vec::new();

    for (number, label, file) in SHAPES {
        let Some(file) = file else {
            println!("| {number} | {label} | — | — | — | — | — | — |");
            per_column_rows.push(format!(
                "| {number} | {label} | — | — | — | — | — | — | — |"
            ));
            transport_rows.push(format!("| {number} | {label} | — | — | — | — | — |"));
            continue;
        };

        let batches = harness::load_fixture(&dir.join(file))
            .unwrap_or_else(|e| panic!("shape {number} ({file}): {e}"));
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert!(rows > 0, "shape {number} ({file}) loaded no rows");

        // --- whole shape ------------------------------------------------
        // One discarded pass first. Without it the shape measured first reports
        // ~5x lower throughput than the identical work done later in the run —
        // allocator growth and CPU frequency ramp, not a property of the codec.
        // Ratios are unaffected (they reproduce bit-identically across runs);
        // only the timings need this.
        for codec in CODECS {
            let _ = measure(&batches, codec, file).await;
        }

        let mut results = Vec::new();
        for codec in CODECS {
            let m = measure(&batches, codec, file).await;
            assert_eq!(m.rows, rows, "shape {number}: {codec:?} lost rows");
            results.push(m);
        }
        let (base, lz4, zstd) = (&results[0], &results[1], &results[2]);
        println!(
            "| {number} | {label} | {rows} | {:.1} MiB ({} msg) | {:.2}x | {:.2}x | {:.0}/{:.0} | {:.0}/{:.0} |",
            mib(base.encoded_bytes),
            base.messages,
            lz4.ratio_over(base),
            zstd.ratio_over(base),
            throughput(base.encoded_bytes, lz4.encode),
            throughput(base.encoded_bytes, lz4.decode),
            throughput(base.encoded_bytes, zstd.encode),
            throughput(base.encoded_bytes, zstd.decode),
        );

        // --- per column -------------------------------------------------
        for (index, field) in batches[0].schema().fields().iter().enumerate() {
            let column = harness::project(&batches, field.name()).expect("project");
            let mut column_results = Vec::new();
            for codec in CODECS {
                column_results.push(measure(&column, codec, field.name()).await);
            }
            let (cbase, clz4, czstd) = (&column_results[0], &column_results[1], &column_results[2]);

            // An all-NULL column still carries a full-width values buffer of
            // zeros, which compresses like a constant. Reported so nobody reads
            // it as a compression result — it is the absence of data.
            let nulls: usize = batches.iter().map(|b| b.column(index).null_count()).sum();
            let null_pct = 100.0 * nulls as f64 / rows as f64;

            per_column_rows.push(format!(
                "| {number} | `{}` | {} | {:.1} MiB | {:.1}% | {:.2}x | {:.2}x | {:.1}% | {} |",
                field.name(),
                field.data_type(),
                mib(cbase.encoded_bytes),
                100.0 * cbase.encoded_bytes as f64 / base.encoded_bytes as f64,
                clz4.ratio_over(cbase),
                czstd.ratio_over(cbase),
                // Share of the shape *after* zstd — what a codec decision is
                // actually spending bytes on, which is not the same ranking as
                // the uncompressed share.
                100.0 * czstd.encoded_bytes as f64 / zstd.encoded_bytes as f64,
                if null_pct >= 99.995 {
                    "**all null**".to_string()
                } else {
                    format!("{null_pct:.1}%")
                },
            ));
        }

        // --- transport layer --------------------------------------------
        // Which layer wins, and what compressing at both costs. Each cell is a
        // real socket, so `wire_bytes` includes HTTP/2 framing and the gRPC
        // length prefixes that `encoded_bytes` excludes.
        let (plain_msgs, _) = harness::encode(&batches, IpcSetting::with_codec(IpcCodec::None))
            .await
            .expect("encode uncompressed");
        let (zstd_msgs, _) = harness::encode(&batches, IpcSetting::with_codec(IpcCodec::Zstd))
            .await
            .expect("encode zstd");

        let mut wire = Vec::new();
        let mut secs = Vec::new();
        for (msgs, transport) in [
            (&plain_msgs, Transport::None),
            (&plain_msgs, Transport::Gzip),
            (&plain_msgs, Transport::Zstd),
            (&zstd_msgs, Transport::None),
            (&zstd_msgs, Transport::Zstd),
        ] {
            let t = harness::measure_transport(msgs, transport)
                .await
                .unwrap_or_else(|e| panic!("shape {number} transport {transport:?}: {e}"));
            assert_eq!(t.rows, rows, "shape {number}: {transport:?} lost rows");
            wire.push(t.wire_bytes);
            secs.push(t.elapsed.as_secs_f64());
        }
        let baseline = wire[0] as f64;
        // Wall time is quoted relative to the uncompressed round trip, so the
        // relay's own per-byte cost cancels. It biases *towards* compression —
        // a compressed stream pushes fewer bytes through the relay — so a codec
        // that still looks slow here is genuinely slow.
        transport_rows.push(format!(
            "| {number} | {label} | {:.1} MiB | {:.2}x / {:.2}x | {:.2}x / {:.2}x | {:.2}x / {:.2}x | {:.2}x / {:.2}x |",
            mib(wire[0]),
            baseline / wire[1] as f64,
            secs[1] / secs[0],
            baseline / wire[2] as f64,
            secs[2] / secs[0],
            baseline / wire[3] as f64,
            secs[3] / secs[0],
            baseline / wire[4] as f64,
            secs[4] / secs[0],
        ));
    }

    println!("\n### Per column\n");
    println!(
        "| # | column | type | uncompressed | share raw | lz4 | zstd | share after zstd | nulls |"
    );
    println!("|---|---|---|---|---|---|---|---|---|");
    for row in per_column_rows {
        println!("{row}");
    }

    println!("\n### Transport layer (real socket, ratio vs uncompressed on both layers)\n");
    println!("| # | shape | wire, both off | +gzip | +grpc zstd | +ipc zstd | ipc+grpc zstd |");
    println!("_Each cell is `bytes saved / wall time vs the uncompressed round trip`._\n");
    println!("|---|---|---|---|---|---|---|");
    for row in transport_rows {
        println!("{row}");
    }
    println!();
}
