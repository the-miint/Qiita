### Changed

- **New lake data is written at zstd level 16 (#773).** Parquet that a `register-files` step
  registers into DuckLake (reads, read masks, alignments, and the reference, amplicon and
  assembly tables) is now written at zstd level 16 instead of DuckDB's default 3, and the
  data plane sets DuckLake's own `parquet_compression_level` to 16 at boot. New files are
  12–15% smaller on synthetic production-shaped tables (−14.6% on one real long-read
  prep_sample's `sequence1`); files already in the lake are unchanged. Files that are not
  kept (inter-step inputs, downloads, exports) stay at the default level. The cost is write
  time, measured on those tables with DuckDB 1.5.5: ~46–52 s more per GB of reads and ~74 s
  per GB of reference sequence at 8 threads, and ~102 s per GB of long reads at
  `bam-to-parquet`'s 2.
- **Sorted lake writes of wide rows get row groups near 64 MB (#773).** A sorted DuckDB COPY
  ignores the 64 MB row-group cap, so a reference chunk batch got row groups of 16,384
  chunks (~1 GB) and a long-read prep_sample groups of 122,880 reads (several GB), each
  compressed on one core. The read and reference-chunk writers now set the row count from
  the rows' width: as many 2,048-row vectors as fit 64 MB, at least one (~130 MB of 64 KB
  chunks). On the same synthetic tables that makes the level-16 write about 3× faster for
  long reads and 2.6× for chunks at 8 threads.
- **`fastq-to-parquet` and `golay-demux` use 8 cores for DuckDB (#773).** The `fastq` step goes
  from 2 cpu / 8 GB to 8 cpu / 13 GB, and `golay-demux` uses all 8 of its cpu, up from 4.
- **Longer walltimes for the slowest lake writers (#773).** `local-reference-add`'s `load` step
  goes from 36 h to 48 h under a 72 h ceiling (was 48 h), and `bam-to-parquet`'s `bam` step
  from 4 h to 6 h.
