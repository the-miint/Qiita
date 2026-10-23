### Changed

- **New lake data is written at zstd level 16 (#N).** Parquet that a `register-files` step
  registers into DuckLake (reads, read masks, alignments, reference and assembly tables) is
  now written at zstd level 16 instead of DuckDB's default 3, and the data plane sets
  DuckLake's own `parquet_compression_level` to 16 at boot. New files are 12–15% smaller;
  files already in the lake are unchanged. Files that are not kept (inter-step inputs,
  downloads, exports) stay at the default level. The cost is write time: at 8 threads,
  ~50 s more per GB of reads and ~74 s more per GB of reference sequence.
- **Sorted lake writes of wide rows are cut into ~64 MB row groups (#N).** A sorted DuckDB
  COPY ignores the 64 MB row-group cap, so a long-read sample or a reference chunk batch
  became one row group of up to several GB, and its compression ran on one core. The read and
  reference-chunk writers now set the row count from the rows' width, which makes their
  level-16 writes about 3× faster at 8 threads.
- **`fastq-to-parquet` and `golay-demux` use 8 cores for DuckDB (#N).** The `fastq` step goes
  from 2 cpu / 8 GB to 8 cpu / 13 GB, and `golay-demux` uses all 8 of its cpu, up from 4.
- **Longer walltimes for the slowest lake writers (#N).** `local-reference-add`'s `load` step
  goes from 36 h to 48 h under a 72 h ceiling (was 48 h), and `bam-to-parquet`'s `bam` step
  from 4 h to 6 h.
