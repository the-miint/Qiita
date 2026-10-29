### Changed

- **Local reference staging reads its whole manifest in one `read_fastx` call per pass
  (#TBD).** `stage_local_fasta` fed its manifest to miint 500 paths at a time, because
  `read_fastx` held every listed file open until the call ended (duckdb-miint#260).
  duckdb-miint#261 fixed that, so each pass is one call again and `fasta_chunks/` holds a
  single part. A real-miint test pins one call over 300 files under a 128-descriptor
  limit.

### Removed

- **ENA ingest no longer reads miint's warnings after each run (#TBD).** A one-run
  `read_ena_sequences` scan that fails now raises (duckdb-miint#312) instead of
  skipping the run with only a warning (duckdb-miint#279), so the skip-warning check is
  gone. Both staged miint builds already raise, so ingest behaves as before.
