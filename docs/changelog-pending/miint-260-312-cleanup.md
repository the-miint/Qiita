### Changed

- **Local reference staging reads its whole manifest in one `read_fastx` call per pass
  (#TBD).** `stage_local_fasta` fed its manifest to miint 500 paths at a time, because
  `read_fastx` held every listed file open until the call ended (duckdb-miint#260).
  duckdb-miint#261 fixed that, so each pass is one call again and `fasta_chunks/` holds a
  single part. It relies on that release, which miint does not yet document
  (duckdb-miint#378).
- **ENA ingest refuses a run accession that is not a run (#TBD).** A pool whose
  sequenced_sample carries an experiment, sample, study or malformed accession as its
  `ena_run_accession` now fails its download at once, naming each prep_sample:
  `read_ena_sequences` would expand such an accession to every run under it, and a scan
  of several runs skips a run that fails. Correct the accession and redrive.

### Removed

- **ENA ingest no longer reads miint's warnings after each run (#TBD).** A one-run
  `read_ena_sequences` scan that fails now raises (duckdb-miint#312) instead of
  skipping the run with only a warning (duckdb-miint#279), so the skip-warning check is
  gone. The miint builds the mirror publishes for DuckDB 1.5.5 and 2.0.0 include
  duckdb-miint#312, so ingest behaves as before.
