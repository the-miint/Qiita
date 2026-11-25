### Fixed

- **Feature minting and the membership loads no longer stall the control plane's API
  (#TBD).** `mint_features` (behind `mint-features`, `mint-gene-features` and
  `mint-annotation-features`) wrote each batch's `sequence_hash -> feature_idx` pairs into
  its working table with a per-row insert on the control plane's event loop, and
  `mint-annotation-features` did the same for its annotation map; with several mints
  running, `/health` and `qiita ticket list` timed out. Each batch is now one Arrow insert
  in a worker thread. The batch reads of those actions, of the genome associations, the
  annotation terms, `write_membership` and `write_assembly_membership` (reading each batch
  from DuckDB and turning it into Python lists) also run in a worker thread now.

### Changed

- **The reviewer agent flags blocking work on the control plane's event loop (#TBD).** A
  rule under R9 and a trigger-map row: a synchronous DuckDB call, file I/O or row loop
  inside an `async def` in `qiita-control-plane/src/` that runs per batch or grows with
  the input.
