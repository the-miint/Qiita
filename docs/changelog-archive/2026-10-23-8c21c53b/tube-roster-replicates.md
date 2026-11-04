### Fixed

- **A matrix tube may repeat in a pool roster as technical replicates (#708).** The
  server-side roster resolver refused a tube that appeared on more than one row, but a
  biosample can legitimately occur several times in a pool as replicates — exactly as a
  repeated `biosample_accession` already does — and each row stays distinct downstream by
  its `sequenced_pool_item_id`, not its tube. A repeated tube now resolves to the same
  biosample on each row, like a repeated accession. Reverses the restriction added in
  #684.
