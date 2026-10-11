### Added

- **golay-demux checks its submitted barcode roster against the pool's stored
  pre-flight (#657).** The CLI builds `barcode_map` client-side; a transposed roster
  would route one sample's reads under another's `prep_sample_idx` with nothing
  downstream to notice. Before running, the runner rebuilds the roster from the
  pre-flight blob stored on the pool (`preflight.amplicon_barcode_from_blob`, joined
  to the pool's sequenced samples) and fails the ticket as bad input, naming each
  differing sample, if the submitted one differs in samples, barcodes or
  orientation. The CLI and the runner validate the pre-flight through one function
  (`preflight.amplicon_samples`), so they refuse the same sheets: not an amplicon
  pre-flight, no samples, or a sample missing its barcode or a required accession.
  The expected roster is the pool's ACTIVE set (retired and ena_status-flagged
  samples excluded, as the CLI's roster read does), barcodes compare
  case-insensitively (the job upper-cases them), and a stored blob that cannot be
  READ (unreadable, or a newer pre-flight schema than this deployment ships) fails
  as a deployment error rather than the submitter's bad input. The `barcode_map`
  input and the workflow version are unchanged.
