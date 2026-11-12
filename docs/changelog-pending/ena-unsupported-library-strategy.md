### Changed

- **A run whose ENA library strategy has no curated protocol now reports `unsupported`, not
  `failed`.** Its `failure_reason` names the strategy and source. This covers a strategy/source
  pair that maps to none of amplicon, metagenomics or transcriptomics, and a transcriptomic run on
  a long-read platform. A client filtering `ena_runs` on `status == "failed"` no longer sees these
  runs. A study whose runs are all unsupported still ends `failed` at the item level. A
  Targeted-Capture run with PCR selection is no longer refused as a mislabeled amplicon; it is
  `unsupported`.
