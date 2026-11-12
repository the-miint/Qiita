### Changed

- **A run whose ENA library strategy has no curated protocol now reports `unsupported`, not
  `failed`.** Its `failure_reason` names the strategy and source. This covers any strategy/source
  pair outside amplicon, WGS or metagenomic, and RNA-Seq or transcriptomic, and long-read
  transcriptomics. A client filtering `ena_runs` on `status == "failed"` no longer sees these
  runs. A study whose runs are all unsupported still ends `failed` at the item level. A
  Targeted-Capture run with PCR selection is no longer refused as a mislabelled amplicon; it is
  `unsupported`.
