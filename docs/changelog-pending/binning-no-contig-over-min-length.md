### Fixed
- **long-read-assembly binning finishes with no bins when no contig reaches metaWRAP's
  16,000 bp minimum, instead of failing (#PR).** MetaBAT2 formed no bins and MaxBin2 ran
  FragGeneScan on an empty file, so metaWRAP exited 1 on every attempt and the ticket
  ended `failed`. The step now checks contig lengths before running metaWRAP and finishes
  with an empty `bins_dir`; the ticket completes with its contigs stored as unbinned.
