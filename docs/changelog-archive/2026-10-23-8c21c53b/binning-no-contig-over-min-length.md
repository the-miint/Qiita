### Fixed
- **long-read-assembly binning finishes with no bins when no contig reaches metaWRAP's
  16,000 bp minimum, instead of failing (#764).** MetaBAT2 formed no bins and MaxBin2 ran
  FragGeneScan on an empty file, so metaWRAP exited 1 on every attempt and the ticket
  ended `failed`. The step now checks contig lengths before running metaWRAP and, when
  none reaches the minimum, finishes with an empty `bins_dir`, as it already did for an
  assembly with no non-circular contig.
  The binning image's build now runs metaWRAP on a one-contig fixture at that minimum
  and one above it, and fails if MaxBin2 stops keeping a contig of exactly the minimum,
  or keeps one under it, or if MetaBAT2's minimum contig length stops being metaWRAP's.
