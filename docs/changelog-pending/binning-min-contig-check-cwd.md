### Fixed
- **The binning image builds again (#PR).** Its build-time minimum-contig check ran
  metaWRAP from the read-only image root, where MaxBin2 could not write the temporary
  file it creates in its working directory before calling FragGeneScan, so every check
  failed and `make redeploy` aborted before any restart. The check now runs from its own
  temporary directory.
