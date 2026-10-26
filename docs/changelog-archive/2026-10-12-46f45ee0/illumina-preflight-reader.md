### Fixed

- **`submit-bcl-convert` no longer crashes reading a real pre-flight (#683).** The reader
  unpacked each `PlatformSampleInfo` as four values; the library returns more, so
  every real pre-flight failed with "too many values to unpack". It reads the
  record's fields by name and is tested against a pre-flight the library builds.
  The library's refusals (a NULL accession, a control/project mismatch) are shown
  verbatim instead of as "verify the file is a kl-run-preflight SQLite". Both the
  Illumina and PacBio readers now refuse a blank or whitespace-padded accession,
  naming the sample, instead of crashing or mismatching at the accession lookup.
