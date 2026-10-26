### Added

- **One server-side route resolves a pool roster, and every submit command uses
  it (#684).** `POST /biosample/resolve-roster` takes every pool row (a matrix tube, or
  biosample and bioproject accessions) and returns each row's `biosample_idx` with
  its primary and secondary study. A tube resolves on its own, since tubes are
  unique across Qiita; its study comes from the biosample's one active study link,
  or from a project accession on the row, which must name one of its studies. A
  tube that lost its leading zero resolves to the stored 10-digit form, and a tube
  repeated within a roster is refused, since a tube-keyed roster does not support
  technical replicates. An accession row resolves through the same reads as the
  biosample and study accession lookups (with no project named, it takes the
  biosample's one active study), and the route takes both their scopes
  (`biosample:read`, `study:read`). Any unresolved row refuses the whole roster
  with a 422 naming each row's problem. `submit-bcl-convert`,
  `submit-pacbio-ingest` and `submit-golay-demux` now resolve through it instead
  of the two lookups, so their unresolved-row report lists each row and its
  problem rather than grouping the missing accessions, and they need a control
  plane that serves the route.
