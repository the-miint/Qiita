### Fixed

- **A reference load reads a jplace of any size (#TBD).** `reference_load` handed the
  whole jplace to miint's `read_jplace`, which cannot read a file over 1 GB
  (duckdb-miint#400), so Greengenes2 2024.09's 2.26 GB placement file could not load at
  all. The job now streams the placements into 16 MiB parts and reads them one at a
  time; that whole file loads in about two minutes.

### Changed

- **A reference load fails on a placement it cannot load as written (#TBD).** A placed
  fragment that matches no sequence identifier used to be dropped without a word; it
  now fails the load. So do a fragment placed twice, an `edge_num` that is not an edge
  of the jplace's own tree, and a tree loaded alongside the jplace that is not the
  jplace's own tree, `{N}` edge numbers and branch lengths included. Each error gives a
  count and examples. A placement with more than one name or location, an `nm`
  multiplicity other than 1, or a jplace whose `fields` are in another order is refused
  before it is read, because `read_jplace` would keep only part of it or put its values
  in the wrong columns (duckdb-miint#407).
- **A feature known by several names takes its lineage and its placement each from the
  lexicographically smallest name that has one (#TBD).** When read_ids whose sequences
  collapse to one feature carry different lineages, the load kept an arbitrary one. It
  now keeps the lineage of the smallest read_id that has one (the record the feature's
  accession names, when that record has a lineage) and warns with a count and examples.
  Placements from several of a feature's fragments follow the same rule.
