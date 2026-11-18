### Added

- **`qiita.genome_derivation` records the assembly a derived genome came from (#fix/wol3-genome-fold).**
  One row per derived genome names its parent assembly by versioned accession (GenBank
  `GCA_` or RefSeq `GCF_`), how the sequence was derived, and the reference whose
  release asserts it. Each Web of Life genome is a derivative of an NCBI assembly
  rather than the assembly itself; until now nothing recorded which one.
- **`qiita-admin backfill wol-genome-repair --reference-idx N --metadata PATH` (#fix/wol3-genome-fold).**
  Folds each `genbank:GCA_G<n>` genome into `G<n>`, the name the reference's own FASTA
  header gives the sequence, and records the parent assembly of each of the
  reference's genomes from the Web of Life release's `metadata.txt`. Dry run by default; with
  `--execute --manifest PATH` it runs in one transaction, refuses anything it cannot
  settle (naming the rows), rolls back if the result does not hold, and writes a CSV of
  every row it deleted and label it retired.

### Changed

- **A reference that asserts a genome derivation cannot be deleted, with or without
  `force` (#fix/wol3-genome-fold).** `DELETE /reference/{reference_idx}` answers `409` naming the count,
  before any step that writes. Its last check, just before the Postgres teardown, now
  holds the reference row: a derivation or a new work ticket still uncommitted at that
  point makes the delete wait for it and then answer 409, leaving the reference's
  Postgres rows in place, rather than fail on the foreign key or lose the derivation.
- **Deleting genomes no longer scans `exported_feature` once per genome (#fix/wol3-genome-fold).** A
  plain index on `exported_feature.genome_idx` serves the foreign key's `SET NULL`
  lookup, which the partial live-genome index could not: deleting 196,062 genomes went
  from 8 min 46 s to 3.3 s with 93,242 genome labels, on Postgres 17.

### Fixed

- **Feature tables built against `web-of-life` `3-rc4` split almost every genome's reads
  between `G<n>` and `GCA_G<n>` (#fix/wol3-genome-fold).** That reference's load gave each of its
  196,062 sequences a second genome, `GCA_` followed by the WoL ID. For 195,943 of them
  `web-of-life` `3` had already linked the same bytes to `G<n>`, so the genome appeared
  in a table twice, each copy with half its reads; 117 were linked instead to the other
  WoL ID that `web-of-life` `3` gave those bytes, and 2 had no `G<n>`. Once the operator
  runs `wol-genome-repair`, each sequence links to `G<n>` alone, the published `GCA_G…`
  labels are retired, and tables rebuilt against the reference are whole again. Those
  117 sequences then report `G<n>` in `web-of-life` `3`'s genome map too, since the
  link that named their old WoL ID is removed.
