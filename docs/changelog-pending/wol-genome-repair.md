### Added

- **`qiita.genome_derivation` records the assembly a derived genome came from (#fix/wol3-genome-fold).**
  One row per derived genome names its parent assembly by versioned accession (GenBank
  `GCA_` or RefSeq `GCF_`), how the sequence was derived, and the reference whose
  release asserts it. Web of Life genomes are host-masked and concatenated, so each is
  a derivative of an NCBI assembly rather than the assembly itself; until now nothing
  recorded which one.
- **`qiita-admin backfill wol-genome-repair --reference-idx N --metadata PATH` (#fix/wol3-genome-fold).**
  Folds each `genbank:GCA_G<n>` genome into `G<n>`, the name the reference's own FASTA
  header gives the sequence, and records each of the reference's genomes' parent
  assembly from the Web of Life release's `metadata.txt`. Dry run by default; with
  `--execute --manifest PATH` it runs in one transaction, refuses anything it cannot
  settle (naming the rows), rolls back if the result does not hold, and writes a CSV of
  every row it deleted and label it retired.

### Changed

- **A reference that asserts a genome derivation cannot be deleted, with or without
  `force` (#fix/wol3-genome-fold).** `DELETE /reference/{reference_idx}` answers `409` naming the count,
  before any step that writes.
- **Deleting genomes no longer scans `exported_feature` once per genome (#fix/wol3-genome-fold).** A
  plain index on `exported_feature.genome_idx` serves the foreign key's `SET NULL`
  lookup, which the partial live-genome index could not: deleting 196,062 genomes went
  from 8 min 46 s to 3.3 s.

### Fixed

- **Feature tables built against `web-of-life` `3-rc4` split every genome's reads
  between `G<n>` and `GCA_G<n>` (#fix/wol3-genome-fold).** That reference's load gave each of its
  196,062 sequences a second genome, `GCA_` followed by the WoL ID, beside the `G<n>`
  that `web-of-life` `3` had already linked to the same bytes, so each genome appeared
  in a table twice, each copy with half its reads. Once the operator runs `wol-genome-repair`,
  each sequence links to `G<n>` alone, the published `GCA_G…` labels are retired, and
  tables rebuilt against the reference are whole again.
