### Fixed

- **`gene_call` gets a 4 h walltime, and feature minting looks up `qiita.feature` in
  smaller batches with a longer timeout (#N).** long-read-assembly 1.0.2's and
  assembly-gene-call-recover 1.0.0's `gene_call` step goes from `PT2H` to `PT4H` (both
  edited in place). Every action that mints through `mint_features` (`mint-features`,
  `mint-gene-features`, `mint-annotation-features`) now looks up and inserts
  `qiita.feature` 2,000 hashes at a time instead of 10,000, each statement with a 60 s
  timeout in place of the connection pool's default; a 10,000-hash lookup had reached
  that default.
