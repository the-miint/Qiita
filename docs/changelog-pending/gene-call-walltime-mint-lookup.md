### Fixed

- **`gene_call` gets a 4 h walltime, and the feature lookup in `mint-features` /
  `mint-gene-features` no longer runs into the 10 s DB timeout (#N).** long-read-assembly
  1.0.2's and assembly-gene-call-recover 1.0.0's `gene_call` step goes from `PT2H` to `PT4H`
  (both edited in place); two of the five recovery canary jobs finished at 2 h 09 min.
  Minting now looks up and inserts `qiita.feature` 2,000 hashes at a time instead of
  10,000, each statement with a 60 s timeout; a 10,000-hash lookup measured 10.9 s on the
  live DB.
