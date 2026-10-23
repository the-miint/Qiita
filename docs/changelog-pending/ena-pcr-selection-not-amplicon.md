### Fixed

- **An ENA run deposited with `library_selection` PCR is no longer imported as shotgun
  metagenomics or transcriptomics (#622).** PCR selection marks an amplicon library, so a run
  whose `library_strategy` is not `AMPLICON` is refused per run with a `failure_reason` naming
  the selection, instead of landing under the wrong prep protocol. Runs already imported are not
  re-checked.
