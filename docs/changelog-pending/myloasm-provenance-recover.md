### Added

- **The myloasm runs finished before long-read-assembly 1.0.2 can get their provenance
  tables back (#TBD).** A one-off workflow, `myloasm-provenance-recover` 1.0.0
  (system_admin only), reads one such run's assembler output from the ticket workspace
  that produced it and writes the run's six `myloasm_*` tables, as 1.0.2 does for a new
  run (#813). Reads are named by `sequence_idx`, looked up through the run's mask. Each
  contig gets its `feature_idx` by hashing the run's `genomes/*.fa` against the contigs
  that the run registered. The ticket fails before writing anything if any contig matches
  none of them, or any of them matches no contig. `qiita-admin backfill
  myloasm-provenance` finds each long-read-assembly 1.0.0/1.0.1 myloasm run's output
  and submits one ticket per run. It is a dry run by default; `--execute` submits, with
  `--limit`, `--rate` and `--retry-failed`. It lists every run that it cannot recover,
  with the reason.
- **A `read_masked` DoGet ticket can name the columns that it streams (#TBD).**
  `POST /api/v1/read-masked/ticket/doget` takes an optional `columns` list, drawn from
  the `read_masked` columns. An unknown or repeated name is a 422. Left out, the ticket
  streams every column, as before.
