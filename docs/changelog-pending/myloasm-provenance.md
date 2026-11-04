### Added

- **A myloasm assembly keeps its read-level provenance (#813).** long-read-assembly
  1.0.2 dumps the six tables duckdb-miint reads out of a myloasm output directory and
  loads them into six new DuckLake tables: `myloasm_contig`, `myloasm_layout`,
  `myloasm_mapping`, `myloasm_link`, `myloasm_split` and `myloasm_run`. They hold which
  reads built each contig, where every read mapped, the contig graph's edges, the
  reads myloasm split, and the run's settings, keyed on the assembly run. No sequence
  bytes are stored. A read is named by its `sequence_idx`, a contig by the `N` of
  myloasm's `u<N>ctg` name and, for a primary contig, by its `feature_idx`. Each
  registration replaces the run's rows. hifiasm_meta runs write none. On myloasm 0.6.0
  a graph edge carries no overlap evidence, so those columns are NULL
  (duckdb-miint#358). The contig number is parsed from myloasm's name, since the
  readers expose none (duckdb-miint#389).

### Changed

- **The assembler's reads are named by `sequence_idx` (#813).** The FASTQ that
  long-read-assembly hands its assembler now names each read by its `sequence_idx`
  instead of the sequencer's `read_id`, so anything the assembler writes about a read
  joins to `read` directly. This is the runner's, so it applies to every version of
  the workflow, not only 1.0.2. Contigs are unaffected: myloasm 0.6.0 and
  hifiasm_meta produced byte-identical contigs, names included, from the same reads
  under both namings.
