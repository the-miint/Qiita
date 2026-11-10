### Fixed

- **Registration removes the staged copy of a file a native job wrote into a subdirectory
  (#TBD).** Native SLURM jobs now run with umask `0007`, so a directory a job creates
  under its output path is group-writable and the data plane can unlink each file it
  moves into the lake. Before, those directories came out `2755`; the move succeeded,
  the unlink failed with `Permission denied`, and the staged copy stayed in the ticket
  workspace. This affected the `read/` directory of bcl-convert, golay-demux,
  download-ena-study and bam-to-parquet, the staging directory of amplicon,
  long-read-assembly and myloasm-provenance-recover, and `reference_sequence_chunks/` of
  the reference-add workflows. Files already left behind are not removed by this change.
  Container steps are unchanged.
