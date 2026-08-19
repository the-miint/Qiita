# PacBio ingest (runbook)

**For:** whoever is loading a PacBio run (`qiita submit-pacbio-ingest`). Read it
before the first PacBio run at your site — the things below catch people out.
Not needed for Illumina.

This covers only what is different about PacBio. Everything the two platforms
share — where to run the command, working from a remote machine, the studies and
samples the pre-flight file has to match, building that file and opening it once
first, how to retry, and why not to use `--force` — is in
[`getting-started.md`](getting-started.md), which these examples carry on from.

## Submit

`$PF` is your own writable copy of the pre-flight file, already opened once
([`getting-started.md`](getting-started.md), step 4).

```bash
qiita submit-pacbio-ingest \
    --run-folder /sequencing/gcore_runs/Knightlab/r84137_20260623_040006 \
    --preflight-blob "$PF" \
    --instrument-run-id r84137_20260623_040006 \
    --instrument-model Revio \
    --prep-protocol-idx 3
```

- **You have to supply `--instrument-run-id` yourself.** Illumina run folders
  carry the run's name in a file Qiita reads; PacBio's do not. Nothing checks
  what you type, so use the run folder's own name.
- **The protocol is always `long_read_metagenomics`.** This command only takes
  the two PacBio sheet types and both are metagenomics, so it is never
  `long_read_amplicon`, whatever the sheet is called. Look the number up on your
  own site with `qiita prep-protocol list` rather than copying one — on
  `qiita-miint` it is **3**, and **5** is the amplicon protocol you must not
  pick. Nothing will catch a wrong number for you.
- **Re-running reports samples that are already loaded as `skipped`.**

## `pool-completion` will not tell you the load finished

That command reports on host-filtering, and on Illumina demultiplexing — neither
of which a freshly loaded PacBio run has done yet. So it reads
`samples_not_submitted: N` and `demux_state: not_submitted`, which here means
"not filtered yet", not "something went wrong".

PacBio runs are never demultiplexed by Qiita, so **`fully_processed` stays
`false` for a PacBio pool forever.** Use `complete` as the signal that the
samples are done instead.
