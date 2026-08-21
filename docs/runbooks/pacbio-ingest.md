# PacBio ingest (runbook)

**For:** whoever is loading a PacBio run (`qiita submit-pacbio-ingest`). Read it
before the first PacBio run at your site — the things below catch people out.
Not needed for Illumina.

This covers only what is different about PacBio. Everything the two platforms
share — where to run the command, working from a remote machine, the studies and
biosamples the pre-flight file has to match, building that file and opening it once
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
  the two PacBio sheet types and both are metagenomics, so it is never the
  amplicon protocol, whatever the sheet is called. Look the number up on your own
  site with `qiita prep-protocol list` rather than copying one from anywhere —
  nothing will catch a wrong number for you, and a wrong one mislabels every
  prep_sample in the run.
- **Re-running the identical command is the retry**, but it is not free. The run
  and pool are reused and missing prep_samples are added, and any whose job is
  still running are reported `skipped`. A prep_sample whose reads already loaded
  is **not** skipped — it gets a fresh job, which then stops at the read-numbering
  step because its reads are already numbered. That is safe (nothing is stored
  twice) but it shows up as a failed job, so expect it and do not chase it.

## `pool-completion` will not tell you the load finished

It reports on two later things, not on the load. Its per-prep_sample counts are
about read masking — PacBio runs **are** host-filtered, exactly like Illumina
ones, via `qiita submit-host-filter-pool`; you just have not done it yet at this
point. Its `demux_state` is about Illumina demultiplexing, which PacBio never
does at all, because the instrument delivers the reads already demultiplexed.

So a freshly loaded PacBio pool reads `samples_not_submitted: N` and
`demux_state: not_submitted`. The first means "not masked yet" and changes once
you mask; the second never changes. Because it never changes, **`fully_processed`
stays `false` for a PacBio pool forever** — use `complete` as the signal that the
prep_samples are done instead.
