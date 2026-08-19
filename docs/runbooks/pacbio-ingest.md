# PacBio ingest (runbook)

**For:** whoever is ingesting a PacBio run (`qiita submit-pacbio-ingest`). Read it
before the first ingest on a new deploy — three of the things below surprise people.
Not needed for Illumina.

This covers only what is specific to PacBio. Everything shared with the Illumina
path — where to run the CLI, carrying a PAT to a headless host, the study and
biosample rows the pre-flight rows resolve against, building the pre-flight file
and pre-patching it, retrying, and the `--force` rule — is in
[`getting-started.md`](getting-started.md), whose examples this one continues.

## Submit

`$PF` is the writable, pre-patched copy of the pre-flight file
([`getting-started.md`](getting-started.md), step 4).

```bash
qiita submit-pacbio-ingest \
    --run-folder /sequencing/gcore_runs/Knightlab/r84137_20260623_040006 \
    --preflight-blob "$PF" \
    --instrument-run-id r84137_20260623_040006 \
    --instrument-model Revio \
    --prep-protocol-idx 3
```

- **`--instrument-run-id` is free-form.** PacBio has no `RunInfo.xml` to read it from,
  so nothing derives or validates it; the run-folder basename is the natural value.
- **The protocol is always `long_read_metagenomics`.** The command only accepts
  `pacbio_absquant` / `pacbio_metag` sheets and both are metagenomics, so it is never
  `long_read_amplicon`, no matter what the sheet filename suggests. Look the idx up on
  your deploy rather than copying a number (`qiita prep-protocol list`); on
  `qiita-miint` it is **3**, and the amplicon protocol you must *not* pick is 5.
  Nothing validates this for you — see the protocol note in
  [`getting-started.md`](getting-started.md), step 5.
- **A re-run reports already-ingested samples as `skipped`.**

## `pool-completion` does not report on ingest

Its per-sample buckets key on the **`read-mask`** action, and `demux_state` keys on
**`bcl-convert`**. So a freshly-ingested PacBio pool reads `samples_not_submitted: N`
and `demux_state: not_submitted` — that means *"not masked yet,"* not a failure.

PacBio never mints a `bcl-convert` ticket, so **`fully_processed` is permanently
`false` for a PacBio pool.** Use `complete` as the done signal instead.
