# PacBio ingest (runbook)

**For:** whoever is ingesting a PacBio run (`qiita submit-pacbio-ingest`). Read it
before the first ingest on a new deploy — three of its behaviours surprise people.
Not needed for Illumina.

This covers only what is specific to PacBio. Everything shared with the Illumina
path — where to run the CLI, carrying a PAT to a headless host, the study and
biosample rows the pre-flight rows resolve against, building the pre-flight file
and pre-patching it, and the `--force` rule — is in
[`getting-started.md`](getting-started.md).

Paths and identifiers below are from the `qiita-miint.ucsd.edu` deploy; substitute your
host's checkout path, mounts, and `prep_protocol` indices.

## Submit

`$PF` is the writable, pre-patched copy of the pre-flight file
([`getting-started.md`](getting-started.md), step 4).

```bash
qiita --base-url https://qiita-miint.ucsd.edu/ submit-pacbio-ingest \
    --run-folder /sequencing/gcore_runs/Knightlab/r84137_20260623_040006 \
    --preflight-blob "$PF" \
    --instrument-run-id r84137_20260623_040006 \
    --instrument-model Revio \
    --prep-protocol-idx 3
```

- **`--instrument-run-id` is free-form.** PacBio has no `RunInfo.xml` to read it from,
  so nothing derives or validates it; the run-folder basename is the natural value.
- **`--prep-protocol-idx` is *not* validated against the platform.** A wrong value is
  accepted silently, so you have to get it right yourself. The command only accepts
  `pacbio_absquant` / `pacbio_metag` sheets, and both are metagenomics — so the answer
  is always the **`long_read_metagenomics`** protocol, never `long_read_amplicon`, no
  matter what the sheet filename suggests. Look the idx up on your deploy rather than
  copying a number (`qiita prep-protocol list`); on `qiita-miint` it is **3**, and the
  amplicon protocol you must *not* pick is 5.
- **Retry by re-running the identical command.** Run and pool are find-or-create, the
  roster is create-missing, and already-ingested samples come back `skipped`. Do not
  reach for `--force`.

## `pool-completion` does not report on ingest

Its per-sample buckets key on the **`read-mask`** action, and `demux_state` keys on
**`bcl-convert`**. So a freshly-ingested PacBio pool reads `samples_not_submitted: N`
and `demux_state: not_submitted` — that means *"not masked yet,"* not a failure.

PacBio never mints a `bcl-convert` ticket, so **`fully_processed` is permanently
`false` for a PacBio pool.** Use `complete` as the done signal instead.
