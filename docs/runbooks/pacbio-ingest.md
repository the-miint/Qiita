# PacBio ingest (runbook)

**For:** whoever is ingesting a PacBio run (`qiita submit-pacbio-ingest`). Read it
before the first ingest on a new deploy — four of its behaviours surprise people, and
two of them (a second pool minted; reads duplicated in the lake) are expensive to undo.
Not needed for Illumina.

Auth and the general CLI flow are **not** repeated here — see
[`user-cli-quickstart.md`](user-cli-quickstart.md), whose *Headless / remote hosts
(carry the PAT)* section is the supported way to authenticate without a browser. This
runbook covers only what is specific to PacBio.

Paths and identifiers below are from the `qiita-miint.ucsd.edu` deploy; substitute your
host's checkout path, mounts, and `prep_protocol` indices.

## Where to run it

- **The CLI is the `qiita` console script in the deployed venv**:
  `/home/qiita/qiita-miint/qiita-control-plane/.venv/bin/qiita`. Call the binary
  **directly** — do *not* wrap it in `uv run`, which tries to sync the project and
  will fail (or half-succeed) against the qiita-owned checkout when you are another
  user.
- **Run it from a node that mounts both `/qmounts` and `/sequencing`** and can reach
  the control plane. A laptop can reach the control plane but cannot see the data,
  and run-folder paths are recorded on the ticket and re-resolved on a compute node
  later — so they must be the *cluster's* paths, not your laptop's. Workers have no
  `sudo`.
- The PAT identifies the Qiita principal regardless of the Unix account, so
  `sudo -u qiita env QIITA_TOKEN=… qiita …` still acts as the token's owner.

## The pre-flight `.db` must be writable by whoever runs the CLI

`run_preflight.open_db_file` opens the SQLite pre-flight **read-write** and applies
schema patches in place. A shared `644` pre-flight owned by `qiita` — the normal case
under `/qmounts/qiita_data/working_dir/…` — therefore fails with `attempt to write a
readonly database`.

Copy it somewhere you own, make it writable, and **pre-apply the patches once, before
submitting**:

```bash
cp "$SHARED_PF" "$PF" && chmod u+w "$PF"
"$QIITA_VENV/python" - "$PF" <<'PY'
import sys
from run_preflight import open_db_file
open_db_file(sys.argv[1]).close()
PY
md5sum "$PF"   # baseline — must be unchanged after the submit
```

**Pre-patching is not cosmetic.** The CLI hashes the blob's bytes *before* opening and
patching it, and pool identity is the SHA-256 of those bytes. Submitting against an
unpatched file and then re-running would hash *different* bytes and mint a **second
pool** instead of converging on the first. Keep the patched copy for the life of the
pool — a later mask submission wants byte-identical content.

## Submit

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
  roster is create-missing, and already-ingested samples come back `skipped`.
- **Never use `--force` to retry.** It re-ingests, duplicating the reads in the lake.

## `pool-completion` does not report on ingest

Its per-sample buckets key on the **`read-mask`** action, and `demux_state` keys on
**`bcl-convert`**. So a freshly-ingested PacBio pool reads `samples_not_submitted: N`
and `demux_state: not_submitted` — that means *"not masked yet,"* not a failure.

PacBio never mints a `bcl-convert` ticket, so **`fully_processed` is permanently
`false` for a PacBio pool.** Use `complete` as the done signal instead.

Watch an ingest with the ticket commands:

```bash
qiita ticket list --active
qiita ticket status <idx>
qiita ticket logs <idx> --step-index 0
qiita ticket run <idx>        # re-dispatch a FAILED ticket in place
```
