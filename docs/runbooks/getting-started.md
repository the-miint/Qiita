# Getting started (runbook)

**For:** anyone bringing a sequencing run into Qiita for the first time. It ends
with the run's reads stored in Qiita and a job you can watch — one for the whole
run on Illumina, one per sample on PacBio. Read masking (host-filtering and the
rest), alignment and feature tables come after this and are covered elsewhere.

Everything up to the submit you can do yourself. The Illumina submit in step 5
needs a `wet_lab_admin` account; the PacBio one does not.

**Do the steps in order.** The submit reads a **pre-flight file** — the sheet
describing your run — and for every row in it looks up a sample and a study that
must already be in Qiita. It matches them on accessions, and if it cannot find
one it tells you which and stops without creating anything. So the study comes
first, then its samples, then a pre-flight file naming both by accession, then
the submit.

You build the pre-flight file yourself, outside Qiita (step 4).

Examples use `https://qiita-miint.ucsd.edu/` — substitute your own site, its
paths, and its protocol numbers. You need an account: sign-up is by invitation,
and logging in goes through your site's identity provider, which has to give
Qiita your email address.

## 0. Get the CLI and log in

Qiita has no web interface. Everything below is the `qiita` command. On a shared
analysis host it is usually installed already — run the binary directly, e.g.
`/home/qiita/qiita-miint/qiita-control-plane/.venv/bin/qiita`. Only put `uv run`
in front of it if the checkout is your own; against someone else's it tries to
reinstall the project and fails. To get it on your own machine, install it from a
checkout of the repository (`uv tool install ./qiita-control-plane`) — ask your
operator which revision your site is running.

```bash
export QIITA_CONTROL_PLANE_URL=https://qiita-miint.ucsd.edu/
qiita login
```

This opens your browser to log in, then writes your access token to
`~/.qiita/token`, readable only by you. Later commands pick it up from there.

Set `QIITA_CONTROL_PLANE_URL` in every shell you use `qiita` from. Logging in
saves your token but not the address of the site, and with no address the command
talks to your own machine instead — where nothing is listening. The examples
below assume you have set it.

### Working on a remote machine

`qiita login` needs a browser and the command on the *same* machine, so it cannot
finish over SSH, on a cluster login node, or in CI. Log in once where you have a
browser, then carry the token:

```bash
# On your laptop, once:
qiita login && cat ~/.qiita/token

# On the remote machine:
export QIITA_CONTROL_PLANE_URL=https://qiita-miint.ucsd.edu/
export QIITA_TOKEN='<paste the token>'
qiita whoami          # no login needed here
```

The token is what identifies you, whatever Unix account you are using — so
`sudo -u qiita env QIITA_TOKEN=… qiita …` still acts as you.

Commands print their result as JSON, so you can pull out an identifier with
`| jq -r .study_idx` and similar. The exception is `qiita ticket logs`, which
prints a job's output as-is so it stays readable.

## 1. Fill in your profile (once)

Qiita will not create studies or samples for you, or issue you a token, until
your profile has an affiliation, an address and a phone number. Logging in does
not check, which is why you can do it after logging in.

```bash
qiita profile set \
    --affiliation "Knight Lab" \
    --address "9500 Gilman Dr, La Jolla, CA 92093" \
    --phone "+1-858-555-0100"
```

Set all three together — the profile only counts as complete when none is blank.
`--orcid` and `--receive-processing-emails` / `--no-receive-processing-emails`
are optional.

## 2. Create the study

```bash
STUDY_IDX=$(qiita study create \
    --title "My first study" \
    --bioproject-accession PRJNA123456 | jq -r .study_idx)
```

`--bioproject-accession` is how your pre-flight file will find this study, so
give it one now. A study without an accession cannot be named by a pre-flight
row at all, and no two studies may share the same one.

Qiita does not check the shape of the accession, only that it is not empty and no
longer than 50 characters — so if your BioProject is not registered yet, any
agreed string works, as long as the *identical* string goes into the pre-flight
file.

The study is yours: you can do everything below on it without anyone granting you
access.

## 3. Create the samples

One command per sample. There is no bulk import yet, so for a plate this is a
loop over your sample list.

```bash
BIOSAMPLE_IDX=$(qiita biosample create \
    --study-idx "$STUDY_IDX" \
    --owner-biosample-id-field-name sample_name \
    --owner-biosample-id-value SAMPLE-1 \
    --biosample-accession SAMN0000001 \
    --metadata "host taxon id=9606" | jq -r .biosample_idx)
```

`--biosample-accession` does for the sample what `--bioproject-accession` did for
the study: it is the only thing the pre-flight file can match on. Same rules —
unique, and unchecked as to shape. Leave it out and the sample exists but no
pre-flight row can reach it.

`--metadata "host taxon id=…"` is **required**. It takes an NCBI taxonomy id, but
only from the list your site has loaded — `9606` for human and `10090` for mouse
are there, along with a handful of metagenome taxa. Anything else is rejected, and
adding a term is an operator job, so ask if the host you need is missing. For a
sample with no host of its own — a blank, a control, an environmental sample —
use `not applicable`.

`--owner-biosample-id-value` is your own name for the sample, stored under the
field named by `--owner-biosample-id-field-name` (the field is created the first
time you use it, so there is nothing to set up). This is the name to match your
sheet, since the accession is what the machines match on and rarely what you say
out loud. It does not travel with the sample: someone reading the sample on its
own sees only the shared metadata, but anyone with access to *this study* sees
it, so keep patient identifiers out of it.

You can add more `--metadata KEY=VALUE` pairs, but only for fields that already
exist — a standard Qiita field, or one you created for this study first with
`qiita biosample create-field`. A key that matches nothing is an error, not a new
field.

Putting a sample into a study you do not own needs access only an operator can
grant — see [`auth.md`](../auth.md) under *User self-service*.

## 4. Build the pre-flight file

The pre-flight file is a [kl-run-preflight](https://github.com/the-miint/kl-run-preflight)
SQLite file describing one sequencing run — its plates, its samples, its projects
and the barcodes or indices for the platform. It is normally produced by whoever
prepped the run. Qiita only reads it, and it has no command-line tool, so the
snippets below are short Python.

### What Qiita takes from it

| From the pre-flight file | Has to match |
|---|---|
| each sample's `biosample_accession` | a sample you created in step 3 |
| each sample's project `bioproject_accession` | a study you created in step 2 |
| any extra projects on the plate (controls only) | further studies the sample belongs to |

A sample's project is the one set on the sample itself, or, if it has none, its
plate's main project.

Qiita refuses the file outright — before creating anything — if:

- A normal sample has no project, or a control has one. Controls take their
  project from the plate, so this pairing has to be the right way round. This is
  checked before the accessions, so it is the only error you see until it is fixed.
- Any `biosample_accession` or `bioproject_accession` it needs is still empty.
  The pre-flight format allows them to be empty, so a file that is perfectly
  valid otherwise can still be unusable here. This is the common one.
- The file describes more than one run.
- There are no usable samples left — rows marked `do_not_use` do not count.

### Filling in the accessions

Build the file from the run's omnibus CSV, then set the two accessions so they
match what you created in steps 2 and 3:

```python
from run_preflight import (
    migrate_legacy_csv_to_db_file,
    open_db_file,
    set_bioproject_accession,
    set_biosample_accession,
)

migrate_legacy_csv_to_db_file("run.csv", "preflight.db")
conn = open_db_file("preflight.db")
set_bioproject_accession(conn, "PRJNA123456", project_name="KnightLab_Run1")
set_biosample_accession(conn, "SAMPLE-1", "SAMN0000001")
conn.close()
```

Both calls find their target by **the pre-flight file's own names** — the project
name in the sheet, and the sheet's sample name — and set the accession on it. The
accession is the only thing the two systems share. Each call saves as it goes,
and refuses rather than guessing if the name matches nothing or matches more than
one row.

### Make your own copy first, and open it once before submitting

Opening a pre-flight file modifies it: the library upgrades it in place. Two
things follow.

A file you cannot write — the usual `644` copy owned by someone else on a shared
filesystem — cannot be opened at all: you get `attempt to write a readonly
database`. Copy it somewhere you own.

More important: Qiita identifies a pool by the exact bytes of the file you hand
it, and the submit reads those bytes *before* opening it. Submit a file that has
never been opened, then re-run to retry, and the second submit sends the
now-upgraded bytes — a different file as far as Qiita is concerned, so you get a
second pool instead of the retry you wanted. Open it once yourself first, and the
bytes stop changing:

```bash
SHARED_PF=/qmounts/qiita_data/working_dir/RunPreflight.db   # the lab's copy
PF="$HOME/preflight/RunPreflight.db"                        # yours
QIITA_VENV=/home/qiita/qiita-miint/qiita-control-plane/.venv

mkdir -p "$(dirname "$PF")" && cp "$SHARED_PF" "$PF" && chmod u+w "$PF"
"$QIITA_VENV/bin/python" - "$PF" <<'PY'
import sys
from run_preflight import open_db_file
open_db_file(sys.argv[1]).close()
PY
md5sum "$PF"   # note this down — it should not change when you submit
```

Use the interpreter from the same place as the `qiita` command, as above; a
system `python` will not have the library.

Keep your copy for as long as the run matters. Later steps expect the same file,
byte for byte.

## 5. Submit the run

Run this from a machine that sees the same files as the cluster. The path you
give is stored as-is and re-opened later on a compute node, so it has to be the
cluster's path — your laptop can talk to Qiita but cannot see the data.

Choose the protocol for the run. It is applied to every sample and **nothing
checks it against the platform**, so a wrong number here silently mislabels the
whole run:

```bash
qiita prep-protocol list
```

### Illumina

Needs a `wet_lab_admin` account — this one reads a path on the sequencer's
filesystem directly, so it is not open to everybody.

```bash
qiita submit-bcl-convert \
    --bcl-input-dir /sequencing/250520_M05314_0001_000000000-ABCDE \
    --preflight-blob "$PF" \
    --prep-protocol-idx 1
```

The run's name and instrument model are read out of the run folder, so there are
no flags for them. One command creates the run, the pool and one entry per
pre-flight sample, then queues the demultiplexing job. Re-running is safe: it
reuses what it already made and adds only what is missing.

Each sample is labelled with the identifier the pre-flight file gives its row,
which is also the prefix `bcl-convert` puts on that sample's FASTQ files — so you
can tell from a filename which sample it belongs to. It is not the row's position
in the sheet, so read it off the file rather than counting.

### PacBio

`qiita submit-pacbio-ingest` takes a run folder that is already demultiplexed and
queues one job per sample, finding each sample's BAM by its barcode. Before your
first PacBio run on a site, read [`pacbio-ingest.md`](pacbio-ingest.md): it
covers the flags Illumina does not have, which protocol to choose, and why
`pool-completion` never calls a PacBio pool finished.

### Retrying, and why not to use `--force`

**To retry either command, run it again unchanged.** Both pick up where they left
off.

`--force` is not how you retry, and it does not mean the same thing on both. For
Illumina, submitting again over a run that already completed is refused, and
`--force` is what overrides that refusal. For PacBio nothing refuses you in the
first place, so there is nothing to override. Either way it needs a
`wet_lab_admin` account.

Forcing will not get you a clean re-ingest. Each sample's reads are numbered once
when they are first loaded, and a fresh submit finds those numbers already taken
and stops, telling you to delete first. If you really do want to load the reads
again, delete the pool with `qiita delete-sequenced-pool` and submit again.

## 6. Watch it run

```bash
qiita ticket list --active
qiita ticket status <idx>
qiita ticket logs <idx> --step-index 0
qiita ticket run <idx>        # start a failed job over from the beginning
qiita pool-completion --sequencing-run-idx <run> --sequenced-pool-idx <pool>
```

A job goes `pending` → `queued` → `processing` → `completed`. Three other endings
are possible. `no_data` means the job ran fine and there was nothing to store —
ordinary at plate scale, where a blank, a no-template control or a well that
failed to yield gives no reads — and you can resubmit it later if that was a
surprise. `cancelled` means somebody stopped it; `qiita ticket run` restarts it.
`failed` comes with the reason and the step it failed at; recovery for the
read-loading failures is in
[`fastq-to-parquet-retry-recovery.md`](fastq-to-parquet-retry-recovery.md).

## Not covered here

- **One sample at a time, with no pre-flight file** — registering a run, pool and
  sample by hand and processing FASTQs you already hold:
  [`user-cli-quickstart.md`](user-cli-quickstart.md).
- **What happens to the reads next** — read masking (host-filtering among it),
  alignment and feature tables: `qiita submit-host-filter-pool`, `submit-block-mask-pool`,
  `submit-align-pool`, `feature-table build`.
- **Loading reference databases** — reserved for `wet_lab_admin` and above;
  everybody else uses the references already loaded.
