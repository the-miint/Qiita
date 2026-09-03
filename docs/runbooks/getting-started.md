# Getting started (runbook)

**For:** anyone bringing a sequencing run into Qiita for the first time. It ends
with the run's reads stored in Qiita and a job you can watch — one for the whole
run on Illumina, one per prep_sample on PacBio. Read masking (host-filtering and
the rest), alignment and feature tables come after this and are covered elsewhere.

Steps 0 through 4 need nothing but your own account. Step 5 needs a
`wet_lab_admin` account whichever platform you are on — it names a folder on the
sequencer's filesystem, and naming one is reserved — so if you do not have that
account, hand the last step to someone who does.

**Do the steps in order.** The submit reads a **pre-flight file** — the sheet
describing your run — and for every row in it looks up a biosample and a study that
must already be in Qiita. It matches them on accessions, and if it cannot find
one it tells you which and stops without creating anything. So the study comes
first, then its biosamples, then a pre-flight file naming both by accession, then
the submit.

You build the pre-flight file yourself, outside Qiita (step 4).

Examples use `https://qiita-miint.ucsd.edu/` — substitute your own site, its
paths, and its protocol numbers. You need an account: sign-up is by invitation,
and logging in goes through your site's identity provider, which has to give
Qiita your email address.

## 0. Get the CLI and log in

Qiita has no web interface. Everything below is the `qiita` command, which has to
be installed and on your `$PATH`.

On a shared analysis host it is usually installed already; if `qiita` is not found,
ask your operator for its path and run it by that path. To install it on your own
machine you need a git clone of the Qiita repository, and then
`uv tool install ./qiita-control-plane` from the top of it — ask your operator
which revision your site is running. Do not put `uv run` in front of a `qiita`
that someone else installed: it tries to reinstall the project and fails.

```bash
export QIITA_CONTROL_PLANE_URL=https://qiita-miint.ucsd.edu/
qiita login
```

This opens your browser to log in, then writes your access token to
`~/.qiita/token`, readable only by you. Later commands pick it up from there.

It needs a browser **and** the `qiita` command on the same machine, so it cannot
finish over SSH, on a cluster login node, or in CI — a browser on your laptop
would send the token back to your laptop, not to the machine you are typing on.
If that is where you are working, see *Working on a remote machine* below.

Set `QIITA_CONTROL_PLANE_URL` in every shell you use `qiita` from. Logging in
saves your token but not the address of the site, and with no address the command
talks to your own machine instead — where nothing is listening. The examples
below assume you have set it.

Tokens expire, and are revocable. If a command that worked yesterday comes back
`401`, that is what happened: run `qiita login` again (or re-copy the token, if
you are working from a remote machine).

### Working on a remote machine

Log in once where you do have a browser, then carry the token across:

```bash
# On your laptop, once:
qiita login && cat ~/.qiita/token

# On the remote machine:
export QIITA_CONTROL_PLANE_URL=https://qiita-miint.ucsd.edu/
export QIITA_TOKEN='<paste the token string>'
qiita whoami          # no login needed here
```

The token is what identifies you, whatever Unix account you are using — so
`sudo -u qiita env QIITA_TOKEN=… qiita …` still acts as you.

Most commands print their result as JSON, so you can pull out an identifier with
`| jq -r .study_idx` and similar. A few print for reading instead of parsing —
`qiita ticket logs` prints a job's output as-is, and so do `feature-table build`
and `reference export`.

## 1. Fill in your profile (once)

Qiita will not create studies or biosamples for you, or issue you a token, until
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

Register the study with NCBI BioProject first and use the accession it issues
(up to 50 characters). Do not invent one: an accession is a handle someone
reading your published data will try to resolve, and a made-up value resolves to
nothing. The *identical* string then goes into the pre-flight file.

The study is yours: you can do everything below on it without anyone granting you
access.

## 3. Create the biosamples

A biosample is the physical sample itself, independent of any sequencing. One
command per biosample — there is no bulk import yet, so for a plate this is a
loop over your sample list.

```bash
BIOSAMPLE_IDX=$(qiita biosample create \
    --study-idx "$STUDY_IDX" \
    --owner-biosample-id-field-name sample_name \
    --owner-biosample-id-value SAMPLE-1 \
    --biosample-accession SAMN0000001 \
    --metadata "host taxon id=9606" | jq -r .biosample_idx)
```

`--biosample-accession` does for the biosample what `--bioproject-accession` did
for the study: it is the only thing the pre-flight file can match on. Register the
sample with NCBI BioSample and use the accession it issues — same rule as above,
do not invent one. Leave it out and the biosample exists but no pre-flight row can
reach it.

`--metadata "host taxon id=…"` is **required**. It takes an NCBI taxonomy id — the
number, not the name — matched against the NCBI Taxonomy your site has loaded. How
much that covers is a per-site choice: a deploy starts with a small seeded set
(`9606` human, `10090` mouse, a few metagenome taxa), and an operator can load a
full NCBI release, after which the whole taxonomy is accepted. An id your site does
not have is refused, and there is no self-service way to list or add one — ask your
operator. For a biosample with no host of its own — a blank, a control, an
environmental sample — use `not applicable`.

`--owner-biosample-id-value` is your own name for the biosample, stored under the
field named by `--owner-biosample-id-field-name` (the field is created the first
time you use it, so there is nothing to set up). This is the name to match your
sheet, since the accession is what the machines match on and rarely what you say
out loud. It does not travel with the biosample: someone reading the biosample on
its own sees only the shared metadata, but anyone with access to *this study* sees
it, so keep any personally identifying information out of it.

You can add more `--metadata KEY=VALUE` pairs, but only for fields that already
exist: a standard Qiita field, or one you created for this study. A key that
matches nothing is an error, not a new field. To add a field of your own to the
study first:

```bash
qiita biosample create-field \
    --study-idx "$STUDY_IDX" \
    --display-name "collection depth m" \
    --data-type numeric
```

`--data-type` is one of `text`, `numeric`, `boolean`, `date` or `terminology`, and
the display name you give is the `KEY` you then pass to `--metadata`.

Putting a biosample into a study you do not own needs access only an operator can
grant — see [`auth.md`](../auth.md) under *User self-service*.

## 4. Build the pre-flight file

The pre-flight file is a [kl-run-preflight](https://github.com/the-miint/kl-run-preflight)
SQLite file describing one sequencing run — its plates, its rows of samples, its projects
and the barcodes or indices for the platform. It is normally produced by whoever
prepped the run. Qiita only reads it, and it has no command-line tool, so the
snippets below are short Python.

### What Qiita takes from it

| From the pre-flight file | Has to match |
|---|---|
| each row's `biosample_accession` | a biosample you created in step 3 |
| each row's project `bioproject_accession` | a study you created in step 2 |
| any extra projects on the plate (controls only) | further studies to link that row to |

A row's project is the one set on the row itself, or, if it has none, its plate's
main project.

Qiita refuses the file outright — before creating anything — if:

- The file does not describe exactly one run — zero runs is refused as well as
  two. This is checked first.
- A row of the standard sample type has no project, or a control row has one.
  The pre-flight file's two control types — an extraction blank and a
  KatharoSeq positive control — both take their project from the plate, which is
  why neither may carry one of its own. This is checked before the accessions,
  so it is the only error you see until it is fixed.
- Any `biosample_accession` or `bioproject_accession` it needs is still empty.
  The pre-flight format allows them to be empty, so a file that is perfectly
  valid otherwise can still be unusable here. This is the common one.
- There are no usable rows left — rows marked `do_not_use` do not count.

Those are the checks on the file's *contents*. A file Qiita cannot read at all
— not a regular file, empty, or not a pre-flight database — is refused before
any of them.

### Filling in the accessions

Build the file from the run's samplesheet CSV — the one describing the whole
run, with its plates, projects and per-row detail, which is what
`migrate_legacy_csv_to_db_file` reads. It is not `SampleSheet.csv`, the narrow
file bcl-convert consumes; run_preflight *emits* that one. Then set the two
accessions so they match what you created in steps 2 and 3:

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
now-upgraded bytes — a different file as far as Qiita is concerned. Your retry is
refused: the run already has a pool under that filename, and the contents no
longer match it. Open the file once yourself first and the bytes stop changing,
so a retry is a retry:

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

Both commands need a `wet_lab_admin` account. Run either from anywhere that can
reach Qiita: Qiita opens the run folder itself to read what it needs, so your own
machine does not have to see it. What you type is the path *as the cluster sees
it* — it is stored as given and re-opened on a compute node later.

Your site allows run folders only under certain directories. A path outside them
is refused at your terminal, listing the ones that are allowed, instead of being
accepted and failing inside a job hours later. Ask your operator which they are.
The pre-flight file is the exception: `--preflight-blob` is read off the machine
you are typing on and sent with the request, so that one does have to be local
(step 4).

Choose the protocol for the run. It is applied to every prep_sample and **nothing
checks it against the platform**, so a wrong number here silently mislabels the
whole run:

```bash
qiita prep-protocol list
```

### Illumina

```bash
qiita submit-bcl-convert \
    --bcl-input-dir /sequencing/250520_M05314_0001_000000000-ABCDE \
    --preflight-blob "$PF" \
    --prep-protocol-idx 1
```

The run's name and instrument model are read out of the run folder, so there are
no flags for them. One command creates the run, the pool and one prep_sample per
pre-flight row, then queues the demultiplexing job. Re-running is safe: it reuses
what it already made and adds only what is missing.

Each prep_sample is labelled with the identifier the pre-flight file gives its
row, which is also the prefix `bcl-convert` puts on that prep_sample's FASTQ
files — so you can tell from a filename which one it belongs to. It is not the
row's position in the sheet, so read it off the file rather than counting.

### PacBio

`qiita submit-pacbio-ingest` takes a run folder that is already demultiplexed and
queues one job per prep_sample, finding each one's BAM by its barcode. Before your
first PacBio run on a site, read [`pacbio-ingest.md`](pacbio-ingest.md): it
covers the flags Illumina does not have, which protocol to choose, and why
`pool-completion` never calls a PacBio pool finished.

### Retrying, and what `--force` is for

**To retry either command, run it again unchanged.** Both pick up where they left
off.

`--force` is not how you retry, and on Illumina it is the one thing here that can
damage what you already have.

It gets a submission past a single refusal: on Illumina, submitting again over a
run whose demultiplexing already completed is refused, and `--force` waives that.
On PacBio nothing refuses you in the first place, so it changes nothing at all
there. It needs a `wet_lab_admin` account either way.

**On Illumina, forcing stores the run's reads a second time.** The re-run finds
each prep_sample's reads already staged from the first run and files them again;
nothing removes the first copy, and nothing merges them. If what you want is to
load the run's reads afresh, delete the pool with `qiita delete-sequenced-pool`
— which removes its jobs too — and submit again.

On PacBio a re-submit stops instead: each prep_sample's reads are numbered once
when they are first loaded, and the second attempt finds those numbers taken and
stops before storing anything. That is the failed-looking job the retry note
above tells you to expect.

## 6. Watch it run

```bash
qiita ticket list --active
qiita ticket status <idx>
qiita ticket logs <idx> --step-index 0
qiita ticket run <idx>        # re-dispatch a failed job, resuming at the first unfinished step
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

- **One prep_sample at a time, with no pre-flight file** — registering a run, a
  pool and a prep_sample by hand and loading reads you already hold, from the
  machine you are typing on (`qiita submit-reads`) or from the cluster:
  [`manual-sample-walkthrough.md`](manual-sample-walkthrough.md).
- **What happens to the reads next** — read masking (host-filtering among it),
  alignment and feature tables: `qiita submit-host-filter-pool`, `submit-block-mask-pool`,
  `submit-align-pool`, `feature-table build`.
- **Loading reference databases** — reserved for `wet_lab_admin` and above;
  everybody else uses the references already loaded.
