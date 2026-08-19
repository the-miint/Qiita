# Getting started (runbook)

**For:** anyone bringing a sequencing run into Qiita for the first time. It ends
with the run's reads registered in the lake and one work-ticket per sample to
watch. Masking, alignment, and feature tables come after and are not covered
here. Steps 0 through 4 are `user`-role; the Illumina submit in step 5 needs
`wet_lab_admin` or `system_admin`, the PacBio one does not.

One constraint fixes the order of everything below. The bundled submit gestures
(`qiita submit-bcl-convert`, `qiita submit-pacbio-ingest`) read a **pre-flight
file** and resolve each of its rows against a biosample and a study that must
**already exist** in Qiita, keyed on accession. If either lookup misses, the CLI
prints the unresolved accessions and exits without creating anything. So: study
first, then its biosamples, then a pre-flight file that names both by accession,
then the submit.

The pre-flight file itself is built **outside Qiita** (step 4).

Examples use `https://qiita-miint.ucsd.edu/`; substitute your deploy's host, its
checkout path, and its `prep_protocol` indices. You need an account on it: sign-up
is by invitation, and login goes through the deploy's OIDC provider, which must
return your email.

## 0. Get the CLI and a PAT

The CLI is the `qiita` console script from `qiita-control-plane`
(`uv tool install qiita-control-plane`). On a deploy host it is already installed
in the service venv — call **the binary directly**, e.g.
`/home/qiita/qiita-miint/qiita-control-plane/.venv/bin/qiita`. Wrap it in `uv run`
only from a checkout that is yours — `uv run` syncs the project first, which fails
(or half-succeeds) against one you do not own.

```bash
export QIITA_CONTROL_PLANE_URL=https://qiita-miint.ucsd.edu/
qiita login
```

Opens the AuthRocket LoginRocket Web flow in a browser; a localhost loopback
receiver writes the PAT to `~/.qiita/token` (mode `0600`). Later commands read
`$QIITA_TOKEN` first and fall back to that file.

Export `QIITA_CONTROL_PLANE_URL` in every shell you run `qiita` from. `login`
persists the token but not the host, and the flag's default is
`http://localhost:8080` — so a command without it silently targets your own
machine. The examples below assume it is set.

### Headless / remote hosts (carry the PAT)

`login` drives a browser **and** a localhost receiver, so both must be on the
same machine. On an SSH session, an HPC login node, or a CI runner that flow
cannot complete — a browser on your laptop would redirect to *your laptop's*
localhost. Carry the PAT instead:

```bash
# On a machine with a browser, once:
qiita login && cat ~/.qiita/token

# On the headless host:
export QIITA_CONTROL_PLANE_URL=https://qiita-miint.ucsd.edu/
export QIITA_TOKEN='<paste the PAT>'
qiita whoami          # no login runs here
```

The PAT identifies the Qiita principal regardless of the Unix account, so
`sudo -u qiita env QIITA_TOKEN=… qiita …` still acts as the token's owner.

Commands print the route's JSON response, so `| jq -r .study_idx` and friends
capture minted identifiers. `ticket logs` is the exception — it prints the job's
streams raw, since JSON-escaping them would make them unreadable.

## 1. Complete your profile (one time)

The authoring routes depend on `require_complete_profile`, so a study or biosample
create 422s until this is set; `POST /auth/pat` refuses to mint a token for the same
reason. `qiita login` itself does not check, which is why it can come first.

```bash
qiita profile set \
    --affiliation "Knight Lab" \
    --address "9500 Gilman Dr, La Jolla, CA 92093" \
    --phone "+1-858-555-0100"
```

Every flag is optional to the parser, but `profile_complete` only flips once
affiliation, address, and phone are all present, so set the three together.
`--orcid` and `--receive-processing-emails` / `--no-receive-processing-emails`
are genuinely optional.

## 2. Create the study

```bash
STUDY_IDX=$(qiita study create \
    --title "My first study" \
    --bioproject-accession PRJNA123456 | jq -r .study_idx)
```

`--bioproject-accession` is what makes the study reachable from a pre-flight
file: `POST /study/lookup-by-accession` resolves accession → `study_idx` keyed on
`bioproject_accession` by default, and that is the lookup the submit gestures
run. A study created without it cannot be named by a pre-flight row.

The column is `UNIQUE`, so one accession string names exactly one study. It is
**not** format-checked — the server only bounds its length — so a site whose
BioProject is not registered yet can agree on any string, as long as the identical
string goes into the pre-flight file.

The route sets `owner_idx` to your principal — the CLI surfaces no owner override
— and inserts an `ADMIN`-tier `study_access` row for you in the same transaction,
so every downstream call on this study passes its per-resource gate.

## 3. Create the biosamples

One call per biosample — `POST /study/{study_idx}/biosample` takes a single
object, there is no bulk import route:

```bash
BIOSAMPLE_IDX=$(qiita biosample create \
    --study-idx "$STUDY_IDX" \
    --owner-biosample-id-field-name sample_name \
    --owner-biosample-id-value SAMPLE-1 \
    --biosample-accession SAMN0000001 \
    --metadata "host taxon id=9606" | jq -r .biosample_idx)
```

`--metadata "host taxon id=…"` is **required**: intake enforces that one global
field and rejects an import without it. Pass the NCBI taxid of the host the sample
came from, or `not applicable` for a sample that has no host of its own (a blank,
a control, an environmental sample).

`--biosample-accession` is the pre-flight join key, exactly as
`--bioproject-accession` is for the study; the column is `UNIQUE` and is not
format-checked either. Without it the biosample exists but no pre-flight row can
reach it.

`--owner-biosample-id-field-name` names a study-local field holding your own name
for the sample. The field is created on first import, so it needs no setup. The
value does not travel with the biosample — `GET /biosample/{idx}` returns globally
linked metadata only — but on the study-scoped read every caller the study gate
admits sees it. The gate is what restricts it; the field's member-tier pin is not
enforced yet.

Further `--metadata KEY=VALUE` pairs write more metadata, but only against fields
that already resolve — a global field's `display_name`, or a study-local field you
created first with `qiita biosample create-field`. Import never creates a new local
field for a metadata key.

Adding a biosample to a study you do not own needs access only an operator can
grant — see [`auth.md`](../auth.md) under *User self-service*.

## 4. Build the pre-flight file (outside Qiita)

The pre-flight is a [kl-run-preflight](https://github.com/the-miint/kl-run-preflight)
SQLite file describing one sequencing run: its plates, samples, projects, and the
platform-specific index/barcode columns. Qiita pins it as a library dependency
(the commit is in `qiita-control-plane/pyproject.toml`) and it ships **no console
script**, so authoring is a short Python program against its public API.

### What Qiita reads out of it

| Pre-flight | Must match in Qiita |
|---|---|
| `input_sample.biosample_accession` | `biosample.biosample_accession` |
| the row's project → `project.bioproject_accession` | `study.bioproject_accession` |
| non-primary plate projects (controls only) | secondary studies on the sample |

A sample's project is its own `input_sample.project_idx` when set, otherwise its
plate's `primary_project_idx`.

Four conditions make the file unreadable before any Qiita call happens:

- A row of `sample_type` `standard` with a NULL `project_idx`, or a non-standard
  (control) row with a non-NULL one. This is checked first, ahead of accessions.
- Any NULL `biosample_accession` or `bioproject_accession` on a row that survives
  the filter. Both columns are nullable in the pre-flight schema, so a file that
  is perfectly valid to kl-run-preflight can still be unusable here.
- More than one `processing_run` in the file.
- No usable sample rows at all; rows flagged `do_not_use` are excluded.

### Authoring it

Produce the file from the run's legacy omnibus CSV, then fill in the two
accession columns:

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

Both setters key on the **pre-flight's own** names, not Qiita's:
`project_name` is a row in its `project` table, and `SAMPLE-1` is a
`Sample_Name` from the sheet. The accession is the only thing the two systems
share, which is why step 3 puts the sample's sheet name in
`--owner-biosample-id-value` — that keeps the human-readable correspondence
visible on both sides while the accession carries the join.

Each setter resolves its target by name, records the change in the file's
`change_log`, and commits; an ambiguous or unmatched name raises rather than
writing.

### Make it writable, and patch it before you submit

`open_db_file` opens the SQLite **read-write** and applies pending schema patches
in place. A shared `644` file owned by someone else therefore fails with `attempt
to write a readonly database`.

A pool's identity is the SHA-256 of the blob's bytes (`run_preflight_sha256`, a
stored generated column, is half the find-or-create key), and both submit gestures
read those bytes **before** opening and patching the file. Submitting an unpatched
file and then re-running would hash *different* bytes the second time and mint a
**second pool**. So copy it somewhere you own and pre-apply the patches once:

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
md5sum "$PF"   # baseline — unchanged after the submit
```

`run_preflight` ships as a dependency of `qiita-control-plane`, so use the
interpreter from the same venv as the `qiita` binary; a host `python` will not
import it.

Keep the patched copy for the life of the pool: a later mask submission wants
byte-identical content.

## 5. Submit the run

Run this step from a machine that sees the same filesystem as the cluster. The run
folder path is recorded on the ticket verbatim and re-resolved on a compute node
later, so it must be the cluster's path — a laptop can reach the control plane but
cannot see the data.

Pick the protocol every sample will be FK'd to. It is applied uniformly across the
pool and is **not** validated against the platform, so a wrong value is accepted
silently and mislabels every sample:

```bash
qiita prep-protocol list
```

### Illumina

Requires `wet_lab_admin` or `system_admin`: the workflow reads an operator
filesystem path verbatim, so end-user submission is disabled on the action.

```bash
qiita submit-bcl-convert \
    --bcl-input-dir /sequencing/250520_M05314_0001_000000000-ABCDE \
    --preflight-blob "$PF" \
    --prep-protocol-idx 1
```

The instrument run id and model come from the run folder's `RunInfo.xml`, so
there is no flag for them. The command creates the sequencing-run, the pool, and
one sequenced-sample per pre-flight row, then submits one pool-scoped
`bcl-convert` ticket. Run and pool are find-or-create and the roster is
create-missing — it reads the pool's existing roster and POSTs only the absent
samples — so re-running after a partial failure converges instead of erroring.

Each sample's `sequenced_pool_item_id` is set to its `illumina_sample_idx`,
because the sample sheet Qiita rehydrates emits that integer as `Sample_ID` — so
it is also the demux FASTQ's basename prefix.

### PacBio

`submit-pacbio-ingest` takes an already-demultiplexed run folder and fans out one
`bam-to-parquet` ticket per sample, locating each sample's BAM by its barcode. Read
[`pacbio-ingest.md`](pacbio-ingest.md) before the first PacBio run on a deploy:
it covers the flags with no Illumina counterpart, which protocol to pick, and why
`pool-completion` never reports a PacBio pool as fully processed.

### Never `--force` a retry

**To retry either gesture, re-run the identical command.** Both are convergent.

`--force` is not the retry mechanism, and it means different things on the two
paths. `bcl-convert`'s ticket is pool-scoped, and a submit over a COMPLETED pool
ticket is refused with a 409 — `--force` is what waives that refusal. PacBio's
tickets are per-sample, and a COMPLETED one never blocks a resubmit, so there is
nothing for `--force` to waive. On both it requires `wet_lab_admin` or
`system_admin`.

Forcing does not get you a clean re-ingest either way. Every read-ingest step mints
its sample's `sequence_range` through the same guard, which refuses a range minted
by a different work-ticket: the submit is admitted and then each sample fails
permanently at that step, with a message naming the DELETE that clears it. To
genuinely re-ingest, `qiita delete-sequenced-pool` and resubmit.

## 6. Watch it

```bash
qiita ticket list --active
qiita ticket status <idx>
qiita ticket logs <idx> --step-index 0
qiita ticket run <idx>        # re-dispatch a FAILED ticket in place
qiita pool-completion --sequencing-run-idx <run> --sequenced-pool-idx <pool>
```

States run `pending` → `queued` → `processing` → `completed`. Three more are
terminal. `no_data` means the step ran and produced nothing to register — routine
at plate scale, where a blank, a no-template control, or a failed-yield well gives
no reads — and the ticket is freely resubmittable. `cancelled` is an operator stop,
with no `failure_*` set, redrivable with `ticket run`. `failed` carries the
`failure_type` / `failure_stage` / `failure_step_name` / `failure_reason` surface;
recovery recipes for the read-ingest failures live in
[`fastq-to-parquet-retry-recovery.md`](fastq-to-parquet-retry-recovery.md).

## Not covered here

- **Authoring one sample by hand**, without a pre-flight file — creating the run,
  pool, and sequenced-sample yourself and submitting `fastq-to-parquet` against
  FASTQs you already have: [`user-cli-quickstart.md`](user-cli-quickstart.md).
- **Masking, alignment, feature tables** — what happens to the reads after
  ingest: `qiita submit-host-filter-pool`, `submit-block-mask-pool`,
  `submit-align-pool`, `feature-table build`.
- **Reference-data authoring** — `reference:write` is `wet_lab_admin`+; end users
  consume references but do not author them.
