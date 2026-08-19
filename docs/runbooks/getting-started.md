# Getting started (runbook)

**For:** anyone bringing a sequencing run into Qiita for the first time. It ends
with the run's reads registered in the lake and one work-ticket per sample to
watch. Masking, alignment, and feature tables come after and are not covered
here.

One constraint fixes the order of everything below. The bundled submit gestures
(`qiita submit-bcl-convert`, `qiita submit-pacbio-ingest`) read a **pre-flight
file** and resolve each of its rows against a biosample and a study that must
**already exist** in Qiita, keyed on accession. If either lookup misses, the CLI
prints the unresolved accessions and exits without creating anything. So: study
first, then its biosamples, then a pre-flight file that names both by accession,
then the submit.

The pre-flight file itself is built **outside Qiita** (step 4).

Examples use `https://qiita-miint.ucsd.edu/`; substitute your deploy's host, its
checkout path, and its `prep_protocol` indices.

## 0. Get the CLI and a PAT

The CLI is the `qiita` console script from `qiita-control-plane`
(`uv tool install qiita-control-plane`). On a deploy host it is already installed
in the service venv — call **the binary directly**, e.g.
`/home/qiita/qiita-miint/qiita-control-plane/.venv/bin/qiita`. Do not wrap it in
`uv run`, which tries to sync the project and fails (or half-succeeds) against a
checkout you do not own.

Run it from a machine that can reach the control plane **and** sees the same
filesystem as the cluster: run-folder paths are recorded on the ticket verbatim
and re-resolved on a compute node later, so they must be the cluster's paths. A
laptop can reach the control plane but cannot see the data.

```bash
qiita --base-url https://qiita-miint.ucsd.edu/ login
```

Opens the AuthRocket LoginRocket Web flow in a browser; a localhost loopback
receiver writes the PAT to `~/.qiita/token` (mode `0600`). Later commands read
`$QIITA_TOKEN` first and fall back to that file.

### Headless / remote hosts (carry the PAT)

`login` drives a browser **and** a localhost receiver, so both must be on the
same machine. On an SSH session, an HPC login node, or a CI runner that flow
cannot complete — a browser on your laptop would redirect to *your laptop's*
localhost. Carry the PAT instead:

```bash
# On a machine with a browser, once:
qiita --base-url https://qiita-miint.ucsd.edu/ login && cat ~/.qiita/token

# On the headless host:
export QIITA_CONTROL_PLANE_URL=https://qiita-miint.ucsd.edu/
export QIITA_TOKEN='<paste the PAT>'
qiita whoami          # no --base-url, no login
```

The PAT identifies the Qiita principal regardless of the Unix account, so
`sudo -u qiita env QIITA_TOKEN=… qiita …` still acts as the token's owner.

Every command prints the route's JSON response, so `| jq -r .study_idx` and
friends work for capturing minted identifiers.

## 1. Complete your profile (one time)

The authoring routes depend on `require_complete_profile`, and the PAT-mint path
refuses to issue tokens for a profile-incomplete user.

```bash
qiita profile set \
    --affiliation "Knight Lab" \
    --address "9500 Gilman Dr, La Jolla, CA 92093" \
    --phone "+1-858-555-0100"
```

Optional: `--orcid`, `--receive-processing-emails` / `--no-receive-processing-emails`.

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
**not** format-checked — the server only requires non-blank text within a length
bound — so a site whose BioProject is not registered yet can agree on any string,
as long as the identical string goes into the pre-flight file.

The route sets `owner_idx` to your principal and inserts an `ADMIN`-tier
`study_access` row for you in the same transaction, so every downstream call on
this study passes its per-resource gate.

## 3. Create the biosamples

One call per biosample — `POST /study/{study_idx}/biosample` takes a single
object, there is no bulk import route:

```bash
qiita biosample create \
    --study-idx "$STUDY_IDX" \
    --owner-biosample-id-field-name sample_name \
    --owner-biosample-id-value SAMPLE-1 \
    --biosample-accession SAMN0000001
```

`--biosample-accession` is the pre-flight join key, exactly as
`--bioproject-accession` is for the study; the column is `UNIQUE` and is not
format-checked either. Without it the biosample exists but no pre-flight row can
reach it.

`--owner-biosample-id-field-name` names a study-local field holding your own name
for the sample. The field is created on first import, so it needs no setup; the
value is PII-restricted and stays off the general read paths.

`--metadata KEY=VALUE` writes additional metadata, but only against fields that
already resolve — a global field's `display_name`, or a study-local field you
created first with `qiita biosample create-field`. Import never creates a new
local field for a metadata key.

Adding a biosample to a study you do not own needs an `ADMIN`-tier
`qiita.study_access` row on it, which only an operator can issue — see
[`auth.md`](../auth.md) under *User self-service*.

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

This matters beyond permissions. A pool's identity is the SHA-256 of the blob's
bytes (`run_preflight_sha256`, a stored generated column, is half the
find-or-create key), and both submit gestures read those bytes **before** opening
and patching the file. Submitting an unpatched file and then re-running would
hash *different* bytes the second time and mint a **second pool**. So copy it
somewhere you own and pre-apply the patches once:

```bash
cp "$SHARED_PF" "$PF" && chmod u+w "$PF"
python - "$PF" <<'PY'
import sys
from run_preflight import open_db_file
open_db_file(sys.argv[1]).close()
PY
md5sum "$PF"   # baseline — unchanged after the submit
```

Keep the patched copy for the life of the pool: a later mask submission wants
byte-identical content.

## 5. Submit the run

Pick the protocol every sample will be FK'd to — it is applied uniformly across
the pool and is **not** validated against the platform:

```bash
qiita prep-protocol list
```

### Illumina

Requires `wet_lab_admin` or `system_admin`: the workflow reads an operator
filesystem path verbatim, so end-user submission is disabled on the action.

```bash
qiita --base-url https://qiita-miint.ucsd.edu/ submit-bcl-convert \
    --bcl-input-dir /sequencing/250520_M05314_0001_000000000-ABCDE \
    --preflight-blob "$PF" \
    --prep-protocol-idx 1
```

The instrument run id and model come from the run folder's `RunInfo.xml`, so
there is no flag for them. The command creates the sequencing-run, the pool, and
one sequenced-sample per pre-flight row, then submits one pool-scoped
`bcl-convert` ticket. Run, pool, and roster are all find-or-create, so re-running
after a partial failure converges instead of erroring.

Each sample's `sequenced_pool_item_id` is set to its `illumina_sample_idx`,
because the sample sheet Qiita rehydrates emits that integer as `Sample_ID` — so
it is also the demux FASTQ's basename prefix.

### PacBio

`submit-pacbio-ingest` takes an already-demultiplexed run folder and fans out one
`bam-to-parquet` ticket per barcode. Read
[`pacbio-ingest.md`](pacbio-ingest.md) before the first PacBio run on a deploy:
it covers the flags with no Illumina counterpart, which protocol to pick, and why
`pool-completion` never reports a PacBio pool as fully processed.

### Never `--force` a retry

Both gestures take `--force`, and on both it means "submit again over a COMPLETED
ticket", which re-registers the pool's reads into the lake — DuckLake has no
uniqueness, so the rows duplicate. To retry, re-run the identical command; to
start over, `qiita delete-sequenced-pool` and resubmit.

## 6. Watch it

```bash
qiita ticket list --active
qiita ticket status <idx>
qiita ticket logs <idx> --step-index 0
qiita ticket run <idx>        # re-dispatch a FAILED ticket in place
qiita pool-completion --sequencing-run-idx <run> --sequenced-pool-idx <pool>
```

States run `pending` → `queued` → `processing` → `completed`, or `failed` with a
`failure_type` / `failure_stage` / `failure_step_name` / `failure_reason` surface.
Recovery recipes for the read-ingest failures live in
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
