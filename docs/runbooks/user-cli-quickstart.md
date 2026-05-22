# User CLI quickstart

> End-to-end walkthrough of the `user`-role authoring flow: log in,
> author a study, register sequencing data, and submit a
> `fastq-to-parquet` work-ticket — no `wet_lab_admin` or `system_admin`
> in the loop.

Two audiences: an operator runs these steps as a post-deploy smoke
([`first-deploy.md`](first-deploy.md) Step 11 links here), and a user
follows them as the reference for the authoring CLI. Every authoring
route gates on a per-resource auth predicate — study owner, run/pool
creator, or per-study `ADMIN` tier — rather than a blanket
`wet_lab_admin` role check; each step below names the gate it clears.

## Prerequisites

- A working deploy (see [`first-deploy.md`](first-deploy.md)).
- An OIDC provider that returns the user's email and that the deploy is
  configured to accept.
- `qiita` CLI installed and reachable on the user's `$PATH` (built from
  `qiita-control-plane/src/qiita_control_plane/cli/user.py`; available
  as a console script after `uv tool install qiita-control-plane`).

## 0. Log in

```bash
qiita --base-url https://qiita.example.org login
```

Opens the AuthRocket LoginRocket Web flow in the browser; the loopback
HTTP receiver writes a PAT to `~/.qiita/token` (mode `0600`). Subsequent
commands read the PAT from `$QIITA_TOKEN` (env var, takes precedence)
or `~/.qiita/token`.

```bash
qiita whoami
```

Expected fields:

- `kind: human`
- `email: <your email>`
- `system_role: user`
- `scopes: [...]` — the USER ceiling: `self:profile, self:token,
  reference:read, biosample:read, biosample:write, prep_sample:read,
  prep_sample:write, study:read, study:write`.

A 401 means the PAT is missing or revoked; re-run `qiita login`.

## 1. Complete your profile (one time)

The PAT-mint path refuses to issue tokens for profile-incomplete users.
If you skipped this on first login:

```bash
qiita profile set \
    --affiliation "Knight Lab" \
    --address "9500 Gilman Dr, La Jolla, CA 92093" \
    --phone "+1-858-555-0100"
```

Optional: `--orcid`, `--receive-processing-emails` /
`--no-receive-processing-emails`.

## 2. Create a study

```bash
qiita study create --title "My first user-CLI study"
```

The route mints the `study` row, sets `owner_idx` to your principal,
and inserts an `ADMIN`-tier `study_access` row for you in the same
transaction. The `study_idx` in the response is the handle for every
downstream call.

## 3. Create a biosample on that study

```bash
qiita biosample create \
    --study-idx $STUDY_IDX \
    --owner-biosample-id-field-name sample_name \
    --owner-biosample-id-value SAMPLE-1
```

Auth path: `require_study_access(min_tier=Tier.ADMIN)` admits the
study owner (owner-bypass) regardless of `study_access` row, so the
study you just created passes immediately. `--owner-idx` defaults to
your own principal via `whoami` if omitted.

To add a biosample to a study you don't own, you need an
`ADMIN`-tier `qiita.study_access` row on that study. There is no
self-service grant flow yet — an operator inserts the row directly
(see the limitations section at the end).

## 4. Create a sequencing run

The instrument-level container. No role / tier gate — any user with
`prep_sample:write` (which is in the USER ceiling) can stand one up.

```bash
qiita sequencing-run create \
    --instrument-run-id "MISEQ-RUN-2026-05-20-001" \
    --platform illumina
```

The route records you as `created_by_idx`; this is the key the next
two steps' caller-creator guards check.

## 5. Create a sequenced pool on the run

```bash
qiita sequenced-pool create --run-idx $RUN_IDX
```

Auth path: `require_caller_owns_run()` admits you because you created
the run in step 4. Wet-lab admins bypass the creator check.

Optional `--run-preflight-blob /path/to/file.sqlite` attaches the
instrument's pre-flight checks; the route stores the raw bytes in
the `run_preflight_blob` BYTEA column and defaults the filename to
the file's basename.

## 6. Create a sequenced sample (the prep_sample)

```bash
qiita sequenced-sample create \
    --run-idx $RUN_IDX \
    --pool-idx $POOL_IDX \
    --biosample-idx $BIOSAMPLE_IDX \
    --prep-protocol-idx $PROTOCOL_IDX \
    --pool-item-id filename_prefix \
    --primary-study-idx $STUDY_IDX
```

`--pool-item-id` is a per-pool unique label for this item (a well
position or library barcode). **It must also be the filename prefix
of every fastq this sample's work-ticket processes** — see step 7.
The value used here, `filename_prefix`, is a placeholder; substitute
the actual prefix of your fastq files (for paired-end input,
`filename_prefix` implies `filename_prefix_R1.fastq` /
`filename_prefix_R2.fastq`). The control plane rejects a
`fastq-to-parquet` submission whose `fastq_path` basename does not
start with this value.

Auth paths:

- `require_caller_owns_pool()` — you created the pool in step 5.
- `require_caller_has_admin_on_all_studies` over the primary study
  plus every secondary — you own the primary study by owner-bypass.
  Add a secondary study you also have ADMIN on with
  `--secondary-study-idx STUDY_IDX` (repeat the flag for several).

The response carries both `prep_sample_idx` (the supertype) and
`sequenced_sample_idx` (the subtype); the work-ticket step uses
`prep_sample_idx`.

`--prep-protocol-idx` resolves to the `qiita.prep_protocol` row
seeded by the migrations (`short_read_metagenomics` is the default
that ships).

## 7. Submit fastq-to-parquet

```bash
qiita ticket submit \
    --action-id fastq-to-parquet \
    --action-version 1.0.0 \
    --prep-sample-idx $PREP_SAMPLE_IDX \
    --context-json '{"fastq_path": "/scratch/filename_prefix_R1.fastq", "reverse_fastq_path": "/scratch/filename_prefix_R2.fastq"}'
```

The `fastq-to-parquet` action's audience admits `user`; the route
applies a per-study ADMIN check over every non-retired
`prep_sample_to_study` link (your primary study passes via
owner-bypass). Response: 202 with `work_ticket_idx` and the initial
`state` (`pending`).

`fastq_path` (and, for paired-end input, `reverse_fastq_path`) must be
absolute paths the orchestrator can read (validated by the action's
`context_schema`).

**Filename-prefix rule.** Every fastq basename must start with the
`--pool-item-id` you chose in step 6 — here `filename_prefix`. The
control plane resolves the prep_sample's `sequenced_pool_item_id` and
rejects the submission (422) when a basename does not carry that
prefix, so the filenames alone identify which DB row a fastq belongs
to. The rule applies to every path you pass:
- **Paired-end** — `fastq_path` and `reverse_fastq_path`, e.g.
  `filename_prefix_R1.fastq` and `filename_prefix_R2.fastq`; both
  basenames are checked.
- **Single-end** — pass only `fastq_path` (e.g. `filename_prefix.fastq`);
  the lone forward read is checked against the same prefix.
  Forward-only submission is fully supported.

## 8. Poll for status

```bash
qiita ticket status $WORK_TICKET_IDX
```

Returns the full `WorkTicket` record: `state`, `action_id /
action_version`, `scope_target`, `action_context`, `retry_count /
max_retries`, the `failure_*` surface, and timestamps. Auth: the
originator (you) passes; wet_lab_admin+ can read any ticket.

State progression:

- `pending` → just submitted, dispatch task scheduled.
- `queued` → dispatcher has it.
- `processing` → orchestrator is running the workflow.
- `completed` → terminal; Parquet has been written under the
  ticket's workspace and (for fastq-to-parquet) `sequence_range`
  is populated.
- `failed` → terminal-for-now; check `failure_type`,
  `failure_stage`, `failure_step_name`, `failure_reason`. Recovery
  recipes live in [`fastq-to-parquet-retry-recovery.md`](fastq-to-parquet-retry-recovery.md).

A `processing` state that stalls past the action's
`walltime_ceiling` is the operator's signal to look at the
orchestrator logs.

## What this flow does NOT cover

- **Cross-study access grants.** Attaching a biosample or sample to a
  study you do not own requires an `ADMIN`-tier `qiita.study_access`
  row on that study. No CLI or API surface issues those grants today:
  an operator inserts the row with a direct
  `INSERT INTO qiita.study_access (study_idx, principal_idx,
  access_tier, granted_by_idx)` against the database. A self-service
  grant flow is future work.
- **Reference-data authoring.** `reference:write` is wet_lab_admin+;
  end-users consume references but do not author them.
- **Service-account flows.** End-user PATs do not carry
  `sequence_range:mint` or `reference:register_files`; those scopes
  are on the service-account ceiling for the orchestrator's CO→CP
  callbacks (see
  [`compute-service-account-provisioning.md`](compute-service-account-provisioning.md)).

## Smoke-testing this flow

The integration test `tests/integration/test_user_authoring_smoke.py`
walks steps 2–8 end-to-end: it stands up a real control-plane server
and shells out to the actual `qiita` CLI for every command, so the
flag names in this runbook are mechanically pinned against argparse
drift. Run it via `make test-integration`.
