# User CLI quickstart

> End-to-end walkthrough for a regular `user`-role principal: log in,
> author a study, register sequencing data, and submit a
> `fastq-to-parquet` work-ticket — no `wet_lab_admin` or `system_admin`
> in the loop.

Phase 2.b widened the USER role's scope ceiling and replaced the
wet_lab_admin role gate on every authoring route with a per-resource
auth predicate that the study owner / pool creator can clear directly.
This runbook is the operator-facing manifest of that flow.

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

To add a biosample you don't own to a study someone else owns, the
study owner must grant you `ADMIN` tier via `qiita-admin
study-access` first.

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
    --sequenced-pool-item-id ITEM-1 \
    --primary-study-idx $STUDY_IDX
```

Auth paths:

- `require_caller_owns_pool()` — you created the pool in step 5.
- `require_caller_has_admin_on_all_studies` over
  `primary_study_idx + secondary_study_idxs` — you own the primary
  study by owner-bypass; add any secondary study you also have ADMIN
  on via `--secondary-study-idxs`.

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
    --context-json '{"fastq_path": "/scratch/myfile.fastq"}'
```

Phase 2.c widened the action's audience to admit `user`; the route
applies a per-study ADMIN check over every non-retired
`prep_sample_to_study` link (your primary study passes via
owner-bypass). Response: 202 with `work_ticket_idx` and the initial
`state` (`pending`).

The action's `fastq_path` must be an absolute path the orchestrator
can read (validated by the action's `context_schema`).

For paired-end input, add `reverse_fastq_path` to the same
`--context-json` object.

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

- **Cross-study sample ownership.** If you want to attach a sample to
  a study you do not own at ADMIN tier, the study owner must grant
  you access via `qiita-admin study-access` first.
- **Reference-data authoring.** `reference:write` is wet_lab_admin+;
  end-users consume references but do not author them.
- **Service-account flows.** End-user PATs do not carry
  `sequence_range:mint` or `reference:register_files`; those scopes
  are on the service-account ceiling for the orchestrator's CO→CP
  callbacks (see
  [`compute-service-account-provisioning.md`](compute-service-account-provisioning.md)).

## Smoke-testing this flow

The integration test `tests/integration/test_user_authoring_smoke.py`
walks steps 2–8 end-to-end against an in-process control plane,
exercising every per-resource auth gate. Run it via
`make test-integration`.
