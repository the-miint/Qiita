# User CLI quickstart

> Authoring one sequenced sample **by hand** — no pre-flight file — and
> processing it with a `fastq-to-parquet` work-ticket. The whole flow is
> `user`-role: no `wet_lab_admin` or `system_admin` in the loop.

Use this when you already hold the FASTQs and want one sample in the
system. Bringing in a whole sequencing run instead goes through a
pre-flight file and one bundled command, which creates the run, the pool,
and every sample for you: [`getting-started.md`](getting-started.md).

Two audiences: an operator runs these steps as a post-deploy smoke
([`first-deploy.md`](first-deploy.md) Step 11 links here), and a user
follows them as the reference for the authoring CLI. Every authoring
route gates on a per-resource auth predicate — study owner, run/pool
creator, or per-study `ADMIN` tier — rather than a blanket
`wet_lab_admin` role check; each step below names the gate it clears.

## Prerequisites

- A working deploy (see [`first-deploy.md`](first-deploy.md)).
- The `qiita` CLI installed, a PAT in hand, a complete profile, and a
  study plus a biosample you own — steps 0 through 3 of
  [`getting-started.md`](getting-started.md). They leave you with the
  `$STUDY_IDX` and `$BIOSAMPLE_IDX` this flow starts from.

## 1. Create a sequencing run

The instrument-level container. No role / tier gate — any user with
`prep_sample:write`, which every `user`-role PAT carries (the per-role scope
ceilings are in [`auth.md`](../auth.md)), can stand one up.

```bash
qiita sequencing-run create \
    --instrument-run-id "MISEQ-RUN-2026-05-20-001" \
    --platform illumina
```

The route records you as `created_by_idx`; this is the key the next
two steps' caller-creator guards check.

## 2. Create a sequenced pool on the run

```bash
qiita sequenced-pool create --run-idx $RUN_IDX
```

Auth path: `require_caller_owns_run()` admits you because you created
the run in step 1. Wet-lab admins bypass the creator check.

`--run-preflight-blob` attaches a pre-flight file to the pool. Hand-authoring
does not need one; the bundled ingest gestures pass it themselves
([`getting-started.md`](getting-started.md)).

## 3. Create a sequenced sample (the prep_sample)

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
of every fastq this sample's work-ticket processes** — see step 4.
The value used here, `filename_prefix`, is a placeholder; substitute
the actual prefix of your fastq files (for paired-end input,
`filename_prefix` implies `filename_prefix_R1.fastq` /
`filename_prefix_R2.fastq`). The control plane rejects a
`fastq-to-parquet` submission whose `fastq_path` basename does not
start with this value.

Auth paths:

- `require_caller_owns_pool()` — you created the pool in step 2.
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

## 4. Submit fastq-to-parquet

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
`--pool-item-id` you chose in step 3 — here `filename_prefix`. The
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

## 5. Poll for status

```bash
qiita ticket status $WORK_TICKET_IDX
```

Returns the full `WorkTicket` record: `state`, `action_id /
action_version`, `scope_target`, `action_context`, `retry_count /
max_retries`, the `failure_*` surface, and timestamps. Auth: the
originator (you) passes; wet_lab_admin+ can read any ticket.

The state progression and the failure surface are the same for every
action; [`getting-started.md`](getting-started.md) lists them. `completed`
here means the Parquet is under the ticket's workspace and the sample's
`sequence_range` is populated. A `processing` state that stalls past the
action's `walltime_ceiling` is the operator's signal to look at the
orchestrator logs.

## What this flow does NOT cover

- **Cross-study access grants** and **reference-data authoring** — see
  [`getting-started.md`](getting-started.md) under *Not covered here*.
- **Service-account flows.** End-user PATs do not carry
  `sequence_range:mint` or `reference:register_files`; those scopes
  are on the service-account ceiling for the orchestrator's CO→CP
  callbacks (see
  [`compute-service-account-provisioning.md`](compute-service-account-provisioning.md)).

## Smoke-testing this flow

The integration test `tests/integration/test_user_authoring_smoke.py`
walks this flow end-to-end, starting from the study and biosample its
prerequisites name: it stands up a real control-plane server and shells
out to the actual `qiita` CLI for every command, so the flag names in
this runbook are mechanically pinned against argparse drift. Run it via
`make test-integration`.
