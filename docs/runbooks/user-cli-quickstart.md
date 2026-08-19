# User CLI quickstart

> Registering **one sample by hand** — no pre-flight file — and processing
> FASTQs you already hold. Everything here you can do with an ordinary
> account; no admin is involved at any point.

Use this when you have the FASTQs in hand and want a single sample in the
system. To bring in a whole sequencing run instead, one command does the
run, the pool and every sample for you from the run's pre-flight file:
[`getting-started.md`](getting-started.md).

## Before you start

You need the `qiita` command, a token, a filled-in profile, and a study
with a sample in it that you own — steps 0 through 3 of
[`getting-started.md`](getting-started.md). They leave you with the
`$STUDY_IDX` and `$BIOSAMPLE_IDX` used below.

Nothing here needs an admin to grant you anything. Each step is allowed
because of something you did in the step before: you created the run, so
you may put a pool on it; you created the pool, so you may put a sample in
it; you own the study, so you may attach the sample to it.

## 1. Register the sequencing run

The run stands for the instrument's output as a whole. Any user account may
create one.

```bash
qiita sequencing-run create \
    --instrument-run-id "MISEQ-RUN-2026-05-20-001" \
    --platform illumina
```

Qiita records you as its creator, which is what lets you do the next two
steps.

## 2. Add a pool to the run

```bash
qiita sequenced-pool create --run-idx $RUN_IDX
```

A pool is what was sequenced together. You may add one because you created
the run in step 1 (wet-lab admins may add one to anybody's run).

There is a `--run-preflight-blob` flag for attaching a run's pre-flight
file, which you do not need here — the whole-run commands in
[`getting-started.md`](getting-started.md) pass it themselves.

## 3. Add your sample to the pool

```bash
qiita sequenced-sample create \
    --run-idx $RUN_IDX \
    --pool-idx $POOL_IDX \
    --biosample-idx $BIOSAMPLE_IDX \
    --prep-protocol-idx $PROTOCOL_IDX \
    --pool-item-id filename_prefix \
    --primary-study-idx $STUDY_IDX
```

`--pool-item-id` labels this sample within the pool — a well position or a
library barcode. **It must also be the start of your FASTQ filenames**, and
Qiita checks that when you submit in step 4, so the filenames alone say
which sample the reads belong to. `filename_prefix` above is a placeholder:
put in the actual prefix, so that paired-end reads are
`filename_prefix_R1.fastq` and `filename_prefix_R2.fastq`.

`--prep-protocol-idx` says how the library was prepared; `qiita
prep-protocol list` shows the numbers your site has, and
`short_read_metagenomics` is the one that ships by default.

To attach the sample to further studies, repeat `--secondary-study-idx` —
you need admin access on each one, which you have on studies you own.

The reply gives you two numbers. `prep_sample_idx` is the one step 4 wants.

## 4. Submit the FASTQs

```bash
qiita ticket submit \
    --action-id fastq-to-parquet \
    --action-version 1.3.0 \
    --prep-sample-idx $PREP_SAMPLE_IDX \
    --context-json '{"fastq_path": "/scratch/filename_prefix_R1.fastq", "reverse_fastq_path": "/scratch/filename_prefix_R2.fastq"}'
```

The paths must be absolute, and must be readable from the cluster — not
just from your laptop.

`--action-version` has to name the version your site is currently running,
exactly. Several versions of `fastq-to-parquet` exist, but a deploy enables
only the newest one it syncs and retires the rest, so naming an older one is
refused. `1.3.0` is the newest at the time of writing — ask your operator if
that is not what your site has.

You get back a job number and a starting state of `pending`.

**The filename rule.** Every FASTQ filename you pass must start with the
`--pool-item-id` from step 3 — here `filename_prefix`. A name that does not
is refused outright, before the job is queued.

- **Paired-end** — pass `fastq_path` and `reverse_fastq_path`; both names
  are checked.
- **Single-end** — pass `fastq_path` only. Forward-only is fully
  supported.

## 5. Watch it

```bash
qiita ticket status $WORK_TICKET_IDX
```

This shows the job's state, what it was asked to do, how many times it has
been retried, and — if it failed — why and at which step. You can read your
own jobs; wet-lab admins can read anyone's.

The states are the same for every kind of job and are listed in
[`getting-started.md`](getting-started.md). Here, `completed` means the
sample's reads are stored and numbered. A job stuck in `processing` well
past the time the step should take is worth raising with your operator.

## What this does not cover

- **Putting a sample into a study you do not own** — that needs access only
  an operator can grant, see [`auth.md`](../auth.md) under *User
  self-service*.
- **Loading reference databases** — reserved for wet-lab admins and above.
- **Machine accounts.** The tokens the compute side uses to call back into
  Qiita are provisioned separately; see
  [`compute-service-account-provisioning.md`](compute-service-account-provisioning.md).
