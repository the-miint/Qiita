# Manual sample walkthrough

> Registering **one prep_sample by hand** — no pre-flight file — and loading
> reads you already hold from your own machine. Everything here you can do with
> an ordinary account; no admin is involved at any point.

**This is not how you bring in real data**, and it is not the quickstart —
[`getting-started.md`](getting-started.md) is. There are two reasons to walk
this one: an operator runs it after a deploy to prove the system works end to
end, and a user runs it once to check they understand the CLI. Neither is a
good idea on a live system. To bring in an actual sequencing run, one command
does the run, the pool and every prep_sample for you from the run's pre-flight
file — that is the path to use.

## Before you start

You need the `qiita` CLI installed and reachable on your `$PATH`, a token, a
filled-in profile, and a study with a biosample in it that you own — steps 0
through 3 of [`getting-started.md`](getting-started.md). They leave you with
the `$STUDY_IDX` and `$BIOSAMPLE_IDX` used below. You also need a FASTQ or an
unaligned BAM on the machine you are working from, and your site's data-plane
URL for step 4.

Nothing here needs an admin to grant you anything. Each step is allowed
because of something you did in the step before: you created the run, so you
may put a pool on it; you created the pool, so you may put a prep_sample in
it; you own the study, so you may attach the prep_sample to it.

## 1. Register the sequencing run

The run stands for the instrument's output as a whole. Any user account may
create one.

```bash
qiita sequencing-run create \
    --instrument-run-id "MISEQ-RUN-2026-05-20-001" \
    --platform illumina
```

Qiita records you as the run's creator, which is what lets you do the next
two steps.

## 2. Add a pool to the run

```bash
qiita sequenced-pool create --run-idx $RUN_IDX
```

A pool is a subset of the prepped samples in the sequencing run that all
share the same preparation info (run preflight). Every run has at least one
pool. You may add one because you created the run in step 1 (wet-lab admins
may add one to anybody's run).

There is a `--run-preflight-blob` flag for attaching a run's pre-flight
file, which you do not need here — the whole-run commands in
[`getting-started.md`](getting-started.md) pass it themselves.

## 3. Add your prep_sample to the pool

```bash
qiita sequenced-sample create \
    --run-idx $RUN_IDX \
    --pool-idx $POOL_IDX \
    --biosample-idx $BIOSAMPLE_IDX \
    --prep-protocol-idx $PROTOCOL_IDX \
    --pool-item-id filename_prefix \
    --primary-study-idx $STUDY_IDX
```

`--pool-item-id` uniquely identifies this prep_sample within the pool. **It
must be prefixed at the start of your FASTQ filenames**, and Qiita checks
that when you submit in step 4, so the filenames alone say which prep_sample
the reads belong to. `filename_prefix` above is a placeholder: put in the
actual prefix, so that paired-end reads are `filename_prefix_R1.fastq` and
`filename_prefix_R2.fastq`. (In a normal run this value comes out of the
pre-flight file, not out of your head — see
[`getting-started.md`](getting-started.md).)

`--prep-protocol-idx` says how the library was prepared; `qiita
prep-protocol list` shows the numbers your site has, and
`short_read_metagenomics` is the one that ships by default.

To attach the prep_sample to further studies, repeat `--secondary-study-idx`
— you need admin access on each one, which you have on studies you own.

The reply gives you two numbers. `prep_sample_idx` is the one step 4 wants.

## 4. Submit the reads

`qiita submit-reads` sends the files from the machine you are typing on:

```bash
qiita submit-reads \
    --prep-sample-idx $PREP_SAMPLE_IDX \
    --fastq ./filename_prefix_R1.fastq \
    --reverse-fastq ./filename_prefix_R2.fastq \
    --data-plane-url grpc+tls://qiita-miint.ucsd.edu:443
```

Each file is streamed to Qiita byte for byte — a `.gz` is sent compressed and
stays that way — and the job is then submitted against what arrived. So the
reads only have to be readable by *you*: nothing here needs them to sit on a
filesystem the cluster mounts. `--data-plane-url` is where they are streamed
to; ask your operator for your site's, and use the `grpc+tls://` form from
anywhere but the deploy host itself.

The command waits for the job and prints it when it finishes; `--no-watch`
returns as soon as it is submitted.

You do not name a workflow or a version. `--fastq` means `fastq-to-parquet`
and `--bam` means `bam-to-parquet`, at whatever version your site has enabled.

- **Paired-end** — pass `--fastq` and `--reverse-fastq`.
- **Single-end** — pass `--fastq` alone. Forward-only is fully supported.
- **An unaligned BAM** — pass `--bam` instead of `--fastq`, with no reverse
  file. An aligned BAM is refused by the loader.

**The filename rule.** Each FASTQ's *filename* must start with the
`--pool-item-id` from step 3 — here `filename_prefix` — followed by `_` or `.`.
The name is checked after the upload, since it is the prep_sample's own
identifier that it is checked against, so a mismatch costs you the transfer and
comes back as a refusal rather than a job. The rule does not apply to a `--bam`:
a demultiplexed BAM is named for its movie and barcode and carries no pool item
id.

If your reads are already on the cluster, a `wet_lab_admin` can name their path
directly instead — `qiita ticket submit --context-json '{"fastq_path": …}'` —
under the directories the site allows. An ordinary account is refused, and the
refusal names the upload route above.

## 5. Watch it

Step 4 already waited for the job and printed it. To look again later — its
number is `work_ticket.work_ticket_idx` in that output:

```bash
qiita ticket status $WORK_TICKET_IDX
```

This shows the job's state, what it was asked to do, how many times it has
been retried, and — if it failed — why and at which step. You can read your
own jobs; wet-lab admins can read anyone's.

The states are the same for every kind of job and are listed in
[`getting-started.md`](getting-started.md). Here, `completed` means the
prep_sample's reads are stored and numbered. A job stuck in `processing` well
past the time the step should take is worth raising with your operator.

## What this does not cover

- **Putting a biosample into a study you do not own** — that needs access
  only an operator can grant, see [`auth.md`](../auth.md) under *User
  self-service*.
- **Loading reference databases** — reserved for wet-lab admins and above.
- **Provisioning machine accounts** — the tokens the compute side uses to
  call back into Qiita are set up separately; see
  [`compute-service-account-provisioning.md`](compute-service-account-provisioning.md).
