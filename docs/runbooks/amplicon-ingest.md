# Rapid 16S amplicon (runbook)

> **Status: OUTLINE.** Headings and the intended shape are here; the step-by-step
> detail is filled in once the workflow has run end-to-end on a real deploy. The
> workflow *contracts* live in the YAML `description:` blocks
> (`workflows/golay-demux/1.0.0.yaml`, `workflows/amplicon/1.0.0.yaml`) — this
> runbook is the operator/analyst playbook, not the contract.

**For:** whoever processes an EMP-style 16S run — Golay-barcoded, arriving as one
multiplexed FASTQ set (R1 + I1, optionally R2) rather than per-sample BCL demux.
Two workflows run in sequence: `golay-demux` (ingest → `read`) then `amplicon`
(denoise → ASV `feature_idx` + counts). Auth and the general CLI flow are not
repeated here — see [`user-cli-quickstart.md`](user-cli-quickstart.md).

## Prerequisites

- _(TODO)_ A SortMeRNA 16S database loaded as an ACTIVE `sequence_reference`; its
  `reference_idx` is the `amplicon` submit arg `sortmerna_reference_idx`. (The
  Golay decode cloud is **not** a prerequisite — it is generated in-job.)
- _(TODO)_ For the derived closed-reference feature table: the reference (e.g. GG2)
  loaded so its `reference_membership` can be intersected with the ASV features.
  **The feature-table reader itself is not part of these workflows** — see the
  tracked follow-up.

## Where to run it

- _(TODO)_ The `qiita` console script in the deployed venv (as in the PacBio
  runbook); the multiplexed FASTQ are compute-node-visible host paths.

## Submit golay-demux (ingest)

- _(TODO)_ context: `index_reads_path`, `forward_reads_path`, optional
  `reverse_reads_path`, and the per-sample `barcode_map` roster submitted in
  action_context (the runner materializes it to a parquet; the orchestrator has
  no DB access). Loads per-sample reads into `read`.

## Submit amplicon (denoise)

- _(TODO)_ context: `sortmerna_reference_idx`, `trim`, optional `primer` /
  `orient_primer`. The pool's reads STREAM from the data plane at runtime (nothing
  staged to scratch). Writes `amplicon_membership` (reference-agnostic ASV counts).

## Re-runs

- A second submit of the same pool+workflow is refused once the first has COMPLETED
  (the same pool-level gate bcl-convert uses); `--force` (admin) is the escape. A
  `--force` re-run with the *same* denoise knobs resolves to the same
  `processing_idx`, and `amplicon_membership` is replace-keyed on
  `(prep_sample_idx, processing_idx)`, so the re-run replaces its own rows rather
  than doubling the counts.

## Deriving the closed-reference (e.g. GG2) feature table

- _(TODO — tracked follow-up, not in this PR)_ The feature table is derived on
  demand by intersecting `amplicon_membership.feature_idx` with a reference's
  `reference_membership`; it is never stored per-reference.
