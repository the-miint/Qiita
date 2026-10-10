# Rapid 16S amplicon (runbook)

> **Status: OUTLINE.** Headings and the intended shape are here; the step-by-step
> detail is filled in once the workflow has run end-to-end on a real deploy. The
> workflow *contracts* live in the YAML `description:` blocks
> (`workflows/golay-demux/1.0.0.yaml`, `workflows/amplicon/1.0.0.yaml`) — this
> runbook is the operator/analyst playbook, not the contract.

**For:** whoever processes an EMP-style 16S run — Golay-barcoded, with no
per-sample Illumina indices (the per-sample identity is a Golay barcode in the
index read). Two workflows run in sequence: `golay-demux` (bcl-convert with a
dummy sheet, then Golay demux → `read`) then `amplicon` (denoise → ASV
`feature_idx` + counts). Auth and the general CLI flow are not repeated here —
see [`getting-started.md`](getting-started.md).

## Prerequisites

- _(TODO)_ A SortMeRNA 16S database loaded as an ACTIVE `sequence_reference`; its
  `reference_idx` is the `amplicon` submit arg `sortmerna_reference_idx`. (The
  Golay decode cloud is **not** a prerequisite — it is generated in-job.)
- _(TODO)_ For the derived ASV-reference-match feature table: the reference (e.g. GG2)
  loaded so its `reference_membership` can be intersected with the ASV features.
  **The feature-table reader itself is not part of these workflows** — see the
  tracked follow-up.

## Where to run it

- _(TODO)_ The `qiita` console script in the deployed venv (as in the PacBio
  runbook). The submitter names a run id, not a path — the run folder is resolved
  server-side against `PATH_INGEST_ROOTS`.

## Submit golay-demux (ingest)

- _(TODO)_ `qiita submit-golay-demux --instrument-run-id <run id> --preflight-blob
  <sqlite> --prep-protocol-idx <n>`. The CP resolves the run folder from the run
  id and reads the instrument identity from its RunInfo.xml. bcl-convert runs with
  a dummy sheet (every decodable read to Undetermined, the Golay I1 emitted), then
  `golay_demux` demultiplexes on the Golay barcode. The per-sample `barcode_map`
  roster is built from the preflight's `amplicon_sample` rows and submitted in
  action_context. Before running, the runner rebuilds the roster from the
  preflight stored on the pool and fails the ticket (bad input) if the submitted
  one differs in any sample, barcode or orientation, or if the pool stores no
  preflight to check against; it then materializes it to a parquet (the
  orchestrator has no DB access). Loads per-sample reads into `read`.

## Submit amplicon (denoise)

- _(TODO)_ context: `sortmerna_reference_idx`, `trim`, optional `primer` /
  `orient_primer`. The pool's reads STREAM from the data plane at runtime (the runner
  stages nothing; the stream spills transiently to the job workspace). Writes
  `amplicon_membership` + the ASV sequence tables (reference-agnostic ASV counts).

## Re-runs

- A second submit of the same pool+workflow is refused once the first has COMPLETED
  (the same pool-level gate bcl-convert uses); `--force` (admin) is the escape. A
  `--force` re-run with the *same* denoise knobs resolves to the same
  `processing_idx`, and `amplicon_membership` is replace-keyed on
  `(prep_sample_idx, processing_idx)`, so the re-run replaces its own rows rather
  than doubling the counts.

## Deriving the ASV-reference-match feature table

- _(TODO — tracked follow-up, not in this PR)_ The feature table is derived on
  demand by intersecting `amplicon_membership.feature_idx` with a reference's
  `reference_membership`; it is never stored per-reference. This is an **exact ASV
  match** (identical sequence -> shared `feature_idx`), NOT the similarity-based
  "closed-reference" of classic OTU pipelines — the name is deliberately distinct
  to avoid implying a percent-identity clustering step that does not happen here.
