# ENA study import (runbook)

**For:** an operator (wet_lab_admin or system_admin) importing one or more public
ENA/SRA studies' metadata and reads into Qiita. Read the *Scope and limits* section
before the first import on a new deploy — several boundaries here are hard limits,
not "not implemented yet."

Auth and the general CLI/API flow are **not** repeated here — see
[`user-cli-quickstart.md`](user-cli-quickstart.md). This runbook covers only what is
specific to importing from ENA.

## What an import does

A single admin-facing call kicks off a **batch**: a list of ENA/SRA study accessions
(`PRJNA…`, `PRJEB…`, `PRJDB…`, `ERP…`, `SRP…`, `DRP…`). Each accession in the batch is
processed independently, with bounded concurrency, in three phases:

1. **Resolve** — the study's header, run list, and per-sample attributes are pulled
   from ENA (via the `duckdb-miint` `read_ena` / `read_ena_attributes` table
   functions).
2. **Register** — the resolved metadata is turned into Qiita rows: one `study`, one
   `biosample` per distinct ENA sample accession (de-duplicated **across studies** —
   two studies that share a sample converge on the same biosample row, never a
   duplicate), and one `sequenced_sample`/`prep_sample` per run. Runs are grouped by
   mapped platform into one `sequencing_run` + `sequenced_pool` per `(study,
   platform)` pair — a multi-platform study yields more than one pool. Each run's ENA
   sample attributes are harmonized onto its biosample's metadata the first time that
   biosample is created (a re-import or a cross-study reuse does not re-harmonize).
3. **Submit** — one `download-ena-study` work ticket per pool created in step 2,
   scoped to that `sequenced_pool`. This is the ticket that actually pulls read bytes;
   registration itself never touches read data.

A failure in any one accession — an unmappable platform, a resolver error, a database
conflict — is recorded on that accession alone; it never aborts the batch or its
sibling accessions. Poll the batch's own status endpoint to see each accession's state
and, on failure, the reason.

### REST surface

- `POST /api/v1/ena-import-batch` — body: `{accessions: [...], backend: "miint",
  source: "ena", download_method: "http"}`. Returns `202` immediately with a batch
  handle and every accession at its initial `pending` state; the resolve/register/
  submit work for the whole batch runs in the background. **Admin-only**
  (wet_lab_admin or system_admin) — this is an operator gesture, not something an
  end user submits, mirroring bcl-convert's own admin-only submission.
- `GET /api/v1/ena-import-batch/{idx}` — the batch's current, rolled-up per-accession
  status: `pending` / `resolving` / `registered` / `downloading` / `done` / `failed`,
  with `study_idx` and the download ticket idx(s) once resolved, and a
  `failure_reason` on any `failed` item. Also admin-only.

The actual read download runs as the `download-ena-study` workflow
(`workflows/download-ena-study/1.0.0.yaml`), the same `qiita ticket status` /
`qiita ticket logs` / `qiita ticket run` commands used for any other workflow apply to
it once submitted.

### Metadata harmonization

Every ENA-imported biosample is bound to the **ERC000011** checklist (the ENA default
sample checklist) — the same shared checklist model every other metadata path in
Qiita uses. A checklist-required field ENA did not supply for a given sample is
reported back on the registration outcome (visible via the batch status endpoint's
failure/harmonization detail), never silently dropped or defaulted — but a
harmonization *gap* does not fail the run; only a genuine harmonization error
(an unparseable value, or a cross-study metadata slot collision) does, isolated
per-run exactly like an unmappable platform.

**A sample with zero ENA attributes is a legitimate, common result, not an import
failure.** Real ENA/DDBJ samples sometimes carry no `<SAMPLE_ATTRIBUTE>` elements at
all (confirmed live against DDBJ study `PRJDB40364`'s sample `SAMD01818724`). Such a
sample still registers normally — study, biosample, and sequenced/prep rows are all
created — it simply harmonizes against an empty attribute map, so it carries no
globally-linked metadata and the checklist's required fields show up in the
`missing_required` report rather than blocking the import.

## Scope and limits

These are **hard limits** of the current import surface, not partial-implementation
gaps expected to close soon (except where noted):

- **Reads and metadata only.** No host-genome handling, no downstream processing
  beyond landing raw reads in DuckLake — an imported study's reads still go through
  the normal read-mask / alignment pipeline like any other ingested data.
- **ENA/SRA source archives only.** GSA (China National GeneBank / BGI) and CNGB are
  out of scope; there is no resolver or accession-prefix support for either.
- **No Aspera.** `download_method` is pinned to `http` — the only transport this
  compute environment supports (no Aspera key-staging exists). A request for any
  other transport is rejected (`422`) before anything is written.
- **DDBJ / legacy-platform metadata is a known gap, not yet closed.** The platform
  and library-strategy mapping tables cover the INSDC/ENA-native platform and
  strategy vocabulary; a DDBJ-submitted record with a legacy or DDBJ-specific
  platform string can fail platform mapping for that run alone (isolated, per the
  per-run failure model above) rather than importing correctly. Filling out DDBJ
  coverage is deferred to the backlog.
- **No ENVO / taxon-ontology harmonization.** Free-text environment and taxonomy
  fields ENA supplies are harmonized onto the checklist's plain text/enum fields as
  given; there is no ENVO term resolution or NCBI taxon-id cross-referencing in this
  path. Also deferred to the backlog.

## The duckdb-miint dependency

Metadata resolution and read download both go through `duckdb-miint` table functions,
not a hand-rolled ENA client:

- `read_ena` — study header + run list (the default resolver backend, `miint`).
- `read_ena_attributes` — per-sample attributes, pivoted into the checklist model.
- `read_ena_sequences` — the actual read download, called by the `ingest_ena_reads`
  compute job once a pool's runs are registered.

**md5 verification is pending an upstream miint change.** `read_ena_sequences` does
not verify a downloaded run's bytes against ENA's published `fastq_md5` today — this
is a known, tracked gap (a duckdb-miint escalation), not a silent oversight; a
downloaded run's read count and format are still checked (a run that comes back
truncated or empty fails loud, never registers silently), but byte-level checksum
verification against ENA's own hash is not wired in yet. Do not add ad hoc
verification around this job in the meantime — the fix belongs in `duckdb-miint`
itself, propagated through the normal extension-version bump.

An unresolvable accession (malformed, or one ENA does not recognize) fails loud with
an actionable message rather than resolving to a silent empty result — see
`ena_import.accession` for the accepted prefix sets per accession kind.
