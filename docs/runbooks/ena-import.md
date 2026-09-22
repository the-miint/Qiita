# ENA study import (runbook)

**For:** an operator (wet_lab_admin or system_admin) importing one or more public
public INSDC studies' metadata and reads into Qiita. Read the *Scope and limits* section
before the first import on a new deploy — several boundaries here are hard limits,
not "not implemented yet."

Auth and the general CLI/API flow are **not** repeated here — see
[`user-cli-quickstart.md`](user-cli-quickstart.md). This runbook covers only what is
specific to importing from ENA.

## What an import does

A single admin-facing call kicks off a **batch**: a list of INSDC study accessions
(`PRJNA…`, `PRJEB…`, `PRJDB…`, `ERP…`, `SRP…`, `DRP…`). Each accession in the batch is
processed independently, with bounded concurrency, in three phases. That bound is shared
by every batch running in the control plane at once, first come first served: a later
batch's accessions are not admitted until every accession of every batch submitted before
it has itself been admitted, so a busy batch can make a newly submitted one wait for it
to clear the gate first.

1. **Resolve** — the study's header, run list, and per-sample attributes are pulled
   from ENA (via the `duckdb-miint` `read_ena` / `read_ena_attributes` table
   functions).
2. **Register** — the resolved metadata is turned into Qiita rows: one `study`, one
   `biosample` per distinct ENA sample accession (de-duplicated **across studies** —
   two studies that share a sample converge on the same biosample row, never a
   duplicate), and one `sequenced_sample`/`prep_sample` per run. Runs are grouped by
   mapped platform into one `sequencing_run` per `(study, platform)` pair, and new
   runs go into a `sequenced_pool` on it — a multi-platform study yields more than one
   pool. Each run's ENA sample attributes are harmonized onto its biosample's metadata
   the first time that biosample is created (a re-import or a cross-study reuse does
   not re-harmonize).
3. **Submit** — one `download-ena-study` work ticket per pool holding the study's
   runs, scoped to that `sequenced_pool`. A pool whose ticket is in flight or finished
   reuses it; one with no ticket, or whose ticket failed or was cancelled, gets a new
   one. This is the ticket that actually pulls read bytes; registration itself never
   touches read data.

### Re-importing, and studies we created ourselves

Re-importing an accession is the supported way to pick up runs a bioproject gained
since the last import: runs already registered come back as `skipped_already_present`
and only the new ones are added. A download ticket reads its pool's run list once, when
it starts, so new runs never join a pool whose download is in flight or finished — they
go into a new pool with its own ticket, and the accession reports `done` only once every
pool's download has. A re-import is also how to retry a failed or cancelled download.
Nothing schedules this — it is an operator gesture.

An import will only add to a study **an import created**. A study Qiita created
natively and later deposited to ENA carries a `bioproject_accession` too, so
importing that accession would otherwise merge ENA-derived samples into curated
data. That case fails the accession with `not created by an ENA import`, before
anything is written. Deleting a batch (which cascades its items) discards the record
that the import created the study, so a later re-import of that accession is refused
as well.

A study matched by either the incoming `bioproject_accession` or `ena_study_accession`
is reused, and is still subject to the import-created guard above. If the pair
identifies two different studies, or contradicts the accession the one study it
resolves to has on file, the accession fails instead of picking a winner. The stored
failure text is the raw contradiction, e.g. `bioproject_accession='PRJNA1',
ena_study_accession='ERP1': study 42 has bioproject_accession 'PRJNA2'` — it does not
say the import was refused. Fix the accession passed to the import, or the study's
recorded value, then re-import.

A failure in any one accession — an unmappable platform, a resolver error, a database
conflict — is recorded on that accession alone; it never aborts the batch or its
sibling accessions. Poll the batch's own status endpoint to see each accession's state
and, on failure, the reason.

### REST surface

- `POST /api/v1/ena-import-batch` — body: `{accessions: [...]}`. Returns `202` immediately with a batch
  handle and every accession at its initial `pending` state; the resolve/register/
  submit work for the whole batch runs in the background. **Admin-only**
  (wet_lab_admin or system_admin) — this is an operator gesture, not something an
  end user submits, mirroring bcl-convert's own admin-only submission.
- `GET /api/v1/ena-import-batch/{idx}` — the batch's current, rolled-up per-accession
  status: `pending` / `resolving` / `registered` / `downloading` / `done` / `failed`,
  with `study_idx` and the download ticket idx(s) once resolved, and a
  `failure_reason` on any `failed` item. An item whose download ticket failed or was
  cancelled reports `failed`. Also admin-only.

A control-plane restart re-drives every accession still `pending`, `resolving` or
`registered`, unless the batch's submitter has since been disabled or retired: those
accessions fail with that reason instead, and a re-import by an active admin picks
them up.

The actual read download runs as the `download-ena-study` workflow
(`workflows/download-ena-study/1.0.0.yaml`), the same `qiita ticket status` /
`qiita ticket logs` / `qiita ticket run` commands used for any other workflow apply to
it once submitted.

### Metadata harmonization

Every ENA-imported biosample is bound to the **ERC000011** checklist (the ENA default
sample checklist) — the same shared checklist model every other metadata path in
Qiita uses. `GET /api/v1/ena-import-batch/{idx}` returns an `ena_runs` array per item,
one entry per ENA run carrying its `status` and a `failure_reason` when it failed. A
harmonization error (an unparseable value, or a cross-study metadata slot collision)
fails the run, surfacing as that run's `status: failed` + `failure_reason`, isolated
per-run exactly like an unmappable platform. A checklist-required field ENA did not
supply is not itself an error — the checklist binding, not enforcement, is what
harmonization records.

**A sample with zero ENA attributes is a legitimate, common result, not an import
failure.** Real ENA/DDBJ samples sometimes carry no `<SAMPLE_ATTRIBUTE>` elements at
all (confirmed live against DDBJ study `PRJDB40364`'s sample `SAMD01818724`). Such a
sample still registers normally — study, biosample, and sequenced/prep rows are all
created — it simply harmonizes against an empty attribute map, so it carries no
globally-linked metadata.

**Both the GSC-MIxS display-name and the underscore MIxS short-name vocabularies are
recognized**, since real submitters (notably DDBJ) commonly use the latter:
`collection_date`, `geo_loc_name`, `lat_lon`, and `depth` are harmonized onto the same
global fields as their `collection date` / `geographic location (...)` / `depth`
display-name twins. `geo_loc_name`'s `country:region:locality` value contributes only
its country/sea part; `lat_lon`'s combined `"<lat> <N|S> <lon> <E|W>"` value splits
into the separate latitude/longitude fields (negated for S/W), or is left as raw local
metadata if it doesn't parse (including an INSDC missing-value marker like
`"missing"`, which real DDBJ submissions do use for `lat_lon`). The three
environmental-context tags (`env_broad_scale`/`env_local_scale`/`env_medium`, and
their GSC-MIxS display-name twins) stay unmapped in either vocabulary — see
"No ENVO / taxon-ontology harmonization" below.

## Scope and limits

These are **hard limits** of the current import surface, not partial-implementation
gaps expected to close soon (except where noted):

- **Reads and metadata only.** No host-genome handling, no downstream processing
  beyond landing raw reads in DuckLake — an imported study's reads still go through
  the normal read-mask / alignment pipeline like any other ingested data.
- **INSDC archives only.** ENA, SRA and DDBJ mirror each other and every accession
  resolves through ENA's API, so the archive that minted it does not change the path.
  GSA (China National GeneBank / BGI) and CNGB are out of scope; there is no resolver
  or accession-prefix support for either.
- **No Aspera.** The download job fetches over `http`: no Aspera key-staging
  exists in this compute environment. The transport is the job's to choose, so
  neither the batch nor its tickets pin one.
- **DDBJ / legacy-platform metadata is a known gap, not yet closed.** The platform
  and library-strategy mapping tables cover the INSDC/ENA-native platform and
  strategy vocabulary; a DDBJ-submitted record with a legacy or DDBJ-specific
  platform string can fail platform mapping for that run alone (isolated, per the
  per-run failure model above) rather than importing correctly. Filling out DDBJ
  coverage is deferred to the backlog.
- **No ENVO / taxon-ontology harmonization.** Free-text environment and taxonomy
  fields ENA supplies are kept as study-local text as given; there is no ENVO term
  resolution or NCBI taxon-id cross-referencing in this path. Also deferred to the
  backlog.

## The duckdb-miint dependency

Metadata resolution and read download both go through `duckdb-miint` table functions,
not a hand-rolled ENA client:

- `read_ena` — study header + run list.
- `read_ena_attributes` — per-sample attributes, grouped into one map per sample.
- `read_ena_sequences` — the actual read download, called by the `ingest_ena_reads`
  compute job once a pool's runs are registered.

**md5 verification is miint's.** `read_ena_sequences` verifies each downloaded FASTQ
file against ENA's published `fastq_md5` by default (`verify_md5`, duckdb-miint#172;
see miint's [`insdc_ena` docs](https://the-miint.github.io/duckdb-miint/insdc_ena/)),
and a mismatch fails the run. Where verification does not apply (an SFF run, a file
that is not gzip-compressed, no `fastq_md5` from ENA) the run still registers. A run
that comes back truncated or empty fails loud either way.

**Network access.** The control-plane host resolves metadata from `www.ebi.ac.uk`, and
the SLURM compute nodes running `ingest_ena_reads` reach both `www.ebi.ac.uk` and
`ftp.sra.ebi.ac.uk`, all over HTTPS. Two deploy rows HEAD those hosts so a blocked
one fails at deploy rather than at the first import — `ena-reachability` from the
control-plane host and `probe/ena-from-compute` from a compute node. What each row
covers, what a green one does *not* prove, and which hatch skips which are in
[`redeploy.md` §7](redeploy.md#7-verify).

An unresolvable accession (malformed, or one ENA does not recognize) fails loud with
an actionable message rather than resolving to a silent empty result — see
`ena_import.accession` for the accepted prefix sets per accession kind.
