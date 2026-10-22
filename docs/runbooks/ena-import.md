# ENA study import (runbook)

**For:** an operator (wet_lab_admin or system_admin) importing one or more public
public INSDC studies' metadata and reads into Qiita. Read the *Scope and limits* section
before the first import on a new deploy — several boundaries here are hard limits,
not "not implemented yet."

Auth and the general CLI/API flow are **not** repeated here — see
[`getting-started.md`](getting-started.md), step 0. This runbook covers only what is
specific to importing from ENA.

## What an import does

A single admin-facing call kicks off a **batch**: a list of INSDC study accessions
(`PRJNA…`, `PRJEB…`, `PRJDB…`, `ERP…`, `SRP…`, `DRP…`), or run accessions (see
[Importing chosen runs](#importing-chosen-runs)). Each study in the batch is
processed independently, with bounded concurrency, in three phases. That bound is shared
by every batch running in the control plane at once, first come first served: a later
batch's studies are not admitted until every study of every batch submitted before
it has itself been admitted, so a busy batch can make a newly submitted one wait for it
to clear the gate first. Listed runs take one slot to be resolved and grouped by study,
then each study's runs queue again behind anything already waiting.

1. **Resolve** — the study's header, run list, and per-sample attributes are pulled
   from ENA (via the `duckdb-miint` `read_ena` / `read_ena_attributes` table
   functions).
   An ENA 5xx, 408 or 429, or a connection failure, is retried with backoff while the
   item stays `resolving`; after six hours of continuous outage on one ENA call the item
   fails naming the outage.
   A control-plane stop during an outage waits for the batch drain, up to 60 s; the item
   is then cancelled, stays `resolving`, and is re-driven when the control plane restarts.
2. **Register** — the resolved metadata is turned into Qiita rows: one `study`, one
   `biosample` per distinct ENA sample accession (de-duplicated **across studies** —
   two studies that share a sample converge on the same biosample row, never a
   duplicate), and one `sequenced_sample`/`prep_sample` per run. Runs are grouped by
   mapped platform into one `sequencing_run` per `(study, platform)` pair, and new
   runs go into a `sequenced_pool` on it — a multi-platform study yields more than one
   pool. Each run's ENA sample attributes are harmonized onto its biosample's metadata
   the first time that biosample is created (a re-import or a cross-study reuse does
   not re-harmonize). Each run also keeps ENA's four deposited `library_*` values as
   study-local `ena library strategy` / `ena library source` /
   `ena library selection` / `ena library layout` metadata on its prep_sample — new
   imports only, since runs imported before that writer existed are not backfilled,
   and a field ENA left blank writes no row.
3. **Submit** — one `download-ena-study` work ticket per pool holding the study's
   runs, scoped to that `sequenced_pool`. A pool whose ticket is in flight or finished
   reuses it; one with no ticket, or whose ticket failed or was cancelled, gets a new
   one. This is the ticket that actually pulls read bytes; registration itself never
   touches read data.

That gate ends at submit. The background dispatch each submitted ticket starts runs
past it under its own process-wide bound: at most **8** dispatch tasks run at once
(`_DISPATCH_CONCURRENCY` in `qiita-control-plane/src/qiita_control_plane/dispatch.py`,
download workflows included), so a large import queues its downloads rather than
pressuring the control plane's connection pool. A ticket past the cap dispatches as
soon as a slot frees — nothing fails while it waits — and logs `queued behind the
dispatch cap` at INFO when it starts waiting, `dispatched after waiting` when it gets
one. The queue is a single FIFO shared by every dispatch path: ENA downloads, user-
submitted tickets, a redrive through `POST /work-ticket/{idx}/run`, and a restart's
re-attach all wait on the same 8 slots, so eight hours-long downloads can hold a user
ticket until one finishes. Raising the cap is a joint decision with the connection
pool and `FANOUT_MAX_INFLIGHT` — the sizing note on the constant says what it is
weighed against.

A download ticket fetches **4** runs at once by default. `ENA_DOWNLOAD_RUN_CONCURRENCY`
(1-8, control-plane env, restart to change) sets it for every ticket an import batch
submits; a manually submitted `download-ena-study` ticket can carry `run_concurrency` in
its `action_context` instead; the job's start-up line on stderr shows the value in effect.
The batch stores the value in the ticket's `action_context`, so a changed value reaches
only tickets built after the restart: a `/run` redrive, an automatic retry, and a batch
reusing a pool's existing ticket keep the value the ticket was built with (4 if none). The
step's memory is a fixed allocation divided across the slots, so more concurrency means
less memory per run, and scratch disk grows with it:

| `run_concurrency` | GB per run at 56 GB | DuckDB threads (total) |
|---|---|---|
| 1 | 53 | 2 |
| 4 (default) | 12 | 8 |
| 8 (max) | 5 | 16 |

Worst-case connections to ENA are 8 dispatch slots times this value, so raise it by
measuring. Above 4 the step oversubscribes its 8 CPUs during sorts. Keep production at
4: 8 concurrent sorts at 5 GB per run is untested on DuckDB 1.5.5.
On the local backend all tickets share one executor, so the effective total is lower.

A download ticket's roster read also waits at most 90 s
(`POOL_LOCK_WAIT_TIMEOUT_S`) for an in-flight registration of the same
sequencing_run to commit, so a study of many hundreds of runs cannot fail the
read by holding that lock past the pool's default 10 s statement budget. A
download ticket that still fails there is FAILED/RETRIABLE: let the import
finish, then redrive it with `POST /work-ticket/{idx}/run` — the redrive re-runs
the download against the current roster; runs already stored are not fetched again.

### Importing chosen runs

Run accessions (`SRR…`, `ERR…`, `DRR…`) may be mixed with study accessions in one
submit. The runs become a single ungrouped item that the control plane resolves in the
background and splits into one item per study they belong to; each item then registers
only its own listed runs. The study is created or reused exactly as for a
whole-study import, so the same import-created-study guard applies.

- **One unavailable run fails the whole item.** A run ENA does not return, or returns
  non-public, fails the ungrouped item with `run(s) unavailable at ENA: …` naming every
  such run, and nothing is registered or flagged. Fix the list and resubmit.
- **Alongside a whole-study import.** A whole-study import of the same study, earlier or
  later, skips the runs already present and registers the rest. A whole-study accession in the
  same batch that exactly matches a run's study absorbs those runs, so no item is
  duplicated. If every run is absorbed, the run-list item takes the absorbing items'
  download tickets, every pool's, and ends `done` or `failed` as they do; its study
  accession stays null. The match is on the exact string, so an `ERP…` whole-study item
  alongside runs whose study reads `PRJEB…` becomes two items for one study. Each run
  is still registered once, the same as with two concurrent batches.
- **Only its own pools are ticketed.** A per-study run-list item submits or reuses a
  download ticket only for pools holding its runs; re-import the whole study to retry
  other failed pools.
- **A failed pool's other runs download too.** New runs go into the newest pool without a
  covering ticket. If that pool's earlier ticket failed, the item's new ticket reads the
  pool's whole roster, so its unread runs are downloaded along with the listed ones.
  Registration still covers exactly the listed runs.

### Re-importing, and studies we created ourselves

Re-importing an accession is the supported way to pick up runs a bioproject gained
since the last import: runs already registered come back as `skipped_already_present`
and only the new ones are added. A download ticket reads its pool's run list once, when
it starts, so new runs never join a pool whose download is in flight or finished — they
go into a new pool with its own ticket, and the accession reports `done` only once every
pool's download has. A re-import is also how to retry a failed or cancelled download,
as is `POST /work-ticket/{idx}/run` on the pool's latest such ticket, which picks up runs
added since. `/run` is refused with a 409 on an older ticket (the error names the newer
one and its state), and on a failed ticket that still has a live download job from before
the failure, which holds the old roster: cancel it by idx (`work_ticket_idxs`) to reap the
job, then re-import the study. The ticket stays FAILED and keeps refusing `/run`.
A re-import does not retry the failed runs of a `completed_partial` pool: its ticket is
COMPLETED and is reused, and the runs keep reporting `download_failed`.
Nothing schedules a re-import or a redrive; each is an operator gesture. A native
`POST .../sequenced-sample` add to a pool whose download has already read (or is reading) its
roster is refused the same way, 409: create a new `sequenced_pool` on the run and submit
its own `download-ena-study` ticket instead. A `PATCH /sequenced-sample/{idx}` that
changes `ena_run_accession` is refused (409) in those states, when a failed ticket still
has a live download job, and once the sequenced_sample's reads have a `sequence_range`
(the download reuses them rather than fetching the new run); retire its prep_sample and
add the corrected run to a new `sequenced_pool` the same way.

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

A failure in any one item — an unmappable platform, a resolver error, a database
conflict — is recorded on that item alone; it never aborts the batch or its
sibling items. Listed runs share one item until they are grouped by study, so one
unresolved run fails them all (see [Importing chosen runs](#importing-chosen-runs)).
Poll with `qiita ena-import-status IDX` to see each item's state and, on failure, the
reason.

### Non-public studies and runs

ENA reports a `status` (`public` or `suppressed`) on every study and run; any other
value fails the accession loudly rather than being treated as public. A **suppressed
study** fails the whole accession before anything is written, naming its status
(`study PRJ... is suppressed`). A study with **no public runs at all** fails the same
way (`no public runs`), before the study is even resolved or created. Within an
otherwise-public study, an individual **suppressed run** is `excluded` — isolated from
its siblings exactly like an unmappable platform, carrying a `failure_reason` naming
the run and its status, and contributing no `biosample`/`prep_sample`/`sequenced_sample`
rows. When a pool was created (at least one run's platform mapped) but every run in
the item still ends up `excluded` and/or `failed` — none reaches
`registered`/`skipped_already_present` — the item fails with "every run failed to
register", naming every excluded or failed run; if no platform ever mapped, no pool
exists and the item instead fails with "no run mapped to a downloadable pool".

Resolution only sees public records (see `qiita_common.models.ena`), so these checks
rarely fire in practice; they enforce the rule rather than assume it.

### ENA availability re-check on re-import

A run Qiita already holds can stop being public between imports: ENA suppresses,
withdraws, or replaces it. The Portal endpoint that resolution uses only ever
returns public records, so it cannot report this itself — **re-importing the
same study**
checks it. Every held run the fresh Portal response no longer names as public is
looked up against ENA's Browser API (`summary/{accession}`), which reports every
run's status regardless of availability. Runs are asked for in batches; a batch
that returns HTTP 500 is re-asked one run at a time, so one run ENA can't serve
doesn't fail the rest. Each request is retried once on a transport error or
a 5xx.

- **Public** (or the run reappears as public in the Portal's fresh response)
  clears any existing flag.
- **Any other status ENA reports** flags the run, storing ENA's own status text on
  `sequenced_sample.ena_status`, and a `flagged_unavailable` outcome on the batch
  item naming it. The outcome is recorded as soon as the flag commits, so it stays
  on the item even if a later step fails it.
- **A run that still errors on its own — HTTP 500 (per ENA's docs, a withdrawn
  run), or a response that leaves it out (observed live for a nonexistent
  accession) — fails the whole item and writes no flags**: neither is evidence a
  run is unavailable, and an availability check never leaves a run half-updated.
- **A status this codebase does not recognize** fails the item the same way.

A run-list import re-checks only its listed runs, and none when ENA does not return
their study or returns it non-public. A listed run ENA no longer returns is not flagged;
it fails the item as unavailable (see [Importing chosen runs](#importing-chosen-runs)).
Re-import the whole study to flag it.

**A study blocked by one run.** A held run that keeps returning 500 fails every
re-import of its study before anything new registers. Check the run on the ENA
Browser by hand; if ENA has withdrawn it, retire its prep_sample
(`qiita prep-sample retire --prep-sample-idx N --reason "withdrawn from ENA"`). A
retired run is not re-checked, so the next re-import proceeds.

The same check runs when a re-import has nothing new to register: the study is
absent from the Portal, is not public, or has no public runs. The Portal drops
non-public runs, so "every run suppressed" arrives this way. If Qiita holds a
study created by an earlier import, its held runs are checked and flagged or
cleared, and the item fails naming any it flagged.

**A held run nothing will download.** A run flagged before its pool's download
read the roster is left off that download. If ENA later releases it again, the
flag clears, but the pool's download has already completed. The re-import reports
such a run `held_not_downloaded` instead of `skipped_already_present`. There is no
automatic retry for it yet.

**The exclusion rule: a flagged run is skipped wherever a sample-selecting query
already filters out a retired prep_sample.** That includes the download roster,
the block planners, the run, pool and study rosters, the amplicon pool roster, the
rollups and exports, `has_sequenced_sample` (so a pool whose only runs are flagged
gets no download ticket), `mask purge-failed` and the backfills. It is not deleted or retired, so it stays
visible on the sample's own record and reappears the moment a later re-import finds
it public again. Two things do **not** filter it, deliberately: a route that reads
a sample by its own id (`read_masked`, alignment/assembly DoGets, the per-sample
masked-export ticket) is not gated on `retired` either and is not gated on the flag;
and pool delete plus the `ena_run_accession` re-registration dedupe must keep
seeing a flagged row to work at all.

**Nothing is cancelled automatically.** Flagging logs (does not act on) any
non-terminal work ticket still touching the run's pool or sample, naming the
ticket so an operator can cancel it through the existing
`POST /api/v1/work-ticket/cancel` filter if the work is no longer wanted.

**Limits:** detection only happens on re-import — there is no periodic recheck of
a study nobody has re-submitted. An `export_id` minted before a run was flagged
stays valid; a published identifier does not change because ENA's availability
did.

### REST surface

- `POST /api/v1/ena-import-batch` — body: `{accessions: [...]}`. Returns `202` immediately with a batch
  handle and one `pending` item per study accession, plus, when run accessions are
  given, one with a null `ena_study_accession` holding them all; the resolve/register/
  submit work for the whole batch runs in the background. **Admin-only**
  (wet_lab_admin or system_admin) — this is an operator gesture, not something an
  end user submits, mirroring bcl-convert's own admin-only submission.
- `GET /api/v1/ena-import-batch/{idx}` — the batch's current, rolled-up per-item
  status (the run item is split into one item per study once its runs resolve):
  `pending` / `resolving` / `registered` / `downloading` / `done` / `completed_partial` /
  `failed`, with `study_idx` and the download ticket idx(s) once resolved, and a
  `failure_reason` on any `failed` or `completed_partial` item. An item whose download
  ticket failed or was cancelled reports `failed`. An item whose ticket completed but
  has a run that failed permanently and stored no reads reports `completed_partial`;
  its `failure_reason` lists the failed runs, and each shows in `ena_runs` as
  `download_failed` with its `failure_reason` and `failure_type`. Also admin-only.

A control-plane restart re-drives every item still `pending`, `resolving` or
`registered`, unless the batch's submitter has since been disabled or retired: those
items fail with that reason instead, and a re-import by an active admin picks
them up.

`qiita submit-ena-import ACCESSION [ACCESSION ...]` (or `--from-file FILE`, one accession
per line — blank lines and whole-line `#` comments are skipped, but a line may not
carry a trailing comment or more than one accession) drives the batch endpoints above.
It validates every accession locally before submitting — one bad shape refuses the
whole call — then by default polls to terminal with `--poll-interval-seconds` (default
2s) between polls up to `--timeout-seconds` (default 86400s); `--no-watch` returns
right after submit. A transient failure while polling — a dropped connection or a 5xx —
is retried until the timeout instead of failing the watch; any other HTTP error is
fatal. Exit codes: `0` every item reached `done`; `1` an item ended `failed` or
`completed_partial`, the watch timed out (naming each still-pending item and its last
known state), an HTTP error, or no token was found; `2` bad input (a malformed
accession, a bad `--from-file` line, an out-of-range flag); `130` Ctrl-C, naming the
batch to poll if the POST had already succeeded. `qiita ena-import-status IDX` reads a
batch's current state and
always exits `0` once the read itself succeeds. See the REST bullets above for the
state set and what each field means.

The actual read download runs as the `download-ena-study` workflow
(`workflows/download-ena-study/1.0.0.yaml`), the same `qiita ticket status` /
`qiita ticket logs` / `qiita ticket run` commands used for any other workflow apply to
it once submitted.

### Metadata harmonization

Every ENA-imported biosample is bound to the **ERC000011** checklist (the ENA default
sample checklist) — the same shared checklist model every other metadata path in
Qiita uses. `GET /api/v1/ena-import-batch/{idx}` returns an `ena_runs` array per item,
one entry per ENA run carrying its `status` (`registered` / `skipped_already_present` /
`excluded` / `failed` / `flagged_unavailable` / `held_not_downloaded` /
`download_failed` — `flagged_unavailable` and `held_not_downloaded` come from a
re-import's ENA availability re-check, see above) and a `failure_reason` on every
status but `registered` and `skipped_already_present`. `download_failed` also carries
a `failure_type`. A
harmonization error (an unparseable value, or a cross-study metadata slot collision)
fails the run, surfacing as that run's `status: failed` + `failure_reason`, isolated
per-run exactly like an unmappable platform or a suppressed run (see *Non-public
studies and runs* above). A checklist-required field ENA did not supply is not itself
an error — the checklist binding, not enforcement, is what harmonization records. A
coordinate past its bound — latitude beyond ±90, or longitude beyond ±180 — is one such
unparseable value: every run referencing that sample fails until the coordinate is
corrected at ENA, so a study submitted on a 0–360 longitude convention, where every
sample is off, fails every run.

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
`"missing"`, which real DDBJ submissions do use for `lat_lon`). The split checks
shape only: a latitude past ±90 or a longitude past ±180 (or a non-finite value),
from `lat_lon` or either coordinate tag, is a metadata parse error on every biosample
write, so the run fails to register with that error as its `failure_reason`. The three
environmental-context tags (`env_broad_scale`/`env_local_scale`/`env_medium`, and
their GSC-MIxS display-name twins) stay unmapped in either vocabulary — see
"No ENVO harmonization or free-text host matching" below.

**Taxon ids come from ENA.** A new biosample's `taxon id` is the run's `tax_id` and its
`host taxon id` is the run's `host_tax_id`. An id is written only if it is a loaded NCBI
Taxonomy term (`qiita-admin terminology prepare-taxdump` loads more, but biosamples
already imported are not revisited); anything else is written as `not provided` and
listed in the run's `metadata_warnings`. A `host_tax_id` that is loaded is written even
when it disagrees with the host the biosample's `tax_id` implies, with a warning. A
`host_tax_id` equal to the biosample's own `tax_id`, or to a taxon in the table (the
curated list of biosample taxa with an implied host, or none) or to the bare `metagenome`
taxon, is not a host and is ignored, with a warning.

With no usable `host_tax_id`, a free-text `host` that is not a missing-value term such as
`missing` gives `not provided` and a warning quoting the text. Otherwise the host comes
from the table: the listed human and mouse metagenomes give that host, and the listed
hostless environments such as seawater or soil give `not applicable`, meaning the
sequenced sample is submitted with no host depletion. A taxon the table does not cover
gives `not provided`, and host-filter resolution then returns unresolved and submit
refuses. `qiita-admin backfill host-taxon-id` reads the same table. An attribute tag
named `taxon id` or `host taxon id` is dropped with a warning, since those fields are
filled from the run's taxon ids (falling back to the table, `not applicable`, or
`not provided`).

A warning appears only on the run that created the biosample, so after a re-import or for
a later run on the same sample, check the creating batch.

A stored `not provided` stops host filtering at submit (unless the biosample's
`qiita sample type` makes it a control), and nothing rewrites it:
`qiita-admin backfill host-taxon-id` skips a biosample that already has a `host taxon id`
row, and a re-import does not revisit it (#669). To correct one, write the field on that
biosample with `PATCH /study/{S}/biosample/{B}/metadata` and body
`{"host taxon id": "<NCBI taxon id>"}`, or from the CLI:

```
qiita biosample patch-metadata-by-unique-field --study-idx S \
  --unique-field-display-name "ena sample id" --unique-field-value <sample alias> \
  --metadata "host taxon id=<NCBI taxon id>"
```

The value is a loaded NCBI Taxonomy id or a missing-value reason such as `not applicable`.
Either path takes one biosample per call and needs ADMIN on the study or `wet_lab_admin`.

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
- **A non-amplicon run deposited with `library_selection` PCR is refused.** PCR selection
  marks an amplicon library, so a run whose `library_strategy` is anything but `AMPLICON`
  (including `RNA-Seq`/transcriptomic) contradicts it. That run fails alone with a
  `failure_reason` naming the selection; its siblings import. Deposited metadata is not
  rewritten and the import has no override. Once the depositor corrects the ENA record,
  a re-import picks the run up. An operator who knows the run is amplicon can instead
  register it by hand: a biosample carrying the run's `ena_sample_accession` (the import
  rolled its own back; a later re-import reuses one with that accession), then
  `POST .../sequenced-sample` with `short_read_amplicon` or `long_read_amplicon` as its
  `prep_protocol_idx` and the run's `ena_run_accession`, in a new `sequenced_pool` with its
  own `download-ena-study` ticket, as under "Re-importing" above. #383 tracks inferring the
  protocol from the reads instead. `RANDOM PCR` and `RT-PCR` are different selections and
  are not refused.
- **No ENVO harmonization or free-text host matching.** Free-text environment and host
  fields ENA supplies are kept as study-local text as given; only ENA's numeric
  `tax_id` and `host_tax_id` are cross-referenced to NCBI Taxonomy. Also deferred to the
  backlog.

## The duckdb-miint dependency

Metadata resolution and read download both go through `duckdb-miint` table functions,
not a hand-rolled ENA client:

- `read_ena` — study header + run list.
- `read_ena_attributes` — per-sample attributes, one map per sample. A tag with one value is
  stored as is. A tag ENA repeats with several values is stored study-local as a JSON array of
  its sorted distinct values (`["10", "5"]`), never on a typed global field.
- `read_ena_sequences` — the actual read download, called by the `ingest_ena_reads`
  compute job once a pool's runs are registered.

**md5 verification is miint's.** `read_ena_sequences` verifies each downloaded FASTQ
file against ENA's published `fastq_md5` by default (`verify_md5`, duckdb-miint#172;
see miint's [`insdc_ena` docs](https://the-miint.github.io/duckdb-miint/insdc_ena/)),
and a mismatch fails the step. miint cannot tell a transfer truncated on a gzip member
boundary from bytes that really disagree with the digest, so the step is retried up to
the ticket's retry limit; when the retries run out the ticket ends FAILED with
failure_type `permanent` and both digests in the reason. A run that broke mid-download
(including a corrupt body that surfaces mid-stream) and an HTTP 5xx, 408 or 429 from ENA
or its file server, a failed connection to the server and an HTTP status 0 (no usable
status) are retried the same way, as is ENA's "Not all results may have been
written" answer with a miint build that includes duckdb-miint#310. A run whose file
cannot be opened depends on the miint build: before duckdb-miint#312 miint skips it with
a warning, which is retried like the rest, so a 404 costs the full retry limit; from
duckdb-miint#312 on, a 404 fails at once.

Where verification does not apply (an SFF run, a file that is not gzip-compressed, no
`fastq_md5` from ENA) the run still registers. A run that comes back empty with no
warning fails permanently.

A run that fails in a way a retry cannot clear (a format error, a run with no sequence
data, a 404 from duckdb-miint#312 on, zero reads) does not fail its pool when another run
was stored: the other runs load, the ticket completes, and the failed run is recorded with
its reason and `permanent` failure type (the item reads `completed_partial`). The ticket
is COMPLETED, so the submitter gets the work-ticket digest unless they opted out of
processing emails, and it carries no `failure_reason`; read the batch status for the
failed runs. If a run failed in a way a retry could clear, the step still fails and
retries first, and a run that failed permanently is fetched again on each retry, so a
large one is downloaded again. Any other error fails the step as a whole: out of memory
retries with more memory, and anything else (a local disk fault) leaves the ticket FAILED
and redrivable.

When the retries run out, or at once when no run was stored and none of the failures is one
a retry could clear (a 404 from duckdb-miint#312 on), the ticket is FAILED `permanent` and
emailed with its reason.
Read the reason first. A dropped transfer or an ENA outage: redrive the ticket
(`POST /work-ticket/{idx}/run`) once ENA is healthy; runs already stored are skipped, and
a redrive resets `retry_count`, so it buys the full budget again. A bad digest or a 404:
compare against the ENA Portal before redriving, since the same bytes fail the same way;
if the run is gone or its digest is wrong at ENA, re-import once ENA is corrected. Each
retry resubmits the whole `ingest_ena_reads` step (resources: `baseline_resources` in
`workflows/download-ena-study/1.0.0.yaml`), the requeue follows the failure with no
delay or backoff, and `max_retries` is the ticket's one budget, shared with the OOM and
timeout retries of that step.

**Network access.** The control-plane host resolves metadata from `www.ebi.ac.uk`, and
the SLURM compute nodes running `ingest_ena_reads` reach both `www.ebi.ac.uk` and
`ftp.sra.ebi.ac.uk`, all over HTTPS. Two deploy rows HEAD those hosts so a blocked
one fails at deploy rather than at the first import — `ena-reachability` from the
control-plane host and `probe/ena-from-compute` from a compute node. What each row
covers, what a green one does *not* prove, and which hatch skips which are in
[`redeploy.md` §7](redeploy.md#7-verify).

An accession must be one of these prefixes followed by digits, e.g. `PRJEB11419`:

| Kind | Prefixes |
|---|---|
| study | `PRJNA`, `PRJEB`, `PRJDB`, `ERP`, `SRP`, `DRP` |
| sample | `SAMN`, `SAME`, `SAMD`, optionally followed by one letter (`SAMEA`, `SAMEG`) |
| run | `SRR`, `ERR`, `DRR` |
| experiment | `SRX`, `ERX`, `DRX` |

Submit takes study and run accessions; anything else, including a well-formed sample or
experiment, is refused at submit (CLI exit 2, HTTP 422) and no batch is created. An accession
of the right shape that ENA does not recognize fails loud with an actionable message
rather than resolving to a silent empty result.
