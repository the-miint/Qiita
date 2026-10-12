### Added

- **Qiita metadata maps into ENA submission bodies, driven by what each checklist requires (#642).**
  A new `checklist_field` table names a published checklist's fields in the publisher's own
  vocabulary, and `metadata_checklist_field` — renamed `metadata_checklist_requirement`, since a row
  is one requirement a checklist places — now links each to the Qiita field supplying its value and
  the unit it is submitted in. The five seeded ENA checklists get their mandatory fields, verified
  against ENA's published definitions, each checklist carrying its own full set rather than
  inheriting one. `ena_submission.mapping` builds a study's project body and a biosample's sample
  body from that. Values Qiita holds for unrequired fields stay home; a required field the biosample
  lacks fails there, naming the field, rather than taking down the whole envelope at submit. Three
  new text biosample fields — `env_broad_scale`, `env_local_scale`, `env_medium` — hold the
  environmental contexts as supplied, alongside the existing ENVO-resolved three.

- **The control plane gains a catalog for depositing studies and biosamples to ENA (#642).**
  `ena_submission.EnaSubmissionCatalog` opens a miint Webin V2 session — registering the secret,
  attaching the `ena` catalog over the service-side LOAD-only connect, and tearing both down on exit
  — then inserts projects and samples and hands back the accessions ENA assigns. Each study and
  biosample is deposited under its `export_entity_id` as the ENA alias. One `EnaObjectSpec` per
  object declares what a submission carries, so a column absent from it is never sent rather than
  sent as NULL. Samples go as one envelope, accepted or rejected whole, each row's accessions
  matched back to it by alias. A rejection is classified by its message text (duckdb-miint#291) into
  the three shapes that need different handling — already deposited, alias collision, failed
  checklist validation — each carrying what that path reports, so a caller can record an existing
  accession against the right row, resubmit, or fix the metadata without re-parsing a message. Blank
  aliases and checklists, and a repeated alias within one batch, are refused before anything is
  sent. The endpoint is an environment selector, `test` or `production` and never a URL, checked
  before a connection opens; the session bounds its own `threads` and `memory_limit`.

- **`qiita_common.sql.sql_string_literal` replaces the three private copies of the same SQL
  string-literal escaper in the control-plane CLI modules (#642).**
