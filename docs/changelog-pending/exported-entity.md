### Added

- **Studies and biosamples can be named publicly without exposing an internal identifier (#682).**
  A new `qiita.exported_entity` table mints a permanent handle — `QS<n>` for a study, `QB<n>` for
  a biosample — so anything crossing the Qiita boundary can name one without carrying its `*_idx`.
  A trigger mints the handle in the transaction that inserts its entity, whichever path inserts
  it, and the migration backfills one for every existing study and biosample. Study responses
  (the full record, the summary view and each row of the study list), biosample responses, and
  each result of a biosample import carry it as `export_entity_id`. Postgres composes the handle
  as a generated column, so no caller can supply one and none can be edited after publication.
  The migration records the conditions an entity must satisfy to belong in the table, and why the
  table carries no retirement columns.
