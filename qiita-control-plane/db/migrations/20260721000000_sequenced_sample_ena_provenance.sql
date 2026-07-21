-- Provenance for sequenced_sample rows created by the ena_import registration
-- composer (TASK-02): which public archive the study/sample metadata (and,
-- once TASK-04 lands, the read bytes) came from, which EnaResolver
-- implementation produced it, and which transport downloaded the reads.
--
-- All three columns are additive, nullable, and TEXT/CHECK rather than a
-- Postgres ENUM -- same carve-out as upload.status / reference.status (see
-- CLAUDE.md "Enum parity"): they are populated only for rows the ena_import
-- path creates, so every pre-existing sequenced_sample row (and every
-- non-ENA import going forward) simply carries NULL in all three, and NULL
-- vacuously satisfies an `IS NULL OR ... IN (...)` CHECK.
--
-- `transport` is deliberately populated by nothing in this migration's scope
-- -- TASK-02's registration composer never writes it (metadata resolution
-- only, no read download yet); TASK-04's download workflow populates it once
-- read_ena_sequences actually fetches bytes over http or aspera.

-- migrate:up
ALTER TABLE qiita.sequenced_sample
  ADD COLUMN source_archive TEXT
    CHECK (source_archive IS NULL OR source_archive IN ('ena', 'sra')),
  ADD COLUMN resolver_kind TEXT
    CHECK (resolver_kind IS NULL OR resolver_kind IN ('miint', 'http')),
  ADD COLUMN transport TEXT
    CHECK (transport IS NULL OR transport IN ('http', 'aspera'));

COMMENT ON COLUMN qiita.sequenced_sample.source_archive IS
  'Mirrored by qiita_common.models.ena.SourceArchive. Stored as TEXT/CHECK, '
  'not a Postgres ENUM -- same carve-out as upload.status / reference.status; '
  'see CLAUDE.md "Enum parity". Which public archive (ENA/SRA) this sample''s '
  'study/sample metadata was resolved from. NULL for every row not created by '
  'the ena_import registration composer (TASK-02).';

COMMENT ON COLUMN qiita.sequenced_sample.resolver_kind IS
  'Mirrored by qiita_common.models.ena.ResolverKind. Stored as TEXT/CHECK, '
  'not a Postgres ENUM -- same carve-out as upload.status / reference.status; '
  'see CLAUDE.md "Enum parity". Names which qiita_control_plane.ena_import.'
  'EnaResolver implementation (BACKEND_MIINT / BACKEND_HTTP) produced this '
  'sample''s imported metadata. NULL for every row not created by the '
  'ena_import registration composer (TASK-02).';

COMMENT ON COLUMN qiita.sequenced_sample.transport IS
  'Which transport (http / aspera) TASK-04''s read_ena_sequences download '
  'used to fetch this sample''s reads. Column added in TASK-02 but left '
  'unpopulated by it -- TASK-02 resolves metadata only, no read bytes; '
  'TASK-04''s download workflow is what writes this column.';

-- migrate:down
ALTER TABLE qiita.sequenced_sample
  DROP COLUMN transport,
  DROP COLUMN resolver_kind,
  DROP COLUMN source_archive;
