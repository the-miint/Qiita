-- Provenance for sequenced_sample rows created by the ena_import registration
-- composer: which public archive the study/sample metadata (and the read bytes)
-- came from, which resolver produced it, and which transport downloaded the reads.
--
-- All three columns are additive, nullable, and TEXT/CHECK rather than a
-- Postgres ENUM -- same carve-out as upload.status / reference.status (see
-- CLAUDE.md "Enum parity"): they are populated only for rows the ena_import
-- path creates, so every pre-existing sequenced_sample row (and every
-- non-ENA import going forward) simply carries NULL in all three, and NULL
-- vacuously satisfies an `IS NULL OR ... IN (...)` CHECK.
--
-- `transport` records the transport that fetched the reads. The registration
-- composer leaves it NULL (metadata resolution only, no read download); the
-- download-ena-study workflow stamps it (set_sequenced_pool_transport) once
-- read_ena_sequences has fetched the bytes.

-- migrate:up
ALTER TABLE qiita.sequenced_sample
  ADD COLUMN source_archive TEXT
    CHECK (source_archive IS NULL OR source_archive IN ('ena', 'sra')),
  ADD COLUMN resolver_kind TEXT
    CHECK (resolver_kind IS NULL OR resolver_kind IN ('miint')),
  ADD COLUMN transport TEXT
    CHECK (transport IS NULL OR transport IN ('http'));

COMMENT ON COLUMN qiita.sequenced_sample.source_archive IS
  'Mirrored by qiita_common.models.ena.SourceArchive. Stored as TEXT/CHECK, '
  'not a Postgres ENUM -- same carve-out as upload.status / reference.status; '
  'see CLAUDE.md "Enum parity". Which public archive (ENA/SRA) this sample''s '
  'study/sample metadata was resolved from. NULL for every row not created by '
  'the ena_import registration composer.';

COMMENT ON COLUMN qiita.sequenced_sample.resolver_kind IS
  'Mirrored by qiita_common.models.ena.ResolverKind. Stored as TEXT/CHECK, '
  'not a Postgres ENUM -- same carve-out as upload.status / reference.status; '
  'see CLAUDE.md "Enum parity". Names which resolver produced this sample''s '
  'imported metadata (qiita_control_plane.ena_import.miint_resolver.BACKEND_MIINT '
  '-- miint is the sole resolver). NULL for every row not created by the '
  'ena_import registration composer.';

COMMENT ON COLUMN qiita.sequenced_sample.transport IS
  'Which transport the read_ena_sequences download used to fetch this sample''s '
  'reads (http today; a future transport is a deliberate CHECK-widening '
  'migration, matching download_method). Added alongside registration but left '
  'unpopulated by it -- registration resolves metadata only, no read bytes; '
  'the download-ena-study workflow writes this column.';

-- migrate:down
ALTER TABLE qiita.sequenced_sample
  DROP COLUMN transport,
  DROP COLUMN resolver_kind,
  DROP COLUMN source_archive;
