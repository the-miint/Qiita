-- migrate:up

-- Add the 'artifact_sequence_set' reference kind: an indexless set of artifact
-- sequences (the canonical adapter set the QC step trims against). Ingested via
-- the same kind-agnostic reference-add flow as a sequence_reference, but carries
-- no taxonomy and builds no rype/minimap2 index. Mirrors
-- qiita_common.models.ReferenceKind. Kind stays plain TEXT + CHECK (not a
-- Postgres ENUM), per the comment on the original reference migration — so this
-- is a CHECK widen, not an ENUM ADD VALUE, and there is no ENUM_PAIRS entry.
--
-- `reference_kind_check` is Postgres's auto-generated name for the column-level
-- CHECK on `kind` in 20260501000003_reference.sql — the same `<table>_<column>_check`
-- convention 20260601000001 relied on to DROP `reference_status_check`, which
-- shipped cleanly, so the name is validated empirically.
ALTER TABLE qiita.reference
    DROP CONSTRAINT reference_kind_check;

ALTER TABLE qiita.reference
    ADD CONSTRAINT reference_kind_check
    CHECK (kind IN ('sequence_reference', 'taxonomy_authority', 'artifact_sequence_set'));


-- migrate:down

-- Re-tighten to the original set. Fails loudly if any row is an
-- artifact_sequence_set, which is correct — don't silently strand one.
ALTER TABLE qiita.reference
    DROP CONSTRAINT reference_kind_check;

ALTER TABLE qiita.reference
    ADD CONSTRAINT reference_kind_check
    CHECK (kind IN ('sequence_reference', 'taxonomy_authority'));
