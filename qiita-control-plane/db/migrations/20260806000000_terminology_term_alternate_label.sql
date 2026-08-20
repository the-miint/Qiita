-- migrate:up
-- =============================================================================
-- TERMINOLOGY TERM — alternate label
-- =============================================================================
-- A second name for a term, for terminologies whose source supplies one
-- alongside the name that becomes `label`. NCBI Taxonomy is the case that
-- motivates it: the scientific name is the label, and the genbank common name
-- is what a person looking for a taxon is most likely to type. The source's
-- remaining name classes (synonyms, authority) are deliberately not retained.
--
-- Single-valued. A source offering several alternate names for one term needs
-- a child table, not a wider column.
--
-- VARCHAR(500) matches `label`, and the width is the point: this column holds
-- a name. A free-text definition does not belong in it.

ALTER TABLE qiita.terminology_term
    ADD COLUMN alternate_label VARCHAR(500),
    ADD CONSTRAINT terminology_term_alternate_label_nonempty
        CHECK (alternate_label IS NULL OR length(alternate_label) >= 1);

COMMENT ON COLUMN qiita.terminology_term.alternate_label IS
    'A second name for the term, when the source supplies one alongside the '
    'name carried in label. For NCBI Taxonomy, label is the scientific name '
    'and this is the genbank common name. NULL when the source offers no '
    'second name, or when the release the term arrived in extracted none. '
    'Never a free-text definition — the width matches label because this '
    'column holds a name.';

COMMENT ON CONSTRAINT terminology_term_alternate_label_nonempty ON qiita.terminology_term IS
    'An empty string is a botched extraction, not a term whose second name is '
    'blank; absence is spelled NULL.';


-- migrate:down

ALTER TABLE qiita.terminology_term
    DROP CONSTRAINT terminology_term_alternate_label_nonempty,
    DROP COLUMN alternate_label;
