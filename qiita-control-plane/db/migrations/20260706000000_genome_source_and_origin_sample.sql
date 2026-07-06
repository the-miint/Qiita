-- migrate:up

-- genome_source becomes a controlled vocabulary, and qiita-derived genomes gain
-- a link to the exact sample they originated from. Mirrors
-- qiita_common.models.GenomeSource (a StrEnum). `source` stays plain TEXT +
-- CHECK (not a Postgres CREATE TYPE ENUM), same rationale as
-- reference.kind/reference.status; the GenomeSource <-> CHECK drift is guarded
-- by qiita-control-plane/tests/test_genome_schema.py, so this value set and the
-- GenomeSource enum must stay in sync by hand.

-- Nullable link to the qiita sample a genome was derived from. NULL for external
-- (genbank/refseq) genomes; required for source='qiita' via the biconditional
-- CHECK below. ON DELETE RESTRICT per the schema-wide no-cascade convention.
ALTER TABLE qiita.genome
    ADD COLUMN prep_sample_idx BIGINT REFERENCES qiita.prep_sample (idx) ON DELETE RESTRICT;

ALTER TABLE qiita.genome
    ADD CONSTRAINT genome_source_check CHECK (source IN ('genbank', 'refseq', 'qiita'));

-- prep_sample_idx is set iff the genome is qiita-derived.
ALTER TABLE qiita.genome
    ADD CONSTRAINT genome_qiita_origin_check
    CHECK ((source = 'qiita') = (prep_sample_idx IS NOT NULL));

-- Reverse lookup + ON DELETE RESTRICT support (mirrors feature_genome(genome_idx)).
CREATE INDEX genome_prep_sample_idx ON qiita.genome (prep_sample_idx);


-- migrate:down

-- Drop the origin CHECK before the column it depends on. The FK and the index
-- on prep_sample_idx are removed with the column.
ALTER TABLE qiita.genome DROP CONSTRAINT IF EXISTS genome_qiita_origin_check;
ALTER TABLE qiita.genome DROP CONSTRAINT IF EXISTS genome_source_check;
DROP INDEX IF EXISTS qiita.genome_prep_sample_idx;
ALTER TABLE qiita.genome DROP COLUMN IF EXISTS prep_sample_idx;
