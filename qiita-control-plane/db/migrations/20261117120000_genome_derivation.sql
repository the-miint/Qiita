-- migrate:up

-- One row per derived genome: the genome whose sequence was produced from another
-- assembly, and that assembly. The COMMENTs below carry each column's rationale.
CREATE TABLE qiita.genome_derivation (
    genome_idx                BIGINT      PRIMARY KEY
                                          REFERENCES qiita.genome (genome_idx) ON DELETE CASCADE,
    parent_source             TEXT        NOT NULL,
    parent_source_id          TEXT        NOT NULL,
    method                    TEXT        NOT NULL,
    asserted_by_reference_idx BIGINT      NOT NULL
                                          REFERENCES qiita.reference (reference_idx) ON DELETE RESTRICT,
    created_at                TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT genome_derivation_parent_check CHECK (
           (parent_source = 'genbank' AND parent_source_id ~ '^GCA_[0-9]{9}\.[0-9]+$')
        OR (parent_source = 'refseq'  AND parent_source_id ~ '^GCF_[0-9]{9}\.[0-9]+$')
    ),
    CONSTRAINT genome_derivation_method_check CHECK (method IN ('wol_concatenated'))
);

COMMENT ON TABLE qiita.genome_derivation IS
    'One row per derived genome: the assembly its sequence was produced from, how, and '
    'which reference asserts it. A genome with no row is not known to be derived.';
COMMENT ON COLUMN qiita.genome_derivation.parent_source IS
    'The parent assembly''s archive: genbank (GCA_) or refseq (GCF_).';
COMMENT ON COLUMN qiita.genome_derivation.parent_source_id IS
    'The parent''s versioned assembly accession, e.g. GCF_000005825.2. No FK: a '
    'qiita.genome row stands for sequence that Qiita holds, and Qiita holds none of an '
    'assembly that no reference loaded. A parent that some reference did load joins on '
    '(source, source_id).';
COMMENT ON COLUMN qiita.genome_derivation.method IS
    'How the derived sequence was produced from the parent. wol_concatenated: Web of Life '
    'host-masked the assembly and concatenated its contigs into one sequence, so the '
    'derived genome''s coordinates are not the parent''s.';
COMMENT ON COLUMN qiita.genome_derivation.asserted_by_reference_idx IS
    'The reference whose release asserts the derivation. RESTRICT: deleting that '
    'reference must not silently withdraw the provenance that it asserted.';

-- migrate:down

DROP TABLE qiita.genome_derivation;
