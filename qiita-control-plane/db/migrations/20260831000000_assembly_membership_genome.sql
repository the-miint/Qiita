-- migrate:up

-- The assembly-origin feature -> genome edge.
--
-- Deliberately NOT qiita.feature_genome. That junction is the resolution substrate for
-- the GLOBAL qiita.reference_exclusion blocklist (a blocked genome expands to all its
-- features through it, unscoped by reference, enforced as a data-plane anti-join), and
-- it is also how a reference's genome map is derived -- there is no reference->genome
-- edge in the schema, so "this reference's genomes" means "genomes of features this
-- reference contains". Its only writer today is a reference load. A per-contig,
-- machine-minted producer writing there would put sample-derived genomes inside every
-- reference map that shares a contig, and inside the curation blocklist's reach.
--
-- A column rather than a second junction, because a bare (feature_idx, genome_idx)
-- table cannot express the per-RUN scoping its consumers need: qiita.genome carries no
-- processing_idx, and prep_sample_idx is identical for two runs of one prep_sample, so
-- there is nothing to filter the genome side on. This table's PRIMARY KEY carries the
-- run, so genome_idx sits on the row that scopes it -- at the cost of a denormalized
-- column, since nothing constrains genome_idx to agree with the
-- (prep_sample_idx, processing_idx, kind, bin_id) it is derived from.
--
-- NULLABLE, and it stays that way: the sequenced-pool delete NULLs this column before
-- deleting the genome, which is what lets the prep_sample delete proceed at all. (That
-- teardown runs on any pool delete; `force` decides only whether terminal tickets,
-- published links and ENA accessions block it.) A bare FK -- NO ACTION, per the
-- schema-wide no-cascade convention -- is what makes that detach required rather than
-- optional.
ALTER TABLE qiita.assembly_membership
    ADD COLUMN genome_idx BIGINT REFERENCES qiita.genome (genome_idx);

-- Reverse lookup (which contigs a genome holds) plus FK delete-check support,
-- mirroring qiita.feature_genome (genome_idx). Dropped with the column on the way down.
CREATE INDEX ON qiita.assembly_membership (genome_idx);

COMMENT ON COLUMN qiita.assembly_membership.genome_idx IS
    'The qiita.genome this subject was minted as: one genome per '
    '(prep_sample_idx, processing_idx, kind, bin_id), so a refined bin''s contigs '
    'share one genome_idx while an LCG or unbinned contig is its own. source is '
    '''qiita'' with prep_sample_idx set; source_id is the SHA-256 of that same tuple. '
    'Minting a genome here asserts NOTHING about completeness, for any kind -- see '
    'this table''s own comment, which states that once. NULL means not yet minted: a '
    'run predating the backfill that introduced this column. A reader rolling contigs '
    'up to genomes must exclude NULLs explicitly; a roll-up that inner-joins on this '
    'column drops that contig''s rows instead of erroring.';

-- migrate:down

-- The FK and the index on genome_idx are removed with the column.
ALTER TABLE qiita.assembly_membership DROP COLUMN IF EXISTS genome_idx;
