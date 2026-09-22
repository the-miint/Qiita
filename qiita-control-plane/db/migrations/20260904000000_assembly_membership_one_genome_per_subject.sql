-- migrate:up

-- One genome per assembled subject, enforced.
--
-- `genome_idx` is minted per (prep_sample_idx, processing_idx, kind, bin_id) and
-- stamped onto every contig row of that subject, so a refined bin's hundreds of
-- rows must all carry the same value. Nothing constrained that until now: the
-- column's own comment (20260831000000) says the value is denormalized and
-- unchecked against the key it is derived from, and every reader that rolls
-- contigs up to subjects has carried a DISTINCT as a stand-in.
--
-- What a violation costs is not a duplicate row, it is a silent fan-out. A reader
-- joining the subject to per-subject data -- `bin_quality`'s CheckM scores are the
-- first -- gets two rows for one genome and no signal that it happened.
--
-- EXCLUDE rather than UNIQUE: the invariant is "no two rows agreeing on the
-- subject may disagree on the genome", which is a <> comparison and so outside
-- what a unique index can say. `btree_gist` supplies the gist operator classes for
-- the scalar/text = terms.
--
-- The migration installs the extension itself, and this is the one place that
-- decision is argued. Measured on the deploy 2026-09-04: the migration role
-- `qiita_miint_rw` is NOT superuser but DOES hold CREATE on `qiita_miint`, and
-- btree_gist is TRUSTED, which together are sufficient. Probed on PG 17 with a
-- control that failed: non-superuser + CREATE installs it; WITHOUT CREATE it is
-- refused; without CREATE but already installed, IF NOT EXISTS is a no-op. So this
-- line works as written today and stays a no-op if a DBA installs it out of band or
-- the role's grants are tightened later.
--
-- NULLs are unconstrained, which is required rather than incidental: a row that
-- predates the genome mint carries NULL, the sequenced-pool delete NULLs this
-- column before deleting the genome, and either would otherwise be refused.
--
-- DEFERRABLE INITIALLY IMMEDIATE. The default is unchanged -- a violation is still
-- refused by the statement that makes it -- but it is checked PER ROW, so even an
-- UPDATE re-stamping a whole subject in one statement trips on its first row. No
-- production writer needs that, but an operator reconciling a violation does, and
-- `SET CONSTRAINTS ... DEFERRED` inside one transaction is the escape hatch. A
-- transaction that ends still violating is refused at commit; both directions probed.
--
-- Why no production writer needs it, stated PER CHUNK because that is the grain the
-- check runs at: `insert_assembly_membership_rows` writes a subject's contigs across
-- several statements with `DO UPDATE SET genome_idx = EXCLUDED.genome_idx`, so if that
-- value ever changed, an early chunk would trip against a later chunk's not-yet-
-- rewritten rows, mid-load. It cannot change: `assembly_genome_source_id` hashes the
-- same four columns and `upsert_genomes` conflicts on (source, source_id), so the
-- upsert re-resolves to the same genome_idx every time. The other writer moves rows
-- G -> NULL (the sequenced-pool delete), which NULLs never conflict with.
CREATE EXTENSION IF NOT EXISTS btree_gist;

ALTER TABLE qiita.assembly_membership
    ADD CONSTRAINT assembly_membership_one_genome_per_subject
    EXCLUDE USING gist (
        prep_sample_idx WITH =,
        processing_idx  WITH =,
        kind            WITH =,
        bin_id          WITH =,
        genome_idx      WITH <>
    ) DEFERRABLE INITIALLY IMMEDIATE;

COMMENT ON CONSTRAINT assembly_membership_one_genome_per_subject
    ON qiita.assembly_membership IS
    'No two rows of one assembled subject (prep_sample_idx, processing_idx, kind, '
    'bin_id) may carry different genome_idx values. The mint is keyed on that same '
    'tuple, so a violation means a stored genome_idx disagrees with the key it was '
    'derived from -- which shows up downstream as a join fanning out, not as a '
    'duplicate row. NULL genome_idx is unconstrained: it is what a row predating the '
    'mint carries, and what the sequenced-pool delete writes before dropping a genome.';

-- The column comment predates this constraint and says nothing constrains
-- genome_idx against the key it is derived from. Still literally true -- this checks
-- that a subject's rows AGREE, not that they agree with the hash -- but it is what a
-- reader gets from \d+, so it now points at the constraint instead of leaving them
-- believing nothing is checked at all. Re-issued whole; COMMENT ON COLUMN replaces.
COMMENT ON COLUMN qiita.assembly_membership.genome_idx IS
    'The qiita.genome this subject was minted as: one genome per '
    '(prep_sample_idx, processing_idx, kind, bin_id), so a refined bin''s contigs '
    'share one genome_idx while an LCG or unbinned contig is its own. source is '
    '''qiita'' with prep_sample_idx set; source_id is the SHA-256 of that same tuple. '
    'Minting a genome here asserts NOTHING about completeness, for any kind -- see '
    'this table''s own comment, which states that once. NULL means not yet minted: a '
    'run predating the backfill that introduced this column. A reader rolling contigs '
    'up to genomes must exclude NULLs explicitly; a roll-up that inner-joins on this '
    'column drops that contig''s rows instead of erroring. That a subject''s non-NULL '
    'values AGREE is enforced by assembly_membership_one_genome_per_subject; nothing '
    'constrains them against the hash they were derived from.';

-- migrate:down

ALTER TABLE qiita.assembly_membership
    DROP CONSTRAINT IF EXISTS assembly_membership_one_genome_per_subject;
-- btree_gist is deliberately NOT dropped: it is database-wide and another object
-- may have come to depend on it.
