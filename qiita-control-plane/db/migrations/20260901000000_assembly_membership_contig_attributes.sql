-- migrate:up

-- The assembler's own per-contig report, carried through to the row that already
-- names the contig. All four are nullable and all four are NULL for every row
-- written before this migration: the values are read out of the assemble step's
-- output, so a run that predates the attribute sidecar cannot be backfilled from
-- anything the lake holds. NULL therefore means "not recorded", never "measured
-- as absent" — the same convention assembly_membership.genome_idx uses.
--
-- These are stored so routing can become a query-time predicate rather than a
-- decision baked into the entrypoint. Today circularity decides, inside
-- assemble.sh, whether a contig bypasses binning as an LCG; the two assemblers
-- disagree on the same molecule (one sample's identical 27 kb sequence is
-- `circular-yes` to hifiasm_meta and `circular-possibly` to myloasm), so the call
-- is a property of the assembler as much as of the contig. Stored, it can be
-- re-asked later without re-assembling.
ALTER TABLE qiita.assembly_membership
    ADD COLUMN raw_name    TEXT,
    ADD COLUMN circularity TEXT,
    ADD COLUMN depth       DOUBLE PRECISION,
    ADD COLUMN mult        DOUBLE PRECISION;

COMMENT ON COLUMN qiita.assembly_membership.raw_name IS
    'The assembler''s own record name, verbatim: myloasm''s full FASTA header, or '
    'hifiasm_meta''s GFA segment name. Kept so the normalized circularity column can always '
    'be traced back to what the tool actually said. NULL for rows written before '
    'the attribute sidecar existed.';

COMMENT ON COLUMN qiita.assembly_membership.circularity IS
    'Normalized circularity call: ''yes'', ''possibly'', or ''no''. myloasm states '
    'it in the header; hifiasm_meta encodes it in the segment name and has no '
    '''possibly'', so that value is myloasm-only. Deliberately TEXT, not an ENUM: '
    'the set is the producer''s, and a new assembler with a fourth call must be '
    'able to store it rather than fail the load.';

COMMENT ON COLUMN qiita.assembly_membership.depth IS
    'Per-contig read depth as the assembler reported it. myloasm: the mean of its '
    'header''s depth triple, which is the scalar myloasm itself derives from it '
    '(the avg_cov its circularity gate tests). hifiasm_meta: the GFA S-line''s dp:f '
    'tag. The two compute coverage differently, so compare across assemblers only '
    'with raw_name in hand.';

COMMENT ON COLUMN qiita.assembly_membership.mult IS
    'myloasm''s k-mer multiplicity. NULL for hifiasm_meta, which has no counterpart, '
    'and NULL below 1 kb, where myloasm reports 0.00 for absence of signal rather '
    'than a measured zero. Stored as a float rather than myloasm''s duplicated-* '
    'bucketing, whose 1.10/1.50 breaks cannot express advice given at 1.05.';

-- migrate:down

ALTER TABLE qiita.assembly_membership
    DROP COLUMN raw_name,
    DROP COLUMN circularity,
    DROP COLUMN depth,
    DROP COLUMN mult;
