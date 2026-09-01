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
    'The assembler''s own record name, verbatim: for myloasm the FASTA header''s '
    'first token (NOT the whole line -- it writes mult= after a space, which the '
    'reader returns as a separate field), for hifiasm_meta the GFA segment name. '
    'Kept so the normalized circularity column can always be traced back to what '
    'the tool actually said. NULL for rows written before the attribute sidecar '
    'existed.';

COMMENT ON COLUMN qiita.assembly_membership.circularity IS
    'Normalized circularity call: ''yes'', ''possibly'', or ''no''. myloasm states '
    'it in the header; hifiasm_meta encodes it in the segment name and has no '
    '''possibly'', so that value is myloasm-only. Deliberately TEXT, not an ENUM: '
    'the set is the producer''s, so adding an assembler that reports a fourth '
    'call is an entrypoint change rather than a migration. Both entrypoints '
    'currently REJECT a value outside the three rather than store it.';

COMMENT ON COLUMN qiita.assembly_membership.depth IS
    'Per-contig read depth as the assembler reported it. myloasm: the mean of its '
    'header''s depth triple; per myloasm''s source that triple is min_read_depth_multi '
    'and the same function averages it into the avg_cov its circularity gate tests, '
    'which is read off the source rather than probed. hifiasm_meta: the GFA '
    'S-line''s dp:f tag, probed present on every segment of the pinned build. The '
    'two compute coverage differently, so compare across assemblers only with '
    'raw_name in hand; which assembler ran is on qiita.processing via '
    'processing_idx.';

COMMENT ON COLUMN qiita.assembly_membership.mult IS
    'myloasm''s k-mer multiplicity. NULL for hifiasm_meta, which has no counterpart, '
    'and NULL below 1 kb, where myloasm reports 0.00 for absence of signal rather '
    'than a measured zero -- the threshold is read off myloasm''s source, not '
    'probed. Stored as a float rather than myloasm''s duplicated-* bucketing, '
    'whose thresholds cannot express a value between them.';

-- migrate:down

ALTER TABLE qiita.assembly_membership
    DROP COLUMN raw_name,
    DROP COLUMN circularity,
    DROP COLUMN depth,
    DROP COLUMN mult;
