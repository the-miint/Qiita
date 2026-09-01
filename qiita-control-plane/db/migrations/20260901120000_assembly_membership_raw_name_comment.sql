-- migrate:up

-- The raw_name comment shipped with a caveat that a MAG row carries the attribute
-- columns only where the binners kept the assembler's contig header, because that
-- passthrough was measured for hifiasm_meta and unmeasured for myloasm. It has
-- since been measured for both id shapes: a run carrying myloasm-shaped ids
-- (u<N>ctg) beside hifiasm-shaped ones as an in-run control returned both verbatim
-- through metabat2, maxbin2, concoct and DAS_Tool, with no record renamed at any
-- stage. The condition is dropped rather than softened; assembly_hash's module
-- docstring carries the measurement and its scope.
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
    'currently REJECT a value outside the three rather than store it. '
    '''possibly'' routes to binning, so where a refined bin claims such a contig '
    'its MAG row is the only surviving record of the call.';

-- migrate:down

COMMENT ON COLUMN qiita.assembly_membership.raw_name IS
    'The assembler''s own record name, verbatim: for myloasm the FASTA header''s '
    'first token (NOT the whole line -- it writes mult= after a space, which the '
    'reader returns as a separate field), for hifiasm_meta the GFA segment name. '
    'Kept so the normalized circularity column can always be traced back to what '
    'the tool actually said. NULL for rows written before the attribute sidecar '
    'existed. A MAG row carries this column, and the three beside it, only where the '
    'binners kept the assembler''s contig header; an LCG or UNBINNED row always does, '
    'since its id comes straight off the published FASTA the values were read from.';

COMMENT ON COLUMN qiita.assembly_membership.circularity IS
    'Normalized circularity call: ''yes'', ''possibly'', or ''no''. myloasm states '
    'it in the header; hifiasm_meta encodes it in the segment name and has no '
    '''possibly'', so that value is myloasm-only. Deliberately TEXT, not an ENUM: '
    'the set is the producer''s, so adding an assembler that reports a fourth '
    'call is an entrypoint change rather than a migration. Both entrypoints '
    'currently REJECT a value outside the three rather than store it. '
    '''possibly'' routes to binning, so where a refined bin claims such a contig '
    'its MAG row is the only surviving record of the call -- and a MAG row carries '
    'these attributes only under the condition stated on raw_name.';
