-- migrate:up

-- Comment only; no schema change. The table comment points at the module that
-- enumerates the `kind` value set rather than listing its members.

COMMENT ON TABLE qiita.assembly_membership IS
    'Junction: which deduped contig features a prep_sample''s assembly RUN '
    '(processing_idx) produced, and in which bin. One row per (prep_sample_idx, '
    'processing_idx, kind, bin_id, feature_idx) — the assembly analogue of '
    'qiita.reference_membership. processing_idx disambiguates re-runs so a reused '
    'bin_id never collides; kind is a TEXT set enumerated in '
    'qiita_common.assembly_constants.';


-- migrate:down

COMMENT ON TABLE qiita.assembly_membership IS
    'Junction: which deduped contig features a prep_sample''s assembly RUN '
    '(processing_idx) produced, and in which bin. One row per (prep_sample_idx, '
    'processing_idx, kind, bin_id, feature_idx) — the assembly analogue of '
    'qiita.reference_membership. processing_idx disambiguates re-runs so a reused '
    'bin_id never collides; kind is a producer-owned TEXT set (''LCG'' = circular '
    'genome, ''MAG'' = refined bin).';
