-- migrate:up

-- Comment only; no schema change.

COMMENT ON TABLE qiita.assembly_membership IS
    'Junction: which deduped contig features a prep_sample''s assembly RUN '
    '(processing_idx) produced, and in which bin — the assembly analogue of '
    'qiita.reference_membership. The key prefix (prep_sample_idx, processing_idx, '
    'kind, bin_id) is the subject identity — one circular genome, one refined bin, '
    'or one unbinned contig — with feature_idx completing the row per member '
    'contig. kind is what tells the three apart; its TEXT value set is enumerated '
    'in qiita_common.assembly_constants. processing_idx disambiguates re-runs so a '
    'reused bin_id never collides. A subject records grouping and nothing about '
    'completeness, for any kind; the bin_quality lake table measures that, per '
    'refined bin, from CheckM.';

COMMENT ON COLUMN qiita.assembly_membership.bin_id IS
    'Names the subject within its (prep_sample, processing, kind), and what it '
    'holds depends on kind: for a refined bin it is the bin FASTA''s filename '
    'stem, and for a circular or unbinned contig — each of which is its own '
    'subject — it is that contig''s own id from the assembler. Producer-chosen '
    'either way (''bin.1'' recurs across samples and runs), which is why the key '
    'scopes it rather than treating it as globally unique.';


-- migrate:down

COMMENT ON COLUMN qiita.assembly_membership.bin_id IS NULL;

COMMENT ON TABLE qiita.assembly_membership IS
    'Junction: which deduped contig features a prep_sample''s assembly RUN '
    '(processing_idx) produced, and in which bin. One row per (prep_sample_idx, '
    'processing_idx, kind, bin_id, feature_idx) — the assembly analogue of '
    'qiita.reference_membership. processing_idx disambiguates re-runs so a reused '
    'bin_id never collides; kind is a TEXT set enumerated in '
    'qiita_common.assembly_constants.';
