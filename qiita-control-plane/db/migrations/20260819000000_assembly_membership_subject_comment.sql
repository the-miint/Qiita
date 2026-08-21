-- migrate:up

-- Comment only; no schema change.

COMMENT ON TABLE qiita.assembly_membership IS
    'Junction: which deduped contig features a prep_sample''s assembly RUN '
    '(processing_idx) produced, and in which bin — the assembly analogue of '
    'qiita.reference_membership. The key prefix (prep_sample_idx, processing_idx, '
    'kind, bin_id) is the subject identity, with feature_idx completing the row per '
    'member contig; how far that identity separates two subjects depends on what '
    'bin_id holds, which the bin_id column comment states. kind tells a refined bin '
    'from a circular or an unbinned contig; its TEXT value set is enumerated '
    'in qiita_common.assembly_constants. processing_idx disambiguates re-runs so a '
    'reused bin_id never collides. A subject records grouping and nothing about '
    'completeness, for any kind; the bin_quality lake table measures that, per '
    'refined bin, from CheckM.';

COMMENT ON COLUMN qiita.assembly_membership.bin_id IS
    'Names the subject within its (prep_sample, processing, kind), and what it '
    'holds depends on kind: for a refined bin it is the bin FASTA''s filename '
    'stem — one file, one bin — and for a circular or unbinned contig it is that '
    'contig''s own id from the assembler, its FASTA header''s first token. '
    'Producer-chosen either way (''bin.1'' recurs across samples and runs, and two '
    'headers in one file can share a first token), which is why the key scopes it '
    'rather than treating it as globally unique or as one contig per row.';


-- migrate:down

COMMENT ON COLUMN qiita.assembly_membership.bin_id IS NULL;

COMMENT ON TABLE qiita.assembly_membership IS
    'Junction: which deduped contig features a prep_sample''s assembly RUN '
    '(processing_idx) produced, and in which bin. One row per (prep_sample_idx, '
    'processing_idx, kind, bin_id, feature_idx) — the assembly analogue of '
    'qiita.reference_membership. processing_idx disambiguates re-runs so a reused '
    'bin_id never collides; kind is a TEXT set enumerated in '
    'qiita_common.assembly_constants.';
