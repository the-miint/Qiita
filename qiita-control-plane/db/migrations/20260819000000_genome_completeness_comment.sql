-- migrate:up

-- Comment only; no schema change. qiita.genome has carried no comment, and the
-- long-read-assembly tail now writes source='qiita' rows for three kinds of
-- subject, one of which is a single unbinned contig. Record what a row does and
-- does not assert, at the table the rows land in.

COMMENT ON TABLE qiita.genome IS
    'A genome-level subject features are grouped under (qiita.feature_genome). A '
    'row says its features were grouped as one subject by whatever produced it. It '
    'asserts NOTHING about completeness, at any source: an external MAG carries no '
    'completeness guarantee, and neither does a qiita-assembled circular contig, '
    'refined bin, or single unbinned contig (qiita.assembly_membership.kind names '
    'which). Completeness is measured — the bin_quality lake table, per refined bin '
    'from CheckM — not implied by the existence of a genome_idx. source = ''qiita'' '
    'additionally requires prep_sample_idx (genome_qiita_origin_check); those rows '
    'are minted by the assembly_hash job, whose _genome_source_id defines source_id.';


-- migrate:down

COMMENT ON TABLE qiita.genome IS NULL;
