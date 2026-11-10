"""A biosample's public name, as everything that shows a sample names it.

The BioSample accession, else the ENA sample accession, else the QB Qiita minted
when the biosample was created (`qiita.exported_entity`). One SQL expression so
the amplicon feature table and anything else that names a sample agree; it needs
the biosample aliased `b` and `BIOSAMPLE_NAME_JOIN` in the FROM clause.
"""

BIOSAMPLE_NAME_SQL = "COALESCE(b.biosample_accession, b.ena_sample_accession, ee.export_entity_id)"
BIOSAMPLE_NAME_JOIN = "LEFT JOIN qiita.exported_entity ee ON ee.biosample_idx = b.idx"
