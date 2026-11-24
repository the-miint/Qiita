"""A biosample's public name as one SQL expression.

The BioSample accession, else the ENA sample accession, else the QB Qiita minted
when the biosample was created (`qiita.exported_entity`). Kept in one place so a
caller naming a sample does not re-spell the precedence; it needs the biosample
aliased `b` and `BIOSAMPLE_NAME_JOIN` in the FROM clause.
"""

BIOSAMPLE_NAME_SQL = "COALESCE(b.biosample_accession, b.ena_sample_accession, ee.export_entity_id)"
BIOSAMPLE_NAME_JOIN = "LEFT JOIN qiita.exported_entity ee ON ee.biosample_idx = b.idx"
