-- Per-interval annotations on a reference: which features are ANNOTATED INTERVALS of
-- another feature (a SynDNA insert on its plasmid, a gene on a chromosome) rather
-- than whole sequences, and what those intervals MEAN.
--
-- Three tables, because an annotation has three separable identities:
--
--   reference_annotation  the OCCURRENCE — this interval, at these coordinates, on
--                         this parent, claimed by this reference.
--   annotation_term       the SEMANTICS — "16S rRNA / RF00177", one row per
--                         (system, system_id), shared across every occurrence and
--                         every reference that observes it.
--   annotation_to_term    the many-to-many between them.
--
-- Why the occurrence is keyed by a MINTED annotation_idx and not by anything in the
-- GFF3:
--
-- The GFF3 `ID` attribute is NOT a unique key, by spec. A feature with multiple
-- locations (a ribosomal-slippage CDS) is ONE feature spread over N lines that all
-- share one ID. NCBI's RefSeq annotation of E. coli K-12 MG1655 — about as standard
-- as a bacterial annotation gets — carries 20 such repeated IDs (`cds-gnl|b4492|CDS1491`
-- appears 3x). So `ID` is provenance, not identity: it is stored, it is not unique,
-- and nothing keys on it. Identity is a minted BIGINT, and the row's NATURAL key
-- (parent + window + type + strand) is what makes a re-ingest idempotent.
--
-- Why the semantics are a separate table, and many-to-many rather than one FK:
--
-- The same gene is observed over and over — one E. coli genome annotates 16S rRNA 14
-- times and tRNA-Leu 16 times — at different coordinates, with sequences that are
-- NOT byte-identical. Those occurrences share a meaning, not a feature_idx, so the
-- meaning has to live somewhere that a per-occurrence column cannot reach.
--
-- It is many-to-many because one interval carries MANY cross-references: in the same
-- RefSeq file, 4816 features carry 3 xrefs and 4161 carry 5, spanning six systems
-- (ASAP, ECOCYC, GenBank, GeneID, UniProtKB/Swiss-Prot, taxon). A single
-- annotation → term FK could hold exactly one of them and would silently drop the
-- rest.
--
-- Why annotated features are NOT in reference_membership:
--
-- Membership is what gets INDEXED and aligned against, and reads align to the parent
-- plasmid/chromosome, never to the bare insert — a membership row would put inserts
-- into the aligner index and shard planning, competing with their own parent for
-- alignments. They also get no reference_sequences/_chunks row: the bytes are
-- recoverable from parent + interval, and a second copy could only drift.
--
-- That exclusion is exactly why this Postgres side has to exist at all rather than
-- the DuckLake twin being enough: `delete_reference_cascade` computes orphan features
-- from the reference's CLAIM tables, so a feature claimed by neither membership nor
-- this table would survive `DELETE /reference/{idx}` forever, referenced by nothing,
-- while the data plane deleted its lake rows — the two stores disagreeing about which
-- features exist. Postgres holds the CLAIM, the lake holds the per-interval DATA, the
-- same split reference_membership already uses.

-- migrate:up
CREATE TABLE qiita.reference_annotation (
    annotation_idx     BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    reference_idx      BIGINT NOT NULL REFERENCES qiita.reference (reference_idx),
    -- The interval's own feature_idx, minted from the canonical hash of the
    -- EXTRACTED sub-sequence. Two occurrences with byte-identical bases legitimately
    -- SHARE one feature_idx (a bacterial 16S occurs in 5-7 identical copies) — a
    -- feature is a SEQUENCE, an annotation is an OCCURRENCE of it at a place, and a
    -- consumer aggregating coverage over the feature sums across its occurrences.
    feature_idx        BIGINT NOT NULL REFERENCES qiita.feature (feature_idx),
    -- The sequence the interval sits on, and what reads actually align to.
    parent_feature_idx BIGINT NOT NULL REFERENCES qiita.feature (feature_idx),
    -- GFF3 `ID`. PROVENANCE ONLY — deliberately nullable and deliberately NOT
    -- unique; see this migration's header for why the spec forbids relying on it.
    annotation_id      TEXT,
    annotation_type    TEXT   NOT NULL,
    -- 1-based HALF-OPEN [position, stop_position), matching alignment_slice /
    -- read_alignments / qiita_lake.alignment. GFF3 arrives 1-based CLOSED; the
    -- conversion happens ONCE, at ingest, in hash_sequences. Both conventions spell
    -- the column `stop_position`, so mixing them raises nothing and merely stops
    -- counting the interval's last base.
    position           BIGINT NOT NULL,
    stop_position      BIGINT NOT NULL,
    -- GFF3 col 7. NOT NULL: the spec makes the column mandatory ('+', '-', '.', '?')
    -- and ingest coalesces a missing one to '.', which keeps the natural key below
    -- free of NULLs and therefore free of NULLS-DISTINCT surprises.
    strand             TEXT   NOT NULL,
    -- GFF3 cols 6 and 8. Both genuinely optional: `score` is NULL on 100% of rows in
    -- both RefSeq and prokka output, and `phase` is populated only on CDS rows.
    -- Stored anyway — they are cheap, and a caller that wants them has no other way
    -- to get them back.
    score              DOUBLE PRECISION,
    phase              SMALLINT,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- An interval that is its own parent spans the whole sequence, so it is not a
    -- sub-interval at all: it hashes to the PARENT's feature_idx, and that feature IS
    -- in reference_membership and IS indexed. At ingest, hash_sequences DROPS the rows
    -- that legitimately do this (GFF3 landmark types — `region` & co., one per NCBI
    -- record) and RAISES on any other type that does; this constraint is the backstop
    -- that makes the state unrepresentable regardless.
    CONSTRAINT reference_annotation_not_self CHECK (feature_idx <> parent_feature_idx),
    CONSTRAINT reference_annotation_window CHECK (position >= 1 AND stop_position > position),
    -- The closed sets GFF3 defines for columns 7 and 8. TEXT + CHECK rather than a
    -- Postgres ENUM, deliberately: these are format constants, not a qiita vocabulary
    -- with a Python twin to keep in parity.
    CONSTRAINT reference_annotation_strand CHECK (strand IN ('+', '-', '.', '?')),
    CONSTRAINT reference_annotation_phase CHECK (phase IS NULL OR phase IN (0, 1, 2)),
    -- The NATURAL key: what makes an interval THE SAME interval on re-ingest. This is
    -- what the minted annotation_idx is stable against, and what lets a re-run of a
    -- reference-add upsert rather than duplicate. Deliberately not the GFF3 ID.
    CONSTRAINT reference_annotation_natural_key UNIQUE
        (reference_idx, parent_feature_idx, position, stop_position, annotation_type, strand)
);

CREATE INDEX ON qiita.reference_annotation (reference_idx);
CREATE INDEX ON qiita.reference_annotation (feature_idx);
CREATE INDEX ON qiita.reference_annotation (parent_feature_idx);

COMMENT ON TABLE qiita.reference_annotation IS
    'One row per annotated INTERVAL of a feature. Holds the reference''s CLAIM (so '
    'delete_reference_cascade can GC the feature); the per-interval detail lives in the '
    'DuckLake twin, keyed by annotation_idx. Keyed by a minted annotation_idx, NOT by the '
    'GFF3 ID (which the spec permits to repeat across lines of a discontinuous feature) '
    'and NOT by feature_idx (identical bases share one). See the migration header.';

-- The SEMANTIC identity of an annotation: what a gene IS, independent of where it
-- occurs or which reference collection observed it. One row per (system, system_id),
-- global and shared — deduplicated across every reference, exactly like qiita.feature.
CREATE TABLE qiita.annotation_term (
    annotation_term_idx BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    -- The annotation authority: 'RFAM', 'PFAM', 'KEGG', 'GeneID', 'ECOCYC',
    -- 'UniProtKB/Swiss-Prot' ... In a GFF3 these are the prefixes of the Dbxref
    -- attribute's comma-separated entries.
    system     TEXT NOT NULL,
    -- The authority's accession: 'RF00177', 'PF00177', '944742'.
    system_id  TEXT NOT NULL,
    -- Human-readable meaning ('16S ribosomal RNA'). NULLABLE, and that is not
    -- laziness: a GFF3 carries `product` on only about half its rows (RefSeq genes
    -- have none — only their CDS children do), so NOT NULL would reject half of a
    -- standard annotation file.
    definition TEXT,
    -- The annotation DATABASE's version. NULLABLE because GFF3 has nowhere to put it:
    -- the nearest thing is col 2 (`source`), which carries the annotating TOOL's
    -- version in prokka ('Prodigal:2.60') and a bare 'RefSeq' in NCBI's. A caller who
    -- knows the db version out of band can set it; ingest cannot invent one.
    version    TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT annotation_term_identity UNIQUE (system, system_id)
);

COMMENT ON TABLE qiita.annotation_term IS
    'The SEMANTICS of an annotation (16S rRNA / RF00177), one row per (system, system_id), '
    'shared across every occurrence and every reference. Separate from reference_annotation '
    'because one genome annotates the same gene many times over, at different coordinates, '
    'with sequences that are NOT byte-identical — they share a meaning, not a feature_idx.';

-- Many-to-many, not a single FK on reference_annotation: one interval routinely
-- carries several cross-references at once (in NCBI's E. coli RefSeq, 4816 features
-- carry 3 and 4161 carry 5, across six systems). A lone annotation_term_idx column
-- could hold one of them and would silently drop the others.
CREATE TABLE qiita.annotation_to_term (
    annotation_idx      BIGINT NOT NULL
        REFERENCES qiita.reference_annotation (annotation_idx),
    annotation_term_idx BIGINT NOT NULL
        REFERENCES qiita.annotation_term (annotation_term_idx),
    PRIMARY KEY (annotation_idx, annotation_term_idx)
);

CREATE INDEX ON qiita.annotation_to_term (annotation_term_idx);

COMMENT ON TABLE qiita.annotation_to_term IS
    'Many-to-many between an annotation OCCURRENCE and its SEMANTIC terms. One GFF3 '
    'feature routinely carries several Dbxref entries across different systems.';

-- migrate:down
DROP TABLE qiita.annotation_to_term;
DROP TABLE qiita.annotation_term;
DROP TABLE qiita.reference_annotation;
