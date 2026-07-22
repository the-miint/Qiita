-- migrate:up

-- The source accession (FASTA-header read_id) each reference used to name this
-- feature. feature_idx is content-hash-global (identical bytes share one
-- feature_idx across references), so the human-facing identifier is a property
-- of the (reference, feature) membership, not of the feature itself: the same
-- bytes can carry a different accession in a different reference. Nullable —
-- pre-existing rows predate the column and are not backfilled, and non-FASTA
-- ingest paths may not carry a header. Populated at load by
-- qiita_control_plane.actions.library.write_membership (representative =
-- lex-smallest read_id when identical bytes repeat under several headers).

ALTER TABLE qiita.reference_membership ADD COLUMN accession TEXT;

-- migrate:down

ALTER TABLE qiita.reference_membership DROP COLUMN accession;
