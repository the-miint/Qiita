-- migrate:up

-- A host reference is used as a NEGATIVE filter (reads matching it are
-- removed) rather than a positive classification target. This is orthogonal
-- to `kind` (a host reference is still a 'sequence_reference'), so it lands as
-- its own boolean column. Mirrors qiita_common.models.ReferenceCreateRequest /
-- ReferenceResponse.is_host. Default false so existing rows stay regular.
ALTER TABLE qiita.reference
    ADD COLUMN is_host BOOLEAN NOT NULL DEFAULT false;


-- migrate:down

ALTER TABLE qiita.reference
    DROP COLUMN is_host;
