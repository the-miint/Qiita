-- migrate:up

-- =============================================================================
-- Control-type biosample fields: neg_cont_type (terminology) and pos_cont_type (text)
-- =============================================================================
--
-- MIxS NegContTypeEnum (the seven MIxS values) plus 'extraction blank' and
-- 'no-template control', as an internally-governed vocabulary; same shape as the
-- Qiita Sample Type seed, except that term_id is the MIxS spelling, which is what
-- submitters write. pos_cont_type is free text in MIxS, so it has no terminology.

INSERT INTO qiita.terminology (name, version, loaded_at, status)
VALUES ('MIxS NegContTypeEnum', '2026-10-12', NOW(), 'active')
ON CONFLICT DO NOTHING;

INSERT INTO qiita.terminology_term (terminology_idx, term_id, label)
SELECT t.idx, v.term_id, v.label
  FROM qiita.terminology t,
       (VALUES
           ('DNA-free PCR mix', 'DNA-free PCR mix'),
           ('distilled water', 'distilled water'),
           ('empty collection device', 'empty collection device'),
           ('empty collection tube', 'empty collection tube'),
           ('phosphate buffer', 'phosphate buffer'),
           ('sterile swab', 'sterile swab'),
           ('sterile syringe', 'sterile syringe'),
           ('extraction blank', 'extraction blank'),
           ('no-template control', 'no-template control')
       ) AS v(term_id, label)
 WHERE t.name = 'MIxS NegContTypeEnum'
ON CONFLICT DO NOTHING;

-- required = false: only controls carry these. created_by_idx = 1 is the seeded
-- system principal; the conflict target is internal_name as in the sample-type seed.
INSERT INTO qiita.biosample_global_field
    (internal_name, display_name, data_type, required, terminology_idx, created_by_idx)
SELECT 'neg_cont_type', 'negative control type', 'terminology', false, idx, 1
  FROM qiita.terminology
 WHERE name = 'MIxS NegContTypeEnum'
ON CONFLICT (internal_name) DO NOTHING;

INSERT INTO qiita.biosample_global_field
    (internal_name, display_name, data_type, required, terminology_idx, created_by_idx)
VALUES ('pos_cont_type', 'positive control type', 'text', false, NULL, 1)
ON CONFLICT (internal_name) DO NOTHING;


-- migrate:down

-- Unwind fields -> terms -> terminology (every inbound reference is ON DELETE
-- RESTRICT). The rollback aborts if any study field, metadata row or checklist
-- row still links a field; clear those first.

DELETE FROM qiita.biosample_global_field
 WHERE internal_name IN ('neg_cont_type', 'pos_cont_type');

DELETE FROM qiita.terminology_term
 WHERE terminology_idx = (
     SELECT idx FROM qiita.terminology WHERE name = 'MIxS NegContTypeEnum'
 );

DELETE FROM qiita.terminology
 WHERE name = 'MIxS NegContTypeEnum';
