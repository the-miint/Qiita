-- migrate:up

-- Seed the GSC MIxS host associated checklist from the ENA / GSC MIxS
-- catalog and slot it into the inheritance chain. ERC000013 (GSC MIxS
-- host associated) extends ERC000011, the ENA default sample checklist
-- (root), and ERC000014 (GSC MIxS human associated) now extends
-- ERC000013 rather than ERC000011 directly.

INSERT INTO qiita.metadata_checklist (name, description, parent_metadata_checklist_idx)
SELECT 'ERC000013', 'GSC MIxS host associated', idx
  FROM qiita.metadata_checklist
 WHERE name = 'ERC000011'
ON CONFLICT DO NOTHING;

UPDATE qiita.metadata_checklist
   SET parent_metadata_checklist_idx = (
         SELECT idx FROM qiita.metadata_checklist WHERE name = 'ERC000013')
 WHERE name = 'ERC000014';


-- migrate:down

UPDATE qiita.metadata_checklist
   SET parent_metadata_checklist_idx = (
         SELECT idx FROM qiita.metadata_checklist WHERE name = 'ERC000011')
 WHERE name = 'ERC000014';

DELETE FROM qiita.metadata_checklist
 WHERE name = 'ERC000013';
