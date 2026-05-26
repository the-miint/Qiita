-- migrate:up

-- Seed three published checklists from the ENA / GSC MIxS catalog.
-- ERC000011 is the ENA default sample checklist (root); ERC000014
-- (GSC MIxS human associated) extends it; ERC000015 (GSC MIxS human
-- gut) extends ERC000014. The parent_metadata_checklist_idx linkage
-- is recorded so a sample claiming conformance to a child implicitly
-- inherits the field set of its ancestors at query time.

INSERT INTO qiita.metadata_checklist (name, description, parent_metadata_checklist_idx)
VALUES ('ERC000011', 'ENA default sample checklist', NULL)
ON CONFLICT DO NOTHING;

INSERT INTO qiita.metadata_checklist (name, description, parent_metadata_checklist_idx)
SELECT 'ERC000014', 'GSC MIxS human associated', idx
  FROM qiita.metadata_checklist
 WHERE name = 'ERC000011'
ON CONFLICT DO NOTHING;

INSERT INTO qiita.metadata_checklist (name, description, parent_metadata_checklist_idx)
SELECT 'ERC000015', 'GSC MIxS human gut', idx
  FROM qiita.metadata_checklist
 WHERE name = 'ERC000014'
ON CONFLICT DO NOTHING;


-- migrate:down

DELETE FROM qiita.metadata_checklist
 WHERE name IN ('ERC000015', 'ERC000014', 'ERC000011');
