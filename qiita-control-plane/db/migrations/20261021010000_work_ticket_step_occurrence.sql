-- Identity of a work_ticket_step row: (step_name, step_occurrence).
--
-- step_index is the entry's position in the action's `steps:` list, and that
-- list is edited in place and re-synced without a version bump, so a position
-- recorded before an edit can name a different entry after it. step_name alone
-- does not identify an entry either: `action:` entries may repeat a name (two
-- `register-index` actions in one workflow). step_occurrence is the 0-based
-- count of earlier entries in `steps:` carrying the same name, so
-- (step_name, step_occurrence) names one entry, and survives inserting,
-- removing or reordering entries with other names. The runner re-points each
-- row's step_index at the entry its identity names before it reads the rows
-- (`step_progress.align_step_progress`).

-- migrate:up
ALTER TABLE qiita.work_ticket_step
  ADD COLUMN step_occurrence INT CHECK (step_occurrence >= 0);

-- Backfill, per ticket. Where the ticket's synced action still names step_name
-- at every row's step_index, the occurrence is read off that definition: the
-- count of earlier entries with the same name. That is exact even when an
-- earlier same-name entry was skipped by its `when:` gate and has no row.
UPDATE qiita.work_ticket_step s
   SET step_occurrence = (
         SELECT count(*)
           FROM jsonb_array_elements(a.steps) WITH ORDINALITY AS e(entry, ord)
          WHERE e.ord - 1 < s.step_index
            AND e.entry ->> 'name' = s.step_name)
  FROM qiita.work_ticket t
  JOIN qiita.action a
    ON a.action_id = t.action_id AND a.version = t.action_version
 WHERE t.work_ticket_idx = s.work_ticket_idx
   AND NOT EXISTS (
         SELECT 1
           FROM qiita.work_ticket_step o
          WHERE o.work_ticket_idx = s.work_ticket_idx
            AND (a.steps -> o.step_index ->> 'name') IS DISTINCT FROM o.step_name);

-- A ticket whose rows the current definition no longer matches was written
-- under an earlier step list. Its occurrences are ranked from its own rows: the
-- n-th distinct step_index carrying a name is occurrence n - 1. Applied to the
-- whole ticket, never mixed with the exact form above, so the unique index
-- below holds.
UPDATE qiita.work_ticket_step s
   SET step_occurrence = r.occurrence
  FROM (SELECT work_ticket_idx, step_index, attempt,
               dense_rank() OVER (
                 PARTITION BY work_ticket_idx, step_name ORDER BY step_index
               ) - 1 AS occurrence
          FROM qiita.work_ticket_step) r
 WHERE s.step_occurrence IS NULL
   AND r.work_ticket_idx = s.work_ticket_idx
   AND r.step_index = s.step_index
   AND r.attempt = s.attempt;

ALTER TABLE qiita.work_ticket_step
  ALTER COLUMN step_occurrence SET NOT NULL;

CREATE UNIQUE INDEX work_ticket_step_identity_attempt
  ON qiita.work_ticket_step (work_ticket_idx, step_name, step_occurrence, attempt);

COMMENT ON COLUMN qiita.work_ticket_step.step_occurrence IS
  'How many earlier entries of the action''s steps: list carry the same name '
  '(0-based). (step_name, step_occurrence) identifies the entry across in-place '
  'edits of the list; step_index is its position in the list as last synced.';

-- migrate:down
DROP INDEX qiita.work_ticket_step_identity_attempt;
ALTER TABLE qiita.work_ticket_step
  DROP COLUMN step_occurrence;
