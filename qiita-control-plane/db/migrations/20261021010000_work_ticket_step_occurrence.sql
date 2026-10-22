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
  ADD COLUMN step_occurrence INT;

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
-- under an earlier step list, which is not kept. A name with at most one entry
-- in the current list and rows at one step_index is occurrence 0. Any other
-- name — repeated in the current list, or recorded at more than one step_index —
-- cannot be matched to an entry from the rows alone (an earlier same-name entry
-- may have been skipped by its `when:` gate, or removed since), so its rows get a
-- negative occurrence, -n for the n-th distinct step_index carrying the name. No
-- entry has a negative occurrence, so the runner fails such a ticket at resume
-- naming the rows rather than fast-forwarding a guess
-- (`step_progress.align_step_progress`). Applied to the whole ticket, never mixed
-- with the exact form above, so the unique index below holds.
UPDATE qiita.work_ticket_step s
   SET step_occurrence = CASE WHEN r.ambiguous THEN -r.rank ELSE 0 END
  FROM (SELECT x.work_ticket_idx, x.step_index, x.attempt, x.rank,
               max(x.rank) OVER (PARTITION BY x.work_ticket_idx, x.step_name) > 1
                 OR (SELECT count(*)
                       FROM qiita.work_ticket t
                       JOIN qiita.action a
                         ON a.action_id = t.action_id AND a.version = t.action_version
                      CROSS JOIN LATERAL jsonb_array_elements(a.steps) AS e(entry)
                      WHERE t.work_ticket_idx = x.work_ticket_idx
                        AND e.entry ->> 'name' = x.step_name) > 1 AS ambiguous
          FROM (SELECT work_ticket_idx, step_name, step_index, attempt,
                       dense_rank() OVER (
                         PARTITION BY work_ticket_idx, step_name ORDER BY step_index
                       ) AS rank
                  FROM qiita.work_ticket_step) x) r
 WHERE s.step_occurrence IS NULL
   AND r.work_ticket_idx = s.work_ticket_idx
   AND r.step_index = s.step_index
   AND r.attempt = s.attempt;

ALTER TABLE qiita.work_ticket_step
  ALTER COLUMN step_occurrence SET NOT NULL;

-- `make migrate` runs before the control plane restarts, and a control plane
-- built before this migration writes its write-ahead row with an INSERT that
-- names no step_occurrence. For an INSERT that leaves it NULL, this trigger
-- counts it off the ticket's synced action, the same count the exact backfill
-- above takes; without a match it is 0 and the NOT NULL holds. An INSERT that
-- sets the column is left as it is. On a ticket the backfill could not match,
-- the rows the previous control plane adds are counted off the current list too;
-- its negative rows still fail the ticket at resume.
CREATE FUNCTION qiita.work_ticket_step_fill_occurrence() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.step_occurrence IS NULL THEN
    SELECT count(*)
      INTO NEW.step_occurrence
      FROM qiita.work_ticket t
      JOIN qiita.action a
        ON a.action_id = t.action_id AND a.version = t.action_version
      CROSS JOIN LATERAL jsonb_array_elements(a.steps) WITH ORDINALITY AS e(entry, ord)
     WHERE t.work_ticket_idx = NEW.work_ticket_idx
       AND e.ord - 1 < NEW.step_index
       AND e.entry ->> 'name' = NEW.step_name;
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER work_ticket_step_fill_occurrence
  BEFORE INSERT ON qiita.work_ticket_step
  FOR EACH ROW EXECUTE FUNCTION qiita.work_ticket_step_fill_occurrence();

CREATE UNIQUE INDEX work_ticket_step_identity_attempt
  ON qiita.work_ticket_step (work_ticket_idx, step_name, step_occurrence, attempt);

COMMENT ON COLUMN qiita.work_ticket_step.step_occurrence IS
  'How many earlier entries of the action''s steps: list carry the same name '
  '(0-based). (step_name, step_occurrence) identifies the entry across in-place '
  'edits of the list; step_index is its position in the list as last synced. '
  'Negative on a row the backfill could not match to an entry.';

-- migrate:down
DROP TRIGGER work_ticket_step_fill_occurrence ON qiita.work_ticket_step;
DROP FUNCTION qiita.work_ticket_step_fill_occurrence();
DROP INDEX qiita.work_ticket_step_identity_attempt;
ALTER TABLE qiita.work_ticket_step
  DROP COLUMN step_occurrence;
