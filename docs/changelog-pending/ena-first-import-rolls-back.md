### Fixed

- **A failed first ENA import no longer leaves an empty study behind (#930).** An item that creates
  its study now registers it in the same transaction, so a registration error, a study whose
  runs map to no pool, or one whose runs all fail rolls the study back and the item fails with
  no `study_idx`. Re-importing creates the study afresh; a rolled-back creation leaves gaps in
  the study idx and `QS` handle numbers. Imports of one accession now take turns, the second
  waiting for the first to commit or roll back. The waiter gives up after 90 s and its item
  fails with a reason naming the accession and saying another import holds it. Studies
  already left empty by an earlier failed import are not removed; the runbook has a query to
  list them.
