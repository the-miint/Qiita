### Changed

- **A run-list ENA import downloads only the runs it lists (#863).** Its pool's
  `download-ena-study` ticket now carries an optional `ena_run_accessions` and stages only those
  runs, where it used to download every unread run in the pool. A manually submitted ticket can
  carry it too. A whole-study import fails the item, naming the ticket, when another import's
  restricted ticket still holds a pool it needs; re-importing once that ticket ends tickets the
  rest. An item with no run left to download (each stored or failed, with another active run still in the pool) now
  fails with "nothing to download" instead of "no pool holds an active sequenced_sample". A run
  whose read numbering a failed or unfinished ticket reserved still counts as needing download. A
  restricted ticket skips, with a warning, listed runs that left the active roster, and fails only
  when none remain.
- **A re-import tickets the runs a completed download left behind (#863).** A held run with no
  reads whose pool's download completed without it gets a new ticket for the pool, restricted to
  such runs and forced past the completed-pool gate, instead of being reported and left.

### Removed

- **The `held_not_downloaded` run status (#863).** The re-import that used to report it now
  tickets the run.
