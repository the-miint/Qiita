### Added

- **`qiita-admin scratch reclaim` frees finished tickets' scratch space (Refs #610).**
  For tickets that are `completed` or `no_data`, or failed/cancelled
  `download-ena-study` tickets a newer download ticket has superseded, last updated
  more than `--older-than` days ago and with no step still submitting or running, it
  deletes everything under `ticket/<idx>/<step>/attempt-<n>/` except `logs/`, so the
  step-logs route keeps working. Dry-run by default (sizes per action and state, plus
  the inodes the kept logs still hold); `--execute` deletes. Tickets the SynDNA
  read-count backfill would still write are skipped and listed, since reclaiming them
  would make that gap permanent. Workspaces with no `work_ticket` row are listed, and
  deleted only with `--include-orphans`. Upload staging and `staging/reads` are not
  touched.
