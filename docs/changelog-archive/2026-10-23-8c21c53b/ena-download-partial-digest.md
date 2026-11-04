### Changed

- **The work-ticket digest flags a partially landed ENA download (#742).** A completed
  `download-ena-study` ticket that stored some runs and failed others now reads
  `completed, partial: N runs failed (see qiita ena-import-status B)` in the digest,
  where `B` is the newest batch that names the ticket (omitted when none does). The
  ticket's state and the subject line are unchanged.
