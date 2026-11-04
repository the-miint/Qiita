### Changed

- **A retired exported identifier says its alignment was deleted, not purged (#831).**
  When an `alignment_definition` row is deleted, the `exported_identifier` and
  `exported_processing` rows that named it are retired with the reason
  "alignment_definition N was deleted; the processing this identifier named no longer
  exists". Rows retired earlier keep the reason they were written with.
