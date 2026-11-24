### Added

- **krepp placement index builds can be registered onto a reference, activated and
  removed (#937).** A build made outside Qiita is placed at
  `PATH_DERIVED/references/{reference_idx}/krepp/{build_label}/`. The
  `reference-index-register` workflow checks it (over the reference's own tree and edge
  numbers, no symlinks), seals it (`manifest.json` and `SHA256SUMS`) and records it
  `registered`, with the krepp version that wrote it. The `reference-index-activate`
  workflow makes it the reference's active build, retiring the previous one.
  `qiita-admin krepp-index remove` marks a registered or retired build removed. No
  workflow places contigs with a build.
- **A node cache for sealed folders, `PATH_NODE_LOCAL` (#937).** Optional: a directory
  on each compute node's own disk, writable by the job user. A job that reads a sealed
  folder (a krepp build) copies it there on first use on that node, under a per-folder
  lock: it checks the free space, then hashes every file as it copies it; later jobs
  on the node check the copy and use it. Copies of removed builds are not deleted yet
  (#942). Unset, jobs read from `PATH_DERIVED`. Nothing is copied at deploy or by hand.
- **`qiita.reference_index.state` (`registered`, `active`, `retired`, `removed`)
  (#937).** Every existing row is `active`; a krepp row's `fs_path` is relative to
  `PATH_DERIVED`. `GET /reference/{reference_idx}/index` returns it.
- **`work_ticket.reference_index_idx` (#937):** the krepp build a ticket is fixed to.
  `POST /work-ticket/{idx}/run` refuses a ticket whose build was removed.
- **Route (#937):** `POST /reference/{reference_idx}/index/{reference_index_idx}/remove`
  (`reference:delete`).
- **compute-readiness `node-cache` row (#937).** It fails the deploy when
  `PATH_NODE_LOCAL` is set but is not a writable directory on the probe's node, and
  skips when it is unset.

### Changed

- **A reference with a krepp build that is not removed cannot be deleted, even with
  force (#937).** Deleting a reference removes its `PATH_DERIVED` subtree, which holds
  the build's master copy.
- **The storage docs describe the tiers that exist (#937).** The `persistent-local`
  tier and its rebuild-on-miss were never implemented; indexes Qiita builds live under
  `PATH_DERIVED`.
