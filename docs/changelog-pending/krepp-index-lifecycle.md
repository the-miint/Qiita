### Added

- **krepp placement index builds can be registered onto a reference, checked on a node,
  activated and removed (#TBD).** A build made outside Qiita is placed at
  `PATH_DERIVED/references/{reference_idx}/krepp/{build_label}/`. The
  `reference-index-register` workflow checks it (over the reference's own tree and edge numbers, no symlinks), writes
  `README.md`, `manifest.json`, `verify.sh` and `SHA256SUMS` into it and records it
  `registered`. After the cluster admins copy it to every compute node, the
  `reference-index-activate` workflow checks a node's copy, places pieces of up to 100
  backbone tips with it, and makes it the reference's active build when enough land on
  their own edge, retiring the previous one. `qiita-admin krepp-index remove` marks a
  registered or retired build removed. No workflow places contigs with a build. The
  build's krepp version is recorded, not required to match the linked krepp's.
- **`qiita.reference_index` rows carry `state` (`registered`, `active`, `retired`,
  `removed`) and `fs_path_root` (#TBD).** Every existing row is `active` and `absolute`;
  a krepp row's `fs_path` is relative to `PATH_NODE_LOCAL` (and to `PATH_DERIVED` for
  the master copy). `GET /reference/{reference_idx}/index` returns both.
- **`work_ticket.reference_index_idx` (#TBD):** the krepp build a ticket is fixed to.
  `POST /work-ticket/{idx}/run` refuses a ticket whose build was removed.
- **Routes (#TBD):** `POST /reference/{reference_idx}/index/{reference_index_idx}/remove`
  (`reference:delete`) and `GET /reference/krepp-build/active` (`reference:read` or
  `ticket:doget`).
- **A node-local storage tier, `PATH_NODE_LOCAL` (#TBD).** A directory on each compute
  node's own disk with the same content on every node. The orchestrator requires it,
  absolute, at SLURM boot and forwards it into every job.
- **compute-readiness `krepp-index` row and a `warn` status (#TBD).** The probe checks
  its node's copy of every active krepp build and `warn`s, without failing the deploy,
  for a missing or changed copy on that node or a build written by a krepp other than
  the one miint links (`miint_versions()`'s `krepp-version` row, duckdb-miint#473 /
  duckdb-miint#475).

### Changed

- **A reference with a krepp build that is not removed cannot be deleted, even with
  force (#TBD).** Deleting a reference removes its `PATH_DERIVED` subtree, which holds
  the build's master copy.
- **The storage docs describe the tiers that exist (#TBD).** The `persistent-local`
  tier and its rebuild-on-miss were never implemented; indexes Qiita builds live under
  `PATH_DERIVED`.
