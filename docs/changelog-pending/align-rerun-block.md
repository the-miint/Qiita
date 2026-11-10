### Added

- **A finished align block can be re-run, and rows no gate tracks can be deleted
  (#TBD).** `POST /api/v1/alignment-definition/{idx}/rerun-block` re-arms one
  completed align block ticket: it deletes the ticket's step rows, sets the block and
  the gates it covers back to `pending`, and dispatches it, so the align workflow
  replaces that block's rows and reconcile completes the gates again. It refuses
  (409, with a `reason` code) a ticket that is not an align ticket of that alignment,
  a block whose other covering blocks are not completed, a missing or invalidated
  gate, an unfinished feature table reading the alignment, and a ticket whose
  registered attempt directory is gone (the re-run would reuse the registered file's
  lake name). A 503 with a `reason` (`deadline_passed`, `statement_timeout`,
  `scratch_timeout`) means it ran out of time and changed nothing; a call that
  commits does so within `RERUN_REQUEST_BOUND_S` of the route starting, so a caller
  that lost the response can check the ticket after that. With `allow_deprecated_mask` it also retries a
  failed re-run whose mask has since been deprecated, which `/run` refuses. The
  re-arm stamps the new `qiita.block.rearmed_at`, and abandon-plan refuses a
  re-armed block that has not completed again (409 with its `block_idx`) unless the
  body sets `allow_rearmed` (`qiita-admin alignment abandon-plan --allow-rearmed`). `POST …/{idx}/purge-sample-rows`
  deletes every row of a prep_sample that has no gate under the alignment, including
  `prep_sample_idx = 0`. Both need `alignment_definition:lifecycle`. The CLI is
  `qiita-admin alignment rerun-blocks` (one batch of at most 6, waits and reports) and
  `purge-sample-rows`; `scripts/align-rerun-compare.sh` checks a batch against a
  lake snapshot, and `docs/runbooks/align-block-rerun.md` is the procedure.
