### Removed

- **Masks can no longer be deleted (#824).** A mask whose filtering config is bad is
  deprecated (`PATCH /mask-definition/{mask_idx}/status`), and a bad run of a sound
  config is invalidated (`PATCH /mask-definition/{mask_idx}/sample-status`); both keep
  the record of what filtered the data. Removed:
  - `DELETE /mask-definition/{mask_idx}`. `GET` on the same path is unchanged.
  - `qiita-admin mask delete` and `qiita-admin mask purge-failed`.
  - The `mask_definition:delete` scope. A personal access token minted with it keeps
    working; the scope is dropped from its effective set.
  - The data plane's `delete_mask` DoAction.

### Changed

- **The block-mask plan's 409 for an already-gated sample no longer says to delete the
  mask (#824).** It names the mask (`mask_idxs` in the body) and says
  to deprecate it if it is bad; `only_missing=true` still plans only the ungated
  samples.
- **`qiita-admin backfill mask-adapter-hash --execute` stops with an error naming a
  planned mask whose row is gone at write time (#824).** It skipped
  the row before. Rows written before the error stay written; a re-run plans only the
  rest.

### Fixed

- **`qiita-admin backfill mask-adapter-hash --execute` skips a mask the mint re-keyed
  after the plan read it, instead of overwriting it with the plan's stale params
  (#824).** The summary line reports how many it skipped.
