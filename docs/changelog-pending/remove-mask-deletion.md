### Removed

- **Masks can no longer be deleted (#remove-mask-deletion).** A mask whose filtering config is bad is
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
  mask (#remove-mask-deletion).** Re-masking a gated sample needs a different filtering config, which
  mints a new `mask_idx`; `only_missing=true` still plans only the ungated samples.
- **`qiita-admin backfill mask-adapter-hash --execute` stops with an error naming a
  planned mask whose row is gone at write time (#remove-mask-deletion).** It skipped
  the row before. Rows written before the error stay written; a re-run plans only the
  rest.
