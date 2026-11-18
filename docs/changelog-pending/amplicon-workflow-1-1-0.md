### Added

- **Amplicon workflow 1.1.0.** The minimal Rapid 16S workflow: the denoise step drops
  the primer-orient knobs (16S reads in these preps already start at the amplicon, so
  there is nothing to orient), and the run ends by minting each processed sample's
  public identifier and closing a per-sample completion gate (`qiita.amplicon_sample`,
  'completed' when denoise produced ASVs for it, else 'no_data' — the gate records
  state; the count is read from `amplicon_membership`). Two new terminal actions —
  `mint-amplicon-identifiers` and `finalize-amplicon-sample` — carry that, and the
  runner materializes the gate's 'pending' rows when it mints the run.

### Changed

- The amplicon denoise step refuses `orient_primer: true`; 1.0.0 keeps loading and binds
  the step unchanged, but orienting is no longer performed.
- ASV sequences are stored inline in the lake's `amplicon_sequence` table rather than a
  separate chunk table — an ASV is short (one 64 KB chunk at most), so chunking is pure
  overhead. The data plane widens an existing lake's three-column table on restart
  (`ALTER TABLE … ADD COLUMN IF NOT EXISTS sequence`), so an amplicon `register-files`
  does not fail on the now-four-column Parquet; the superseded `amplicon_sequence_chunks`
  table is left in the lake unread. See `DEPLOY_CHECKLIST.md`.
