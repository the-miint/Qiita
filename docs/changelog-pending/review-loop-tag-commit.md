### Changed

- **`review-loop-check` no longer needs the PR-number tag commit named on the "Reviewed
  at" line (#TBD).** A later commit that touches only `docs/changelog-pending/` and
  `DEPLOY_CHECKLIST.md`, and whose changed lines differ from the old ones only by
  ` (#N)` tags, replacing a placeholder or adding the tag, is exempt. In CI each tag it
  adds must be the PR's own number. Any other change in the same commit, a rewrap
  included, still has to be named.
