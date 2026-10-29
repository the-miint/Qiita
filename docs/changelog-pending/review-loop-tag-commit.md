### Changed

- **`review-loop-check` no longer needs the PR-number tag commit named on the "Reviewed
  at" line (#786).** A later commit is exempt when it only modifies files under
  `docs/changelog-pending/` and `DEPLOY_CHECKLIST.md`, adds at least one tag with the
  PR's own number, adds no placeholder tag such as `(#TBD)` or `(#<branch>)`, and
  leaves each file byte-identical once that tag and the placeholder tags are dropped.
  Any other change in the same commit, a rewrap or a moved line included, still has to
  be named. The workflow passes the PR number to the script; run without one, it
  exempts nothing.
