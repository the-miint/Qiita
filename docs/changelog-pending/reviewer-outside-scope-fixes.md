### Changed

- **A small fix outside a PR's subject lands in that PR instead of becoming an issue.** The
  reviewer's Scope discipline now offers a pre-existing problem as a fix for the branch when
  the fix is correct on reading the diff alone, changes no behaviour a test does not pin,
  touches no operator-impacting surface, and makes no design choice for a maintainer; a
  finding in code that only such a fix touched is deferred. The `qiita-review` skill lands
  each one as its own commit, names the paths it touched to the next round, and lists it
  under `Fixed:` in the `## Reviewer loop` block. Everything else is deferred to an issue
  as before. `review-loop-check` is unchanged.
