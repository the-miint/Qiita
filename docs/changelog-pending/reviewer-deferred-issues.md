### Changed

- **A deferred review finding has its issue opened before the PR is.** The `qiita-review`
  skill, the reviewer's Scope discipline, `CLAUDE.md` and the PR template now say that
  each `Deferred:` entry of the `## Reviewer loop` block names an issue by number;
  `issue to file` is no longer an entry. `review-loop-check` is unchanged and does not
  check this.
