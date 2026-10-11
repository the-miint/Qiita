### Changed

- **A deferred review finding has its issue opened before the PR is (#688).** The `qiita-review`
  skill now says that each `Deferred:` entry of the `## Reviewer loop` block names an
  issue by number, and that `issue to file` is not an entry; the PR template says the
  same in one line. The reviewer's Scope discipline asks each deferred finding for an
  issue title and what would close it. `review-loop-check` is unchanged and does not
  check this.
