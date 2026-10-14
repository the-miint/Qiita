### Changed

- **A small fix outside a PR's subject lands in that PR instead of becoming an issue
  (#712).** The reviewer's Scope discipline now offers a pre-existing problem as an
  `[outside-subject]` fix for the branch when reading settles that the fix is correct,
  every code behaviour it changes is pinned by a test, it touches no surface
  `deploy-note-check` gates and needs no `DEPLOY_CHECKLIST.md` fold, and it makes no design
  choice for a maintainer. The `qiita-review` skill lands each one as its own commit, adds
  a changelog bullet when it would warrant an entry on its own, names its commits to the
  next round, and lists it with them under `Fixed:` in the `## Reviewer loop` block. In
  code such a commit wrote, a defect it introduced is a finding against it and a further
  pre-existing problem is deferred. An offer the author does not take, and everything
  else, is deferred to an issue as before. `review-loop-check` is unchanged.

### Fixed

- **A fanned-out reviewer subagent now gets the caller's prompt (#712).** The
  qiita-reviewer's subagent prompt template carried none of it, so in a later round a
  family subagent could read past the round's scope, re-raise a finding the caller had
  already declined or deferred, and leave the round's fixes unconfirmed. The template now
  passes the prompt on verbatim: its scope, standing ledger, `[outside-subject]` commits
  and ask.
