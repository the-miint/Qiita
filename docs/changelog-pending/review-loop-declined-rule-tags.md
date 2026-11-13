### Changed

- **A PR's `Reviewer loop` block names the rule each declined finding sets aside (#TBD).**
  Each `Declined:` entry now starts with the rule's strength, id and short name
  (`[strong] R9 re-implementation of a helper another module owns: …`), `[strong]` entries
  first, so a human reviewer can see which rules the author set aside, and which of them
  are `[strong]`, without opening `.claude/agents/qiita-reviewer.md`. The `qiita-review`
  skill carries the rule into its ledger and the next round's prompt; the PR template
  shows the format. `review-loop-check` is unchanged and does not enforce it.
