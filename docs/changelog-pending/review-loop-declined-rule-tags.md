### Changed

- **A PR's `Reviewer loop` block names the rule each declined finding sets aside (#870).**
  Each `Declined:` entry now starts with the rule's strength, id and short name
  (`[strong] R9 copy-pasted parallel handler: …`), `[strong]` entries first, so a human
  reviewer can see which rules the author set aside, and which of them are `[strong]`,
  without opening `.claude/agents/qiita-reviewer.md`. The reviewer heads each group of
  findings that way, the `qiita-review` skill carries it into its ledger and the next
  round's prompt, and the PR template shows the format. `review-loop-check` is unchanged
  and does not enforce it.
