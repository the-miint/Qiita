### Changed

- **The `qiita-reviewer` agent flags more prose patterns and two code patterns
  (#TBD).** R2 now flags a counterfactual written as current behavior, an action where
  a capability is meant, a docstring that restates its own name or signature, a comment
  that rules out what the code is unable to do or names a case that the code does not
  treat differently, and British spelling in words that a diff adds. R4 now flags a
  create-or-reuse path that sets a property on create but does not check it on reuse,
  and a timestamp, count, ETag or error detail that reveals the existence of rows
  hidden by a permission filter; both have their own Trigger-map row.
