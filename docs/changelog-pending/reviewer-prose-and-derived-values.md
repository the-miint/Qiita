### Added

- **The `qiita-reviewer` agent flags more prose and two code patterns (#TBD).** R2 now
  flags a counterfactual written as current behavior, an action where a capability is
  meant, a docstring that restates its own name or signature, a comment that rules out
  what the code cannot do, and British spelling in new prose. R4 now flags a
  create-or-reuse path that sets a property on create but does not check it on reuse,
  and a timestamp, count, ETag or error detail that reveals rows a permission filter
  hides; both have their own Trigger-map row.
