### Changed

- **`review-loop-check` fails while a commit pushed after the reviewed one is not named
  on the "Reviewed at" line (#784).** Before, any one commit of the PR passed, so a
  description written after the first round kept passing however much was pushed
  after it; now a push after the block was written fails the check until the line
  names the new commit by sha (a prefix of at least 7 characters) or another round
  moves it. A merge of the base needs no naming; a merge that brings in other commits
  is named itself.
