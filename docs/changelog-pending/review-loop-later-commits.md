### Changed

- **`review-loop-check` fails while a commit pushed after the reviewed one is not named
  on the "Reviewed at" line (#TBD).** Before, any one commit of the PR passed, so a
  description written after the first round kept passing however much was pushed
  after it. Each later commit on the branch's own line of history now has to be named
  there by sha (a prefix of at least 7 characters); merge commits, and the commits a
  merge brings in, are exempt. The check re-runs on every push.
