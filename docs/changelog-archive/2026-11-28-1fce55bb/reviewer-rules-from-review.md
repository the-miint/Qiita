### Changed

- **The `qiita-reviewer` agent checks for patterns that human review found and it did not
  (#940).** New rules cover:
  - a new index type, feature source, public identifier or route following the mechanisms
    of the existing ones;
  - a new gate table, identifier kind or lake table being added to every list that names
    its siblings (a new PR-level check that greps for them);
  - a lake table change in the data plane's `ensure_*` functions reaching the deployed lake;
  - a `migrate:down` that fails once the new kind of row exists;
  - an input cap that applies to only one form of a request;
  - an artifact check that goes further than its siblings' checks;
  - a constraint that rules out a state the data can be in;
  - an artifact that an admin, rather than Qiita, places or removes;
  - a docstring that explains one feature's code by an unrelated feature's, or carries its
    callers' reasons;
  - a claim that one kind is "the one" with some property;
  - a count over lake data copied into a control-plane column;
  - a query result fetched only to test whether it is empty.

  Sequence storage is now chunked only when a sequence can be longer than one chunk.
  Casts of a column already of that type, a producer `ORDER BY` with no named consumer, an
  alias that renames nothing, and general code named for one feature are now findings
  rather than questions.
