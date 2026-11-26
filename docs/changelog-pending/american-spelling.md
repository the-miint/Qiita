### Changed

- **The code and docs use American spelling (#952).** British forms (`behaviour`,
  `labelled`, `recognise`, `serialise`, …) are rewritten in comments, docs, test names
  and internal identifiers, including the feature-table relabel's `LABELED_*` names.
  No API field, stored value, CLI flag or output file name changes: the work-ticket
  state stays `cancelled`. Left as written: applied migrations, the changelog and
  deploy archives, and the files a container image's build hash covers, so no image
  rebuilds.
