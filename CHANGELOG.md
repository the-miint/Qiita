# Changelog

The "what changed" log for this repo. The git history is the authoritative record;
the `(#N)` tag on each entry traces it to its PR. Operator deploy steps live separately
in [`DEPLOY_CHECKLIST.md`](DEPLOY_CHECKLIST.md): a changelog entry says what changed, a
step there says what the operator must do, and a change can warrant either or both.

This file holds no entries. Each PR adds its own file, so two PRs never edit the same
lines.

## Adding an entry

Add one new Markdown file under `docs/changelog-pending/`, named for the change
(`study-list.md`, `ena-md5-retry.md`). Inside, put each bullet under the
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) heading it belongs to, and tag
it with the PR number:

```markdown
### Fixed

- **An ENA md5 mismatch is retried instead of failing the ticket (#661).** What changed,
  in the terms a reader of the API or the CLI would use.
```

The headings are `### Added`, `### Changed`, `### Fixed` and `### Removed`; a file may
carry more than one. Do not edit another PR's file, and do not add entries to this one.

The `changelog-check` CI job fails a PR that adds or changes no file under
`docs/changelog-pending/`; changing a file is for correcting your own earlier
entry. A PR that warrants no entry (a typo fix, a CI-only change) carries the
`no-changelog` label.

## Reading it

- **Merged, not yet deployed:** the files in `docs/changelog-pending/`.
- **Deployed:** one folder per deploy that shipped entries, under
  [`docs/changelog-archive/`](docs/changelog-archive/), named `<YYYY-MM-DD>-<short SHA>`
  like its checklist twin in [`docs/deploy-archive/`](docs/deploy-archive/).
  `/deploy-archive` moves the pending files there after a deploy; nothing is merged into
  one file.
- **Before one file per PR:** the two `<date>-<sha>.md` files in
  `docs/changelog-archive/`. The newer one includes entries that were merged but not yet
  deployed when it was rotated; its header says which.

To read a set as one page: `cat docs/changelog-pending/*.md`.
