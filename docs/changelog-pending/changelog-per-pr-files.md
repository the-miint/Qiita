### Changed

- **A changelog entry is now its own file (#672).** A PR adds one Markdown file under
  `docs/changelog-pending/` instead of a bullet in `CHANGELOG.md`, so two PRs no
  longer conflict on the same lines. `changelog-check` requires that file;
  `/deploy-archive` moves the pending files into a per-deploy folder beside the checklist
  archive. `CHANGELOG.md` now holds only the instructions, and its previous contents are
  in `docs/changelog-archive/2026-10-09-cbe09ce9.md`.
