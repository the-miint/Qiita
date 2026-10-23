### Fixed

- **The CI section of `docs/architecture/build-and-deploy.md` links to the workflows instead of copying `ci.yml` (#658).**
  The copy had drifted out of date. The section now links to
  `.github/workflows/ci.yml` and `.github/workflows/review-loop-check.yml` and describes
  only what the files cannot say about themselves: which jobs run on pull requests, the
  Ubuntu-only versus macOS matrix, and the `ci-macos`, `no-changelog`, `no-deploy-note`
  and `no-agent-review` labels.
