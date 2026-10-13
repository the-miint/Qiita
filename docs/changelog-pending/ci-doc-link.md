### Fixed

- **The CI section of `docs/architecture/build-and-deploy.md` can no longer go stale against `ci.yml` (#658).**
  It was a pasted copy of the workflow that had drifted out of date. It now links to
  `.github/workflows/ci.yml` and describes only what the file cannot say about itself:
  which jobs run on pull requests, the Ubuntu-only versus macOS matrix, and the `ci-macos`,
  `no-changelog` and `no-deploy-note` labels.
