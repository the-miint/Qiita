### Fixed

- **The CI section of `docs/architecture/build-and-deploy.md` links to `ci.yml` instead of copying it (#658).**
  The copy had drifted out of date. The section now links to
  `.github/workflows/ci.yml` and describes only what the file cannot say about itself:
  which jobs run on pull requests, the Ubuntu-only versus macOS matrix, and the `ci-macos`,
  `no-changelog` and `no-deploy-note` labels.
