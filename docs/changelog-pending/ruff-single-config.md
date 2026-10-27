### Changed

- **Ruff settings live in one file, `ruff.toml` at the repo root (#783).** The
  `pyproject.toml` files of the three packages and of `tests/integration` point at it
  with `extend`, so each keeps its own first-party import detection.
  `tests/integration` and `scripts`, which had no ruff config and were formatted only
  by the pre-commit hook on ruff's defaults (88 columns), now follow the same
  settings, and `make lint-python` (which CI and `make lint` run) checks them through
  a new `lint-repo-python` target. `workflows/` keeps ruff's defaults through
  `workflows/ruff.toml`: its scripts run on each container's own Python. The
  pre-commit hook moves from ruff 0.15.0 to 0.15.7, the version the lock files resolve.
