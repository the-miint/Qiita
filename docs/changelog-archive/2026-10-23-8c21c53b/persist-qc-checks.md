### Added

- **A step's QC check results can be persisted.** The `persist-qc-checks` action primitive
  reads a step's `qc_checks.json` and upserts its `qiita.prep_sample_qc_check` rows under
  the producer taken from the gate the workflow finalizes (alignment, mask, or none). An
  action definition is refused at load when it finalizes assembly or places
  `persist-qc-checks` before its gate's finalize, and a sidecar number must be a JSON
  number, not a boolean or a string. A re-run under the same producer replaces its row and
  returns an invalidated one to `current`. A run whose mask or alignment definition was
  deleted mid-flight fails and writes nothing. No workflow wires it yet.
