### Fixed

- **Resumed tickets now wait for the data plane after a control-plane restart (#NNN).**
  Startup reconcile resumes in-flight tickets in the background once the data plane answers
  its health check, instead of resuming them at once and failing them at submission when the
  control plane came up first.
