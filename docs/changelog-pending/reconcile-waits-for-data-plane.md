### Fixed

- **Resumed tickets now wait for the data plane after a control-plane restart (#899).**
  Startup reconcile resumes in-flight tickets in the background once the data plane reports
  SERVING, instead of resuming them at once and failing them at submission when the control
  plane came up first. A ticket `/run` dispatches during the wait is not resumed a second time,
  and `DATA_PLANE_URL` must now be a `grpc://host:port` address (the health probe dials nothing
  else). Startup follow-on and ENA-batch submissions are not covered yet.
