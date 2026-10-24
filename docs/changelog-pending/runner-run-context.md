### Changed

- **The workflow runner passes the values fixed for a run as one `RunContext` (#774).**
  `run_workflow` builds it once; `_run_entry_with_retry`, `_dispatch_step`,
  `_adopt_or_submit`, `_dispatch_action`, `_run_action_primitive` and
  `_reconstruct_completed_outputs` take it in place of the pool, ticket, backend client,
  signing key, data-plane URL, scope target, action ceiling, retry budget, poll interval,
  resume flag, fan-out settings, ticket-wide memory override, ticket workspace and prior
  step progress they each took separately. No behaviour changes.
  The reviewer's R10 gains a rule that a value fixed for the run reaches a runner function
  through the context.
