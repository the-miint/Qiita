### Fixed

- **Running a work ticket again applies the same checks as submitting it (#NNN).** `POST
  /work-ticket/{idx}/run` now requires the ticket's originator or a wet_lab_admin+, and re-checks the
  action's audience and scopes, the stored `resource_override` and host paths, and study access
  against the caller. A caller who may not see the ticket gets the same 404 as for a missing one.
