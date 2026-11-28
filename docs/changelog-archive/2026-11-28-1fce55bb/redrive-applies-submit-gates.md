### Fixed

- **Running a work ticket again applies the same checks as submitting it (#902).** `POST
  /work-ticket/{idx}/run` now requires the ticket's originator or a wet_lab_admin+, and re-checks the
  action's audience and scopes, the stored `resource_override` and host paths, and study access
  against the caller. A caller who may not see the ticket gets the same 404 as for a missing one. The
  ticket is checked against its action's current definition: one stored under a version since
  edited in place (a tighter `context_schema`, a lower `mem_ceiling_gb`) is refused with 422 and
  the work must be submitted again.
