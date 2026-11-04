### Changed

- **The work-ticket submit path moves out of the routes layer into its own module (#705).**
  `submit_work_ticket_core`, its submission gates (audience, scope, ingest-path and
  one-in-flight checks, and the follow-on gate) and their helpers move from
  `routes/work_ticket.py` into a new `qiita_control_plane/work_ticket_submit.py`. The
  follow-on completion hook and the ENA import batch, which both submit tickets in
  process, now reach the submit path there instead of importing a routes module. No
  behavior change: every moved function is unchanged and the `POST /work-ticket` route is
  a thin caller.
