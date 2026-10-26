### Added

- **A work ticket can carry a follow-on (`on_success`) (#685).** `POST /work-ticket`
  accepts an `on_success` action and context; when the ticket completes, the
  control plane submits it as the same user on the same scope target.
  The follow-on passes its own action's submission checks when the parent is
  submitted, so most refusals come up front (`"on_success was refused"`). The
  ticket it created, or the reason a submission at completion was refused, is
  recorded on the parent (`follow_on_work_ticket_idx` / `follow_on_error`); the
  record is written with the new ticket, so a follow-on is never created twice.
  A failure other than a refusal records nothing, and the next control-plane
  start retries it. A follow-on may itself carry an `on_success`, so one
  submission arms a chain of stages that run in order as each completes (each
  ticket still has exactly one immediate follow-on; the chain is bounded in
  depth, and every link's submission checks run up front). Human callers only.
  In the CLI, `qiita ticket submit` and `submit-golay-demux` take
  `--then-action-id`, `--then-action-version` and `--then-context-json` to chain
  one, e.g. golay-demux followed by amplicon.
