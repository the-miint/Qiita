### Added

- **`DISPATCH_MAX_INFLIGHT` sets the control plane's process-wide dispatch cap (#953).**
  The cap on work tickets running at once, across every action, was a fixed 8. It is
  now read from `control-plane.env` at boot: one of 8, 12, 16 or 24, default 8, and any
  other value fails the boot. The control plane's Postgres pool is sized from it, at
  least 25 connections as before and more above a cap of 12.
- **`GET`/`PATCH /work-ticket/dispatch-cap` and `qiita-admin dispatch-cap` read and
  retune the cap on a running control plane (#953).** `system_admin` with
  `work_ticket:cancel`. The response gives the cap in force, the boot value, any
  override, how many tickets hold a slot and how many wait, and the largest override
  the pool built at boot can carry (12 when booted at 8 or 12). `--set N` raises or
  lowers it; raising starts queued tickets at once, lowering cancels nothing. `--reset`
  clears it. The override is in-memory and a restart reverts to
  `DISPATCH_MAX_INFLIGHT`.
