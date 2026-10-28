### Added

- **Data-plane scaling is host configuration rendered on every deploy, and can include data planes on other hosts (#363).**
  Three keys in `/etc/qiita/data-plane.env` — `QIITA_DATA_PLANE_PORTS` (default `50051`),
  `QIITA_DATA_PLANE_PEERS` (default none) and `QIITA_DATA_PLANE_BIND_HOST` (default
  `127.0.0.1`) — decide what `activate.sh` renders into the nginx `qiita_data_plane`
  upstream, which `qiita-data-plane@<port>` units it enables and restarts, and what
  `verify-deploy` checks: `health/data-plane@<port>` per instance (replacing the single
  `health/data-plane` row), `health/data-plane-peer@<host:port>` per peer,
  `health/data-plane-lb`, `commit/data-plane@<port>` and
  `commit/data-plane-peer@<host:port>`, which fail for a member not running this
  host's deployed commit, and `data-plane-units/<unit>`, which fails for a running or
  enabled instance whose port the list no longer names. A deploy that finds such an
  instance stops before installing anything, unless it runs with
  `QIITA_DP_DISABLE_REMOVED=1`, which disables and stops it alongside the restart. Before, the upstream was a checked-in literal the deploy
  overwrote and only `@50051` was restarted, so an added instance lost its upstream entry
  and kept running old code after the next deploy. The keys are validated, and peer names
  resolved, before anything is installed; a peer and the bind host must be private
  addresses (the bind host may also be loopback), and the rendered nginx config is tested before
  any service restarts; one `nginx -t` rejects is replaced by the config it overwrote,
  so the next nginx start does not fail on it. nginx also serves the upstream on a loopback
  listener, `127.0.0.1:50050`, which the control plane uses when
  `DATA_PLANE_URL=grpc://127.0.0.1:50050`. nginx gives a member 5 s to accept a
  connection (`grpc_connect_timeout`) before passing the call to the next one; at the
  60 s default, a call to a member that stopped answering failed instead. `make deploy`
  prints the `local-deploy.sh` / `make redeploy` command instead of a hand copy of
  `deploy/nginx/qiita.conf`, a template that fails `nginx -t` unrendered. The deploy's
  idle wait covers every local instance, counting each one's open `*.partial` uploads,
  and all of them restart in one `systemctl restart`, so their drains overlap. Data
  planes booting together against one catalog set it up one at a time, on a Postgres
  advisory lock, each waiting up to 60 s for it (`ducklake::setup_catalog`). Procedure, including how to divide memory, threads and
  catalog connections across instances on one host: `docs/runbooks/data-plane-scaling.md`.
