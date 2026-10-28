# Data-plane scaling

For the admin who deploys a host: running more than one data-plane instance on it,
adding data planes that run on other hosts, and undoing either. What the deploy
renders from which setting, and how each setting is validated, is in
`deploy/_common.sh` under "Data-plane topology"; this page is the procedure. This
repo has no measurement of what an added instance or peer buys in throughput.

## Settings

Three keys in `/etc/qiita/data-plane.env`, read on every deploy by `activate.sh` and
`verify.sh`, and (the bind host) by the `qiita-data-plane@.service` units:

| Key | Default | Meaning |
|---|---|---|
| `QIITA_DATA_PLANE_PORTS` | `50051` | Instances this host runs, space-separated and quoted. Each port is one `qiita-data-plane@<port>` unit. |
| `QIITA_DATA_PLANE_PEERS` | none | Data planes on other hosts that this host's nginx also balances to, as `host:port`. |
| `QIITA_DATA_PLANE_BIND_HOST` | `127.0.0.1` | The IPv4 address this host's instances listen on: loopback or private, never public. Leave it at `127.0.0.1` on a host that runs the control plane, unless the control plane is first pointed at the loopback listener ([below](#the-control-plane-through-nginx)): its default `DATA_PLANE_URL`, `grpc://localhost:50051`, finds no instance once they listen elsewhere. |

With none of them set, a deploy runs one instance on `127.0.0.1:50051`. The standing
`sudo make redeploy` reads them from the file; nothing is passed on the command line.

Never edit `/etc/nginx/conf.d/qiita.conf` on the host: every deploy overwrites it.

## More instances on this host

1. Set the list in `/etc/qiita/data-plane.env`:

   ```bash
   # [admin]
   QIITA_DATA_PLANE_PORTS="50051 50052 50053"
   ```

2. Redeploy (`sudo make redeploy QIITA_HOSTNAME=<fqdn>`). `activate.sh` renders one
   upstream member per port, waits for every instance to go idle and restarts them
   together ([deployments.md](../deployments.md), "Restarts wait on the data plane's
   in-flight work"), and enables each unit, so an added instance also starts at boot.
3. `sudo make verify-deploy QIITA_HOSTNAME=<fqdn>` shows one `health/data-plane@<port>`
   row per port and a `health/data-plane-lb` row for the loopback listener.

Each instance is another process on the same cores and memory, and none of them knows
about the others. Unset, `DUCKDB_MEMORY_LIMIT` and `DUCKDB_THREADS` are derived per
instance from the whole host's memory and CPU, divided among that instance's own pool
(`qiita-data-plane/src/config.rs`), so N instances on one host each budget for the whole
host. Before adding an instance, set both in `/etc/qiita/data-plane.env`, which every
local instance reads: `DUCKDB_MEMORY_LIMIT` to the memory the data plane may use divided
by (instances × `DUCKLAKE_POOL_SIZE`, default 8), and `DUCKDB_THREADS` to the cores it may
use divided the same way. Each pooled connection also holds one backend on the catalog's
Postgres for as long as it lives; count instances × pool size on every host, plus the
control plane, against that server's `max_connections` (`DEFAULT_POOL_SIZE` in the same
file says what it was chosen against).

## Fewer instances on this host

Remove the port from `QIITA_DATA_PLANE_PORTS` and redeploy with
`QIITA_DP_DISABLE_REMOVED=1`:

```bash
# [admin]
sudo make redeploy QIITA_HOSTNAME=<fqdn> QIITA_DP_DISABLE_REMOVED=1
```

The deploy waits for the removed instance to go idle along with the others, then
disables and stops it while the listed instances restart, so its drain runs at the
same time as theirs. Without the flag, a deploy that finds a `qiita-data-plane@` unit
running or enabled outside the list stops before installing anything and names it;
an instance left there would keep running the previous deploy's build, outside the
upstream. `systemctl disable --now <unit>` by hand also removes it, but stops it
without first waiting for its in-flight requests the way the deploy does. If a
unit is started outside the list after a deploy, `sudo make verify-deploy
QIITA_HOSTNAME=<fqdn>` fails `data-plane-units/<unit>`.

## The control plane through nginx

The control plane's default `DATA_PLANE_URL`, `grpc://localhost:50051`, reaches
instance 50051 only. On a host running more than one instance, point it at the
loopback listener so its calls spread across the upstream. Do this after a deploy
whose `verify-deploy` shows `health/data-plane-lb` green:

1. Set `DATA_PLANE_URL=grpc://127.0.0.1:50050` in `/etc/qiita/control-plane.env`
   (`sudoedit /etc/qiita/control-plane.env`; replace an existing `DATA_PLANE_URL`
   line if there is one).
2. `sudo systemctl restart qiita-control-plane`, then re-run `verify-deploy`.

Before rolling back to a commit whose `deploy/nginx/qiita.conf` has no loopback
listener, remove that line and restart the control plane: nothing would be listening
on port 50050.

## Data planes on other hosts (peers)

A peer is a data plane another host runs, listed in this host's upstream. Traffic from
this host's nginx to a peer is unencrypted gRPC (see the upstream comment in
`deploy/nginx/qiita.conf`), so the deploy accepts only private addresses (10.0.0.0/8,
172.16.0.0/12, 192.168.0.0/16): a peer, and every address a peer's name resolves to,
must be one, and `QIITA_DATA_PLANE_BIND_HOST` must be one or loopback. Anything else
stops the deploy before anything is installed.

On the peer host:

1. Deploy the same commit there. Its `/etc/qiita/data-plane.env` must name the same
   DuckLake catalog (`DUCKLAKE_CATALOG_CONNSTR`) and the same `FLIGHT_TICKET_PUBLIC_KEY`
   as this host's, and its `PATH_SCRATCH` and `PATH_PERSISTENT` must be the same
   shared filesystems mounted at the same paths: the peer serves this host's lake,
   verifies tickets this host's control plane signs, and moves uploads between
   staging and the lake by path. A data plane pointed at a different catalog returns
   that catalog's rows, not an error. The peer needs nginx installed, because
   `activate.sh` renders `/etc/nginx/conf.d/qiita.conf` on every host; without the TLS
   files that nginx is not reloaded. `activate.sh` restarts only services whose env
   file exists, so a host with only `data-plane.env` restarts only its data-plane
   units. A data-plane-only host deploy has not been exercised in this repo.
2. Set `QIITA_DATA_PLANE_BIND_HOST=<the peer's private address>` in its
   `data-plane.env`, so its instances listen where this host can reach them.
3. Allow the peer's data-plane ports only from this host's address in the peer's
   firewall. The listener has no TLS and no authentication beyond the ticket signature.
4. Optional, to spill to the peer's own disk rather than the shared filesystem: set
   `DUCKDB_TEMP_DIRECTORY` in the peer's `data-plane.env` to a local path the data
   plane can create or write, and `DUCKDB_MAX_TEMP_DIRECTORY_SIZE`, a cap per pooled
   connection, so that instances × `DUCKLAKE_POOL_SIZE` × the cap fits that disk
   (`.env.data-plane.example` describes both and their defaults).

On this host:

5. Set `QIITA_DATA_PLANE_PEERS="<peer address>:50051"` (one entry per peer instance) in
   `/etc/qiita/data-plane.env` and redeploy. A peer name that does not resolve, or
   resolves to any public address, aborts the deploy before anything is installed. Prefer an IP address: the nginx Rocky 10
   ships resolves a name only when it loads the config (see `deploy/nginx/qiita.conf`).
6. `verify-deploy` shows one `health/data-plane-peer@<host:port>` row per peer entry,
   and one `commit/data-plane-peer@<host:port>` row that fails unless the peer runs
   this host's deployed commit. A peer deployed from another commit, or one whose
   instances were not restarted, fails it; the peer's journal names the commit it
   runs (`qiita-data-plane build:`).

To remove a peer, drop it from `QIITA_DATA_PLANE_PEERS` and redeploy; it leaves the
upstream on that deploy. Stopping the peer's own instances is done on the peer host.
