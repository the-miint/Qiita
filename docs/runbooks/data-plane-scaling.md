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
| `QIITA_DATA_PLANE_BIND_HOST` | `127.0.0.1` | The IPv4 address this host's instances listen on. |

With none of them set, a deploy runs one instance on `127.0.0.1:50051`. The standing
`sudo make redeploy` reads them from the file; nothing is passed on the command line.

Never edit `/etc/nginx/conf.d/qiita.conf` on the host: every deploy overwrites it.

## SELinux

Every host needs the loopback-listener port label from
[`first-deploy.md` §0.4](first-deploy.md#04-nginx--tls--dns) when SELinux is
Enforcing, one instance or many. `make preflight` checks that label only. Whether
nginx may *connect* to an added instance or a peer is governed by whatever already
lets it reach `127.0.0.1:50051` on the host (for example the
`httpd_can_network_connect` boolean).

## More instances on this host

1. Set the list in `/etc/qiita/data-plane.env`:

   ```bash
   # [admin]
   QIITA_DATA_PLANE_PORTS="50051 50052 50053"
   ```

2. Redeploy (`sudo make redeploy QIITA_HOSTNAME=<fqdn>`). `activate.sh` renders one
   upstream member per port and enables and restarts each unit, so an added instance
   also starts at boot.
3. `sudo make verify-deploy QIITA_HOSTNAME=<fqdn>` shows one `health/data-plane@<port>`
   row per port and a `health/data-plane-lb` row for the loopback listener.

Each instance is another process on the same cores and memory.

## Fewer instances on this host

Remove the port from `QIITA_DATA_PLANE_PORTS` and redeploy, then disable the removed
unit. The deploy never disables a unit; until you do, the instance keeps running on
the previous deploy's code, outside the upstream:

```bash
# [admin]
sudo systemctl disable --now qiita-data-plane@50053
```

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
`deploy/nginx/qiita.conf`), so a peer belongs on a private network.

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

On this host:

4. Set `QIITA_DATA_PLANE_PEERS="<peer address>:50051"` (one entry per peer instance) in
   `/etc/qiita/data-plane.env` and redeploy. A peer name that does not resolve aborts
   the deploy before anything is installed. Prefer an IP address: the nginx Rocky 10
   ships resolves a name only when it loads the config (see `deploy/nginx/qiita.conf`).
5. `verify-deploy` shows one `health/data-plane-peer@<host:port>` row per peer entry.

To remove a peer, drop it from `QIITA_DATA_PLANE_PEERS` and redeploy; it leaves the
upstream on that deploy. Stopping the peer's own instances is done on the peer host.
