### Changed

- **Restarting the data plane lets the requests it is serving finish (#767).** On SIGTERM
  (what `systemctl stop` and `restart` send) the data plane stops accepting connections and
  exits once every DoGet stream, DoPut and DoAction already running has finished; before,
  it exited at once and each of them failed mid-call. The unit waits up to
  `TimeoutStopSec=3900` for that before SIGKILL. New requests are refused while it drains,
  and a client sees that refusal as the data plane being unavailable.
- **A deploy waits for the data plane to go idle before restarting services (#767).**
  `activate.sh` waits, before any service restart, until the data plane has no DoGet
  stream, DoPut or registration in flight, for up to `QIITA_DP_IDLE_WAIT_S` (default 3900
  s; 0 skips the wait), and then restarts whether or not it went idle. The data plane keeps
  serving while it waits. If the wait cannot query the catalog, the deploy stops before
  any restart and says to set `QIITA_DP_IDLE_WAIT_S=0`. So a deploy can now spend up to that long before its restarts, and up to the
  unit's stop timeout on the data plane's own restart.
- **A DoPut upload appears as `upload.parquet` only once it is complete (#767).** It is
  written to `upload.parquet.partial` and published when closed; a data plane killed
  mid-upload leaves the `.partial`, never a truncated `upload.parquet`.
- **`qiita-admin masked-read-export` retries a prep_sample whose export failed because the
  data plane or control plane was unreachable (#767).** The prep_sample is exported again
  from the start with a fresh ticket, for up to `--retry-seconds` (default 3900) from its
  first failure. Any other failure still ends the export at once.
