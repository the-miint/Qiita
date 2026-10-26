### Fixed

- **The Flight edge's `limit_conn` comment described the opposite of what the directive does (#704).**
  `deploy/nginx/qiita.conf` said the cap counts HTTP/2 connections rather than streams, and concluded
  that "a legitimate bulk client needs only one connection". Measured on nginx 1.26.3: six concurrent
  DoGets under `limit_conn 2` ran two at a time whether multiplexed on one connection or spread over
  six, and all six ran under `limit_conn 1000` — the directive counts in-flight requests and
  aggregates them per client address. Comment only; the cap's value is unchanged.
