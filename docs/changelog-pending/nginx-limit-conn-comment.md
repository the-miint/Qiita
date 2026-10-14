### Fixed

- **The Flight edge's `limit_conn` comment described the opposite of what the directive does (#PRNUM).**
  `deploy/nginx/qiita.conf` said the cap counts HTTP/2 connections rather than streams, and concluded
  that "a legitimate bulk client needs only one connection". Measured on nginx 1.26.3 in front of two
  data-plane instances: six concurrent DoGets multiplexed on one connection ran two at a time under
  `limit_conn 2` and all six under `limit_conn 1000`, so the cap bounds concurrent requests per client
  address whatever the connection count. Comment only; the cap's value is unchanged.
