### Fixed

- **A data-plane instance keeps its catalog backend open while idle (#TBD).** Each pooled
  DuckLake connection now attaches with `pg_pool_idle_timeout_millis=0`, so DuckDB's
  postgres extension no longer closes its Postgres backend after 60 s without a request
  and opens a new one on the next. The idle limit is read at ATTACH, so it is set on the
  connection's database before the attach.
- **`verify-deploy`'s `catalog-connections` row passes against an idle data plane (#TBD).**
  It counts each of this host's instances from `QIITA_DATA_PLANE_PORTS` at
  `DUCKLAKE_POOL_SIZE` plus the lock sessions, whether or not the catalog server shows a
  session of it now, and no longer fails when no instance has a session open. An
  instance with no session is listed as `(no session open)`. Instances on other hosts
  still count only while one of their sessions is open.
