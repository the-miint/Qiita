### Fixed

- **A data-plane instance keeps its catalog backend open while idle (#917).** Each pooled
  DuckLake connection now attaches with `pg_pool_idle_timeout_millis=0`, so DuckDB's
  postgres extension no longer closes its Postgres backend after 60 s without a request
  and opens a new one on the next. The idle limit is read at ATTACH, so it is set on the
  connection's database before the attach.
- **`verify-deploy`'s `catalog-connections` row passes against an idle data plane (#917).**
  It counts each of this host's instances from `QIITA_DATA_PLANE_PORTS` at
  `DUCKLAKE_POOL_SIZE` plus the lock sessions, whether or not the catalog server shows a
  session of it now, and no longer fails when no instance has a session open. An
  instance with no session is listed as `(no session under its name)`. Instances on other hosts
  still count only while one of their sessions is open.
- **`verify-deploy`'s `health/data-plane-lb` row passes against the shipped nginx config
  (#917).** nginx's loopback listener now routes gRPC server reflection
  (`grpc.reflection.v1` and `v1alpha`) to the data plane, which `grpcurl` calls before a
  `Health/Check` when it has no proto file. Any on-host client of the listener can now
  list the data plane's services through it; the 443 edge does not route reflection.
