### Added

- **A feature table's taxonomy on demand.** `POST /feature-table/taxonomy` answers the
  same cohort request as the feature table with each observed feature's taxonomic lineage
  — its md5 and the reference's ranks `(domain … strain)` with their `d__/p__…` prefixes
  — as an Arrow IPC stream. The lineage is a direct lookup: a feature's canonical
  `feature_idx` joins to the taxonomy of the reference the request names, so matching is
  strand-robust, and a feature the reference does not place keeps its row with NULL ranks
  rather than being dropped. It is served separately from the table itself — the feature
  table carries no taxonomy; a caller that wants lineage asks for it with this second
  request. `qiita feature-table runs --taxonomy --taxonomy-reference-name NAME
  --taxonomy-reference-version VERSION` writes it beside the bundle as
  `<stem>.taxonomy.parquet`.

  The reference travels with the request, named by its public `(name, version)` handle
  (what `GET /reference` lists) — the internal `reference_idx` is resolved server-side and
  never crosses the boundary, and there is no server-wide default, so one request can
  classify a run's features against one reference release and another against a newer
  release without a redeploy. The route refuses 404 for an unknown handle, and 409 when
  the reference exists but has no taxonomy loaded yet, told apart from "places nothing" by
  a readiness probe.
