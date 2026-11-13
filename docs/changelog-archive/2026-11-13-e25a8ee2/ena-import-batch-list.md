### Added

- **`GET /api/v1/ena-import-batch` and `qiita ena-import-list` (#862)** list ENA import batches
  newest first, each with its idx and a count of items per state, so a batch whose submit
  response was lost can be found. `--mine` limits to your own, `--since` takes a timestamp
  with timezone. `qiita submit-ena-import` prints the exact list command to run after a
  Ctrl-C before the server answered, when the submit was sent but no response came back, or
  when a gateway answered 502/503/504.
