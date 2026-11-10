### Changed

- **The data plane's Flight service is split into modules, with no behaviour change
  (#790).** `qiita-data-plane/src/flight_service.rs` and its test file are now the
  `flight_service/` module directory: `mod.rs` holds the service and the `do_action`
  dispatcher, and `doget.rs`, `query.rs`, `doput.rs`, `export.rs`, `registration.rs`
  and `lake_edit.rs` each hold one surface, with its tests beside it in
  `<module>_tests.rs`; `mod.rs`'s own tests are in `tests.rs`, and the fixtures the
  test modules share are in `test_support.rs`. Items moved verbatim apart from
  visibility, `use` lines and module declarations. Each DoAction arm's body moved into
  a method on the module that owns the action, and the verify preamble and response
  tail those methods repeated are now two helpers in `mod.rs`, `verify_action_payload`
  and `action_response`; every status code and message is unchanged.
