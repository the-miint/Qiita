### Changed

- **The data plane's Flight service is split into modules, with no behaviour change
  (#TBD).** `qiita-data-plane/src/flight_service.rs` and its test file are now the
  `flight_service/` module directory: `mod.rs` holds the service and the `do_action`
  dispatcher, and `doget.rs`, `query.rs`, `doput.rs`, `export.rs`, `registration.rs`
  and `delete.rs` each hold one surface, with its tests beside it in
  `<module>_tests.rs`; `mod.rs`'s own tests are in `tests.rs`, and the fixtures the
  test modules share are in `test_support.rs`. Items moved verbatim; only visibility,
  `use` lines and module declarations changed, and each DoAction arm's body moved into
  a method on the module that owns the action.
