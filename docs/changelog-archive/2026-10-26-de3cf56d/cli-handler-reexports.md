### Changed

- **The `cli.admin` and `cli.user` packages no longer re-export their `_handle_*`
  subcommand handlers (#775).** Each is imported from the submodule that defines it,
  as every test already did. The reviewer's R10 gains a rule that a package
  `__init__` does not re-export a submodule's private name, with a trigger-map row
  for one.
