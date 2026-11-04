### Changed

- **A biosample is now recognised as a control from its `qiita sample type`, ahead of its
  `host taxon id` (#690).** `qiita_common/data/control_types.yml` classifies every
  `Qiita Sample Type` term as `negative`, `positive` or `not_control`: `control_blank`,
  `control_sterile_water` and `control_sea_water` are negative, `control_spike_in` is
  positive. A biosample with a control term is a control for host filtering whatever its
  `host taxon id`, and a negative control that yields zero reads is treated as an expected
  empty blank rather than a failure; a positive control that yields zero reads still
  fails. A `qiita sample type` the file does not list resolves UNRESOLVED and names the
  term. `qiita-admin backfill host-taxon-id` writes `missing: control sample` for negative
  controls found in metadata too.
