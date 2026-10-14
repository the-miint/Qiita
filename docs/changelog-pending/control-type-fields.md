### Added

- **`negative control type` and `positive control type` biosample fields (#690).**
  `negative control type` takes the MIxS negative-control values plus `extraction blank`
  and `no-template control`; `positive control type` is free text.

### Changed

- **A biosample is now recognised as a control from its own metadata, ahead of its
  `host taxon id` (#690).** A real value on either new field, or a control
  `qiita sample type` term (`control_blank`, `control_sterile_water`,
  `control_sea_water`, `control_spike_in`), makes the biosample a control for host
  filtering whatever its `host taxon id`; a missing-value reason does not count. A
  negative control (`negative control type`, or one of the first three sample types)
  that yields zero reads is treated as an expected empty blank rather than a failure; a
  positive control that yields zero reads still fails.
  `qiita-admin backfill host-taxon-id` writes `missing: control sample` for negative
  controls found in metadata too.
