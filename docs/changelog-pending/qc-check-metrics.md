### Added

- **A QC check carries validated metrics, and `verify-data-type` writes the first (#N).**
  `prep_sample_qc_check` gains a nullable `metrics` JSON object, which `persist-qc-checks`
  validates against a per-check model in `qiita_common.qc_metrics`: metrics for a check with
  no model, or that fail its model, reject the whole file and write nothing. A re-run replaces
  them, and `GET /qc-check/{prep_sample_idx}` returns `metrics` on each row. `data_type` is the
  first check with a model, and gains the reason `insufficient_reads`.
- **`verify-data-type` 1.0.0 checks a prep_sample's reads against its protocol (#N).** It
  samples up to 10,000 raw reads, classifies them with SortMeRNA against one reference per
  rRNA class (16S, 18S, 23S/28S, 5S/5.8S) and writes one `data_type` row: the share of reads
  per class, the references used, and a verdict from the prep_sample's protocol. It is
  prep_sample-scoped and admin-only; the four reference idx values go in the submit context
  (`docs/runbooks/verify-data-type.md`).
- **The runner can bind several SortMeRNA references and the sample's protocol (#N).** A
  step that declares `marker_16s_ref`, `marker_18s_ref`, `marker_23s_28s_ref` or
  `marker_5s_5_8s_ref` gets one FASTA per class from the matching `marker_*_reference_idx`
  context key; a step whose `params:` names `prep_protocol` gets the prep_sample's protocol
  name, which a submitted value cannot override.
