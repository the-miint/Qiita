### Added

- **A run feature table over REST.** `POST /feature-table/{sample,value,feature}`
  answers one cohort request — one or more amplicon runs over a study's samples or a
  named prep_sample list — with the table's columns (each processed sample under its
  public name), its values as sparse COO `(sample, feature, value)`, and its features
  `(feature, sequence, length)`: an ASV's md5, its sequence, its length. Values and
  features are Arrow IPC streams; access is all-or-nothing over the cohort, checked
  before the data plane is read. `qiita feature-table runs` writes the three as a
  bundle with the map needed to read it.
