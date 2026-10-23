### Fixed

- **golay-demux converts real amplicon runs (#686).** Its bcl-convert dummy sample sheet
  failed on every run: bcl-convert (4.5.4, the shipped image) refuses `CreateFastqForIndexReads` on a sheet
  with no index. The sheet also assumed RunInfo.xml lists reads as R1/I1/I2/R2 with
  equal template lengths, which not every run does. It now follows the run's own
  read order, gives each template read its own length, and
  declares the Golay read as a 12-cycle index with one placeholder sample whose
  index (`AAAAAAAAAAAA`) sits beyond the Golay decoder's correctable radius, so
  every decodable read still reaches Undetermined with its I1.
