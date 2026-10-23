### Fixed

- **A live mask or assembly row can no longer hold part of its deprecation or invalidation
  provenance (#728).** The CHECKs on `mask_definition`, `mask_sample`, `processing` and
  `assembly_sample` accepted a live row with one or two of the three fields set; they now
  require none when live and all three when withdrawn.
