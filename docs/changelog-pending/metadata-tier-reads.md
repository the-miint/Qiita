### Changed

- **Biosample and prep_sample metadata reads now honour each field's and each value's
  visibility tier.**
  A value is returned only when the caller's tier on the study that wrote it reaches
  the tier the value requires. That tier comes from the global field's `default_tier`,
  a study-local field's `tier_override`, or the study's `default_tier`, and a
  `*_field_exception` row on that one value can raise it. A value the caller may not
  read is absent from `global_metadata` / `local_metadata`, with no placeholder.
  wet_lab_admin and system_admin read every value.
  - `GET /study/{study_idx}/biosample/{biosample_idx}`,
    `POST /study/{study_idx}/biosample/by-unique-field` and
    `GET /study/{study_idx}/sequenced-sample/{sequenced_sample_idx}` now admit a caller
    with `viewer` tier on the study, instead of only `admin`. A viewer sees the
    `public`- and `viewer`-tier values, and a member also sees `member`-tier values,
    including the owner-biosample-id.
  - A lookup by a unique-in-study value the caller may not read answers 404, in the
    same words as a value that matches nothing.
  - `GET /biosample/{biosample_idx}` judges each global value on the study that wrote
    it, so a caller with a grant on one of the biosample's studies no longer sees
    another study's restricted values. The biosample's owner reads at public on any
    study they neither own nor hold a grant on.
  - Metadata writes and field creation still require `admin` on the study.
