### Changed

- **Biosample and prep_sample metadata reads now honor each field's and each value's
  visibility tier (#779).**
  A value is returned only when the caller's tier on the study that wrote it reaches
  the tier the value requires. That tier comes from the global field's `default_tier`,
  a study-local field's `tier_override`, or the study's `default_tier`, and a
  `*_field_exception` row on that one value can increase it. A value the caller may not
  read is absent from `global_metadata` / `local_metadata`, with no placeholder.
  wet_lab_admin and system_admin read every value.
  - `GET /study/{study_idx}/biosample/{biosample_idx}`,
    `POST /study/{study_idx}/biosample/by-unique-field` and
    `GET /study/{study_idx}/sequenced-sample/{sequenced_sample_idx}` now admit a caller
    with `viewer` tier on the study, instead of only `admin`. A viewer sees the
    `public`- and `viewer`-tier values, and a member also sees `member`-tier values,
    including the owner-biosample-id.
  - An owner-biosample-id value requires `member` or above even when its study
    declared the field at a lower tier before the first import.
  - `last_metadata_change_at` on the biosample and sequenced-sample reads is now when
    the metadata values in that response last changed. `updated_at` and
    `effective_updated_at` no longer move when metadata is written, and the ETag is the
    later of the two. A write to a value the caller cannot read moves none of them.
  - The study-scoped reads' ETag also counts that study's local values, so it is no
    longer an If-Match for `PATCH /biosample/{biosample_idx}` or
    `PATCH /sequenced-sample/{sequenced_sample_idx}`; take that from the GET on the
    PATCH's own path.
  - A lookup by a unique-in-study value the caller may not read answers 404, in the
    same words as a value that matches nothing.
  - `GET /biosample/{biosample_idx}` judges each global value on the study that wrote
    it, so a caller with a grant on one of the biosample's studies no longer sees
    another study's restricted values. The biosample's owner reads at public on any
    study they neither own nor hold a grant on.
  - Metadata writes and field creation still require `admin` on the study.
