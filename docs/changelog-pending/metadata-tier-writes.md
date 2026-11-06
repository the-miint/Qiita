### Changed

- **Study members can write biosample and prep_sample metadata and create study
  fields, within their visibility tier (#TBD).**
  A write may touch only the fields and values that the caller can read, by the same
  rule that the metadata reads apply.
  - `PATCH /study/{study_idx}/biosample/{biosample_idx}/metadata`,
    `PATCH /study/{study_idx}/biosample/by-unique-field/metadata` and
    `PATCH /study/{study_idx}/sequenced-sample/{sequenced_sample_idx}/metadata` now
    admit a caller with `member` tier on the study, instead of only `admin`.
  - A body naming a field whose tier is above the caller's is refused with 403 before
    anything is written. The detail names each such field and the tier that it requires.
  - A write that would insert, change, or confirm a value that the caller cannot read,
    such as one that a value-level exception restricts, is refused with 403 and writes
    nothing. The answer is the same whatever the body sent for that value.
  - That 403 replaces the 409 for another study's value in a global field, and the 422
    for a change to a value stored through a `unique_in_study` field, when the caller
    cannot read the stored value. It names neither the value nor the other study.
  - `POST /study/{study_idx}/biosample-field`,
    `PATCH /study/{study_idx}/biosample-field/{study_field_idx}` and their
    `prep-sample-field` twins now admit `member` instead of only `admin`. Setting
    `tier_override` still requires `admin`, and a member cannot create, link, or edit a
    field whose tier is above `member`.
  - Biosample and sequenced-sample imports still require `admin` on the study.
