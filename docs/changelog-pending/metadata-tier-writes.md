### Changed

- **Study members can write biosample and prep_sample metadata and create study
  fields, within their visibility tier (#TBD).**
  A write may touch only the fields and values the caller can read, by the same rule
  the metadata reads apply.
  - `PATCH /study/{study_idx}/biosample/{biosample_idx}/metadata`,
    `PATCH /study/{study_idx}/biosample/by-unique-field/metadata` and
    `PATCH /study/{study_idx}/sequenced-sample/{sequenced_sample_idx}/metadata` now
    admit a caller with `member` tier on the study, instead of only `admin`.
  - A body naming a field whose tier is above the caller's is refused with 403 before
    anything is written. The detail names each such field and the tier it requires.
  - A write that would insert, change, or confirm a value the caller cannot read, such
    as one an exception restricts, is refused with 403 and writes nothing.
  - A collision with a value the caller cannot read, whether another study's value in a
    global field or a value stored through a `unique_in_study` field, answers that same
    403. It no longer names the other study or says whether the two values differ.
  - `POST /study/{study_idx}/biosample-field`,
    `PATCH /study/{study_idx}/biosample-field/{study_field_idx}` and their
    `prep-sample-field` twins now admit `member` instead of only `admin`. Setting
    `tier_override` still requires `admin`, and a member cannot create, link, or edit a
    field whose tier is above `member`.
  - Biosample and sequenced-sample imports still require `admin` on the study.
