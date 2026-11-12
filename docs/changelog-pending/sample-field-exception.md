### Added

- **A study admin can restrict one biosample's or prep_sample's value of one field (#TBD).**
  `GET`, `PUT` and `DELETE /api/v1/study/{study_idx}/biosample/{biosample_idx}/field-exception/{study_field_idx}`,
  and the same under `sequenced-sample/{sequenced_sample_idx}`, read, set and clear the
  exception that increases the tier that the value requires above its field's own,
  for example to hide one free-text value into which a submitter typed personal
  information. They need admin on the study, or a wet-lab admin or higher role. The
  exception records who first set it (`created_by_idx`) and who set its current tier
  and reason (`updated_by_idx`).
  - On a field linked to a global field, the exception follows the value into every
    study that links that field. When another study wrote the value, the caller also
    needs admin on that study, for all three verbs.
  - A field linked to a global field that holds no value on that biosample or
    prep_sample yet refuses an exception (409), and a tier at or below the field's own
    is refused as restricting nothing (422). A reason is required, up to 2000
    characters.
  - A `PUT` without `If-Match` only creates, answering 428 when an exception is already
    set; with `If-Match` it only replaces. `DELETE` needs `If-Match`. The `GET` returns
    the `ETag` that a replace or clear sends as `If-Match`.
  - The CLI equivalents are `qiita biosample get-field-exception`,
    `set-field-exception` and `clear-field-exception`, with `--study-idx`,
    `--biosample-idx` and `--study-field-idx`, and the same under `qiita
    sequenced-sample` with `--sequenced-sample-idx`. `set` takes `--tier-override` and
    `--reason`, and stops at an exception already set unless given `--replace`.
    `clear` prints the exception that it removed.

### Changed

- **A field exception and `unique_in_study` exclude each other (#TBD).** An
  exception on a `unique_in_study` field is refused, and so is declaring
  `unique_in_study` on a field that exceptions are set on (409 from the field `PATCH`).
  A write refused as a duplicate on a `unique_in_study` field therefore no longer
  shows the writer that a value hidden from them equals the one they sent.
