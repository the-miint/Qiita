### Added

- **An existing biosample can be linked into another study (#N).**
  `POST /api/v1/study/{study_idx}/biosample/{biosample_idx}/link` adds the
  biosample-to-study link and returns the new row. The caller needs admin access on
  the study and must own the biosample; a wet-lab admin or system admin needs neither,
  so the wet lab can link a plate's controls into every study whose samples shared
  that plate. A biosample that is missing or that the caller does not own answers the
  same 404. A retired biosample, or a pair that already has a link (active or
  retired), answers 409. No owner-biosample-id is recorded on the new study. The CLI
  equivalent is `qiita biosample link --study-idx S --biosample-idx B`.

### Changed

- **The 403 for a sequencing run or pool the caller does not own reads "caller does not
  own …" (#N).** It read "caller did not create …"; the status and the condition are
  unchanged.
