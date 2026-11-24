### Added

- **`POST /study/{study_idx}/biosample/bulk-link` links many existing biosamples into a
  study at once (#939).** The body names one identifier kind (`export_entity_id`, `idx`,
  `biosample_accession`, `ena_sample_accession` or `matrix_tube_id`) and up to 2,000 values
  of it, none repeated. Access is that of
  `POST /study/{study_idx}/biosample/{biosample_idx}/link`, applied to every biosample. The
  request is all or nothing: a value that names no biosample, one that the caller does not
  own, a retired biosample, or one whose link to the study is retired refuses the whole
  request with a 422 listing every such value. Otherwise the response lists each value as
  `linked` or `already_linked`, with 201 when any link was created and 200 when none was.
  A 503 with `Retry-After` means that a named biosample's identifier changed while the
  request ran; nothing was linked, and the identical request can be resent.
- **`qiita biosample bulk-link` links into a study every biosample that a TSV file's
  column names (#939).** `--key` names the identifier kind (`export_entity_id` by default),
  and the file's column with that exact header holds the values; other columns are
  ignored. A blank cell, a repeated value, more than 2,000 values, or a file whose data
  lines are not one row each (an empty line between rows, or a line break inside a cell)
  is refused before anything is sent. The command prints which values were linked and
  which were already linked; if any value cannot be linked, it lists every such value and
  links none.

### Changed

- **`POST /study/{study_idx}/biosample/{biosample_idx}/link` answers 403 for a biosample
  that the caller does not own, and 200 for a link that already exists (#939).** A caller who
  passes the study check but does not own a live biosample now gets 403 instead of 404; one
  who does not own a retired biosample still gets 404. When the pair is already actively
  linked, the route answers 200 with that link, as first created, instead of 409; a retired
  link for the pair is still refused with 409.
- **`qiita biosample link` says on stderr when the link already existed (#939).** Its JSON
  output is unchanged.
