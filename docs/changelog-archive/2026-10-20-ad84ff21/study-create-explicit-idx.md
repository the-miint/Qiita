### Added

- **A system_admin can create a study at an explicit `study_idx` in the band reserved for studies
  from legacy Qiita (#711).** `POST /api/v1/study` takes `study_idx` below 25000, so a study
  carried over keeps its original id. Anyone else gets a 403, and an idx already in use is a 409.
  In the CLI, `qiita study create` takes `--study-idx`, and `--owner-email` to create the study
  for another user (resolved through the admin principal lookup, so it needs a system_admin token
  carrying `admin:user`). `GET /api/v1/study` lists studies by `study_idx`, highest first, so a
  study created at a legacy `study_idx` comes after every study minted at 25000 and up.

### Changed

- **`qiita-admin principal set-role --email` refuses a malformed address before sending any
  request (exit 2) (#711).**
- **`POST /api/v1/study` refuses a body key it does not define (422) (#711).** It used to drop the
  key and create the study without it.

### Fixed

- **`qiita-admin principal set-role` and `qiita mask syndna-read-count`, among others, report a
  response they do not recognise (such as one from a control plane at a different version) on one
  stderr line and exit 1 (#711).** They printed a Python traceback, or for `syndna-read-count`
  pydantic's multi-line error.
- **Eight CLI commands report HTTP and connection errors with the messages the CLI's other
  control-plane commands use (#711).** A 403 from a token that predates a newly granted scope now
  prompts `qiita login`, and an unreachable control plane is one stderr line naming the URL it
  tried. `qiita submit-reads`, `qiita reference load`, `qiita reference export`, and
  `qiita-admin`'s `masked-read-export` and `owner-biosample-id` printed a Python traceback when
  the control plane was unreachable. `qiita mask syndna-read-count`, `qiita feature-table build`
  and `qiita assembly export` printed one line, without the URL.
- **`qiita-admin masked-read-export` reports a failure during an export as an error message and
  exits 1 (#711).** It printed a Python traceback when the data plane refused a prep_sample's
  stream or cut it partway, when writing a file failed, when an existing parquet file could not be
  read, when pyarrow refused `--data-plane-url`, and, for fastq, when miint could not be installed
  or loaded. Every file already written is complete. Its messages now count prep_samples, where
  they said samples.
