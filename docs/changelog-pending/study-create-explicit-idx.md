### Added

- **A system_admin can create a study at an explicit `study_idx` in the band reserved for
  studies from legacy Qiita.** `POST /api/v1/study` takes `study_idx` below 25000, so a study
  carried over keeps its original id. Anyone else gets a 403, and an idx already in use is a 409.
  In the CLI, `qiita study create` takes `--study-idx`, and `--owner-email` to create the study
  for another user (resolved through the admin principal lookup, so it needs a system_admin token
  carrying `admin:user`).

### Changed

- **`qiita-admin principal set-role --email` refuses a malformed address before sending any
  request (exit 2).**
- **`POST /api/v1/study` refuses a body key it does not define (422).** It used to drop the key
  and create the study without it.

### Fixed

- **`qiita study create --owner-email` and `qiita-admin principal set-role`, among others, report a
  response they do not recognise (such as one from a control plane at a different version) on one
  stderr line and exit 1, instead of printing a Python traceback.**
