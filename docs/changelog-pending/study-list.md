### Added

- **List the studies you can read: `GET /api/v1/study` and `qiita study list` (#660).**
  Returns owned studies, studies shared with you at any tier, and public-default
  studies (every study for wet_lab_admin+), newest first, each with your
  `caller_tier`, why you can read it (`access_via`), and which view of its record
  you get (`record_view`). Filters: full-text `q` (a summary reader matches only the
  summary fields; accessions are searchable), `min_tier` on your own tier, and cursor
  paging. The CLI defaults to your own and shared studies; `--min-tier public` widens
  it to public studies. Someone granted access no longer needs to be told a study's idx to find it.

### Changed

- **A grant below a study's `default_tier` reads the study's summary (#660).**
  `GET /api/v1/study/{idx}` previously answered 403 to any caller whose tier was
  below the study's `default_tier` (`member` unless set). It now returns them a
  summary — title, alias, accessions, tiers, `updated_at`, marked `"view":
  "summary"` — so a study shared with someone can be found and recognised; the full
  record still requires a tier at or above `default_tier`, and carries `"view":
  "full"` (on POST and PATCH responses too). What each tier reads is defined in
  `docs/architecture/data-model.md`.
