### Fixed

- **An ENA run-list import no longer fails when ENA has none of a study's samples.** ENA can
  serve an NCBI study's runs while holding no record of their samples; its sample XML then
  answers 404 for a request whose samples are all missing, which failed the whole study.
  Those samples now register with no ENA sample attributes, like a sample that has none, and
  the control plane logs each one. Samples ENA does hold keep their attributes.
