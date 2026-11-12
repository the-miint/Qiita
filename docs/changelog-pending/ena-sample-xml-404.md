### Fixed

- **An ENA run-list import no longer fails when ENA has no record of a study's biosamples (#843).**
  ENA can serve an NCBI study's runs while holding no record of their biosamples; its sample
  XML then answers 404 for a request whose biosamples are all missing, which failed the whole
  study. Those biosamples now register with no ENA sample attributes, like a biosample that has
  none, and the control plane logs each one. Only ENA's own not-found 404 is absorbed; any other
  404 still fails the import. Biosamples ENA does hold keep their attributes.
