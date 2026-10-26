### Fixed

- **A biosample latitude or longitude must name a place (#718).** A latitude outside
  ±90, a longitude outside ±180, or a non-finite value (`NaN`, `Infinity`) written to
  either coordinate global is now a metadata parse error instead of a stored number,
  whether the field is keyed by display name, by internal name, or through a
  study-local alias. Other numeric fields, and values already stored, are unchanged.
