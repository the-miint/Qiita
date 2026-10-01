"""ENA submission package — depositing Qiita studies and biosamples to ENA.

Layers: the miint I/O boundary (`catalog`, a prepared Webin V2 submission
session over DuckDB's `ena` catalog).
"""

from .catalog import EnaSubmissionCatalog, EnaSubmissionError

__all__ = [
    "EnaSubmissionCatalog",
    "EnaSubmissionError",
]
