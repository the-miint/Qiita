"""ENA-study import package — batch, per-study ingestion of ENA/SRA metadata
and reads into Qiita.

TASK-01 lands the metadata resolver: `EnaResolver` (the contract),
`MiintEnaResolver` (default, D2), `HttpEnaResolver` (experimental fallback),
accession validation (`ena_import.accession`), and the `get_resolver`
backend factory. Batch driving / registration land in later tickets of this
epic.
"""

from .accession import EnaAccessionKind, InvalidEnaAccessionError, detect_accession_kind
from .factory import BACKEND_HTTP, BACKEND_MIINT, get_resolver
from .http_resolver import HttpEnaResolver
from .miint_resolver import MiintEnaResolver
from .resolver import EnaAccessionNotFoundError, EnaResolver

__all__ = [
    "BACKEND_HTTP",
    "BACKEND_MIINT",
    "EnaAccessionKind",
    "EnaAccessionNotFoundError",
    "EnaResolver",
    "HttpEnaResolver",
    "InvalidEnaAccessionError",
    "MiintEnaResolver",
    "detect_accession_kind",
    "get_resolver",
]
