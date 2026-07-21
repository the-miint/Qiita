"""ENA-study import package — batch, per-study ingestion of ENA/SRA metadata
and reads into Qiita.

TASK-01 lands the metadata resolver: `EnaResolver` (the contract),
`MiintEnaResolver` (default, D2), `HttpEnaResolver` (experimental fallback),
accession validation (`ena_import.accession`), and the `get_resolver`
backend factory. TASK-02 lands registration: `platform_mapping` /
`protocol_mapping` (ENA metadata -> qiita.platform / curated prep_protocol
name) and `registration.register_ena_study`, the composer that turns
resolved metadata into study/biosample/prep_sample/sequenced_sample rows.
Batch driving (TASK-06) and metadata harmonization (TASK-03) land in later
tickets of this epic.
"""

from .accession import EnaAccessionKind, InvalidEnaAccessionError, detect_accession_kind
from .factory import BACKEND_HTTP, BACKEND_MIINT, get_resolver
from .http_resolver import HttpEnaResolver
from .miint_resolver import MiintEnaResolver
from .platform_mapping import UnmappableEnaPlatformError, map_ena_platform
from .protocol_mapping import (
    UnmappableEnaLibraryStrategyError,
    map_ena_run_to_prep_protocol_name,
)
from .registration import (
    EnaStudyRegistrationResult,
    RunRegistrationOutcome,
    RunRegistrationStatus,
    register_ena_study,
)
from .resolver import EnaAccessionNotFoundError, EnaResolver

__all__ = [
    "BACKEND_HTTP",
    "BACKEND_MIINT",
    "EnaAccessionKind",
    "EnaAccessionNotFoundError",
    "EnaResolver",
    "EnaStudyRegistrationResult",
    "HttpEnaResolver",
    "InvalidEnaAccessionError",
    "MiintEnaResolver",
    "RunRegistrationOutcome",
    "RunRegistrationStatus",
    "UnmappableEnaLibraryStrategyError",
    "UnmappableEnaPlatformError",
    "detect_accession_kind",
    "get_resolver",
    "map_ena_platform",
    "map_ena_run_to_prep_protocol_name",
    "register_ena_study",
]
