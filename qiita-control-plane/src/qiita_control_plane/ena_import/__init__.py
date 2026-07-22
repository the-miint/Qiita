"""ENA-study import package — batch, per-study ingestion of ENA/SRA metadata
and reads into Qiita.

The metadata resolver seam: `EnaResolver` (the contract),
`MiintEnaResolver` (default, D2), `HttpEnaResolver` (experimental fallback),
accession validation (`ena_import.accession`), and the `get_resolver`
backend factory. The registration path: `platform_mapping` /
`protocol_mapping` (ENA metadata -> qiita.platform / curated prep_protocol
name) and `registration.register_ena_study`, the composer that turns
resolved metadata into study/biosample/prep_sample/sequenced_sample rows.
Metadata harmonization: `attribute_mapping` (curated ENA
sample-attribute tag -> biosample_global_field.display_name mapping) and
`harmonization.harmonize_biosample_attributes`, wired into
`registration.register_ena_study` so a newly-created biosample's ENA
attributes land on the shared checklist model. The download
workflow + CO job (`workflows/download-ena-study/1.0.0.yaml`,
`qiita_compute_orchestrator.jobs.ingest_ena_reads`) and this package's thin
submit helper (`submit.build_download_ena_study_ticket`). The batch driver
fans this out across multiple studies.
"""

from .accession import EnaAccessionKind, InvalidEnaAccessionError, detect_accession_kind
from .attribute_mapping import map_ena_attributes
from .batch import (
    BatchImportItemHandle,
    create_ena_import_batch,
    drain_running_ena_import_batches,
    fetch_batch_status,
    reconcile_inflight_batches,
    schedule_ena_import_batch,
)
from .factory import BACKEND_HTTP, BACKEND_MIINT, get_resolver
from .harmonization import HarmonizationResult, harmonize_biosample_attributes
from .http_resolver import HttpEnaResolver
from .miint_resolver import MiintEnaResolver
from .platform_mapping import UnmappableEnaPlatformError, map_ena_platform
from .protocol_mapping import (
    UnmappableEnaLibraryStrategyError,
    map_ena_run_to_prep_protocol_name,
)
from .registration import (
    CreatedPool,
    EnaStudyRegistrationResult,
    RunRegistrationOutcome,
    RunRegistrationStatus,
    register_ena_study,
)
from .resolver import EnaAccessionNotFoundError, EnaResolver
from .submit import (
    DEFAULT_DOWNLOAD_METHOD,
    DOWNLOAD_ENA_STUDY_ACTION_ID,
    DOWNLOAD_ENA_STUDY_ACTION_VERSION,
    build_download_ena_study_ticket,
)

__all__ = [
    "BACKEND_HTTP",
    "BACKEND_MIINT",
    "DEFAULT_DOWNLOAD_METHOD",
    "DOWNLOAD_ENA_STUDY_ACTION_ID",
    "DOWNLOAD_ENA_STUDY_ACTION_VERSION",
    "BatchImportItemHandle",
    "CreatedPool",
    "EnaAccessionKind",
    "EnaAccessionNotFoundError",
    "EnaResolver",
    "EnaStudyRegistrationResult",
    "HarmonizationResult",
    "HttpEnaResolver",
    "InvalidEnaAccessionError",
    "MiintEnaResolver",
    "RunRegistrationOutcome",
    "RunRegistrationStatus",
    "UnmappableEnaLibraryStrategyError",
    "UnmappableEnaPlatformError",
    "build_download_ena_study_ticket",
    "create_ena_import_batch",
    "detect_accession_kind",
    "drain_running_ena_import_batches",
    "fetch_batch_status",
    "get_resolver",
    "harmonize_biosample_attributes",
    "map_ena_attributes",
    "map_ena_platform",
    "map_ena_run_to_prep_protocol_name",
    "reconcile_inflight_batches",
    "register_ena_study",
    "schedule_ena_import_batch",
]
