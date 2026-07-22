"""Cross-service path-contract tests for qiita_common.api_paths.

The Rust data plane and the Python control plane share filesystem
conventions where the data plane writes and the control plane reads.
These tests pin those conventions on the Python side so a drift in
either direction surfaces as a test failure rather than a mysterious
"file not found" at workflow run time.
"""

from pathlib import Path

from qiita_common import api_paths
from qiita_common.api_paths import compute_upload_staging_path
from qiita_common.auth_constants import API_PREFIX


def test_compute_upload_staging_path_matches_rust_layout():
    """Locked layout: ``{root}/uploads/{idx}/upload.parquet``.

    Mirrors the Rust unit test ``staging_path_for_layout`` in
    ``qiita-data-plane/src/flight_service.rs``. If you change one,
    change the other in the same commit; both sides will move together
    or not at all."""
    assert compute_upload_staging_path(Path("/scratch/ephemeral/staging"), 42) == Path(
        "/scratch/ephemeral/staging/uploads/42/upload.parquet"
    )


def test_compute_upload_staging_path_accepts_relative_root():
    """A relative ``staging_root`` is preserved verbatim — used by tests
    that synthesize an isolated tmpdir-rooted staging area."""
    assert compute_upload_staging_path(Path("tmp/staging"), 7) == Path(
        "tmp/staging/uploads/7/upload.parquet"
    )


# =============================================================================
# Path-constant parity
# =============================================================================
# Every URL_FOO must equal API_PREFIX + <anchor prefix> + PATH_FOO. The mapping
# from URL_FOO to its anchor isn't always "PATH_FOO_PREFIX" — biosample and
# sequenced-sample re-anchor onto /study and /sequencing-run — so we list the
# pairings explicitly. A new route added without its triple landing here will
# fall off the parity check and is the signal to add the entry.

# (URL_*, PATH_*_PREFIX-or-anchor, PATH_*-or-suffix)
_TRIPLES: list[tuple[str, str, str]] = [
    # /reference
    ("URL_REFERENCE_BY_IDX", "PATH_REFERENCE_PREFIX", "PATH_REFERENCE_BY_IDX"),
    ("URL_REFERENCE_STATUS", "PATH_REFERENCE_PREFIX", "PATH_REFERENCE_STATUS"),
    ("URL_REFERENCE_INDEX", "PATH_REFERENCE_PREFIX", "PATH_REFERENCE_INDEX"),
    (
        "URL_REFERENCE_SHARD_INDEX_STATUS",
        "PATH_REFERENCE_PREFIX",
        "PATH_REFERENCE_SHARD_INDEX_STATUS",
    ),
    ("URL_REFERENCE_DOGET", "PATH_REFERENCE_PREFIX", "PATH_REFERENCE_DOGET"),
    ("URL_REFERENCE_EXCLUSION", "PATH_REFERENCE_PREFIX", "PATH_REFERENCE_EXCLUSION"),
    (
        "URL_REFERENCE_EXCLUSION_BY_IDX",
        "PATH_REFERENCE_PREFIX",
        "PATH_REFERENCE_EXCLUSION_BY_IDX",
    ),
    # /host-filter-profile
    (
        "URL_HOST_FILTER_PROFILE_LIST",
        "PATH_HOST_FILTER_PROFILE_PREFIX",
        "PATH_HOST_FILTER_PROFILE_ROOT",
    ),
    # /step
    ("URL_STEP_SUBMIT", "PATH_STEP_PREFIX", "PATH_STEP_SUBMIT"),
    ("URL_STEP_STATUS", "PATH_STEP_PREFIX", "PATH_STEP_STATUS"),
    ("URL_STEP_RESULT", "PATH_STEP_PREFIX", "PATH_STEP_RESULT"),
    ("URL_STEP_PLAN", "PATH_STEP_PREFIX", "PATH_STEP_PLAN"),
    ("URL_STEP_FIND_BY_NAME", "PATH_STEP_PREFIX", "PATH_STEP_FIND_BY_NAME"),
    # /reference-artifact
    (
        "URL_REFERENCE_ARTIFACT_BY_IDX",
        "PATH_REFERENCE_ARTIFACT_PREFIX",
        "PATH_REFERENCE_ARTIFACT_BY_IDX",
    ),
    # /work-ticket
    ("URL_WORK_TICKET_LIST", "PATH_WORK_TICKET_PREFIX", "PATH_WORK_TICKET_ROOT"),
    ("URL_WORK_TICKET_BY_IDX", "PATH_WORK_TICKET_PREFIX", "PATH_WORK_TICKET_BY_IDX"),
    ("URL_WORK_TICKET_RUN", "PATH_WORK_TICKET_PREFIX", "PATH_WORK_TICKET_RUN"),
    ("URL_WORK_TICKET_STEP_LOGS", "PATH_WORK_TICKET_PREFIX", "PATH_WORK_TICKET_STEP_LOGS"),
    # /upload
    ("URL_UPLOAD_BY_IDX", "PATH_UPLOAD_PREFIX", "PATH_UPLOAD_BY_IDX"),
    ("URL_UPLOAD_DONE", "PATH_UPLOAD_PREFIX", "PATH_UPLOAD_DONE"),
    # /sequence-range
    (
        "URL_SEQUENCE_RANGE_BY_PREP_SAMPLE",
        "PATH_SEQUENCE_RANGE_PREFIX",
        "PATH_SEQUENCE_RANGE_BY_PREP_SAMPLE",
    ),
    # /auth
    ("URL_AUTH_WHOAMI", "PATH_AUTH_PREFIX", "PATH_AUTH_WHOAMI"),
    ("URL_AUTH_PAT", "PATH_AUTH_PREFIX", "PATH_AUTH_PAT"),
    ("URL_AUTH_TOKEN", "PATH_AUTH_PREFIX", "PATH_AUTH_TOKEN"),
    ("URL_AUTH_TOKEN_BY_IDX", "PATH_AUTH_PREFIX", "PATH_AUTH_TOKEN_BY_IDX"),
    ("URL_AUTH_LOGIN", "PATH_AUTH_PREFIX", "PATH_AUTH_LOGIN"),
    ("URL_AUTH_HANDOFF", "PATH_AUTH_PREFIX", "PATH_AUTH_HANDOFF"),
    ("URL_AUTH_CLI_EXCHANGE", "PATH_AUTH_PREFIX", "PATH_AUTH_CLI_EXCHANGE"),
    # /admin
    ("URL_ADMIN_SERVICE_ACCOUNT", "PATH_ADMIN_PREFIX", "PATH_ADMIN_SERVICE_ACCOUNT"),
    (
        "URL_ADMIN_PRINCIPAL_DISABLED",
        "PATH_ADMIN_PREFIX",
        "PATH_ADMIN_PRINCIPAL_DISABLED",
    ),
    (
        "URL_ADMIN_PRINCIPAL_RETIRED",
        "PATH_ADMIN_PREFIX",
        "PATH_ADMIN_PRINCIPAL_RETIRED",
    ),
    (
        "URL_ADMIN_PRINCIPAL_SYSTEM_ROLE",
        "PATH_ADMIN_PREFIX",
        "PATH_ADMIN_PRINCIPAL_SYSTEM_ROLE",
    ),
    ("URL_ADMIN_AUDIT", "PATH_ADMIN_PREFIX", "PATH_ADMIN_AUDIT"),
    (
        "URL_ADMIN_PRINCIPAL_REVOKE_ALL_TOKENS",
        "PATH_ADMIN_PREFIX",
        "PATH_ADMIN_PRINCIPAL_REVOKE_ALL_TOKENS",
    ),
    (
        "URL_ADMIN_STUDY_OWNER_BIOSAMPLE_ID",
        "PATH_ADMIN_PREFIX",
        "PATH_ADMIN_STUDY_OWNER_BIOSAMPLE_ID",
    ),
    (
        "URL_ADMIN_SEQUENCED_POOL_MASKED_READ_EXPORT",
        "PATH_ADMIN_PREFIX",
        "PATH_ADMIN_SEQUENCED_POOL_MASKED_READ_EXPORT",
    ),
    (
        "URL_ADMIN_MASKED_READ_EXPORT_TICKET",
        "PATH_ADMIN_PREFIX",
        "PATH_ADMIN_MASKED_READ_EXPORT_TICKET",
    ),
    # /user
    ("URL_USER_ME", "PATH_USER_PREFIX", "PATH_USER_ME"),
    # /study
    ("URL_STUDY_BY_IDX", "PATH_STUDY_PREFIX", "PATH_STUDY_BY_IDX"),
    (
        "URL_STUDY_LOOKUP_BY_ACCESSION",
        "PATH_STUDY_PREFIX",
        "PATH_STUDY_LOOKUP_BY_ACCESSION",
    ),
    # /sequencing-run
    (
        "URL_SEQUENCING_RUN_BY_IDX",
        "PATH_SEQUENCING_RUN_PREFIX",
        "PATH_SEQUENCING_RUN_BY_IDX",
    ),
    (
        "URL_SEQUENCING_RUN_LOOKUP_BY_INSTRUMENT_RUN_ID",
        "PATH_SEQUENCING_RUN_PREFIX",
        "PATH_SEQUENCING_RUN_LOOKUP_BY_INSTRUMENT_RUN_ID",
    ),
    (
        "URL_SEQUENCING_RUN_SEQUENCED_POOL",
        "PATH_SEQUENCING_RUN_PREFIX",
        "PATH_SEQUENCING_RUN_SEQUENCED_POOL",
    ),
    (
        "URL_SEQUENCED_POOL_PREFLIGHT",
        "PATH_SEQUENCING_RUN_PREFIX",
        "PATH_SEQUENCED_POOL_PREFLIGHT",
    ),
    (
        "URL_SEQUENCED_POOL_PREFLIGHT_UPDATE_LANE",
        "PATH_SEQUENCING_RUN_PREFIX",
        "PATH_SEQUENCED_POOL_PREFLIGHT_UPDATE_LANE",
    ),
    (
        "URL_SEQUENCED_POOL_BY_IDX",
        "PATH_SEQUENCING_RUN_PREFIX",
        "PATH_SEQUENCED_POOL_BY_IDX",
    ),
    (
        "URL_SEQUENCED_POOL_QC_REPORT",
        "PATH_SEQUENCING_RUN_PREFIX",
        "PATH_SEQUENCED_POOL_QC_REPORT",
    ),
    (
        "URL_SEQUENCED_POOL_COMPLETION",
        "PATH_SEQUENCING_RUN_PREFIX",
        "PATH_SEQUENCED_POOL_COMPLETION",
    ),
    (
        "URL_SEQUENCED_POOL_BLOCK_MASK_PLAN",
        "PATH_SEQUENCING_RUN_PREFIX",
        "PATH_SEQUENCED_POOL_BLOCK_MASK_PLAN",
    ),
    (
        "URL_SEQUENCED_POOL_ALIGN_PLAN",
        "PATH_SEQUENCING_RUN_PREFIX",
        "PATH_SEQUENCED_POOL_ALIGN_PLAN",
    ),
    # /biosample — three routes; two re-anchor on /study, one on /biosample
    ("URL_BIOSAMPLE_BY_STUDY", "PATH_STUDY_PREFIX", "PATH_BIOSAMPLE_BY_STUDY"),
    (
        "URL_BIOSAMPLE_LIST_BY_STUDY",
        "PATH_STUDY_PREFIX",
        "PATH_BIOSAMPLE_LIST_BY_STUDY",
    ),
    ("URL_BIOSAMPLE_BY_IDX", "PATH_BIOSAMPLE_PREFIX", "PATH_BIOSAMPLE_BY_IDX"),
    (
        "URL_BIOSAMPLE_LOOKUP_BY_ACCESSION",
        "PATH_BIOSAMPLE_PREFIX",
        "PATH_BIOSAMPLE_LOOKUP_BY_ACCESSION",
    ),
    (
        "URL_BIOSAMPLE_LOOKUP_BY_MATRIX_TUBE_ID",
        "PATH_BIOSAMPLE_PREFIX",
        "PATH_BIOSAMPLE_LOOKUP_BY_MATRIX_TUBE_ID",
    ),
    # /sequenced-sample — re-anchored on /sequencing-run, /study, /sequenced-sample
    (
        "URL_SEQUENCED_SAMPLE_FROM_RUN",
        "PATH_SEQUENCING_RUN_PREFIX",
        "PATH_SEQUENCED_SAMPLE_FROM_RUN",
    ),
    (
        "URL_SEQUENCED_SAMPLE_LIST_BY_RUN",
        "PATH_SEQUENCING_RUN_PREFIX",
        "PATH_SEQUENCED_SAMPLE_LIST_BY_RUN",
    ),
    (
        "URL_SEQUENCED_SAMPLE_LIST_BY_RUN_FULL",
        "PATH_SEQUENCING_RUN_PREFIX",
        "PATH_SEQUENCED_SAMPLE_LIST_BY_RUN_FULL",
    ),
    (
        "URL_SEQUENCED_SAMPLE_LIST_BY_STUDY",
        "PATH_STUDY_PREFIX",
        "PATH_SEQUENCED_SAMPLE_LIST_BY_STUDY",
    ),
    (
        "URL_SEQUENCED_SAMPLE_LIST_BY_POOL",
        "PATH_SEQUENCING_RUN_PREFIX",
        "PATH_SEQUENCED_SAMPLE_LIST_BY_POOL",
    ),
    (
        "URL_SEQUENCED_SAMPLE_BY_IDX",
        "PATH_SEQUENCED_SAMPLE_PREFIX",
        "PATH_SEQUENCED_SAMPLE_BY_IDX",
    ),
    # /prep-sample
    (
        "URL_PREP_SAMPLE_STUDY_LIST",
        "PATH_PREP_SAMPLE_PREFIX",
        "PATH_PREP_SAMPLE_STUDY_LIST",
    ),
    (
        "URL_PREP_SAMPLE_RETIRED",
        "PATH_PREP_SAMPLE_PREFIX",
        "PATH_PREP_SAMPLE_RETIRED",
    ),
    # /read-masked
    ("URL_READ_MASKED_DOGET", "PATH_READ_MASKED_PREFIX", "PATH_READ_MASKED_DOGET"),
    # /mask-definition
    (
        "URL_MASK_DEFINITION_BY_IDX",
        "PATH_MASK_DEFINITION_PREFIX",
        "PATH_MASK_DEFINITION_BY_IDX",
    ),
    # /alignment-definition
    (
        "URL_ALIGNMENT_DEFINITION_BY_IDX",
        "PATH_ALIGNMENT_DEFINITION_PREFIX",
        "PATH_ALIGNMENT_DEFINITION_BY_IDX",
    ),
    # /alignment (Flight DoGet ticket for the alignment sink)
    ("URL_ALIGNMENT_DOGET", "PATH_ALIGNMENT_PREFIX", "PATH_ALIGNMENT_DOGET"),
]


def test_url_path_triples_compose():
    """Every URL_* equals ``API_PREFIX + <anchor>_PREFIX + PATH_*``.

    Tripping this means either the URL_ string was written out by hand
    (not composed via f-string from the PATH_ pieces) or the routing
    anchor in ``_TRIPLES`` is wrong. Fix the composition; don't tweak
    the expectation."""
    for url_name, prefix_name, path_name in _TRIPLES:
        url_value = getattr(api_paths, url_name)
        prefix_value = getattr(api_paths, prefix_name)
        path_value = getattr(api_paths, path_name)
        expected = f"{API_PREFIX}{prefix_value}{path_value}"
        assert url_value == expected, (
            f"{url_name} = {url_value!r}, expected {expected!r} "
            f"({API_PREFIX!r} + {prefix_name}={prefix_value!r} + {path_name}={path_value!r})"
        )


def test_every_url_constant_is_registered():
    """Every non-prefix ``URL_*`` exposed by ``api_paths`` must appear in
    ``_TRIPLES`` (or be a ``URL_*_PREFIX`` covered by the sibling test).

    Without this guard, adding ``URL_NEW_ROUTE`` and forgetting to register
    its triple here silently bypasses ``test_url_path_triples_compose`` —
    the very drift mode the parity setup is supposed to catch."""
    registered = {url_name for url_name, _prefix, _path in _TRIPLES}
    declared = {
        name for name in dir(api_paths) if name.startswith("URL_") and not name.endswith("_PREFIX")
    }
    missing = declared - registered
    assert not missing, (
        f"URL_* constants exist but are not in _TRIPLES: {sorted(missing)}. "
        "Add an entry to _TRIPLES in this file so the per-route parity check "
        "exercises the new constant."
    )


def test_url_prefixes_compose():
    """Every URL_*_PREFIX equals ``API_PREFIX + PATH_*_PREFIX``.

    A simpler version of the triple check above for the "prefix itself
    is a usable URL" case (POST against the root of a router)."""
    prefix_pairs = [
        name[len("PATH_") :].removesuffix("_PREFIX")
        for name in dir(api_paths)
        if name.startswith("PATH_") and name.endswith("_PREFIX")
    ]
    for tag in prefix_pairs:
        path_prefix = getattr(api_paths, f"PATH_{tag}_PREFIX")
        url_prefix = getattr(api_paths, f"URL_{tag}_PREFIX")
        expected = f"{API_PREFIX}{path_prefix}"
        assert url_prefix == expected, (
            f"URL_{tag}_PREFIX = {url_prefix!r}, expected {expected!r} "
            f"(API_PREFIX + PATH_{tag}_PREFIX={path_prefix!r})"
        )
