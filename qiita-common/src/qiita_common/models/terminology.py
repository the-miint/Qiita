"""Terminology models: the contract a staged release describes itself by, and
the closed value sets a terminology row and its terms are constrained to."""

from enum import StrEnum
from typing import Annotated

from pydantic import AwareDatetime, BaseModel, Field

from qiita_common.auth_constants import MAX_NAME_LENGTH

# Mirrors qiita.terminology.version VARCHAR(50); note that column's own
# comment mistakenly offers an ontology version IRI as an example, which
# does not fit in 50, but seems not worth a db migration to correct.
MAX_TERMINOLOGY_VERSION_LENGTH = 50

# The two strings that identify a release, named so one set of bounds governs
# every point a name or version is accepted rather than only the manifest.
TerminologyName = Annotated[str, Field(min_length=1, max_length=MAX_NAME_LENGTH)]
TerminologyVersion = Annotated[str, Field(min_length=1, max_length=MAX_TERMINOLOGY_VERSION_LENGTH)]


class TerminologyStatus(StrEnum):
    """Lifecycle states of a terminology row.

    Mirrors the Postgres `qiita.terminology_status` enum. `loading` while a
    load is in flight; `active` when the load is complete and the row
    reflects a consistent terminology version; `failed` when a load aborted
    and the row's contents may be inconsistent with the source.

    A load currently applies as a single transaction, so only `active` is
    ever observable outside it: `loading` is rolled back or superseded
    before commit, and an aborted load restores the prior contents rather
    than committing `failed`. Both values are kept against a load design
    that can commit a partial state. The Postgres column comment describes
    the broader contract; reconciling it needs a migration, deferred until
    that design settles.
    """

    LOADING = "loading"
    ACTIVE = "active"
    FAILED = "failed"


class TerminologyTermObsoletionKind(StrEnum):
    """Reason a terminology_term row was marked obsolete on the most
    recent load.

    Mirrors the Postgres `qiita.terminology_term_obsoletion_kind` enum.
    `source_deprecated` when the source vocabulary deprecates the term;
    `source_merged` when the source merges this term into another;
    `silently_dropped` when the term disappears from a reload without a
    recorded replacement.
    """

    SOURCE_DEPRECATED = "source_deprecated"
    SOURCE_MERGED = "source_merged"
    SILENTLY_DROPPED = "silently_dropped"


class TerminologyManifestFile(BaseModel):
    """One file declared by a terminology manifest.

    `path` is relative to the staging directory holding the manifest;
    `sha256` is the lowercase hex digest of the file's bytes, carrying no
    `sha256:` prefix.
    """

    path: str = Field(min_length=1)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class TerminologyManifest(BaseModel):
    """Description of one terminology release staged for load, read from
    `<staging_dir>/manifest.json`.

    Carries the (name, version) pair the load applies and the two release
    tables it reads, each with the checksum that must verify before the
    tables are parsed. Nothing describing how the tables were produced is
    recorded here — the manifest declares only what the load consumes.
    """

    name: TerminologyName
    version: TerminologyVersion
    terms: TerminologyManifestFile
    closure: TerminologyManifestFile


class TerminologyResponse(BaseModel):
    terminology_idx: Annotated[int, Field(gt=0)]
    name: str
    version: str
    status: TerminologyStatus
    loaded_at: AwareDatetime


# A terminology's `active` state is not terminal: a reload of the same
# terminology drops the row back to `loading` for the duration of the new
# load, and `failed` -> `loading` retries a failed load in place without
# deleting term rows that may already be referenced. The `failed` edges are
# unreachable while a load is one transaction — see TerminologyStatus.
VALID_TERMINOLOGY_STATUS_TRANSITIONS: dict[TerminologyStatus, set[TerminologyStatus]] = {
    TerminologyStatus.LOADING: {TerminologyStatus.ACTIVE, TerminologyStatus.FAILED},
    TerminologyStatus.ACTIVE: {TerminologyStatus.LOADING},
    TerminologyStatus.FAILED: {TerminologyStatus.LOADING},
}
