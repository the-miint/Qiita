"""Terminology models: the release-load lifecycle status and its allowed
transitions, term obsoletion kinds, the staging manifest contract, and the
projection of a terminology row."""

from enum import StrEnum
from typing import Annotated

from pydantic import AwareDatetime, BaseModel, Field

from qiita_common.auth_constants import MAX_NAME_LENGTH, MAX_VERSION_LENGTH


class TerminologyStatus(StrEnum):
    """Lifecycle states of a terminology row.

    Mirrors the Postgres `qiita.terminology_status` enum. `loading` while a
    load is in flight; `active` when the load is complete and the row
    reflects a consistent terminology version; `failed` when a load aborted
    and the row's contents may be inconsistent with the source.
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


class TerminologyManifestSource(BaseModel):
    """One source file declared by a terminology manifest.

    `path` is relative to the staging directory holding the manifest;
    `sha256` is the lowercase hex digest of the file's bytes, carrying no
    `sha256:` prefix.
    """

    path: str = Field(min_length=1)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class TerminologyManifest(BaseModel):
    """Operator-supplied description of one terminology release staged for
    load, read from `<staging_dir>/manifest.json`. Carries the (name,
    version) pair the load applies and the source file whose checksum must
    verify before the release is extracted.
    """

    name: str = Field(min_length=1, max_length=MAX_NAME_LENGTH)
    version: str = Field(min_length=1, max_length=MAX_VERSION_LENGTH)
    source: TerminologyManifestSource


class TerminologyResponse(BaseModel):
    terminology_idx: Annotated[int, Field(gt=0)]
    name: str
    version: str
    status: TerminologyStatus
    loaded_at: AwareDatetime


# A terminology's `active` state is not terminal: a reload of the same
# terminology drops the row back to `loading` for the duration of the new
# load, and `failed` -> `loading` retries a failed load in place without
# deleting term rows that may already be referenced.
VALID_TERMINOLOGY_STATUS_TRANSITIONS: dict[TerminologyStatus, set[TerminologyStatus]] = {
    TerminologyStatus.LOADING: {TerminologyStatus.ACTIVE, TerminologyStatus.FAILED},
    TerminologyStatus.ACTIVE: {TerminologyStatus.LOADING},
    TerminologyStatus.FAILED: {TerminologyStatus.LOADING},
}
