"""Upload: generic Arrow-data staging slots.

The upload domain is content-agnostic on purpose — no reference_idx, no
role enum. A `qiita.upload` row is a handle on staged bytes; the workflow
that references the handle in its `action_context` is what knows what
the upload IS.
"""

from enum import StrEnum
from typing import Annotated

from pydantic import AwareDatetime, BaseModel, Field

from qiita_common.auth_constants import MAX_NAME_LENGTH


class UploadStatus(StrEnum):
    """Mirrored by the `upload.status` CHECK constraint in
    db/migrations/20260521000000_upload.sql. Stored as TEXT/CHECK, not a
    Postgres ENUM — same carve-out as ReferenceStatus and AuthEventType;
    see CLAUDE.md "Enum parity". Keep both sides in sync by hand."""

    PENDING = "pending"
    READY = "ready"
    CONSUMED = "consumed"
    FAILED = "failed"


class EmailReceiptStatus(StrEnum):
    """Delivery lifecycle of a qiita.email_receipt row.

    Mirrored DB-side by the `email_receipt.status` CHECK constraint. Stored as
    TEXT/CHECK, not a Postgres ENUM — same carve-out as UploadStatus /
    ReferenceStatus / AuthEventType; see CLAUDE.md "Enum parity". Keep both sides
    in sync by hand (a light StrEnum↔CHECK parity test guards drift).

    PENDING is written before the transport send; SENT/FAILED record the
    outcome. DEAD_LETTER is a terminal give-up after NOTIFY_MAX_ATTEMPTS failed
    sends — the sweeper stamps the ticket notified and stops retrying.
    """

    PENDING = "pending"
    SENT = "sent"
    FAILED = "failed"
    DEAD_LETTER = "dead_letter"


class UploadCreateRequest(BaseModel):
    """Body for POST /api/v1/upload.

    `description` is free-form audit text — optional. The slot itself has
    no consumer-specific fields; binding to a reference / study / etc.
    happens later via the work_ticket that references the upload_idx.

    `source_filename` is the basename of the file the client is about to
    stream. Also optional, and descriptive like `sha256` — nothing opens it.
    The work_ticket submit gate reads it so the fastq filename-prefix rule
    (basename carries the prep_sample's sequenced_pool_item_id) applies to an
    upload-fed submission as well as a path-fed one; an upload feeding a
    workflow with no such rule has no reason to send it.
    """

    description: str | None = Field(default=None, max_length=MAX_NAME_LENGTH)
    source_filename: str | None = Field(
        default=None,
        min_length=1,
        max_length=MAX_NAME_LENGTH,
        # A basename, not a path: rejecting the separator at the model layer
        # means a client that sends a path gets a 422 naming the field rather
        # than a 500 from the DB CHECK. STRICTER than that CHECK, which tests
        # only non-empty and slash-free — `\A`/`\z` because `$` also matches
        # before a trailing newline, and the newline classes because `[^/]`
        # matches one, so `^[^/]+$` admitted both "name.fastq\n" and
        # "na\nme.fastq". The value is echoed back in the submit gate's 422.
        pattern=r"\A[^/\r\n]+\z",
    )


# sha256 wire shape: 64 lowercase hex characters. Pinned at the model
# layer so a misbehaving client surfaces as a 422 before the DB write.
SHA256_HEX_PATTERN = r"^[0-9a-f]{64}$"


class UploadCreateResponse(BaseModel):
    """Returned by POST /api/v1/upload with HTTP 201.

    `doput_ticket` is the base64-encoded HMAC-signed Flight ticket the
    client passes to the data plane on DoPut. The ticket's payload carries
    only `upload_idx`; the data plane resolves the staging path itself.
    The client never names server-side paths.
    """

    upload_idx: Annotated[int, Field(gt=0)]
    doput_ticket: str


class UploadDoneRequest(BaseModel):
    """Body for POST /api/v1/upload/{idx}/done.

    The client forwards the sha256 + row_count + bytes_received the data
    plane returned in its PutResult body. These are recorded descriptively;
    a future authenticated DP→CP channel can replace the client-forwarded
    claim with a server-verified signature.
    """

    sha256: Annotated[str, Field(pattern=SHA256_HEX_PATTERN)]
    row_count: Annotated[int, Field(ge=0)]
    bytes_received: Annotated[int, Field(ge=0)]


class UploadResponse(BaseModel):
    """Returned by GET /api/v1/upload/{idx} and POST /api/v1/upload/{idx}/done."""

    upload_idx: Annotated[int, Field(gt=0)]
    status: UploadStatus
    description: str | None = None
    source_filename: str | None = None
    sha256: str | None = None
    row_count: int | None = None
    bytes_received: int | None = None
    created_by_idx: Annotated[int, Field(gt=0)]
    created_at: AwareDatetime
    completed_at: AwareDatetime | None = None
