"""Run-folder inspection: the two filesystem reads a submit gesture needs.

A submit gesture must read the run folder before it can mint anything —
Illumina for the instrument identity in `RunInfo.xml`, PacBio for the barcode
-> HiFi BAM index. `POST /api/v1/run-folder/inspect` does those reads on the
control plane, so the submitting machine need not mount the cluster. The
response carries facts about the folder, never bytes from it: the caller names
the same path in `action_context`, and the workflow opens it on the node.

The response is platform-discriminated — exactly one of `illumina` / `pacbio`
is populated — because the two platforms share nothing. A PacBio run folder has
no `RunInfo.xml` at all (its instrument identity comes from operator flags),
and an Illumina one has no `hifi_reads/` tree.
"""

from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, model_validator

from ..auth_constants import MAX_NAME_LENGTH
from .reference import Platform


class RunFolderInspectRequest(BaseModel):
    """Body for POST /api/v1/run-folder/inspect.

    Exactly one of `path` or `run_id` names the run folder. `path` is the folder
    as the CLUSTER sees it (bounded by PATH_INGEST_ROOTS, checked for existence);
    `run_id` is the instrument_run_id, resolved server-side against those roots
    so a submitter names a run rather than a host path. Either way the response
    echoes the resolved path, which is what goes into `action_context`.
    """

    model_config = ConfigDict(extra="forbid")

    path: Annotated[str, Field(min_length=1, pattern="^/")] | None = None
    run_id: Annotated[str, Field(min_length=1, max_length=MAX_NAME_LENGTH)] | None = None
    platform: Platform

    @model_validator(mode="after")
    def _exactly_one_locator(self) -> RunFolderInspectRequest:
        if (self.path is None) == (self.run_id is None):
            raise ValueError("provide exactly one of path or run_id")
        return self


class IlluminaRunInfo(BaseModel):
    """What `RunInfo.xml` yields: the run's own id and the instrument model
    resolved from its serial-number prefix.

    The model selects the bcl_convert step's SLURM resource profile, so a folder
    whose serial prefix is not in the vendored table fails here rather than at
    dispatch.
    """

    # Same bound as `SequencingRunCreateRequest.instrument_run_id`, which is
    # where this value is headed; a looser one here would pass inspect and
    # then fail the create.
    instrument_run_id: Annotated[str, Field(min_length=1, max_length=MAX_NAME_LENGTH)]
    instrument_model: Annotated[str, Field(min_length=1)]


class PacbioRunIndex(BaseModel):
    """The barcode -> HiFi BAM index under `{run}/{smartcell_well}/hifi_reads/`.

    `duplicated_barcodes` lists barcodes seen in more than one SMART cell. They
    are reported rather than raised on: whether a duplicate matters depends on
    the pre-flight roster, which this endpoint does not have. The caller pairs
    the index against its own rows and decides.
    """

    hifi_bam_by_barcode: dict[str, str] = Field(default_factory=dict)
    duplicated_barcodes: list[str] = Field(default_factory=list)


class RunFolderInspectResponse(BaseModel):
    """Returned by POST /api/v1/run-folder/inspect with HTTP 200.

    Exactly one of `illumina` / `pacbio` is populated, keyed by the requested
    platform. `path` echoes the NORMALIZED form the gate resolved — the value
    to put in `action_context`, which may differ from what was sent if it
    carried a `.` or `..` segment.
    """

    path: str
    platform: Platform
    illumina: IlluminaRunInfo | None = None
    pacbio: PacbioRunIndex | None = None
