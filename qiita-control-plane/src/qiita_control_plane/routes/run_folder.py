"""Run-folder inspection — the filesystem reads a submit gesture needs.

`POST /api/v1/run-folder/inspect` reads a sequencing run folder on the shared
filesystem and returns facts about it: an Illumina run's instrument identity
(from `RunInfo.xml`), or a PacBio run's barcode -> HiFi BAM index.

Both reads used to happen in the CLI, which is what made `submit-bcl-convert`
and `submit-pacbio-ingest` require a machine that mounts the cluster. Doing them
here removes that requirement without changing what is stored: the caller still
puts the same path in `action_context`, and the workflow still opens it on a
compute node.

Read-only, and it returns facts rather than bytes — no file content crosses the
wire, only the parsed instrument id / model and the BAM filenames. The path is
bounded by the same `PATH_INGEST_ROOTS` gate the work-ticket submit applies, so
this endpoint cannot be used to probe the filesystem outside the roots.

**The account split matters here in a way it does not at the submit gate.** The
control plane runs as `qiita-api`; SLURM steps run as `qiita-job`, with wider
group membership. The submit gate can shrug at a permission error (it admits,
and lets the step be the judge), but this route has to actually READ, so it
cannot. A folder `qiita-job` can read and `qiita-api` cannot gets a 403 naming
the account — not a 404, which would send the operator hunting for a typo in a
path that is right.
"""

import os
import pwd
from pathlib import Path
from stat import S_ISDIR

from fastapi import APIRouter, Depends, HTTPException, status
from qiita_common.api_paths import PATH_RUN_FOLDER_INSPECT, PATH_RUN_FOLDER_PREFIX
from qiita_common.auth_constants import SystemRole
from qiita_common.illumina import read_instrument_run_info
from qiita_common.models import (
    IlluminaRunInfo,
    PacbioRunIndex,
    Platform,
    RunFolderInspectRequest,
    RunFolderInspectResponse,
)
from qiita_common.pacbio import HIFI_READS_DIR, index_run_bams

from ..auth.guards import require_role_at_least
from ..auth.principal import Principal
from ..config import Settings
from ..deps import get_settings
from ..ingest_path import IngestPathError, resolve_ingest_path

router = APIRouter(prefix=PATH_RUN_FOLDER_PREFIX, tags=["run-folder"])


def _service_account() -> str:
    """The account this process runs as, for the permission-denied message.

    Named rather than described: the fix is a group or ACL grant to a specific
    user, and "the control plane's service account" leaves the operator to work
    out which one that is.
    """
    try:
        return pwd.getpwuid(os.geteuid()).pw_name
    except KeyError:
        return f"uid {os.geteuid()}"


def _reject_if_unreadable_below(run_folder: Path) -> None:
    """403 when the empty BAM index is a permission problem rather than an
    empty run.

    `Path.glob` swallows the `OSError` from a directory it cannot open and
    yields nothing, so a grant that reaches the run folder but not the well
    directories under it is indistinguishable from a run with no demultiplexed
    reads — a wrong answer with a 200 on it. Walk the same two levels the glob
    walks and let the error out.
    """
    try:
        for well in run_folder.iterdir():
            # `stat()`, not `is_dir()`: the latter reports False for a path it
            # cannot stat, which is the very case this function exists to catch.
            if not S_ISDIR(well.stat().st_mode):
                continue
            hifi = well / HIFI_READS_DIR
            try:
                hifi_is_dir = S_ISDIR(hifi.stat().st_mode)
            except FileNotFoundError:
                continue
            if hifi_is_dir:
                next(hifi.iterdir(), None)
    except PermissionError as exc:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={
                "reason": (
                    f"the control plane runs as {_service_account()}, which can read this"
                    " run folder but not a directory under it, so the BAM index would be"
                    " silently empty. Grant it read+traverse on the whole run tree"
                ),
                "path": str(exc.filename or run_folder),
            },
        ) from exc


def _inspect_illumina(run_folder: Path) -> IlluminaRunInfo:
    """Parse `RunInfo.xml` for the run's id and instrument model.

    `read_instrument_run_info` raises ValueError on a missing or malformed
    RunInfo.xml and on a serial-number prefix absent from the vendored table
    (which includes every PacBio serial — bcl-convert does not run on PacBio
    data). Its message is already operator-facing, so it becomes the 422 detail
    unchanged.
    """
    try:
        instrument_run_id, instrument_model = read_instrument_run_info(run_folder)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail={"reason": str(exc), "path": str(run_folder)},
        ) from exc
    return IlluminaRunInfo(instrument_run_id=instrument_run_id, instrument_model=instrument_model)


def _inspect_pacbio(run_folder: Path) -> PacbioRunIndex:
    """Index `{run}/{well}/hifi_reads/*.bam` by barcode.

    An empty index is returned rather than raised on — whether it is a problem
    depends on the caller's pre-flight roster, which this route does not have.
    See `PacbioRunIndex` for what the caller does with it.
    """
    index, duplicated = index_run_bams(run_folder)
    if not index:
        _reject_if_unreadable_below(run_folder)
    return PacbioRunIndex(
        hifi_bam_by_barcode={barcode: str(path) for barcode, path in sorted(index.items())},
        duplicated_barcodes=sorted(duplicated),
    )


@router.post(PATH_RUN_FOLDER_INSPECT)
async def inspect_run_folder(
    body: RunFolderInspectRequest,
    settings: Settings = Depends(get_settings),
    principal: Principal = Depends(require_role_at_least(SystemRole.WET_LAB_ADMIN)),
) -> RunFolderInspectResponse:
    """Read a run folder and return what a submit gesture needs from it.

    wet_lab_admin+, matching who may name a host path at work-ticket submit —
    the answer here is only useful to someone who can then submit against that
    path, and the two gates would be inconsistent otherwise.
    """
    try:
        run_folder = resolve_ingest_path(body.path, roots=settings.path_ingest_roots)
    except IngestPathError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail={
                "reason": exc.reason,
                "path": exc.path,
                "ingest_roots": [str(root) for root in exc.roots],
            },
        ) from exc

    # The gate admits a path it could not stat, because a step running as a
    # different account may still read it. This route cannot: it has to open the
    # folder now, so it separates EACCES from every other failure and names the
    # account, since the fix is a grant to that specific user.
    #
    # `Path.is_dir()` cannot do this test. It reports False for anything it
    # fails to stat — an untraversable ANCESTOR included — so it would answer
    # "not a directory" for a directory, which is the case an ACL grant fixes.
    try:
        is_directory = S_ISDIR(run_folder.stat().st_mode)
        if is_directory:
            next(run_folder.iterdir(), None)
    except PermissionError as exc:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={
                "reason": (
                    f"the control plane runs as {_service_account()}, which cannot read"
                    " this run folder or one of its parents; a compute node may still be"
                    " able to. Grant it read+traverse, or submit from a machine that"
                    " mounts the path"
                ),
                "path": str(run_folder),
            },
        ) from exc
    except OSError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail={
                "reason": f"run folder could not be opened: {exc.strerror}",
                "path": str(run_folder),
            },
        ) from exc
    if not is_directory:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail={
                "reason": "run folder is not a directory",
                "path": str(run_folder),
            },
        )

    if body.platform is Platform.ILLUMINA:
        return RunFolderInspectResponse(
            path=str(run_folder),
            platform=body.platform,
            illumina=_inspect_illumina(run_folder),
        )
    if body.platform is Platform.PACBIO_SMRT:
        return RunFolderInspectResponse(
            path=str(run_folder),
            platform=body.platform,
            pacbio=_inspect_pacbio(run_folder),
        )
    raise HTTPException(
        status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
        detail={
            "reason": "no run-folder layout is defined for this platform",
            "platform": body.platform.value,
            "supported": [Platform.ILLUMINA.value, Platform.PACBIO_SMRT.value],
        },
    )
