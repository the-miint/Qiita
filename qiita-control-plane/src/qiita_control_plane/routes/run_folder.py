"""Run-folder inspection — the filesystem reads a submit gesture needs.

`POST /api/v1/run-folder/inspect` reads a sequencing run folder on the shared
filesystem and returns facts about it: an Illumina run's instrument identity
(from `RunInfo.xml`), or a PacBio run's barcode -> HiFi BAM index.

Doing them here is what frees `submit-bcl-convert` and `submit-pacbio-ingest`
from needing a machine that mounts the cluster. Nothing about what is stored
changes: the caller puts the same path in `action_context`, and the workflow
opens it on a compute node.

Read-only, and it returns facts rather than bytes — no file content crosses the
wire, only the parsed instrument id / model and the BAM filenames. The path is
bounded by the same `PATH_INGEST_ROOTS` gate the work-ticket submit applies, so
this endpoint cannot be used to probe the filesystem outside the roots.

Where the submit gate admits a path it cannot stat (see `ingest_path._probe`
for why), this route has to open the folder now, so every permission failure is
a 403 naming the account to grant — never a 404, which would send the operator
hunting for a typo in a path that is right.
"""

import errno
import os
import pwd
from pathlib import Path
from stat import S_ISDIR

from fastapi import APIRouter, Depends, HTTPException, status
from qiita_common.api_paths import PATH_RUN_FOLDER_INSPECT, PATH_RUN_FOLDER_PREFIX
from qiita_common.auth_constants import SystemRole
from qiita_common.illumina import RUNINFO_FILENAME, read_instrument_run_info
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
    """The account this process runs as. See `_permission_denied` for why."""
    try:
        return pwd.getpwuid(os.geteuid()).pw_name
    except KeyError:
        return f"uid {os.geteuid()}"


def _permission_denied(clause: str, path: Path | str) -> HTTPException:
    """A 403 naming the account and the path a grant has to land on.

    The account is named rather than described because the fix is a grant to
    one specific user, and "the control plane's service account" leaves the
    operator to work out which. This is the one site that states that; the
    module docstring says when a 403 rather than a 404 is owed.
    """
    return HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail={
            "reason": f"the control plane runs as {_service_account()}, which {clause}",
            "path": str(path),
        },
    )


# Errnos that mean the entry resolves to nothing, so it can hide no reads: an
# absent target — a dangling symlink, or an entry removed while the scan runs —
# and a symlink that never resolves. `Path.glob` indexes a run folder holding
# either without tripping on it, and this walk exists to match the glob, so it
# skips them too. Everything else (ESTALE, EIO, ENOTCONN on a shared mount) is
# a directory that may be there and may hold BAMs, which is the case the walk
# cannot answer and must not guess at.
_RESOLVES_TO_NOTHING = frozenset({errno.ENOENT, errno.ELOOP})


def _walk_failure(exc: OSError, *, reported: Path, denied: Path) -> HTTPException:
    """The one answer this module gives for a failure walking the run tree.

    EACCES is a 403 naming `denied`, the directory whose contents could not be
    reached — not always the path that raised: a stat of `<well>/hifi_reads`
    fails with EACCES when the unreadable directory is `<well>`, and that is
    where the grant has to land. Anything else is a 422 carrying the errno
    text, matching what `inspect_run_folder` answers for the same failures on
    the run folder itself.
    """
    if isinstance(exc, PermissionError):
        return _permission_denied(
            "can read this run folder but not a directory under it, so the BAM"
            " index would silently omit whatever that directory holds. Grant it"
            " read+traverse on the whole run tree",
            denied,
        )
    return HTTPException(
        status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
        detail={
            "reason": f"an entry under the run folder could not be read: {exc.strerror}",
            "path": str(reported),
        },
    )


def _stat_or_skip(path: Path, *, denied: Path) -> os.stat_result | None:
    """`os.stat(path)`, or `None` for an entry that resolves to nothing."""
    try:
        return os.stat(path)
    except OSError as exc:
        if exc.errno in _RESOLVES_TO_NOTHING:
            return None
        raise _walk_failure(exc, reported=path, denied=denied) from exc


def _entries(directory: Path, *, denied: Path) -> list[Path]:
    """`iterdir` materialized, so the failure surfaces at the call rather than
    on the first `next()`."""
    try:
        return list(directory.iterdir())
    except OSError as exc:
        if exc.errno in _RESOLVES_TO_NOTHING:
            return []
        raise _walk_failure(exc, reported=directory, denied=denied) from exc


def _reject_if_unopenable(directory: Path, *, denied: Path) -> None:
    """Open `directory` far enough to surface a denial and no further.

    A well's `hifi_reads` holds one BAM per barcode and this walk needs none of
    them — only whether the directory opens at all.
    """
    try:
        next(directory.iterdir(), None)
    except OSError as exc:
        if exc.errno in _RESOLVES_TO_NOTHING:
            return
        raise _walk_failure(exc, reported=directory, denied=denied) from exc


def _reject_if_unreadable_below(run_folder: Path) -> None:
    """403 when a directory under the run folder cannot be opened.

    `Path.glob` swallows the `OSError` from a directory it cannot open and
    yields nothing, so a grant that reaches the run folder but not the well
    directories under it drops those wells' barcodes from the index with no
    error — a wrong answer with a 200 on it, whether it leaves the index empty
    or merely short. Walk the same two levels the glob walks and let the error
    out.

    Every directory under the run folder is walked, not only the ones that turn
    out to hold reads: whether an unopenable directory hides a `hifi_reads` is
    exactly what cannot be determined without opening it. Whatever reaches the
    run folder is expected to reach the whole tree under it (`DEPLOY_CHECKLIST.md`
    holds the grant and what it costs), so a denial here means it did not land.
    An entry that resolves to nothing is a different matter — see
    `_RESOLVES_TO_NOTHING`.
    """
    for well in _entries(run_folder, denied=run_folder):
        # `stat()`, not `is_dir()`: the latter reports False for a path it
        # cannot stat, which is the case this function exists to catch.
        well_st = _stat_or_skip(well, denied=well)
        if well_st is None or not S_ISDIR(well_st.st_mode):
            continue
        hifi = well / HIFI_READS_DIR
        hifi_st = _stat_or_skip(hifi, denied=well)
        if hifi_st is None or not S_ISDIR(hifi_st.st_mode):
            continue
        _reject_if_unopenable(hifi, denied=hifi)


def _inspect_illumina(run_folder: Path) -> IlluminaRunInfo:
    """Parse `RunInfo.xml` for the run's id and instrument model.

    `read_instrument_run_info` raises ValueError on a missing or malformed
    RunInfo.xml and on a serial-number prefix absent from the vendored table
    (which includes every PacBio serial — bcl-convert does not run on PacBio
    data). Its message is already operator-facing, so it becomes the 422 detail
    unchanged.
    """
    # `read_instrument_run_info` reaches RunInfo.xml through `Path.is_file()`,
    # which reports False for a path it cannot stat. Without this, a run folder
    # that lists but does not traverse answers "RunInfo.xml not found" — the
    # typo-hunt this module exists to avoid. Statting it here keeps the
    # permission contract in the module that owns it.
    runinfo_path = run_folder / RUNINFO_FILENAME
    try:
        os.stat(runinfo_path)
    except PermissionError as exc:
        raise _permission_denied(
            "cannot reach RunInfo.xml in this run folder. Grant it read+traverse"
            " on the whole run tree",
            runinfo_path,
        ) from exc
    except FileNotFoundError:
        # Genuinely absent. `read_instrument_run_info` names that better than
        # this pre-check could, and its message becomes the 422 below.
        pass
    except OSError as exc:
        # ELOOP, ESTALE and the rest: the reader reaches the file through
        # `Path.is_file()`, which reports False for all of them, so leaving
        # them to it answers "RunInfo.xml not found" for a file that exists.
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail={
                "reason": f"RunInfo.xml could not be read: {exc.strerror}",
                "path": str(runinfo_path),
            },
        ) from exc

    try:
        instrument_run_id, instrument_model = read_instrument_run_info(run_folder)
    except PermissionError as exc:
        raise _permission_denied(
            "cannot read RunInfo.xml in this run folder. Grant it read on the file",
            runinfo_path,
        ) from exc
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
    See `PacbioRunIndex` for what the caller does with it. What is raised on is
    an index the walk could not finish reading (`_reject_if_unreadable_below`),
    which is not the same thing as an index with nothing in it.
    """
    _reject_if_unreadable_below(run_folder)
    index, duplicated = index_run_bams(run_folder)
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
    # "not a directory" for a directory, which is the case a grant fixes.
    try:
        is_directory = S_ISDIR(run_folder.stat().st_mode)
        if is_directory:
            next(run_folder.iterdir(), None)
    except PermissionError as exc:
        raise _permission_denied(
            "cannot read this run folder or one of its parents; a compute node may"
            " still be able to. Grant it read+traverse, or submit from a machine"
            " that mounts the path",
            run_folder,
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
