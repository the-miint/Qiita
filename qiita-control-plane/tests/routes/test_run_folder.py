"""Route tests for POST /api/v1/run-folder/inspect.

This is what lets `submit-bcl-convert` / `submit-pacbio-ingest` run from a
machine that does not mount the cluster: the two filesystem reads they need
happen here instead of in the CLI. So the coverage that used to sit on the CLI's
local reads sits here now — a missing RunInfo.xml, an unknown serial prefix, a
run folder with no HiFi BAMs — against real folders on disk.
"""

from __future__ import annotations

import errno
import os
import pwd

import pytest
from httpx import ASGITransport, AsyncClient
from qiita_common.api_paths import URL_RUN_FOLDER_INSPECT
from qiita_common.auth_constants import SystemRole

pytestmark = pytest.mark.db


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
async def role_token(postgres_pool):
    """Mint a PAT for a throwaway principal at a chosen role.

    The route gates on role alone (`require_role_at_least`), so the token needs
    no scopes — which is the point of the fixture: it varies the one thing under
    test and holds everything else fixed."""
    import uuid

    from qiita_control_plane.auth.token import mint_api_token

    created: list[int] = []

    async def _make(role: SystemRole) -> tuple[str, int]:
        email = f"rf-{role.value}-{uuid.uuid4()}@example.com"
        pidx = await postgres_pool.fetchval(
            "INSERT INTO qiita.principal (display_name, system_role, created_by_idx)"
            " VALUES ($1, $2, 1) RETURNING idx",
            email,
            role,
        )
        await postgres_pool.execute(
            "INSERT INTO qiita.user (principal_idx, email, affiliation, address, phone)"
            " VALUES ($1, $2, 'X', 'Y', 'Z')",
            pidx,
            email,
        )
        created.append(pidx)
        plaintext, _ = await mint_api_token(
            postgres_pool, principal_idx=pidx, label="rf-test", scopes=[]
        )
        return plaintext, pidx

    yield _make

    if created:
        await postgres_pool.execute(
            "DELETE FROM qiita.api_token WHERE principal_idx = ANY($1::bigint[])", created
        )
        await postgres_pool.execute(
            "DELETE FROM qiita.user WHERE principal_idx = ANY($1::bigint[])", created
        )
        await postgres_pool.execute(
            "DELETE FROM qiita.principal WHERE idx = ANY($1::bigint[])", created
        )


@pytest.fixture
async def wet_lab_admin_token(role_token):
    return await role_token(SystemRole.WET_LAB_ADMIN)


@pytest.fixture
async def system_admin_token(role_token):
    return await role_token(SystemRole.SYSTEM_ADMIN)


@pytest.fixture
async def regular_user_token(role_token):
    return await role_token(SystemRole.USER)


@pytest.fixture
async def rf_client(postgres_pool, ingest_root):
    """App wired with `ingest_root` as the only PATH_INGEST_ROOTS entry, so the
    route's containment check has a root that exists on this machine."""
    from qiita_control_plane.config import Settings
    from qiita_control_plane.main import app

    app.state.pool = postgres_pool
    app.state.oidc_verifier = None
    app.state.settings = Settings(
        database_url="unused",
        flight_signing_key=b"\x00" * 32,
        data_plane_url="unused",
        path_ingest_roots=(ingest_root,),
    )
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        yield ac


def _illumina_folder(ingest_root, name: str, *, with_runinfo: bool = True):
    """A BCL run folder. With `with_runinfo`, a RunInfo.xml whose `Run@Id` is
    the folder name and whose `Instrument` serial is the name's second
    underscore segment — the real run-ID convention the reader parses."""
    folder = ingest_root / name
    folder.mkdir(parents=True, exist_ok=True)
    if with_runinfo:
        serial = name.split("_")[1]
        (folder / "RunInfo.xml").write_text(
            '<?xml version="1.0"?>\n'
            f'<RunInfo Version="6"><Run Id="{name}">'
            f"<Instrument>{serial}</Instrument></Run></RunInfo>\n",
            encoding="utf-8",
        )
    return folder


def _pacbio_bam(run_folder, well: str, movie: str, barcode: str | None):
    """One demultiplexed HiFi BAM under `{run}/{well}/hifi_reads/`. A `None`
    barcode writes the per-cell unassigned file instead."""
    cell = run_folder / well / "hifi_reads"
    cell.mkdir(parents=True, exist_ok=True)
    name = (
        f"{movie}.hifi_reads.unassigned.bam"
        if barcode is None
        else (f"{movie}.hifi_reads.{barcode}.bam")
    )
    path = cell / name
    path.write_bytes(b"")
    return path


async def _inspect(client, token, path, platform):
    return await client.post(
        URL_RUN_FOLDER_INSPECT,
        json={"path": str(path), "platform": platform},
        headers={"Authorization": f"Bearer {token}"},
    )


# ---------------------------------------------------------------------------
# Illumina
# ---------------------------------------------------------------------------


async def test_illumina_returns_run_id_and_model(rf_client, wet_lab_admin_token, ingest_root):
    """The two facts submit-bcl-convert needs before it can POST /sequencing-run,
    read on the control plane rather than on the submitter's machine."""
    token, _ = wet_lab_admin_token
    folder = _illumina_folder(ingest_root, "230101_A00123_0001_BHXYZ")

    resp = await _inspect(rf_client, token, folder, "illumina")

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["platform"] == "illumina"
    assert body["path"] == str(folder)
    assert body["illumina"] == {
        "instrument_run_id": "230101_A00123_0001_BHXYZ",
        "instrument_model": "Illumina NovaSeq 6000",
    }
    assert body["pacbio"] is None


async def test_illumina_missing_runinfo_returns_422(rf_client, wet_lab_admin_token, ingest_root):
    token, _ = wet_lab_admin_token
    folder = _illumina_folder(ingest_root, "230101_A00123_0002_BHXYZ", with_runinfo=False)

    resp = await _inspect(rf_client, token, folder, "illumina")

    assert resp.status_code == 422, resp.text
    assert "RunInfo.xml not found" in resp.json()["detail"]["reason"]


async def test_illumina_unknown_serial_prefix_returns_422(
    rf_client, wet_lab_admin_token, ingest_root
):
    """A serial that matches no vendored Illumina prefix. The model selects the
    bcl_convert step's SLURM profile, so failing here beats failing at dispatch."""
    token, _ = wet_lab_admin_token
    folder = _illumina_folder(ingest_root, "230101_ZZZZZ999_0001_BHXYZ")

    resp = await _inspect(rf_client, token, folder, "illumina")

    assert resp.status_code == 422, resp.text
    assert "unknown instrument serial prefix" in resp.json()["detail"]["reason"]


async def test_pacbio_folder_inspected_as_illumina_returns_422(
    rf_client, wet_lab_admin_token, ingest_root
):
    """A PacBio Revio serial starts with a lowercase `r`, which the loader
    filters out of the table. bcl-convert is Illumina-only, so this surfaces as
    the same unknown-prefix error a malformed Illumina serial would."""
    token, _ = wet_lab_admin_token
    folder = _illumina_folder(ingest_root, "230101_r00012_0001_BHXYZ")

    resp = await _inspect(rf_client, token, folder, "illumina")

    assert resp.status_code == 422, resp.text
    assert "unknown instrument serial prefix" in resp.json()["detail"]["reason"]


# ---------------------------------------------------------------------------
# PacBio
# ---------------------------------------------------------------------------


async def test_pacbio_indexes_bams_by_barcode(rf_client, wet_lab_admin_token, ingest_root):
    token, _ = wet_lab_admin_token
    run = ingest_root / "pacbio-run-1"
    _pacbio_bam(run, "1_A01", "m84_s1", "bc1001")
    _pacbio_bam(run, "1_B01", "m84_s2", "bc1002")

    resp = await _inspect(rf_client, token, run, "pacbio_smrt")

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["illumina"] is None
    index = body["pacbio"]["hifi_bam_by_barcode"]
    assert sorted(index) == ["bc1001", "bc1002"]
    assert index["bc1001"].endswith("1_A01/hifi_reads/m84_s1.hifi_reads.bc1001.bam")
    assert body["pacbio"]["duplicated_barcodes"] == []
    # ABSOLUTE, and under the root. The CLI puts these straight into
    # `action_context.bam_path`, where the submit gate re-checks them against
    # PATH_INGEST_ROOTS — a relative path would be refused there, one gesture
    # later, with nothing pointing back at this route.
    for bam in index.values():
        assert bam.startswith(str(ingest_root) + "/"), bam


async def test_pacbio_skips_unassigned_reads(rf_client, wet_lab_admin_token, ingest_root):
    """Reads the demux could not assign to a barcode are not a sample."""
    token, _ = wet_lab_admin_token
    run = ingest_root / "pacbio-run-2"
    _pacbio_bam(run, "1_A01", "m84_s1", "bc1001")
    _pacbio_bam(run, "1_A01", "m84_s1", None)

    resp = await _inspect(rf_client, token, run, "pacbio_smrt")

    assert sorted(resp.json()["pacbio"]["hifi_bam_by_barcode"]) == ["bc1001"]


async def test_pacbio_reports_a_cross_cell_duplicate_rather_than_failing(
    rf_client, wet_lab_admin_token, ingest_root
):
    """A barcode in two SMRT cells is reported, not raised on: whether it
    matters depends on the pre-flight roster, which this route does not have.
    It is left OUT of the index so it cannot bind silently."""
    token, _ = wet_lab_admin_token
    run = ingest_root / "pacbio-run-3"
    _pacbio_bam(run, "1_A01", "m84_s1", "bc2083")
    _pacbio_bam(run, "1_B01", "m84_s2", "bc2083")
    _pacbio_bam(run, "1_A01", "m84_s1", "bc1001")

    body = (await _inspect(rf_client, token, run, "pacbio_smrt")).json()

    assert body["pacbio"]["duplicated_barcodes"] == ["bc2083"]
    assert sorted(body["pacbio"]["hifi_bam_by_barcode"]) == ["bc1001"]


async def test_pacbio_empty_run_folder_returns_an_empty_index(
    rf_client, wet_lab_admin_token, ingest_root
):
    """Not an error here. The caller pairs the index against its roster and can
    say which sample is missing a BAM — a better message than "no BAMs here"."""
    token, _ = wet_lab_admin_token
    run = ingest_root / "pacbio-run-4"
    run.mkdir(parents=True)

    body = (await _inspect(rf_client, token, run, "pacbio_smrt")).json()

    assert body["pacbio"] == {"hifi_bam_by_barcode": {}, "duplicated_barcodes": []}


# ---------------------------------------------------------------------------
# The path gate, and who may ask
# ---------------------------------------------------------------------------


async def test_path_outside_the_roots_returns_422(rf_client, wet_lab_admin_token):
    """The same bound the work-ticket submit applies — so a laptop path fails
    here, one step before it would at submit, with the roots named."""
    token, _ = wet_lab_admin_token

    resp = await _inspect(rf_client, token, "/Users/me/Desktop/run", "illumina")

    assert resp.status_code == 422, resp.text
    detail = resp.json()["detail"]
    assert "outside every configured ingest root" in detail["reason"]
    assert detail["ingest_roots"]


async def test_missing_path_returns_422(rf_client, wet_lab_admin_token, ingest_root):
    token, _ = wet_lab_admin_token

    resp = await _inspect(rf_client, token, ingest_root / "no-such-run", "illumina")

    assert resp.status_code == 422, resp.text
    assert resp.json()["detail"]["reason"] == "host path does not exist"


async def test_a_file_is_not_a_run_folder(rf_client, wet_lab_admin_token, ingest_root):
    token, _ = wet_lab_admin_token
    a_file = ingest_root / "not-a-folder.txt"
    a_file.write_text("x")

    resp = await _inspect(rf_client, token, a_file, "illumina")

    assert resp.status_code == 422, resp.text
    assert resp.json()["detail"]["reason"] == "run folder is not a directory"


@pytest.mark.skipif(os.geteuid() == 0, reason="root traverses regardless of mode bits")
async def test_unreadable_well_dirs_return_403_not_an_empty_index(
    rf_client, wet_lab_admin_token, ingest_root
):
    """A grant that reaches the run folder but not the wells under it.

    `Path.glob` swallows the error from a directory it cannot open and yields
    nothing, so this would otherwise be a 200 carrying an empty BAM index —
    indistinguishable from a run with no demultiplexed reads.
    """
    token, _ = wet_lab_admin_token
    run = ingest_root / "partial-grant-run"
    hifi = run / "1_A01" / "hifi_reads"
    hifi.mkdir(parents=True, exist_ok=True)
    (hifi / "m84.hifi_reads.bc2073.bam").write_bytes(b"")
    (run / "1_A01").chmod(0o000)
    try:
        resp = await _inspect(rf_client, token, run, "pacbio_smrt")
    finally:
        (run / "1_A01").chmod(0o755)

    assert resp.status_code == 403, resp.text
    assert "silently omit" in resp.json()["detail"]["reason"]


@pytest.mark.skipif(os.geteuid() == 0, reason="root traverses regardless of mode bits")
async def test_an_empty_run_folder_is_still_a_200(rf_client, wet_lab_admin_token, ingest_root):
    """The control for the test above: readable the whole way down and simply
    holding no BAMs is a 200 with an empty index, not a 403."""
    token, _ = wet_lab_admin_token
    run = ingest_root / "empty-but-readable-run"
    (run / "1_A01" / "hifi_reads").mkdir(parents=True, exist_ok=True)

    resp = await _inspect(rf_client, token, run, "pacbio_smrt")

    assert resp.status_code == 200, resp.text
    assert resp.json()["pacbio"]["hifi_bam_by_barcode"] == {}


@pytest.mark.skipif(os.geteuid() == 0, reason="root traverses regardless of mode bits")
async def test_one_unreadable_well_among_readable_ones_returns_403(
    rf_client, wet_lab_admin_token, ingest_root
):
    """The partial grant that still produces a NON-empty index.

    Gating the guard on an empty index misses this: one readable well fills the
    map, the unreadable one drops out of the glob, and the caller gets a 200
    carrying a barcode set that is short by however many wells it could not
    open. The pre-flight sequenced_sample roster then reads them as absent.
    """
    token, _ = wet_lab_admin_token
    run = ingest_root / "half-granted-run"
    _pacbio_bam(run, "1_A01", "m84137_250101_000000", "bc2001")
    _pacbio_bam(run, "1_B01", "m84137_250101_000000", "bc2002")
    (run / "1_B01").chmod(0o000)
    try:
        resp = await _inspect(rf_client, token, run, "pacbio_smrt")
    finally:
        (run / "1_B01").chmod(0o755)

    assert resp.status_code == 403, resp.text
    assert "silently omit" in resp.json()["detail"]["reason"]


@pytest.mark.skipif(os.geteuid() == 0, reason="root traverses regardless of mode bits")
async def test_an_unreadable_non_well_directory_also_returns_403(
    rf_client, wet_lab_admin_token, ingest_root
):
    """A run folder carries siblings of the wells (`metadata`, `statistics`), and
    an unopenable one is refused even though the barcode map came out complete.

    Whether it hides a `hifi_reads` is exactly what cannot be determined without
    opening it, so there is no narrower rule available. The bucket-2 ACL grant
    covers every directory in the tree, so a denial here means it did not land.
    """
    token, _ = wet_lab_admin_token
    run = ingest_root / "unreadable-sibling-run"
    _pacbio_bam(run, "1_A01", "m84137_250101_000000", "bc2001")
    (run / "metadata").mkdir(parents=True, exist_ok=True)
    (run / "metadata").chmod(0o000)
    try:
        resp = await _inspect(rf_client, token, run, "pacbio_smrt")
    finally:
        (run / "metadata").chmod(0o755)

    assert resp.status_code == 403, resp.text
    # The grant belongs on the directory that could not be opened, not on the
    # `hifi_reads` under it whose path never resolved.
    assert resp.json()["detail"]["path"] == str(run / "metadata")


async def test_a_symlink_loop_among_the_wells_is_skipped(
    rf_client, wet_lab_admin_token, ingest_root
):
    """A self-referential symlink raises ELOOP from `stat`, but it resolves to
    nothing, so it hides no reads.

    Measured: `Path.glob("*/hifi_reads/*.bam")` indexes this run completely and
    never trips on the link. The walk mirrors the glob, so refusing here would
    brick a run whose barcode map is correct — over a link on a drop directory
    the control plane cannot delete.
    """
    token, _ = wet_lab_admin_token
    run = ingest_root / "symlink-loop-run"
    _pacbio_bam(run, "1_A01", "m84137_250101_000000", "bc2001")
    (run / "loop").symlink_to(run / "loop")

    resp = await _inspect(rf_client, token, run, "pacbio_smrt")

    assert resp.status_code == 200, resp.text
    assert list(resp.json()["pacbio"]["hifi_bam_by_barcode"]) == ["bc2001"]


async def test_an_entry_the_walk_cannot_resolve_is_422_not_500(
    rf_client, wet_lab_admin_token, ingest_root, monkeypatch
):
    """ESTALE — a directory that may be there and may hold BAMs.

    Unlike ENOENT and ELOOP this one cannot be skipped: the walk does not know
    whether it hid a `hifi_reads`, and answering 200 would be the short index
    the guard exists to prevent. It is injected because no portable filesystem
    operation produces a stale NFS handle on demand.
    """
    from qiita_control_plane.routes import run_folder as rf

    token, _ = wet_lab_admin_token
    run = ingest_root / "stale-handle-run"
    _pacbio_bam(run, "1_A01", "m84137_250101_000000", "bc2001")

    real_stat = os.stat

    def _stale(path, *args, **kwargs):
        if str(path).endswith("1_A01"):
            raise OSError(errno.ESTALE, "Stale file handle", str(path))
        return real_stat(path, *args, **kwargs)

    monkeypatch.setattr(rf.os, "stat", _stale)
    resp = await _inspect(rf_client, token, run, "pacbio_smrt")

    assert resp.status_code == 422, resp.text
    assert "could not be read" in resp.json()["detail"]["reason"]


async def test_a_runinfo_that_exists_but_cannot_be_read_is_not_reported_absent(
    rf_client, wet_lab_admin_token, ingest_root
):
    """A `RunInfo.xml` symlink loop. `Path.is_file()` reports False for it, so
    leaving it to the reader answers "RunInfo.xml not found" for a file that is
    right there — the typo hunt again, in a case permissions do not cover."""
    token, _ = wet_lab_admin_token
    folder = ingest_root / "runinfo-loop-run"
    folder.mkdir(parents=True, exist_ok=True)
    (folder / "RunInfo.xml").symlink_to(folder / "RunInfo.xml")

    resp = await _inspect(rf_client, token, folder, "illumina")

    assert resp.status_code == 422, resp.text
    reason = resp.json()["detail"]["reason"]
    assert "could not be read" in reason
    assert "not found" not in reason


async def test_a_dangling_symlink_among_the_wells_is_skipped(
    rf_client, wet_lab_admin_token, ingest_root
):
    """`stat()` on a symlink to nothing raises ENOENT, which is not a denial.

    The walk that catches an unreadable well has to stat every entry, so it
    meets whatever else the run folder holds — including a link whose target is
    gone. That is the glob's own behaviour (it skips it), not a 500.
    """
    token, _ = wet_lab_admin_token
    run = ingest_root / "dangling-link-run"
    _pacbio_bam(run, "1_A01", "m84137_250101_000000", "bc2001")
    (run / "gone").symlink_to(ingest_root / "no-such-target")

    resp = await _inspect(rf_client, token, run, "pacbio_smrt")

    assert resp.status_code == 200, resp.text
    assert list(resp.json()["pacbio"]["hifi_bam_by_barcode"]) == ["bc2001"]


@pytest.mark.skipif(os.geteuid() == 0, reason="root traverses regardless of mode bits")
async def test_an_unreadable_runinfo_returns_403_not_500(
    rf_client, wet_lab_admin_token, ingest_root
):
    """The exact shape an ACL grant on directories leaves behind: the run folder
    traverses, the file inside it does not open. Opening it raises
    PermissionError from inside the XML reader, which handles only ParseError.
    """
    token, _ = wet_lab_admin_token
    folder = _illumina_folder(ingest_root, "230101_A00123_0009_BHXYZ")
    (folder / "RunInfo.xml").chmod(0o000)
    try:
        resp = await _inspect(rf_client, token, folder, "illumina")
    finally:
        (folder / "RunInfo.xml").chmod(0o644)

    assert resp.status_code == 403, resp.text
    assert "cannot read RunInfo.xml" in resp.json()["detail"]["reason"]


@pytest.mark.skipif(os.geteuid() == 0, reason="root traverses regardless of mode bits")
async def test_a_listable_but_untraversable_run_folder_returns_403_not_404(
    rf_client, wet_lab_admin_token, ingest_root
):
    """Mode 0o444: the prelude's `iterdir()` succeeds, so the folder looks fine,
    and only the child lookup fails. `Path.is_file()` reports False for a path
    it cannot stat, so without a stat of our own this answers "RunInfo.xml not
    found" — the typo hunt the 403 exists to prevent.
    """
    token, _ = wet_lab_admin_token
    folder = _illumina_folder(ingest_root, "230101_A00123_0010_BHXYZ")
    folder.chmod(0o444)
    try:
        resp = await _inspect(rf_client, token, folder, "illumina")
    finally:
        folder.chmod(0o755)

    assert resp.status_code == 403, resp.text
    assert "cannot reach RunInfo.xml" in resp.json()["detail"]["reason"]


@pytest.mark.skipif(os.geteuid() == 0, reason="root traverses regardless of mode bits")
async def test_an_untraversable_ancestor_returns_403_not_422(
    rf_client, wet_lab_admin_token, ingest_root
):
    """The denial an ACL grant actually fixes sits on a PARENT of the run folder,
    not on the folder itself.

    `Path.is_dir()` reports False for anything it cannot stat, so testing
    directory-ness first turns this into "run folder is not a directory" — a
    message that describes the wrong problem and names no fix.
    """
    token, _ = wet_lab_admin_token
    parent = ingest_root / "closed-parent"
    run = parent / "the-run"
    run.mkdir(parents=True, exist_ok=True)
    (run / "RunInfo.xml").write_text("<x/>")
    parent.chmod(0o000)
    try:
        resp = await _inspect(rf_client, token, run, "illumina")
    finally:
        parent.chmod(0o755)

    assert resp.status_code == 403, resp.text
    reason = resp.json()["detail"]["reason"]
    assert "or one of its parents" in reason
    # Names the account to grant, rather than "the service account".
    assert pwd.getpwuid(os.geteuid()).pw_name in reason


@pytest.mark.skipif(os.geteuid() == 0, reason="root traverses regardless of mode bits")
async def test_an_unreadable_run_folder_returns_403_not_404(
    rf_client, wet_lab_admin_token, ingest_root
):
    """The arm the qiita-api / qiita-job split makes real.

    The submit gate can shrug at a permission error and admit, because the step
    runs as a different account with wider group membership. This route has to
    open the folder now, so it cannot — and it must not report "cannot read" as
    "not found", which would send an operator hunting for a typo in a path that
    is correct. The message names the situation and the two ways out.
    """
    token, _ = wet_lab_admin_token
    closed = ingest_root / "closed-run"
    closed.mkdir(parents=True, exist_ok=True)
    (closed / "RunInfo.xml").write_text("<x/>")
    closed.chmod(0o000)
    try:
        resp = await _inspect(rf_client, token, closed, "illumina")
    finally:
        closed.chmod(0o755)

    assert resp.status_code == 403, resp.text
    reason = resp.json()["detail"]["reason"]
    assert "cannot read this run folder" in reason
    assert "compute node may still be able to" in reason


async def test_a_path_with_an_embedded_nul_returns_422_not_500(
    rf_client, wet_lab_admin_token, ingest_root
):
    """The model's `^/` pattern matches at the prefix, so the value reaches the
    gate; `os.path.realpath` then raises ValueError rather than OSError for it.
    Answer it the way every other malformed path is answered."""
    token, _ = wet_lab_admin_token

    resp = await _inspect(rf_client, token, f"{ingest_root}/a\x00b", "illumina")

    assert resp.status_code == 422, resp.text
    assert "not a usable filesystem path" in resp.json()["detail"]["reason"]


async def test_relative_path_is_rejected_by_the_model(rf_client, wet_lab_admin_token):
    """`pattern: "^/"` on the request model, so this never reaches the gate."""
    token, _ = wet_lab_admin_token

    resp = await _inspect(rf_client, token, "relative/run", "illumina")

    assert resp.status_code == 422, resp.text


async def test_a_platform_with_no_run_folder_layout_returns_422(
    rf_client, wet_lab_admin_token, ingest_root
):
    """`Platform` is a wider set than the two layouts defined here. A valid
    platform with no layout is a 422 naming the supported ones, not a 500."""
    token, _ = wet_lab_admin_token
    run = ingest_root / "ont-run"
    run.mkdir(parents=True)

    resp = await _inspect(rf_client, token, run, "oxford_nanopore")

    assert resp.status_code == 422, resp.text
    detail = resp.json()["detail"]
    assert detail["platform"] == "oxford_nanopore"
    assert sorted(detail["supported"]) == ["illumina", "pacbio_smrt"]


async def test_a_regular_user_is_refused(rf_client, regular_user_token, ingest_root):
    """wet_lab_admin+, matching who may name a host path at work-ticket submit.
    The answer is only useful to someone who can then submit against it."""
    token, _ = regular_user_token
    folder = _illumina_folder(ingest_root, "230101_A00123_0003_BHXYZ")

    resp = await _inspect(rf_client, token, folder, "illumina")

    assert resp.status_code == 403, resp.text


async def test_a_system_admin_is_allowed(rf_client, system_admin_token, ingest_root):
    token, _ = system_admin_token
    folder = _illumina_folder(ingest_root, "230101_A00123_0004_BHXYZ")

    resp = await _inspect(rf_client, token, folder, "illumina")

    assert resp.status_code == 200, resp.text


@pytest.mark.parametrize("role", [SystemRole.WET_LAB_ADMIN, SystemRole.SYSTEM_ADMIN])
def test_the_allowed_roles_match_the_submit_gate(role):
    """Pins the pairing rather than restating it: the roles that may inspect a
    run folder are exactly the roles `_check_ingest_paths` lets name one."""
    from qiita_control_plane.auth.principal import _ROLE_ORDER

    assert _ROLE_ORDER[role] >= _ROLE_ORDER[SystemRole.WET_LAB_ADMIN]
