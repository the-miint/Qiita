"""`qiita-admin check-stored-preflights`: the scan is pinned against stored
pre-flights written under an older schema, and the handler against a fixed scan
result, for what the operator sees and the exit code."""

import secrets
import sqlite3

import pytest
import pytest_asyncio
from run_preflight import load_db_bytes

from qiita_control_plane.cli.admin import check_stored_preflights as cli
from qiita_control_plane.cli.admin import main
from qiita_control_plane.cli.admin.check_stored_preflights import (
    PreflightCheck,
    UnopenablePreflight,
    find_unopenable_preflights,
)
from qiita_control_plane.testing.db_seeds import seed_user_principal
from qiita_control_plane.testing.db_teardown import delete_idxs, delete_principal

# (filename, statement dirtying the pre-flight, column the library names) per pool
_DIRTY_PREFLIGHTS = (
    (
        "padded.db",
        "UPDATE input_sample SET sample_name = ' ' || sample_name"
        " WHERE input_sample_idx = (SELECT min(input_sample_idx) FROM input_sample)",
        "input_sample.sample_name",
    ),
    ("blank.db", "UPDATE project SET bioproject_accession = ''", "project.bioproject_accession"),
)
_SCAN = PreflightCheck(
    checked=3,
    unopenable=[UnopenablePreflight(7, 2, "padded.db", "ValueError: padded sample_name")],
)


def _open_error(blob: bytes, column: str) -> str:
    """The library's refusal of `blob`, which must name `column`."""
    with pytest.raises(ValueError, match=column) as exc_info:
        load_db_bytes(blob)
    error = f"ValueError: {exc_info.value}"
    return error


@pytest_asyncio.fixture
async def stored_preflights(postgres_pool, build_amplicon_preflight):
    """One run with a clean stored pre-flight and the dirty ones, all written under
    the committed fixture's older schema. Yields the dirty pools' expected reports."""
    owner_idx = await seed_user_principal(postgres_pool, prefix="pf-check", suffix="owner")
    run_idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.sequencing_run (instrument_run_id, platform, created_by_idx)"
        " VALUES ($1, 'illumina'::qiita.platform, $2) RETURNING idx",
        f"pf-check-{secrets.token_hex(4)}",
        owner_idx,
    )
    blobs = {"clean.db": build_amplicon_preflight(name="clean.db").read_bytes()}
    for filename, sql, _column in _DIRTY_PREFLIGHTS:
        db = build_amplicon_preflight(name=filename)
        conn = sqlite3.connect(db)
        conn.execute(sql)
        conn.commit()
        conn.close()
        blobs[filename] = db.read_bytes()

    # Store each blob on its own pool
    pool_idx_by_filename = {}
    for filename, blob in blobs.items():
        pool_idx_by_filename[filename] = await postgres_pool.fetchval(
            "INSERT INTO qiita.sequenced_pool"
            " (sequencing_run_idx, run_preflight_blob, run_preflight_filename, created_by_idx)"
            " VALUES ($1, $2, $3, $4) RETURNING idx",
            run_idx,
            blob,
            filename,
            owner_idx,
        )
    expected = [
        UnopenablePreflight(
            pool_idx_by_filename[filename], run_idx, filename, _open_error(blobs[filename], column)
        )
        for filename, _sql, column in _DIRTY_PREFLIGHTS
    ]
    yield set(pool_idx_by_filename.values()), expected
    await delete_idxs(postgres_pool, "sequenced_pool", pool_idx_by_filename.values())
    await delete_idxs(postgres_pool, "sequencing_run", run_idx)
    await delete_principal(postgres_pool, owner_idx)


@pytest.mark.db
async def test_find_unopenable_preflights(postgres_pool, stored_preflights):
    """Tests the case where a run holds a clean stored pre-flight and two that this
    build's patches refuse: the two are reported in pool order, with the library's
    reason. Other tests' pools share the database, so only this run's are compared."""
    own_pool_idxs, expected = stored_preflights
    check = await find_unopenable_preflights(postgres_pool)
    own = [u for u in check.unopenable if u.sequenced_pool_idx in own_pool_idxs]
    assert own == expected
    assert check.checked >= len(own_pool_idxs)


@pytest.mark.parametrize(
    ("scan", "exit_code", "expected_out"),
    [
        (
            _SCAN,
            1,
            "stored pre-flights checked: 3\n"
            "unopenable: 1\n"
            "  sequenced_pool 7 (sequencing_run 2, padded.db): ValueError: padded sample_name\n",
        ),
        (
            PreflightCheck(checked=3, unopenable=[]),
            0,
            "stored pre-flights checked: 3\nunopenable: 0\n",
        ),
    ],
    ids=["unopenable", "clean"],
)
def test__handle(monkeypatch, capsys, scan, exit_code, expected_out):
    """Tests the case where the scan finds unopenable pre-flights, or none: each is
    listed, and the command fails only when there is one."""
    closed = []

    class _Pool:
        async def close(self):
            closed.append(True)

    async def _open(url):
        return _Pool()

    async def _find(pool):
        return scan

    monkeypatch.setenv("DATABASE_URL", "postgresql://x")
    monkeypatch.setattr(cli, "open_admin_pool", _open)
    monkeypatch.setattr(cli, "find_unopenable_preflights", _find)

    result = main(["check-stored-preflights"])
    assert (result, capsys.readouterr().out, closed) == (exit_code, expected_out, [True])
