"""End-to-end masked-read export against live components: real Flight wire, real
Rust streaming DoGet, real ticket signing, and the real `read_masked` redaction —
all driven through the actual `qiita-admin masked-read-export` CLI `main()`.

This is the layer the faked CLI unit tests (qiita-control-plane/tests/cli/
test_admin_cli.py, which stub both httpx and pyarrow.flight) cannot reach. It
proves, against the running data-plane process + integration Postgres:

  * a REAL signed ticket minted by the REAL CP route (`sign_ticket` over the
    `{prep_sample_idx, mask_idx}` filter) is accepted by the data plane;
  * the data plane's `read_masked` view REDACTS every non-'pass' read (host/QC
    hits never cross the Flight boundary) — the privacy invariant — while the
    'pass' reads stream through;
  * the real `FlightStreamReader` feeds the CLI's DuckDB+miint writer, producing
    correct parquet AND paired fastq (R1/R2) on the operator's disk.

The manifest + per-sample ticket are produced by calling the REAL route
functions in-process (`export_masked_read_manifest` /
`create_masked_read_export_ticket`) against the integration Postgres and the data
plane's HMAC secret; the single-pass CLI's synchronous HTTP layer
(`_common.httpx.request`) is bridged to those precomputed real responses so
`main()` can run unchanged. The HTTP transport + the system_admin/scope auth
gates are covered exhaustively by the ASGI route tests in
qiita-control-plane/tests/routes/test_admin_masked_read_export.py; here the value
is everything downstream of a correctly-signed ticket.

Shared fixtures (`data_plane`, `hmac_secret`, `postgres_pool`,
`human_admin_session`, `ducklake_connect`) live in conftest.py.
"""

from __future__ import annotations

import secrets
from pathlib import Path

import duckdb
import httpx
import pytest
from qiita_common.api_paths import (
    LOOPBACK_HOST,
    PATH_ADMIN_MASKED_READ_EXPORT_TICKET,
    PATH_ADMIN_SEQUENCED_POOL_MASKED_READ_EXPORT,
)
from qiita_common.models import MaskedReadExportTicketRequest, ReadMaskReason

from conftest import ducklake_connect


def _u8(values: list[int]) -> str:
    """A UTINYINT[] literal — read.qual1/qual2 are PHRED arrays in that type."""
    return "[" + ",".join(str(v) for v in values) + "]::UTINYINT[]"


def _parse_fastq(path: Path) -> dict[str, tuple[str, str]]:
    """Parse a 4-line-per-record FASTQ into {read_id: (sequence, qual)}; the read
    order over the Flight stream isn't guaranteed, so we key by id, not position."""
    lines = path.read_text().splitlines()
    out: dict[str, tuple[str, str]] = {}
    for i in range(0, len(lines), 4):
        out[lines[i][1:]] = (lines[i + 1], lines[i + 3])  # strip leading '@'
    return out


def _bridge_http(manifest_dict: dict, tickets_by_prep: dict[int, str], pool_idx: int):
    """A synchronous httpx.request stand-in that serves the precomputed REAL
    manifest (GET) and the precomputed REAL signed ticket for each sample (POST,
    keyed off the request body's prep_sample_idx)."""
    manifest_suffix = PATH_ADMIN_SEQUENCED_POOL_MASKED_READ_EXPORT.format(
        sequenced_pool_idx=pool_idx
    )

    def fake_request(method, url, headers=None, json=None, params=None, timeout=None):
        if method == "GET" and url.endswith(manifest_suffix):
            return httpx.Response(200, json=manifest_dict, request=httpx.Request(method, url))
        if method == "POST" and url.endswith(PATH_ADMIN_MASKED_READ_EXPORT_TICKET):
            ticket = tickets_by_prep[json["prep_sample_idx"]]
            return httpx.Response(201, json={"ticket": ticket}, request=httpx.Request(method, url))
        return httpx.Response(404, request=httpx.Request(method, url))

    return fake_request


@pytest.fixture
async def seeded(postgres_pool, human_admin_session):
    """One sequenced_pool with a single sequenced sample (accession set) plus a
    mask_definition, in the integration Postgres. Returns the ids the DuckLake
    seed + the export need; FK-reverse cleanup on teardown."""
    from qiita_control_plane.repositories.mask_definition import mint_mask_definition
    from qiita_control_plane.testing.db_seeds import (
        seed_biosample,
        seed_sequenced_prep_sample,
        seed_sequenced_sample_subtype,
    )

    owner = human_admin_session["principal_idx"]
    token = secrets.token_hex(4)
    accession = f"SAMN{token}"

    bs = await seed_biosample(postgres_pool, owner_idx=owner, created_by_idx=owner)
    await postgres_pool.execute(
        "UPDATE qiita.biosample SET biosample_accession = $1 WHERE idx = $2", accession, bs
    )
    ps = await seed_sequenced_prep_sample(postgres_pool, biosample_idx=bs, owner_idx=owner)
    run_idx, pool_idx, ss = await seed_sequenced_sample_subtype(
        postgres_pool, prep_sample_idx=ps, owner_idx=owner, sequenced_pool_item_id=f"item-{token}"
    )
    async with postgres_pool.acquire() as conn:
        mask = await mint_mask_definition(
            conn,
            filter_workflow="host_filter",
            filter_version="1.0.0",
            params={"k": token},
            principal_idx=owner,
        )
    mask_idx = mask["mask_idx"]

    yield {
        "accession": accession,
        "prep_sample_idx": ps,
        "run_idx": run_idx,
        "pool_idx": pool_idx,
        "mask_idx": mask_idx,
    }

    await postgres_pool.execute("DELETE FROM qiita.sequenced_sample WHERE idx = $1", ss)
    await postgres_pool.execute("DELETE FROM qiita.sequenced_pool WHERE idx = $1", pool_idx)
    await postgres_pool.execute("DELETE FROM qiita.sequencing_run WHERE idx = $1", run_idx)
    await postgres_pool.execute("DELETE FROM qiita.prep_sample WHERE idx = $1", ps)
    await postgres_pool.execute("DELETE FROM qiita.biosample WHERE idx = $1", bs)
    await postgres_pool.execute("DELETE FROM qiita.mask_definition WHERE mask_idx = $1", mask_idx)


async def test_masked_read_export_e2e_parquet_and_fastq(
    data_plane, seeded, postgres_pool, tmp_path, monkeypatch
):
    """Seed a paired sample with two 'pass' reads and one host-filtered read in
    DuckLake, then run the real CLI export for both formats against the live data
    plane. The host read must be absent from both outputs (redaction), and the two
    pass reads' bytes + phred+33 quals must round-trip into parquet and R1/R2 fastq.
    """
    from qiita_control_plane.cli import _common
    from qiita_control_plane.cli import admin as cli
    from qiita_control_plane.routes.admin import (
        create_masked_read_export_ticket,
        export_masked_read_manifest,
    )

    prep = seeded["prep_sample_idx"]
    mask_idx = seeded["mask_idx"]
    pool_idx = seeded["pool_idx"]
    run_idx = seeded["run_idx"]
    accession = seeded["accession"]
    secret = data_plane["secret"]

    # --- Seed DuckLake: a paired sample. r1/r3 pass; r2 is a host hit (redacted).
    # Zero trims, so passing reads stream through unchanged — the trim arithmetic
    # itself is pinned by the data plane's own Rust unit tests; here we prove the
    # WIRING + reason-redaction, not the substr math.
    #
    # These DuckLake rows are intentionally NOT torn down: the `data_plane`
    # catalog is module-scoped and reset on the next module load, and every test
    # gets a freshly-minted prep_sample_idx/mask_idx whose DoGet ticket filters to
    # exactly its own rows — so leftover rows can't contaminate a sibling test. A
    # second test added to this module must keep relying on that per-id scoping.
    conn = ducklake_connect(data_plane["data_path"])
    try:
        conn.execute(
            "INSERT INTO qiita_lake.read"
            " (prep_sample_idx, sequence_idx, read_id, sequence1, qual1, sequence2, qual2) VALUES"
            f" ({prep}, 1, 'r1', 'ACGTACGT', {_u8([40] * 8)}, 'TTTTGGGG', {_u8([30] * 8)}),"
            f" ({prep}, 2, 'r2', 'AAAACCCC', {_u8([40] * 8)}, 'GGGGCCCC', {_u8([40] * 8)}),"
            f" ({prep}, 3, 'r3', 'CCCCAAAA', {_u8([35] * 8)}, 'TTAATTAA', {_u8([20] * 8)})"
        )
        conn.execute(
            "INSERT INTO qiita_lake.read_mask"
            " (mask_idx, prep_sample_idx, sequence_idx, reason,"
            "  left_trim1, right_trim1, left_trim2, right_trim2) VALUES"
            f" ({mask_idx}, {prep}, 1, '{ReadMaskReason.PASS.value}', 0, 0, 0, 0),"
            f" ({mask_idx}, {prep}, 2, '{ReadMaskReason.HOST_RYPE.value}', 0, 0, 0, 0),"
            f" ({mask_idx}, {prep}, 3, '{ReadMaskReason.PASS.value}', 0, 0, 0, 0)"
        )
    finally:
        conn.close()

    # --- Real CP routes (in-process): roster manifest + a real signed ticket/sample.
    manifest_dict = (
        await export_masked_read_manifest(
            sequenced_pool_idx=pool_idx,
            pool=postgres_pool,
            _role=None,
            _scope=None,
            mask_idx=mask_idx,
        )
    ).model_dump()
    tickets_by_prep: dict[int, str] = {}
    for sample in manifest_dict["samples"]:
        resp = await create_masked_read_export_ticket(
            body=MaskedReadExportTicketRequest(
                prep_sample_idx=sample["prep_sample_idx"], mask_idx=mask_idx
            ),
            hmac_secret=secret,
            _role=None,
            _scope=None,
        )
        tickets_by_prep[sample["prep_sample_idx"]] = resp.ticket

    monkeypatch.setenv("QIITA_TOKEN", "qk_admin")
    monkeypatch.setattr(
        _common.httpx, "request", _bridge_http(manifest_dict, tickets_by_prep, pool_idx)
    )
    dp_url = f"grpc://{LOOPBACK_HOST}:{data_plane['port']}"
    stem = f"{accession}.{run_idx}.{pool_idx}.{prep}"

    def _run_export(fmt: str, out_dir: Path) -> int:
        out_dir.mkdir()
        return cli.main(
            [
                "masked-read-export",
                "--sequenced-pool-idx",
                str(pool_idx),
                "--mask-idx",
                str(mask_idx),
                "--format",
                fmt,
                "--output-dir",
                str(out_dir),
                "--data-plane-url",
                dp_url,
            ]
        )

    # --- parquet: exactly the two pass reads, with their real bytes. ---
    pq_dir = tmp_path / "parquet"
    assert _run_export("parquet", pq_dir) == 0
    pq = pq_dir / f"{stem}.parquet"
    assert pq.is_file()
    rows = (
        duckdb.connect(":memory:")
        .execute(f"SELECT read_id, sequence1, sequence2 FROM read_parquet('{pq}') ORDER BY read_id")
        .fetchall()
    )
    assert [r[0] for r in rows] == ["r1", "r3"]  # r2 (host_rype) redacted by the view
    by_id = {r[0]: r for r in rows}
    assert by_id["r1"][1] == "ACGTACGT" and by_id["r1"][2] == "TTTTGGGG"
    assert by_id["r3"][1] == "CCCCAAAA" and by_id["r3"][2] == "TTAATTAA"

    # --- fastq: paired → R1/R2, redaction holds, quals encode phred+33. ---
    fq_dir = tmp_path / "fastq"
    assert _run_export("fastq", fq_dir) == 0
    r1 = _parse_fastq(fq_dir / f"{stem}.R1.fastq")
    r2 = _parse_fastq(fq_dir / f"{stem}.R2.fastq")
    assert set(r1) == {"r1", "r3"} and set(r2) == {"r1", "r3"}  # host read absent
    # R1 carries sequence1/qual1; R2 carries sequence2/qual2. Q40→'I', Q35→'D',
    # Q30→'?', Q20→'5' (value + 33 as ASCII).
    assert r1["r1"] == ("ACGTACGT", "IIIIIIII")
    assert r1["r3"] == ("CCCCAAAA", "DDDDDDDD")
    assert r2["r1"] == ("TTTTGGGG", "????????")
    assert r2["r3"] == ("TTAATTAA", "55555555")
    # A paired sample never produces the single-file form.
    assert not (fq_dir / f"{stem}.fastq").exists()
