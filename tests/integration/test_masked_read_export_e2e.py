"""End-to-end masked-read export against live components: real Flight wire, real
Rust streaming DoGet, real ticket signing, and the real `read_masked` redaction —
all driven through the actual `qiita-admin masked-read-export` CLI `main()`.

This is the layer the faked CLI unit tests (qiita-control-plane/tests/cli/
test_admin_cli.py, which stub both httpx and pyarrow.flight) cannot reach. It
proves, against the running data-plane process + integration Postgres:

  * a REAL signed ticket minted by the REAL CP route (`sign_ticket` over the
    `{prep_sample_idx, mask_idx}` filter) is accepted by the data plane;
  * the data plane's `read_masked` macro REDACTS every non-'pass' read (host/QC
    hits never cross the Flight boundary) — the privacy invariant — while the
    'pass' reads stream through;
  * the real `FlightStreamReader` feeds the CLI's DuckDB+miint writer, producing
    correct parquet AND paired fastq (R1/R2) on the operator's disk.

The roster, the per-sample tickets and the mask handles are produced by calling
the REAL route functions in-process (`export_masked_read_manifest`,
`create_masked_read_export_ticket`, `mint_exported_mask_processing_handle`,
`mint_exported_mask_identifier_map`) against the integration Postgres and the data
plane's Ed25519 signing key; the single-pass CLI's synchronous HTTP layer
(`_common.httpx.request`) is bridged to those precomputed real responses so
`main()` can run unchanged. `test_masked_read_export_e2e_across_a_mask_group`
exports a pool whose samples resolve to two masks of one group. The HTTP transport
+ the system_admin/scope auth gates are covered exhaustively by the ASGI route tests in
qiita-control-plane/tests/routes/test_admin_masked_read_export.py; here the value
is everything downstream of a correctly-signed ticket.

Shared fixtures (`data_plane`, `signing_key`, `postgres_pool`,
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
    URL_EXPORTED_IDENTIFIER_MASK,
    URL_EXPORTED_PROCESSING_MASK,
)
from qiita_common.models import (
    ExportedMaskIdentifierRequest,
    ExportedMaskProcessingRequest,
    MaskedReadExportTicketRequest,
    ReadMaskReason,
)
from qiita_control_plane.repositories.block import MASK_SAMPLE_COMPLETED
from qiita_control_plane.testing.db_teardown import teardown_entity_graph

from conftest import ducklake_connect


def _u8(values: list[int]) -> str:
    """A UTINYINT[] literal — read.qual1/qual2 are PHRED arrays in that type."""
    return "[" + ",".join(str(v) for v in values) + "]::UTINYINT[]"


def _parse_fastq(path: Path) -> dict[str, tuple[str, str]]:
    """Parse a 4-line-per-record gzip FASTQ into {read_id: (sequence, qual)}; the
    read order over the Flight stream isn't guaranteed, so we key by id, not
    position. The export writes `<stem>.fastq.gz` (FORMAT FASTQ + COMPRESSION
    'gzip')."""
    import gzip

    with gzip.open(path, "rt") as fh:
        lines = fh.read().splitlines()
    out: dict[str, tuple[str, str]] = {}
    for i in range(0, len(lines), 4):
        out[lines[i][1:]] = (lines[i + 1], lines[i + 3])  # strip leading '@'
    return out


async def _real_bridge(postgres_pool, secret: bytes, *, pool_idx: int, mask_idx: int, owner: int):
    """Call the real routes in-process for the roster, a signed ticket per processed
    sample under its resolved mask, and the mask handles; return the roster and a
    synchronous httpx.request stand-in serving those responses."""
    from types import SimpleNamespace

    from qiita_control_plane.routes.admin import (
        create_masked_read_export_ticket,
        export_masked_read_manifest,
    )
    from qiita_control_plane.routes.exported_identifier import mint_exported_mask_identifier_map
    from qiita_control_plane.routes.exported_processing import (
        mint_exported_mask_processing_handle,
    )

    caller = SimpleNamespace(principal_idx=owner)
    roster = (
        await export_masked_read_manifest(
            sequenced_pool_idx=pool_idx,
            pool=postgres_pool,
            _role=None,
            _scope=None,
            mask_idx=mask_idx,
        )
    ).model_dump(mode="json")
    tickets: dict[tuple[int, int], str] = {}
    by_mask: dict[int, list[int]] = {}
    for sample in roster["samples"]:
        if sample["resolution"]["state"] != "processed":
            continue
        key = (sample["prep_sample_idx"], sample["resolution"]["definition_idx"])
        resp = await create_masked_read_export_ticket(
            body=MaskedReadExportTicketRequest(prep_sample_idx=key[0], mask_idx=key[1]),
            pool=postgres_pool,
            signing_key=secret,
            _role=None,
            _scope=None,
        )
        tickets[key] = resp.ticket
        by_mask.setdefault(key[1], []).append(key[0])
    mask_handles: dict[int, dict] = {}
    identifiers: dict[int, dict] = {}
    for mask, preps in by_mask.items():
        mask_handles[mask] = (
            await mint_exported_mask_processing_handle(
                body=ExportedMaskProcessingRequest(mask_idx=mask),
                pool=postgres_pool,
                caller=caller,
                _scope=None,
            )
        ).model_dump(mode="json")
        identifiers[mask] = (
            await mint_exported_mask_identifier_map(
                body=ExportedMaskIdentifierRequest(mask_idx=mask, prep_sample_idx=preps),
                pool=postgres_pool,
                caller=caller,
                _scope=None,
            )
        ).model_dump(mode="json")

    roster_suffix = PATH_ADMIN_SEQUENCED_POOL_MASKED_READ_EXPORT.format(sequenced_pool_idx=pool_idx)

    def fake_request(method, url, headers=None, json=None, params=None, timeout=None):
        request = httpx.Request(method, url)
        if method == "GET" and url.endswith(roster_suffix):
            return httpx.Response(200, json=roster, request=request)
        if method == "POST" and url.endswith(PATH_ADMIN_MASKED_READ_EXPORT_TICKET):
            ticket = tickets[(json["prep_sample_idx"], json["mask_idx"])]
            return httpx.Response(201, json={"ticket": ticket}, request=request)
        if method == "POST" and url.endswith(URL_EXPORTED_PROCESSING_MASK):
            return httpx.Response(201, json=mask_handles[json["mask_idx"]], request=request)
        if method == "POST" and url.endswith(URL_EXPORTED_IDENTIFIER_MASK):
            assert sorted(json["prep_sample_idx"]) == sorted(by_mask[json["mask_idx"]])
            return httpx.Response(201, json=identifiers[json["mask_idx"]], request=request)
        return httpx.Response(404, request=request)

    return roster, fake_request


def _seed_lake(data_path, *, prep: int, mask_idx: int, reads: list[tuple]) -> None:
    """Write `reads` — `(sequence_idx, read_id, sequence1, sequence2, reason)` — into
    `read` and `read_mask` under `mask_idx`, with zero trims."""
    conn = ducklake_connect(data_path)
    try:
        conn.execute(
            "INSERT INTO qiita_lake.read"
            " (prep_sample_idx, sequence_idx, read_id, sequence1, qual1, sequence2, qual2) VALUES "
            + ", ".join(
                f"({prep}, {idx}, '{rid}', '{s1}', {_u8([40] * len(s1))},"
                f" '{s2}', {_u8([30] * len(s2))})"
                for idx, rid, s1, s2, _ in reads
            )
        )
        conn.execute(
            "INSERT INTO qiita_lake.read_mask"
            " (mask_idx, prep_sample_idx, sequence_idx, reason,"
            "  left_trim1, right_trim1, left_trim2, right_trim2) VALUES "
            + ", ".join(
                f"({mask_idx}, {prep}, {idx}, '{reason.value}', 0, 0, 0, 0)"
                for idx, _, _, _, reason in reads
            )
        )
    finally:
        conn.close()


@pytest.fixture
async def seeded(postgres_pool, human_admin_session):
    """One sequenced_pool with a single sequenced sample (accession set) plus a
    mask_definition, in the integration Postgres. Returns the ids the DuckLake
    seed + the export need."""
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
        "UPDATE qiita.biosample SET biosample_accession = $1 WHERE idx = $2",
        accession,
        bs,
    )
    ps = await seed_sequenced_prep_sample(postgres_pool, biosample_idx=bs, owner_idx=owner)
    run_idx, pool_idx, _ss = await seed_sequenced_sample_subtype(
        postgres_pool,
        prep_sample_idx=ps,
        owner_idx=owner,
        sequenced_pool_item_id=f"item-{token}",
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

    # First-class mask_sample completion gate: the tightened export route requires a
    # 'completed' gate row for (mask_idx, prep_sample) — in the real per-sample
    # read-mask workflow the finalize-mask-sample terminal step writes it. Seed it
    # directly so the fully-masked e2e sample is not 409'd as masking-incomplete.
    await postgres_pool.execute(
        "INSERT INTO qiita.mask_sample (mask_idx, prep_sample_idx, state) VALUES ($1, $2, $3)",
        mask_idx,
        ps,
        MASK_SAMPLE_COMPLETED,
    )

    yield {
        "accession": accession,
        "prep_sample_idx": ps,
        "run_idx": run_idx,
        "pool_idx": pool_idx,
        "mask_idx": mask_idx,
        "owner": owner,
    }

    await teardown_entity_graph(
        postgres_pool,
        study_idxs=[],
        biosample_idxs=[bs],
        prep_sample_idxs=[ps],
    )
    await postgres_pool.execute("DELETE FROM qiita.sequenced_pool WHERE idx = $1", pool_idx)
    await postgres_pool.execute("DELETE FROM qiita.sequencing_run WHERE idx = $1", run_idx)
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

    # --- Real CP routes (in-process): roster, a real signed ticket/sample, handles.
    _roster, bridge = await _real_bridge(
        postgres_pool, secret, pool_idx=pool_idx, mask_idx=mask_idx, owner=seeded["owner"]
    )
    monkeypatch.setenv("QIITA_TOKEN", "qk_admin")
    monkeypatch.setattr(_common.httpx, "request", bridge)
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
    r1 = _parse_fastq(fq_dir / f"{stem}.R1.fastq.gz")
    r2 = _parse_fastq(fq_dir / f"{stem}.R2.fastq.gz")
    assert set(r1) == {"r1", "r3"} and set(r2) == {"r1", "r3"}  # host read absent
    # R1 carries sequence1/qual1; R2 carries sequence2/qual2. Q40→'I', Q35→'D',
    # Q30→'?', Q20→'5' (value + 33 as ASCII).
    assert r1["r1"] == ("ACGTACGT", "IIIIIIII")
    assert r1["r3"] == ("CCCCAAAA", "DDDDDDDD")
    assert r2["r1"] == ("TTTTGGGG", "????????")
    assert r2["r3"] == ("TTAATTAA", "55555555")
    # A paired sample never produces the single-file form.
    assert not (fq_dir / f"{stem}.fastq.gz").exists()


async def test_masked_read_export_e2e_parquet_idempotent_recount(
    data_plane, seeded, postgres_pool, tmp_path, monkeypatch, capsys
):
    """Re-export idempotency over the REAL count_masked DoAction: a second run with
    nothing changed skips (the data plane's read_mask count matches the on-disk
    parquet record count); after a pass read is added, the next run sees the higher
    count and overwrites. Proves the cheap count(*) probe and the skip/overwrite
    decision end to end against the live data plane — the piece the faked CLI unit
    tests can't reach (they stub the count response)."""
    from qiita_control_plane.cli import _common
    from qiita_control_plane.cli import admin as cli

    prep = seeded["prep_sample_idx"]
    mask_idx = seeded["mask_idx"]
    pool_idx = seeded["pool_idx"]
    run_idx = seeded["run_idx"]
    accession = seeded["accession"]
    secret = data_plane["secret"]

    # Two 'pass' reads + one host hit (redacted): the on-disk parquet starts at 2
    # rows. (Per-id scoped, like the sibling test; left undisturbed on teardown.)
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

    # --- Real CP routes (in-process): roster, a real signed ticket/sample, handles.
    _roster, bridge = await _real_bridge(
        postgres_pool, secret, pool_idx=pool_idx, mask_idx=mask_idx, owner=seeded["owner"]
    )
    monkeypatch.setenv("QIITA_TOKEN", "qk_admin")
    monkeypatch.setattr(_common.httpx, "request", bridge)
    dp_url = f"grpc://{LOOPBACK_HOST}:{data_plane['port']}"
    stem = f"{accession}.{run_idx}.{pool_idx}.{prep}"

    # One out_dir reused across runs; the CLI creates it on the first run.
    out_dir = tmp_path / "exp"
    pq_path = out_dir / f"{stem}.parquet"
    argv = [
        "masked-read-export",
        "--sequenced-pool-idx",
        str(pool_idx),
        "--mask-idx",
        str(mask_idx),
        "--format",
        "parquet",
        "--output-dir",
        str(out_dir),
        "--data-plane-url",
        dp_url,
    ]

    def _rows() -> int:
        return (
            duckdb.connect(":memory:")
            .execute(f"SELECT count(*) FROM read_parquet('{pq_path}')")
            .fetchone()[0]
        )

    # Run 1: first export streams the two pass reads.
    assert cli.main(argv) == 0
    assert "exported 1 prep_sample(s)" in capsys.readouterr().out
    assert _rows() == 2

    # Run 2: nothing changed → live count matches on disk → skipped, file untouched.
    assert cli.main(argv) == 0
    assert "exported 0 prep_sample(s) (skipped 1 already up to date)" in capsys.readouterr().out
    assert _rows() == 2

    # Add a third pass read so the live count_masked now returns 3, not 2.
    conn = ducklake_connect(data_plane["data_path"])
    try:
        conn.execute(
            "INSERT INTO qiita_lake.read"
            " (prep_sample_idx, sequence_idx, read_id, sequence1, qual1, sequence2, qual2) VALUES"
            f" ({prep}, 4, 'r4', 'GGGGTTTT', {_u8([40] * 8)}, 'CCCCAAAA', {_u8([30] * 8)})"
        )
        conn.execute(
            "INSERT INTO qiita_lake.read_mask"
            " (mask_idx, prep_sample_idx, sequence_idx, reason,"
            "  left_trim1, right_trim1, left_trim2, right_trim2) VALUES"
            f" ({mask_idx}, {prep}, 4, '{ReadMaskReason.PASS.value}', 0, 0, 0, 0)"
        )
    finally:
        conn.close()

    # Run 3: count (3) differs from on disk (2) → re-stream and overwrite.
    assert cli.main(argv) == 0
    assert "exported 1 prep_sample(s)" in capsys.readouterr().out
    assert _rows() == 3


@pytest.fixture
async def group_seeded(seeded, postgres_pool):
    """`seeded` plus a second mask decided similar to its mask (`mask_2`, the newer
    one), and two more samples on the same pool: `ps_2`, completed under both masks,
    and `ps_3`, with no run under either."""
    from qiita_common.models import DefinitionKind, DefinitionPairVerdict
    from qiita_control_plane.definition_similarity import decide_pair, open_pair
    from qiita_control_plane.repositories.mask_definition import mint_mask_definition
    from qiita_control_plane.testing.db_seeds import (
        seed_biosample,
        seed_sequenced_prep_sample,
    )

    owner = seeded["owner"]
    async with postgres_pool.acquire() as conn:
        mask_2 = (
            await mint_mask_definition(
                conn,
                filter_workflow="host_filter",
                filter_version="1.0.0",
                params={"k": secrets.token_hex(4)},
                principal_idx=owner,
            )
        )["mask_idx"]
    assert mask_2 > seeded["mask_idx"]
    async with postgres_pool.acquire() as conn, conn.transaction():
        pair, _ = await open_pair(
            conn,
            kind=DefinitionKind.MASK,
            definition_idxs=[seeded["mask_idx"], mask_2],
            principal_idx=owner,
        )
        await decide_pair(
            conn,
            definition_pair_idx=pair["definition_pair_idx"],
            verdict=DefinitionPairVerdict.SIMILAR,
            reason="same filter, checked against a mock community",
            principal_idx=owner,
        )

    biosamples, preps, accessions = [], [], []
    for _ in range(2):
        bs = await seed_biosample(postgres_pool, owner_idx=owner, created_by_idx=owner)
        accession = f"SAMN{secrets.token_hex(4)}"
        await postgres_pool.execute(
            "UPDATE qiita.biosample SET biosample_accession = $1 WHERE idx = $2", accession, bs
        )
        ps = await seed_sequenced_prep_sample(postgres_pool, biosample_idx=bs, owner_idx=owner)
        await postgres_pool.execute(
            "INSERT INTO qiita.sequenced_sample"
            "  (prep_sample_idx, sequenced_pool_idx, sequenced_pool_item_id, created_by_idx)"
            " VALUES ($1, $2, $3, $4)",
            ps,
            seeded["pool_idx"],
            f"item-{secrets.token_hex(4)}",
            owner,
        )
        biosamples.append(bs)
        preps.append(ps)
        accessions.append(accession)
    ps_2, ps_3 = preps
    await postgres_pool.executemany(
        "INSERT INTO qiita.mask_sample (mask_idx, prep_sample_idx, state) VALUES ($1, $2, $3)",
        [(seeded["mask_idx"], ps_2, MASK_SAMPLE_COMPLETED), (mask_2, ps_2, MASK_SAMPLE_COMPLETED)],
    )

    yield {
        **seeded,
        "mask_2": mask_2,
        "ps_2": ps_2,
        "accession_2": accessions[0],
        "ps_3": ps_3,
    }

    await postgres_pool.execute(
        "DELETE FROM qiita.definition_pair WHERE definition_pair_idx = $1",
        pair["definition_pair_idx"],
    )
    await teardown_entity_graph(
        postgres_pool, study_idxs=[], biosample_idxs=biosamples, prep_sample_idxs=preps
    )
    await postgres_pool.execute("DELETE FROM qiita.mask_definition WHERE mask_idx = $1", mask_2)


async def test_masked_read_export_e2e_across_a_mask_group(
    data_plane, group_seeded, postgres_pool, tmp_path, monkeypatch, capsys
):
    """A pool whose samples resolve to two masks of one group, exported by naming the
    older mask: the first sample's reads come from the older mask, the second's from
    the newer one (it also has a different pass set under the older mask, which must
    not be read), the third is listed and skipped, and the manifest names each file
    by the sample's and its mask's public handles."""
    import json

    from qiita_control_plane.cli import _common
    from qiita_control_plane.cli import admin as cli
    from qiita_control_plane.cli.admin.masked_export import MANIFEST_NAME

    g = group_seeded
    ps_1, ps_2, ps_3 = g["prep_sample_idx"], g["ps_2"], g["ps_3"]
    mask_1, mask_2 = g["mask_idx"], g["mask_2"]
    pass_, host = ReadMaskReason.PASS, ReadMaskReason.HOST_RYPE
    g_path = data_plane["data_path"]
    _seed_lake(
        g_path,
        prep=ps_1,
        mask_idx=mask_1,
        reads=[(1, "a1", "ACGTACGT", "TTTTGGGG", pass_), (2, "a2", "AAAACCCC", "GGGGCCCC", host)],
    )
    # ps_2 under the older mask: a pass set the export must not read.
    _seed_lake(
        g_path,
        prep=ps_2,
        mask_idx=mask_1,
        reads=[(1, "b1", "CCCCAAAA", "TTAATTAA", pass_), (2, "b2", "GGGGTTTT", "CCCCAAAA", pass_)],
    )
    conn = ducklake_connect(g_path)
    try:
        conn.execute(
            "INSERT INTO qiita_lake.read_mask"
            " (mask_idx, prep_sample_idx, sequence_idx, reason,"
            "  left_trim1, right_trim1, left_trim2, right_trim2) VALUES"
            f" ({mask_2}, {ps_2}, 1, '{host.value}', 0, 0, 0, 0),"
            f" ({mask_2}, {ps_2}, 2, '{pass_.value}', 0, 0, 0, 0)"
        )
    finally:
        conn.close()

    roster, bridge = await _real_bridge(
        postgres_pool,
        data_plane["secret"],
        pool_idx=g["pool_idx"],
        mask_idx=mask_1,
        owner=g["owner"],
    )
    by_prep = {s["prep_sample_idx"]: s["resolution"] for s in roster["samples"]}
    assert roster["group"] == [mask_1, mask_2]
    assert (by_prep[ps_1]["state"], by_prep[ps_1]["definition_idx"]) == ("processed", mask_1)
    assert (by_prep[ps_2]["state"], by_prep[ps_2]["definition_idx"]) == ("processed", mask_2)
    assert (by_prep[ps_3]["state"], by_prep[ps_3]["reason"]) == ("not_processed", "no_run")

    monkeypatch.setenv("QIITA_TOKEN", "qk_admin")
    monkeypatch.setattr(_common.httpx, "request", bridge)
    out_dir = tmp_path / "exp"
    rc = cli.main(
        [
            "masked-read-export",
            "--sequenced-pool-idx",
            str(g["pool_idx"]),
            "--mask-idx",
            str(mask_1),
            "--format",
            "parquet",
            "--output-dir",
            str(out_dir),
            "--data-plane-url",
            f"grpc://{LOOPBACK_HOST}:{data_plane['port']}",
        ]
    )
    assert rc == 0
    captured = capsys.readouterr()
    assert "exported 2 prep_sample(s)" in captured.out
    assert f"prep_sample {ps_3}: not_processed (no_run)" in captured.err

    name_1 = f"{g['accession']}.{g['run_idx']}.{g['pool_idx']}.{ps_1}.parquet"
    name_2 = f"{g['accession_2']}.{g['run_idx']}.{g['pool_idx']}.{ps_2}.parquet"

    def read_ids(name):
        rows = duckdb.connect(":memory:").execute(
            f"SELECT read_id FROM read_parquet('{out_dir / name}') ORDER BY read_id"
        )
        return [r[0] for r in rows.fetchall()]

    assert read_ids(name_1) == ["a1"]
    assert read_ids(name_2) == ["b2"]  # mask_2's pass set, not mask_1's (b1, b2)

    text = (out_dir / MANIFEST_NAME).read_text()
    files = json.loads(text)["files"]
    assert set(files) == {name_1, name_2}
    assert files[name_1]["biosample_accession"] == g["accession"]
    assert files[name_2]["biosample_accession"] == g["accession_2"]
    handles = {
        idx: await postgres_pool.fetchval(
            "SELECT export_processing_id FROM qiita.exported_processing"
            " WHERE mask_idx = $1 AND NOT retired",
            idx,
        )
        for idx in (mask_1, mask_2)
    }
    assert files[name_1]["export_processing_id"] == handles[mask_1]
    assert files[name_2]["export_processing_id"] == handles[mask_2]
    assert handles[mask_1] != handles[mask_2]
    for name, (ps, mask) in {name_1: (ps_1, mask_1), name_2: (ps_2, mask_2)}.items():
        assert files[name]["export_id"] == await postgres_pool.fetchval(
            "SELECT export_id FROM qiita.exported_identifier"
            " WHERE mask_idx = $1 AND prep_sample_idx = $2 AND NOT retired",
            mask,
            ps,
        )
    assert "_idx" not in text
