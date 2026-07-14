"""Unit tests for the PacBio HiFi ingest submission CLI (cli/user/pacbio.py).

Three surfaces, all pure-unit (no Postgres):
  * `_index_run_bams` / `_resolve_sample_bams` — the BAM glob + (barcode)
    disambiguation, exercised against a synthetic run folder on disk.
  * `_read_pacbio_preflight_rows` — the preflight reader (kl-run-preflight's
    `get_pacbio_sample_info`), exercised end-to-end against a REAL kl-run-preflight
    SQLite built from the pinned case-5 fixture (good_pacbio_absquantv11.csv).
  * `_handle_submit_pacbio_ingest` — the full submit flow, HTTP mocked, asserting
    the run/pool/sample setup and the per-sample bam-to-parquet fan-out.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import httpx
import pytest

from qiita_control_plane.cli import _common
from qiita_control_plane.cli.user import (
    _index_run_bams,
    _read_pacbio_preflight_rows,
    _resolve_sample_bams,
    main,
)
from qiita_control_plane.cli.user.pacbio import _validate_pacbio_protocol

_CASE5_CSV = Path(__file__).parent / "data" / "good_pacbio_absquantv11.csv"


class _RaisingParser:
    """Stand-in for argparse.ArgumentParser whose `error` raises instead of
    calling sys.exit, so tests can assert on the message."""

    class Error(Exception):
        pass

    def error(self, message: str):
        raise self.Error(message)


# ---------------------------------------------------------------------------
# BAM index + resolution
# ---------------------------------------------------------------------------


def _make_bam(run: Path, well: str, movie: str, barcode: str) -> Path:
    d = run / well / "hifi_reads"
    d.mkdir(parents=True, exist_ok=True)
    bam = d / f"{movie}.hifi_reads.{barcode}.bam"
    bam.write_text("x")
    return bam


def test_index_run_bams_keys_by_barcode_and_skips_unassigned(tmp_path):
    _make_bam(tmp_path, "1_A01", "m84_s1", "bc1")
    _make_bam(tmp_path, "1_A01", "m84_s1", "bc2")
    _make_bam(tmp_path, "1_A01", "m84_s1", "unassigned")  # dropped
    index, duplicated = _index_run_bams(tmp_path)
    assert set(index) == {"bc1", "bc2"}
    assert duplicated == set()
    # keyed on the *.bam file itself, under the SMRT-cell well dir
    assert index["bc1"].parts[-3:] == ("1_A01", "hifi_reads", "m84_s1.hifi_reads.bc1.bam")


def test_index_run_bams_quarantines_barcode_reused_across_cells(tmp_path):
    """A barcode under two SMRT cells cannot be disambiguated without a cell
    column, so it is left OUT of the index and recorded as duplicated."""
    _make_bam(tmp_path, "1_A01", "m84_s1", "bc1")
    _make_bam(tmp_path, "1_B01", "m84_s2", "bc1")  # collision
    _make_bam(tmp_path, "1_B01", "m84_s2", "bc3")
    index, duplicated = _index_run_bams(tmp_path)
    assert set(index) == {"bc3"}
    assert duplicated == {"bc1"}


def _row(barcode: str, idx: int = 1):
    from qiita_control_plane.cli.user import _PacbioPreflightRow

    return _PacbioPreflightRow(
        pacbio_sample_idx=idx,
        barcode=barcode,
        biosample_accession="BIO",
        primary_project_accession="99999",
        secondary_project_accessions=[],
        human_filtering=False,
        sheet_type="pacbio_absquant",
        twist_adaptor_id="t",
        syndna_is_twisted=False,
    )


def test_resolve_sample_bams_happy_path(tmp_path):
    _make_bam(tmp_path, "1_A01", "m84_s1", "bc1")
    resolved = _resolve_sample_bams([_row("bc1")], tmp_path, _RaisingParser())
    assert set(resolved) == {"bc1"}
    assert resolved["bc1"].name == "m84_s1.hifi_reads.bc1.bam"


def test_resolve_sample_bams_errors_on_missing(tmp_path):
    _make_bam(tmp_path, "1_A01", "m84_s1", "bc1")
    with pytest.raises(_RaisingParser.Error, match="no HiFi BAM found"):
        _resolve_sample_bams([_row("bcX", 5)], tmp_path, _RaisingParser())


def test_resolve_sample_bams_errors_on_cross_cell_barcode(tmp_path):
    """A barcode with BAMs in two SMRT cells can't be bound without the cell."""
    _make_bam(tmp_path, "1_A01", "m84_s1", "bc1")
    _make_bam(tmp_path, "1_B01", "m84_s2", "bc1")
    with pytest.raises(_RaisingParser.Error, match="reused across samples or SMRT cells"):
        _resolve_sample_bams([_row("bc1", 5)], tmp_path, _RaisingParser())


def test_resolve_sample_bams_errors_on_two_samples_sharing_a_barcode(tmp_path):
    """Two distinct samples claiming the same barcode can't be split between the
    BAM(s) without the SMRT cell — ambiguous, not a silent shared BAM."""
    _make_bam(tmp_path, "1_A01", "m84_s1", "bc1")
    with pytest.raises(_RaisingParser.Error, match="reused across samples or SMRT cells"):
        _resolve_sample_bams([_row("bc1", 1), _row("bc1", 2)], tmp_path, _RaisingParser())


def test_resolve_sample_bams_errors_on_empty_run_folder(tmp_path):
    with pytest.raises(_RaisingParser.Error, match="no HiFi BAMs"):
        _resolve_sample_bams([_row("bc1")], tmp_path, _RaisingParser())


# ---------------------------------------------------------------------------
# _validate_pacbio_protocol
# ---------------------------------------------------------------------------


def test_validate_protocol_rejects_twisted_without_adapter():
    from qiita_control_plane.cli.user import _PacbioPreflightRow

    row = _PacbioPreflightRow(
        pacbio_sample_idx=1,
        barcode="bc",
        biosample_accession="B",
        primary_project_accession="9",
        secondary_project_accessions=[],
        human_filtering=False,
        sheet_type="pacbio_absquant",
        twist_adaptor_id=None,
        syndna_is_twisted=True,
    )
    with pytest.raises(_RaisingParser.Error, match="twisted with no twist_adaptor_id"):
        _validate_pacbio_protocol(row, Path("pf.db"), _RaisingParser())


def test_validate_protocol_allows_untwisted_without_adapter():
    """syndna_is_twisted False + empty twist is protocol 2 — valid, no error."""
    from qiita_control_plane.cli.user import _PacbioPreflightRow

    row = _PacbioPreflightRow(
        pacbio_sample_idx=1,
        barcode="bc",
        biosample_accession="B",
        primary_project_accession="9",
        secondary_project_accessions=[],
        human_filtering=False,
        sheet_type="pacbio_absquant",
        twist_adaptor_id=None,
        syndna_is_twisted=False,
    )
    _validate_pacbio_protocol(row, Path("pf.db"), _RaisingParser())  # no raise


# ---------------------------------------------------------------------------
# _read_pacbio_preflight_rows — the preflight reader, against a REAL preflight
# ---------------------------------------------------------------------------


def test_read_preflight_rows_case5(build_case5_preflight):
    """The seam returns one row per sample for the real case-5 sheet, including
    the control blank (sample.3, which the reader resolves to the plate primary
    bioproject). twist filled + syndna_is_twisted False is the case-5 signature.
    The project accession is the ENA **bioproject** (what the study lookup keys
    on), and smrt_cell rides through from the reader when the preflight records it."""
    db = build_case5_preflight()
    conn = sqlite3.connect(db)
    # Record a SMRT cell on sample.1 only, to prove it threads onto the row.
    conn.execute(
        "UPDATE pacbio_sample SET smrt_cell_well_sample_id = '1_A01' WHERE barcode_id = 'bc3011'"
    )
    # Flip the project's human_filtering (fixture default False) so the assertion
    # proves the row reads the PROJECT flag — including the control (sample.3),
    # which inherits it via the view's plate-primary resolution — not a constant.
    conn.execute("UPDATE project SET human_filtering = 1")
    conn.commit()
    conn.close()

    rows = _read_pacbio_preflight_rows(db, _RaisingParser())
    # pacbio_sample_idx is the unique identity (ordered by the accessor); barcode is
    # only the BAM-locating key.
    assert [r.pacbio_sample_idx for r in rows] == [1, 2, 3]
    assert [r.barcode for r in rows] == ["bc3011", "bc0112", "bc9992"]
    assert [r.biosample_accession for r in rows] == ["BIO_sample.1", "BIO_sample.2", "BIO_sample.3"]
    assert [r.smrt_cell for r in rows] == ["1_A01", None, None]
    assert [r.human_filtering for r in rows] == [True, True, True]
    for r in rows:
        assert r.primary_project_accession == "PRJNA99999"  # control resolves to plate primary
        assert r.secondary_project_accessions == []
        assert r.sheet_type == "pacbio_absquant"
        assert r.twist_adaptor_id  # case 5: filled
        assert r.syndna_is_twisted is False


def test_read_preflight_rows_accepts_barcode_shared_across_samples(build_case5_preflight):
    """The reader does NOT reject a barcode shared by two samples at parse —
    pacbio_sample_idx is the identity now, so the reader returns both rows. The
    ambiguity is a BAM-resolution concern (see the _resolve_sample_bams tests), so
    the guard genuinely MOVED rather than vanished."""
    db = build_case5_preflight()
    conn = sqlite3.connect(db)
    conn.execute("UPDATE pacbio_sample SET barcode_id = 'bc3011' WHERE barcode_id = 'bc0112'")
    conn.commit()
    conn.close()
    rows = _read_pacbio_preflight_rows(db, _RaisingParser())  # no raise
    assert [r.pacbio_sample_idx for r in rows] == [1, 2, 3]  # distinct identities
    assert [r.barcode for r in rows].count("bc3011") == 2  # the shared barcode


def test_read_preflight_rows_fails_on_missing_accession(build_case5_preflight):
    """Unpopulated biosample / bioproject accessions are an operator-actionable
    fail-fast: `get_pacbio_sample_info` raises and the CLI surfaces its message."""
    db = build_case5_preflight(populate_accessions=False)
    with pytest.raises(_RaisingParser.Error, match="missing required accession"):
        _read_pacbio_preflight_rows(db, _RaisingParser())


# ---------------------------------------------------------------------------
# _handle_submit_pacbio_ingest — full flow, HTTP mocked
# ---------------------------------------------------------------------------


def _stub_submit_flow(
    monkeypatch,
    captured: dict,
    *,
    existing_samples: list[dict] | None = None,
    fail_ticket_when=None,
    conflict_ticket_when=None,
) -> None:
    """Route each POST/GET of the submit flow to a canned response and record
    every request.

    `existing_samples` seeds the pool roster GET (empty = fresh pool → every
    sample is created; pre-populated = a retry that reuses them). Each dict needs
    `sequenced_pool_item_id` + `prep_sample_idx`. `fail_ticket_when(body)` forces
    that ticket POST to 500 (real failure); `conflict_ticket_when(body)` forces it
    to 409 (already-done / in-flight → skip). sequenced-sample POSTs get a unique
    prep_sample_idx per call."""
    captured["requests"] = []
    counter = {"sample": 0}
    roster = {"samples": list(existing_samples or [])}

    def fake_request(method, url, headers=None, json=None, params=None, timeout=None):
        captured["requests"].append({"method": method, "url": url, "json": json})

        def resp(status, body):
            return httpx.Response(status, json=body, request=httpx.Request(method, url))

        if url.endswith("/auth/whoami"):
            return resp(200, {"kind": "human", "principal_idx": 7})
        if url.endswith("/lookup-by-accession") and "biosample" in url:
            return resp(
                200,
                {
                    "resolved": {"BIO_sample.1": 11, "BIO_sample.2": 12, "BIO_sample.3": 13},
                    "missing": [],
                },
            )
        if url.endswith("/lookup-by-accession"):  # study, keyed on the bioproject accession
            return resp(200, {"resolved": {"PRJNA99999": 900}, "missing": []})
        if url.endswith("/sequenced-pool"):
            return resp(201, {"sequenced_pool_idx": 50})
        if url.rstrip("/").endswith("/sequencing-run"):
            return resp(201, {"sequencing_run_idx": 40})
        if url.endswith("/sequenced-sample/list"):  # pool roster GET
            return resp(200, roster)
        if "/sequenced-pool/" in url and url.endswith("/sequenced-sample"):  # create
            counter["sample"] += 1
            n = counter["sample"]
            return resp(201, {"prep_sample_idx": 100 + n, "sequenced_sample_idx": 200 + n})
        if url.endswith("/work-ticket"):
            if fail_ticket_when is not None and fail_ticket_when(json):
                return resp(500, {"detail": "boom"})
            if conflict_ticket_when is not None and conflict_ticket_when(json):
                return resp(409, {"detail": "already ingested"})
            return resp(201, {"work_ticket_idx": 999})
        raise AssertionError(f"unexpected request to {url}")

    monkeypatch.setattr(_common.httpx, "request", fake_request)
    monkeypatch.setenv("QIITA_TOKEN", "qk_test")


_BASE_ARGS = [
    "--base-url",
    "https://q.example.test",
    "submit-pacbio-ingest",
]


def _submit_args(run, db, *, force=False):
    args = [
        *_BASE_ARGS,
        "--run-folder",
        str(run),
        "--preflight-blob",
        str(db),
        "--instrument-run-id",
        "m84137_260702",
        "--prep-protocol-idx",
        "3",
    ]
    if force:
        args.append("--force")
    return args


def test_submit_pacbio_ingest_fans_out_bam_to_parquet(monkeypatch, tmp_path, build_case5_preflight):
    db = build_case5_preflight()
    run = tmp_path / "run"
    for bc in ("bc3011", "bc0112", "bc9992"):
        _make_bam(run, "1_A01", "m84_s1", bc)

    captured: dict = {}
    _stub_submit_flow(monkeypatch, captured)

    rc = main(
        [
            "--base-url",
            "https://q.example.test",
            "submit-pacbio-ingest",
            "--run-folder",
            str(run),
            "--preflight-blob",
            str(db),
            "--instrument-run-id",
            "m84137_260702",
            "--instrument-model",
            "Revio",
            "--prep-protocol-idx",
            "3",
        ]
    )
    assert rc == 0

    ticket_posts = [
        r
        for r in captured["requests"]
        if r["method"] == "POST" and r["url"].endswith("/work-ticket")
    ]
    # One bam-to-parquet ingest ticket per sample.
    assert len(ticket_posts) == 3
    for r in ticket_posts:
        body = r["json"]
        assert body["action_id"] == "bam-to-parquet"
        assert body["scope_target"]["kind"] == "prep_sample"
        assert body["action_context"]["expect_unaligned"] is True
        assert body["action_context"]["bam_path"].endswith(".bam")
    # Each sample's own BAM path is forwarded (barcode -> its resolved file).
    bam_paths = sorted(r["json"]["action_context"]["bam_path"] for r in ticket_posts)
    assert [Path(p).name for p in bam_paths] == [
        "m84_s1.hifi_reads.bc0112.bam",
        "m84_s1.hifi_reads.bc3011.bam",
        "m84_s1.hifi_reads.bc9992.bam",
    ]
    # sequencing-run created with the PacBio platform.
    run_post = next(
        r
        for r in captured["requests"]
        if r["method"] == "POST" and r["url"].rstrip("/").endswith("/sequencing-run")
    )
    assert run_post["json"]["platform"] == "pacbio_smrt"
    # Each sequenced-sample is created with its pacbio_sample_idx as the
    # pool-item-id (NOT the barcode); the fixture's samples are idx 1/2/3.
    sample_posts = [
        r
        for r in captured["requests"]
        if r["method"] == "POST" and r["url"].endswith("/sequenced-sample")
    ]
    assert len(sample_posts) == 3
    assert sorted(r["json"]["sequenced_pool_item_id"] for r in sample_posts) == ["1", "2", "3"]


def test_submit_pacbio_ingest_ambiguous_barcode_aborts_before_network(
    monkeypatch, tmp_path, build_case5_preflight
):
    """A barcode reused across SMRT cells fails fast (exit 2) with NO network
    call — resolution happens before the flow's _run."""
    db = build_case5_preflight()
    run = tmp_path / "run"
    _make_bam(run, "1_A01", "m84_s1", "bc3011")
    _make_bam(run, "1_B01", "m84_s2", "bc3011")  # collide the first sample's barcode
    _make_bam(run, "1_A01", "m84_s1", "bc0112")
    _make_bam(run, "1_A01", "m84_s1", "bc9992")

    captured: dict = {}
    _stub_submit_flow(monkeypatch, captured)

    with pytest.raises(SystemExit) as ei:
        main(
            [
                "--base-url",
                "https://q.example.test",
                "submit-pacbio-ingest",
                "--run-folder",
                str(run),
                "--preflight-blob",
                str(db),
                "--instrument-run-id",
                "m84137_260702",
                "--prep-protocol-idx",
                "3",
            ]
        )
    assert ei.value.code == 2
    assert captured["requests"] == []  # aborted before any HTTP


def test_submit_pacbio_ingest_missing_bam_aborts_before_network(
    monkeypatch, tmp_path, build_case5_preflight
):
    """A sample whose barcode has no BAM fails fast (exit 2) before any HTTP,
    like the ambiguous case — no half-populated pool."""
    db = build_case5_preflight()
    run = tmp_path / "run"
    _make_bam(run, "1_A01", "m84_s1", "bc3011")
    _make_bam(run, "1_A01", "m84_s1", "bc0112")
    # bc9992 (sample.3) intentionally absent.

    captured: dict = {}
    _stub_submit_flow(monkeypatch, captured)
    with pytest.raises(SystemExit) as ei:
        main(_submit_args(run, db))
    assert ei.value.code == 2
    assert captured["requests"] == []


def test_submit_pacbio_ingest_resilient_to_ticket_failure(
    monkeypatch, tmp_path, build_case5_preflight
):
    """One bam-to-parquet ticket 500ing does NOT strand the others: the remaining
    tickets still POST, the summary records the failure, and the command exits 1."""
    db = build_case5_preflight()
    run = tmp_path / "run"
    for bc in ("bc3011", "bc0112", "bc9992"):
        _make_bam(run, "1_A01", "m84_s1", bc)

    captured: dict = {}
    # Fail exactly the ticket for sample bc0112.
    _stub_submit_flow(
        monkeypatch,
        captured,
        fail_ticket_when=lambda body: "bc0112" in body["action_context"]["bam_path"],
    )
    with pytest.raises(SystemExit) as ei:
        main(_submit_args(run, db))
    assert ei.value.code == 1  # partial fan-out surfaces non-zero
    # All three tickets were attempted (the failure did not abort the loop).
    ticket_posts = [
        r
        for r in captured["requests"]
        if r["method"] == "POST" and r["url"].endswith("/work-ticket")
    ]
    assert len(ticket_posts) == 3


def test_submit_pacbio_ingest_force_reaches_ticket_body(
    monkeypatch, tmp_path, build_case5_preflight
):
    db = build_case5_preflight()
    run = tmp_path / "run"
    for bc in ("bc3011", "bc0112", "bc9992"):
        _make_bam(run, "1_A01", "m84_s1", bc)

    captured: dict = {}
    _stub_submit_flow(monkeypatch, captured)
    rc = main(_submit_args(run, db, force=True))
    assert rc == 0
    ticket_posts = [
        r
        for r in captured["requests"]
        if r["method"] == "POST" and r["url"].endswith("/work-ticket")
    ]
    assert ticket_posts and all(r["json"]["force"] is True for r in ticket_posts)


def test_submit_pacbio_ingest_retry_reuses_existing_roster(
    monkeypatch, tmp_path, build_case5_preflight
):
    """A re-run against a pool that already has the samples creates NONE of them
    (create-missing), reuses their prep_sample_idx, and still fans out the tickets
    — the convergent-retry contract."""
    db = build_case5_preflight()
    run = tmp_path / "run"
    for bc in ("bc3011", "bc0112", "bc9992"):
        _make_bam(run, "1_A01", "m84_s1", bc)

    # Pool-item-id is str(pacbio_sample_idx) — the fixture's samples are idx 1/2/3.
    existing = [
        {
            "sequenced_pool_item_id": str(i + 1),
            "prep_sample_idx": 300 + i,
            "sequenced_sample_idx": 400 + i,
        }
        for i in range(3)
    ]
    captured: dict = {}
    _stub_submit_flow(monkeypatch, captured, existing_samples=existing)
    rc = main(_submit_args(run, db))
    assert rc == 0
    # No sequenced-sample was CREATED (all reused from the roster).
    create_posts = [
        r
        for r in captured["requests"]
        if r["method"] == "POST" and r["url"].endswith("/sequenced-sample")
    ]
    assert create_posts == []
    # Tickets still fan out, targeting the reused prep_sample_idx values.
    ticket_posts = [
        r
        for r in captured["requests"]
        if r["method"] == "POST" and r["url"].endswith("/work-ticket")
    ]
    assert sorted(r["json"]["scope_target"]["prep_sample_idx"] for r in ticket_posts) == [
        300,
        301,
        302,
    ]


def test_submit_pacbio_ingest_reused_sample_biosample_mismatch_fails(
    monkeypatch, tmp_path, build_case5_preflight
):
    """A re-run cannot silently change an existing sample's identity: if the roster
    row's biosample_idx differs from what this submission resolves, fail loud
    (exit 1) instead of reusing it and pretending the correction landed."""
    db = build_case5_preflight()
    run = tmp_path / "run"
    for bc in ("bc3011", "bc0112", "bc9992"):
        _make_bam(run, "1_A01", "m84_s1", bc)

    # pacbio_sample_idx 1 (barcode bc3011) resolves to biosample_idx 11 (BIO_sample.1)
    # in the stub, but the roster claims it maps to a different biosample — a
    # divergent re-run (pool-item-id is str(pacbio_sample_idx) = "1").
    existing = [{"sequenced_pool_item_id": "1", "prep_sample_idx": 300, "biosample_idx": 999}]
    captured: dict = {}
    _stub_submit_flow(monkeypatch, captured, existing_samples=existing)
    with pytest.raises(SystemExit) as ei:
        main(_submit_args(run, db))
    assert ei.value.code == 1


def test_submit_pacbio_ingest_409_ticket_is_skip_not_failure(
    monkeypatch, tmp_path, build_case5_preflight
):
    """A real re-submit: the samples exist AND their ingest tickets already
    COMPLETED (or are in-flight), so the work-ticket POSTs 409. Those are the
    convergence signal, not failures — the command records them as skipped and
    exits 0 (the operator must be able to tell already-done from a real failure)."""
    db = build_case5_preflight()
    run = tmp_path / "run"
    for bc in ("bc3011", "bc0112", "bc9992"):
        _make_bam(run, "1_A01", "m84_s1", bc)

    existing = [
        {
            "sequenced_pool_item_id": str(i + 1),
            "prep_sample_idx": 300 + i,
            "sequenced_sample_idx": 400 + i,
        }
        for i in range(3)
    ]
    captured: dict = {}
    _stub_submit_flow(
        monkeypatch, captured, existing_samples=existing, conflict_ticket_when=lambda body: True
    )
    rc = main(_submit_args(run, db))
    assert rc == 0  # all-already-done converges to success, not a failure exit


def test_read_preflight_rows_rejects_non_pacbio_sheet(build_case5_preflight):
    """A preflight whose sheet_type is not a PacBio sheet fails loud."""
    db = build_case5_preflight()
    conn = sqlite3.connect(db)
    # Retarget only THIS run's format row (updating all rows collides on the
    # (legacy_sheet_type, legacy_version) unique constraint).
    conn.execute(
        "UPDATE legacy_samplesheet_format SET legacy_sheet_type = 'bogus_sheet'"
        " WHERE legacy_format_idx = (SELECT legacy_format_idx FROM processing_run LIMIT 1)"
    )
    conn.commit()
    conn.close()
    with pytest.raises(_RaisingParser.Error, match="not a.*PacBio sheet"):
        _read_pacbio_preflight_rows(db, _RaisingParser())


def test_validate_protocol_rejects_twisted_on_metag():
    from qiita_control_plane.cli.user import _PacbioPreflightRow

    row = _PacbioPreflightRow(
        pacbio_sample_idx=1,
        barcode="bc",
        biosample_accession="B",
        primary_project_accession="9",
        secondary_project_accessions=[],
        human_filtering=False,
        sheet_type="pacbio_metag",
        twist_adaptor_id="t",
        syndna_is_twisted=True,
    )
    with pytest.raises(_RaisingParser.Error, match="twisted on a 'pacbio_metag'"):
        _validate_pacbio_protocol(row, Path("pf.db"), _RaisingParser())


def test_index_run_bams_skips_combined_bam_without_barcode(tmp_path):
    """A non-demuxed combined BAM (`<movie>.hifi_reads.bam`, no barcode field) is
    not indexed under a spurious 'hifi_reads' barcode."""
    d = tmp_path / "1_A01" / "hifi_reads"
    d.mkdir(parents=True)
    (d / "m84_s1.hifi_reads.bam").write_text("x")  # combined, no barcode
    (d / "m84_s1.hifi_reads.bc1.bam").write_text("x")
    index, duplicated = _index_run_bams(tmp_path)
    assert set(index) == {"bc1"}
    assert duplicated == set()
