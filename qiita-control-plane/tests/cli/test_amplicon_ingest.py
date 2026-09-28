"""Unit tests for the golay-demux submission CLI (cli/user/amplicon.py).

Two surfaces, pure-unit (no Postgres):
  * `_read_amplicon_preflight_rows` — the preflight reader (kl-run-preflight's
    `get_amplicon_sample_info`), exercised end-to-end against a REAL kl-run-preflight
    SQLite built from the pinned good_amplicon_v1 fixture.
  * `_handle_submit_golay_demux` — the full submit flow, HTTP mocked, asserting the
    run/pool/sample setup and the ONE pool-scoped golay-demux ticket, including the
    barcode_map + FASTQ paths carried in action_context.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import httpx
import pytest

from qiita_control_plane.cli import _common
from qiita_control_plane.cli.user import main
from qiita_control_plane.cli.user.amplicon import _read_amplicon_preflight_rows

_AMPLICON_V1_CSV = Path(__file__).parent / "data" / "good_amplicon_v1.txt"


class _RaisingParser:
    """Stand-in for argparse.ArgumentParser whose `error` raises instead of
    calling sys.exit, so tests can assert on the message."""

    class Error(Exception):
        pass

    def error(self, message: str):
        raise self.Error(message)


def _build_amplicon_preflight(tmp_path: Path, *, populate_accessions: bool = True) -> Path:
    """Build a real kl-run-preflight SQLite from the pinned amplicon_v1 fixture.

    Mirrors conftest's `build_case5_preflight`: parse the legacy sheet with
    run_preflight's own loader (so the seam is exercised against the true schema and
    the real `get_amplicon_sample_info`), then populate the biosample + bioproject
    accessions the reader REQUIRES (left NULL by the fixture; set upstream in
    production) via plain sqlite.
    """
    from run_preflight.legacy.api import migrate_legacy_csv_to_db_file

    db = tmp_path / "amplicon_v1.db"
    migrate_legacy_csv_to_db_file(str(_AMPLICON_V1_CSV), str(db))
    if populate_accessions:
        conn = sqlite3.connect(db)
        conn.execute("UPDATE input_sample SET biosample_accession = 'BIO_' || sample_name")
        conn.execute("UPDATE project SET bioproject_accession = 'PRJNA' || external_project_id")
        conn.commit()
        conn.close()
    return db


# ---------------------------------------------------------------------------
# _read_amplicon_preflight_rows — the preflight reader, against a REAL preflight
# ---------------------------------------------------------------------------


def test_read_amplicon_preflight_rows_v1(tmp_path):
    db = _build_amplicon_preflight(tmp_path)
    rows, run_info = _read_amplicon_preflight_rows(db, _RaisingParser())

    assert len(rows) == 181
    # Every row carries a Golay barcode + its orientation flag, keyed to a resolved
    # biosample/bioproject accession.
    assert all(r.barcode for r in rows)
    assert all(r.barcodes_are_rc is True for r in rows)  # EMP 515rcbc set
    assert all(r.biosample_accession.startswith("BIO_") for r in rows)
    assert all(r.primary_project_accession.startswith("PRJNA") for r in rows)
    # prepped_sample_idx is the unique pool-item key.
    assert len({r.prepped_sample_idx for r in rows}) == len(rows)

    # The v1 fixture leaves external_run_id NULL but records the instrument model,
    # so the reader surfaces (None, model) and the caller must supply the run id.
    assert run_info.instrument_run_id is None
    assert run_info.instrument_model == "Illumina MiSeq"


def test_read_amplicon_preflight_rows_fails_on_missing_accession(tmp_path):
    """A preflight without the required biosample/bioproject accessions fails
    fast: `get_amplicon_sample_info` raises and the CLI surfaces its message."""
    db = _build_amplicon_preflight(tmp_path, populate_accessions=False)
    with pytest.raises(_RaisingParser.Error):
        _read_amplicon_preflight_rows(db, _RaisingParser())


def test_read_amplicon_preflight_rows_rejects_non_sqlite(tmp_path):
    bad = tmp_path / "not.db"
    bad.write_bytes(b"this is not a sqlite file")
    with pytest.raises(_RaisingParser.Error):
        _read_amplicon_preflight_rows(bad, _RaisingParser())


# ---------------------------------------------------------------------------
# _handle_submit_golay_demux — the full submit flow, HTTP mocked
# ---------------------------------------------------------------------------


def _stub_submit_flow(monkeypatch, captured: dict) -> None:
    """Route each POST/GET of the submit flow to a canned response, recording every
    request. Accession lookups resolve every requested accession to a deterministic
    idx; the pool roster starts empty so every sample is created."""
    captured["requests"] = []
    counter = {"sample": 0}

    def fake_request(method, url, headers=None, json=None, params=None, timeout=None):
        captured["requests"].append({"method": method, "url": url, "json": json})

        def resp(status, body):
            return httpx.Response(status, json=body, request=httpx.Request(method, url))

        if url.endswith("/auth/whoami"):
            return resp(200, {"kind": "human", "principal_idx": 7})
        if url.endswith("/lookup-by-accession"):  # biosample or study
            accs = (json or {}).get("accessions", [])
            return resp(200, {"resolved": {a: 1000 + i for i, a in enumerate(accs)}, "missing": []})
        if url.endswith("/sequenced-pool"):
            return resp(201, {"sequenced_pool_idx": 50})
        if url.rstrip("/").endswith("/sequencing-run"):
            return resp(201, {"sequencing_run_idx": 40})
        if url.endswith("/sequenced-sample/list"):  # pool roster GET (empty → all created)
            return resp(200, {"samples": []})
        if "/sequenced-pool/" in url and url.endswith("/sequenced-sample"):  # create
            counter["sample"] += 1
            n = counter["sample"]
            return resp(201, {"prep_sample_idx": 100 + n, "sequenced_sample_idx": 200 + n})
        if url.endswith("/work-ticket"):
            return resp(201, {"work_ticket_idx": 999})
        raise AssertionError(f"unexpected request to {url}")

    monkeypatch.setattr(_common.httpx, "request", fake_request)
    monkeypatch.setenv("QIITA_TOKEN", "qk_test")


def _fastq_set(tmp_path: Path) -> tuple[Path, Path, Path]:
    i1, r1, r2 = tmp_path / "I1.fastq", tmp_path / "R1.fastq", tmp_path / "R2.fastq"
    for p in (i1, r1, r2):
        p.write_text("")
    return i1, r1, r2


def test_submit_golay_demux_builds_barcode_map_and_one_ticket(monkeypatch, tmp_path):
    db = _build_amplicon_preflight(tmp_path)
    i1, r1, r2 = _fastq_set(tmp_path)

    captured: dict = {}
    _stub_submit_flow(monkeypatch, captured)

    rc = main(
        [
            "--base-url",
            "https://q.example.test",
            "submit-golay-demux",
            "--index-reads-path",
            str(i1),
            "--forward-reads-path",
            str(r1),
            "--reverse-reads-path",
            str(r2),
            "--preflight-blob",
            str(db),
            "--instrument-run-id",
            "M05314_260101",
            "--prep-protocol-idx",
            "5",
        ]
    )
    assert rc == 0

    ticket_posts = [
        r
        for r in captured["requests"]
        if r["method"] == "POST" and r["url"].endswith("/work-ticket")
    ]
    # Pool-scoped: exactly ONE golay-demux ticket for the whole pool.
    assert len(ticket_posts) == 1
    ctx = ticket_posts[0]["json"]["action_context"]
    assert ctx["index_reads_path"] == str(i1)
    assert ctx["forward_reads_path"] == str(r1)
    assert ctx["reverse_reads_path"] == str(r2)

    barcode_map = ctx["barcode_map"]
    assert len(barcode_map) == 181
    # Each entry pairs a created prep_sample_idx with its Golay barcode + orientation.
    assert all(set(e) == {"prep_sample_idx", "barcode", "barcodes_are_rc"} for e in barcode_map)
    assert all(e["barcodes_are_rc"] is True for e in barcode_map)
    assert all(e["prep_sample_idx"] >= 101 for e in barcode_map)
    assert all(e["barcode"] for e in barcode_map)

    # The run row used the operator-supplied id (the v1 preflight's external_run_id
    # is NULL) and the model read from the preflight.
    run_posts = [
        r
        for r in captured["requests"]
        if r["method"] == "POST" and r["url"].rstrip("/").endswith("/sequencing-run")
    ]
    assert len(run_posts) == 1
    assert run_posts[0]["json"]["instrument_run_id"] == "M05314_260101"
    assert run_posts[0]["json"]["instrument_model"] == "Illumina MiSeq"


def test_submit_golay_demux_omits_reverse_when_absent(monkeypatch, tmp_path):
    db = _build_amplicon_preflight(tmp_path)
    i1, r1, _ = _fastq_set(tmp_path)

    captured: dict = {}
    _stub_submit_flow(monkeypatch, captured)

    rc = main(
        [
            "--base-url",
            "https://q.example.test",
            "submit-golay-demux",
            "--index-reads-path",
            str(i1),
            "--forward-reads-path",
            str(r1),
            "--preflight-blob",
            str(db),
            "--instrument-run-id",
            "M05314_260101",
            "--prep-protocol-idx",
            "5",
        ]
    )
    assert rc == 0
    ticket = next(r for r in captured["requests"] if r["url"].endswith("/work-ticket"))
    assert "reverse_reads_path" not in ticket["json"]["action_context"]


def test_submit_golay_demux_requires_run_id_when_preflight_null(monkeypatch, tmp_path):
    """The v1 preflight has a NULL external_run_id; without --instrument-run-id the
    submit fails BEFORE any network call (a blank run id would mint an
    unidentifiable sequencing_run)."""
    db = _build_amplicon_preflight(tmp_path)
    i1, r1, _ = _fastq_set(tmp_path)

    captured: dict = {}
    _stub_submit_flow(monkeypatch, captured)

    with pytest.raises(SystemExit) as ei:
        main(
            [
                "--base-url",
                "https://q.example.test",
                "submit-golay-demux",
                "--index-reads-path",
                str(i1),
                "--forward-reads-path",
                str(r1),
                "--preflight-blob",
                str(db),
                "--prep-protocol-idx",
                "5",
            ]
        )
    assert ei.value.code == 2  # argparse parser.error exit code
    assert captured["requests"] == []


def test_submit_golay_demux_rejects_relative_fastq_path(monkeypatch, tmp_path):
    db = _build_amplicon_preflight(tmp_path)
    captured: dict = {}
    _stub_submit_flow(monkeypatch, captured)

    with pytest.raises(SystemExit) as ei:
        main(
            [
                "--base-url",
                "https://q.example.test",
                "submit-golay-demux",
                "--index-reads-path",
                "relative/I1.fastq",
                "--forward-reads-path",
                str(tmp_path / "R1.fastq"),
                "--preflight-blob",
                str(db),
                "--prep-protocol-idx",
                "5",
            ]
        )
    assert ei.value.code == 2
    assert captured["requests"] == []
