"""Unit tests for the golay-demux submission CLI (cli/user/amplicon.py).

Two surfaces, pure-unit (no Postgres):
  * `_read_amplicon_preflight_rows` — the preflight reader (kl-run-preflight's
    `get_amplicon_sample_info`), exercised end-to-end against the committed migrated
    preflight SQLite (good_amplicon_v1.sqlite.gz).
  * `_handle_submit_golay_demux` — the full submit flow, HTTP mocked, asserting the
    run/pool/sample setup and the ONE pool-scoped golay-demux ticket, including the
    barcode_map and the CP-resolved bcl_input_dir carried in action_context.
"""

from __future__ import annotations

import httpx
import pytest

from qiita_control_plane.cli import _common
from qiita_control_plane.cli.user import main
from qiita_control_plane.cli.user.amplicon import _read_amplicon_preflight_rows

_RUN_ID = "20260925_SL00377_0008_ASC2267726-SC3"
_RESOLVED_BCL_DIR = f"/sequencing/{_RUN_ID}"


class _RaisingParser:
    """Stand-in for argparse.ArgumentParser whose `error` raises instead of
    calling sys.exit, so tests can assert on the message."""

    class Error(Exception):
        pass

    def error(self, message: str):
        raise self.Error(message)


# ---------------------------------------------------------------------------
# _read_amplicon_preflight_rows — the preflight reader, against a REAL preflight
# ---------------------------------------------------------------------------


def test_read_amplicon_preflight_rows_v1(build_amplicon_preflight):
    db = build_amplicon_preflight()
    rows = _read_amplicon_preflight_rows(db, _RaisingParser())

    assert len(rows) == 181
    # Every row carries a Golay barcode + its orientation flag, keyed to a resolved
    # biosample/bioproject accession.
    assert all(r.barcode for r in rows)
    assert all(r.barcodes_are_rc is True for r in rows)  # EMP 515rcbc set
    assert all(r.biosample_accession.startswith("BIO_") for r in rows)
    assert all(r.primary_project_accession.startswith("PRJNA") for r in rows)
    # prepped_sample_idx is the unique pool-item key.
    assert len({r.prepped_sample_idx for r in rows}) == len(rows)


def test_read_amplicon_preflight_rows_fails_on_missing_accession(build_amplicon_preflight):
    """A preflight without the required biosample/bioproject accessions fails
    fast: `get_amplicon_sample_info` raises and the CLI surfaces its message."""
    db = build_amplicon_preflight(populate_accessions=False)
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
    request. /run-folder/inspect resolves the run id to a bcl_input_dir + instrument
    facts; accession lookups resolve everything; the pool roster starts empty so
    every sample is created."""
    captured["requests"] = []
    counter = {"sample": 0}

    def fake_request(method, url, headers=None, json=None, params=None, timeout=None):
        captured["requests"].append({"method": method, "url": url, "json": json})

        def resp(status, body):
            return httpx.Response(status, json=body, request=httpx.Request(method, url))

        if url.endswith("/auth/whoami"):
            return resp(200, {"kind": "human", "principal_idx": 7})
        if url.endswith("/run-folder/inspect"):
            return resp(
                200,
                {
                    "path": _RESOLVED_BCL_DIR,
                    "platform": "illumina",
                    "illumina": {
                        "instrument_run_id": _RUN_ID,
                        "instrument_model": "Illumina MiSeq i100",
                    },
                },
            )
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


def test_submit_golay_demux_builds_barcode_map_and_one_ticket(
    monkeypatch, build_amplicon_preflight
):
    db = build_amplicon_preflight()

    captured: dict = {}
    _stub_submit_flow(monkeypatch, captured)

    rc = main(
        [
            "--base-url",
            "https://q.example.test",
            "submit-golay-demux",
            "--instrument-run-id",
            _RUN_ID,
            "--preflight-blob",
            str(db),
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
    # The CP-resolved run folder (from /run-folder/inspect), not a submitter path.
    assert ctx["bcl_input_dir"] == _RESOLVED_BCL_DIR
    assert ctx["amplicon"] is True
    assert "index_reads_path" not in ctx  # the run-id flow carries no FASTQ paths

    barcode_map = ctx["barcode_map"]
    assert len(barcode_map) == 181
    # Each entry pairs a created prep_sample_idx with its Golay barcode + orientation.
    assert all(set(e) == {"prep_sample_idx", "barcode", "barcodes_are_rc"} for e in barcode_map)
    assert all(e["barcodes_are_rc"] is True for e in barcode_map)
    assert all(e["prep_sample_idx"] >= 101 for e in barcode_map)
    assert all(e["barcode"] for e in barcode_map)

    # The run row used the id + model the CP read from RunInfo.xml via inspect.
    run_posts = [
        r
        for r in captured["requests"]
        if r["method"] == "POST" and r["url"].rstrip("/").endswith("/sequencing-run")
    ]
    assert len(run_posts) == 1
    assert run_posts[0]["json"]["instrument_run_id"] == _RUN_ID
    assert run_posts[0]["json"]["instrument_model"] == "Illumina MiSeq i100"


def test_submit_golay_demux_requires_run_id(monkeypatch, build_amplicon_preflight):
    """--instrument-run-id is required; without it argparse exits 2 before any
    network call (there is no run folder to resolve otherwise)."""
    db = build_amplicon_preflight()

    captured: dict = {}
    _stub_submit_flow(monkeypatch, captured)

    with pytest.raises(SystemExit) as ei:
        main(
            [
                "--base-url",
                "https://q.example.test",
                "submit-golay-demux",
                "--preflight-blob",
                str(db),
                "--prep-protocol-idx",
                "5",
            ]
        )
    assert ei.value.code == 2  # argparse required-arg exit code
    assert captured["requests"] == []
