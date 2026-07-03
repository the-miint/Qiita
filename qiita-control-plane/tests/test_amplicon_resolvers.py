"""Unit tests for the amplicon / golay-demux runner resolvers.

Pure-function coverage (no DB / no orchestrator) matching test_read_ingest_resolvers:
the data-plane calls are monkeypatched and the reference-status pool is a stub, so
these exercise the runner-side glue (roster materialization, GG2 export + validation,
pool-masked-read export) without infrastructure.
"""

from __future__ import annotations

import asyncio

import duckdb
import pytest
from qiita_common.backend_failure import BackendFailure, FailureKind

from qiita_control_plane.runner import (
    BARCODE_MAP_BINDING,
    GG2_FEATURES_BINDING,
    POOL_READS_BINDING,
    _resolve_barcode_map,
    _resolve_gg2_features,
    _resolve_pool_masked_reads,
    _write_small_parquet,
)


class _FakePool:
    """asyncpg-pool stand-in: fetchrow returns the preset row (or None)."""

    def __init__(self, row):
        self._row = row

    async def fetchrow(self, *_a, **_k):
        return self._row


def _dp_kwargs(tmp_path):
    return {
        "data_plane_url": "grpc://unused",
        "hmac_secret": b"x" * 16,
        "workspace": tmp_path / "ws",
    }


# --- _write_small_parquet -----------------------------------------------------


def test_write_small_parquet_typed_columns(tmp_path):
    out = tmp_path / "t.parquet"
    _write_small_parquet(
        {
            "prep_sample_idx": ("int64", [1, 2]),
            "barcode": ("string", ["AC", "GT"]),
            "barcodes_are_rc": ("bool", [True, False]),
        },
        out,
    )
    with duckdb.connect(":memory:") as conn:
        rows = conn.execute(
            f"SELECT prep_sample_idx, barcode, barcodes_are_rc FROM read_parquet('{out}') "
            "ORDER BY prep_sample_idx"
        ).fetchall()
    assert rows == [(1, "AC", True), (2, "GT", False)]


# --- _resolve_barcode_map -----------------------------------------------------


def test_resolve_barcode_map_writes_parquet(tmp_path):
    """the roster becomes barcode_map.parquet with the per-barcode barcodes_are_rc flag."""
    ctx = {
        BARCODE_MAP_BINDING: [
            {"prep_sample_idx": 11, "barcode": "GATC", "barcodes_are_rc": True},
            {"prep_sample_idx": 12, "barcode": "TTAG", "barcodes_are_rc": False},
        ]
    }
    bound = asyncio.run(_resolve_barcode_map(ctx, tmp_path / "ws"))
    out = bound[BARCODE_MAP_BINDING]
    with duckdb.connect(":memory:") as conn:
        rows = conn.execute(
            f"SELECT prep_sample_idx, barcode, barcodes_are_rc FROM read_parquet('{out}') "
            "ORDER BY prep_sample_idx"
        ).fetchall()
    assert rows == [(11, "GATC", True), (12, "TTAG", False)]


def test_resolve_barcode_map_rejects_empty_roster(tmp_path):
    with pytest.raises(BackendFailure) as exc:
        asyncio.run(_resolve_barcode_map({BARCODE_MAP_BINDING: []}, tmp_path / "ws"))
    assert exc.value.kind == FailureKind.BAD_INPUT


def test_resolve_barcode_map_requires_barcodes_are_rc(tmp_path):
    """an entry missing the RC provenance flag is rejected at submission."""
    ctx = {BARCODE_MAP_BINDING: [{"prep_sample_idx": 11, "barcode": "GATC"}]}
    with pytest.raises(BackendFailure) as exc:
        asyncio.run(_resolve_barcode_map(ctx, tmp_path / "ws"))
    assert exc.value.kind == FailureKind.BAD_INPUT
    assert "barcodes_are_rc" in exc.value.reason


# --- _resolve_pool_masked_reads -----------------------------------------------

_EXPORT_POOL_MASKED = "qiita_control_plane.runner._do_action_export_pool_masked"


def test_resolve_pool_masked_reads_binds_dest(tmp_path, monkeypatch):
    """a non-zero export count binds pool_reads to the per-ticket Parquet."""
    monkeypatch.setattr(_EXPORT_POOL_MASKED, lambda _u, _t: {"count": 100, "dest": "ignored"})
    bound = asyncio.run(
        _resolve_pool_masked_reads(
            sequenced_pool_idx=25, prep_sample_idxs=[1, 2], mask_idx=100, **_dp_kwargs(tmp_path)
        )
    )
    assert bound[POOL_READS_BINDING] == tmp_path / "ws" / "pool_reads.parquet"


def test_resolve_pool_masked_reads_empty_is_bad_input(tmp_path, monkeypatch):
    """no 'pass' reads for the mask → BAD_INPUT (pool must be ingested + masked)."""
    monkeypatch.setattr(_EXPORT_POOL_MASKED, lambda _u, _t: {"count": 0, "dest": ""})
    with pytest.raises(BackendFailure) as exc:
        asyncio.run(
            _resolve_pool_masked_reads(
                sequenced_pool_idx=25, prep_sample_idxs=[1], mask_idx=100, **_dp_kwargs(tmp_path)
            )
        )
    assert exc.value.kind == FailureKind.BAD_INPUT
    assert "no 'pass' reads" in exc.value.reason


def test_resolve_pool_masked_reads_export_failure_is_bad_input(tmp_path, monkeypatch):
    def _boom(_u, _t):
        raise RuntimeError("Flight: connection refused")

    monkeypatch.setattr(_EXPORT_POOL_MASKED, _boom)
    with pytest.raises(BackendFailure) as exc:
        asyncio.run(
            _resolve_pool_masked_reads(
                sequenced_pool_idx=25, prep_sample_idxs=[1], mask_idx=100, **_dp_kwargs(tmp_path)
            )
        )
    assert exc.value.kind == FailureKind.BAD_INPUT
    assert "could not materialize" in exc.value.reason


# --- _resolve_gg2_features ----------------------------------------------------

_DO_GET_REF = "qiita_control_plane.runner._do_get_reference_sequences"


def test_resolve_gg2_features_writes_parquet(tmp_path, monkeypatch):
    """an ACTIVE reference's (feature_idx, sequence_hash) rows are written, sorted."""
    monkeypatch.setattr(_DO_GET_REF, lambda _u, _t: [(102, "hashB"), (101, "hashA")])
    bound = asyncio.run(
        _resolve_gg2_features(
            _FakePool({"status": "active"}), gg2_reference_idx=2, **_dp_kwargs(tmp_path)
        )
    )
    out = bound[GG2_FEATURES_BINDING]
    with duckdb.connect(":memory:") as conn:
        rows = conn.execute(
            f"SELECT feature_idx, sequence_hash FROM read_parquet('{out}') ORDER BY feature_idx"
        ).fetchall()
    assert rows == [(101, "hashA"), (102, "hashB")]


def test_resolve_gg2_features_missing_reference_is_bad_input(tmp_path):
    with pytest.raises(BackendFailure) as exc:
        asyncio.run(
            _resolve_gg2_features(_FakePool(None), gg2_reference_idx=99, **_dp_kwargs(tmp_path))
        )
    assert exc.value.kind == FailureKind.BAD_INPUT
    assert "does not exist" in exc.value.reason


def test_resolve_gg2_features_inactive_is_bad_input(tmp_path):
    with pytest.raises(BackendFailure) as exc:
        asyncio.run(
            _resolve_gg2_features(
                _FakePool({"status": "draft"}), gg2_reference_idx=2, **_dp_kwargs(tmp_path)
            )
        )
    assert exc.value.kind == FailureKind.BAD_INPUT
    assert "status" in exc.value.reason


def test_resolve_gg2_features_empty_set_is_bad_input(tmp_path, monkeypatch):
    """a reference that returns no features is a misconfiguration, not a valid target."""
    monkeypatch.setattr(_DO_GET_REF, lambda _u, _t: [])
    with pytest.raises(BackendFailure) as exc:
        asyncio.run(
            _resolve_gg2_features(
                _FakePool({"status": "active"}), gg2_reference_idx=2, **_dp_kwargs(tmp_path)
            )
        )
    assert exc.value.kind == FailureKind.BAD_INPUT
    assert "no features" in exc.value.reason
