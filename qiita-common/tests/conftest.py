"""Shared fixtures for the qiita-common tests."""

from __future__ import annotations

import re
from collections.abc import Callable
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def lake_table_ddl() -> Callable[[str], list[tuple[str, str]]]:
    """Read one lake table's columns out of the data plane's Rust DDL, as
    `(name, type)` in DDL order.

    The two components cannot import each other, so a column contract shared
    between a Python writer and a lake table is a constant on the Python side plus
    this parse of `ducklake.rs`. Constraints after the type (`NOT NULL`) are not
    part of the contract the writer controls, so they are dropped.
    """

    def _parse(table: str) -> list[tuple[str, str]]:
        src = (REPO_ROOT / "qiita-data-plane" / "src" / "ducklake.rs").read_text()
        marker = f"CREATE TABLE IF NOT EXISTS qiita_lake.{table} ("
        start = src.find(marker)
        assert start != -1, f"the {table} DDL moved; this test reads it out of ducklake.rs"
        body = src[start + len(marker) :]
        body = body[: body.index(")")]

        columns = []
        for line in body.splitlines():
            line = line.strip()
            if not line or line.startswith("--"):
                continue
            parts = re.split(r"\s+", line.rstrip(","))
            columns.append((parts[0], parts[1]))
        return columns

    return _parse
