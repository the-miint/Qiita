"""Tests for the shared run-preflight reader (`qiita_control_plane.preflight`).

The stored blob is the single source of truth for a sample's intake intent, and
BOTH the submit CLI and the pool-roster route parse it. The join lives in one
module so the two cannot drift; these tests are what pin the coupling to
run_preflight's `pacbio_sample` / `prepped_sample` / `input_sample` / `project`
schema — a pin bump that renames those fails HERE, loudly, rather than silently
nulling every PacBio sample's protocol facts at runtime.

Built from run_preflight's own case-5 fixture, so the assertions are about real
sheet semantics: `pacbio_absquant` + a filled `twist_adaptor_id` +
`syndna_is_twisted == False` is the case-5 signature (syndna and lima both on).
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from qiita_control_plane.preflight import (
    SHEET_TYPE_PACBIO_ABSQUANT,
    is_pacbio_sheet_type,
    pacbio_protocol_from_blob,
)

_CASE5_CSV = Path(__file__).parent / "cli" / "data" / "good_pacbio_absquantv11.csv"


def _case5_blob(tmp_path: Path) -> bytes:
    from run_preflight.legacy.api import migrate_legacy_csv_to_db_file

    db = tmp_path / "case5.db"
    migrate_legacy_csv_to_db_file(str(_CASE5_CSV), str(db))
    return db.read_bytes()


def test_pacbio_protocol_from_blob_keys_on_barcode(tmp_path):
    """The barcode IS the sequenced_pool_item_id the PacBio composer assigns, so
    this map keys the pool roster directly."""
    facts = pacbio_protocol_from_blob(_case5_blob(tmp_path))
    assert facts, "case-5 fixture produced no PacBio rows"
    assert all(isinstance(k, str) and k for k in facts)


def test_pacbio_protocol_carries_the_case5_signature(tmp_path):
    """sheet_type pacbio_absquant + twist filled + syndna_is_twisted False.
    The submit reads exactly these three to derive syndna_enabled / lima_enabled."""
    facts = pacbio_protocol_from_blob(_case5_blob(tmp_path))
    for barcode, p in facts.items():
        assert p.sheet_type == SHEET_TYPE_PACBIO_ABSQUANT, barcode
        assert p.twist_adaptor_id, f"{barcode} has no twist_adaptor_id"
        assert p.syndna_is_twisted is False, barcode
        # The gates the read-mask submit derives from these facts.
        assert (p.sheet_type == SHEET_TYPE_PACBIO_ABSQUANT) is True  # syndna_enabled
        assert (bool(p.twist_adaptor_id) and not p.syndna_is_twisted) is True  # lima_enabled


def test_pacbio_protocol_carries_human_filtering(tmp_path):
    """PacBio human_filtering comes from the SAME join as the protocol fields. The
    Illumina reader walks run_illumina_sample, which PacBio has no analogue for, so
    it would return {} and leave every PacBio sample's intent null — aborting
    submit-host-filter-pool before it starts."""
    facts = pacbio_protocol_from_blob(_case5_blob(tmp_path))
    # The control blank inherits its plate's primary project, so every row resolves.
    assert all(p.human_filtering is not None for p in facts.values())
    assert all(isinstance(p.human_filtering, bool) for p in facts.values())


def test_pacbio_protocol_returns_empty_for_a_non_pacbio_blob(tmp_path):
    """A blob the PacBio reader doesn't own yields {} rather than raising, so the
    roster route can try both readers without branching on exception type."""
    db = tmp_path / "empty.db"
    sqlite3.connect(db).close()
    assert pacbio_protocol_from_blob(db.read_bytes()) == {}


def test_pacbio_protocol_raises_on_an_unreadable_blob():
    with pytest.raises(sqlite3.DatabaseError):
        pacbio_protocol_from_blob(b"this is not a sqlite file")


@pytest.mark.parametrize(
    "sheet_type,expected",
    [
        ("pacbio_absquant", True),
        ("pacbio_metag", True),
        ("bclconvert", False),
        ("", False),
        (None, False),
    ],
)
def test_is_pacbio_sheet_type(sheet_type, expected):
    assert is_pacbio_sheet_type(sheet_type) is expected
