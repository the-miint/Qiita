"""Tests for the server-side run-preflight reader (`qiita_control_plane.preflight`).

The stored blob is the single source of truth for a sample's intake intent, and
BOTH the ingest CLI (`cli/user/pacbio.py`, at submit) and the pool-roster route (at
read-back) parse it. They call the same kl-run-preflight accessor
(`get_pacbio_sample_info`), and `test_cli_and_route_readers_agree_on_case5` is what
PINS them to each other: it fails if the two ever disagree on a protocol fact, so
the roster cannot start reporting a twist/syndna value ingest never validated.

These are also the pin on run_preflight's own schema — a dependency bump that
renames `run_pacbio_sample` / `project.human_filtering`, or reshapes
`PacbioSampleRow`, fails HERE, loudly, rather than silently nulling every PacBio
sample's protocol facts at runtime.

Built from run_preflight's own case-5 fixture (via the shared `build_case5_preflight`
fixture, the same builder the CLI and roster tests use), so the assertions are about
real sheet semantics: `pacbio_absquant` + a filled `twist_adaptor_id` +
`syndna_is_twisted == False` is the case-5 signature (syndna and lima both on).
"""

from __future__ import annotations

import argparse
import sqlite3

import pytest

from qiita_control_plane.preflight import (
    SHEET_TYPE_PACBIO_ABSQUANT,
    is_pacbio_sheet_type,
    pacbio_human_filtering_from_blob,
    pacbio_protocol_from_blob,
)


def test_pacbio_protocol_keys_on_pacbio_sample_idx(build_case5_preflight):
    """`str(pacbio_sample_idx)` IS the sequenced_pool_item_id the PacBio composer
    assigns, so this map joins the pool roster directly. NOT the barcode — that only
    locates the BAM on disk and is not unique across PacBio protocols."""
    facts = pacbio_protocol_from_blob(build_case5_preflight().read_bytes())
    assert facts, "case-5 fixture produced no PacBio rows"
    # Every key is the string form of an integer sample idx, not a `bc####` barcode.
    assert all(k.isdigit() for k in facts), sorted(facts)
    assert sorted(facts) == ["1", "2", "3"]


def test_pacbio_protocol_carries_the_case5_signature(build_case5_preflight):
    """sheet_type pacbio_absquant + twist filled + syndna_is_twisted False.
    The submit reads exactly these three to derive syndna_enabled / lima_enabled."""
    facts = pacbio_protocol_from_blob(build_case5_preflight().read_bytes())
    for idx, p in facts.items():
        assert p.sheet_type == SHEET_TYPE_PACBIO_ABSQUANT, idx
        assert p.twist_adaptor_id, f"{idx} has no twist_adaptor_id"
        assert p.syndna_is_twisted is False, idx
        # The gates the read-mask submit derives from these facts.
        assert (p.sheet_type == SHEET_TYPE_PACBIO_ABSQUANT) is True  # syndna_enabled
        assert (bool(p.twist_adaptor_id) and p.syndna_is_twisted is False) is True  # lima_enabled


def test_human_filtering_is_a_separate_reader(build_case5_preflight):
    """`human_filtering` is host-filtering POLICY, not a prep fact, and its source is
    moving to sample metadata — so it is read on its own and is NOT a field of
    `PacbioProtocol`. This test guards that separation: fusing it back into the
    protocol type is what would make that migration surgery instead of an excision.

    Both readers must still cover the same sample set, keyed identically, or the
    roster would attach one sample's filtering intent to another's protocol facts."""
    db = build_case5_preflight()
    conn = sqlite3.connect(db)
    # Flip the project flag (fixture default False) so this proves the reader reads
    # the PROJECT flag — including the control (sample.3), which inherits it via the
    # view's plate-primary resolution — rather than returning a constant.
    conn.execute("UPDATE project SET human_filtering = 1")
    conn.commit()
    conn.close()

    filtering = pacbio_human_filtering_from_blob(db.read_bytes())
    protocol = pacbio_protocol_from_blob(db.read_bytes())

    assert set(filtering) == set(protocol), "the two readers must cover the same samples"
    assert filtering == {"1": True, "2": True, "3": True}
    assert not hasattr(next(iter(protocol.values())), "human_filtering")


def test_cli_and_route_readers_agree_on_case5(build_case5_preflight):
    """PARITY PIN. The ingest CLI validates the protocol facts at submit; the roster
    route reports them at read-back. They must be the same values, or the roster
    would gate a read-mask on facts ingest never saw. Both go through
    `get_pacbio_sample_info`; this asserts they agree field-for-field on the case-5
    fixture — including the KEY, which the rekey turned from the barcode into
    pacbio_sample_idx."""
    from qiita_control_plane.cli.user.pacbio import _read_pacbio_preflight_rows

    db = build_case5_preflight()
    rows = _read_pacbio_preflight_rows(db, argparse.ArgumentParser())
    protocol = pacbio_protocol_from_blob(db.read_bytes())
    filtering = pacbio_human_filtering_from_blob(db.read_bytes())

    assert rows, "case-5 fixture produced no CLI rows"
    # The CLI's pool_item_id (str(pacbio_sample_idx)) is exactly the roster map's key.
    assert {str(r.pacbio_sample_idx) for r in rows} == set(protocol)

    for row in rows:
        key = str(row.pacbio_sample_idx)
        p = protocol[key]
        assert p.sheet_type == row.sheet_type, key
        assert p.twist_adaptor_id == row.twist_adaptor_id, key
        assert p.syndna_is_twisted == row.syndna_is_twisted, key
        assert filtering[key] == row.human_filtering, key


def test_pacbio_readers_decline_a_non_pacbio_blob(build_case5_preflight):
    """A well-formed pre-flight whose sheet_type is NOT PacBio yields {} rather than
    raising, so the roster route can probe both platforms without branching on
    exception type — and, critically, an Illumina pool never pays for a PacBio read.

    Uses a real pre-flight with its sheet_type swapped (rather than an empty SQLite):
    `open_db_file` applies run_preflight's schema patches on open, so a schema-less
    database is not a "non-PacBio blob" — it is an unopenable one."""
    db = build_case5_preflight(sheet_type="bclconvert")
    assert pacbio_protocol_from_blob(db.read_bytes()) == {}
    assert pacbio_human_filtering_from_blob(db.read_bytes()) == {}


def test_pacbio_protocol_raises_on_an_unreadable_blob():
    """An unreadable blob PROPAGATES: the roster route degrades it to "unknown" and
    warns; the CLI fails fast. Neither is served by swallowing it here."""
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


def test_an_unreadable_blob_raises_rather_than_looking_non_pacbio(build_case5_preflight):
    """REGRESSION. `run_sheet_type` must not degrade an unreadable blob to None.

    If it did, `pacbio_protocol_by_sample_idx` would return {} — indistinguishable from
    "this blob is not PacBio" — and the consequences run all the way to the compute:
    the roster reports sheet_type null, `submit-host-filter-pool` takes the Illumina
    branch, and every ticket is written `lima_enabled: false, syndna_enabled: false`.
    A case-5 pool would be masked with no lima and no syndna, and its spike-in count
    would be structurally zero — the exact failure the chain's step order prevents,
    reintroduced through the error path.

    So: a CORRUPT blob raises, while a well-formed NON-PacBio blob still returns {}.
    Those two must never collapse into the same answer."""
    corrupt = build_case5_preflight().read_bytes()[:512] + b"\x00" * 64
    with pytest.raises((sqlite3.DatabaseError, ValueError)):
        pacbio_protocol_from_blob(corrupt)

    # ...and the benign case is unchanged: a real, readable, non-PacBio sheet is {}.
    assert (
        pacbio_protocol_from_blob(build_case5_preflight(sheet_type="bclconvert").read_bytes()) == {}
    )
