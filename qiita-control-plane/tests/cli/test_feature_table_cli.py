"""Unit tests for the client-side feature-table recipe's maps and relabel
(`qiita_control_plane.cli.user.feature_table`) — no DB, no server, no data plane.

Covers the two route helpers (patching `httpx.request`, the entry point
`_common.call` delegates to), what the JSON responses become once staged into
DuckDB, and the relabel driver. The analytic and every refusal the relabel makes
are pinned against real miint in `tests/test_feature_table_analytic.py`; what is
specific here is the boundary between a REST response and a DuckDB relation, where
a wrong column type or a lost row would surface as a confusing bind error two steps
later.
"""

import duckdb
import httpx
import pytest
from qiita_common import feature_table as ft
from qiita_common.api_paths import URL_EXPORTED_IDENTIFIER, URL_REFERENCE_GENOME_MAP

from qiita_control_plane.cli.user import feature_table as ftc
from qiita_control_plane.miint import connect_with_miint_staged

_ENTRIES = [
    # G400's two contigs: the per-(feature, genome) fan-out the label relation
    # collapses and the roll-up key must keep.
    {"feature_idx": 10, "genome_idx": 100, "source": "refseq", "source_id": "GCF_100"},
    {"feature_idx": 40, "genome_idx": 400, "source": "refseq", "source_id": "GCF_400"},
    {"feature_idx": 41, "genome_idx": 400, "source": "refseq", "source_id": "GCF_400"},
]
_IDENTIFIERS = [
    {"prep_sample_idx": 1, "export_id": "QM1", "biosample_accession": "SAMN1"},
    {"prep_sample_idx": 2, "export_id": "QM2", "biosample_accession": None},
]


def _fake_request(captured, *, status=200, json_body=None, text=""):
    def fake_request(method, url, headers=None, json=None, params=None, timeout=None):
        captured["method"] = method
        captured["url"] = url
        captured["json"] = json
        request = httpx.Request(method, url)
        if json_body is not None:
            return httpx.Response(status, json=json_body, request=request)
        return httpx.Response(status, text=text, request=request)

    return fake_request


def test_fetch_genome_map_gets_the_route_and_returns_its_entries(monkeypatch):
    captured: dict = {}
    body = {"reference_idx": 7, "entries": _ENTRIES, "count": len(_ENTRIES)}
    monkeypatch.setattr(ftc._common.httpx, "request", _fake_request(captured, json_body=body))

    entries = ftc._fetch_genome_map("http://cp", "qk_tok", reference_idx=7)
    assert captured["method"] == "GET"
    assert captured["url"] == f"http://cp{URL_REFERENCE_GENOME_MAP.format(reference_idx=7)}"
    assert entries == _ENTRIES


def test_a_genome_map_over_the_route_cap_reaches_the_caller_with_the_real_size(monkeypatch):
    """The route refuses over its cap rather than truncating, and this helper must not
    soften that: a lookup table silently missing rows yields a WRONG feature table, not
    a short one, and there is no paged form to fall back to. The 413 and the size it
    names have to arrive intact at whoever can tell the user.
    """
    detail = "Genome map for reference 7 has 400000 entries, over the 250000 maximum"
    monkeypatch.setattr(ftc._common.httpx, "request", _fake_request({}, status=413, text=detail))

    with pytest.raises(httpx.HTTPStatusError) as exc:
        ftc._fetch_genome_map("http://cp", "qk_tok", reference_idx=7)
    assert exc.value.response.status_code == 413
    assert detail in exc.value.response.text


def test_mint_exported_identifiers_posts_the_alignment_and_the_cohort(monkeypatch):
    captured: dict = {}
    body = {"alignment_idx": 3, "identifiers": _IDENTIFIERS, "count": 2}
    monkeypatch.setattr(ftc._common.httpx, "request", _fake_request(captured, json_body=body))

    identifiers = ftc._mint_exported_identifiers(
        "http://cp", "qk_tok", alignment_idx=3, prep_sample_idx=[2, 1]
    )
    assert captured["method"] == "POST"
    assert captured["url"] == f"http://cp{URL_EXPORTED_IDENTIFIER}"
    assert captured["json"] == {"alignment_idx": 3, "prep_sample_idx": [2, 1]}
    assert identifiers == _IDENTIFIERS


def test_an_invalid_cohort_is_refused_before_the_round_trip(monkeypatch):
    """The cohort's shape and cap are the request model's, shared with the alignment
    mint. Validating the body through it costs nothing and turns a 422 from the far end
    into a local error."""

    def explode(*args, **kwargs):
        raise AssertionError("no request should be issued for an invalid cohort")

    monkeypatch.setattr(ftc._common.httpx, "request", explode)
    with pytest.raises(ValueError):
        ftc._mint_exported_identifiers("http://cp", "qk_tok", alignment_idx=3, prep_sample_idx=[])


def _staged(entries=None, identifiers=None):
    """Stage both responses into a miint connection and describe what landed."""
    conn = connect_with_miint_staged()
    ftc._stage_genome_map(conn, _ENTRIES if entries is None else entries)
    ftc._stage_exported_identifiers(conn, _IDENTIFIERS if identifiers is None else identifiers)
    return conn


def _described(conn, relation) -> dict[str, str]:
    return {r[0]: r[1] for r in conn.execute(f"DESCRIBE {relation}").fetchall()}


def test_the_staged_maps_carry_native_integer_keys_and_varchar_handles():
    """miint's functions take NATIVE-INTEGER id columns, and BIOM takes VARCHAR ids.
    The arrow schema is written out rather than inferred so neither depends on what
    the JSON happened to contain."""
    with _staged() as conn:
        assert _described(conn, ft.MAP_TABLE) == {"contig_id": "BIGINT", "genome_id": "BIGINT"}
        assert _described(conn, ft.GENOME_LABEL_TABLE) == {
            "genome_idx": "BIGINT",
            "feature_id": "VARCHAR",
        }
        assert _described(conn, ft.SAMPLE_LABEL_TABLE) == {
            "prep_sample_idx": "BIGINT",
            "sample_id": "VARCHAR",
        }


def test_the_roll_up_key_keeps_the_fan_out_and_the_label_collapses_it():
    """One response, two relations with deliberately different cardinalities: the
    roll-up needs a row per contig, the label exactly one per genome."""
    with _staged() as conn:
        pairs = conn.execute(f"SELECT count(*) FROM {ft.MAP_TABLE}").fetchone()[0]
        labels = conn.execute(f"SELECT count(*) FROM {ft.GENOME_LABEL_TABLE}").fetchone()[0]
    assert pairs == 3
    assert labels == 2


def test_an_empty_genome_map_still_stages_typed_relations():
    """A 16S reference has no genome-bearing features, so an empty map is a legitimate
    200. Inferring the arrow types from the rows would give NULL-typed columns here and
    fail the first join with a type error instead of yielding an empty table."""
    with _staged(entries=[]) as conn:
        assert _described(conn, ft.MAP_TABLE) == {"contig_id": "BIGINT", "genome_id": "BIGINT"}
        assert conn.execute(f"SELECT count(*) FROM {ft.GENOME_LABEL_TABLE}").fetchone()[0] == 0


def test_the_staged_sources_do_not_outlive_the_staging():
    """The registered arrow tables are released once the CREATEs have copied them — a
    250 000-pair map is worth not holding twice on a laptop — so their names are gone
    afterwards and only the contract's relations remain."""
    with _staged() as conn:
        for source in (ftc._GENOME_MAP_SOURCE, ftc._MINT_SOURCE):
            with pytest.raises(duckdb.CatalogException):
                conn.execute(f"SELECT count(*) FROM {source}")


def test_a_failed_staging_releases_its_source_too():
    """The release is in a `finally`, so a CREATE that raises does not leave the arrow
    table registered. Provoked by staging twice on one connection: the second attempt
    fails on the relation that already exists, part-way through the loop."""
    with _staged() as conn:
        with pytest.raises(duckdb.CatalogException, match="already exists"):
            ftc._stage_genome_map(conn, _ENTRIES)
        with pytest.raises(duckdb.CatalogException):
            conn.execute(f"SELECT count(*) FROM {ftc._GENOME_MAP_SOURCE}")


def test_the_relabel_driver_writes_the_public_table_and_reports_its_size():
    """The driver's own job: pass the diagnostics row to the check BY NAME, then write
    the cleared relabel. What it refuses is pinned against the analytic; what matters
    here is that a clean run produces the public table and the count a caller reports.
    """
    with _staged() as conn:
        conn.execute(
            f"CREATE TABLE {ft.OGU_OUTPUT_TABLE} AS SELECT * FROM (VALUES "
            f"(1::BIGINT, 100::BIGINT, 1.0::DOUBLE), (2::BIGINT, 400::BIGINT, 2.0::DOUBLE)) "
            f"AS t({', '.join(ft.OUTPUT_COLUMNS)})"
        )
        clearance = ftc._relabel(conn)
        rows = conn.execute(f"SELECT * FROM {ft.LABELLED_TABLE} ORDER BY 1").fetchall()

    assert clearance.rows == 2
    assert rows == [("QM1", "GCF_100", 1.0), ("QM2", "GCF_400", 2.0)]
