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

import json

import duckdb
import httpx
import pytest
from qiita_common import feature_table as ft
from qiita_common.api_paths import URL_EXPORTED_IDENTIFIER, URL_REFERENCE_GENOME_MAP

from qiita_control_plane.cli.user import feature_table as ftc
from qiita_control_plane.miint import connect_with_miint

_ENTRIES = [
    # G400's two contigs: the per-(feature, genome) fan-out the roll-up key must keep.
    # `source` / `source_id` ride the response and are deliberately not staged — what a
    # row is NAMED comes from the exported-feature mint, not from this map.
    {"feature_idx": 10, "genome_idx": 100, "source": "refseq", "source_id": "GCF_100"},
    {"feature_idx": 40, "genome_idx": 400, "source": "refseq", "source_id": "GCF_400"},
    {"feature_idx": 41, "genome_idx": 400, "source": "refseq", "source_id": "GCF_400"},
]
# The exported-feature mint's answer for the two genomes above. The accession wins in
# both cases here; `accession`/`accession_published` ride the response so a caller can
# tell a `QF<n>` fallback from an entity that never had an accession.
_FEATURES = [
    {
        "genome_idx": 100,
        "export_feature_id": "GCF_100",
        "accession": "GCF_100",
        "accession_published": True,
    },
    {
        "genome_idx": 400,
        "export_feature_id": "GCF_400",
        "accession": "GCF_400",
        "accession_published": True,
    },
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


def _staged(entries=None, identifiers=None, features=None):
    """Stage all three responses into a miint connection and describe what landed."""
    conn = connect_with_miint()
    ftc._stage_genome_map(conn, _ENTRIES if entries is None else entries)
    ftc._stage_exported_identifiers(conn, _IDENTIFIERS if identifiers is None else identifiers)
    ftc._stage_exported_features(conn, _FEATURES if features is None else features)
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


def test_the_roll_up_key_keeps_the_fan_out_the_label_never_had():
    """Two relations with deliberately different cardinalities, now from two different
    responses: the roll-up needs a row per contig, and the mint answers once per
    genome so the label has nothing to collapse."""
    with _staged() as conn:
        pairs = conn.execute(f"SELECT count(*) FROM {ft.MAP_TABLE}").fetchone()[0]
        labels = conn.execute(f"SELECT count(*) FROM {ft.GENOME_LABEL_TABLE}").fetchone()[0]
    assert pairs == 3
    assert labels == 2


def test_an_empty_genome_map_still_stages_typed_relations():
    """A 16S reference has no genome-bearing features, so an empty map is a legitimate
    200. Inferring the arrow types from the rows would give NULL-typed columns here and
    fail the first join with a type error instead of yielding an empty table."""
    with _staged(entries=[], features=[]) as conn:
        assert _described(conn, ft.MAP_TABLE) == {"contig_id": "BIGINT", "genome_id": "BIGINT"}
        assert _described(conn, ft.GENOME_LABEL_TABLE) == {
            "genome_idx": "BIGINT",
            "feature_id": "VARCHAR",
        }
        assert conn.execute(f"SELECT count(*) FROM {ft.GENOME_LABEL_TABLE}").fetchone()[0] == 0


def test_the_staged_sources_do_not_outlive_the_staging():
    """The registered arrow tables are released once the CREATEs have copied them — a
    250 000-pair map is worth not holding twice on a laptop — so their names are gone
    afterwards and only the contract's relations remain."""
    with _staged() as conn:
        for source in (
            ftc._GENOME_MAP_SOURCE,
            ftc._MINT_SOURCE,
            ftc._EXPORTED_FEATURE_SOURCE,
        ):
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
        rows = conn.execute(f"SELECT * FROM {ft.LABELLED_RELATION} ORDER BY 1").fetchall()

    assert clearance.rows == 2
    assert rows == [("QM1", "GCF_100", 1.0), ("QM2", "GCF_400", 2.0)]


# ---------------------------------------------------------------------------
# The bundle
# ---------------------------------------------------------------------------


def _labelled(conn, rows=(("QM1", "GCF_100", 1.0), ("QM2", "GCF_400", 2.0))) -> None:
    """Stage a relabelled table, as the relabel would leave it."""
    values = ", ".join("(?::VARCHAR, ?::VARCHAR, ?::DOUBLE)" for _ in rows)
    conn.execute(
        f"CREATE TABLE {ft.LABELLED_RELATION} AS SELECT * FROM (VALUES {values}) "
        f"AS v({', '.join(ft.LABELLED_COLUMNS)})",
        [x for r in rows for x in r],
    )


@pytest.mark.parametrize("fmt", ["parquet", "biom"])
def test_the_bundle_is_the_table_AND_the_identifier_map(fmt, tmp_path):
    """One table, in the format asked for, at exactly the name the caller gave — plus
    the map, because the table alone cannot be joined back to the caller's own records
    (`export_id` is the only sample name it carries). The two formats hold the same
    numbers, so a run writes one of them, never both.
    """
    table = tmp_path / f"gut-study-ogu.{fmt}"
    with connect_with_miint() as conn:
        _labelled(conn)
        written = ftc._write_bundle(
            conn, table_path=table, fmt=fmt, identifiers=_IDENTIFIERS, clearances={}
        )

    assert [p.name for p in written] == [
        f"gut-study-ogu.{fmt}",
        "gut-study-ogu.exported-identifier.json",
    ]
    assert all(p.exists() for p in written)
    assert not list(tmp_path.glob("*.partial"))


def test_two_runs_can_share_a_directory(tmp_path):
    """What deriving the map's name from the table's buys: the pooled and per-sample
    builds of one cohort live side by side instead of colliding on a fixed name."""
    with connect_with_miint() as conn:
        _labelled(conn)
        first = ftc._write_bundle(
            conn,
            table_path=tmp_path / "pooled.parquet",
            fmt="parquet",
            identifiers=_IDENTIFIERS,
            clearances={},
        )
        second = ftc._write_bundle(
            conn,
            table_path=tmp_path / "per-sample.parquet",
            fmt="parquet",
            identifiers=_IDENTIFIERS,
            clearances={},
        )

    assert sorted(p.name for p in first + second) == [
        "per-sample.exported-identifier.json",
        "per-sample.parquet",
        "pooled.exported-identifier.json",
        "pooled.parquet",
    ]


def test_a_name_that_contradicts_the_format_is_refused(tmp_path):
    """A `.biom` holding Parquet bytes lies about itself to every reader downstream, and
    the name is the caller's — so this is a refusal, not a silent rewrite of their
    extension. A name with no format extension at all is theirs to choose and is left
    alone.
    """
    with connect_with_miint() as conn:
        _labelled(conn)
        with pytest.raises(ValueError, match="named for biom"):
            ftc._write_bundle(
                conn,
                table_path=tmp_path / "t.biom",
                fmt="parquet",
                identifiers=_IDENTIFIERS,
                clearances={},
            )
        written = ftc._write_bundle(
            conn,
            table_path=tmp_path / "no-extension",
            fmt="parquet",
            identifiers=_IDENTIFIERS,
            clearances={},
        )
    assert written[0].name == "no-extension"
    assert written[1].name == "no-extension.exported-identifier.json"


def test_the_identifier_map_carries_the_join_key_and_says_not_to_ship_it(tmp_path):
    """The map is the one artifact holding `prep_sample_idx` beside `export_id`, which
    is exactly what makes it useful and what makes publishing it a leak. The warning
    rides the file, not just the terminal: a file outlives its scrollback.
    """
    with connect_with_miint() as conn:
        _labelled(conn)
        _, map_path = ftc._write_bundle(
            conn,
            table_path=tmp_path / "t.parquet",
            fmt="parquet",
            identifiers=_IDENTIFIERS,
            clearances={},
        )

    payload = json.loads(map_path.read_text())
    assert payload["identifiers"] == _IDENTIFIERS
    assert payload["count"] == len(_IDENTIFIERS)
    assert "prep_sample_idx" in payload["note"]
    assert payload["note"] == ftc.IDENTIFIER_MAP_NOTE


def test_the_written_table_carries_only_public_columns(tmp_path):
    """Read back rather than trusted: this is the file a user publishes."""
    with connect_with_miint() as conn:
        _labelled(conn)
        table_path, _ = ftc._write_bundle(
            conn,
            table_path=tmp_path / "t.parquet",
            fmt="parquet",
            identifiers=_IDENTIFIERS,
            clearances={},
        )
        described = conn.execute(f"DESCRIBE SELECT * FROM read_parquet('{table_path}')").fetchall()
        rows = conn.execute(f"SELECT * FROM read_parquet('{table_path}') ORDER BY 1").fetchall()

    assert [(r[0], r[1]) for r in described] == list(ft.LABELLED_SCHEMA.items())
    assert rows == [("QM1", "GCF_100", 1.0), ("QM2", "GCF_400", 2.0)]


def test_a_failure_partway_through_leaves_NEITHER_file(tmp_path):
    """All-or-nothing across the pair. The table is written first, so a map that fails
    to serialize must take the committed table back with it — half a bundle is a table
    nobody can join, published beside a missing map.
    """
    with connect_with_miint() as conn:
        _labelled(conn)
        with pytest.raises(TypeError):
            ftc._write_bundle(
                conn,
                table_path=tmp_path / "t.parquet",
                fmt="parquet",
                # A set is not JSON-serializable, so the map's write raises after the
                # table has already been committed.
                identifiers=[{"prep_sample_idx": {1, 2}, "export_id": "QM1"}],
                clearances={},
            )

    assert not list(tmp_path.iterdir())


def test_an_existing_artifact_is_refused_before_anything_is_written(tmp_path):
    """The BIOM writer refuses to overwrite and the Parquet COPY replaces silently, so
    without this check the same second run would fail loudly in one format and quietly
    destroy a published file in the other."""
    table = tmp_path / "t.parquet"
    table.write_text("earlier run")
    with connect_with_miint() as conn:
        _labelled(conn)
        with pytest.raises(FileExistsError, match="t.parquet"):
            ftc._write_bundle(
                conn, table_path=table, fmt="parquet", identifiers=_IDENTIFIERS, clearances={}
            )

    assert table.read_text() == "earlier run"
    assert not (tmp_path / "t.exported-identifier.json").exists()


def test_a_lone_survivor_is_reported_as_an_unfinished_run(tmp_path):
    """The one shape a kill between the two renames leaves, and on disk it is
    indistinguishable from a finished run — so the refusal says which it is rather than
    let a user delete the wrong thing."""
    (tmp_path / "t.exported-identifier.json").write_text("{}")
    with connect_with_miint() as conn:
        _labelled(conn)
        with pytest.raises(FileExistsError, match="did not finish"):
            ftc._write_bundle(
                conn,
                table_path=tmp_path / "t.parquet",
                fmt="parquet",
                identifiers=_IDENTIFIERS,
                clearances={},
            )


def test_a_stale_partial_does_not_block_a_retry(tmp_path):
    """A killed run can leave a `.partial` behind, and the BIOM writer refuses to
    overwrite ANY existing target — including that one. Clearing our own partials first
    is what keeps the error a user sees about their own files, not ours.
    """
    (tmp_path / "t.biom.partial").write_text("interrupted")
    with connect_with_miint() as conn:
        _labelled(conn)
        written = ftc._write_bundle(
            conn,
            table_path=tmp_path / "t.biom",
            fmt="biom",
            identifiers=_IDENTIFIERS,
            clearances={},
        )
        assert conn.execute(f"SELECT count(*) FROM read_biom('{written[0]}')").fetchone()[0] == 2

    assert not list(tmp_path.glob("*.partial"))


def test_a_missing_output_directory_is_named(tmp_path):
    """Rather than left to the writers: DuckDB's Parquet COPY and HDF5 report this
    differently, and one of them badly."""
    with connect_with_miint() as conn:
        _labelled(conn)
        with pytest.raises(FileNotFoundError, match="no such directory"):
            ftc._write_bundle(
                conn,
                table_path=tmp_path / "nope" / "t.parquet",
                fmt="parquet",
                identifiers=_IDENTIFIERS,
                clearances={},
            )


def test_an_unsupported_format_is_refused(tmp_path):
    with connect_with_miint() as conn:
        _labelled(conn)
        with pytest.raises(ValueError, match="tsv"):
            ftc._write_bundle(
                conn,
                table_path=tmp_path / "t.tsv",
                fmt="tsv",
                identifiers=_IDENTIFIERS,
                clearances={},
            )


def test_parquet_is_the_default_format():
    """Both formats carry the same numbers, so the default is about who reads the file
    next: Parquet is what the rest of this system and every dataframe tool read."""
    assert ftc.DEFAULT_TABLE_FORMAT == "parquet"
    assert ftc.DEFAULT_TABLE_FORMAT in ftc.TABLE_FORMATS


def test_only_the_companions_asked_for_are_part_of_the_bundle(tmp_path):
    """The flags decide the bundle's shape before anything is computed, which is what lets
    the handler refuse an occupied name without streaming a reference first."""
    table = tmp_path / "t.parquet"
    assert [p.name for p in ftc._bundle_targets(table, "parquet")] == [
        "t.parquet",
        "t.exported-identifier.json",
    ]
    both = ftc._requested_companions(taxonomy=True, tree=True)
    assert [p.name for p in ftc._bundle_targets(table, "parquet", companions=both)] == [
        "t.parquet",
        "t.exported-identifier.json",
        "t.taxonomy.parquet",
        "t.tree.parquet",
    ]
    tree_only = ftc._requested_companions(taxonomy=False, tree=True)
    assert [c.name for c in tree_only] == ["tree"]


def test_an_occupied_companion_path_is_refused_like_any_other_bundle_member(tmp_path):
    """A companion is not a nice-to-have that can be skipped: the bundle is all of its
    files, so an existing sidecar refuses the run rather than being quietly replaced."""
    (tmp_path / "t.tree.parquet").write_text("an earlier run")
    with pytest.raises(FileExistsError, match="t.tree.parquet"):
        ftc._bundle_targets(
            tmp_path / "t.parquet",
            "parquet",
            companions=ftc._requested_companions(taxonomy=False, tree=True),
        )
