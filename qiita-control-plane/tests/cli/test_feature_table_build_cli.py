"""The `qiita feature-table build` handler, driven end to end with the two REST maps
and both Flight streams faked — real miint, real analytic, real writers.

The pieces this wires are pinned elsewhere: the analytic against real miint in
`tests/test_feature_table_analytic.py`, the maps / relabel / bundle in
`tests/cli/test_feature_table_cli.py`, the BIOM writer's own behaviours in
`tests/test_biom_writer_contract.py`. **What is only testable here is the wiring**:
which columns ride the ticket, which scope reaches the survivor set, whether the
lengths stream is opened at all, and that every refusal the recipe can make comes
out as a message and a non-zero exit rather than a traceback or a half-written file.

The fakes stop exactly at the two boundaries a client cannot reach in a unit test —
HTTP and Flight. Everything past them is the real thing, so the table asserted at
the bottom is the file a user would publish.
"""

from __future__ import annotations

import argparse
import json

import httpx
import pyarrow as pa
import pytest
from qiita_common import feature_table as ft
from qiita_common.hashing import canonical_params_hash

from qiita_control_plane.cli.user import feature_table as ftc
from qiita_control_plane.miint import connect_with_miint

# One 1000 bp genome fully covered in sample 1, and one 10 000 bp genome each sample
# covers 0.6% of — extending halves, so 1.2% pooled. At a 1% threshold that genome
# survives pooled and fails per-sample, which is the only asymmetry the two scopes
# have (pooling unions intervals, so pooled breadth is never the smaller one).
_MAP_ENTRIES = [
    {"feature_idx": 10, "genome_idx": 100, "source": "refseq", "source_id": "GCF_100"},
    {"feature_idx": 20, "genome_idx": 200, "source": "refseq", "source_id": "GCF_200"},
]
_LENGTHS = [(10, 1000), (20, 10000)]
# (prep_sample_idx, sequence_idx, feature_idx, flags, position, stop_position, cigar)
_ALIGNMENT = [
    (1, 1, 10, 0, 0, 500, "500="),
    (1, 2, 20, 0, 0, 60, "30=30X"),  # identity 0.5 — the row a gate at 0.9 drops
    (2, 3, 20, 0, 60, 120, "60="),
]
_IDENTIFIERS = [
    {"prep_sample_idx": 1, "export_id": "QM1", "biosample_accession": "SAMN1"},
    {"prep_sample_idx": 2, "export_id": "QM2", "biosample_accession": None},
]
_PARAMS = {"reference_idx": 9, "aligner": "minimap2", "mask_idx": 2, "shard_ids": [0]}
_ALIGNMENTS_BODY = {
    "alignments": [
        {
            "alignment_idx": 3,
            "params": _PARAMS,
            # The real digest: the build verifies it before reading anything off
            # `params`, so a placeholder here would refuse every test in this module.
            "params_hash": canonical_params_hash(_PARAMS).hex(),
            "samples_completed": 2,
            "samples_total": 2,
        }
    ]
}

_ALIGNMENT_TICKET = b"alignment-ticket"
_LENGTHS_TICKET = b"lengths-ticket"
_TAXONOMY_TICKET = b"taxonomy-ticket"
_PHYLOGENY_TICKET = b"phylogeny-ticket"

# `((c10:0.2,c30:0.5)inner:0.1,c20:0.3);` in the lake's shape, node indices and all, as
# `read_newick` assigns them at ingest. Written out rather than parsed so the fixture does
# not depend on that indexing — and shaped so shearing it does real work: feature 30 has no
# genome in the map, so its tip is unpublished and drops, leaving `inner` with one child.
# The collapse must then hand c10 a branch of 0.2 + 0.1.
#
# (node_index, name, branch_length, parent_index, is_tip, feature_idx)
_PHYLOGENY = [
    (0, "c10", 0.2, 2, True, 10),
    (1, "c30", 0.5, 2, True, 30),
    (2, "inner", 0.1, 4, False, None),
    (3, "c20", 0.3, 4, True, 20),
    (4, "", None, None, False, None),
]

# The reference's per-feature taxonomy as the data plane streams it. Feature 10 (G100)
# is classified to genus; feature 20 (G200) has a row with every rank NULL, which is how
# ingest records an unclassified feature — a MISSING row is what an exclusion produces.
_TAXONOMY = [
    (
        10,
        "Bacteria",
        "Bacillota",
        "Bacilli",
        "Lactobacillales",
        "Listeriaceae",
        "Listeria",
        "",
        None,
    ),
    (20, None, None, None, None, None, None, None, None),
]

_ALIGNMENT_TYPES = {
    "prep_sample_idx": pa.int64(),
    "sequence_idx": pa.int64(),
    "feature_idx": pa.int64(),
    "flags": pa.uint16(),
    "position": pa.int64(),
    "stop_position": pa.int64(),
    "cigar": pa.string(),
    "mate_position": pa.int64(),
}


def _alignment_reader(columns: list[str], rows: list[tuple]) -> pa.RecordBatchReader:
    """The alignment slice as the data plane would stream it: **exactly the projected
    columns**, in the ticket's order. A wider stream would hide the projection drift
    the signed column list exists to prevent, so the fake honours it."""
    padded = [tuple(r) + (None,) * (8 - len(r)) for r in rows]
    named = dict(zip(_ALIGNMENT_TYPES, zip(*padded, strict=True), strict=True))
    table = pa.table(
        {name: pa.array(list(named[name]), _ALIGNMENT_TYPES[name]) for name in columns}
    )
    return table.to_reader()


def _lengths_reader() -> pa.RecordBatchReader:
    """`reference_sequences` as the whole-reference ticket streams it — `sequence_hash`
    rides along unused, which is what the analytic's projection has to tolerate."""
    return pa.table(
        {
            "feature_idx": pa.array([f for f, _ in _LENGTHS], pa.int64()),
            "sequence_hash": pa.array([None] * len(_LENGTHS), pa.string()),
            "sequence_length_bp": pa.array([n for _, n in _LENGTHS], pa.int64()),
        }
    ).to_reader()


def _taxonomy_reader(rows) -> pa.RecordBatchReader:
    """`reference_taxonomy_visible` as the whole-reference ticket streams it —
    `reference_idx` and `ncbi_taxon_id` ride along and the analytic's projection drops
    them, which is what makes the sidecar's column list ours rather than the lake's."""
    ranks = ("domain", "phylum", "class", "order", "family", "genus", "species", "strain")
    columns = {
        "reference_idx": pa.array([9] * len(rows), pa.int64()),
        "feature_idx": pa.array([r[0] for r in rows], pa.int64()),
    }
    for i, rank in enumerate(ranks, start=1):
        columns[rank] = pa.array([r[i] for r in rows], pa.string())
    columns["ncbi_taxon_id"] = pa.array([None] * len(rows), pa.int64())
    return pa.table(columns).to_reader()


def _phylogeny_reader(rows) -> pa.RecordBatchReader:
    """`reference_phylogeny` as the whole-reference ticket streams it. `reference_idx` and
    `edge_id` ride along — the first is dropped by our projection, the second is carried
    into the published tree because it is the handle back to the reference's placements."""
    return pa.table(
        {
            "reference_idx": pa.array([9] * len(rows), pa.int64()),
            "node_index": pa.array([r[0] for r in rows], pa.int64()),
            "name": pa.array([r[1] for r in rows], pa.string()),
            "branch_length": pa.array([r[2] for r in rows], pa.float64()),
            "edge_id": pa.array([None] * len(rows), pa.int64()),
            "parent_index": pa.array([r[3] for r in rows], pa.int64()),
            "is_tip": pa.array([r[4] for r in rows], pa.bool_()),
            "feature_idx": pa.array([r[5] for r in rows], pa.int64()),
        }
    ).to_reader()


class _FakeStream:
    def __init__(self, reader):
        self._reader = reader

    def to_reader(self):
        return self._reader


class _FakeFlightClient:
    """Routes each DoGet by the ticket bytes it was signed for, so a handler that
    streamed the lengths ticket where the alignment belongs fails here rather than
    producing a confusing bind error."""

    instances: list[_FakeFlightClient] = []

    def __init__(self, url, readers):
        self.url = url
        self._readers = readers
        self.tickets: list[bytes] = []
        self.closed = False
        _FakeFlightClient.instances.append(self)

    def do_get(self, ticket, *options):
        self.tickets.append(ticket.ticket)
        return _FakeStream(self._readers[ticket.ticket]())

    def close(self):
        self.closed = True

    # `flight.FlightClient` is itself a context manager and the handler uses it as one,
    # so the fake has to be too — otherwise the test would pass against a handler that
    # leaked the real client.
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False


def _namespace(tmp_path, **overrides) -> argparse.Namespace:
    fields = {
        "base_url": "http://cp",
        "data_plane_url": "grpc://dp:50051",
        "sequencing_run_idx": 4,
        "sequenced_pool_idx": 5,
        "alignment_idx": 3,
        "prep_sample_idx": None,
        "coverage_scope": ft.CoverageScope.POOLED.value,
        "coverage_threshold": 0.01,
        "min_identity": None,
        "min_query_coverage": None,
        "unpaired_gate": False,
        "format": ftc.DEFAULT_TABLE_FORMAT,
        "taxonomy": False,
        "tree": False,
        "output": tmp_path / "table.parquet",
    }
    return argparse.Namespace(**(fields | overrides))


def _patched(
    monkeypatch,
    *,
    alignment=None,
    map_entries=None,
    cohort=None,
    feature_handles=None,
    taxonomy_rows=None,
    phylogeny_rows=None,
):
    """Patch the five REST seams, both ticket mints, and the Flight client; return the
    recorder every assertion below reads. A test that wants a seam to RAISE re-patches
    it itself afterwards, rather than this growing a parameter per failure mode."""
    rec: dict = {"lengths_minted": 0, "taxonomy_minted": 0, "phylogeny_minted": 0}
    _FakeFlightClient.instances = []

    monkeypatch.setattr(ftc._common, "read_token", lambda *a, **k: "qk_tok")

    def _alignments(*args, **kwargs):
        rec["alignments_fetched"] = True
        return _ALIGNMENTS_BODY

    monkeypatch.setattr(ftc, "_fetch_pool_alignments", _alignments)
    monkeypatch.setattr(
        ftc,
        "_fetch_alignment_cohort",
        lambda *a, **k: {"prep_sample_idx": [1, 2] if cohort is None else cohort},
    )
    monkeypatch.setattr(
        ftc,
        "_fetch_genome_map",
        lambda *a, **k: _MAP_ENTRIES if map_entries is None else map_entries,
    )

    def _mint_ids(base_url, token, *, alignment_idx, prep_sample_idx):
        rec["minted_cohort"] = prep_sample_idx
        return [i for i in _IDENTIFIERS if i["prep_sample_idx"] in prep_sample_idx]

    monkeypatch.setattr(ftc, "_mint_exported_identifiers", _mint_ids)

    def _mint_features(base_url, token, *, genome_idx):
        """Stands in for the exported-feature mint. Answers with each genome's own
        accession, which is what the real mint does whenever that accession is unique
        in the published namespace — `feature_handles` overrides it for the tests that
        need a handle the accession would not produce."""
        rec["minted_genomes"] = list(genome_idx)
        resolved = (
            {e["genome_idx"]: e["source_id"] for e in (map_entries or _MAP_ENTRIES)}
            if feature_handles is None
            else feature_handles
        )
        return [
            {
                "genome_idx": g,
                "export_feature_id": resolved[g],
                "accession": resolved[g],
                "accession_published": True,
            }
            for g in genome_idx
        ]

    monkeypatch.setattr(ftc, "_mint_exported_features", _mint_features)

    def _mint_processing(base_url, token, *, alignment_idx, prep_sample_idx):
        """The processing mint. `prep_sample_idx` authorizes and is not part of the
        handle, so the fake records it to prove the cohort is what gets sent."""
        rec["processing_cohort"] = list(prep_sample_idx)
        return "QP7"

    monkeypatch.setattr(ftc, "_mint_exported_processing", _mint_processing)
    monkeypatch.setattr(
        ftc,
        "_fetch_reference",
        lambda *a, **k: {
            "reference_idx": 9,
            "name": "gg2",
            "version": "2022.10",
            "kind": "sequence_reference",
            "status": "active",
            "is_host": False,
            "created_by_idx": 1,
            "created_at": "2026-01-01T00:00:00Z",
        },
    )

    def _alignment_ticket(base_url, token, *, alignment_idx, prep_sample_idx, columns):
        rec["columns"] = list(columns)
        rec["ticket_cohort"] = list(prep_sample_idx)
        return _ALIGNMENT_TICKET

    def _reference_ticket(base_url, token, *, reference_idx, table):
        """One helper signs every reference table now, so the fake dispatches on the
        table name the same way the route does."""
        rec["reference_idx"] = reference_idx
        if table == ftc._TAXONOMY_TABLE:
            rec["taxonomy_minted"] += 1
            return _TAXONOMY_TICKET
        if table == ftc._PHYLOGENY_TABLE:
            rec["phylogeny_minted"] += 1
            return _PHYLOGENY_TICKET
        rec["lengths_minted"] += 1
        return _LENGTHS_TICKET

    monkeypatch.setattr(ftc, "_create_alignment_doget_ticket", _alignment_ticket)
    monkeypatch.setattr(ftc, "_create_reference_doget_ticket", _reference_ticket)

    rows = _ALIGNMENT if alignment is None else alignment
    readers = {
        # The signed cohort IS the scope on the real data plane — it serves exactly the
        # prep_samples the ticket carries — so the fake filters by it too. Streaming
        # rows outside the cohort would otherwise look like a passing build here and a
        # relabel refusal in production.
        _ALIGNMENT_TICKET: lambda: _alignment_reader(
            rec["columns"], [r for r in rows if r[0] in rec["ticket_cohort"]]
        ),
        _LENGTHS_TICKET: _lengths_reader,
        _TAXONOMY_TICKET: lambda: _taxonomy_reader(
            _TAXONOMY if taxonomy_rows is None else taxonomy_rows
        ),
        _PHYLOGENY_TICKET: lambda: _phylogeny_reader(
            _PHYLOGENY if phylogeny_rows is None else phylogeny_rows
        ),
    }
    monkeypatch.setattr("pyarrow.flight.FlightClient", lambda url: _FakeFlightClient(url, readers))
    return rec


def _table_rows(path, *, fmt="parquet") -> list[tuple]:
    reader = "read_parquet" if fmt == "parquet" else "read_biom"
    with connect_with_miint() as conn:
        return sorted(conn.execute(f"SELECT * FROM {reader}('{path}')").fetchall())


def test_a_pooled_build_writes_the_public_table_and_its_map(monkeypatch, tmp_path, capsys):
    """The whole recipe, from an alignment_idx to a file a user publishes: public
    handles on both axes, and the identifier map beside it with the warning that it is
    the one artifact not to ship."""
    rec = _patched(monkeypatch)
    args = _namespace(tmp_path)

    assert ftc._handle_feature_table_build(args, parser=None) == 0

    assert _table_rows(args.output) == [
        ("QM1", "GCF_100", 1.0),
        ("QM1", "GCF_200", 1.0),
        ("QM2", "GCF_200", 1.0),
    ]
    assert (tmp_path / "table.exported-identifier.json").is_file()
    out = capsys.readouterr().out
    assert ftc.IDENTIFIER_MAP_NOTE in out
    assert str(args.output) in out
    # The reference came from the alignment's own params, never from a flag.
    assert rec["reference_idx"] == 9
    assert all(client.closed for client in _FakeFlightClient.instances)


def test_the_per_sample_scope_reaches_the_survivor_set(monkeypatch, tmp_path):
    """The one thing a wrong wiring here would hide: `--coverage-scope` silently
    ignored still produces a plausible table, just the pooled one. G200 clears 1%
    only when the two samples' intervals are unioned, so its absence is the proof the
    flag arrived."""
    _patched(monkeypatch)
    args = _namespace(tmp_path, coverage_scope=ft.CoverageScope.PER_SAMPLE.value)

    assert ftc._handle_feature_table_build(args, parser=None) == 0
    assert _table_rows(args.output) == [("QM1", "GCF_100", 1.0)]


def test_a_zero_threshold_never_opens_the_lengths_stream(monkeypatch, tmp_path):
    """At 0 every genome with any alignment qualifies, so there is no coverage
    calculation — and the lengths stream is its only consumer. Observed at the
    boundary rather than in the output, because a wasted whole-reference stream
    produces exactly the same table."""
    rec = _patched(monkeypatch)
    args = _namespace(tmp_path, coverage_threshold=0.0)

    assert ftc._handle_feature_table_build(args, parser=None) == 0
    assert rec["lengths_minted"] == 0
    assert _FakeFlightClient.instances[0].tickets == [_ALIGNMENT_TICKET]
    # Every genome survives, including the one a 1% threshold drops per-sample.
    assert len(_table_rows(args.output)) == 3


def test_an_ungated_build_leaves_cigar_off_the_wire(monkeypatch, tmp_path):
    """`cigar` is ~96% of an alignment row and the analytic never reads it, so the
    default projection must not carry it — that is what the signed column list is
    for."""
    rec = _patched(monkeypatch)
    assert ftc._handle_feature_table_build(_namespace(tmp_path), parser=None) == 0
    assert rec["columns"] == list(ft.ALIGNMENT_COLUMNS)
    assert "cigar" not in rec["columns"]


def test_a_gate_requests_its_columns_and_filters_the_table(monkeypatch, tmp_path):
    """A gate pays for `cigar` and `mate_position` on the wire and must be seen to
    change the result: dropping sample 1's 60 bp of G200 takes that genome under 1%
    pooled, so the gate reaches the coverage filter and not just the counts."""
    rec = _patched(monkeypatch)
    args = _namespace(tmp_path, min_identity=0.9)

    assert ftc._handle_feature_table_build(args, parser=None) == 0
    assert rec["columns"] == [*ft.ALIGNMENT_COLUMNS, "cigar", "mate_position"]
    assert _table_rows(args.output) == [("QM1", "GCF_100", 1.0)]


def test_the_gate_pools_mates_by_default(monkeypatch, tmp_path):
    """`paired=True` is correct for single-end rows too (their partition is one row),
    and getting it wrong in the cheap direction is a silent correctness loss — so the
    default is the safe one, and `mate_position` riding the projection above is what
    that costs."""
    rec = _patched(monkeypatch)
    ftc._handle_feature_table_build(_namespace(tmp_path, min_query_coverage=0.5), parser=None)
    assert "mate_position" in rec["columns"]

    rec = _patched(monkeypatch)
    unpaired = _namespace(
        tmp_path, min_query_coverage=0.5, unpaired_gate=True, output=tmp_path / "b.parquet"
    )
    ftc._handle_feature_table_build(unpaired, parser=None)
    assert "mate_position" not in rec["columns"]


def test_an_unpaired_gate_over_paired_reads_is_refused(monkeypatch, tmp_path, capsys):
    """The escape hatch exists (a slice whose mates did not both map cannot be pooled),
    so it can also be reached by mistake — and judging a placement's mates
    independently orphans one of them without any error. The refusal has to arrive as
    a message and a non-zero exit."""
    paired = [(1, 1, 10, 1, 0, 500, "500="), (1, 1, 10, 1, 500, 1000, "500=", 0)]
    _patched(monkeypatch, alignment=paired)
    args = _namespace(tmp_path, min_identity=0.9, unpaired_gate=True)

    assert ftc._handle_feature_table_build(args, parser=None) == 1
    assert "paired" in capsys.readouterr().err
    assert not args.output.exists()


def test_unpaired_gate_alone_is_refused_before_any_round_trip(monkeypatch, tmp_path, capsys):
    """`--unpaired-gate` only says how to judge a gate, so on its own it silently does
    nothing — the same shape as a gate with no threshold, which the shared module refuses
    for the same reason. It needs nothing but the flags, so it is refused before the
    first REST call rather than after two."""
    rec = _patched(monkeypatch)
    args = _namespace(tmp_path, unpaired_gate=True)

    assert ftc._handle_feature_table_build(args, parser=None) == 1
    assert "--unpaired-gate" in capsys.readouterr().err
    assert "alignments_fetched" not in rec
    assert not _FakeFlightClient.instances


def test_an_unreachable_control_plane_is_named_not_traced(monkeypatch, tmp_path, capsys):
    """No HTTP response at all — the control plane is down, or --base-url is wrong. A
    transport traceback tells a user nothing they can act on."""

    def _refused(*args, **kwargs):
        raise httpx.ConnectError("connection refused")

    _patched(monkeypatch)
    monkeypatch.setattr(ftc, "_fetch_pool_alignments", _refused)

    assert ftc._handle_feature_table_build(_namespace(tmp_path), parser=None) == 1
    assert "--base-url" in capsys.readouterr().err


def test_a_flight_failure_is_named_not_traced(monkeypatch, tmp_path, capsys):
    """The data plane's half of the recipe fails on its own terms — an expired ticket, a
    dead instance — and must arrive as a message, not a gRPC traceback."""
    import pyarrow.flight as flight

    rec = _patched(monkeypatch)

    def _dead_stream(self, ticket, *options):
        rec["reached_flight"] = True
        raise flight.FlightUnavailableError("data plane unavailable")

    monkeypatch.setattr(_FakeFlightClient, "do_get", _dead_stream)

    assert ftc._handle_feature_table_build(_namespace(tmp_path), parser=None) == 1
    assert rec["reached_flight"]
    assert "flight error" in capsys.readouterr().err
    assert all(client.closed for client in _FakeFlightClient.instances)


def test_a_cohort_named_on_the_command_line_is_what_gets_signed(monkeypatch, tmp_path):
    """The explicit list has to reach BOTH the ticket and the mint, not just one of
    them. And narrowing it is not a filter on the same answer: dropping sample 2 takes
    G200's pooled breadth from 1.2% to 0.6%, so the genome leaves the table — which is
    why the cohort is part of the scientific question rather than a convenience.
    """
    rec = _patched(monkeypatch)
    args = _namespace(tmp_path, prep_sample_idx=[1])

    assert ftc._handle_feature_table_build(args, parser=None) == 0
    assert rec["ticket_cohort"] == [1]
    assert rec["minted_cohort"] == [1]
    assert _table_rows(args.output) == [("QM1", "GCF_100", 1.0)]


def test_an_omitted_cohort_resolves_to_the_pools_mintable_set(monkeypatch, tmp_path):
    """The cohort route returns what is both readable by the caller and completed for
    this alignment — a valid mint body by construction — so the common case needs no
    sample list at all."""
    rec = _patched(monkeypatch)
    assert ftc._handle_feature_table_build(_namespace(tmp_path), parser=None) == 0
    assert rec["ticket_cohort"] == [1, 2]


def test_an_empty_resolved_cohort_says_so_before_minting_anything(monkeypatch, tmp_path, capsys):
    """Empty is a legitimate answer from the cohort route ('nothing here you may
    mint'), but it is not a table: both mints reject an empty cohort, and a 422 from
    the far end would name a body the user never wrote."""
    _patched(monkeypatch, cohort=[])
    assert ftc._handle_feature_table_build(_namespace(tmp_path), parser=None) == 1
    err = capsys.readouterr().err
    assert "no prep_sample" in err
    assert not _FakeFlightClient.instances


def test_a_genome_map_over_the_cap_stops_the_build_with_the_size(monkeypatch, tmp_path, capsys):
    """The route refuses over its cap rather than truncating, because a lookup table
    silently missing rows yields a WRONG table. The 413 and the real size it names
    have to reach the user."""
    detail = "Genome map for reference 9 has 400000 entries, over the 250000 maximum"

    def _too_big(*args, **kwargs):
        request = httpx.Request("GET", "http://cp/genome-map")
        raise httpx.HTTPStatusError(
            detail, request=request, response=httpx.Response(413, text=detail, request=request)
        )

    _patched(monkeypatch)
    monkeypatch.setattr(ftc, "_fetch_genome_map", _too_big)
    assert ftc._handle_feature_table_build(_namespace(tmp_path), parser=None) == 1
    err = capsys.readouterr().err
    assert "413" in err
    assert "400000" in err


def test_an_existing_output_is_refused_before_any_streaming(monkeypatch, tmp_path, capsys):
    """The recipe streams a cohort's alignment and a whole reference before it has
    anything to write, so discovering the collision at the end would waste all of it —
    and the file it would collide with may be one somebody published."""
    _patched(monkeypatch)
    args = _namespace(tmp_path)
    args.output.write_text("an earlier run")

    assert ftc._handle_feature_table_build(args, parser=None) == 1
    assert "refusing to overwrite" in capsys.readouterr().err
    assert args.output.read_text() == "an earlier run"
    assert not _FakeFlightClient.instances


def test_a_mint_answering_one_handle_for_two_genomes_stops_the_build(monkeypatch, tmp_path, capsys):
    """A BIOM write SUMS duplicate (feature_id, sample_id) pairs, so merging two
    organisms under one handle would never surface in the file.

    The published namespace is UNIQUE across the mint's live rows, so the server cannot
    answer this way — a genome whose accession is taken gets a `QF<n>`. What this pins
    is the client-side backstop: it exits non-zero with the message and leaves nothing
    behind.

    A genome with no label at all is not reachable either: the mint is asked for the
    roll-up's OWN output, so the label set covers the counts by construction.
    """
    _patched(monkeypatch, feature_handles={100: "GCF_1", 200: "GCF_1"})
    args = _namespace(tmp_path)

    assert ftc._handle_feature_table_build(args, parser=None) == 1
    assert "merges genomes" in capsys.readouterr().err
    assert not list(tmp_path.iterdir())


def test_the_row_labels_are_minted_only_for_the_genomes_actually_published(monkeypatch, tmp_path):
    """A public handle is a durable act, so the mint is asked for the genomes the
    roll-up EMITTED — not for every genome in the reference. Minting the reference's
    whole set because a table mentioned it would leave permanent public identifiers for
    rows nobody published; for GG2's backbone that is a six-figure number of them.

    At this threshold G200 (0.012 breadth) fails and G100 (0.5) survives, so the map
    covers two genomes and exactly one is minted for.
    """
    rec = _patched(monkeypatch)
    args = _namespace(tmp_path, coverage_threshold=0.02)

    assert ftc._handle_feature_table_build(args, parser=None) == 0
    assert rec["minted_genomes"] == [100]


def test_biom_is_written_when_asked_for(monkeypatch, tmp_path):
    """The other format, read back through miint's own reader: the same numbers, so
    the choice is only about who opens the file next."""
    _patched(monkeypatch)
    args = _namespace(tmp_path, format="biom", output=tmp_path / "table.biom")

    assert ftc._handle_feature_table_build(args, parser=None) == 0
    assert _table_rows(args.output, fmt="biom") == [
        ("QM1", "GCF_100", 1.0),
        ("QM1", "GCF_200", 1.0),
        ("QM2", "GCF_200", 1.0),
    ]


def test_an_empty_cohort_result_still_writes_a_readable_table(monkeypatch, tmp_path):
    """Every genome dropped by the threshold is a legitimate answer, and it must be a
    real file rather than an error or an absent one — the path a caller exercises least
    and would notice last."""
    _patched(monkeypatch)
    args = _namespace(tmp_path, coverage_threshold=1.0)

    assert ftc._handle_feature_table_build(args, parser=None) == 0
    assert _table_rows(args.output) == []


def test_the_data_plane_url_reaches_the_flight_client(monkeypatch, tmp_path):
    _patched(monkeypatch)
    args = _namespace(tmp_path, data_plane_url="grpc+tls://qiita.example.com:443")
    assert ftc._handle_feature_table_build(args, parser=None) == 0
    assert _FakeFlightClient.instances[0].url == "grpc+tls://qiita.example.com:443"


# The flags a valid `build` needs, so each parser test states only its own delta
# instead of re-spelling thirteen argv elements. A `None` value drops the flag.
_BUILD_FLAGS = {
    "--sequencing-run-idx": "4",
    "--sequenced-pool-idx": "5",
    "--alignment-idx": "3",
    "--coverage-threshold": "0.01",
    "--output": "/tmp/ft/gut.parquet",
    "--data-plane-url": "grpc://dp:50051",
}


def _build_argv(**overrides) -> list[str]:
    """`feature-table build` argv from `_BUILD_FLAGS`, with `--flag-name` written as
    `flag_name=`. A value of `None` omits the flag; a list repeats it."""
    flags = _BUILD_FLAGS | {f"--{name.replace('_', '-')}": v for name, v in overrides.items()}
    argv = ["feature-table", "build"]
    for flag, value in flags.items():
        if value is None:
            continue
        for one in value if isinstance(value, list) else [value]:
            argv += [flag, one]
    return argv


def test_parser_wires_the_build_with_parquet_and_pooled_as_defaults():
    from pathlib import Path

    from qiita_control_plane.cli.user._parser import _build_parser

    args = _build_parser().parse_args(_build_argv())
    assert args.handler is ftc._handle_feature_table_build
    assert args.output == Path("/tmp/ft/gut.parquet")
    assert args.format == ftc.DEFAULT_TABLE_FORMAT == "parquet"
    assert args.coverage_scope == ft.CoverageScope.POOLED.value
    assert args.coverage_threshold == 0.01
    assert args.prep_sample_idx is None  # omitted -> resolved from the pool
    assert args.min_identity is None and args.min_query_coverage is None
    assert args.unpaired_gate is False


def test_parser_takes_a_repeated_cohort_and_the_per_sample_scope():
    from qiita_control_plane.cli.user._parser import _build_parser

    args = _build_parser().parse_args(
        _build_argv(
            prep_sample_idx=["1", "2"],
            coverage_scope="per-sample",
            coverage_threshold="0",
            min_identity="0.95",
            format="biom",
            output="/tmp/ft/gut.biom",
        )
    )
    assert args.prep_sample_idx == [1, 2]
    assert args.coverage_scope == ft.CoverageScope.PER_SAMPLE.value
    assert args.min_identity == 0.95
    assert args.format == "biom"


@pytest.mark.parametrize(
    "missing", ["alignment_idx", "coverage_threshold", "output", "data_plane_url"]
)
def test_parser_rejects_a_build_missing_any_required_flag(missing):
    from qiita_control_plane.cli.user._parser import _build_parser

    with pytest.raises(SystemExit):
        _build_parser().parse_args(_build_argv(**{missing: None}))


@pytest.mark.parametrize("value", ["1.5", "-0.1", "not-a-number"])
def test_parser_rejects_a_threshold_that_is_not_a_proportion(value):
    """The thresholds are proportions in [0, 1] and a bad one is a usage error, caught
    at parse time rather than after a cohort's worth of streaming."""
    from qiita_control_plane.cli.user._parser import _build_parser

    with pytest.raises(SystemExit):
        _build_parser().parse_args(_build_argv(coverage_threshold=value))


# ---------------------------------------------------------------------------
# The taxonomy sidecar
# ---------------------------------------------------------------------------


def test_the_taxonomy_sidecar_names_exactly_the_tables_rows(monkeypatch, tmp_path, capsys):
    """The point of both files coming from one mint: they join on one column.

    G100 is classified to genus and reports a blank species; G200's taxonomy row is all
    NULL, which is how ingest records an unclassified feature — it appears with NULL
    ranks rather than being left out, because a row missing from the sidecar reads as
    though it were unclassified anyway and there would be no way to tell.
    """
    _patched(monkeypatch)
    args = _namespace(tmp_path, taxonomy=True)

    assert ftc._handle_feature_table_build(args, parser=None) == 0
    sidecar = tmp_path / "table.taxonomy.parquet"
    assert sidecar.exists()
    assert str(sidecar) in capsys.readouterr().out

    with connect_with_miint() as conn:
        rows = conn.execute(
            f"SELECT * FROM read_parquet('{sidecar}') ORDER BY feature_id"
        ).fetchall()
        columns = [
            r[0]
            for r in conn.execute(f"DESCRIBE SELECT * FROM read_parquet('{sidecar}')").fetchall()
        ]

    assert columns == list(ft.TAXONOMY_SIDECAR_COLUMNS)
    assert rows == [
        (
            "GCF_100",
            "d__Bacteria",
            "p__Bacillota",
            "c__Bacilli",
            "o__Lactobacillales",
            "f__Listeriaceae",
            "g__Listeria",
            "s__",
            None,
        ),
        ("GCF_200", None, None, None, None, None, None, None, None),
    ]
    # The join key is the table's own, not a second vocabulary.
    assert {r[0] for r in rows} == {r[1] for r in _table_rows(args.output)}


def test_no_sidecar_is_written_without_the_flag(monkeypatch, tmp_path):
    """And no taxonomy ticket is minted either — a whole reference's taxonomy is not
    something to stream for a caller who did not ask for it."""
    rec = _patched(monkeypatch)
    args = _namespace(tmp_path)

    assert ftc._handle_feature_table_build(args, parser=None) == 0
    assert rec["taxonomy_minted"] == 0
    assert sorted(p.name for p in tmp_path.iterdir()) == [
        "table.exported-identifier.json",
        "table.manifest.json",
        "table.parquet",
    ]


def test_a_blocked_lowest_contig_does_not_unclassify_its_genome(monkeypatch, tmp_path):
    """The exclusion case, end to end. G200's two contigs: the lower one is excluded so
    the stream carries no row for it, and the higher one is classified. The genome must
    publish the surviving member's lineage rather than tiling as unclassified.
    """
    _patched(
        monkeypatch,
        map_entries=[
            {"feature_idx": 10, "genome_idx": 100, "source": "refseq", "source_id": "GCF_100"},
            {"feature_idx": 20, "genome_idx": 200, "source": "refseq", "source_id": "GCF_200"},
            {"feature_idx": 21, "genome_idx": 200, "source": "refseq", "source_id": "GCF_200"},
        ],
        # No row for feature 20 at all — that is what an exclusion looks like here.
        taxonomy_rows=[
            (10, "Bacteria", None, None, None, None, None, None, None),
            (21, "Archaea", None, None, None, None, None, None, None),
        ],
    )
    args = _namespace(tmp_path, taxonomy=True)

    assert ftc._handle_feature_table_build(args, parser=None) == 0
    with connect_with_miint() as conn:
        rows = conn.execute(
            f"SELECT feature_id, domain FROM read_parquet("
            f"'{tmp_path / 'table.taxonomy.parquet'}') ORDER BY feature_id"
        ).fetchall()
    assert rows == [("GCF_100", "d__Bacteria"), ("GCF_200", "d__Archaea")]


def test_a_reference_whose_taxonomy_repeats_a_feature_is_refused(monkeypatch, tmp_path, capsys):
    """Two rows for one feature, which the reference's own ingest forbids — and here they
    disagree, one calling feature 10 Bacteria and the other Archaea.

    The reduction resolves a repeat to one row per genome, so nothing downstream can see
    it: the sidecar would be the right SHAPE and carry whichever lineage the scan reached.
    That is why the repeat is measured on the streamed taxonomy instead.
    """
    _patched(
        monkeypatch,
        taxonomy_rows=[
            (10, "Bacteria", None, None, None, None, None, None, None),
            (10, "Archaea", None, None, None, None, None, None, None),
            (20, None, None, None, None, None, None, None, None),
        ],
    )
    args = _namespace(tmp_path, taxonomy=True)

    assert ftc._handle_feature_table_build(args, parser=None) == 1
    assert "more than one row" in capsys.readouterr().err
    assert not list(tmp_path.iterdir()), "no half-bundle survives a refused sidecar"


def test_alignments_to_features_with_no_genome_are_reported(monkeypatch, tmp_path, capsys):
    """The roll-up's INNER JOIN to the map drops them silently, and for some references
    that is most of the data streamed. Nothing else in the recipe would ever mention it,
    so a table that is a fraction of the alignment says so.
    """
    _patched(
        monkeypatch,
        map_entries=[
            {"feature_idx": 10, "genome_idx": 100, "source": "refseq", "source_id": "GCF_100"},
        ],
    )
    args = _namespace(tmp_path)

    assert ftc._handle_feature_table_build(args, parser=None) == 0
    err = capsys.readouterr().err
    assert "no genome in this reference" in err
    assert "2 of 3 alignment rows" in err


# ---------------------------------------------------------------------------
# The sheared tree
# ---------------------------------------------------------------------------


def _tree_rows(path) -> list[tuple]:
    with connect_with_miint() as conn:
        return conn.execute(
            f"SELECT name, branch_length, is_tip FROM read_parquet('{path}') ORDER BY node_index"
        ).fetchall()


def test_the_tree_is_sheared_to_the_published_rows_and_keeps_their_distances(monkeypatch, tmp_path):
    """The whole point of shearing rather than filtering: c30 drops because feature 30 has
    no genome, `inner` is left with one child and collapses, and c10's branch becomes
    0.2 + 0.1 — the distance it had in the whole tree. Tips are named the way the table's
    rows are, so the two files join on one column."""
    _patched(monkeypatch)
    args = _namespace(tmp_path, tree=True)

    assert ftc._handle_feature_table_build(args, parser=None) == 0
    rows = _tree_rows(tmp_path / "table.tree.parquet")

    tips = {name: branch for name, branch, is_tip in rows if is_tip}
    assert tips == pytest.approx({"GCF_100": 0.3, "GCF_200": 0.3})
    # Two tips and the new root, which the shear leaves unnamed — `inner` is gone.
    assert [name for name, _, is_tip in rows if not is_tip] == [""]


def test_no_tree_is_written_or_streamed_without_the_flag(monkeypatch, tmp_path):
    """A whole reference's tree is the largest stream in this recipe, so the flag has to
    gate the ticket mint too, not just the file."""
    rec = _patched(monkeypatch)
    args = _namespace(tmp_path)

    assert ftc._handle_feature_table_build(args, parser=None) == 0
    assert rec["phylogeny_minted"] == 0
    assert not (tmp_path / "table.tree.parquet").exists()


def test_both_companions_land_beside_the_table_when_both_are_asked_for(monkeypatch, tmp_path):
    """Four files, and the same `feature_id` vocabulary across all three that carry one."""
    _patched(monkeypatch)
    args = _namespace(tmp_path, taxonomy=True, tree=True)

    assert ftc._handle_feature_table_build(args, parser=None) == 0
    assert sorted(p.name for p in tmp_path.iterdir()) == [
        "table.exported-identifier.json",
        "table.manifest.json",
        "table.parquet",
        "table.taxonomy.parquet",
        "table.tree.parquet",
    ]
    published = {feature_id for _, feature_id, _ in _table_rows(tmp_path / "table.parquet")}
    tips = {name for name, _, is_tip in _tree_rows(tmp_path / "table.tree.parquet") if is_tip}
    assert tips == published


def test_a_reference_with_no_tree_stops_the_build_rather_than_writing_a_bundle(
    monkeypatch, tmp_path, capsys
):
    """An absent phylogeny is an empty stream, not an error — the recipe has to notice."""
    _patched(monkeypatch, phylogeny_rows=[])
    args = _namespace(tmp_path, tree=True)

    assert ftc._handle_feature_table_build(args, parser=None) == 1
    assert "no phylogeny" in capsys.readouterr().err
    assert not list(tmp_path.iterdir())


def test_a_published_genome_with_two_tips_stops_the_build(monkeypatch, tmp_path, capsys):
    """A contig-level tree for a genome the table publishes as one row. The shear would
    accept it and emit both tips under the one handle."""
    both_contigs_of_g100 = [
        (0, "c10", 0.2, 2, True, 10),
        (1, "c11", 0.5, 2, True, 11),
        (2, "inner", 0.1, 4, False, None),
        (3, "c20", 0.3, 4, True, 20),
        (4, "", None, None, False, None),
    ]
    _patched(
        monkeypatch,
        map_entries=[
            *_MAP_ENTRIES,
            {"feature_idx": 11, "genome_idx": 100, "source": "refseq", "source_id": "GCF_100"},
        ],
        phylogeny_rows=both_contigs_of_g100,
    )
    args = _namespace(tmp_path, tree=True)

    assert ftc._handle_feature_table_build(args, parser=None) == 1
    assert "GCF_100" in capsys.readouterr().err
    assert not list(tmp_path.iterdir())


def test_a_build_without_shear_tree_stops_before_any_round_trip(monkeypatch, tmp_path, capsys):
    """The probe's placement is the point: `shear_tree` is absent from builds a user's
    extension cache may still hold, and discovering that after a whole reference has been
    streamed wastes the run."""
    rec = _patched(monkeypatch)

    def _absent(con, name, *, needed_for):
        raise RuntimeError(f"the miint build in use has no {name}(), which {needed_for} needs")

    monkeypatch.setattr(ftc, "require_miint_function", _absent)
    args = _namespace(tmp_path, tree=True)

    assert ftc._handle_feature_table_build(args, parser=None) == 1
    assert "shear_tree" in capsys.readouterr().err
    assert "alignments_fetched" not in rec
    assert not list(tmp_path.iterdir())


def test_an_empty_cohort_still_writes_a_readable_tree(monkeypatch, tmp_path):
    """Every genome dropped by the threshold is a legitimate answer, and the bundle stays
    whole: an empty table, an empty sidecar, an empty tree. `shear_tree` raises rather
    than shearing a tree to nothing, so the empty case cannot go through it — and reaching
    it left a raw `InvalidInputException` escaping as a traceback, past every `except` the
    handler has."""
    _patched(monkeypatch)
    args = _namespace(tmp_path, coverage_threshold=1.0, taxonomy=True, tree=True)

    assert ftc._handle_feature_table_build(args, parser=None) == 0
    assert _table_rows(args.output) == []
    assert _tree_rows(tmp_path / "table.tree.parquet") == []
    with connect_with_miint() as conn:
        described = conn.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{tmp_path / 'table.tree.parquet'}')"
        ).fetchall()
    assert [(r[0], r[1]) for r in described] == list(ft.TREE_SCHEMA.items())


# ---------------------------------------------------------------------------
# The bundle manifest
# ---------------------------------------------------------------------------


def _manifest(tmp_path) -> dict:
    return json.loads((tmp_path / "table.manifest.json").read_text())


def _walk(node, *, keys: set, strings: set) -> None:
    """Every key and every string value anywhere in the document."""
    if isinstance(node, dict):
        for key, value in node.items():
            keys.add(key)
            _walk(value, keys=keys, strings=strings)
    elif isinstance(node, list):
        for item in node:
            _walk(item, keys=keys, strings=strings)
    elif isinstance(node, str):
        strings.add(node)


def test_the_manifest_records_the_processing_by_its_public_handle(monkeypatch, tmp_path):
    """The whole reason the handle exists: a manifest has to say what produced the table,
    and `alignment_idx` is ours. The digest rides alongside because it identifies the
    config by content, which is what somebody reproducing the table actually needs."""
    rec = _patched(monkeypatch)
    args = _namespace(tmp_path)

    assert ftc._handle_feature_table_build(args, parser=None) == 0
    manifest = _manifest(tmp_path)

    assert manifest["processing"]["export_processing_id"] == "QP7"
    assert manifest["processing"]["params_hash"] == canonical_params_hash(_PARAMS).hex()
    assert manifest["processing"]["aligner"] == "minimap2"
    # The reference by name and version, resolved rather than carried as an idx.
    assert manifest["processing"]["reference"] == {
        "name": "gg2",
        "version": "2022.10",
        "kind": "sequence_reference",
    }
    # The cohort authorizes the mint and is not part of the handle.
    assert rec["processing_cohort"] == [1, 2]


def test_the_manifest_names_the_cohort_and_the_table_it_describes(monkeypatch, tmp_path):
    rec = _patched(monkeypatch)
    args = _namespace(tmp_path)

    assert ftc._handle_feature_table_build(args, parser=None) == 0
    table = _manifest(tmp_path)["table"]

    assert table["cohort"] == ["QM1", "QM2"]
    assert table["rows"] == len(_table_rows(args.output))
    assert table["format"] == "parquet"
    assert table["coverage_scope"] == ft.CoverageScope.POOLED.value
    assert table["coverage_threshold"] == 0.01
    assert table["gate"] is None
    assert rec["minted_cohort"] == [1, 2]


def test_the_manifest_records_the_gate_that_filtered_the_table(monkeypatch, tmp_path):
    """A gate changes which rows exist, so a record of the table that omitted it would
    describe a different table."""
    _patched(monkeypatch)
    args = _namespace(tmp_path, min_identity=0.9)

    assert ftc._handle_feature_table_build(args, parser=None) == 0
    assert _manifest(tmp_path)["table"]["gate"] == {
        "min_identity": 0.9,
        "min_query_coverage": None,
        "mates_pooled": True,
    }


def test_the_manifest_lists_every_file_in_the_bundle_including_itself(monkeypatch, tmp_path):
    """Basenames, so the record does not describe the machine that built it — and the
    list is derived from the same targets the writer uses, not a second guess at them."""
    _patched(monkeypatch)
    args = _namespace(tmp_path, taxonomy=True, tree=True)

    assert ftc._handle_feature_table_build(args, parser=None) == 0
    assert _manifest(tmp_path)["files"] == [
        "table.parquet",
        "table.exported-identifier.json",
        "table.manifest.json",
        "table.taxonomy.parquet",
        "table.tree.parquet",
    ]
    assert sorted(_manifest(tmp_path)["files"]) == sorted(p.name for p in tmp_path.iterdir())


def test_the_manifest_records_what_computed_the_table(monkeypatch, tmp_path):
    """The miint build especially: every bioinformatics primitive is compiled INTO the
    extension, so two clients on different builds can get different numbers from the same
    input. Read from the catalog, since the client's cache never refreshes itself."""
    _patched(monkeypatch)
    args = _namespace(tmp_path)

    assert ftc._handle_feature_table_build(args, parser=None) == 0
    tools = _manifest(tmp_path)["tools"]

    assert set(tools) == {"duckdb", "miint", "qiita_cli"}
    with connect_with_miint() as conn:
        expected = conn.execute(
            "SELECT extension_version FROM duckdb_extensions() WHERE extension_name = 'miint'"
        ).fetchone()[0]
    assert tools["miint"] == expected
    assert tools["duckdb"] and tools["qiita_cli"]
    # A path on this machine is not something a published file should carry.
    assert not [value for value in tools.values() if "/" in value]


def test_no_internal_identifier_appears_anywhere_in_the_manifest(monkeypatch, tmp_path):
    """The property the two mints exist to make possible, asserted over the whole
    document rather than field by field — a future field cannot quietly reintroduce one."""
    _patched(monkeypatch)
    args = _namespace(tmp_path, taxonomy=True, tree=True, min_identity=0.5)

    assert ftc._handle_feature_table_build(args, parser=None) == 0
    keys: set[str] = set()
    strings: set[str] = set()
    _walk(_manifest(tmp_path), keys=keys, strings=strings)

    assert not [key for key in keys if "idx" in key]
    assert not [value for value in strings if "idx" in value]
    # `shard_ids` and `mask_idx` ride the params blob, which is why the manifest lifts
    # `aligner` out of it rather than copying it whole.
    assert "shard_ids" not in keys
    assert "params" not in keys


def test_a_params_hash_mismatch_stops_the_build_before_anything_is_written(
    monkeypatch, tmp_path, capsys
):
    """The manifest cites the digest as its reproducibility key, so publishing one the
    client never verified would publish a claim it cannot support."""
    rec = _patched(monkeypatch)
    monkeypatch.setattr(
        ftc,
        "_fetch_pool_alignments",
        lambda *a, **k: {
            "alignments": [
                {"alignment_idx": 3, "params": _PARAMS, "params_hash": "00" * 32},
            ]
        },
    )
    args = _namespace(tmp_path)

    assert ftc._handle_feature_table_build(args, parser=None) == 1
    assert "params_hash" in capsys.readouterr().err
    assert not list(tmp_path.iterdir())
    # Both mints, named separately. The processing handle is the one this test's premise
    # is about, and asserting only the identifier mint would prove it just transitively —
    # via an ordering inside `_run_build` that nothing here checks.
    assert "processing_cohort" not in rec, "no public handle for a processing we cannot vouch for"
    assert "minted_cohort" not in rec, "nothing should be minted for a build that cannot run"


def test_a_version_the_manifest_cannot_resolve_is_recorded_as_unknown(monkeypatch, tmp_path):
    """These describe the build, not the data. A run whose table, digests and sidecars are
    all correct must not be thrown away because a version string could not be read — and
    `PackageNotFoundError` is the one that happens, on a from-source run with no installed
    distribution. It is also an ImportError, which no `except` in the handler catches, so
    unguarded it would be a traceback rather than a build.
    """
    _patched(monkeypatch)

    def _absent(name):
        raise ftc.metadata.PackageNotFoundError(name)

    monkeypatch.setattr(ftc.metadata, "version", _absent)
    args = _namespace(tmp_path)

    assert ftc._handle_feature_table_build(args, parser=None) == 0
    tools = _manifest(tmp_path)["tools"]
    assert tools["qiita_cli"] == "unknown"
    # The two it could still read are unaffected.
    assert tools["duckdb"] and tools["duckdb"] != "unknown"
    assert tools["miint"] != "unknown"


class _VersionCatalog:
    """A connection stub answering the two version queries. `duckdb_extensions()` cannot be
    made to yield a NULL version through a real install, so the guard for that row is
    pinned here instead of by inspection."""

    def __init__(self, miint_row):
        self._miint_row = miint_row

    def execute(self, sql):
        self._row = ("v1.5.4",) if "SELECT version()" in sql else self._miint_row
        return self

    def fetchone(self):
        return self._row


@pytest.mark.parametrize("miint_row", [None, (None,)], ids=["no-row", "null-version"])
def test_an_unresolvable_miint_version_is_a_string_not_a_json_null(miint_row):
    """A row that is present but carries no version is a non-empty tuple, so testing the
    tuple alone would return None and write `"miint": null` — breaking the declared
    `dict[str, str]` while looking like a working fallback."""
    tools = ftc._tool_versions(_VersionCatalog(miint_row))
    assert tools["miint"] == "unknown"
    assert all(isinstance(value, str) for value in tools.values())
