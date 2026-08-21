"""Integration test: sign ticket in Python → DoGet via pyarrow.flight → verify Arrow data.

Relies on the shared `data_plane` fixture in conftest.py, which starts the
pre-built binary and lets it own schema creation. Test rows are seeded via
a short-lived DuckDB connection after the data plane is live.
"""

import pyarrow as pa
import pyarrow.flight as flight
import pytest
from qiita_common.api_paths import LOOPBACK_HOST

from conftest import ducklake_connect

# Deterministic, out-of-band feature_idx values for this module's seeded rows.
SEED_FEATURE_IDXS = [700001, 700002]


@pytest.fixture(scope="module", autouse=True)
def _seed_reference_rows(data_plane):
    """Seed reference rows against the live DuckLake (tables created by data plane)."""
    conn = ducklake_connect(data_plane["data_path"])
    try:
        conn.execute(
            "INSERT INTO qiita_lake.reference_sequences VALUES "
            "(700001, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::UUID, 8), "
            "(700002, 'b0eebc99-9c0b-4ef8-bb6d-6bb9bd380a22'::UUID, 8)"
        )
        conn.execute(
            "INSERT INTO qiita_lake.reference_sequence_chunks VALUES "
            "(700001, 0, 'ATCGATCG'), (700002, 0, 'GCTAGCTA')"
        )
        conn.execute(
            "INSERT INTO qiita_lake.reference_membership VALUES "
            "(1, 700001), (1, 700002)"
        )
    finally:
        conn.close()


# One assembly run, and the three neighbours a mis-scoped resolution would pick
# up. `ASSEMBLY_RUN` is the run under test; every other contig below is seeded
# into the SAME two tables, interleaved by feature_idx, so nothing separates them
# except which membership rows name them.
ASSEMBLY_SAMPLE, ASSEMBLY_RUN = 8801, 9901
ASSEMBLY_OTHER_RUN = 9902  # same sample, second assembly run
ASSEMBLY_OTHER_SAMPLE = 8802  # same run identity, second sample

# The run's own contigs: one LCG, one MAG, and one that a SECOND (kind, bin_id)
# of the same run also claims (700013) — the duplicate a semi join must collapse.
# 700013 is additionally claimed by the other run: a contig two runs assembled to
# identical bytes is ONE content-deduped feature_idx, so it belongs to both.
ASSEMBLY_RUN_FEATURE_IDXS = [700011, 700013]
ASSEMBLY_OTHER_RUN_FEATURE_IDXS = [700012, 700013]
ASSEMBLY_OTHER_SAMPLE_FEATURE_IDX = 700014
ASSEMBLY_UNCLAIMED_FEATURE_IDX = 700015  # a contig no membership row names


@pytest.fixture(scope="module", autouse=True)
def _seed_assembly_rows(data_plane):
    """Seed the two assembly surfaces and their membership against the live
    DuckLake.

    Neither sequence table has a `prep_sample_idx` column — a contig is stored
    once, keyed by the content-deduped `feature_idx` — so "this run's contigs" is
    a fact only `assembly_membership` holds, and resolving it is what the data
    plane's `build_assembly_run_query` does. Seeding all four neighbours into one
    table is what makes that resolution falsifiable.
    """
    conn = ducklake_connect(data_plane["data_path"])
    try:
        conn.execute(
            "INSERT INTO qiita_lake.assembled_sequence VALUES "
            "(700011, 'c0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::UUID, 8), "
            "(700012, 'c0eebc99-9c0b-4ef8-bb6d-6bb9bd380a22'::UUID, 8), "
            "(700013, 'c0eebc99-9c0b-4ef8-bb6d-6bb9bd380a33'::UUID, 8), "
            "(700014, 'c0eebc99-9c0b-4ef8-bb6d-6bb9bd380a44'::UUID, 8), "
            "(700015, 'c0eebc99-9c0b-4ef8-bb6d-6bb9bd380a55'::UUID, 8)"
        )
        conn.execute(
            "INSERT INTO qiita_lake.assembled_sequence_chunks VALUES "
            "(700011, 0, 'AAAACCCC'), (700012, 0, 'GGGGTTTT'), "
            "(700013, 0, 'ACGT'), (700013, 1, 'TGCA'), "
            "(700014, 0, 'TTTTAAAA'), (700015, 0, 'CCCCGGGG')"
        )
        conn.execute(
            "INSERT INTO qiita_lake.assembly_membership VALUES "
            f"({ASSEMBLY_SAMPLE}, {ASSEMBLY_RUN}, 'LCG', 'contig_1', 700011), "
            f"({ASSEMBLY_SAMPLE}, {ASSEMBLY_RUN}, 'MAG', 'bin.1', 700013), "
            # Same contig, second bin of the SAME run — one output row, not two.
            f"({ASSEMBLY_SAMPLE}, {ASSEMBLY_RUN}, 'MAG', 'bin.2', 700013), "
            f"({ASSEMBLY_SAMPLE}, {ASSEMBLY_OTHER_RUN}, 'MAG', 'bin.1', 700012), "
            f"({ASSEMBLY_SAMPLE}, {ASSEMBLY_OTHER_RUN}, 'MAG', 'bin.1', 700013), "
            f"({ASSEMBLY_OTHER_SAMPLE}, {ASSEMBLY_RUN}, 'MAG', 'bin.1', 700014)"
        )
    finally:
        conn.close()


def _assembly_run_filter(prep_sample_idx=ASSEMBLY_SAMPLE, processing_idx=ASSEMBLY_RUN):
    return {"prep_sample_idx": [prep_sample_idx], "processing_idx": [processing_idx]}


@pytest.fixture
def flight_client(data_plane):
    client = flight.FlightClient(f"grpc://{LOOPBACK_HOST}:{data_plane['port']}")
    yield client
    client.close()


def _sign_ticket(table, filter_dict, secret_bytes):
    from qiita_control_plane.auth.tickets import sign_ticket

    return sign_ticket(table=table, filter=filter_dict, secret=secret_bytes)


def test_doget_reference_sequences(data_plane, flight_client):
    """DoGet with a valid ticket returns Arrow data from reference_sequences."""
    ticket_bytes = _sign_ticket(
        "reference_sequences",
        {"feature_idx": SEED_FEATURE_IDXS},
        data_plane["secret"],
    )
    reader = flight_client.do_get(flight.Ticket(ticket_bytes))
    table = reader.read_all()

    assert table.num_rows == 2
    assert set(table.column_names) >= {
        "feature_idx",
        "sequence_hash",
        "sequence_length_bp",
    }
    assert sorted(table.column("feature_idx").to_pylist()) == SEED_FEATURE_IDXS


def test_doget_tampered_ticket(data_plane, flight_client):
    """DoGet with a tampered ticket must fail with Unauthenticated."""
    ticket_bytes = bytearray(
        _sign_ticket("reference_sequences", {"feature_idx": [1]}, data_plane["secret"])
    )
    ticket_bytes[10] ^= 0xFF

    with pytest.raises(flight.FlightUnauthenticatedError):
        flight_client.do_get(flight.Ticket(bytes(ticket_bytes))).read_all()


def test_doget_empty_result(data_plane, flight_client):
    """DoGet for non-existent feature_idx returns empty table, not error."""
    ticket_bytes = _sign_ticket(
        "reference_sequences",
        {"feature_idx": [999999]},
        data_plane["secret"],
    )
    reader = flight_client.do_get(flight.Ticket(ticket_bytes))
    assert reader.read_all().num_rows == 0


def test_doget_assembly_chunks_streams_exactly_this_runs_contigs(
    data_plane, flight_client
):
    """The signed pair IS the scope, and the data plane resolves it: the ticket
    names no contig, and what streams is exactly the run's own — excluding the
    same sample's other run, the same run's other sample, and a contig no
    membership row claims. Asserted verbatim; "some rows came back" is not the
    property under test.

    700013 is claimed twice by this run (two bins) and once by the other run: it
    appears once here, because the resolution is a semi join and not a join."""
    ticket_bytes = _sign_ticket(
        "assembled_sequence_chunks", _assembly_run_filter(), data_plane["secret"]
    )
    table = flight_client.do_get(flight.Ticket(ticket_bytes)).read_all()

    # 700013's two chunks reassemble in chunk_index order, the form every
    # consumer of this surface uses.
    rows = sorted(
        zip(
            table.column("feature_idx").to_pylist(),
            table.column("chunk_index").to_pylist(),
            table.column("chunk_data").to_pylist(),
            strict=True,
        )
    )
    assert rows == [
        (700011, 0, "AAAACCCC"),
        (700013, 0, "ACGT"),
        (700013, 1, "TGCA"),
    ]
    returned = set(table.column("feature_idx").to_pylist())
    assert sorted(returned) == ASSEMBLY_RUN_FEATURE_IDXS
    assert 700012 not in returned, "the same sample's other run"
    assert ASSEMBLY_OTHER_SAMPLE_FEATURE_IDX not in returned, (
        "the same run's other sample"
    )
    assert ASSEMBLY_UNCLAIMED_FEATURE_IDX not in returned, "a contig no run claims"


@pytest.mark.parametrize(
    ("prep_sample_idx", "processing_idx", "expected"),
    [
        pytest.param(
            ASSEMBLY_SAMPLE,
            ASSEMBLY_OTHER_RUN,
            ASSEMBLY_OTHER_RUN_FEATURE_IDXS,
            id="same-sample-other-run",
        ),
        pytest.param(
            ASSEMBLY_OTHER_SAMPLE,
            ASSEMBLY_RUN,
            [ASSEMBLY_OTHER_SAMPLE_FEATURE_IDX],
            id="same-run-other-sample",
        ),
        pytest.param(ASSEMBLY_SAMPLE, 9999, [], id="pair-that-never-assembled"),
    ],
)
def test_doget_assembly_each_half_of_the_pair_discriminates(
    data_plane, flight_client, prep_sample_idx, processing_idx, expected
):
    """Neither half of the run key is ignorable. Change only `processing_idx` and
    the SAME sample yields the other run's contigs (700013 in both — one
    content-deduped feature two runs assembled); change only `prep_sample_idx`
    and the same run yields the other sample's. A pair naming no run streams
    nothing rather than everything."""
    ticket_bytes = _sign_ticket(
        "assembled_sequence",
        _assembly_run_filter(prep_sample_idx, processing_idx),
        data_plane["secret"],
    )
    table = flight_client.do_get(flight.Ticket(ticket_bytes)).read_all()
    assert sorted(table.column("feature_idx").to_pylist()) == sorted(expected)
    assert set(table.column_names) >= {
        "feature_idx",
        "sequence_hash",
        "sequence_length_bp",
    }


@pytest.mark.parametrize(
    ("table", "filter_dict", "message"),
    [
        pytest.param(
            "assembled_sequence",
            {},
            "missing single-valued filter column",
            id="unscoped-sequence",
        ),
        pytest.param(
            "assembled_sequence_chunks",
            {},
            "missing single-valued filter column",
            id="unscoped-chunks",
        ),
        pytest.param(
            "assembled_sequence_chunks",
            {"feature_idx": [700011, 700014]},
            "missing single-valued filter column",
            id="contig-roster-is-not-a-scope",
        ),
        pytest.param(
            "assembled_sequence_chunks",
            {
                "prep_sample_idx": [ASSEMBLY_SAMPLE],
                "processing_idx": [ASSEMBLY_RUN, ASSEMBLY_OTHER_RUN],
            },
            "expected exactly one value",
            id="two-runs-blended",
        ),
        pytest.param(
            "assembled_sequence_chunks",
            {
                "prep_sample_idx": [ASSEMBLY_SAMPLE],
                "processing_idx": [ASSEMBLY_RUN],
                "feature_idx": [ASSEMBLY_OTHER_SAMPLE_FEATURE_IDX],
            },
            "accepts only prep_sample_idx and processing_idx",
            id="run-plus-a-smuggled-contig",
        ),
        pytest.param(
            "assembly_membership",
            {"prep_sample_idx": [ASSEMBLY_SAMPLE]},
            "unknown table",
            id="junction-not-readable",
        ),
    ],
)
def test_doget_assembly_off_scope_or_off_surface_is_refused(
    data_plane, flight_client, table, filter_dict, message
):
    """The refusals the assembly path depends on, at the server rather than the
    signer: an unscoped ticket (which would read every sample's contigs), a
    ticket naming contigs directly, one blending two runs into an
    indistinguishable stream, and the membership junction itself — which the
    server READS to resolve a run but will not stream.

    Signed here directly rather than through the control plane — no CP route
    produces any of them, which is the point: the data plane refuses regardless
    of what signed it. InvalidArgument surfaces as ArrowInvalid, not FlightError.
    """
    from qiita_control_plane.auth.tickets import _sign_payload

    ticket_bytes = _sign_payload(
        {"filter": filter_dict, "table": table}, data_plane["secret"]
    )
    with pytest.raises(pa.ArrowInvalid, match=message):
        flight_client.do_get(flight.Ticket(ticket_bytes)).read_all()


def test_doget_zstd_compression_round_trips_through_pyarrow(data_plane, flight_client):
    """The whole chain, with the consumer every caller actually uses: a real
    pyarrow client asks for compression via gRPC metadata, the data plane
    zstd-compresses the IPC bodies, and pyarrow decodes them transparently.

    The Rust tests prove the server stamps the codec; only this proves the
    client can read what it produced. Compressed and uncompressed must return
    identical data — compression is a transport concern and must never be
    observable in the result.
    """
    from qiita_common.flight_constants import ipc_compression_headers

    ticket_bytes = _sign_ticket(
        "reference_sequences",
        {"feature_idx": SEED_FEATURE_IDXS},
        data_plane["secret"],
    )
    plain = flight_client.do_get(flight.Ticket(ticket_bytes)).read_all()
    compressed = flight_client.do_get(
        flight.Ticket(ticket_bytes),
        # Through the shared helper, so the one test that most resembles a real
        # client exercises what a real client actually calls.
        flight.FlightCallOptions(headers=ipc_compression_headers(True)),
    ).read_all()

    assert compressed.equals(plain), "compression changed the data the client sees"
    assert sorted(compressed.column("feature_idx").to_pylist()) == SEED_FEATURE_IDXS


def test_doget_unsupported_codec_is_rejected(data_plane, flight_client):
    """A codec the server will not apply is an error, not a silent fall back to
    uncompressed — a client that asked for compression, did not get it, and was
    not told would draw the wrong conclusion about its own transfer."""
    from qiita_common.flight_constants import IPC_COMPRESSION_HEADER

    ticket_bytes = _sign_ticket(
        "reference_sequences",
        {"feature_idx": SEED_FEATURE_IDXS},
        data_plane["secret"],
    )
    # InvalidArgument surfaces as ArrowInvalid, not FlightError. The assertion is
    # on the message because that is the part a client acts on: it must name the
    # rejected value and the accepted ones.
    with pytest.raises(pa.ArrowInvalid, match=r'unsupported .*"lz4".*"zstd".*"none"'):
        flight_client.do_get(
            flight.Ticket(ticket_bytes),
            flight.FlightCallOptions(
                headers=[(IPC_COMPRESSION_HEADER.encode(), b"lz4")]
            ),
        ).read_all()
