"""Contract tests for `jobs._blob_input.resolve_reads_blob_input`.

Read ingest takes a sample's reads by two routes — a host path a wet-lab admin
named, or a chunked-BLOB upload a user DoPut — and both reach the step under
the same `fastq_path` / `bam_path` binding. What the resolver has to get right
is which of the two it was handed, and, for the upload, what to call the file
it stitches: miint reads compression off the extension
(<https://the-miint.github.io/duckdb-miint/reading/>), so the name has to match
the bytes.
"""

from __future__ import annotations

import gzip

import duckdb
import pytest
from helpers import write_chunked_blob_upload

from qiita_compute_orchestrator.jobs._blob_input import resolve_reads_blob_input

_PLAIN_FASTQ = b"@r1\nACGT\n+\nIIII\n"

# What `fastq_to_parquet` and `bam_to_parquet` respectively pass for the two
# answers the gzip sniff can give.
_FASTQ_SUFFIXES = {"gzipped_suffix": ".fastq.gz", "plain_suffix": ".fastq"}
_ALIGNMENT_SUFFIXES = {"gzipped_suffix": ".bam", "plain_suffix": ".sam"}


def _resolve_fastq(conn, path, out_dir, stem="R1"):
    return resolve_reads_blob_input(conn, path=path, out_dir=out_dir, stem=stem, **_FASTQ_SUFFIXES)


def _resolve_alignment(conn, path, out_dir, stem="reads"):
    return resolve_reads_blob_input(
        conn, path=path, out_dir=out_dir, stem=stem, **_ALIGNMENT_SUFFIXES
    )


@pytest.fixture
def conn():
    with duckdb.connect() as c:
        yield c


# ---------------------------------------------------------------------------
# Which shape was handed over
# ---------------------------------------------------------------------------


def test_raw_host_path_is_returned_untouched(conn, tmp_path):
    """The wet-lab-admin route: the binding already IS the FASTQ, and nothing
    should be copied or renamed."""
    fastq = tmp_path / "ABC_R1.fastq"
    fastq.write_bytes(_PLAIN_FASTQ)
    assert _resolve_fastq(conn, fastq, tmp_path / "out") == fastq
    assert not (tmp_path / "out").exists()


def test_raw_gzipped_host_path_is_returned_untouched(conn, tmp_path):
    fastq = tmp_path / "ABC_R1.fastq.gz"
    fastq.write_bytes(gzip.compress(_PLAIN_FASTQ))
    assert _resolve_fastq(conn, fastq, tmp_path / "out") == fastq


# ---------------------------------------------------------------------------
# Naming the stitched file
# ---------------------------------------------------------------------------


def test_plaintext_upload_is_stitched_as_fastq(conn, tmp_path):
    upload = write_chunked_blob_upload(tmp_path / "upload.parquet", _PLAIN_FASTQ)
    out = _resolve_fastq(conn, upload, tmp_path / "ws")
    assert out.name == "R1.fastq"
    assert out.read_bytes() == _PLAIN_FASTQ


def test_gzipped_upload_is_stitched_as_fastq_gz(conn, tmp_path):
    """The `.gz` has to be on the name, not just in the bytes: miint decides
    whether to inflate from the extension, so a stitched `.fastq` holding gzip
    bytes parses as garbage."""
    payload = gzip.compress(_PLAIN_FASTQ)
    upload = write_chunked_blob_upload(tmp_path / "upload.parquet", payload)
    out = _resolve_fastq(conn, upload, tmp_path / "ws")
    assert out.name == "R1.fastq.gz"
    assert out.read_bytes() == payload
    assert gzip.decompress(out.read_bytes()) == _PLAIN_FASTQ


def test_stitched_bytes_are_ordered_by_chunk_index(conn, tmp_path):
    """The fixture inserts chunk 1 before chunk 0 on purpose — reassembly is by
    `chunk_index`, not by row order."""
    payload = b"".join(f"@r{i}\nACGT\n+\nIIII\n".encode() for i in range(200))
    upload = write_chunked_blob_upload(tmp_path / "upload.parquet", payload)
    out = _resolve_fastq(conn, upload, tmp_path / "ws")
    assert out.read_bytes() == payload


def test_stem_names_the_read(conn, tmp_path):
    """R1 and R2 stitch to distinct files in the same workspace."""
    upload = write_chunked_blob_upload(tmp_path / "upload.parquet", _PLAIN_FASTQ)
    r1 = _resolve_fastq(conn, upload, tmp_path / "ws")
    r2 = _resolve_fastq(conn, upload, tmp_path / "ws", stem="R2")
    assert {r1.name, r2.name} == {"R1.fastq", "R2.fastq"}


# ---------------------------------------------------------------------------
# The alignment loader reads the same sniff differently
# ---------------------------------------------------------------------------


def test_compressed_alignment_upload_is_stitched_as_bam(conn, tmp_path):
    """BGZF is gzip, so a compressed payload under the alignment suffixes is the
    binary container."""
    upload = write_chunked_blob_upload(tmp_path / "upload.parquet", gzip.compress(b"BAM\x01x"))
    assert _resolve_alignment(conn, upload, tmp_path / "ws").name == "reads.bam"


def test_plaintext_alignment_upload_is_stitched_as_sam(conn, tmp_path):
    """The loader takes SAM as well as BAM. Naming an uncompressed payload
    `.bam` would hand text to the binary parser."""
    upload = write_chunked_blob_upload(tmp_path / "upload.parquet", b"@HD\tVN:1.6\nr1\t4\t*\n")
    assert _resolve_alignment(conn, upload, tmp_path / "ws").name == "reads.sam"


def test_the_same_bytes_get_different_names_per_format(conn, tmp_path):
    """The sniff answers one question — is this gzipped — and each caller says
    what that means for its own format. Same payload, two names."""
    upload = write_chunked_blob_upload(tmp_path / "upload.parquet", gzip.compress(_PLAIN_FASTQ))
    assert _resolve_fastq(conn, upload, tmp_path / "ws-fq").name == "R1.fastq.gz"
    assert _resolve_alignment(conn, upload, tmp_path / "ws-aln").name == "reads.bam"


# ---------------------------------------------------------------------------
# Malformed uploads fail loudly
# ---------------------------------------------------------------------------


def test_chunkless_upload_raises(conn, tmp_path):
    dest = tmp_path / "upload.parquet"
    conn.execute("CREATE OR REPLACE TABLE up (chunk_index INTEGER, chunk_data BLOB)")
    conn.execute(f"COPY up TO '{dest}' (FORMAT PARQUET)")
    with pytest.raises(ValueError, match="carries no chunks"):
        _resolve_fastq(conn, dest, tmp_path / "ws")


def test_null_chunk_raises(conn, tmp_path):
    dest = tmp_path / "upload.parquet"
    conn.execute("CREATE OR REPLACE TABLE up (chunk_index INTEGER, chunk_data BLOB)")
    conn.execute("INSERT INTO up VALUES (0, ?), (1, NULL)", [_PLAIN_FASTQ])
    conn.execute(f"COPY up TO '{dest}' (FORMAT PARQUET)")
    with pytest.raises(ValueError, match="NULL chunk_data"):
        _resolve_fastq(conn, dest, tmp_path / "ws")
