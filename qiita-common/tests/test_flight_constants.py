"""Unit tests for the Flight DoGet IPC-compression request header.

The header name is a wire contract with the data plane's Rust twin
(`IPC_COMPRESSION_HEADER` in `qiita-data-plane/src/flight_service.rs`), so it is
pinned by value here: a rename on one side alone silently stops compressing
rather than failing, which is the worst outcome available.
"""

from qiita_common.flight_constants import (
    IPC_COMPRESSION_HEADER,
    IPC_COMPRESSION_NONE,
    IPC_COMPRESSION_ZSTD,
    ipc_compression_headers,
)


def test_header_name_and_values_match_the_data_plane():
    # gRPC metadata keys must be lowercase; the server looks up exactly this.
    assert IPC_COMPRESSION_HEADER == "qiita-ipc-compression"
    assert IPC_COMPRESSION_HEADER == IPC_COMPRESSION_HEADER.lower()
    assert IPC_COMPRESSION_ZSTD == "zstd"
    assert IPC_COMPRESSION_NONE == "none"


def test_requesting_compression_emits_the_zstd_header():
    headers = ipc_compression_headers(True)
    assert headers == [(b"qiita-ipc-compression", b"zstd")]


def test_not_requesting_compression_emits_no_header_at_all():
    # Absent, not `none`: the default must be indistinguishable from a client
    # that predates this feature, so nothing about existing traffic changes.
    assert ipc_compression_headers(False) == []


def test_headers_are_bytes_as_pyarrow_requires():
    for key, value in ipc_compression_headers(True):
        assert isinstance(key, bytes)
        assert isinstance(value, bytes)


def test_constants_match_the_rust_ones_exactly():
    """These three are hand-copies of the data plane's constants
    (`flight_service.rs`). Rust cannot import the Python and vice versa, so the
    values are parsed out of the source — the same approach
    `test_cp_doget_allowlist_matches_the_rust_one_exactly` takes for the DoGet
    allowlist.

    Drift here fails **silently**, which is why it needs a test: rename the
    header on one side and clients quietly stop getting compression, with every
    request still succeeding. Change a codec value and requests start being
    rejected as unsupported, naming a value the client thought it sent.
    """
    import re
    from pathlib import Path

    src = (
        Path(__file__).resolve().parents[2] / "qiita-data-plane" / "src" / "flight_service.rs"
    ).read_text()

    def rust_const(name: str) -> str:
        m = re.search(rf'const {name}: &str = "([^"]+)";', src)
        assert m, f"{name} not found in flight_service.rs"
        return m.group(1)

    assert rust_const("IPC_COMPRESSION_HEADER") == IPC_COMPRESSION_HEADER
    assert rust_const("IPC_COMPRESSION_ZSTD") == IPC_COMPRESSION_ZSTD
    assert rust_const("IPC_COMPRESSION_NONE") == IPC_COMPRESSION_NONE
