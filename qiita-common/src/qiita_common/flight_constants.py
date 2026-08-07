"""Wire constants for Arrow Flight DoGet requests.

Single-sourced HERE for the same reason as `parquet.py`: qiita-common is the one
module both the control plane and the compute orchestrator depend on, so a change
to a Flight wire contract touches exactly one place. This module is deliberately
**pyarrow-free** — pyarrow is not a qiita-common dependency, and every caller
imports `pyarrow.flight` lazily at its own call site.
"""

# gRPC metadata key by which a DoGet client asks for a compressed IPC body.
#
# Lowercase because HTTP/2 requires it of header names. **Rust twin:**
# `IPC_COMPRESSION_HEADER` in `qiita-data-plane/src/flight_service.rs` — the two
# are a wire contract and must change together.
IPC_COMPRESSION_HEADER = "qiita-ipc-compression"

# The only codec the data plane will apply; anything else is rejected rather
# than quietly downgraded. Why zstd alone, and why compression defaults to off:
# `docs/architecture.md`, "DoGet stream compression".
IPC_COMPRESSION_ZSTD = "zstd"
# Explicitly asking for no compression. Equivalent to sending no header; it
# exists so a client can be unambiguous rather than relying on absence.
IPC_COMPRESSION_NONE = "none"


def ipc_compression_headers(compress: bool) -> list[tuple[bytes, bytes]]:
    """gRPC metadata asking the data plane to compress this DoGet's IPC body.

    Pass the result as `FlightCallOptions(headers=...)`. Returns `[]` when
    `compress` is false, so the request is indistinguishable from one made by a
    client that predates the feature.

    **Do not pass `True` reflexively.** Above a break-even bandwidth compression
    makes a DoGet *slower*, and every in-repo caller is above it; this is for
    clients on slow links. The arithmetic is in `docs/architecture.md`.
    """
    if not compress:
        return []
    return [(IPC_COMPRESSION_HEADER.encode(), IPC_COMPRESSION_ZSTD.encode())]
