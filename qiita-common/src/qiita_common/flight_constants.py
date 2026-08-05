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

# The only codec the data plane will apply. LZ4 is deliberately not offered:
# M0 measured it at roughly half zstd's ratio on every production shape, and the
# server rejects it rather than quietly serving a worse stream.
IPC_COMPRESSION_ZSTD = "zstd"
# Explicitly asking for no compression. Equivalent to sending no header; it
# exists so a client can be unambiguous rather than relying on absence.
IPC_COMPRESSION_NONE = "none"


def ipc_compression_headers(compress: bool) -> list[tuple[bytes, bytes]]:
    """gRPC metadata asking the data plane to compress this DoGet's IPC body.

    Pass the result as `FlightCallOptions(headers=...)`. Returns `[]` when
    `compress` is false, so the request is indistinguishable from one made by a
    client that predates the feature.

    **Compression is not free, and off is the right default for most callers.**
    Whether it pays depends on the client's bandwidth: it is a large win over a
    slow link and a *loss* over a fast one. M0 measured the break-even at
    ~4 Gbit/s — a 775 MiB stream takes 0.65 s uncompressed over 10 GbE against
    1.53 s with zstd. Every in-repo caller sits above that (the control-plane
    runner reaches the data plane over loopback, compute jobs over the cluster
    fabric), so they send nothing. Turn it on for links slower than that.
    """
    if not compress:
        return []
    return [(IPC_COMPRESSION_HEADER.encode(), IPC_COMPRESSION_ZSTD.encode())]
