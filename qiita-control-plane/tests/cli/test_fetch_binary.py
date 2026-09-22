"""Contract tests for `cli._common.fetch_binary` — the generic bulk-binary read.

Nothing here is about the genome map; it is the transport helper's own behaviour,
pinned with a real socket server because the CLI's `httpx.Response` fakes yield a
whole body as one chunk and so never exercise the loop.

The map's own wire properties live in `tests/test_genome_map_parquet_contract.py`.
"""

import socket
import threading
import time

import httpx
import pytest
from qiita_common.api_paths import LOOPBACK_HOST
from qiita_common.parquet import PARQUET_MEDIA_TYPE

from qiita_control_plane.cli import _common


def _serve_once(handler):
    srv = socket.socket()
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((LOOPBACK_HOST, 0))
    srv.listen(1)
    port = srv.getsockname()[1]

    def run():
        conn, _ = srv.accept()
        conn.recv(65536)
        try:
            handler(conn)
        except OSError:
            pass
        conn.close()
        srv.close()

    threading.Thread(target=run, daemon=True).start()
    return port


def _headers(length):
    return (
        b"HTTP/1.1 200 OK\r\nContent-Type: " + PARQUET_MEDIA_TYPE.encode() + b"\r\n"
        b"Content-Length: " + str(length).encode() + b"\r\n\r\n"
    )


def test_fetch_binary_returns_a_complete_body():
    """Control for the rate-floor test below: a body arriving above the floor rate
    completes. Without it the abort test shows only that the helper can fail."""
    payload = b"P" * 200_000
    port = _serve_once(lambda c: c.sendall(_headers(len(payload)) + payload))
    body, content_type = _common.fetch_binary(
        f"http://{LOOPBACK_HOST}:{port}", "tok", "/x", accept=PARQUET_MEDIA_TYPE
    )
    assert body == payload
    assert content_type == PARQUET_MEDIA_TYPE


def test_fetch_binary_gives_up_below_the_floor_rate():
    """httpx's timeout is per-read, so a body dribbling forever never trips it.
    The observed-rate floor is what bounds the transfer."""
    payload = b"P" * 200_000

    def dribble(conn):
        conn.sendall(_headers(len(payload)))
        for i in range(0, len(payload), 10_000):
            conn.sendall(payload[i : i + 10_000])
            time.sleep(0.5)

    port = _serve_once(dribble)
    with pytest.raises(_common.SlowResponseError) as exc:
        _common.fetch_binary(
            f"http://{LOOPBACK_HOST}:{port}",
            "tok",
            "/x",
            accept=PARQUET_MEDIA_TYPE,
            floor_rate=1_000_000,
            slack=0.2,
        )
    # The message must name what was actually observed, or an operator cannot
    # tell a slow link from a wedged server.
    assert "KB/s" in str(exc.value)


def test_fetch_binary_raises_on_a_body_cut_short():
    """A peer that closes against a declared Content-Length is a protocol error,
    not a short read — the second guard behind Parquet's own footer check."""
    payload = b"P" * 200_000
    port = _serve_once(lambda c: c.sendall(_headers(len(payload)) + payload[: len(payload) // 2]))
    with pytest.raises(httpx.RemoteProtocolError):
        _common.fetch_binary(
            f"http://{LOOPBACK_HOST}:{port}", "tok", "/x", accept=PARQUET_MEDIA_TYPE
        )


def test_fetch_binary_accepts_a_chunked_body_with_no_content_length():
    """A body with no `Content-Length` must still work.

    Enabling `gzip` at the gateway makes every response chunked with no length
    (measured: nginx 1.27, `gzip_types` covering this media type). A client that
    refused a length-less body would break the day anyone enabled it.
    """
    payload = b"P" * 50_000

    def chunked(conn):
        conn.sendall(
            b"HTTP/1.1 200 OK\r\nContent-Type: " + PARQUET_MEDIA_TYPE.encode() + b"\r\n"
            b"Transfer-Encoding: chunked\r\n\r\n"
        )
        conn.sendall(b"%x\r\n" % len(payload) + payload + b"\r\n0\r\n\r\n")

    port = _serve_once(chunked)
    body, _ = _common.fetch_binary(
        f"http://{LOOPBACK_HOST}:{port}", "tok", "/x", accept=PARQUET_MEDIA_TYPE
    )
    assert body == payload


def test_fetch_binary_still_bounds_a_body_with_no_content_length():
    """Accepting a length-less body must not mean accepting it forever. The floor
    is measured on observed rate, so it holds without a declared size."""
    payload = b"P" * 50_000

    def slow_chunked(conn):
        conn.sendall(
            b"HTTP/1.1 200 OK\r\nContent-Type: " + PARQUET_MEDIA_TYPE.encode() + b"\r\n"
            b"Transfer-Encoding: chunked\r\n\r\n"
        )
        for i in range(0, len(payload), 5_000):
            part = payload[i : i + 5_000]
            conn.sendall(b"%x\r\n" % len(part) + part + b"\r\n")
            time.sleep(0.4)

    port = _serve_once(slow_chunked)
    with pytest.raises(_common.SlowResponseError):
        _common.fetch_binary(
            f"http://{LOOPBACK_HOST}:{port}",
            "tok",
            "/x",
            accept=PARQUET_MEDIA_TYPE,
            floor_rate=10_000_000,
            slack=0.2,
        )


class _FakeStream:
    """Drives `fetch_binary`'s loop with known chunks on a controlled clock.

    The rate rule is about WHEN observations fall relative to each other, which a
    real socket can only approximate — a loaded runner shifts the timings and the
    test starts flaking (measured: a 0.3s delay before the first body bytes flips
    both keep-the-body cases to an abort, with the rate arithmetic unchanged).
    The socket tests above cover framing and the abort path, where the outcome
    does not turn on where an observation falls inside the slack window.
    """

    def __init__(self, chunks_at, headers=None):
        self.chunks_at = chunks_at  # [(elapsed_seconds, payload), ...]
        self.headers = headers or {"content-type": PARQUET_MEDIA_TYPE}
        self.is_success = True

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def iter_bytes(self):
        for _, payload in self.chunks_at:
            yield payload

    def read(self):  # pragma: no cover - only the success path is driven here
        return b""

    def raise_for_status(self):  # pragma: no cover
        return None


def _drive(monkeypatch, chunks_at, **kwargs):
    """Run `fetch_binary` over `chunks_at`, with the clock stepping to each
    entry's stated elapsed time as its chunk is consumed."""
    stream = _FakeStream(chunks_at)
    # monotonic() is read once before the loop and exactly once per chunk: t=0,
    # then each chunk's own timestamp.
    ticks = iter([0.0, *(at for at, _ in chunks_at)])
    monkeypatch.setattr(_common.time, "monotonic", lambda: next(ticks))
    monkeypatch.setattr(_common.httpx, "stream", lambda *a, **k: stream)
    return _common.fetch_binary("http://cp", "tok", "/x", accept=PARQUET_MEDIA_TYPE, **kwargs)


def test_a_transfer_that_dips_and_recovers_is_not_abandoned(monkeypatch):
    """One observation under the floor is not enough.

    A link that stalls and then bursts back has not failed — the average catches
    up. Requiring two consecutive observations is what makes the recovery
    meaningful rather than the dip fatal; under a one-observation rule this
    transfer dies on its first chunk.
    """
    body, _ = _drive(
        monkeypatch,
        [(1.0, b"S" * 1_000), (2.0, b"B" * 999_000)],  # 1 KB/s, then 500 KB/s
        floor_rate=100_000,
        slack=0.5,
    )
    assert len(body) == 1_000_000


def test_a_breach_first_seen_on_the_final_chunk_keeps_the_body(monkeypatch):
    """A complete body is not discarded for a breach with nothing after it.

    The loop ends without a second observation to confirm, so the transfer is
    returned whole. The property is that narrow one, not "a slow transfer that
    finished is always kept" — had the preceding observation also been under the
    floor, this same complete body would have been discarded, which is what the
    abort test below pins.
    """
    body, _ = _drive(
        monkeypatch,
        [(0.05, b"F" * 20_000), (1.0, b"S" * 20_000)],  # inside slack, then 40 KB/s
        floor_rate=100_000,
        slack=0.5,
    )
    assert len(body) == 40_000


def test_two_consecutive_observations_under_the_floor_do_abort(monkeypatch):
    """The other half of the rule, so the test above cannot pass by never
    aborting at all."""
    with pytest.raises(_common.SlowResponseError):
        _drive(
            monkeypatch,
            [(1.0, b"S" * 1_000), (2.0, b"S" * 1_000)],
            floor_rate=100_000,
            slack=0.5,
        )


def test_an_error_body_is_read_before_the_status_is_raised():
    """A streamed response has not read its body when the status is checked, so
    `raise_for_status` alone would give the caller a `ResponseNotRead` the moment
    anything touched `.text`. The CLI prints exactly that text.

    Pinned over a real socket: the `httpx.Response` fakes elsewhere set `_content`
    at construction, so they are already read and cannot catch this.
    """
    detail = b'{"detail":"Reference not found"}'

    def not_found(conn):
        conn.sendall(
            b"HTTP/1.1 404 Not Found\r\nContent-Type: application/json\r\n"
            b"Content-Length: " + str(len(detail)).encode() + b"\r\n\r\n" + detail
        )

    port = _serve_once(not_found)
    with pytest.raises(httpx.HTTPStatusError) as exc:
        _common.fetch_binary(
            f"http://{LOOPBACK_HOST}:{port}", "tok", "/x", accept=PARQUET_MEDIA_TYPE
        )
    assert exc.value.response.status_code == 404
    assert "Reference not found" in exc.value.response.text
