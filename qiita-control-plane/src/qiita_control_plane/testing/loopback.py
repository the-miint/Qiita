"""A local HTTP server for tests that need a stand-in for a remote service."""

import threading
from collections.abc import Iterator
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, HTTPServer

from qiita_common.api_paths import LOOPBACK_HOST

# How long shutdown() waits for the serve_forever loop to notice the stop flag;
# the 0.5s default adds ~0.5s of pure wall-clock wait to every teardown.
_POLL_INTERVAL_SECONDS = 0.01


@contextmanager
def serve_on_loopback(handler_class: type[BaseHTTPRequestHandler]) -> Iterator[int]:
    """Serve `handler_class` on an OS-picked loopback port for the life of the
    block, yielding the port; the server is shut down and closed on exit."""
    server = HTTPServer((LOOPBACK_HOST, 0), handler_class)
    port = server.server_address[1]
    thread = threading.Thread(
        target=server.serve_forever, args=(_POLL_INTERVAL_SECONDS,), daemon=True
    )
    thread.start()
    try:
        yield port
    finally:
        server.shutdown()
        server.server_close()
        thread.join()
