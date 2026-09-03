"""Shared fakes for the CLI tests that drive an upload.

`qiita reference load` and `qiita submit-reads` both go through
`cli.reference_load.upload_file`, so they share the stand-ins for the two
services it talks to: a mocked control-plane REST surface and a fake Flight
client. pyarrow.flight needs a running gRPC server, and tests at this tier
should not spawn one — the fake records the FlightDescriptor.cmd bytes (the
signed DoPut ticket) and returns a canned PutResult, which the CLI then
forwards to /upload/{idx}/done.
"""

from __future__ import annotations

import base64
import json

import httpx
import pytest
from qiita_common.api_paths import (
    URL_REFERENCE_PREFIX,
    URL_UPLOAD_PREFIX,
    URL_WORK_TICKET_PREFIX,
)


class _FakeWriter:
    def __init__(self):
        self.batches = []
        self.done = False
        self.closed = False

    def write_batch(self, batch):
        self.batches.append(batch)

    def done_writing(self):
        self.done = True

    def close(self):
        self.closed = True


class _FakeReader:
    def __init__(self, put_metadata_bytes: bytes):
        self._payload = put_metadata_bytes

    def read(self):
        # pyarrow exposes the metadata as a Buffer-like — for the CLI's
        # use, returning the raw bytes object works because the helper
        # wraps `bytes(put_metadata)` before decoding.
        return self._payload


class FakeFlightClient:
    """Records each DoPut invocation and returns a scripted PutResult.
    The CLI calls `client.do_put(descriptor, schema)` → (writer, reader);
    we capture the ticket bytes from descriptor.cmd and return canned
    metadata."""

    def __init__(self):
        self.calls: list[bytes] = []
        # `responses` is a list of (upload_idx, sha256) tuples consumed
        # in order, one per do_put call. Empty → the CLI's invariant
        # check (put_body['upload_idx'] == upload_idx) drives the value.
        self.responses: list[dict] = []
        self._next_upload_idx = 1

    def queue_response(self, upload_idx: int, *, sha256: str = "a" * 64, row_count: int = 1):
        self.responses.append(
            {
                "upload_idx": upload_idx,
                "sha256": sha256,
                "row_count": row_count,
                "bytes_received": 1024,
            }
        )

    def do_put(self, descriptor, schema):
        self.calls.append(bytes(descriptor.command))
        if not self.responses:
            raise RuntimeError("FakeFlightClient: no scripted response remaining")
        body = self.responses.pop(0)
        return _FakeWriter(), _FakeReader(json.dumps(body).encode())

    def close(self):
        pass


@pytest.fixture
def upload_state():
    """Track minted slots: maps upload_idx → status. Lets the route
    fixture transition pending → ready on /done."""
    return {"next_idx": 100, "slots": {}}


@pytest.fixture
def reference_state():
    """Controls the `is_host` the mock GET /reference/{idx} reports — used by
    the `--host` + `--reference-idx` bind-path verification tests. Default
    false; tests that bind to a host reference flip it to true."""
    return {"is_host": False}


@pytest.fixture
def cp_transport(upload_state, reference_state):
    """Mock the CP REST surface the CLI hits: POST /reference,
    GET /reference/{idx}, POST /upload, POST /upload/{idx}/done,
    POST /work-ticket, GET /work-ticket/{idx}. Returns the AsyncTransport +
    the captured call log."""
    calls: list[tuple[str, str, dict | None]] = []
    work_tickets: dict[int, dict] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content) if request.content else None
        calls.append((request.method, request.url.path, body))
        path = request.url.path
        if path == URL_REFERENCE_PREFIX and request.method == "POST":
            return httpx.Response(
                201,
                json={
                    "reference_idx": 999,
                    "name": body["name"],
                    "version": body["version"],
                    "kind": body["kind"],
                    "status": "pending",
                    "created_by_idx": 1,
                    "created_at": "2026-05-20T00:00:00Z",
                },
            )
        if path.startswith(f"{URL_REFERENCE_PREFIX}/") and request.method == "GET":
            ref_idx = int(path.split("/")[-1])
            return httpx.Response(
                200,
                json={
                    "reference_idx": ref_idx,
                    "name": "existing",
                    "version": "1.0",
                    "kind": "sequence_reference",
                    "status": "active",
                    "is_host": reference_state["is_host"],
                    "created_by_idx": 1,
                    "created_at": "2026-05-20T00:00:00Z",
                },
            )
        if path == URL_UPLOAD_PREFIX and request.method == "POST":
            upload_idx = upload_state["next_idx"]
            upload_state["next_idx"] += 1
            upload_state["slots"][upload_idx] = "pending"
            # Token bytes mimic the CP's signed payload shape; the fake
            # flight client doesn't verify, just records.
            ticket_bytes = f"signed-ticket-for-{upload_idx}".encode()
            return httpx.Response(
                201,
                json={
                    "upload_idx": upload_idx,
                    "doput_ticket": base64.b64encode(ticket_bytes).decode(),
                },
            )
        if path.startswith(f"{URL_UPLOAD_PREFIX}/") and path.endswith("/done"):
            upload_idx = int(path.split("/")[-2])
            upload_state["slots"][upload_idx] = "ready"
            return httpx.Response(
                200,
                json={
                    "upload_idx": upload_idx,
                    "status": "ready",
                    "sha256": body["sha256"],
                    "row_count": body["row_count"],
                    "bytes_received": body["bytes_received"],
                    "created_by_idx": 1,
                    "created_at": "2026-05-20T00:00:00Z",
                    "completed_at": "2026-05-20T00:00:01Z",
                },
            )
        if path == URL_WORK_TICKET_PREFIX and request.method == "POST":
            idx = 4242
            work_tickets[idx] = {"work_ticket_idx": idx, "state": "completed"}
            return httpx.Response(202, json={"work_ticket_idx": idx, "state": "pending"})
        if path.startswith(f"{URL_WORK_TICKET_PREFIX}/") and request.method == "GET":
            idx = int(path.split("/")[-1])
            return httpx.Response(200, json=work_tickets.get(idx, {"state": "completed"}))
        return httpx.Response(404, text=f"unhandled mock path: {request.method} {path}")

    transport = httpx.MockTransport(handler)
    return transport, calls
