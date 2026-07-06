"""JWKS harness for OIDC-related tests.

Provides the `JwksHarness` helper class — an in-process HTTP server that
serves a JWKS document and signs JWTs against a private RSA key — and the
function-scoped `jwks_harness` fixture that lifecycle-manages it.

Counts JWKS fetches so callers can assert caching/refresh behavior. Used by
control-plane unit tests of the OIDC verifier and by integration/control-plane
tests that exercise the full auth resolver / endpoint stack.
"""

import json
import secrets
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from jwt.algorithms import RSAAlgorithm

# Pinned IPv4 (not "localhost") so the server bind and the issuer URL agree
# byte-for-byte. On macOS "localhost" can resolve to ::1, which wouldn't
# reach the IPv4-bound socket.
_LOOPBACK_HOST = "127.0.0.1"

# Prefix for generated JWK key ids; the suffix is random hex per harness
# instance and per rotation, so kid values collide across runs only if the
# random suffix collides.
_KID_PREFIX = "kid-"


class JwksHarness:
    """Local HTTP server that serves a JWKS document and signs JWTs.

    Use as a context manager or via the `jwks_harness` pytest fixture.
    Counts JWKS fetches so tests can assert caching/refresh behavior.
    """

    def __init__(self) -> None:
        self.fetch_count = 0
        self._lock = threading.Lock()
        self._private_key = self._gen_key()
        self._kid = f"{_KID_PREFIX}{secrets.token_hex(4)}"
        self._jwks = self._build_jwks(self._private_key, self._kid)
        self._server = HTTPServer((_LOOPBACK_HOST, 0), self._make_handler())
        self.port = self._server.server_address[1]
        # poll_interval bounds how long shutdown() blocks waiting for the
        # serve_forever loop to observe the stop flag. The 0.5s default added
        # ~0.5s to every teardown — with ~50 tests taking this fixture, the
        # dominant repeated cost in --durations, and pure wall-clock wait that
        # neither faster CPUs nor more xdist workers shrink. 10ms is plenty.
        self._thread = threading.Thread(
            target=self._server.serve_forever, args=(0.01,), daemon=True
        )
        self._thread.start()

    @property
    def issuer(self) -> str:
        return f"http://{_LOOPBACK_HOST}:{self.port}"

    @property
    def jwks_url(self) -> str:
        return f"{self.issuer}/connect/jwks"

    @property
    def current_kid(self) -> str:
        return self._kid

    def sign(
        self,
        claims: dict,
        *,
        kid: str | None = None,
        key=None,
    ) -> str:
        """Sign a JWT with the harness's private key (or a caller-supplied one)."""
        return jwt.encode(
            claims,
            key or self._private_key,
            algorithm="RS256",
            headers={"kid": kid or self._kid},
        )

    def rotate_key(self) -> str:
        """Replace the served key. Returns the new kid."""
        with self._lock:
            self._private_key = self._gen_key()
            self._kid = f"{_KID_PREFIX}{secrets.token_hex(4)}"
            self._jwks = self._build_jwks(self._private_key, self._kid)
        return self._kid

    def shutdown(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=2)

    @staticmethod
    def _gen_key():
        return rsa.generate_private_key(public_exponent=65537, key_size=2048)

    @staticmethod
    def _build_jwks(private_key, kid: str) -> dict:
        public_jwk = json.loads(RSAAlgorithm.to_jwk(private_key.public_key()))
        return {"keys": [{**public_jwk, "kid": kid, "alg": "RS256", "use": "sig"}]}

    def _make_handler(self):
        harness = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                if self.path == "/connect/jwks":
                    with harness._lock:
                        harness.fetch_count += 1
                        body = json.dumps(harness._jwks).encode()
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(body)))
                    self.send_header("Cache-Control", "no-cache")
                    self.end_headers()
                    self.wfile.write(body)
                else:
                    self.send_error(404)

            def log_message(self, *args, **kwargs):
                # Silence the access log; tests assert via fetch_count.
                pass

        return Handler


@pytest.fixture
def jwks_harness():
    h = JwksHarness()
    yield h
    h.shutdown()
