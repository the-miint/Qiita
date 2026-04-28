"""Authentication and ticket signing for the control plane.

Token format for opaque API tokens (PATs and service-account tokens):

    qk_<43 chars of base64url-without-padding>

Total length 46. The body is `secrets.token_urlsafe(32)` (32 random bytes
encoded as 43 url-safe base64 chars without padding). Plaintext is shown
exactly once at mint time and never logged. The DB stores SHA-256(plaintext)
in qiita.api_tokens.token_hash (BYTEA, 32 bytes, UNIQUE).

`BEARER_PREFIX` is re-exported from `qiita_common.auth_constants` so token
verification and JWT bearer parsing use the same canonical literal across the
two packages.
"""

from qiita_common.auth_constants import BEARER_PREFIX  # noqa: F401 — re-export

# Prefix is grep-friendly for leak scanners.
TOKEN_PREFIX = "qk_"

# Number of random bytes input to secrets.token_urlsafe(). 32 bytes = 256 bits
# of entropy — well past the collision floor for any reasonable token volume.
TOKEN_BODY_BYTES = 32

# Length of the base64url-without-padding encoding of TOKEN_BODY_BYTES.
# 32 bytes -> ceil(32 * 8 / 6) = 43 chars.
TOKEN_BODY_LEN = 43

# Total token length: prefix + body.
TOKEN_TOTAL_LEN = len(TOKEN_PREFIX) + TOKEN_BODY_LEN  # 46

# SHA-256 digest is always 32 bytes. The token_hash column is BYTEA NOT NULL
# UNIQUE; this constant is used by tests asserting the hash size.
TOKEN_HASH_BYTES = 32
