"""Control plane configuration — reads from environment variables."""

import base64
from dataclasses import dataclass

from qiita_common.config import require_env


@dataclass(frozen=True, slots=True)
class Settings:
    database_url: str
    hmac_secret_key: bytes

    @classmethod
    def from_env(cls) -> Settings:
        raw = require_env("HMAC_SECRET_KEY")
        try:
            secret = base64.b64decode(raw)
        except Exception as exc:
            raise RuntimeError("HMAC_SECRET_KEY must be valid base64") from exc
        if len(secret) < 16:
            raise RuntimeError("HMAC_SECRET_KEY must decode to at least 16 bytes")
        return cls(
            database_url=require_env("DATABASE_URL"),
            hmac_secret_key=secret,
        )
