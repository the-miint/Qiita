"""Control plane configuration — reads from environment variables."""

import base64
import os
from dataclasses import dataclass

from qiita_common.config import require_env


@dataclass(frozen=True, slots=True)
class Settings:
    database_url: str
    hmac_secret_key: bytes
    data_plane_url: str

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
            # Default enables local dev without extra config. Production
            # deployments must set DATA_PLANE_URL explicitly.
            data_plane_url=os.environ.get("DATA_PLANE_URL", "grpc://localhost:50051"),
        )
