"""Control plane configuration — reads from environment variables."""

from dataclasses import dataclass

from qiita_common.config import require_env


@dataclass(frozen=True, slots=True)
class Settings:
    database_url: str
    hmac_secret_key: str

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            database_url=require_env("DATABASE_URL"),
            hmac_secret_key=require_env("HMAC_SECRET_KEY"),
        )
