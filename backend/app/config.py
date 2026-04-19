from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    # Database
    database_url: str = "postgresql+asyncpg://vigil:vigil@localhost:5432/vigil"

    # JWT
    secret_key: str = "change-me-in-production-use-openssl-rand-hex-32"
    algorithm: str = "HS256"
    access_token_expire_minutes: int = 60
    refresh_token_expire_days: int = 30

    # App
    app_env: str = "development"
    cors_origins: list[str] = ["http://localhost:3000"]

    # Storage (for uploaded documents)
    storage_path: str = "/tmp/vigil-documents"

    class Config:
        env_file = ".env"


@lru_cache
def get_settings() -> Settings:
    return Settings()
