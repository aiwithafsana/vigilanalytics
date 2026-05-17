from pydantic import model_validator
from pydantic_settings import BaseSettings
from functools import lru_cache

_DEFAULT_SECRET = "change-me-in-production-use-openssl-rand-hex-32"


class Settings(BaseSettings):
    # Database
    database_url: str = "postgresql+asyncpg://vigil:vigil@localhost:5432/vigil"

    # JWT
    secret_key: str = _DEFAULT_SECRET
    algorithm: str = "HS256"
    access_token_expire_minutes: int = 60
    refresh_token_expire_days: int = 30

    # App
    app_env: str = "development"
    cors_origins: list[str] = ["http://localhost:3000"]

    # Storage (for uploaded documents)
    storage_path: str = "/tmp/vigil-documents"

    @model_validator(mode="after")
    def _check_production_secrets(self) -> "Settings":
        if self.app_env == "production":
            if self.secret_key == _DEFAULT_SECRET or len(self.secret_key) < 32:
                raise ValueError(
                    "SECRET_KEY must be set to a strong random value in production. "
                    "Generate one with: openssl rand -hex 32"
                )
            if "vigil:vigil@localhost" in self.database_url:
                raise ValueError(
                    "DATABASE_URL still uses default local credentials in production. "
                    "Set a proper DATABASE_URL in your .env file."
                )
            if self.cors_origins == ["http://localhost:3000"]:
                raise ValueError(
                    "CORS_ORIGINS still points to localhost in production. "
                    "Set CORS_ORIGINS to your actual domain(s)."
                )
        return self

    class Config:
        env_file = ".env"
        # Allow undeclared env vars (SAM_GOV_API_KEY, COURTLISTENER_API_KEY, etc.)
        # to live in .env without each one needing a Settings field.  Feature
        # code reads them via os.getenv() directly.
        extra = "ignore"


@lru_cache
def get_settings() -> Settings:
    return Settings()
