from typing import Optional

from pydantic import Field, HttpUrl
from pydantic_settings import BaseSettings, SettingsConfigDict


class SentryConfig(BaseSettings):
    """Sentry configuration."""

    dsn: Optional[HttpUrl] = Field(None, alias="SENTRY_DSN")
    environment: Optional[str] = Field(None, alias="SENTRY_ENVIRONMENT")
    traces_sample_rate: Optional[float] = Field(
        None, alias="SENTRY_TRACES_SAMPLE_RATE", ge=0.0, le=1.0
    )

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        populate_by_name=True,
        extra="ignore",
    )
