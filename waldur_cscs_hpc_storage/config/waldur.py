from typing import Optional

from pydantic import Field, HttpUrl, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class WaldurApiConfig(BaseSettings):
    """Waldur API configuration."""

    api_url: HttpUrl = Field(..., alias="WALDUR_API_URL")
    access_token: str = Field(
        ...,
        alias="WALDUR_API_TOKEN",
        min_length=40,
        max_length=40,
        pattern=r"^[0-9a-fA-F]{40}$",
    )
    verify_ssl: bool = Field(True, alias="WALDUR_VERIFY_SSL")
    socks_proxy: Optional[str] = Field(None, alias="WALDUR_SOCKS_PROXY")
    agent_header: Optional[str] = None

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        populate_by_name=True,
        extra="ignore",
    )

    @field_validator("socks_proxy")
    @classmethod
    def validate_proxy_url(cls, v: Optional[str]) -> Optional[str]:
        if v:
            allowed_schemes = ("socks5://", "socks5h://", "http://", "https://")
            if not v.lower().startswith(allowed_schemes):
                raise ValueError(
                    f"Proxy URL must start with one of: {', '.join(allowed_schemes)}"
                )
        return v
