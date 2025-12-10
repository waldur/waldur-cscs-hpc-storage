from typing import Optional

from pydantic import Field, HttpUrl, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class HpcUserApiConfig(BaseSettings):
    """HPC User API configuration."""

    api_url: Optional[HttpUrl] = Field(None, alias="HPC_USER_API_URL")
    client_id: Optional[str] = Field(None, alias="HPC_USER_CLIENT_ID")
    client_secret: Optional[str] = Field(None, alias="HPC_USER_CLIENT_SECRET")
    oidc_token_url: Optional[HttpUrl] = Field(None, alias="HPC_USER_OIDC_TOKEN_URL")
    oidc_scope: Optional[str] = Field(None, alias="HPC_USER_OIDC_SCOPE")
    socks_proxy: Optional[str] = Field(
        None,
        description='SOCKS proxy URL (e.g., "socks5://localhost:12345")',
        alias="HPC_USER_SOCKS_PROXY",
    )
    development_mode: bool = Field(
        default=False,
        description="Enable mock fallback when API lookup fails",
        alias="HPC_USER_DEVELOPMENT_MODE",
    )

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

    @model_validator(mode="after")
    def validate_prod_requirements(self) -> "HpcUserApiConfig":
        if not self.development_mode:
            missing = []
            if not self.api_url:
                missing.append("api_url")
            if not self.client_id:
                missing.append("client_id")
            if not self.client_secret:
                missing.append("client_secret")

            if missing:
                raise ValueError(
                    f"Connection credentials ({', '.join(missing)}) are required "
                    "when development_mode is disabled."
                )
        return self
