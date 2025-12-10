from typing import Optional

from pydantic import Field, HttpUrl, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class AuthConfig(BaseSettings):
    """Authentication configuration."""

    disable_auth: bool = Field(default=False, alias="DISABLE_AUTH")
    keycloak_url: HttpUrl = Field(
        default="https://auth-tds.cscs.ch/auth/",
        alias="CSCS_KEYCLOAK_URL",
    )
    keycloak_realm: str = Field(default="cscs", alias="CSCS_KEYCLOAK_REALM")
    keycloak_client_id: Optional[str] = Field(
        default=None, alias="CSCS_KEYCLOAK_CLIENT_ID"
    )
    keycloak_client_secret: Optional[str] = Field(
        default=None, alias="CSCS_KEYCLOAK_CLIENT_SECRET"
    )

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        populate_by_name=True,
        extra="ignore",
    )

    @model_validator(mode="after")
    def validate_auth_requirements(self) -> "AuthConfig":
        if not self.disable_auth:
            if not self.keycloak_client_id:
                raise ValueError("keycloak_client_id is required when auth is enabled.")
            if not self.keycloak_client_secret:
                raise ValueError(
                    "keycloak_client_secret is required when auth is enabled."
                )
        return self
