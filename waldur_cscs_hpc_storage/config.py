"""Configuration loader for CSCS Storage Proxy."""

import logging
import os
from pathlib import Path
from typing import Any, Optional, Tuple, Type

from pydantic import (
    Field,
    field_validator,
    model_validator,
)
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    YamlConfigSettingsSource as PydanticYamlConfigSettingsSource,
)

logger = logging.getLogger(__name__)


class AuthConfig(BaseSettings):
    """Authentication configuration."""

    disable_auth: bool = Field(default=False, alias="DISABLE_AUTH")
    keycloak_url: str = Field(
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

    model_config = SettingsConfigDict(populate_by_name=True, extra="ignore")


class HpcUserApiConfig(BaseSettings):
    """HPC User API configuration."""

    api_url: Optional[str] = Field(None, alias="HPC_USER_API_URL")
    client_id: Optional[str] = Field(None, alias="HPC_USER_CLIENT_ID")
    client_secret: Optional[str] = Field(None, alias="HPC_USER_CLIENT_SECRET")
    oidc_token_url: Optional[str] = Field(None, alias="HPC_USER_OIDC_TOKEN_URL")
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

    model_config = SettingsConfigDict(populate_by_name=True, extra="ignore")


class WaldurApiConfig(BaseSettings):
    """Waldur API configuration."""

    api_url: str = Field(..., alias="WALDUR_API_URL")
    access_token: str = Field(..., alias="WALDUR_API_TOKEN")
    verify_ssl: bool = Field(True, alias="WALDUR_VERIFY_SSL")
    socks_proxy: Optional[str] = Field(None, alias="WALDUR_SOCKS_PROXY")
    agent_header: Optional[str] = None

    model_config = SettingsConfigDict(populate_by_name=True, extra="ignore")


class BackendConfig(BaseSettings):
    """Backend configuration settings."""

    storage_file_system: str = Field(default="lustre", min_length=1)
    inode_soft_coefficient: float = Field(default=1.33, gt=0)
    inode_hard_coefficient: float = Field(default=2.0, gt=0)
    inode_base_multiplier: float = Field(default=1_000_000, gt=0)
    use_mock_target_items: bool = False

    @model_validator(mode="after")
    def check_coefficients(self) -> "BackendConfig":
        """Validate logical relationship between coefficients."""
        if self.inode_hard_coefficient < self.inode_soft_coefficient:
            msg = (
                f"inode_hard_coefficient {self.inode_hard_coefficient} must be greater than "
                f"inode_soft_coefficient {self.inode_soft_coefficient}"
            )
            # Pydantic will wrap this ValueError into a ValidationError
            raise ValueError(msg)
        return self

    model_config = SettingsConfigDict(populate_by_name=True, extra="ignore")


class SentryConfig(BaseSettings):
    """Sentry configuration."""

    dsn: Optional[str] = Field(None, alias="SENTRY_DSN")
    environment: Optional[str] = Field(None, alias="SENTRY_ENVIRONMENT")
    traces_sample_rate: Optional[float] = Field(None, alias="SENTRY_TRACES_SAMPLE_RATE")

    model_config = SettingsConfigDict(populate_by_name=True, extra="ignore")


class StorageProxyConfig(BaseSettings):
    """Configuration for the CSCS Storage Proxy.

    Loads configuration from:
    1. Environment variables (specific aliases only)
    2. YAML file (specified by WALDUR_CSCS_STORAGE_PROXY_CONFIG_PATH)
    3. Defaults
    """

    debug: bool = Field(default=False, alias="DEBUG")
    waldur_api: WaldurApiConfig = Field(default_factory=WaldurApiConfig)
    backend_settings: BackendConfig = Field(default_factory=BackendConfig)
    storage_systems: dict[str, str]
    auth: Optional[AuthConfig] = Field(default_factory=AuthConfig)
    hpc_user_api: Optional[HpcUserApiConfig] = Field(default_factory=HpcUserApiConfig)
    sentry: Optional[SentryConfig] = Field(default_factory=SentryConfig)

    model_config = SettingsConfigDict(
        # We handle env vars manually via aliases on fields or nested models,
        # so we disable generic env prefixing to avoid pollution/confusion.
        env_nested_delimiter=None,
        case_sensitive=False,
        extra="ignore",
    )

    @field_validator("storage_systems")
    @classmethod
    def check_storage_systems(cls, v: dict[str, str]) -> dict[str, str]:
        if not v:
            raise ValueError("At least one storage_system mapping is required")
        return v

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: Type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> Tuple[PydanticBaseSettingsSource, ...]:
        """Define configuration source priority."""
        return (
            init_settings,
            env_settings,
            YamlConfigSettingsSource(settings_cls),
            file_secret_settings,
        )


class YamlConfigSettingsSource(PydanticYamlConfigSettingsSource):
    """Custom YAML settings source."""

    def __init__(self, settings_cls: Type[BaseSettings]):
        config_path = os.getenv("WALDUR_CSCS_STORAGE_PROXY_CONFIG_PATH")
        yaml_file = Path(config_path) if config_path else None
        super().__init__(settings_cls, yaml_file=yaml_file)

    def __call__(self) -> dict[str, Any]:
        """Load and transform YAML data to match model structure."""
        d = super().__call__()

        # Transform flat Waldur API config
        if "waldur_api" not in d and "waldur_api_url" in d and "waldur_api_token" in d:
            d["waldur_api"] = {
                "api_url": d.get("waldur_api_url"),
                "access_token": d.get("waldur_api_token"),
                "verify_ssl": d.get("waldur_verify_ssl", True),
                "socks_proxy": d.get("waldur_socks_proxy"),
            }

        return d
