from typing import Optional

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from waldur_cscs_hpc_storage.models.enums import StorageSystem
from waldur_cscs_hpc_storage.config.auth import AuthConfig
from waldur_cscs_hpc_storage.config.backend import BackendConfig
from waldur_cscs_hpc_storage.config.hpc_user import HpcUserApiConfig
from waldur_cscs_hpc_storage.config.sentry import SentryConfig
from waldur_cscs_hpc_storage.config.waldur import WaldurApiConfig


class StorageProxyConfig(BaseSettings):
    """Configuration for the CSCS Storage Proxy.

    Loads configuration from:
    1. Environment variables (specific aliases only)
    2. Defaults
    """

    debug: bool = Field(default=False, alias="DEBUG")
    waldur_api: WaldurApiConfig = Field(default_factory=WaldurApiConfig)
    backend_settings: BackendConfig = Field(default_factory=BackendConfig)
    storage_systems: dict[StorageSystem, str]
    auth: Optional[AuthConfig] = Field(default_factory=AuthConfig)
    hpc_user_api: Optional[HpcUserApiConfig] = Field(default_factory=HpcUserApiConfig)
    sentry: Optional[SentryConfig] = Field(default_factory=SentryConfig)

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        # We handle env vars manually via aliases on fields or nested models,
        # so we disable generic env prefixing to avoid pollution/confusion.
        env_nested_delimiter=None,
        case_sensitive=False,
        extra="ignore",
    )

    @field_validator("storage_systems")
    @classmethod
    def check_storage_systems(
        cls, v: dict[StorageSystem, str]
    ) -> dict[StorageSystem, str]:
        if not v:
            raise ValueError("At least one storage_system mapping is required")
        return v
