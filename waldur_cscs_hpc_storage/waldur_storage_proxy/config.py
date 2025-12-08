"""Configuration loader for CSCS Storage Proxy."""

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Union

import yaml

logger = logging.getLogger(__name__)


@dataclass
class AuthConfig:
    """Authentication configuration."""

    disable_auth: bool = False
    keycloak_url: str = "https://auth-tds.cscs.ch/auth/"
    keycloak_realm: str = "cscs"
    keycloak_client_id: Optional[str] = None
    keycloak_client_secret: Optional[str] = None


@dataclass
class HpcUserApiConfig:
    """HPC User API configuration."""

    api_url: Optional[str] = None
    client_id: Optional[str] = None
    client_secret: Optional[str] = None
    oidc_token_url: Optional[str] = None
    oidc_scope: Optional[str] = None
    socks_proxy: Optional[str] = (
        None  # SOCKS proxy URL (e.g., "socks5://localhost:12345")
    )


@dataclass
class WaldurApiConfig:
    """Waldur API configuration."""

    api_url: str
    access_token: str
    verify_ssl: bool = True
    socks_proxy: Optional[str] = None
    agent_header: Optional[str] = None


@dataclass
class BackendConfig:
    """Backend configuration settings."""

    storage_file_system: str = "lustre"
    inode_soft_coefficient: float = 1.33
    inode_hard_coefficient: float = 2.0
    inode_base_multiplier: float = 1_000_000
    use_mock_target_items: bool = False
    development_mode: bool = False

    def validate(self) -> None:
        """Validate backend configuration."""
        if (
            not isinstance(self.inode_soft_coefficient, (int, float))
            or self.inode_soft_coefficient <= 0
        ):
            msg = "inode_soft_coefficient must be a positive number"
            raise ValueError(msg)

        if (
            not isinstance(self.inode_hard_coefficient, (int, float))
            or self.inode_hard_coefficient <= 0
        ):
            msg = "inode_hard_coefficient must be a positive number"
            raise ValueError(msg)

        if self.inode_hard_coefficient < self.inode_soft_coefficient:
            msg = (
                f"inode_hard_coefficient {self.inode_hard_coefficient} must be greater than "
                f"inode_soft_coefficient {self.inode_soft_coefficient}"
            )
            raise ValueError(msg)

        if (
            not isinstance(self.storage_file_system, str)
            or not self.storage_file_system.strip()
        ):
            msg = "storage_file_system must be a non-empty string"
            raise ValueError(msg)

        if (
            not isinstance(self.inode_base_multiplier, (int, float))
            or self.inode_base_multiplier <= 0
        ):
            msg = "inode_base_multiplier must be a positive number"
            raise ValueError(msg)


@dataclass
class StorageProxyConfig:
    """Configuration for the CSCS Storage Proxy."""

    waldur_api: Optional[WaldurApiConfig]
    backend_config: BackendConfig
    backend_components: list[str]
    storage_systems: dict[str, str]
    auth: Optional[AuthConfig] = None
    hpc_user_api: Optional[HpcUserApiConfig] = None
    # Sentry settings
    sentry_dsn: Optional[str] = None
    sentry_environment: Optional[str] = None
    sentry_traces_sample_rate: Optional[float] = None

    @classmethod
    def from_yaml(cls, config_path: Union[str, Path]) -> "StorageProxyConfig":
        """Load configuration from YAML file."""
        config_path = Path(config_path)
        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")

        with config_path.open() as f:
            data = yaml.safe_load(f)

        # Parse waldur api config if present
        waldur_api_config = None
        if "waldur_api_url" in data and "waldur_api_token" in data:
            waldur_api_config = WaldurApiConfig(
                api_url=data["waldur_api_url"],
                access_token=data["waldur_api_token"],
                verify_ssl=data.get("waldur_verify_ssl", True),
                socks_proxy=data.get("waldur_socks_proxy"),
            )

        # Parse auth config if present
        auth_config = None
        if "auth" in data:
            auth_data = data["auth"]
            auth_config = AuthConfig(
                disable_auth=auth_data.get("disable_auth", False),
                keycloak_url=auth_data.get(
                    "keycloak_url", "https://auth-tds.cscs.ch/auth/"
                ),
                keycloak_realm=auth_data.get("keycloak_realm", "cscs"),
                keycloak_client_id=auth_data.get("keycloak_client_id"),
                keycloak_client_secret=auth_data.get("keycloak_client_secret"),
            )

        # Parse HPC User API config if present
        hpc_user_api_config = None
        if "hpc_user_api" in data:
            hpc_api_data = data["hpc_user_api"]
            hpc_user_api_config = HpcUserApiConfig(
                api_url=hpc_api_data.get("api_url"),
                client_id=hpc_api_data.get("client_id"),
                client_secret=hpc_api_data.get("client_secret"),
                oidc_token_url=hpc_api_data.get("oidc_token_url"),
                oidc_scope=hpc_api_data.get("oidc_scope"),
                socks_proxy=hpc_api_data.get("socks_proxy"),
            )

        # Parse backend settings
        backend_settings_data = data.get("backend_settings", {})
        backend_config = BackendConfig(
            storage_file_system=backend_settings_data.get(
                "storage_file_system", "lustre"
            ),
            inode_soft_coefficient=backend_settings_data.get(
                "inode_soft_coefficient", 1.33
            ),
            inode_hard_coefficient=backend_settings_data.get(
                "inode_hard_coefficient", 2.0
            ),
            inode_base_multiplier=backend_settings_data.get(
                "inode_base_multiplier", 1_000_000
            ),
            use_mock_target_items=backend_settings_data.get(
                "use_mock_target_items", False
            ),
            development_mode=backend_settings_data.get("development_mode", False),
        )

        return cls(
            waldur_api=waldur_api_config,
            backend_config=backend_config,
            backend_components=data.get("backend_components", []),
            storage_systems=data.get("storage_systems", {}),
            auth=auth_config,
            hpc_user_api=hpc_user_api_config,
            sentry_dsn=data.get("sentry_dsn"),
            sentry_environment=data.get("sentry_environment"),
            sentry_traces_sample_rate=data.get("sentry_traces_sample_rate"),
        )

    def validate(self) -> None:
        """Validate the configuration."""
        if not self.waldur_api:
            msg = "waldur_api configuration is required (waldur_api_url and waldur_api_token)"
            raise ValueError(msg)
        if not self.storage_systems:
            msg = "At least one storage_system mapping is required"
            raise ValueError(msg)
        if not self.backend_components:
            msg = "backend_components is required"
            raise ValueError(msg)

        # Validate that storage component exists
        if "storage" not in self.backend_components:
            msg = "'storage' component is required in backend_components"
            raise ValueError(msg)

        logger.info("Configuration validated successfully")
        logger.info("  Waldur API URL: %s", self.waldur_api.api_url)
        logger.info("  Storage systems: %s", self.storage_systems)
        logger.info("  Backend components: %s", self.backend_components)
