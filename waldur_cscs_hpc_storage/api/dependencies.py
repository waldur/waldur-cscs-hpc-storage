import logging
from typing import Annotated, Optional, Union

from fastapi import Depends

from waldur_cscs_hpc_storage.config import StorageProxyConfig
from waldur_cscs_hpc_storage.services.gid_service import GidService
from waldur_cscs_hpc_storage.services.mapper import ResourceMapper
from waldur_cscs_hpc_storage.services.mock_gid_service import MockGidService
from waldur_cscs_hpc_storage.services.orchestrator import StorageOrchestrator
from waldur_cscs_hpc_storage.services.waldur_service import WaldurService

logger = logging.getLogger(__name__)

# Global config instance (loaded at startup in main.py)
_config: Optional[StorageProxyConfig] = None


def set_global_config(config: StorageProxyConfig):
    """Called by main.py on startup to inject the loaded configuration."""
    global _config
    _config = config


def get_config() -> StorageProxyConfig:
    """Dependency to retrieve the global configuration."""
    if _config is None:
        raise RuntimeError("Configuration not initialized")
    return _config


def get_waldur_service(
    config: Annotated[StorageProxyConfig, Depends(get_config)],
) -> WaldurService:
    """
    Creates a singleton WaldurService.
    """
    if not config.waldur_api:
        raise ValueError("Waldur API configuration is missing")
    return WaldurService(config.waldur_api)


def get_gid_service(
    config: Annotated[StorageProxyConfig, Depends(get_config)],
) -> Union[GidService, MockGidService]:  # Corrected return type hint
    """
    Creates a GidService or MockGidService based on configuration.
    """
    # 1. Try to initialize the real HPC User API client
    if config.hpc_user_api:
        try:
            # development_mode is already synced in config_parser.py
            service = GidService(config.hpc_user_api)
            logger.info("Initialized real HPC User API client")
            return service
        except Exception as e:
            logger.warning("Failed to initialize real GidService: %s", e)
            if not config.hpc_user_api.development_mode:
                raise

    # 2. Fallback to Mock service
    logger.info("Using MockGidService (HPC User API not configured or failed)")
    # Default to False if hpc_user_api is not configured
    dev_mode = config.hpc_user_api.development_mode if config.hpc_user_api else False
    return MockGidService(dev_mode)


def get_mapper(
    config: Annotated[StorageProxyConfig, Depends(get_config)],
    gid_service: Annotated[Union[GidService, MockGidService], Depends(get_gid_service)],
) -> ResourceMapper:
    """
    Creates the ResourceMapper, injecting the specific GID service strategy.
    """
    return ResourceMapper(config.backend_settings, gid_service)


def get_orchestrator(
    config: Annotated[StorageProxyConfig, Depends(get_config)],
    waldur_service: Annotated[WaldurService, Depends(get_waldur_service)],
    mapper: Annotated[ResourceMapper, Depends(get_mapper)],
) -> StorageOrchestrator:
    """
    Factory function for StorageOrchestrator.

    This is the main dependency used in route handlers.
    but creating a new instance is cheap and safe.
    """
    return StorageOrchestrator(
        config=config, waldur_service=waldur_service, mapper=mapper
    )
