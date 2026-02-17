import logging
from typing import Annotated, Optional, Union

from fastapi import Depends

from waldur_cscs_hpc_storage.config import StorageProxyConfig
from waldur_cscs_hpc_storage.services.gid_service import GidService
from waldur_cscs_hpc_storage.mapper import ResourceMapper
from waldur_cscs_hpc_storage.services.mock_gid_service import MockGidService
from waldur_cscs_hpc_storage.services.orchestrator import StorageOrchestrator
from waldur_cscs_hpc_storage.mapper import QuotaCalculator
from waldur_cscs_hpc_storage.services.waldur_service import WaldurService

logger = logging.getLogger(__name__)

# Global config instance (loaded at startup in main.py)
_config: Optional[StorageProxyConfig] = None

# Singleton service instances (persist across requests for caching)
_waldur_service: Optional[WaldurService] = None
_gid_service: Optional[Union[GidService, MockGidService]] = None
_quota_calculator: Optional[QuotaCalculator] = None
_mapper: Optional[ResourceMapper] = None


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
    Returns a singleton WaldurService, reused across requests.
    """
    global _waldur_service
    if _waldur_service is None:
        if not config.waldur_api:
            raise ValueError("Waldur API configuration is missing")
        _waldur_service = WaldurService(config.waldur_api)
    return _waldur_service


def get_gid_service(
    config: Annotated[StorageProxyConfig, Depends(get_config)],
) -> Union[GidService, MockGidService]:
    """
    Returns a singleton GidService or MockGidService.

    The GID cache persists across requests, avoiding redundant
    HPC User API calls for already-resolved project slugs.
    """
    global _gid_service
    if _gid_service is not None:
        return _gid_service

    # 1. Try to initialize the real HPC User API client
    if config.hpc_user_api and config.hpc_user_api.api_url:
        try:
            # development_mode is already synced in config_parser.py
            _gid_service = GidService(config.hpc_user_api)
            logger.info("Initialized real HPC User API client")
            return _gid_service
        except Exception as e:
            logger.warning("Failed to initialize real GidService: %s", e)
            if not config.hpc_user_api.development_mode:
                raise

    # 2. Fallback to Mock service
    logger.info("Using MockGidService (HPC User API not configured or failed)")
    # Default to False if hpc_user_api is not configured
    dev_mode = config.hpc_user_api.development_mode if config.hpc_user_api else False
    _gid_service = MockGidService(dev_mode)
    return _gid_service


def get_quota_calculator(
    config: Annotated[StorageProxyConfig, Depends(get_config)],
) -> QuotaCalculator:
    """
    Returns a singleton QuotaCalculator.
    """
    global _quota_calculator
    if _quota_calculator is None:
        _quota_calculator = QuotaCalculator(config.backend_settings)
    return _quota_calculator


def get_mapper(
    config: Annotated[StorageProxyConfig, Depends(get_config)],
    gid_service: Annotated[Union[GidService, MockGidService], Depends(get_gid_service)],
    quota_calculator: Annotated[QuotaCalculator, Depends(get_quota_calculator)],
) -> ResourceMapper:
    """
    Returns a singleton ResourceMapper.
    """
    global _mapper
    if _mapper is None:
        _mapper = ResourceMapper(config.backend_settings, gid_service, quota_calculator)
    return _mapper


def get_orchestrator(
    config: Annotated[StorageProxyConfig, Depends(get_config)],
    waldur_service: Annotated[WaldurService, Depends(get_waldur_service)],
    mapper: Annotated[ResourceMapper, Depends(get_mapper)],
) -> StorageOrchestrator:
    """
    Factory function for StorageOrchestrator.

    Creates a new instance per request. This is intentional — the orchestrator
    holds per-request state (hierarchy builder). The expensive services it
    depends on (WaldurService, ResourceMapper, GidService) are singletons.
    """
    return StorageOrchestrator(
        config=config, waldur_service=waldur_service, mapper=mapper
    )
