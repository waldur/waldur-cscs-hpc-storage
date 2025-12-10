"""Configuration parsing module for the API server."""

import logging
import os
import sys
from typing import Optional, Tuple

from pydantic import ValidationError

from waldur_cscs_hpc_storage.config import (
    HpcUserApiConfig,
    StorageProxyConfig,
    WaldurApiConfig,
)
from waldur_cscs_hpc_storage.sentry_config import initialize_sentry

logger = logging.getLogger(__name__)


def setup_logging() -> bool:
    """Set up logging configuration.

    Returns:
        True if debug mode is enabled.
    """
    debug_mode = os.getenv("DEBUG", "false").lower() in ("true", "yes", "1")
    log_level = logging.DEBUG if debug_mode else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    if debug_mode:
        logger.info("Debug mode is enabled")
        cscs_logger = logging.getLogger(__name__)
        cscs_logger.setLevel(logging.DEBUG)

    return debug_mode


def load_config() -> StorageProxyConfig:
    """Load configuration using pydantic-settings.

    Returns:
        StorageProxyConfig object.
    """
    try:
        # Pydantic settings loads env vars and yaml automatically
        config = StorageProxyConfig()
    except (ValidationError, Exception) as e:
        logger.exception("Failed to load or validate configuration: %s", e)
        sys.exit(1)

    return config


def initialize_sentry_from_config(config: StorageProxyConfig) -> None:
    """Initialize Sentry if configured.

    Args:
        config: The storage proxy configuration.
    """
    try:
        initialize_sentry(
            dsn=config.sentry.dsn,
            environment=config.sentry.environment,
            traces_sample_rate=config.sentry.traces_sample_rate,
        )
    except Exception as e:
        logger.error("Failed to initialize Sentry: %s", e)
        # Continue without Sentry - don't fail the application startup


def parse_configuration() -> Tuple[
    StorageProxyConfig, WaldurApiConfig, Optional[HpcUserApiConfig], bool
]:
    """Parse all configuration.

    Returns:
        Tuple of (config, waldur_api_config, hpc_user_api_config, disable_auth).

    """
    setup_logging()
    config = load_config()
    initialize_sentry_from_config(config)

    waldur_api_config = config.waldur_api
    hpc_user_api_config = config.hpc_user_api

    # disable_auth is now part of config.auth or env var override handled by pydantic
    disable_auth = False
    if config.auth:
        disable_auth = config.auth.disable_auth

    disable_auth_env = os.getenv("DISABLE_AUTH")
    if disable_auth_env is not None:
        disable_auth = disable_auth_env.lower() in ("true", "yes", "1")

    return config, waldur_api_config, hpc_user_api_config, disable_auth
