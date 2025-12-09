"""Configuration parsing module for the API server."""

import logging
import os
import sys
from dataclasses import replace
from typing import Optional, Tuple

import yaml

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
    """Load configuration from YAML file.

    Returns:
        StorageProxyConfig object.

    Raises:
        SystemExit: If configuration file path is not set or loading fails.
    """
    config_file_path = os.getenv("WALDUR_CSCS_STORAGE_PROXY_CONFIG_PATH")

    if not config_file_path:
        logger.error("WALDUR_CSCS_STORAGE_PROXY_CONFIG_PATH variable is not set")
        sys.exit(1)

    try:
        config = StorageProxyConfig.from_yaml(config_file_path)
    except (FileNotFoundError, ValueError, yaml.YAMLError):
        logger.exception("Failed to load configuration")
        sys.exit(1)

    logger.info("Using configuration file: %s", config_file_path)
    logger.info("Configured storage systems: %s", config.storage_systems)

    return config


def initialize_sentry_from_config(config: StorageProxyConfig) -> None:
    """Initialize Sentry if configured.

    Args:
        config: The storage proxy configuration.
    """
    try:
        initialize_sentry(
            dsn=config.sentry_dsn,
            environment=config.sentry_environment,
            traces_sample_rate=config.sentry_traces_sample_rate,
        )
    except Exception as e:
        logger.error("Failed to initialize Sentry: %s", e)
        # Continue without Sentry - don't fail the application startup


def apply_waldur_api_overrides(config: StorageProxyConfig) -> Optional[WaldurApiConfig]:
    """Apply environment variable overrides to Waldur API configuration.

    Args:
        config: The storage proxy configuration.

    Returns:
        WaldurApiConfig if configured, None otherwise.

    Raises:
        SystemExit: If configuration validation fails.
    """
    # Fetch potential environment variable overrides
    waldur_api_token = os.getenv("WALDUR_API_TOKEN", "")
    waldur_api_url = os.getenv("WALDUR_API_URL")
    waldur_verify_ssl = os.getenv("WALDUR_VERIFY_SSL")
    waldur_socks_proxy = os.getenv("WALDUR_SOCKS_PROXY")

    # Initialize waldur_api if missing but essential env vars are present
    if not config.waldur_api and waldur_api_url and waldur_api_token:
        # Create a new WaldurApiConfig
        return WaldurApiConfig(api_url=waldur_api_url, access_token=waldur_api_token)

    if config.waldur_api:
        # Apply env var overrides if present
        api_url = waldur_api_url or config.waldur_api.api_url
        access_token = waldur_api_token or config.waldur_api.access_token

        verify_ssl = config.waldur_api.verify_ssl
        if waldur_verify_ssl is not None:
            verify_ssl = waldur_verify_ssl.lower() in ("true", "yes", "1")

        socks_proxy = waldur_socks_proxy or config.waldur_api.socks_proxy

        # Log proxy configuration
        if socks_proxy:
            logger.info(
                "Using SOCKS proxy for Waldur API connections: %s",
                socks_proxy,
            )
        else:
            logger.info("No SOCKS proxy configured for Waldur API connections")

        # Create a new WaldurApiConfig with overrides
        new_waldur_api = WaldurApiConfig(
            api_url=api_url,
            access_token=access_token,
            verify_ssl=verify_ssl,
            socks_proxy=socks_proxy,
        )

        return new_waldur_api
    else:
        logger.warning("Waldur API configuration is missing")
        return None


def build_hpc_user_api_config(
    config: StorageProxyConfig,
) -> Optional[HpcUserApiConfig]:
    """Build HPC User API configuration with environment variable support.

    Args:
        config: The storage proxy configuration.

    Returns:
        HpcUserApiConfig if any values are present, None otherwise.
    """
    hpc_user_api_url = os.getenv("HPC_USER_API_URL")
    if hpc_user_api_url is None and config.hpc_user_api:
        hpc_user_api_url = config.hpc_user_api.api_url

    hpc_user_client_id = os.getenv("HPC_USER_CLIENT_ID")
    if hpc_user_client_id is None and config.hpc_user_api:
        hpc_user_client_id = config.hpc_user_api.client_id

    hpc_user_client_secret = os.getenv("HPC_USER_CLIENT_SECRET")
    if hpc_user_client_secret is None and config.hpc_user_api:
        hpc_user_client_secret = config.hpc_user_api.client_secret

    hpc_user_oidc_token_url = os.getenv("HPC_USER_OIDC_TOKEN_URL")
    if hpc_user_oidc_token_url is None and config.hpc_user_api:
        hpc_user_oidc_token_url = config.hpc_user_api.oidc_token_url

    hpc_user_oidc_scope = os.getenv("HPC_USER_OIDC_SCOPE")
    if hpc_user_oidc_scope is None and config.hpc_user_api:
        hpc_user_oidc_scope = config.hpc_user_api.oidc_scope

    hpc_user_socks_proxy = os.getenv("HPC_USER_SOCKS_PROXY")
    if hpc_user_socks_proxy is None and config.hpc_user_api:
        hpc_user_socks_proxy = config.hpc_user_api.socks_proxy

    # Convert HPC User API settings to config object if any values are present
    if any(
        [
            hpc_user_api_url,
            hpc_user_client_id,
            hpc_user_client_secret,
            hpc_user_oidc_token_url,
            hpc_user_oidc_scope,
            hpc_user_socks_proxy,
        ]
    ):
        return HpcUserApiConfig(
            api_url=hpc_user_api_url,
            client_id=hpc_user_client_id,
            client_secret=hpc_user_client_secret,
            oidc_token_url=hpc_user_oidc_token_url,
            oidc_scope=hpc_user_oidc_scope,
            socks_proxy=hpc_user_socks_proxy,
        )

    return None


def get_disable_auth_flag(config: StorageProxyConfig) -> bool:
    """Get the disable auth flag with environment variable override.

    Args:
        config: The storage proxy configuration.

    Returns:
        True if authentication should be disabled.
    """
    disable_auth_env = os.getenv("DISABLE_AUTH")
    if disable_auth_env is not None:
        return disable_auth_env.lower() in ("true", "yes", "1")
    elif config.auth:
        return config.auth.disable_auth
    else:
        return False


def parse_configuration() -> Tuple[
    StorageProxyConfig, WaldurApiConfig, Optional[HpcUserApiConfig], bool
]:
    """Parse all configuration.

    Returns:
        Tuple of (config, waldur_api_config, hpc_user_api_config, disable_auth).

    Raises:
        SystemExit: If configuration is invalid.
    """
    setup_logging()
    config = load_config()
    initialize_sentry_from_config(config)

    waldur_api_config = apply_waldur_api_overrides(config)
    if waldur_api_config is None:
        logger.error("Waldur API configuration validation failed")
        sys.exit(1)

    # Update main config with the possibly modified waldur_api_config
    config = replace(config, waldur_api=waldur_api_config)

    # Validate configuration after overrides
    try:
        config.validate()
    except ValueError:
        logger.exception("Configuration validation failed")
        sys.exit(1)

    hpc_user_api_config = build_hpc_user_api_config(config)
    disable_auth = get_disable_auth_flag(config)

    return config, waldur_api_config, hpc_user_api_config, disable_auth
