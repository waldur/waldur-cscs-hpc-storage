"""Configuration parsing module for the API server."""

import logging
import sys
from pydantic import ValidationError

from waldur_cscs_hpc_storage.config import StorageProxyConfig
from waldur_cscs_hpc_storage.sentry_config import initialize_sentry

logger = logging.getLogger(__name__)


def load_config() -> StorageProxyConfig:
    """Load and validate configuration.

    - Loads configuration from env vars and YAML (via pydantic-settings).
    - Sets up logging based on debug mode.
    - Initializes Sentry if configured.
    """

    try:
        config = StorageProxyConfig()
    except (ValidationError, Exception) as e:
        logger.exception("Failed to load or validate configuration: %s", e)
        sys.exit(1)

    log_level = logging.DEBUG if config.debug else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    if config.debug:
        logger.info("Debug mode is enabled")

    if config.sentry:
        initialize_sentry(config.sentry)

    return config
