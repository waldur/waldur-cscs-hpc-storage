"""Configuration parsing module for the API server."""

import logging
import os
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

    try:
        config = StorageProxyConfig()
    except (ValidationError, Exception) as e:
        logger.exception("Failed to load or validate configuration: %s", e)
        sys.exit(1)

    try:
        initialize_sentry(
            dsn=config.sentry.dsn,
            environment=config.sentry.environment,
            traces_sample_rate=config.sentry.traces_sample_rate,
        )
    except Exception as e:
        logger.error("Failed to initialize Sentry: %s", e)

    return config
