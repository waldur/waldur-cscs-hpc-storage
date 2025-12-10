"""Configuration parsing module for the API server."""

import logging
import pprint
import sys
from typing import Any
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
    except ValidationError as e:
        print(_format_validation_error(e), file=sys.stderr)
        sys.exit(1)
    except Exception as e:
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

    if config.sentry and config.sentry.dsn:
        initialize_sentry(config.sentry)

    # Log merged configuration
    safe_config = mask_sensitive_data(config.model_dump())
    logger.info("Merged configuration:\n%s", pprint.pformat(safe_config))

    return config


def mask_sensitive_data(data: Any) -> Any:
    """Recursively mask sensitive data in a dictionary."""
    if isinstance(data, dict):
        return {
            k: mask_sensitive_data(v)
            if k not in {"access_token", "client_secret", "keycloak_client_secret"}
            else "********"
            for k, v in data.items()
        }
    if isinstance(data, list):
        return [mask_sensitive_data(item) for item in data]
    return data


def _format_validation_error(e: ValidationError) -> str:
    """Format a Pydantic ValidationError into a human-readable string.

    Args:
        e: The ValidationError to format.

    Returns:
        A formatted string summary of the errors.
    """
    error_messages = []
    for error in e.errors():
        loc = ".".join(str(i) for i in error["loc"])
        msg = error["msg"]
        error_messages.append(f"  - {loc}: {msg}")

    return "Configuration Error:\n" + "\n".join(error_messages)
