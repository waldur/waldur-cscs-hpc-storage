"""Sentry configuration and initialization module."""

import logging
from typing import Optional

from waldur_cscs_hpc_storage.config.sentry import SentryConfig

logger = logging.getLogger(__name__)


def initialize_sentry(
    sentry_config: SentryConfig,
    release: Optional[str] = None,
):
    """Initialize Sentry SDK with optional configuration."""
    if not sentry_config.dsn:
        logger.info("Sentry DSN not provided - error tracking disabled")
        return False

    try:
        import sentry_sdk
        from sentry_sdk.integrations.fastapi import FastApiIntegration
        from sentry_sdk.integrations.logging import LoggingIntegration

        # Initialize Sentry with integrations
        sentry_sdk.init(
            dsn=sentry_config.dsn,
            environment=sentry_config.environment,
            traces_sample_rate=sentry_config.traces_sample_rate,
            release=release,
            integrations=[
                FastApiIntegration(
                    transaction_style="url",  # Use URL path as transaction name
                ),
                LoggingIntegration(
                    level=logging.INFO,  # Capture info and above as breadcrumbs
                    event_level=logging.ERROR,  # Send errors and above as events
                ),
            ],
            # Send default PII (Personally Identifiable Information) - can be disabled if needed
            send_default_pii=True,
        )

        logger.info(
            "Sentry initialized successfully (environment: %s, traces_sample_rate: %.2f)",
            sentry_config.environment,
            sentry_config.traces_sample_rate,
        )

    except ImportError:
        logger.error(
            "Sentry SDK not installed. Install with: pip install sentry-sdk[fastapi]"
        )
    except Exception as e:
        logger.error("Failed to initialize Sentry: %s", e, exc_info=True)
