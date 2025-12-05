"""Sentry configuration and initialization module."""

import logging
import os
from typing import Optional

logger = logging.getLogger(__name__)


def initialize_sentry(
    dsn: Optional[str] = None,
    environment: Optional[str] = None,
    traces_sample_rate: Optional[float] = None,
    release: Optional[str] = None,
) -> bool:
    """Initialize Sentry SDK with optional configuration.

    Args:
        dsn: Sentry DSN (Data Source Name). If None, checks SENTRY_DSN env var.
        environment: Environment name (e.g., "production", "staging"). If None, checks SENTRY_ENVIRONMENT env var.
        traces_sample_rate: Sample rate for performance monitoring (0.0 to 1.0). If None, checks SENTRY_TRACES_SAMPLE_RATE env var.
        release: Release version string. If None, checks SENTRY_RELEASE env var.

    Returns:
        True if Sentry was initialized, False if skipped (no DSN provided)

    Raises:
        Exception: If Sentry initialization fails
    """
    # Get DSN from parameter or environment variable
    sentry_dsn = dsn or os.getenv("SENTRY_DSN")

    if not sentry_dsn:
        logger.info("Sentry DSN not provided - error tracking disabled")
        return False

    try:
        import sentry_sdk
        from sentry_sdk.integrations.fastapi import FastApiIntegration
        from sentry_sdk.integrations.logging import LoggingIntegration

        # Get environment from parameter or environment variable
        sentry_environment = environment or os.getenv(
            "SENTRY_ENVIRONMENT", "production"
        )

        # Get traces sample rate from parameter or environment variable
        if traces_sample_rate is None:
            traces_sample_rate_str = os.getenv("SENTRY_TRACES_SAMPLE_RATE")
            if traces_sample_rate_str:
                try:
                    traces_sample_rate = float(traces_sample_rate_str)
                except ValueError:
                    logger.warning(
                        "Invalid SENTRY_TRACES_SAMPLE_RATE value: %s, using 0.1",
                        traces_sample_rate_str,
                    )
                    traces_sample_rate = 0.1
            else:
                traces_sample_rate = 0.1  # Default 10% sampling

        # Get release version
        sentry_release = release or os.getenv("SENTRY_RELEASE")

        # Initialize Sentry with integrations
        sentry_sdk.init(
            dsn=sentry_dsn,
            environment=sentry_environment,
            traces_sample_rate=traces_sample_rate,
            release=sentry_release,
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
            sentry_environment,
            traces_sample_rate,
        )
        if sentry_release:
            logger.info("Sentry release: %s", sentry_release)

        return True

    except ImportError:
        logger.error(
            "Sentry SDK not installed. Install with: pip install sentry-sdk[fastapi]"
        )
        raise
    except Exception as e:
        logger.error("Failed to initialize Sentry: %s", e, exc_info=True)
        raise


def set_user_context(
    user_id: str, username: Optional[str] = None, email: Optional[str] = None
) -> None:
    """Set user context for Sentry events.

    Args:
        user_id: Unique user identifier
        username: Optional username
        email: Optional email address
    """
    try:
        import sentry_sdk

        sentry_sdk.set_user(
            {
                "id": user_id,
                "username": username,
                "email": email,
            }
        )
    except ImportError:
        pass  # Sentry not installed or not initialized


def set_context(key: str, value: dict) -> None:
    """Set custom context for Sentry events.

    Args:
        key: Context key
        value: Context dictionary
    """
    try:
        import sentry_sdk

        sentry_sdk.set_context(key, value)
    except ImportError:
        pass  # Sentry not installed or not initialized


def add_breadcrumb(
    message: str,
    category: str = "default",
    level: str = "info",
    data: Optional[dict] = None,
) -> None:
    """Add a breadcrumb to Sentry events.

    Args:
        message: Breadcrumb message
        category: Breadcrumb category
        level: Breadcrumb level (debug, info, warning, error, fatal)
        data: Optional additional data
    """
    try:
        import sentry_sdk

        sentry_sdk.add_breadcrumb(
            message=message,
            category=category,
            level=level,
            data=data or {},
        )
    except ImportError:
        pass  # Sentry not installed or not initialized
