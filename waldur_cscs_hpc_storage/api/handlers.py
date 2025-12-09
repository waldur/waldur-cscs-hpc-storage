"""Exception handlers for FastAPI application."""

import logging

from fastapi import Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from waldur_cscs_hpc_storage.exceptions import (
    ConfigurationError,
    ResourceProcessingError,
    StorageProxyError,
    UpstreamServiceError,
)

logger = logging.getLogger(__name__)


def validation_exception_handler(
    _request: Request, exc: RequestValidationError
) -> JSONResponse:
    """Custom validation error handler with helpful messages for storage_system validation."""
    # Check validation errors for storage_system parameter
    for error in exc.errors():
        if error.get("loc") == ["query", "storage_system"]:
            error_type = error.get("type")
            error_input = error.get("input")

            # Handle empty string or invalid enum values
            if error_type == "enum" or (error_input == ""):
                # Special message for empty string
                if error_input == "":
                    msg = (
                        "storage_system cannot be empty. "
                        "Please specify one of the allowed storage systems or omit the parameter."
                    )
                    help_text = "Use ?storage_system=capstor (not just ?storage_system=) or omit parameter"
                else:
                    msg = (
                        f"Invalid storage_system value '{error_input}'. "
                        "Must be one of the allowed values."
                    )
                    help_text = (
                        "Use one of: ?storage_system=capstor, ?storage_system=vast, "
                        "or ?storage_system=iopsstor"
                    )

                return JSONResponse(
                    status_code=422,
                    content={
                        "detail": [
                            {
                                "type": "enum_validation",
                                "loc": ["query", "storage_system"],
                                "msg": msg,
                                "input": error_input,
                                "ctx": {
                                    "allowed_values": ["capstor", "vast", "iopsstor"],
                                    "help": help_text,
                                },
                            }
                        ]
                    },
                )

    # For other validation errors, return the default FastAPI error format
    return JSONResponse(status_code=422, content={"detail": exc.errors()})


def upstream_service_error_handler(
    _request: Request, exc: UpstreamServiceError
) -> JSONResponse:
    """Handle errors from upstream services (Waldur, HPC User API)."""
    logger.error("Upstream service error: %s", exc)
    return JSONResponse(
        status_code=502,
        content={
            "detail": str(exc),
            "error": "UpstreamServiceError",
            "message": "Failed to communicate with an upstream service.",
        },
    )


def resource_processing_error_handler(
    _request: Request, exc: ResourceProcessingError
) -> JSONResponse:
    """Handle errors during resource processing/mapping."""
    logger.error("Resource processing error: %s", exc)
    return JSONResponse(
        status_code=500,
        content={
            "detail": str(exc),
            "error": "ResourceProcessingError",
            "message": "Failed to process storage resource.",
        },
    )


def configuration_error_handler(
    _request: Request, exc: ConfigurationError
) -> JSONResponse:
    """Handle configuration errors."""
    logger.critical("Configuration error: %s", exc)
    return JSONResponse(
        status_code=500,
        content={
            "detail": "Internal server configuration error.",
            "error": "ConfigurationError",
        },
    )


def storage_proxy_error_handler(
    _request: Request, exc: StorageProxyError
) -> JSONResponse:
    """Handle generic storage proxy errors."""
    logger.error("Storage proxy error: %s", exc)
    return JSONResponse(
        status_code=500,
        content={
            "detail": str(exc),
            "error": "StorageProxyError",
        },
    )


def general_exception_handler(_request: Request, exc: Exception) -> JSONResponse:
    """Handle authentication and other general errors."""
    error_message = str(exc)
    logger.error("Unhandled exception in API: %s", exc, exc_info=True)

    # Check if it's an authentication-related error
    if "AuthClaimMissing" in error_message or "authentication" in error_message.lower():
        return JSONResponse(
            status_code=401,
            content={
                "detail": (
                    "Authentication failed. Please check your Bearer token and ensure it contains "
                    "required claims."
                ),
                "error": "AuthenticationError",
                "help": (
                    "The JWT token may be missing required claims like 'preferred_username', "
                    "'sub', or 'email'."
                ),
            },
        )

    # For other errors, return generic server error
    return JSONResponse(
        status_code=500,
        content={
            "detail": f"An error occurred: {error_message}",
            "error": "InternalServerError",
        },
    )
