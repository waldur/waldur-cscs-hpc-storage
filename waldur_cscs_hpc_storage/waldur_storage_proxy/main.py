"""API server used as proxy to Waldur storage resources."""

from enum import Enum
from typing import Annotated, Optional
import logging
import os
import sys
import yaml

from fastapi import Depends, FastAPI, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.logger import logger
from fastapi.responses import JSONResponse
from fastapi_keycloak_middleware import (
    get_user,
)
from waldur_api_client.models.user import User
from waldur_api_client.models.resource_state import ResourceState

from waldur_cscs_hpc_storage.waldur_storage_proxy.auth import mock_user
from waldur_cscs_hpc_storage.waldur_storage_proxy.auth import setup_auth
from waldur_cscs_hpc_storage.utils import get_client
from waldur_cscs_hpc_storage.backend import CscsHpcStorageBackend
from waldur_cscs_hpc_storage.sync_script import setup_logging
from waldur_cscs_hpc_storage.waldur_storage_proxy.config import StorageProxyConfig
from waldur_cscs_hpc_storage.sentry_config import initialize_sentry, set_user_context


# Check if debug mode is enabled via environment variable
DEBUG_MODE = os.getenv("DEBUG", "false").lower() in ("true", "yes", "1")

setup_logging(verbose=DEBUG_MODE)

if DEBUG_MODE:
    logger.info("Debug mode is enabled")
    # Set debug level for the cscs backend logger
    cscs_logger = logging.getLogger(__name__)
    cscs_logger.setLevel(logging.DEBUG)

config_file_path = os.getenv("WALDUR_CSCS_STORAGE_PROXY_CONFIG_PATH")

if not config_file_path:
    logger.error("WALDUR_CSCS_STORAGE_PROXY_CONFIG_PATH variable is not set")
    sys.exit(1)

# Load simplified proxy configuration
try:
    config = StorageProxyConfig.from_yaml(config_file_path)
    config.validate()
except (FileNotFoundError, ValueError, yaml.YAMLError):
    logger.exception("Failed to load configuration")
    sys.exit(1)

logger.info("Using configuration file: %s", config_file_path)
logger.info("Configured storage systems: %s", config.storage_systems)

# Initialize Sentry if configured
try:
    initialize_sentry(
        dsn=config.sentry_dsn,
        environment=config.sentry_environment,
        traces_sample_rate=config.sentry_traces_sample_rate,
    )
except Exception as e:
    logger.error("Failed to initialize Sentry: %s", e)
    # Continue without Sentry - don't fail the application startup


# Override verify SSL from environment if set
waldur_verify_ssl = os.getenv("WALDUR_VERIFY_SSL")
if waldur_verify_ssl is not None:
    config.waldur_verify_ssl = waldur_verify_ssl.lower() in ("true", "yes", "1")

# Override proxy from environment if set
waldur_socks_proxy = os.getenv("WALDUR_SOCKS_PROXY")
if waldur_socks_proxy is not None:
    config.waldur_socks_proxy = waldur_socks_proxy

# Log proxy configuration
if config.waldur_socks_proxy:
    logger.info(
        "Using SOCKS proxy for Waldur API connections: %s", config.waldur_socks_proxy
    )
else:
    logger.info("No SOCKS proxy configured for Waldur API connections")

# Create Waldur API client
WALDUR_API_TOKEN = os.getenv("WALDUR_API_TOKEN", "")
if not WALDUR_API_TOKEN and config.waldur_api_token:
    WALDUR_API_TOKEN = config.waldur_api_token

waldur_client = get_client(
    api_url=config.waldur_api_url,
    access_token=WALDUR_API_TOKEN,
    verify_ssl=config.waldur_verify_ssl,
    proxy=config.waldur_socks_proxy,
)


# HPC User API settings with environment variable support
HPC_USER_API_URL = os.getenv("HPC_USER_API_URL")
if HPC_USER_API_URL is None and config.hpc_user_api:
    HPC_USER_API_URL = config.hpc_user_api.api_url

HPC_USER_CLIENT_ID = os.getenv("HPC_USER_CLIENT_ID")
if HPC_USER_CLIENT_ID is None and config.hpc_user_api:
    HPC_USER_CLIENT_ID = config.hpc_user_api.client_id

HPC_USER_CLIENT_SECRET = os.getenv("HPC_USER_CLIENT_SECRET")
if HPC_USER_CLIENT_SECRET is None and config.hpc_user_api:
    HPC_USER_CLIENT_SECRET = config.hpc_user_api.client_secret

HPC_USER_OIDC_TOKEN_URL = os.getenv("HPC_USER_OIDC_TOKEN_URL")
if HPC_USER_OIDC_TOKEN_URL is None and config.hpc_user_api:
    HPC_USER_OIDC_TOKEN_URL = config.hpc_user_api.oidc_token_url

HPC_USER_OIDC_SCOPE = os.getenv("HPC_USER_OIDC_SCOPE")
if HPC_USER_OIDC_SCOPE is None and config.hpc_user_api:
    HPC_USER_OIDC_SCOPE = config.hpc_user_api.oidc_scope

HPC_USER_SOCKS_PROXY = os.getenv("HPC_USER_SOCKS_PROXY")
if HPC_USER_SOCKS_PROXY is None and config.hpc_user_api:
    HPC_USER_SOCKS_PROXY = config.hpc_user_api.socks_proxy

# Convert HPC User API settings to dict if any values are present
hpc_user_api_settings = None
if any(
    [
        HPC_USER_API_URL,
        HPC_USER_CLIENT_ID,
        HPC_USER_CLIENT_SECRET,
        HPC_USER_OIDC_TOKEN_URL,
        HPC_USER_OIDC_SCOPE,
        HPC_USER_SOCKS_PROXY,
    ]
):
    hpc_user_api_settings = {
        "api_url": HPC_USER_API_URL,
        "client_id": HPC_USER_CLIENT_ID,
        "client_secret": HPC_USER_CLIENT_SECRET,
        "oidc_token_url": HPC_USER_OIDC_TOKEN_URL,
        "oidc_scope": HPC_USER_OIDC_SCOPE,
        "socks_proxy": HPC_USER_SOCKS_PROXY,
    }

cscs_storage_backend = CscsHpcStorageBackend(
    config.backend_settings,
    config.backend_components,
    hpc_user_api_settings=hpc_user_api_settings,
)

# Authentication settings - environment variables override config file
disable_auth_env = os.getenv("DISABLE_AUTH")
if disable_auth_env is not None:
    DISABLE_AUTH = disable_auth_env.lower() in ("true", "yes", "1")
elif config.auth:
    DISABLE_AUTH = config.auth.disable_auth
else:
    DISABLE_AUTH = False

app = FastAPI(redirect_slashes=True)


class StorageSystem(str, Enum):
    """Allowed storage system values."""

    CAPSTOR = "capstor"
    VAST = "vast"
    IOPSSTOR = "iopsstor"


@app.exception_handler(RequestValidationError)
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


@app.exception_handler(Exception)
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


if not DISABLE_AUTH:
    setup_auth(app, config)
    user_dependency = get_user
else:
    logger.warning(
        "Authentication is disabled! This should only be used in development."
    )
    user_dependency = mock_user

OIDCUserDependency = Annotated[User, Depends(user_dependency)]


@app.get("/api/storage-resources/")
def storage_resources(
    user: OIDCUserDependency,
    storage_system: Annotated[
        Optional[StorageSystem], Query(description="Optional: Storage system filter")
    ] = None,
    state: Optional[ResourceState] = None,
    page: Annotated[int, Query(ge=1, description="Page number (starts from 1)")] = 1,
    page_size: Annotated[
        int, Query(ge=1, le=500, description="Number of items per page")
    ] = 100,
    data_type: Annotated[
        Optional[str],
        Query(description="Optional: Data type filter (users/scratch/store/archive)"),
    ] = None,
    status: Annotated[
        Optional[str],
        Query(description="Optional: Status filter (pending/removing/active/error)"),
    ] = None,
    debug: Annotated[
        bool, Query(description="Enable debug mode to return raw Waldur data")
    ] = False,
) -> JSONResponse:
    """Exposes list of all storage resources with pagination and filtering."""
    logger.info(
        "Processing request for user %s (page=%d, page_size=%d, storage_system=%s, "
        "data_type=%s, status=%s, debug=%s)",
        user.preferred_username,
        page,
        page_size,
        storage_system,
        data_type,
        status,
        debug,
    )

    # Set Sentry user context for error tracking
    set_user_context(
        user_id=getattr(user, "sub", "unknown"),
        username=user.preferred_username,
        email=getattr(user, "email", None),
    )

    # Validate that storage_system is one of the configured storage systems (if provided)
    if storage_system and storage_system.value not in config.storage_systems:
        logger.warning(
            "Requested storage_system '%s' is not in configured storage_systems: %s",
            storage_system.value,
            list(config.storage_systems.keys()),
        )
        # Return empty result for non-configured storage systems
        return JSONResponse(
            content={
                "status": "success",
                "resources": [],
                "pagination": {
                    "current": page,
                    "limit": page_size,
                    "offset": (page - 1) * page_size,
                    "pages": 0,
                    "total": 0,
                },
                "filters_applied": {
                    "storage_system": storage_system.value if storage_system else None,
                    "data_type": data_type,
                    "status": status,
                    "state": state.value if state else None,
                },
            }
        )

    # Handle debug mode - return raw Waldur data
    if debug:
        logger.info("Debug mode enabled - returning raw Waldur data")

        # Prepare agent's configuration info
        agent_config_info = {
            "waldur_api_url": config.waldur_api_url,
            "backend_settings": config.backend_settings,
            "backend_components": config.backend_components,
            "configured_storage_systems": config.storage_systems,
            "requested_storage_system": storage_system.value
            if storage_system
            else None,
            "resolved_offering_slug": config.storage_systems.get(storage_system.value)
            if storage_system
            else None,
        }

        # Get raw resources
        if storage_system:
            # Get raw resources for the specific storage_system
            storage_system_offering_slug = config.storage_systems[storage_system.value]
            debug_data = cscs_storage_backend.get_debug_resources_by_slug(
                offering_slug=storage_system_offering_slug,
                client=waldur_client,
                state=state,
                page=page,
                page_size=page_size,
                data_type=data_type,
                status=status,
            )
        else:
            # Get raw resources from all storage systems
            debug_data = cscs_storage_backend.get_debug_resources_by_slugs(
                offering_slugs=list(config.storage_systems.values()),
                client=waldur_client,
                state=state,
                page=page,
                page_size=page_size,
                data_type=data_type,
                status=status,
            )

        return JSONResponse(
            content={
                "status": "success",
                "debug_mode": True,
                "agent_config": agent_config_info,
                "raw_resources": debug_data,
            }
        )

    # Normal mode - translated API response
    if storage_system:
        # Fetch resources for the specific storage_system
        storage_system_offering_slug = config.storage_systems[storage_system.value]
        storage_data = cscs_storage_backend.generate_all_resources_json_by_slug(
            offering_slug=storage_system_offering_slug,
            client=waldur_client,
            state=state,
            page=page,
            page_size=page_size,
            data_type=data_type,
            status=status,
        )
    else:
        # Fetch resources from all storage systems
        storage_data = cscs_storage_backend.generate_all_resources_json_by_slugs(
            offering_slugs=list(config.storage_systems.values()),
            client=waldur_client,
            state=state,
            page=page,
            page_size=page_size,
            data_type=data_type,
            status=status,
        )

    # Return appropriate HTTP status code based on response status
    if storage_data.get("status") == "error":
        return JSONResponse(
            content=storage_data, status_code=storage_data.get("code", 500)
        )

    return JSONResponse(content=storage_data)
