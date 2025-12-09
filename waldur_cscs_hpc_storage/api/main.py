"""API server used as proxy to Waldur storage resources."""

from typing import Annotated, Optional

from fastapi import Depends, FastAPI, Query
from fastapi.exceptions import RequestValidationError
from fastapi.logger import logger
from fastapi.responses import JSONResponse
from fastapi_keycloak_middleware import get_user
from waldur_api_client.models.resource_state import ResourceState
from waldur_api_client.models.user import User

from waldur_cscs_hpc_storage.api.config_parser import parse_configuration
from waldur_cscs_hpc_storage.api.dependencies import (
    get_orchestrator,
    set_global_config,
)
from waldur_cscs_hpc_storage.api.handlers import (
    configuration_error_handler,
    general_exception_handler,
    resource_processing_error_handler,
    storage_proxy_error_handler,
    upstream_service_error_handler,
    validation_exception_handler,
)
from waldur_cscs_hpc_storage.exceptions import (
    ConfigurationError,
    ResourceProcessingError,
    StorageProxyError,
    UpstreamServiceError,
)
from waldur_cscs_hpc_storage.base.enums import (
    StorageDataType,
    StorageSystem,
    TargetStatus,
)
from waldur_cscs_hpc_storage.services.auth import mock_user, setup_auth
from waldur_cscs_hpc_storage.services.orchestrator import StorageOrchestrator

# Parse all configuration
config, waldur_api_config, hpc_user_api_config, DISABLE_AUTH = parse_configuration()

# Initialize global config for dependency injection
set_global_config(config)

app = FastAPI(redirect_slashes=True)


app.add_exception_handler(RequestValidationError, validation_exception_handler)
app.add_exception_handler(UpstreamServiceError, upstream_service_error_handler)
app.add_exception_handler(ResourceProcessingError, resource_processing_error_handler)
app.add_exception_handler(ConfigurationError, configuration_error_handler)
app.add_exception_handler(StorageProxyError, storage_proxy_error_handler)
app.add_exception_handler(Exception, general_exception_handler)


if not DISABLE_AUTH:
    setup_auth(app, config)
    user_dependency = get_user
else:
    logger.warning(
        "Authentication is disabled! This should only be used in development."
    )
    user_dependency = mock_user

OIDCUserDependency = Annotated[User, Depends(user_dependency)]
OrchestratorDependency = Annotated[StorageOrchestrator, Depends(get_orchestrator)]


@app.get("/api/storage-resources/")
async def storage_resources(
    user: OIDCUserDependency,
    orchestrator: OrchestratorDependency,
    storage_system: Annotated[
        Optional[StorageSystem], Query(description="Storage system filter")
    ] = None,
    state: Optional[ResourceState] = None,
    page: Annotated[int, Query(ge=1, description="Page number (starts from 1)")] = 1,
    page_size: Annotated[
        int, Query(ge=1, le=500, description="Number of items per page")
    ] = 100,
    data_type: Annotated[
        Optional[StorageDataType],
        Query(description="Data type filter"),
    ] = None,
    status: Annotated[
        Optional[TargetStatus],
        Query(description="Status filter"),
    ] = None,
) -> JSONResponse:
    """Exposes list of all storage resources with pagination and filtering."""
    logger.info(
        "Processing request for user %s (page=%d, page_size=%d, storage_system=%s, "
        "data_type=%s, status=%s)",
        user.preferred_username,
        page,
        page_size,
        storage_system,
        data_type,
        status,
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
                    "data_type": data_type.value if data_type else None,
                    "status": status.value if status else None,
                    "state": state.value if state else None,
                },
            }
        )

    # Fetch resources for the specific storage_system
    if storage_system:
        storage_system_offering_slug = config.storage_systems[storage_system.value]
        offering_slugs = [storage_system_offering_slug]
    else:
        # Fetch resources for all storage_systems
        offering_slugs = list(config.storage_systems.values())

    storage_data = await orchestrator.get_resources(
        offering_slugs=offering_slugs,
        state=state,
        page=page,
        page_size=page_size,
        data_type=data_type,
        status=status,
    )

    # Return appropriate HTTP status code based on response status
    # Note: Exceptions are now handled by global exception handlers
    if storage_data.get("status") == "error":
        # Keep this for backward compatibility if any service still returns dict error
        return JSONResponse(
            content=storage_data, status_code=storage_data.get("code", 500)
        )

    return JSONResponse(content=storage_data)
