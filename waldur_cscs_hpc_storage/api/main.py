from typing import Annotated

from fastapi import Depends, FastAPI
from fastapi.logger import logger
from fastapi_keycloak_middleware import get_user
from waldur_api_client.models.user import User

from waldur_cscs_hpc_storage.config import load_config
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
)
from waldur_cscs_hpc_storage.exceptions import (
    ConfigurationError,
    ResourceProcessingError,
    StorageProxyError,
    UpstreamServiceError,
)
from waldur_cscs_hpc_storage.models import StorageResourceFilter
from waldur_cscs_hpc_storage.services.auth import mock_user, setup_auth
from waldur_cscs_hpc_storage.services.orchestrator import StorageOrchestrator
from waldur_cscs_hpc_storage.serialization import JSONResponse

# Load configuration
config = load_config()

# Initialize global config for dependency injection
set_global_config(config)

app = FastAPI(redirect_slashes=True, default_response_class=JSONResponse)
app.add_exception_handler(UpstreamServiceError, upstream_service_error_handler)
app.add_exception_handler(ResourceProcessingError, resource_processing_error_handler)
app.add_exception_handler(ConfigurationError, configuration_error_handler)
app.add_exception_handler(StorageProxyError, storage_proxy_error_handler)
app.add_exception_handler(Exception, general_exception_handler)


if config.auth and not config.auth.disable_auth:
    setup_auth(app, config.auth)
    user_dependency = get_user
else:
    logger.warning(
        "Authentication is disabled! This should only be used in development."
    )
    user_dependency = mock_user


@app.get("/api/storage-resources/")
async def storage_resources(
    user: Annotated[User, Depends(user_dependency)],
    orchestrator: Annotated[StorageOrchestrator, Depends(get_orchestrator)],
    filters: Annotated[StorageResourceFilter, Depends()],
) -> JSONResponse:
    """Exposes list of all storage resources with pagination and filtering."""
    storage_data = await orchestrator.get_resources(filters)
    return storage_data
