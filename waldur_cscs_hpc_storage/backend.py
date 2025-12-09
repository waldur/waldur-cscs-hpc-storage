import logging
from typing import Any, Optional, Union

from waldur_api_client.models.resource_state import ResourceState

from waldur_cscs_hpc_storage.base.enums import (
    StorageDataType,
    TargetStatus,
)
from waldur_cscs_hpc_storage.base.models import (
    StorageResource,
)
from waldur_cscs_hpc_storage.base.serializers import JsonSerializer
from waldur_cscs_hpc_storage.config import (
    BackendConfig,
    HpcUserApiConfig,
    WaldurApiConfig,
)
from waldur_cscs_hpc_storage.services.gid_service import GidService
from waldur_cscs_hpc_storage.services.mapper import ResourceMapper
from waldur_cscs_hpc_storage.services.mock_gid_service import MockGidService
from waldur_cscs_hpc_storage.services.orchestrator import StorageOrchestrator
from waldur_cscs_hpc_storage.services.waldur_service import WaldurService

logger = logging.getLogger(__name__)


def make_storage_resource_predicate(
    data_type: Optional[StorageDataType] = None,
    status: Optional[TargetStatus] = None,
) -> Any:
    """
    Deprecated: Predicate logic is now handled by StorageOrchestrator._filter_resources.
    Kept for test compatibility if tests import it directly.
    """

    def predicate(resource: StorageResource) -> bool:
        # Check storage data type
        if data_type and resource.storageDataType.key != data_type.value:
            return False

        # Check status
        if status and resource.status != status:
            return False

        return True

    return predicate


class CscsHpcStorageBackend:
    def __init__(
        self,
        backend_config: BackendConfig,
        waldur_api_config: WaldurApiConfig,
        hpc_user_api_config: Optional[HpcUserApiConfig] = None,
    ) -> None:
        """
        Initialize the CSCS HPC Storage backend.

        Args:
            backend_config: Backend configuration (BackendConfig object)
            waldur_api_config: Waldur API configuration (WaldurApiConfig object)
            hpc_user_api_config: Optional HPC User API configuration (HpcUserApiConfig object)
        """
        self.backend_config = backend_config
        self.serializer = JsonSerializer()
        self.waldur_service = WaldurService(waldur_api_config)

        # Initialize GID Service (Mock or Real based on config)
        if backend_config.development_mode or not hpc_user_api_config:
            if not backend_config.development_mode:
                logger.warning(
                    "HPC User API not configured. Falling back to Mock GID Service."
                )
            else:
                logger.info("Development mode enabled. Using Mock GID Service.")
            self.gid_service: Union[GidService, MockGidService] = MockGidService()
        else:
            self.gid_service = GidService(hpc_user_api_config)

        # Initialize helpers
        self.mapper = ResourceMapper(backend_config, self.gid_service)

        # Initialize Orchestrator
        self.orchestrator = StorageOrchestrator(
            config=backend_config,
            waldur_service=self.waldur_service,
            mapper=self.mapper,
        )

    def generate_all_resources_json_by_slugs(
        self,
        offering_slugs: Union[str, list[str]],
        state: Optional[ResourceState] = None,
        page: int = 1,
        page_size: int = 100,
        data_type: Optional[StorageDataType] = None,
        status: Optional[TargetStatus] = None,
    ) -> dict[str, Any]:
        """
        Generate JSON structure for all resources in specific offerings.
        Delegates to StorageOrchestrator.
        """
        return self.orchestrator.get_resources(
            offering_slugs=offering_slugs,
            state=state,
            page=page,
            page_size=page_size,
            data_type=data_type,
            status=status,
        )
