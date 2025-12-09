import logging
from typing import Any, Callable, Optional, Union

from waldur_api_client.models.resource_state import ResourceState
from waldur_cscs_hpc_storage.base.enums import (
    StorageDataType,
    TargetStatus,
)
from waldur_cscs_hpc_storage.base.mappers import (
    get_target_status_from_waldur_state,
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
from waldur_cscs_hpc_storage.hierarchy_builder import HierarchyBuilder
from waldur_cscs_hpc_storage.resource_mapper import ResourceMapper
from waldur_cscs_hpc_storage.services.gid_service import GidService
from waldur_cscs_hpc_storage.services.mock_gid_service import MockGidService
from waldur_cscs_hpc_storage.services.waldur_service import WaldurService

logger = logging.getLogger(__name__)


def make_storage_resource_predicate(
    data_type: Optional[StorageDataType] = None,
    status: Optional[TargetStatus] = None,
) -> Callable[[StorageResource], bool]:
    """
    Create a filter predicate for StorageResources.

    Args:
        data_type: Optional StorageDataType to filter by.
        status: Optional TargetStatus to filter by.

    Returns:
        A callable that takes a StorageResource and returns True if it matches the criteria.
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
        self.serializer = JsonSerializer()

        # Configuration
        self.storage_file_system = backend_config.storage_file_system
        self.inode_soft_coefficient = backend_config.inode_soft_coefficient
        self.inode_hard_coefficient = backend_config.inode_hard_coefficient

        # Initialize services
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
        self.hierarchy_builder = HierarchyBuilder(self.storage_file_system)
        self.mapper = ResourceMapper(backend_config, self.gid_service)

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

        Args:
            offering_slugs: Single offering slug or list of offering slugs
            state: Optional state filter for Waldur resources
            page: Page number (1-based)
            page_size: Items per page (default 100)
            data_type: Optional filter by storage data type
            status: Optional filter by target status

        Returns:
            Dictionary containing list of resources, pagination info, and status
        """
        try:
            logger.info("Generating resources JSON for offerings: %s", offering_slugs)
            logger.info(
                "Filters - State: %s, DataType: %s, Status: %s",
                state,
                data_type,
                status,
            )

            resources, total_items = self._get_resources_by_offering_slugs(
                offering_slugs,
                state_filter=state,
                data_type_filter=data_type,
                status_filter=status,
                page=page,
                page_size=page_size,
            )

            # Calculate pagination
            total_pages = (
                (total_items + page_size - 1) // page_size if total_items > 0 else 0
            )

            paginated_resources = resources

            logger.info(
                "Found %d total resources. Returning page %d/%d (%d items)",
                total_items,
                page,
                total_pages,
                len(paginated_resources),
            )

            resource_dicts = [self.serializer.serialize(r) for r in paginated_resources]

            return {
                "status": "success",
                "resources": resource_dicts,
                "pagination": {
                    "current": page,
                    "limit": page_size,
                    "offset": (page - 1) * page_size,
                    "pages": total_pages,
                    "total": total_items,
                },
            }

        except Exception as e:
            logger.exception("Error generating resources JSON")
            return {
                "status": "error",
                "message": str(e),
                "type": type(e).__name__,
                "code": 500,
            }

    def _get_resources_by_offering_slugs(
        self,
        offering_slugs: Union[str, list[str]],
        state_filter: Optional[ResourceState] = None,
        data_type_filter: Optional[StorageDataType] = None,
        status_filter: Optional[TargetStatus] = None,
        page: int = 1,
        page_size: int = 100,
    ) -> tuple[list[StorageResource], int]:
        """
        Fetch and transform resources from Waldur for multiple offering slugs.
        Applies filtering at the API level (state) and post-processing level (data_type, status).

        Returns:
            Tuple of (list of storage resources, total count from API)
        """
        if isinstance(offering_slugs, str):
            offering_slug_list = [offering_slugs]
        else:
            offering_slug_list = offering_slugs

        storage_resources: list[StorageResource] = []

        # Fetch specific page
        response = self.waldur_service.list_resources(
            offering_slug=offering_slug_list,
            state=state_filter,
            page=page,
            page_size=page_size,
        )
        self._process_resources(
            response.resources, offering_slug_list, storage_resources
        )

        # 3. Append gathered hierarchy items (Tenants/Customers)
        # We add them to the list so they appear in the output
        storage_resources.extend(self.hierarchy_builder.get_hierarchy_resources())

        # 4. Filter results
        predicate = make_storage_resource_predicate(
            data_type=data_type_filter,
            status=status_filter,
        )
        filtered_resources = list(filter(predicate, storage_resources))

        return filtered_resources, response.total_count

    def _process_resources(
        self,
        resources: list[Any],
        offering_slug_list: list[str],
        storage_resources: list[StorageResource],
    ) -> None:
        """Process a list of Waldur resources and map them to StorageResources."""
        for resource in resources:
            try:
                # Resource is already parsed by WaldurService
                parsed_resource = resource

                # Determine storage system for this offering
                # We use the first requested slug as the default storage system name
                storage_system = offering_slug_list[0]
                storage_data_type = parsed_resource.attributes.storage_data_type.value

                # Add hierarchy items (Tenant/Customer)
                tenant_id = resource.provider_slug
                tenant_name = resource.provider_name or tenant_id.upper()

                # Check status for hierarchy activation
                should_be_active = (
                    get_target_status_from_waldur_state(resource.state)
                    == TargetStatus.ACTIVE
                )

                # Ensure tenant exists
                self.hierarchy_builder.get_or_create_tenant(
                    tenant_id=tenant_id,
                    tenant_name=tenant_name,
                    storage_system=storage_system,
                    storage_data_type=storage_data_type,
                    active=should_be_active,
                )

                # Ensure customer exists (linked to tenant)
                customer_info = {
                    "itemId": parsed_resource.customer_uuid,
                    "key": parsed_resource.customer_slug,
                    "name": parsed_resource.customer_name,
                }
                self.hierarchy_builder.get_or_create_customer(
                    customer_info=customer_info,
                    storage_system=storage_system,
                    storage_data_type=storage_data_type,
                    tenant_id=tenant_id,
                    active=should_be_active,
                )

                # Get parent customer UUID for project linkage
                parent_item_id = self.hierarchy_builder.get_customer_uuid(
                    customer_slug=parsed_resource.customer_slug,
                    storage_system=storage_system,
                    storage_data_type=storage_data_type,
                )

                # Map resource
                storage_resource = self.mapper.map_resource(
                    parsed_resource,
                    storage_system,
                    parent_item_id=parent_item_id,
                )

                if storage_resource:
                    storage_resources.append(storage_resource)

            except Exception as e:
                logger.exception(
                    "Error processing resource %s: %s",
                    getattr(resource, "uuid", "unknown"),
                    e,
                )
                continue
