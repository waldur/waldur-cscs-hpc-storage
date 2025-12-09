import logging
from typing import Any, Dict, List, Optional, Union

from waldur_api_client.models.resource_state import ResourceState

from waldur_cscs_hpc_storage.base.enums import StorageDataType, TargetStatus
from waldur_cscs_hpc_storage.base.models import StorageResource
from waldur_cscs_hpc_storage.base.schemas import ParsedWaldurResource
from waldur_cscs_hpc_storage.config import BackendConfig
from waldur_cscs_hpc_storage.hierarchy_builder import HierarchyBuilder
from waldur_cscs_hpc_storage.services.mapper import ResourceMapper
from waldur_cscs_hpc_storage.services.waldur_service import WaldurService

logger = logging.getLogger(__name__)


class StorageOrchestrator:
    """
    Coordinator service that bridges external API fetching, data mapping,
    and hierarchy construction.

    This replaces the monolithic logic previously found in CscsHpcStorageBackend.
    """

    def __init__(
        self,
        config: BackendConfig,
        waldur_service: WaldurService,
        mapper: ResourceMapper,
    ):
        """
        Initialize the orchestrator.

        Args:
            config: Backend configuration (for file system names, etc).
            waldur_service: Service to fetch raw data from Waldur.
            mapper: Service to transform raw data into CSCS domain models.
        """
        self.config = config
        self.waldur_service = waldur_service
        self.mapper = mapper

    def get_resources(
        self,
        offering_slugs: Union[str, List[str]],
        state: Optional[ResourceState] = None,
        page: int = 1,
        page_size: int = 100,
        data_type: Optional[StorageDataType] = None,
        status: Optional[TargetStatus] = None,
    ) -> Dict[str, Any]:
        """
        Main entry point to retrieve and format storage resources.

        Flow:
        1. Fetch raw resources from Waldur API based on offering slugs and state.
        2. Fetch associated customer metadata for hierarchy naming.
        3. Build the resource hierarchy (Tenant -> Customer -> Project).
        4. Map raw resources to StorageResource objects.
        5. Apply post-fetch filters (data_type, status).
        6. Calculate pagination and format response.
        """
        # Normalize slugs to list
        slugs_list = (
            [offering_slugs] if isinstance(offering_slugs, str) else offering_slugs
        )

        try:
            logger.info("Orchestrating resource fetch for slugs: %s", slugs_list)

            # 1. Fetch raw data from Waldur
            response = self.waldur_service.list_resources(
                page=page,
                page_size=page_size,
                offering_slug=slugs_list,
                state=state,
            )
            raw_resources = response.resources
            total_api_count = response.total_count

            logger.info(
                "Fetched %d resources from Waldur (Total API count: %d)",
                len(raw_resources),
                total_api_count,
            )

            # 2. Process resources if any exist
            if raw_resources:
                processed_resources = self._process_resources(raw_resources)
            else:
                processed_resources = []

            # 3. Apply post-processing filters (Memory-side filtering)
            # Note: We filter *after* hierarchy building because the API
            # might return a 'Project' that we want, but we also need its
            # 'Tenant' and 'Customer' parents which are generated locally.
            filtered_resources = self._filter_resources(
                processed_resources, data_type, status
            )

            # 4. Serialize and Paginate
            serialized_resources = [r.to_dict() for r in filtered_resources]

            # Calculate pagination based on the filtered list size
            # Note: This represents the "view" pagination, not necessarily 1:1 with API pages
            # due to hierarchy expansion (adding parent nodes) and filtering.
            total_items = len(filtered_resources)
            total_pages = (
                (total_items + page_size - 1) // page_size if total_items > 0 else 0
            )

            return {
                "status": "success",
                "resources": serialized_resources,
                "pagination": {
                    "current": page,
                    "limit": page_size,
                    "offset": (page - 1) * page_size,
                    "pages": total_pages,
                    "total": total_items,
                    "api_total": total_api_count,
                },
                "filters_applied": {
                    "offering_slugs": slugs_list,
                    "data_type": data_type.value if data_type else None,
                    "status": status.value if status else None,
                    "state": state.value if state else None,
                },
            }

        except Exception as e:
            logger.error(
                "Orchestration failed for slugs %s: %s", slugs_list, e, exc_info=True
            )
            return {
                "status": "error",
                "message": f"Failed to fetch storage resources: {str(e)}",
                "code": 500,
            }

    def _process_resources(
        self, raw_resources: List[ParsedWaldurResource]
    ) -> List[StorageResource]:
        """
        Core loop: Metadata fetching, Hierarchy building, and Resource Mapping.
        """
        # A. Pre-fetch Customer Metadata for efficient Hierarchy building
        # We need distinct offering UUIDs to query the customers endpoint
        offering_uuids = {r.offering_uuid for r in raw_resources}
        all_offering_customers = {}

        for offering_uuid in offering_uuids:
            customers = self.waldur_service.get_offering_customers(offering_uuid)
            all_offering_customers.update(customers)

        # B. Initialize a fresh HierarchyBuilder for this request
        hierarchy_builder = HierarchyBuilder(self.config.storage_file_system)
        project_resources: List[StorageResource] = []

        logger.debug("Processing %d raw resources", len(raw_resources))

        for resource in raw_resources:
            try:
                # 1. Identify Storage System and Data Type
                # The offering_slug corresponds to the storage system (e.g., 'capstor')
                storage_system = resource.offering_slug
                storage_data_type_str = (
                    resource.attributes.storage_data_type or StorageDataType.STORE.value
                )

                # 2. Register Tenant (Top Level)
                tenant_id = resource.provider_slug
                tenant_name = resource.provider_name or tenant_id.upper()

                hierarchy_builder.get_or_create_tenant(
                    tenant_id=tenant_id,
                    tenant_name=tenant_name,
                    storage_system=storage_system,
                    storage_data_type=storage_data_type_str,
                    offering_uuid=resource.offering_uuid,
                )

                # 3. Register Customer (Mid Level)
                if resource.customer_slug in all_offering_customers:
                    customer_info = all_offering_customers[resource.customer_slug]
                    hierarchy_builder.get_or_create_customer(
                        customer_info=customer_info,
                        storage_system=storage_system,
                        storage_data_type=storage_data_type_str,
                        tenant_id=tenant_id,
                    )

                # 4. Map the Project/User Resource (Bottom Level)
                # We need the parent ID (Customer ID) to link the project correctly
                customer_id = hierarchy_builder.get_customer_uuid(
                    customer_slug=resource.customer_slug,
                    storage_system=storage_system,
                    storage_data_type=storage_data_type_str,
                )

                mapped_resource = self.mapper.map_resource(
                    waldur_resource=resource,
                    storage_system=storage_system,
                    parent_item_id=customer_id,
                )

                if mapped_resource:
                    project_resources.append(mapped_resource)

            except Exception as e:
                logger.warning(
                    "Skipping resource %s due to processing error: %s",
                    resource.uuid,
                    e,
                )
                continue

        # Combine generated hierarchy nodes (Tenants/Customers) with mapped project nodes
        # The hierarchy nodes must come first in the list
        return hierarchy_builder.get_hierarchy_resources() + project_resources

    def _filter_resources(
        self,
        resources: List[StorageResource],
        data_type: Optional[StorageDataType],
        status: Optional[TargetStatus],
    ) -> List[StorageResource]:
        """
        Apply filtering predicates to the processed list of resources.
        """
        if not data_type and not status:
            return resources

        filtered = []
        for res in resources:
            # Filter by Data Type
            if data_type:
                # Access the 'key' or 'value' from the StorageItem
                # The StorageItem.key is lowercase (e.g., 'store')
                res_type = res.storageDataType.key
                if res_type != data_type.value:
                    continue

            # Filter by Status
            if status:
                # res.status is a TargetStatus enum
                if res.status != status:
                    continue

            filtered.append(res)

        return filtered
