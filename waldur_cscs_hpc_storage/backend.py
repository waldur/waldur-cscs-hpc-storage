import logging
from typing import Any, Optional, Sequence, Union

from waldur_cscs_hpc_storage.gid_service import GidService
from waldur_cscs_hpc_storage.waldur_service import WaldurService
from waldur_api_client.models.resource_state import ResourceState

from waldur_api_client.types import Unset
from waldur_cscs_hpc_storage.base.enums import (
    StorageDataType,
    TargetStatus,
    TargetType,
)
from waldur_cscs_hpc_storage.base.mappers import (
    get_target_status_from_waldur_state,
    get_target_type_from_data_type,
)
from waldur_cscs_hpc_storage.config import (
    BackendConfig,
    HpcUserApiConfig,
    WaldurApiConfig,
)
from waldur_cscs_hpc_storage.base.models import (
    Permission,
    StorageItem,
    StorageResource,
    Target,
    TargetItem,
    TenantTargetItem,
    CustomerTargetItem,
    ProjectTargetItem,
    UserTargetItem,
    UserPrimaryProject,
    MountPoint,
)
from waldur_cscs_hpc_storage.base.mount_points import generate_project_mount_point
from waldur_cscs_hpc_storage.base.target_ids import (
    generate_customer_target_id,
    generate_project_target_id,
    generate_storage_data_type_target_id,
    generate_storage_filesystem_target_id,
    generate_storage_system_target_id,
    generate_tenant_target_id,
    generate_user_target_id,
)
from waldur_cscs_hpc_storage.base.schemas import ParsedWaldurResource
from waldur_cscs_hpc_storage.hierarchy_builder import HierarchyBuilder

from waldur_cscs_hpc_storage.base.serializers import JsonSerializer


logger = logging.getLogger(__name__)


class CscsHpcStorageBackend:
    """CSCS HPC Storage backend for JSON file generation."""

    def __init__(
        self,
        backend_config: BackendConfig,
        backend_components: list[str],
        waldur_api_config: WaldurApiConfig,
        hpc_user_api_config: Optional[HpcUserApiConfig] = None,
    ) -> None:
        """Initialize CSCS storage backend.

        Args:
            backend_config: Backend configuration (BackendConfig object)
            backend_components: List of enabled backend components
            waldur_api_config: Waldur API configuration (WaldurApiConfig object)
            hpc_user_api_config: Optional HPC User API configuration (HpcUserApiConfig object)
        """
        self.backend_components = backend_components
        self.backend_config = backend_config
        self.serializer = JsonSerializer()
        self.waldur_api_config = waldur_api_config

        # Configuration
        self.storage_file_system = backend_config.storage_file_system
        self.inode_soft_coefficient = backend_config.inode_soft_coefficient
        self.inode_hard_coefficient = backend_config.inode_hard_coefficient
        self.inode_base_multiplier = backend_config.inode_base_multiplier
        self.use_mock_target_items = backend_config.use_mock_target_items
        self.development_mode = backend_config.development_mode

        self.gid_service: Optional[GidService] = None
        if hpc_user_api_config:
            self.gid_service = GidService(hpc_user_api_config)
        else:
            logger.info("HPC User client not configured - using mock unixGid values")

        self.waldur_service = WaldurService(waldur_api_config)

        self.backend_config.validate()

    def _apply_filters(
        self,
        storage_resources: Sequence[Union[StorageResource, dict[str, Any]]],
        storage_system: Optional[str] = None,
        data_type: Optional[str] = None,
        status: Optional[str] = None,
    ) -> list[Union[StorageResource, dict[str, Any]]]:
        """Apply filtering to storage resources list.

        Args:
            storage_resources: List of storage resource objects or dicts
            storage_system: Optional filter for storage system
            data_type: Optional filter for data type
            status: Optional filter for status

        Returns:
            Filtered list of storage resources
        """
        logger.debug(
            "Applying filters: storage_system=%s, data_type=%s, status=%s on %d resources",
            storage_system,
            data_type,
            status,
            len(storage_resources),
        )
        filtered_resources: list[Union[StorageResource, dict[str, Any]]] = []

        for resource in storage_resources:
            # Handle both StorageResource objects and dicts
            if isinstance(resource, dict):
                resource_storage_system = resource.get("storageSystem", {}).get("key")
                resource_data_type = resource.get("storageDataType", {}).get("key")
                resource_status = resource.get("status")
            else:
                resource_storage_system = resource.storageSystem.key
                resource_data_type = resource.storageDataType.key
                resource_status = (
                    resource.status.value
                    if hasattr(resource.status, "value")
                    else str(resource.status)
                )

            # Optional storage_system filter
            if storage_system:
                if resource_storage_system != storage_system:
                    continue

            # Optional data_type filter
            if data_type:
                logger.debug(
                    "Comparing data_type filter '%s' with resource data_type '%s'",
                    data_type,
                    resource_data_type,
                )
                if resource_data_type != data_type:
                    continue

            # Optional status filter
            if status:
                if resource_status != status:
                    continue

            filtered_resources.append(resource)

        logger.debug(
            "Applied filters: storage_system=%s, data_type=%s, status=%s. "
            "Filtered %d resources from %d total",
            storage_system,
            data_type,
            status,
            len(filtered_resources),
            len(storage_resources),
        )

        return filtered_resources

    def _get_project_unix_gid(self, project_slug: str) -> Optional[int]:
        """Get unixGid for project from HPC User service with caching.

        Cache persists until server restart. No TTL-based expiration.

        In production mode: Returns None if service fails (resource should be skipped)
        In development mode: Falls back to mock values if service fails

        Args:
            project_slug: Project slug to look up

        Returns:
            unixGid value from service, mock value (dev mode), or None (prod mode on failure)
        """
        # Try to fetch from HPC User service
        if self.gid_service:
            unix_gid = self.gid_service.get_project_unix_gid(project_slug)
            if unix_gid is not None:
                return unix_gid

            # Project not found in service
            if self.development_mode:
                logger.warning(
                    "Project %s not found in HPC User service, using mock value (dev mode)",
                    project_slug,
                )
            else:
                logger.error(
                    "Project %s not found in HPC User service, "
                    "skipping resource (production mode)",
                    project_slug,
                )
                return None

        # No HPC User client configured - use development mode behavior
        if not self.development_mode:
            logger.error(
                "HPC User service not configured for project %s, "
                "skipping resource (production mode)",
                project_slug,
            )
            return None

        # Development mode or no HPC client: use mock value
        mock_gid = 30000 + hash(project_slug) % 10000
        logger.debug(
            "Using mock unixGid %d for project %s (dev mode)",
            mock_gid,
            project_slug,
        )
        return mock_gid

    def _get_target_item_data(  # noqa: PLR0911
        self,
        waldur_resource: ParsedWaldurResource,
        target_type: TargetType,
    ) -> Optional[TargetItem]:
        """Get target item data from backend_metadata or generate mock data."""
        if not self.use_mock_target_items:
            # Try to get real data from backend_metadata
            # Using getattr/dict access because ResourceBackendMetadata has fields like tenant_item, project_item
            target_item_field = f"{target_type.value}_item"
            target_data = getattr(
                waldur_resource.backend_metadata, target_item_field, None
            )
            if target_data:
                return target_data

        # Generate mock data for development/testing
        if target_type == TargetType.TENANT:
            return TenantTargetItem(
                itemId=generate_tenant_target_id(waldur_resource.customer_slug),
                key=waldur_resource.customer_slug.lower(),
                name=waldur_resource.customer_name,
            )
        if target_type == TargetType.CUSTOMER:
            return CustomerTargetItem(
                itemId=generate_customer_target_id(waldur_resource.project_slug),
                key=waldur_resource.project_slug.lower(),
                name=waldur_resource.project_name,
            )
        if target_type == TargetType.PROJECT:
            target_status = get_target_status_from_waldur_state(waldur_resource.state)
            project_slug = waldur_resource.project_slug or "unknown"

            unix_gid = self._get_project_unix_gid(project_slug)
            if unix_gid is None:
                return None  # Skip resource when unixGid lookup fails in production
            return ProjectTargetItem(
                itemId=generate_project_target_id(waldur_resource.slug),
                key=None,  # Not used for project
                name=waldur_resource.slug,
                status=target_status,
                unixGid=unix_gid,
                active=target_status == TargetStatus.ACTIVE,
            )
        if target_type == TargetType.USER:
            target_status = get_target_status_from_waldur_state(waldur_resource.state)
            project_slug = waldur_resource.project_slug or "default-project"

            # TODO: Just a placeholder, for user a default gid would be needed, which could be
            # looked up from https://api-user.hpc-user.tds.cscs.ch/api/v1/export/cscs/users/{username}
            unix_gid = self._get_project_unix_gid(project_slug)
            if unix_gid is None:
                return None  # Skip resource when unixGid lookup fails in production

            return UserTargetItem(
                itemId=generate_user_target_id(waldur_resource.slug),
                key=None,
                name=None,
                status=target_status,
                email=f"user-{waldur_resource.slug}@example.com",  # Mock email
                unixUid=20000 + hash(waldur_resource.slug) % 10000,  # Mock UID
                primaryProject=UserPrimaryProject(
                    name=project_slug,
                    unixGid=unix_gid,
                    active=target_status == TargetStatus.ACTIVE,
                ),
                active=target_status == TargetStatus.ACTIVE,
            )

        return TargetItem(itemId="unknown")  # Should not happen with valid target_type

    def _get_target_data(
        self,
        waldur_resource: ParsedWaldurResource,
        storage_data_type: str,
    ) -> Optional[Target]:
        """Get target data based on storage data type mapping."""
        target_type = get_target_type_from_data_type(
            storage_data_type, waldur_resource.uuid
        )

        target_item = self._get_target_item_data(waldur_resource, target_type)
        if target_item is None:
            return None  # Skip resource when target item creation fails (e.g., unixGid lookup fails)

        return Target(
            targetType=target_type,
            targetItem=target_item,
        )

    def _create_storage_resource_json(
        self,
        waldur_resource: ParsedWaldurResource,
        storage_system: str,
    ) -> Optional[StorageResource]:
        """Create JSON representation for a single storage resource.

        Args:
            waldur_resource: Waldur resource object
            storage_system: Storage system name
        """
        logger.debug(
            "Creating storage resource JSON for resource %s", waldur_resource.uuid
        )
        logger.debug(
            "Input storage_system: %s (type: %s)",
            storage_system,
            type(storage_system),
        )

        # Validate storage_system is a string
        if not isinstance(storage_system, str):
            error_msg = (
                f"Invalid storage_system type for resource {waldur_resource.uuid}: "
                f"expected string, got {type(storage_system).__name__}. "
                f"Value: {storage_system!r}"
            )
            logger.error(error_msg)
            raise TypeError(error_msg)

        if not storage_system:
            error_msg = (
                f"Empty storage_system provided for resource {waldur_resource.uuid}. "
                "A valid storage system name is required."
            )
            logger.error(error_msg)
            raise TypeError(error_msg)

        logger.debug("Final storage_system: %s", storage_system)

        # Calculate and render quotas (storage and inodes) with overrides from options
        quotas = waldur_resource.render_quotas(
            self.inode_base_multiplier,
            self.inode_soft_coefficient,
            self.inode_hard_coefficient,
        )

        # Get storage data type
        storage_data_type = waldur_resource.attributes.storage_data_type

        # Generate mount point now that we have the storage_data_type
        mount_point = generate_project_mount_point(
            storage_system=storage_system,
            tenant_id=waldur_resource.provider_slug,
            customer=waldur_resource.customer_slug,
            project_id=waldur_resource.project_slug,  # might not be unique
            data_type=storage_data_type,
        )

        # Get status from waldur resource state
        cscs_status = get_target_status_from_waldur_state(waldur_resource.state)

        logger.debug(
            "Mapped waldur state '%s' to CSCS status '%s'",
            waldur_resource.state,
            cscs_status,
        )

        # Get target data - return None if target creation fails
        # (e.g., unixGid lookup fails in production)
        target_data = self._get_target_data(waldur_resource, storage_data_type)
        if target_data is None:
            logger.warning(
                "Skipping resource %s due to target data creation failure (production mode)",
                waldur_resource.uuid,
            )
            return None

        return StorageResource(
            itemId=waldur_resource.uuid,
            status=cscs_status,
            mountPoint=MountPoint(default=mount_point),
            permission=Permission(value=waldur_resource.effective_permissions),
            quotas=quotas,
            target=target_data,
            storageSystem=StorageItem(
                itemId=generate_storage_system_target_id(storage_system),
                key=storage_system.lower(),
                name=storage_system.upper(),
            ),
            storageFileSystem=StorageItem(
                itemId=generate_storage_filesystem_target_id(self.storage_file_system),
                key=self.storage_file_system.lower(),
                name=self.storage_file_system.upper(),
            ),
            storageDataType=StorageItem(
                itemId=generate_storage_data_type_target_id(storage_data_type),
                key=storage_data_type.lower(),
                name=storage_data_type.upper(),
                path=storage_data_type.lower(),
            ),
            parentItemId=None,
            extra_fields=waldur_resource.callback_urls,
        )

    def generate_all_resources_json_by_slugs(
        self,
        offering_slugs: Union[str, list[str]],
        state: Optional[ResourceState] = None,
        page: int = 1,
        page_size: int = 100,
        data_type: Optional[str] = None,
        status: Optional[str] = None,
        storage_system_filter: Optional[str] = None,
    ) -> dict:
        """Generate JSON with resources filtered by offering slug(s).

        Args:
            offering_slugs: Single offering slug or list of offering slugs.
            state: Optional resource state filter.
            page: Page number (1-based).
            page_size: Number of items per page.
            data_type: Optional data type filter.
            status: Optional status filter.
            storage_system_filter: Optional storage system filter.

        Returns:
            Dictionary with status, resources, pagination, and filters_applied.
        """
        # Normalize to list for consistent handling
        slugs_list = (
            [offering_slugs] if isinstance(offering_slugs, str) else offering_slugs
        )

        try:
            storage_resources, pagination_info = self._get_resources_by_offering_slugs(
                offering_slugs=slugs_list,
                state=state,
                page=page,
                page_size=page_size,
                data_type=data_type,
                status=status,
                storage_system_filter=storage_system_filter,
            )

            # Serialize storage resources to dicts for JSON response
            serialized_resources = [r.to_dict() for r in storage_resources]

            return {
                "status": "success",
                "resources": serialized_resources,
                "pagination": pagination_info,
                "filters_applied": {
                    "offering_slugs": slugs_list,
                    "storage_system": storage_system_filter,
                    "data_type": data_type,
                    "status": status,
                    "state": state.value if state else None,
                },
            }

        except Exception as e:
            logger.error(
                "Failed to generate storage resources JSON: %s", e, exc_info=True
            )
            return {
                "status": "error",
                "error": f"Failed to fetch storage resources: {e}",
                "code": 500,
            }

    def _get_resources_by_offering_slugs(
        self,
        offering_slugs: list[str],
        state: Optional[ResourceState] = None,
        page: int = 1,
        page_size: int = 100,
        data_type: Optional[str] = None,
        status: Optional[str] = None,
        storage_system_filter: Optional[str] = None,
    ) -> tuple[list[StorageResource], dict[str, Any]]:
        """Fetch and process resources filtered by multiple offering slugs."""
        # HPC User client diagnostics
        if self.gid_service:
            logger.info("HPC User API configured: %s", self.gid_service.api_url)
            hpc_user_available = self.gid_service.ping()
            logger.info("HPC User API accessible: %s", hpc_user_available)
            if not hpc_user_available:
                logger.warning(
                    "HPC User API not accessible, falling back to mock unixGid values"
                )
        else:
            logger.info("HPC User API: Not configured (using mock unixGid values)")
        logger.debug(
            "_get_resources_by_offering_slugs called with data_type=%s", data_type
        )
        try:
            # Use single API call with comma-separated offering slugs
            logger.info(
                "Fetching resources for offering slugs: %s", ", ".join(offering_slugs)
            )

            filters = {}
            if state:
                filters["state"] = state

            response = self.waldur_service.list_resources(
                page=page,
                page_size=page_size,
                offering_slug=offering_slugs,
                **filters,
            )

            all_storage_resources = []
            total_api_count = 0

            if response.resources:
                # Get count from response
                total_api_count = response.total_count
                logger.debug("Total API count: %d", total_api_count)

                logger.info(
                    "Found %d resources from API (total: %d)",
                    len(response.resources),
                    total_api_count,
                )

                # Get offering customers for hierarchical resources
                # For multiple slugs, we need to get customers for all unique offerings
                offering_uuids: set[str] = set()
                for resource in response.resources:
                    offering_uuids.add(resource.offering_uuid)

                all_offering_customers = {}
                for offering_uuid in offering_uuids:
                    customers = self.waldur_service.get_offering_customers(
                        offering_uuid
                    )
                    all_offering_customers.update(customers)  # Merge all customers

                # Use HierarchyBuilder for hierarchy management
                hierarchy_builder = HierarchyBuilder(self.storage_file_system)

                logger.debug(
                    "Starting to process %d resources", len(response.resources)
                )
                for resource in response.resources:
                    try:
                        # Apply storage_system filter if provided
                        if (
                            storage_system_filter
                            and resource.offering_slug != storage_system_filter
                        ):
                            logger.debug(
                                "Skipping resource due to storage_system filter"
                            )
                            continue

                        # Note: Additional filters (data_type, status) are applied
                        # after serialization

                        # Get storage data type for the resource
                        storage_data_type = StorageDataType.STORE  # default
                        if resource.attributes.storage_data_type:
                            try:
                                storage_data_type = StorageDataType(
                                    resource.attributes.storage_data_type
                                )
                            except ValueError:
                                pass  # keep default

                        # Get tenant information
                        tenant_id = resource.provider_slug
                        tenant_name = (
                            resource.provider_name
                            if hasattr(resource, "provider_name")
                            and not isinstance(resource.provider_name, Unset)
                            else tenant_id.upper()
                        )

                        # Create tenant-level entry using HierarchyBuilder
                        offering_uuid_str = str(resource.offering_uuid)
                        hierarchy_builder.get_or_create_tenant(
                            tenant_id=tenant_id,
                            tenant_name=tenant_name,
                            storage_system=resource.offering_slug,
                            storage_data_type=storage_data_type,
                            offering_uuid=offering_uuid_str,
                        )

                        # Create customer-level entry if customer info is available
                        if resource.customer_slug in all_offering_customers:
                            customer_info = all_offering_customers[
                                resource.customer_slug
                            ]
                            hierarchy_builder.get_or_create_customer(
                                customer_info=customer_info,
                                storage_system=resource.offering_slug,
                                storage_data_type=storage_data_type,
                                tenant_id=tenant_id,
                            )

                        # Create project-level resource (the original resource)
                        storage_resource = self._create_storage_resource_json(
                            waldur_resource=resource,
                            storage_system=resource.offering_slug,
                        )
                        if storage_resource is not None:
                            # Set parent reference using HierarchyBuilder
                            hierarchy_builder.assign_parent_to_project(
                                project_resource=storage_resource,
                                customer_slug=resource.customer_slug,
                                storage_system=resource.offering_slug,
                                storage_data_type=storage_data_type,
                            )

                            all_storage_resources.append(storage_resource)

                    except Exception as e:
                        logger.warning(
                            "Failed to process resource %s: %s",
                            getattr(resource, "uuid", "unknown"),
                            e,
                        )
                        continue

                # Prepend hierarchy resources (tenants, customers) to the list
                all_storage_resources = (
                    hierarchy_builder.get_hierarchy_resources() + all_storage_resources
                )
            else:
                logger.warning(
                    "No resources found for offering slugs: %s",
                    ", ".join(offering_slugs),
                )

            storage_resources = all_storage_resources
            total_count = total_api_count

            # Apply additional filters (data_type, status) in memory after JSON serialization
            logger.debug(
                "About to apply filters on %d resources", len(storage_resources)
            )
            filtered_resources = self._apply_filters(  # type: ignore[arg-type]
                storage_resources, None, data_type, status
            )
            storage_resources = filtered_resources  # type: ignore[assignment]

            # Calculate pagination based on filtered results
            total_pages = (
                (len(storage_resources) + page_size - 1) // page_size
                if storage_resources
                else 0
            )
            pagination_info = {
                "current": page,
                "limit": page_size,
                "offset": (page - 1) * page_size,
                "pages": total_pages,
                "total": len(storage_resources),
                "api_total": total_count,
            }

            return storage_resources, pagination_info

        except Exception as e:
            logger.error(
                "Failed to fetch storage resources by slugs: %s", e, exc_info=True
            )
            raise
