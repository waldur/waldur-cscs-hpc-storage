import logging
from typing import Optional

from waldur_cscs_hpc_storage.base.enums import (
    TargetStatus,
    TargetType,
)
from waldur_cscs_hpc_storage.base.mappers import (
    get_target_status_from_waldur_state,
    get_target_type_from_data_type,
)
from waldur_cscs_hpc_storage.base.models import (
    CustomerTargetItem,
    MountPoint,
    Permission,
    ProjectTargetItem,
    StorageItem,
    StorageResource,
    Target,
    TargetItem,
    TenantTargetItem,
    UserPrimaryProject,
    UserTargetItem,
)
from waldur_cscs_hpc_storage.base.mount_points import (
    generate_project_mount_point,
)
from waldur_cscs_hpc_storage.base.schemas import ParsedWaldurResource
from waldur_cscs_hpc_storage.base.target_ids import (
    generate_customer_target_id,
    generate_project_target_id,
    generate_storage_data_type_target_id,
    generate_storage_filesystem_target_id,
    generate_storage_system_target_id,
    generate_tenant_target_id,
    generate_user_target_id,
)
from waldur_cscs_hpc_storage.config import BackendConfig
from waldur_cscs_hpc_storage.services.gid_service import GidService
from waldur_cscs_hpc_storage.services.mock_gid_service import MockGidService


logger = logging.getLogger(__name__)


class ResourceMapper:
    """
    Responsible for mapping Waldur API resources to CSCS Storage Resources.

    This class handles:
    - Quota calculations based on configuration coefficients.
    - Target ID generation.
    - Unix GID lookup via GidService.
    - Creation of the Target object (Project/User).
    """

    def __init__(self, config: BackendConfig, gid_service: GidService | MockGidService):
        """
        Initialize the mapper with configuration and services.

        Args:
            config: Backend configuration containing coefficients and system names.
            gid_service: Service to lookup UNIX GIDs.
        """
        self.config = config
        self.gid_service = gid_service

    async def map_resource(
        self,
        waldur_resource: ParsedWaldurResource,
        storage_system: str,
        parent_item_id: Optional[str] = None,
    ) -> Optional[StorageResource]:
        """
        Transform a parsed Waldur resource into a StorageResource.

        Args:
            waldur_resource: The resource data from Waldur.
            storage_system: The name of the storage system (e.g., 'capstor').
            parent_item_id: The UUID of the parent Customer resource (optional).

        Returns:
            A populated StorageResource object, or None if mapping failed
            (e.g., GID lookup failed in production mode).
        """
        resource_uuid = waldur_resource.uuid
        logger.debug("Mapping resource %s for system %s", resource_uuid, storage_system)

        # 1. Determine Data Type & Target Type
        storage_data_type_str = waldur_resource.attributes.storage_data_type
        target_type = get_target_type_from_data_type(
            storage_data_type_str, resource_uuid
        )

        # 2. Build the Target Item (Project, User, etc.)
        # This step involves GID lookups and might return None if they fail.
        target_item = await self._build_target_item(waldur_resource, target_type)
        if target_item is None:
            logger.warning(
                "Skipping resource %s: Failed to build target item (likely missing GID)",
                resource_uuid,
            )
            return None

        target = Target(targetType=target_type, targetItem=target_item)

        # 3. Calculate Quotas
        quotas = waldur_resource.render_quotas(
            inode_base_multiplier=self.config.inode_base_multiplier,
            inode_soft_coefficient=self.config.inode_soft_coefficient,
            inode_hard_coefficient=self.config.inode_hard_coefficient,
        )

        # 4. Generate Mount Point
        # Note: Even for User targets, we currently generate a project-level mount point structure
        mount_point_path = generate_project_mount_point(
            storage_system=storage_system,
            tenant_id=waldur_resource.provider_slug,
            customer=waldur_resource.customer_slug,
            project_id=waldur_resource.project_slug,
            data_type=storage_data_type_str,
        )

        # 5. Determine Status
        cscs_status = get_target_status_from_waldur_state(waldur_resource.state)

        # 6. Assemble the StorageResource
        return StorageResource(
            itemId=resource_uuid,
            status=cscs_status,
            mountPoint=MountPoint(default=mount_point_path),
            permission=Permission(value=waldur_resource.effective_permissions),
            quotas=quotas,
            target=target,
            storageSystem=StorageItem(
                itemId=generate_storage_system_target_id(storage_system),
                key=storage_system.lower(),
                name=storage_system.upper(),
            ),
            storageFileSystem=StorageItem(
                itemId=generate_storage_filesystem_target_id(
                    self.config.storage_file_system
                ),
                key=self.config.storage_file_system.lower(),
                name=self.config.storage_file_system.upper(),
            ),
            storageDataType=StorageItem(
                itemId=generate_storage_data_type_target_id(storage_data_type_str),
                key=storage_data_type_str.lower(),
                name=storage_data_type_str.upper(),
                path=storage_data_type_str.lower(),
            ),
            parentItemId=parent_item_id,
            extra_fields=waldur_resource.callback_urls,
        )

    async def _build_target_item(
        self, waldur_resource: ParsedWaldurResource, target_type: TargetType
    ) -> Optional[TargetItem]:
        """
        Construct the specific TargetItem subclass based on the TargetType.
        Handles checking backend_metadata first, then falling back to generation/lookup.
        """
        # 1. Check if the backend_metadata already contains the fully formed item
        # This supports cases where the backend might have pre-populated data
        if not self.config.use_mock_target_items:
            target_item_field = f"{target_type.value}_item"
            pre_existing_data = getattr(
                waldur_resource.backend_metadata, target_item_field, None
            )
            if pre_existing_data:
                return pre_existing_data

        # 2. Generate data based on type
        if target_type == TargetType.PROJECT:
            return await self._build_project_target(waldur_resource)

        elif target_type == TargetType.USER:
            return await self._build_user_target(waldur_resource)

        # Ideally, orchestration handles Tenant/Customer creation via HierarchyBuilder,
        # but if logic dictates mapping them here (e.g. for specialized resources),
        # we include fallback logic similar to the original backend.py
        elif target_type == TargetType.TENANT:
            return TenantTargetItem(
                itemId=generate_tenant_target_id(waldur_resource.provider_slug),
                key=waldur_resource.provider_slug.lower(),
                name=waldur_resource.provider_name,
            )

        elif target_type == TargetType.CUSTOMER:
            return CustomerTargetItem(
                itemId=generate_customer_target_id(waldur_resource.customer_slug),
                key=waldur_resource.customer_slug.lower(),
                name=waldur_resource.customer_name,
            )

        logger.warning("Unsupported target type: %s", target_type)
        return TargetItem(itemId="unknown")

    async def _build_project_target(
        self, waldur_resource: ParsedWaldurResource
    ) -> Optional[ProjectTargetItem]:
        """Build ProjectTargetItem with GID lookup."""
        target_status = get_target_status_from_waldur_state(waldur_resource.state)
        project_slug = waldur_resource.project_slug or "unknown"

        # Lookup GID
        unix_gid = await self.gid_service.get_project_unix_gid(project_slug)
        if unix_gid is None:
            # If GID lookup fails in production, we cannot provision this resource.
            return None

        return ProjectTargetItem(
            itemId=generate_project_target_id(waldur_resource.slug),
            # Key is typically null for Project items in this schema
            key=None,
            name=waldur_resource.slug,
            status=target_status,
            unixGid=unix_gid,
            active=target_status == TargetStatus.ACTIVE,
        )

    async def _build_user_target(
        self, waldur_resource: ParsedWaldurResource
    ) -> Optional[UserTargetItem]:
        """
        Build UserTargetItem.

        Note: The user logic in the original backend contained placeholders for
        UID and Email generation. This logic is preserved here.
        """
        target_status = get_target_status_from_waldur_state(waldur_resource.state)
        project_slug = waldur_resource.project_slug or "default-project"

        # Lookup Primary Project GID
        unix_gid = await self.gid_service.get_project_unix_gid(project_slug)
        if unix_gid is None:
            return None

        # Placeholder logic from original backend.py
        # TODO: Implement actual User UID lookup logic if an API becomes available
        # e.g. https://api-user.hpc-user.tds.cscs.ch/api/v1/export/cscs/users/{username}
        mock_uid = 20000 + hash(waldur_resource.slug) % 10000
        mock_email = f"user-{waldur_resource.slug}@example.com"

        return UserTargetItem(
            itemId=generate_user_target_id(waldur_resource.slug),
            key=None,
            name=None,
            status=target_status,
            email=mock_email,
            unixUid=mock_uid,
            primaryProject=UserPrimaryProject(
                name=project_slug,
                unixGid=unix_gid,
                active=target_status == TargetStatus.ACTIVE,
            ),
            active=target_status == TargetStatus.ACTIVE,
        )
