"""HierarchyBuilder: Manages the structural hierarchy (Tenants, Customers) for storage resources."""

import logging
from typing import Optional

from waldur_cscs_hpc_storage.base.enums import TargetStatus, TargetType
from waldur_cscs_hpc_storage.base.models import (
    CustomerTargetItem,
    MountPoint,
    Permission,
    StorageItem,
    StorageResource,
    Target,
    TenantTargetItem,
)
from waldur_cscs_hpc_storage.base.mount_points import (
    generate_customer_mount_point,
    generate_tenant_mount_point,
)
from waldur_cscs_hpc_storage.base.target_ids import (
    generate_storage_data_type_target_id,
    generate_storage_filesystem_target_id,
    generate_storage_system_target_id,
    generate_tenant_resource_id,
    generate_tenant_target_id,
)

logger = logging.getLogger(__name__)


class HierarchyBuilder:
    """Builds and manages the structural hierarchy (Tenants, Customers) for storage resources.

    This class encapsulates the logic for creating and tracking tenant and customer-level
    entries in the storage resource hierarchy. It ensures that:
    - Each unique tenant/storage_system/data_type combination gets one entry
    - Each unique customer/storage_system/data_type combination gets one entry
    - Customer entries are linked to their parent tenant entries
    - Project resources can be linked to their parent customer entries
    """

    def __init__(self, storage_file_system: str):
        """Initialize with the storage file system name.

        Args:
            storage_file_system: The storage file system identifier (e.g., 'GPFS')
        """
        self._tenant_entries: dict[str, str] = {}  # tenant_key -> itemId
        self._customer_entries: dict[str, str] = {}  # customer_key -> itemId
        self._hierarchy_resources: list[StorageResource] = []
        self._storage_file_system = storage_file_system

    def _build_tenant_key(
        self, tenant_id: str, storage_system: str, storage_data_type: str
    ) -> str:
        """Build a unique key for tenant entry deduplication."""
        return f"{tenant_id}-{storage_system}-{storage_data_type}"

    def _build_customer_key(
        self, customer_slug: str, storage_system: str, storage_data_type: str
    ) -> str:
        """Build a unique key for customer entry deduplication."""
        return f"{customer_slug}-{storage_system}-{storage_data_type}"

    def get_or_create_tenant(
        self,
        tenant_id: str,
        tenant_name: str,
        storage_system: str,
        storage_data_type: str,
        offering_uuid: Optional[str] = None,
        active: bool = False,
    ) -> str:
        """Create or retrieve a tenant entry.

        If a tenant entry for this combination already exists, returns its itemId.
        Otherwise, creates a new tenant resource and adds it to the hierarchy.

        Args:
            tenant_id: The tenant identifier (e.g., provider_slug)
            tenant_name: Human-readable tenant name
            storage_system: Storage system name (e.g., 'capstor')
            storage_data_type: Data type (e.g., 'store', 'scratch')
            offering_uuid: Optional offering UUID to use as the itemId
            active: Whether the tenant should be marked as active

        Returns:
            The itemId of the tenant entry
        """
        tenant_key = self._build_tenant_key(
            tenant_id, storage_system, storage_data_type
        )

        if tenant_key in self._tenant_entries:
            logger.debug("Tenant entry already exists for %s", tenant_key)
            return self._tenant_entries[tenant_key]

        # Generate mount point for tenant
        mount_point = generate_tenant_mount_point(
            storage_system=storage_system,
            tenant_id=tenant_id,
            data_type=storage_data_type,
        )

        # Use offering UUID if provided, otherwise generate deterministic UUID
        tenant_item_id = (
            offering_uuid
            if offering_uuid
            else generate_tenant_resource_id(
                tenant_id, storage_system, storage_data_type
            )
        )

        status = TargetStatus.ACTIVE if active else TargetStatus.PENDING

        tenant_resource = StorageResource(
            itemId=tenant_item_id,
            status=status,
            mountPoint=MountPoint(default=mount_point),
            permission=Permission(value="775"),
            quotas=None,
            target=Target(
                targetType=TargetType.TENANT,
                targetItem=TenantTargetItem(
                    itemId=offering_uuid
                    if offering_uuid
                    else generate_tenant_target_id(tenant_id),
                    key=tenant_id.lower(),
                    name=tenant_name,
                ),
            ),
            storageSystem=StorageItem(
                itemId=generate_storage_system_target_id(storage_system),
                key=storage_system.lower(),
                name=storage_system.upper(),
            ),
            storageFileSystem=StorageItem(
                itemId=generate_storage_filesystem_target_id(self._storage_file_system),
                key=self._storage_file_system.lower(),
                name=self._storage_file_system.upper(),
            ),
            storageDataType=StorageItem(
                itemId=generate_storage_data_type_target_id(storage_data_type),
                key=storage_data_type.lower(),
                name=storage_data_type.upper(),
                path=storage_data_type.lower(),
            ),
            parentItemId=None,
        )

        self._hierarchy_resources.append(tenant_resource)
        self._tenant_entries[tenant_key] = tenant_item_id
        logger.debug(
            "Created tenant entry for %s with itemId %s", tenant_key, tenant_item_id
        )

        return tenant_item_id

    def get_or_create_customer(
        self,
        customer_info: dict,
        storage_system: str,
        storage_data_type: str,
        tenant_id: str,
        active: bool = False,
    ) -> Optional[str]:
        """Create or retrieve a customer entry.

        If a customer entry for this combination already exists, returns its itemId.
        Otherwise, creates a new customer resource linked to the parent tenant.

        Args:
            customer_info: Dictionary with 'itemId', 'key', and 'name' fields
            storage_system: Storage system name
            storage_data_type: Data type
            tenant_id: The parent tenant identifier
            active: Whether the customer should be marked as active

        Returns:
            The itemId of the customer entry, or None if customer_info is invalid
        """
        customer_slug = customer_info.get("key")
        if not customer_slug:
            logger.warning("Customer info missing 'key' field: %s", customer_info)
            return None

        customer_key = self._build_customer_key(
            customer_slug, storage_system, storage_data_type
        )

        if customer_key in self._customer_entries:
            logger.debug("Customer entry already exists for %s", customer_key)
            return self._customer_entries[customer_key]

        # Get parent tenant ID
        tenant_key = self._build_tenant_key(
            tenant_id, storage_system, storage_data_type
        )
        parent_tenant_id = self._tenant_entries.get(tenant_key)

        # Generate mount point for customer
        mount_point = generate_customer_mount_point(
            storage_system=storage_system,
            tenant_id=tenant_id,
            customer=customer_slug,
            data_type=storage_data_type,
        )

        customer_item_id = customer_info.get("itemId", "")
        status = TargetStatus.ACTIVE if active else TargetStatus.PENDING

        customer_resource = StorageResource(
            itemId=customer_item_id,
            status=status,
            mountPoint=MountPoint(default=mount_point),
            permission=Permission(value="775"),
            quotas=None,
            target=Target(
                targetType=TargetType.CUSTOMER,
                targetItem=CustomerTargetItem(
                    itemId=customer_item_id,
                    key=customer_slug,
                    name=customer_info.get("name", ""),
                ),
            ),
            storageSystem=StorageItem(
                itemId=generate_storage_system_target_id(storage_system),
                key=storage_system.lower(),
                name=storage_system.upper(),
            ),
            storageFileSystem=StorageItem(
                itemId=generate_storage_filesystem_target_id(self._storage_file_system),
                key=self._storage_file_system.lower(),
                name=self._storage_file_system.upper(),
            ),
            storageDataType=StorageItem(
                itemId=generate_storage_data_type_target_id(storage_data_type),
                key=storage_data_type.lower(),
                name=storage_data_type.upper(),
                path=storage_data_type.lower(),
            ),
            parentItemId=parent_tenant_id,
        )

        self._hierarchy_resources.append(customer_resource)
        self._customer_entries[customer_key] = customer_item_id
        logger.debug(
            "Created customer entry for %s with parent tenant %s",
            customer_key,
            parent_tenant_id,
        )

        return customer_item_id

    def get_customer_uuid(
        self,
        customer_slug: str,
        storage_system: str,
        storage_data_type: str,
    ) -> Optional[str]:
        """Get the itemId for an existing customer entry.

        Args:
            customer_slug: The customer slug
            storage_system: Storage system name
            storage_data_type: Data type

        Returns:
            The itemId if customer entry exists, None otherwise
        """
        customer_key = self._build_customer_key(
            customer_slug, storage_system, storage_data_type
        )
        return self._customer_entries.get(customer_key)

    def assign_parent_to_project(
        self,
        project_resource: StorageResource,
        customer_slug: str,
        storage_system: str,
        storage_data_type: str,
    ) -> None:
        """Assign parentItemId to a project resource if the customer entry exists.

        Args:
            project_resource: The project-level StorageResource to update
            customer_slug: The customer slug
            storage_system: Storage system name
            storage_data_type: Data type
        """
        customer_id = self.get_customer_uuid(
            customer_slug, storage_system, storage_data_type
        )
        if customer_id:
            project_resource.parentItemId = customer_id
            logger.debug(
                "Assigned parentItemId %s to project resource %s",
                customer_id,
                project_resource.itemId,
            )

    def get_hierarchy_resources(self) -> list[StorageResource]:
        """Get all tenant and customer resources created by this builder.

        Returns:
            List of StorageResource objects for tenants and customers
        """
        return list(self._hierarchy_resources)

    def reset(self) -> None:
        """Reset the builder state, clearing all tracked entries."""
        self._tenant_entries.clear()
        self._customer_entries.clear()
        self._hierarchy_resources.clear()
        logger.debug("HierarchyBuilder state reset")
