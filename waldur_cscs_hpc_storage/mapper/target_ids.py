from uuid import UUID
from uuid import NAMESPACE_OID, uuid5

from waldur_cscs_hpc_storage.models.enums import TargetIdScope


def _generate_scoped_id(scope: TargetIdScope, identifier: str) -> UUID:
    """Generate a deterministic UUID given a scope and identifier.

    Args:
        scope: Target ID scope
        identifier: Unique identifier string

    Returns:
        Deterministic UUID string
    """
    return uuid5(NAMESPACE_OID, f"{scope}:{identifier}")


def generate_storage_system_target_id(system_name: str) -> UUID:
    """Generate deterministic UUID for a storage system.

    Args:
        system_name: Name of the storage system

    Returns:
        Deterministic UUID string
    """
    return _generate_scoped_id(TargetIdScope.STORAGE_SYSTEM, system_name)


def generate_storage_filesystem_target_id(filesystem_name: str) -> UUID:
    """Generate deterministic UUID for a storage filesystem.

    Args:
        filesystem_name: Name of the storage filesystem

    Returns:
        Deterministic UUID string
    """
    return _generate_scoped_id(TargetIdScope.STORAGE_FILE_SYSTEM, filesystem_name)


def generate_storage_data_type_target_id(data_type: str) -> UUID:
    """Generate deterministic UUID for a storage data type.

    Args:
        data_type: Storage data type

    Returns:
        Deterministic UUID string
    """
    return _generate_scoped_id(TargetIdScope.STORAGE_DATA_TYPE, data_type)


def generate_tenant_resource_id(
    tenant_id: str, storage_system: str, data_type: str
) -> UUID:
    """Generate deterministic UUID for a tenant storage resource.

    Args:
        tenant_id: Tenant ID
        storage_system: Storage system name
        data_type: Storage data type

    Returns:
        Deterministic UUID string
    """
    # Tenant resource ID is a special case composition of scope-like parts
    return _generate_scoped_id(
        TargetIdScope.TENANT, f"{tenant_id}-{storage_system}-{data_type}"
    )


def generate_tenant_target_id(tenant_id: str) -> UUID:
    """Generate deterministic UUID for a tenant target.

    Args:
        tenant_id: Tenant ID (provider slug)

    Returns:
        Deterministic UUID string
    """
    return _generate_scoped_id(TargetIdScope.TENANT, tenant_id)


def generate_customer_target_id(customer_slug: str) -> UUID:
    """Generate deterministic UUID for a customer target.

    Args:
        customer_slug: Customer slug

    Returns:
        Deterministic UUID string
    """
    return _generate_scoped_id(TargetIdScope.CUSTOMER, customer_slug)


def generate_project_target_id(project_slug: str) -> UUID:
    """Generate deterministic UUID for a project target.

    Args:
        project_slug: Project slug

    Returns:
        Deterministic UUID string
    """
    return _generate_scoped_id(TargetIdScope.PROJECT, project_slug)


def generate_user_target_id(user_slug: str) -> UUID:
    """Generate deterministic UUID for a user target.

    Args:
        user_slug: User slug

    Returns:
        Deterministic UUID string
    """
    return _generate_scoped_id(TargetIdScope.USER, user_slug)
