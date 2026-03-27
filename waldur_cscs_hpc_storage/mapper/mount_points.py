from posixpath import dirname


def derive_parent_mount_points(backend_id: str) -> tuple[str, str]:
    """Derive tenant and customer mount point paths from a backend_id.

    Splits the backend_id path to extract parent (customer) and grandparent (tenant) paths.

    Args:
        backend_id: The full backend path (e.g., '/scratch/mch/msclim')

    Returns:
        Tuple of (tenant_path, customer_path)
        e.g. ('/scratch', '/scratch/mch')
    """
    customer_path = dirname(backend_id.rstrip("/"))
    tenant_path = dirname(customer_path)
    return tenant_path, customer_path


def generate_tenant_mount_point(
    storage_system: str,
    data_type: str,
    tenant_id: str,
) -> str:
    """Generate mount point path for tenant-level entry.

    Args:
        storage_system: Storage system name
        data_type: Storage data type
        tenant_id: Tenant ID (provider slug)

    Returns:
        Mount point path string
    """
    return f"/{storage_system}/{data_type}/{tenant_id}"


def generate_customer_mount_point(
    storage_system: str,
    data_type: str,
    tenant_id: str,
    customer: str,
) -> str:
    """Generate mount point path for customer-level entry.

    Args:
        storage_system: Storage system name
        data_type: Storage data type
        tenant_id: Tenant ID (provider slug)
        customer: Customer slug

    Returns:
        Mount point path string
    """
    return f"/{storage_system}/{data_type}/{tenant_id}/{customer}"


def generate_project_mount_point(
    storage_system: str,
    data_type: str,
    tenant_id: str,
    customer: str,
    project_id: str,
) -> str:
    """Generate mount point path based on hierarchy and storage data type.

    Args:
        storage_system: Storage system name
        data_type: Storage data type
        tenant_id: Tenant ID (provider slug)
        customer: Customer slug
        project_id: Project slug

    Returns:
        Mount point path string
    """
    return f"/{storage_system}/{data_type}/{tenant_id}/{customer}/{project_id}"
