"""Base package containing core modules for waldur_cscs_hpc_storage."""

from waldur_cscs_hpc_storage.models.enums import (
    EnforcementType,
    QuotaType,
    QuotaUnit,
    StorageDataType,
    StorageSystem,
    StrEnum,
    TargetIdScope,
    TargetStatus,
    TargetType,
)
from waldur_cscs_hpc_storage.base.mappers import (
    DATA_TYPE_TO_TARGET_MAPPING,
    TARGET_STATUS_MAPPING,
    get_target_status_from_waldur_state,
    get_target_type_from_data_type,
)
from waldur_cscs_hpc_storage.models import (
    CustomerTargetItem,
    MountPoint,
    Permission,
    ProjectTargetItem,
    Quota,
    StorageItem,
    StorageResource,
    Target,
    TargetItem,
    TenantTargetItem,
    UserPrimaryProject,
    UserTargetItem,
)
from waldur_cscs_hpc_storage.base.mount_points import (
    generate_customer_mount_point,
    generate_project_mount_point,
    generate_tenant_mount_point,
)
from waldur_cscs_hpc_storage.models import (
    LooseInt,
    ParsedWaldurResource,
    ResourceAttributes,
    ResourceBackendMetadata,
    ResourceLimits,
    ResourceOptions,
)
from waldur_cscs_hpc_storage.base.target_ids import (
    generate_customer_target_id,
    generate_project_target_id,
    generate_storage_data_type_target_id,
    generate_storage_filesystem_target_id,
    generate_storage_system_target_id,
    generate_tenant_resource_id,
    generate_tenant_target_id,
    generate_user_target_id,
)

__all__ = [
    # enums
    "EnforcementType",
    "QuotaType",
    "QuotaUnit",
    "StorageDataType",
    "StorageSystem",
    "StrEnum",
    "TargetIdScope",
    "TargetStatus",
    "TargetType",
    # mappers
    "DATA_TYPE_TO_TARGET_MAPPING",
    "TARGET_STATUS_MAPPING",
    "get_target_status_from_waldur_state",
    "get_target_type_from_data_type",
    # models
    "CustomerTargetItem",
    "MountPoint",
    "Permission",
    "ProjectTargetItem",
    "Quota",
    "StorageItem",
    "StorageResource",
    "Target",
    "TargetItem",
    "TenantTargetItem",
    "UserPrimaryProject",
    "UserTargetItem",
    # mount_points
    "generate_customer_mount_point",
    "generate_project_mount_point",
    "generate_tenant_mount_point",
    # schemas
    "LooseInt",
    "ParsedWaldurResource",
    "ResourceAttributes",
    "ResourceBackendMetadata",
    "ResourceLimits",
    "ResourceOptions",
    # target_ids
    "generate_customer_target_id",
    "generate_project_target_id",
    "generate_storage_data_type_target_id",
    "generate_storage_filesystem_target_id",
    "generate_storage_system_target_id",
    "generate_tenant_resource_id",
    "generate_tenant_target_id",
    "generate_user_target_id",
]
