from typing import Optional, Union

from pydantic import BaseModel, ConfigDict

from waldur_cscs_hpc_storage.base.enums import (
    EnforcementType,
    QuotaType,
    QuotaUnit,
    TargetStatus,
)


class Permission(BaseModel):
    """Represents a permission settings."""

    value: str
    permissionType: str = "octal"


class Quota(BaseModel):
    """Represents a storage quota."""

    type: QuotaType
    quota: float
    unit: QuotaUnit
    enforcementType: EnforcementType


class StorageItem(BaseModel):
    """Represents a storage-related item (system, filesystem, or data type)."""

    itemId: str
    key: str
    name: str
    active: bool = True
    path: str = ""  # Optional path field (used for data type)


class TargetItem(BaseModel):
    """Base class for target items."""

    itemId: str
    key: Optional[str] = None
    name: Optional[str] = None


class TenantTargetItem(TargetItem):
    """Target item for a tenant."""

    pass


class CustomerTargetItem(TargetItem):
    """Target item for a customer."""

    pass


class ProjectTargetItem(TargetItem):
    """Target item for a project."""

    status: Optional[TargetStatus] = None
    unixGid: Optional[int] = None
    active: Optional[bool] = None


class UserPrimaryProject(BaseModel):
    """Primary project information for a user."""

    name: str
    unixGid: int
    active: bool


class UserTargetItem(TargetItem):
    """Target item for a user."""

    status: Optional[TargetStatus] = None
    email: Optional[str] = None
    unixUid: Optional[int] = None
    primaryProject: Optional[UserPrimaryProject] = None
    active: Optional[bool] = None


class Target(BaseModel):
    """Wrapper for target item and type."""

    targetType: str
    targetItem: Union[
        TargetItem,
        TenantTargetItem,
        CustomerTargetItem,
        ProjectTargetItem,
        UserTargetItem,
        dict,
    ]


class MountPoint(BaseModel):
    default: str


class StorageResource(BaseModel):
    """Main storage resource class."""

    model_config = ConfigDict(extra="allow")

    itemId: str
    status: TargetStatus
    mountPoint: MountPoint
    permission: Permission
    storageSystem: StorageItem
    storageFileSystem: StorageItem
    storageDataType: StorageItem
    target: Target
    quotas: Optional[list[Quota]] = None
    parentItemId: Optional[str] = None
