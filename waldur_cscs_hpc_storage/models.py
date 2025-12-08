import dataclasses
from typing import Any, Optional, Union

from waldur_cscs_hpc_storage.enums import TargetStatus


@dataclasses.dataclass
class Permission:
    """Represents a permission settings."""

    value: str
    permissionType: str = "octal"

    def to_dict(self) -> dict[str, Any]:
        return dataclasses.asdict(self)


@dataclasses.dataclass
class Quota:
    """Represents a storage quota."""

    type: str
    quota: float
    unit: str
    enforcementType: str

    def to_dict(self) -> dict[str, Any]:
        return dataclasses.asdict(self)


@dataclasses.dataclass
class StorageItem:
    """Represents a storage-related item (system, filesystem, or data type)."""

    itemId: str
    key: str
    name: str
    active: bool = True
    path: str = ""  # Optional path field (used for data type)

    def to_dict(self) -> dict[str, Any]:
        return dataclasses.asdict(self)


@dataclasses.dataclass
class TargetItem:
    """Base class for target items."""

    itemId: str
    key: Optional[str] = None
    name: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        data = dataclasses.asdict(self)
        return {k: v for k, v in data.items() if v is not None}


@dataclasses.dataclass
class TenantTargetItem(TargetItem):
    """Target item for a tenant."""

    pass


@dataclasses.dataclass
class CustomerTargetItem(TargetItem):
    """Target item for a customer."""

    pass


@dataclasses.dataclass
class ProjectTargetItem(TargetItem):
    """Target item for a project."""

    status: Optional[TargetStatus] = None
    unixGid: Optional[int] = None
    active: Optional[bool] = None


@dataclasses.dataclass
class UserPrimaryProject:
    """Primary project information for a user."""

    name: str
    unixGid: int
    active: bool


@dataclasses.dataclass
class UserTargetItem(TargetItem):
    """Target item for a user."""

    status: Optional[TargetStatus] = None
    email: Optional[str] = None
    unixUid: Optional[int] = None
    primaryProject: Optional[UserPrimaryProject] = None
    active: Optional[bool] = None

    def to_dict(self) -> dict[str, Any]:
        data = super().to_dict()
        if self.primaryProject:
            data["primaryProject"] = dataclasses.asdict(self.primaryProject)
        return data


@dataclasses.dataclass
class Target:
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

    def to_dict(self) -> dict[str, Any]:
        return {
            "targetType": self.targetType,
            "targetItem": self.targetItem.to_dict()
            if hasattr(self.targetItem, "to_dict")
            else self.targetItem,
        }


@dataclasses.dataclass
class MountPoint:
    default: str

    def to_dict(self) -> dict[str, Any]:
        return dataclasses.asdict(self)


@dataclasses.dataclass
class StorageResource:
    """Main storage resource class."""

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

    # Additional fields that might be added dynamically like URLs
    extra_fields: dict[str, Any] = dataclasses.field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        data = dataclasses.asdict(self)
        # Flatten extra_fields into the main dict
        if self.extra_fields:
            data.update(self.extra_fields)
        del data["extra_fields"]

        # Handle nested objects that might need custom serialization
        if self.quotas:
            data["quotas"] = [q.to_dict() for q in self.quotas]

        # Ensure target is serialized correctly if it hasn't been handled automatically by asdict
        # (dataclasses.asdict usually handles recursive dataclasses, but for Union types or custom methods we might need care)
        # Actually asdict works recursively for dataclasses.
        # But for TargetItem subclasses we might want to ensure None values are stripped if that's the desired behavior.
        # We implemented to_dict on TargetItem for that reason, but asdict calls to_dict? No, it doesn't.
        # So we should probably override serialization for fields that have objects with custom to_dict.

        # Let's rely on manual construction for cleaner control or just trust asdict if we didn't have custom logic.
        # Since I added to_dict to TargetItem to strip Nones, I should use it.

        data["target"] = self.target.to_dict()

        return data
