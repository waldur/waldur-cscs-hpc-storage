from typing import Optional, Annotated
from pydantic import BaseModel, Field, field_validator, BeforeValidator

# Re-importing Enums from your existing structure to ensure compatibility
from waldur_cscs_hpc_storage.enums import StorageDataType
from waldur_cscs_hpc_storage.models import (
    TenantTargetItem,
    CustomerTargetItem,
    ProjectTargetItem,
    UserTargetItem,
)


# Helper for loose numeric parsing (handles "100.0" string for ints)
def loose_int(v):
    if v is None:
        return None
    try:
        return int(float(v))
    except (ValueError, TypeError):
        raise ValueError(f"Invalid integer value: {v}")


LooseInt = Annotated[Optional[int], BeforeValidator(loose_int)]


class ResourceLimits(BaseModel):
    """
    Validates the 'limits' field from WaldurResource.
    """

    storage: Optional[float] = Field(
        default=0.0, ge=0, description="Storage limit in Terabytes"
    )


class ResourceAttributes(BaseModel):
    """
    Validates the 'attributes' field from WaldurResource.
    """

    storage_data_type: StorageDataType = Field(
        default=StorageDataType.STORE,
        description="Type of storage (store, scratch, etc.)",
    )
    permissions: str = Field(
        default="775",
        pattern=r"^[0-7]{3,4}$",
        description="Unix permission string (e.g., 775)",
    )

    @field_validator("storage_data_type", mode="before")
    @classmethod
    def validate_data_type(cls, v):
        # Handle cases where the API might send None or empty string
        if not v:
            return StorageDataType.STORE
        try:
            StorageDataType(v)
            return v
        except ValueError:
            return StorageDataType.STORE


class ResourceOptions(BaseModel):
    """
    Validates the 'options' field from WaldurResource.
    Used for administrative overrides of quotas and permissions.
    """

    soft_quota_space: Optional[float] = Field(
        None, ge=0, description="Override soft storage quota (TB)"
    )
    hard_quota_space: Optional[float] = Field(
        None, ge=0, description="Override hard storage quota (TB)"
    )

    # Using LooseInt to handle cases where API sends numbers as strings "10000.0"
    soft_quota_inodes: LooseInt = Field(
        None, ge=0, description="Override soft inode quota"
    )
    hard_quota_inodes: LooseInt = Field(
        None, ge=0, description="Override hard inode quota"
    )

    permissions: Optional[str] = Field(
        None, pattern=r"^[0-7]{3,4}$", description="Override permissions"
    )


class ResourceBackendMetadata(BaseModel):
    """
    Validates the 'backend_metadata' field from WaldurResource.
    Contains pre-validated target items if the backend populated them.
    """

    tenant_item: Optional[TenantTargetItem] = None
    customer_item: Optional[CustomerTargetItem] = None
    project_item: Optional[ProjectTargetItem] = None
    user_item: Optional[UserTargetItem] = None

    model_config = {"arbitrary_types_allowed": True}


class ParsedWaldurResource(BaseModel):
    """
    A composite class to hold the strictly typed configurations
    extracted from the loose WaldurResource dicts.
    """

    limits: ResourceLimits = Field(default_factory=ResourceLimits)
    attributes: ResourceAttributes = Field(default_factory=ResourceAttributes)
    options: ResourceOptions = Field(default_factory=ResourceOptions)
    backend_metadata: ResourceBackendMetadata = Field(
        default_factory=ResourceBackendMetadata
    )

    @property
    def effective_permissions(self) -> str:
        """Logic extracted from _extract_permissions"""
        return self.options.permissions or self.attributes.permissions

    def get_effective_storage_quotas(self) -> tuple[float, float]:
        """
        Returns (soft_tb, hard_tb).
        Logic extracted from _calculate_quotas overrides.
        """
        limit = self.limits.storage or 0.0

        soft = (
            self.options.soft_quota_space
            if self.options.soft_quota_space is not None
            else limit
        )
        hard = (
            self.options.hard_quota_space
            if self.options.hard_quota_space is not None
            else limit
        )

        return soft, hard

    def get_effective_inode_quotas(
        self, base_soft: int, base_hard: int
    ) -> tuple[int, int]:
        """
        Returns (soft_inode, hard_inode).
        Logic extracted from _calculate_quotas overrides.
        """
        soft = (
            self.options.soft_quota_inodes
            if self.options.soft_quota_inodes is not None
            else base_soft
        )
        hard = (
            self.options.hard_quota_inodes
            if self.options.hard_quota_inodes is not None
            else base_hard
        )

        return soft, hard
