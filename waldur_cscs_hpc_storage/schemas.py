from typing import Any, Optional, Annotated
from pydantic import BaseModel, Field, field_validator, BeforeValidator

from waldur_api_client.models.resource_state import ResourceState

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

    # Identity & metadata
    uuid: str
    name: str = ""
    slug: str = ""
    state: ResourceState = ""

    # Hierarchy info
    offering_uuid: str
    offering_name: str = ""
    offering_slug: str = ""
    project_uuid: str
    project_name: str = ""
    project_slug: str = ""
    customer_uuid: str
    customer_name: str = ""
    customer_slug: str = ""

    limits: ResourceLimits = Field(default_factory=ResourceLimits)
    attributes: ResourceAttributes = Field(default_factory=ResourceAttributes)
    options: ResourceOptions = Field(default_factory=ResourceOptions)
    backend_metadata: ResourceBackendMetadata = Field(
        default_factory=ResourceBackendMetadata
    )

    # Optional order info
    order_in_progress: Optional[Any] = None

    @classmethod
    def from_waldur_resource(cls, resource: Any) -> "ParsedWaldurResource":
        # Extract raw dictionaries, handling Unset/None types from the API client
        raw_limits: dict[str, Any] = getattr(
            resource.limits, "additional_properties", {}
        )
        if not isinstance(raw_limits, dict):
            raw_limits = {}

        raw_attributes: dict[str, Any] = getattr(
            resource.attributes, "additional_properties", {}
        )
        if not isinstance(raw_attributes, dict):
            raw_attributes = {}

        raw_options: dict[str, Any] = resource.options or {}
        if not isinstance(raw_options, dict):
            raw_options = {}

        # safely handle backend_metadata
        raw_metadata: dict[str, Any] = {}
        if resource.backend_metadata:
            metadata_props = getattr(
                resource.backend_metadata, "additional_properties", {}
            )
            if isinstance(metadata_props, dict):
                raw_metadata = metadata_props

        return cls(
            uuid=getattr(resource.uuid, "hex", "") or str(resource.uuid),
            name=resource.name or "",
            slug=resource.slug or "",
            state=resource.state or "",
            offering_uuid=getattr(resource.offering_uuid, "hex", "")
            or str(resource.offering_uuid),
            offering_name=resource.offering_name or "",
            offering_slug=resource.offering_slug or "",
            project_uuid=getattr(resource.project_uuid, "hex", "")
            or str(resource.project_uuid),
            project_name=resource.project_name or "",
            project_slug=resource.project_slug or "",
            customer_uuid=getattr(resource.customer_uuid, "hex", "")
            or str(resource.customer_uuid),
            customer_name=resource.customer_name or "",
            customer_slug=resource.customer_slug or "",
            limits=ResourceLimits(**raw_limits),
            attributes=ResourceAttributes(**raw_attributes),
            options=ResourceOptions(**raw_options),
            backend_metadata=ResourceBackendMetadata(**raw_metadata),
            order_in_progress=resource.order_in_progress,
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
