from typing import Any, Optional, Annotated
from pydantic import BaseModel, Field, field_validator, BeforeValidator

from waldur_api_client.models.resource_state import ResourceState
from waldur_api_client.models.order_state import OrderState

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

    storage: Optional[int] = Field(
        default=0, ge=0, description="Storage limit in Terabytes"
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
        return cls(
            uuid=resource.uuid.hex,
            name=resource.name,
            slug=resource.slug,
            state=resource.state,
            offering_uuid=resource.offering_uuid.hex,
            offering_name=resource.offering_name,
            offering_slug=resource.offering_slug,
            project_uuid=resource.project_uuid.hex,
            project_name=resource.project_name,
            project_slug=resource.project_slug,
            customer_uuid=resource.customer_uuid.hex,
            customer_name=resource.customer_name,
            customer_slug=resource.customer_slug,
            limits=ResourceLimits(**resource.limits.additional_properties),
            attributes=ResourceAttributes(**resource.attributes.additional_properties),
            options=ResourceOptions(**resource.options.additional_properties),
            backend_metadata=ResourceBackendMetadata(
                **resource.backend_metadata.additional_properties
            ),
            order_in_progress=resource.order_in_progress,
        )

    @property
    def effective_permissions(self) -> str:
        """Logic extracted from _extract_permissions"""
        return self.options.permissions or self.attributes.permissions

    def get_effective_storage_quotas(self) -> tuple[float, float]:
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

    @property
    def callback_urls(self) -> dict[str, str]:
        """
        Get callback URLs for the given Waldur resource.
        """
        try:
            order = self.order_in_progress
            if not order:
                return {}

            order_state = getattr(order, "state", None)
            order_url = getattr(order, "url", None)

            if not order_url:
                return {}

        except AttributeError:
            return {}

        allowed_actions = set()

        if order_state == OrderState.PENDING_PROVIDER:
            allowed_actions.update(["approve_by_provider", "reject_by_provider"])

        if order_state == OrderState.EXECUTING:
            allowed_actions.update(["set_state_done", "set_state_erred"])

        if order_state == OrderState.DONE:
            allowed_actions.add("set_backend_id")

        base = order_url.rstrip("/")
        return {f"{action}_url": f"{base}/{action}/" for action in allowed_actions}
