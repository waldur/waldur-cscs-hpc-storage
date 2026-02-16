from uuid import UUID
from typing import Optional, Annotated
from pydantic import BaseModel, Field, field_validator, BeforeValidator

from waldur_api_client.models.order_details import OrderDetails
from waldur_api_client.models.order_state import OrderState
from waldur_api_client.models.resource import Resource
from waldur_api_client.models.resource_state import ResourceState
from waldur_api_client.types import Unset

# Re-importing Enums from your existing structure to ensure compatibility
from waldur_cscs_hpc_storage.models.enums import (
    StorageDataType,
    StorageSystem,
    TargetStatus,
)
from waldur_cscs_hpc_storage.models.domain import (
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
        default=0, ge=0, description="Storage limit in Terabytes"
    )

    model_config = {"extra": "ignore"}


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

    model_config = {"extra": "ignore"}


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
    uuid: UUID
    name: str = ""
    slug: str = ""
    state: ResourceState = ""

    # Hierarchy info
    offering_uuid: UUID
    offering_name: str = ""
    offering_slug: str = ""
    project_uuid: UUID
    project_name: str = ""
    project_slug: str = ""
    customer_uuid: UUID
    customer_name: str = ""
    customer_slug: str = ""
    provider_slug: str = ""
    provider_name: str = ""

    limits: ResourceLimits = Field(default_factory=ResourceLimits)
    attributes: ResourceAttributes = Field(default_factory=ResourceAttributes)
    options: ResourceOptions = Field(default_factory=ResourceOptions)
    backend_metadata: ResourceBackendMetadata = Field(
        default_factory=ResourceBackendMetadata
    )

    # Optional order info
    order_in_progress: Optional[OrderDetails] = None

    model_config = {"arbitrary_types_allowed": True}

    @classmethod
    def from_waldur_resource(cls, resource: Resource) -> "ParsedWaldurResource":
        state = resource.state
        if (
            state in [ResourceState.UPDATING, ResourceState.TERMINATING]
            and resource.order_in_progress
            and resource.order_in_progress.state == OrderState.PENDING_CONSUMER
        ):
            state = ResourceState.OK

        return cls(
            uuid=resource.uuid.hex,
            name=resource.name,
            slug=resource.slug,
            state=state,
            offering_uuid=resource.offering_uuid.hex,
            offering_name=resource.offering_name,
            offering_slug=resource.offering_slug,
            project_uuid=resource.project_uuid.hex,
            project_name=resource.project_name,
            project_slug=resource.project_slug,
            customer_uuid=resource.customer_uuid.hex,
            customer_name=resource.customer_name,
            customer_slug=resource.customer_slug,
            provider_slug=(
                resource.provider_slug
                if hasattr(resource, "provider_slug")
                and not isinstance(resource.provider_slug, Unset)
                else ""
            ),
            provider_name=(
                resource.provider_name
                if hasattr(resource, "provider_name")
                and not isinstance(resource.provider_name, Unset)
                else ""
            ),
            limits=resource.limits
            and ResourceLimits(**resource.limits.additional_properties)
            or ResourceLimits(),
            attributes=resource.attributes
            and ResourceAttributes(**resource.attributes.additional_properties)
            or ResourceAttributes(),
            options=resource.options
            and ResourceOptions(**resource.options)
            or ResourceOptions(),
            backend_metadata=resource.backend_metadata
            and ResourceBackendMetadata(
                **resource.backend_metadata.additional_properties
            )
            or ResourceBackendMetadata(),
            order_in_progress=resource.order_in_progress,
        )

    @property
    def effective_permissions(self) -> str:
        """Logic extracted from _extract_permissions"""
        return self.options.permissions or self.attributes.permissions

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

        # Order-level actions (approve, reject, set_state_done, set_state_erred)
        order_actions = set()

        if order_state == OrderState.PENDING_PROVIDER:
            order_actions.update(
                [
                    "approve_by_provider",
                    "reject_by_provider",
                    "set_state_done",
                ]
            )

        if order_state == OrderState.EXECUTING:
            order_actions.update(["set_state_done", "set_state_erred"])

        base = order_url.rstrip("/")
        urls = {f"{action}_url": f"{base}/{action}/" for action in order_actions}

        # Resource-level actions (set_backend_id, update_options_direct)
        if "/marketplace-orders/" in order_url:
            api_root = order_url.split("/marketplace-orders/")[0]
            resource_base = f"{api_root}/marketplace-provider-resources/{self.uuid}"
            urls["set_backend_id_url"] = f"{resource_base}/set_backend_id/"
            if order_state == OrderState.PENDING_PROVIDER:
                urls["update_resource_options_url"] = (
                    f"{resource_base}/update_options_direct/"
                )
            if self.state in [
                ResourceState.ERRED,
                ResourceState.CREATING,
                ResourceState.UPDATING,
                ResourceState.TERMINATING,
            ]:
                urls["set_state_ok_url"] = f"{resource_base}/set_state_ok/"

        return urls


class StorageResourceFilter(BaseModel):
    """Filter parameters for storage resources endpoint."""

    storage_system: Annotated[
        Optional[StorageSystem],
        Field(description="Storage system filter"),
    ] = None
    state: Optional[ResourceState] = None
    page: Annotated[int, Field(ge=1, description="Page number (starts from 1)")] = 1
    page_size: Annotated[
        int, Field(ge=1, le=500, description="Number of items per page")
    ] = 100
    data_type: Annotated[
        Optional[StorageDataType],
        Field(description="Data type filter"),
    ] = None
    status: Annotated[
        Optional[TargetStatus],
        Field(description="Status filter"),
    ] = None
