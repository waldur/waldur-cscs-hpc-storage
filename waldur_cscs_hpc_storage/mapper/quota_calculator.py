import logging
from typing import Optional, List, Tuple

from waldur_api_client.models.request_types import RequestTypes

from waldur_cscs_hpc_storage.models.enums import (
    EnforcementType,
    QuotaType,
    QuotaUnit,
)
from waldur_cscs_hpc_storage.models import Quota
from waldur_cscs_hpc_storage.models import (
    ParsedWaldurResource,
    ResourceLimits,
    ResourceOptions,
)
from waldur_cscs_hpc_storage.config import BackendConfig

logger = logging.getLogger(__name__)


class QuotaCalculator:
    """
    Service responsible for calculating storage resource quotas.
    """

    def __init__(self, config: BackendConfig):
        self.config = config

    def calculate_quotas(
        self,
        resource: ParsedWaldurResource,
        override_limits: Optional[ResourceLimits] = None,
        override_options: Optional[ResourceOptions] = None,
    ) -> Optional[List[Quota]]:
        """
        Calculate and render quota objects based on resource limits and options.
        """
        limits = override_limits if override_limits is not None else resource.limits
        options = override_options if override_options is not None else resource.options

        # Get storage limit
        storage_limit = limits.storage or 0.0

        # Calculate effective storage quotas (with option overrides)
        storage_quota_soft_tb = storage_limit
        storage_quota_hard_tb = (
            options.hard_quota_space
            if options.hard_quota_space is not None
            else storage_limit
        )

        # Calculate base inode quotas
        base_inodes = storage_limit * self.config.inode_base_multiplier
        base_soft_inode = int(base_inodes * self.config.inode_soft_coefficient)
        base_hard_inode = int(base_inodes * self.config.inode_hard_coefficient)

        # Calculate effective inode quotas (with option overrides)
        inode_soft = (
            options.soft_quota_inodes
            if options.soft_quota_inodes is not None
            else base_soft_inode
        )
        inode_hard = (
            options.hard_quota_inodes
            if options.hard_quota_inodes is not None
            else base_hard_inode
        )

        if storage_quota_soft_tb <= 0 and storage_quota_hard_tb <= 0:
            return None

        return [
            Quota(
                type=QuotaType.SPACE,
                quota=float(storage_quota_soft_tb),
                unit=QuotaUnit.TERA,
                enforcementType=EnforcementType.SOFT,
            ),
            Quota(
                type=QuotaType.SPACE,
                quota=float(storage_quota_hard_tb),
                unit=QuotaUnit.TERA,
                enforcementType=EnforcementType.HARD,
            ),
            Quota(
                type=QuotaType.INODES,
                quota=float(inode_soft),
                unit=QuotaUnit.NONE,
                enforcementType=EnforcementType.SOFT,
            ),
            Quota(
                type=QuotaType.INODES,
                quota=float(inode_hard),
                unit=QuotaUnit.NONE,
                enforcementType=EnforcementType.HARD,
            ),
        ]

    def calculate_update_quotas(
        self, resource: ParsedWaldurResource
    ) -> Tuple[Optional[List[Quota]], Optional[List[Quota]]]:
        """
        Calculate old and new quotas if an update order is in progress.
        Returns: (old_quotas, new_quotas)
        """
        order = resource.order_in_progress

        # Basic Validation
        if not order or order.type_ != RequestTypes.UPDATE:
            return None, None

        # Attributes are often a plain dict or a property wrapper
        attrs = order.attributes
        if not attrs:
            return None, None

        old_limits_override = None
        new_limits_override = None
        old_options_override = None
        new_options_override = None

        has_limit_update = "old_limits" in attrs
        has_option_update = "old_options" in attrs or "new_options" in attrs

        # Scenario 1: Limits Update
        if has_limit_update:
            old_limits_override = self._extract_old_limits(attrs)
            new_limits_override = self._extract_new_limits(order)

        # Scenario 2: Options Update
        if has_option_update:
            old_options_override = self._extract_old_options(attrs)
            new_options_override = self._extract_new_options(attrs)

        # If neither scenario matched sufficiently, return None
        if not (
            old_limits_override
            or new_limits_override
            or old_options_override
            or new_options_override
        ):
            return None, None

        old_quotas = self.calculate_quotas(
            resource,
            override_limits=old_limits_override,
            override_options=old_options_override,
        )

        new_quotas = self.calculate_quotas(
            resource,
            override_limits=new_limits_override,
            override_options=new_options_override,
        )

        return old_quotas, new_quotas

    def _extract_old_limits(self, attributes: dict) -> Optional[ResourceLimits]:
        old_limits_data = attributes.get("old_limits")
        if old_limits_data:
            return ResourceLimits(**old_limits_data)
        return None

    def _extract_new_limits(self, order) -> Optional[ResourceLimits]:
        order_limits = order.limits
        if order_limits:
            # order.limits might be a model or dict
            limits_dict = getattr(order_limits, "additional_properties", None)
            if limits_dict:
                return ResourceLimits(**limits_dict)
        return None

    def _extract_old_options(self, attributes: dict) -> Optional[ResourceOptions]:
        old_options_data = attributes.get("old_options")
        if old_options_data:
            return ResourceOptions(**old_options_data)
        return None

    def _extract_new_options(self, attributes: dict) -> Optional[ResourceOptions]:
        new_options_data = attributes.get("new_options")
        if new_options_data:
            return ResourceOptions(**new_options_data)
        return None
