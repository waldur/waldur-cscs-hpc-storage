import pytest
from unittest.mock import Mock
from uuid import uuid4


from waldur_api_client.models.request_types import RequestTypes

from waldur_cscs_hpc_storage.models.enums import (
    EnforcementType,
    QuotaType,
    QuotaUnit,
)
from waldur_cscs_hpc_storage.models import (
    ParsedWaldurResource,
    ResourceLimits,
    ResourceOptions,
)
from waldur_cscs_hpc_storage.config import BackendConfig
from waldur_cscs_hpc_storage.mapper import QuotaCalculator


@pytest.fixture
def quota_calculator():
    config = BackendConfig(
        storage_file_system="lustre",
        inode_base_multiplier=1000,  # 1 TB = 1000 inodes (for simplicity)
        inode_soft_coefficient=0.9,
        inode_hard_coefficient=1.0,
        development_mode=True,
    )
    return QuotaCalculator(config)


@pytest.fixture
def mock_resource():
    resource = Mock(spec=ParsedWaldurResource)
    resource.uuid = str(uuid4())
    resource.limits = ResourceLimits(storage=10.0)  # 10 TB
    resource.options = ResourceOptions()
    resource.order_in_progress = None
    return resource


class TestQuotaCalculator:
    def test_calculate_quotas_defaults(self, quota_calculator, mock_resource):
        """Test quota calculation with default limits and no options."""
        quotas = quota_calculator.calculate_quotas(mock_resource)

        assert len(quotas) == 4

        # Verify Space Quotas
        space_soft = next(
            q
            for q in quotas
            if q.type == QuotaType.SPACE and q.enforcementType == EnforcementType.SOFT
        )
        space_hard = next(
            q
            for q in quotas
            if q.type == QuotaType.SPACE and q.enforcementType == EnforcementType.HARD
        )

        assert space_soft.quota == 10.0
        assert space_hard.quota == 10.0
        assert space_soft.unit == QuotaUnit.TERA

        # Verify Inode Quotas
        # Base = 10 * 1000 = 10000
        # Soft = 10000 * 0.9 = 9000
        # Hard = 10000 * 1.0 = 10000
        inode_soft = next(
            q
            for q in quotas
            if q.type == QuotaType.INODES and q.enforcementType == EnforcementType.SOFT
        )
        inode_hard = next(
            q
            for q in quotas
            if q.type == QuotaType.INODES and q.enforcementType == EnforcementType.HARD
        )

        assert inode_soft.quota == 9000.0
        assert inode_hard.quota == 10000.0
        assert inode_soft.unit == QuotaUnit.NONE

    def test_calculate_quotas_with_options_override(
        self, quota_calculator, mock_resource
    ):
        """Test overriding quotas via resource options."""
        mock_resource.options = ResourceOptions(
            hard_quota_space=15.0,
            soft_quota_inodes=500,
            hard_quota_inodes=1500,
        )

        quotas = quota_calculator.calculate_quotas(mock_resource)

        space_soft = next(
            q
            for q in quotas
            if q.type == QuotaType.SPACE and q.enforcementType == EnforcementType.SOFT
        )
        space_hard = next(
            q
            for q in quotas
            if q.type == QuotaType.SPACE and q.enforcementType == EnforcementType.HARD
        )
        inode_soft = next(
            q
            for q in quotas
            if q.type == QuotaType.INODES and q.enforcementType == EnforcementType.SOFT
        )
        inode_hard = next(
            q
            for q in quotas
            if q.type == QuotaType.INODES and q.enforcementType == EnforcementType.HARD
        )

        assert space_soft.quota == 10.0  # From limits.storage=10.0 (no override)
        assert space_hard.quota == 15.0  # From options override
        assert inode_soft.quota == 500.0
        assert inode_hard.quota == 1500.0

    def test_calculate_quotas_zero_storage(self, quota_calculator, mock_resource):
        """Test behavior when storage limit is 0."""
        mock_resource.limits = ResourceLimits(storage=0.0)

        # Should return None if effectively no storage
        quotas = quota_calculator.calculate_quotas(mock_resource)
        assert quotas is None

        # But if overrides are present, it should respect them
        mock_resource.limits = ResourceLimits(storage=1.1)
        # mock_resource.options = ResourceOptions(soft_quota_space=1.0) # Old way
        quotas_with_override = quota_calculator.calculate_quotas(mock_resource)
        assert quotas_with_override is not None
        assert quotas_with_override[0].quota == 1.1

    def test_calculate_quotas_override_arguments(self, quota_calculator, mock_resource):
        """Test passing explicit override arguments to the method."""
        limits_override = ResourceLimits(storage=20.0)
        options_override = ResourceOptions(soft_quota_inodes=9999)

        quotas = quota_calculator.calculate_quotas(
            mock_resource,
            override_limits=limits_override,
            override_options=options_override,
        )

        # Should use 20.0 TB for calculations
        space_hard = next(
            q
            for q in quotas
            if q.type == QuotaType.SPACE and q.enforcementType == EnforcementType.HARD
        )
        assert space_hard.quota == 20.0

        # Should use explicit inode override
        inode_soft = next(
            q
            for q in quotas
            if q.type == QuotaType.INODES and q.enforcementType == EnforcementType.SOFT
        )
        assert inode_soft.quota == 9999.0

    def test_calculate_update_quotas_no_order(self, quota_calculator, mock_resource):
        """Test update quotas when no order is in progress."""
        mock_resource.order_in_progress = None
        old, new = quota_calculator.calculate_update_quotas(mock_resource)
        assert old is None
        assert new is None

    def test_calculate_update_quotas_wrong_type(self, quota_calculator, mock_resource):
        """Test update quotas when order type is not UPDATE."""
        mock_order = Mock()
        mock_order.type_ = RequestTypes.CREATE
        mock_resource.order_in_progress = mock_order

        old, new = quota_calculator.calculate_update_quotas(mock_resource)
        assert old is None
        assert new is None

    def test_calculate_update_quotas_limits_change(
        self, quota_calculator, mock_resource
    ):
        """Test calculating old/new quotas when limits change."""
        # Current state: storage=10.0
        mock_resource.limits = ResourceLimits(storage=10.0)

        mock_order = Mock()
        mock_order.type_ = RequestTypes.UPDATE
        mock_order.attributes = {"old_limits": {"storage": 10.0}}
        # New limits in order
        mock_order.limits = Mock()
        mock_order.limits.additional_properties = {"storage": 20.0}

        mock_resource.order_in_progress = mock_order

        old, new = quota_calculator.calculate_update_quotas(mock_resource)

        assert old is not None
        assert new is not None

        # Verify Old (10TB)
        old_space = next(
            q
            for q in old
            if q.type == QuotaType.SPACE and q.enforcementType == EnforcementType.HARD
        )
        assert old_space.quota == 10.0

        # Verify New (20TB)
        new_space = next(
            q
            for q in new
            if q.type == QuotaType.SPACE and q.enforcementType == EnforcementType.HARD
        )
        assert new_space.quota == 20.0

    def test_calculate_update_quotas_options_change(
        self, quota_calculator, mock_resource
    ):
        """Test calculating old/new quotas when options change."""
        mock_resource.limits = ResourceLimits(storage=10.0)

        mock_order = Mock()
        mock_order.type_ = RequestTypes.UPDATE
        mock_order.attributes = {
            "old_options": {"soft_quota_inodes": 100},
            "new_options": {"soft_quota_inodes": 200},
        }

        mock_resource.order_in_progress = mock_order

        old, new = quota_calculator.calculate_update_quotas(mock_resource)

        old_inode = next(
            q
            for q in old
            if q.type == QuotaType.INODES and q.enforcementType == EnforcementType.SOFT
        )
        new_inode = next(
            q
            for q in new
            if q.type == QuotaType.INODES and q.enforcementType == EnforcementType.SOFT
        )

        assert old_inode.quota == 100.0
        assert new_inode.quota == 200.0

    def test_calculate_update_quotas_limits_and_options(
        self, quota_calculator, mock_resource
    ):
        """Test both limits and options changing."""
        mock_resource.limits = ResourceLimits(storage=10.0)

        mock_order = Mock()
        mock_order.type_ = RequestTypes.UPDATE
        mock_order.attributes = {
            "old_limits": {
                "storage": 10.0,
            },
            "old_options": {},
        }
        mock_order.limits = Mock()
        mock_order.limits.additional_properties = {"storage": 50.0}
        # And let's say new options are removed/empty (so revert to defaults based on new limits)

        mock_resource.order_in_progress = mock_order

        old, new = quota_calculator.calculate_update_quotas(mock_resource)

        # Old: hard=10 (limit), soft=5 (override)
        old_soft = next(
            q
            for q in old
            if q.type == QuotaType.SPACE and q.enforcementType == EnforcementType.SOFT
        )
        old_hard = next(
            q
            for q in old
            if q.type == QuotaType.SPACE and q.enforcementType == EnforcementType.HARD
        )
        assert old_soft.quota == 10.0
        assert old_hard.quota == 10.0

        # New: storage=50. No option override, so soft=50, hard=50
        new_soft = next(
            q
            for q in new
            if q.type == QuotaType.SPACE and q.enforcementType == EnforcementType.SOFT
        )
        new_hard = next(
            q
            for q in new
            if q.type == QuotaType.SPACE and q.enforcementType == EnforcementType.HARD
        )
        assert new_soft.quota == 50.0
        assert new_hard.quota == 50.0
