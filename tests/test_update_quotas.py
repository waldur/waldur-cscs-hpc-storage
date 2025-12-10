import pytest
from unittest.mock import Mock

from waldur_api_client.models.resource_state import ResourceState
from waldur_api_client.models.request_types import RequestTypes

from waldur_cscs_hpc_storage.base.enums import (
    EnforcementType,
    QuotaType,
)
from waldur_cscs_hpc_storage.base.schemas import (
    ParsedWaldurResource,
    ResourceLimits,
    ResourceOptions,
    ResourceAttributes,
)
from waldur_cscs_hpc_storage.config import BackendConfig
from waldur_cscs_hpc_storage.services.mapper import ResourceMapper

# --- Fixtures ---


@pytest.fixture
def mock_config():
    """Returns a config with easy-to-calculate coefficients."""
    return BackendConfig(
        storage_file_system="lustre",
        inode_base_multiplier=1_000_000,  # 1 TB = 1M Inodes
        inode_soft_coefficient=0.5,  # Soft = 50%
        inode_hard_coefficient=1.0,  # Hard = 100%
        use_mock_target_items=False,
    )


@pytest.fixture
def mock_gid_service():
    service = Mock()

    # Configure get_project_unix_gid to return an awaitable
    async def get_gid(*args, **kwargs):
        return 5000

    service.get_project_unix_gid.side_effect = get_gid
    return service


@pytest.fixture
def mapper(mock_config, mock_gid_service):
    return ResourceMapper(mock_config, mock_gid_service)


@pytest.fixture
def base_resource():
    """Creates a standard ParsedWaldurResource."""
    return ParsedWaldurResource(
        uuid="res-123",
        name="Test Resource",
        slug="test-resource",
        state=ResourceState.OK,
        offering_uuid="off-123",
        offering_name="Test Offering",
        offering_slug="capstor",
        project_uuid="proj-123",
        project_name="Test Project",
        project_slug="test-project",
        customer_uuid="cust-123",
        customer_name="Test Customer",
        customer_slug="test-customer",
        # Default limits: 1 TB
        limits=ResourceLimits(storage=1.0),
        attributes=ResourceAttributes(storage_data_type="store"),
        options=ResourceOptions(),
    )


# --- Helper to create Order Mocks ---


def create_mock_order(order_type, attributes_dict, limits_dict=None):
    """
    Simulates the structure of a Waldur Order object.
    Waldur client models often wrap dicts in `additional_properties`.
    """
    order = Mock()
    order.type_ = order_type

    # Mock attributes to just be the dict, as expected by the mapper
    order.attributes = attributes_dict

    # Mock limits structure (for new limits)
    if limits_dict:
        # If the code expects order.limits to have additional_properties
        limits_mock = Mock()
        limits_mock.additional_properties = limits_dict
        order.limits = limits_mock
    else:
        order.limits = None

    return order


# --- Tests ---


@pytest.mark.asyncio
async def test_calculate_quotas_no_order(mapper, base_resource):
    """Ensure old/new quotas are None if no order exists."""
    base_resource.order_in_progress = None

    result = await mapper.map_resource(base_resource, "capstor")

    assert result.oldQuotas is None
    assert result.newQuotas is None


@pytest.mark.asyncio
async def test_calculate_quotas_wrong_order_type(mapper, base_resource):
    """Ensure we ignore non-UPDATE orders."""
    order = create_mock_order(RequestTypes.CREATE, {})
    base_resource.order_in_progress = order

    result = await mapper.map_resource(base_resource, "capstor")

    assert result.oldQuotas is None
    assert result.newQuotas is None


@pytest.mark.asyncio
async def test_limit_update_scenario(mapper, base_resource):
    """
    Test Scenario: Storage Limit increase from 1TB to 2TB.
    """
    # 1. Setup Order
    # Old: 1TB (implied base inodes: 1M -> Soft: 500k, Hard: 1M)
    # New: 2TB (implied base inodes: 2M -> Soft: 1M, Hard: 2M)

    attributes = {"old_limits": {"storage": 1.0}}
    new_limits = {"storage": 2.0}

    base_resource.order_in_progress = create_mock_order(
        RequestTypes.UPDATE, attributes, new_limits
    )

    # 2. Run Map
    result = await mapper.map_resource(base_resource, "capstor")

    # 3. Assertions
    assert result.oldQuotas is not None
    assert result.newQuotas is not None

    # Check Old Quotas (1TB)
    old_space = next(
        q
        for q in result.oldQuotas
        if q.type == QuotaType.SPACE and q.enforcementType == EnforcementType.HARD
    )
    old_inodes = next(
        q
        for q in result.oldQuotas
        if q.type == QuotaType.INODES and q.enforcementType == EnforcementType.HARD
    )

    assert old_space.quota == 1.0
    assert old_inodes.quota == 1_000_000.0  # 1TB * 1M * 1.0 coeff

    # Check New Quotas (2TB)
    new_space = next(
        q
        for q in result.newQuotas
        if q.type == QuotaType.SPACE and q.enforcementType == EnforcementType.HARD
    )
    new_inodes = next(
        q
        for q in result.newQuotas
        if q.type == QuotaType.INODES and q.enforcementType == EnforcementType.HARD
    )

    assert new_space.quota == 2.0
    assert new_inodes.quota == 2_000_000.0  # 2TB * 1M * 1.0 coeff


@pytest.mark.asyncio
async def test_options_update_scenario(mapper, base_resource):
    """
    Test Scenario: Admin manually overrides inodes via Options.
    Storage limit stays 1TB.
    """
    # Current resource has 1TB.

    # Old Options: Override soft inodes to 500.
    # New Options: Override soft inodes to 900.
    attributes = {
        "old_options": {"soft_quota_inodes": 500},
        "new_options": {"soft_quota_inodes": 900},
    }

    base_resource.order_in_progress = create_mock_order(RequestTypes.UPDATE, attributes)

    # Run Map
    result = await mapper.map_resource(base_resource, "capstor")

    assert result.oldQuotas is not None
    assert result.newQuotas is not None

    # Check Old Quotas
    old_soft_inode = next(
        q
        for q in result.oldQuotas
        if q.type == QuotaType.INODES and q.enforcementType == EnforcementType.SOFT
    )
    assert old_soft_inode.quota == 500.0

    # Check New Quotas
    new_soft_inode = next(
        q
        for q in result.newQuotas
        if q.type == QuotaType.INODES and q.enforcementType == EnforcementType.SOFT
    )
    assert new_soft_inode.quota == 900.0

    # Check that Space is preserved (inherited from base resource because override options didn't specify space)
    # Base resource is 1.0 TB
    new_space_hard = next(
        q
        for q in result.newQuotas
        if q.type == QuotaType.SPACE and q.enforcementType == EnforcementType.HARD
    )
    assert new_space_hard.quota == 1.0


@pytest.mark.asyncio
async def test_malformed_order_handling(mapper, base_resource):
    """
    Ensure the mapper doesn't crash if order attributes are missing expected keys.
    """
    # Order exists and is UPDATE, but attributes are empty
    base_resource.order_in_progress = create_mock_order(RequestTypes.UPDATE, {})

    result = await mapper.map_resource(base_resource, "capstor")

    # Should gracefully fail to calculate update quotas and return None
    assert result.oldQuotas is None
    assert result.newQuotas is None
    # Main quotas should still exist
    assert result.quotas is not None


@pytest.mark.asyncio
async def test_missing_nested_attributes_handling(mapper, base_resource):
    """
    Edge case: Attributes exist but are plain dict (not Wrapped in additional_properties)
    or just missing entirely.
    """
    order = Mock()
    order.type_ = RequestTypes.UPDATE
    # Simulate order.attributes returning None or {}
    order.attributes = {}

    base_resource.order_in_progress = order

    result = await mapper.map_resource(base_resource, "capstor")
    assert result.oldQuotas is None


@pytest.mark.asyncio
async def test_calculate_update_quotas_direct_method(mapper, base_resource):
    """
    Direct unit test of the _calculate_update_quotas private method
    to verify specific fallback logic.
    """
    # Scenario: Only new_options are provided (e.g. setting an override for the first time).
    # old_options implies "None" (use defaults).

    attributes = {"new_options": {"soft_quota_inodes": 999}}
    base_resource.order_in_progress = create_mock_order(RequestTypes.UPDATE, attributes)

    old_q, new_q = mapper._calculate_update_quotas(base_resource)

    # Old Quotas: Should use calculated defaults based on 1TB
    # 1TB * 1M * 0.5 (config soft coeff) = 500,000
    old_soft = next(
        q
        for q in old_q
        if q.type == QuotaType.INODES and q.enforcementType == EnforcementType.SOFT
    )
    assert old_soft.quota == 500_000.0

    # New Quotas: Should use override
    new_soft = next(
        q
        for q in new_q
        if q.type == QuotaType.INODES and q.enforcementType == EnforcementType.SOFT
    )
    assert new_soft.quota == 999.0
