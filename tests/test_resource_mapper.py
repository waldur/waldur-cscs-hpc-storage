from unittest.mock import Mock, AsyncMock
from uuid import uuid4

import pytest
from tests.conftest import make_test_uuid
from waldur_cscs_hpc_storage.models.enums import (
    EnforcementType,
    QuotaType,
    QuotaUnit,
    TargetType,
)
from waldur_cscs_hpc_storage.models import Quota
from waldur_cscs_hpc_storage.config import BackendConfig
from waldur_cscs_hpc_storage.mapper import ResourceMapper
from waldur_cscs_hpc_storage.mapper import QuotaCalculator
from waldur_api_client.types import Unset


def create_mock_quotas(storage_limit: float = 150.0) -> list[Quota]:
    """Create a list of mock Quota objects for testing."""
    soft_inode = int(storage_limit * 1000 * 1000 * 1.5)
    hard_inode = int(storage_limit * 1000 * 1000 * 2.0)
    return [
        Quota(
            type=QuotaType.SPACE,
            quota=float(storage_limit),
            unit=QuotaUnit.TERA,
            enforcementType=EnforcementType.SOFT,
        ),
        Quota(
            type=QuotaType.SPACE,
            quota=float(storage_limit),
            unit=QuotaUnit.TERA,
            enforcementType=EnforcementType.HARD,
        ),
        Quota(
            type=QuotaType.INODES,
            quota=float(soft_inode),
            unit=QuotaUnit.NONE,
            enforcementType=EnforcementType.SOFT,
        ),
        Quota(
            type=QuotaType.INODES,
            quota=float(hard_inode),
            unit=QuotaUnit.NONE,
            enforcementType=EnforcementType.HARD,
        ),
    ]


class TestResourceMapper:
    """Tests for ResourceMapper class."""

    @pytest.fixture
    def backend_settings(self):
        return BackendConfig(
            storage_file_system="lustre",
            inode_soft_coefficient=1.5,
            inode_hard_coefficient=2.0,
            use_mock_target_items=True,
            development_mode=True,
        )

    @pytest.fixture
    def mock_gid_service(self):
        service = Mock()
        # Use AsyncMock for async method
        service.get_project_unix_gid = AsyncMock(return_value=30042)
        return service

    @pytest.fixture
    def mapper(self, backend_settings, mock_gid_service):
        quota_calculator = QuotaCalculator(backend_settings)
        return ResourceMapper(backend_settings, mock_gid_service, quota_calculator)

    @pytest.mark.asyncio
    async def test_map_resource_basic(self, mapper):
        """Test basic resource mapping."""
        mock_resource = Mock()
        mock_resource.uuid = str(uuid4())
        mock_resource.slug = "test-resource"
        mock_resource.customer_slug = "customer"
        mock_resource.project_slug = "project"
        mock_resource.state = "OK"

        mock_resource.limits = Mock(storage=100)
        mock_resource.attributes = Mock(permissions="2770", storage_data_type="store")
        mock_resource.options = Mock(
            soft_quota_space=None,
            hard_quota_space=None,
            soft_quota_inodes=None,
            hard_quota_inodes=None,
            permissions=None,
        )
        mock_resource.backend_metadata = Mock(additional_properties={})
        mock_resource.get_effective_storage_quotas.return_value = (100, 100)
        mock_resource.get_effective_inode_quotas.return_value = (1000, 2000)
        mock_resource.effective_permissions = "2770"
        mock_resource.render_quotas.return_value = create_mock_quotas(100)
        mock_resource.order_in_progress = Unset()
        mock_resource.callback_urls = {}

        parent_uuid = str(make_test_uuid("parent-uuid"))
        result = await mapper.map_resource(
            mock_resource, "capstor", parent_item_id=parent_uuid
        )

        assert str(result.itemId) == mock_resource.uuid
        assert result.storageSystem.key == "capstor"
        assert result.storageDataType.key == "store"
        assert result.target.targetItem.unixGid == 30042
        assert result.status == "active"
        assert str(result.parentItemId) == parent_uuid

    @pytest.mark.asyncio
    async def test_map_resource_quotas(self, mapper):
        """Test mapping of quotas."""
        mock_resource = Mock()
        mock_resource.uuid = str(uuid4())
        mock_resource.slug = "test-resource"
        mock_resource.customer_slug = "customer"
        mock_resource.project_slug = "project"
        mock_resource.state = "OK"

        mock_resource.limits = Mock(storage=50)
        mock_resource.attributes = Mock(permissions="2770", storage_data_type="store")
        mock_resource.options = Mock(
            soft_quota_space=None,
            hard_quota_space=None,
            soft_quota_inodes=None,
            hard_quota_inodes=None,
            permissions=None,
        )
        mock_resource.backend_metadata = Mock(additional_properties={})
        mock_resource.get_effective_storage_quotas.return_value = (50, 50)
        mock_resource.get_effective_inode_quotas.return_value = (500, 1000)
        mock_resource.effective_permissions = "2770"
        # Mocking render_quotas behavior implicitly via return value
        mock_resource.render_quotas.return_value = create_mock_quotas(50)
        mock_resource.order_in_progress = Unset()
        mock_resource.callback_urls = {}

        result = await mapper.map_resource(mock_resource, "capstor")

        assert len(result.quotas) == 4
        space_quotas = [q for q in result.quotas if q.type == QuotaType.SPACE]
        assert space_quotas[0].quota == 50.0

    @pytest.mark.asyncio
    async def test_dynamic_target_type_mapping(self, mapper):
        """Test that storage data type determines target type."""
        mock_resource = Mock()
        mock_resource.uuid = str(uuid4())
        mock_resource.slug = "test-resource"
        mock_resource.customer_slug = "customer"
        mock_resource.project_slug = "project"
        mock_resource.project_uuid = "project-uuid"
        mock_resource.state = "OK"

        mock_resource.limits = Mock(storage=50)
        mock_resource.attributes = Mock(permissions="2770", storage_data_type="users")
        mock_resource.options = Mock(
            soft_quota_space=None,
            hard_quota_space=None,
            soft_quota_inodes=None,
            hard_quota_inodes=None,
            permissions=None,
        )
        mock_resource.backend_metadata = Mock(additional_properties={})
        mock_resource.get_effective_storage_quotas.return_value = (50, 50)
        mock_resource.get_effective_inode_quotas.return_value = (500, 1000)
        mock_resource.effective_permissions = "2770"
        mock_resource.render_quotas.return_value = create_mock_quotas(50)
        mock_resource.order_in_progress = Unset()
        mock_resource.callback_urls = {}

        result = await mapper.map_resource(mock_resource, "capstor")

        assert result.storageDataType.key == "users"
        assert result.target.targetType == TargetType.USER

    @pytest.mark.asyncio
    async def test_build_target_item_project(self, mapper):
        """Test _build_target_item for PROJECT type."""
        mock_resource = Mock()
        mock_resource.slug = "project-slug"  # Used for name
        mock_resource.project_slug = "project-slug"
        mock_resource.state = "OK"
        mock_resource.backend_metadata = Mock(additional_properties={})

        target_item = await mapper._build_target_item(mock_resource, TargetType.PROJECT)

        assert target_item.name == "project-slug"
        assert target_item.unixGid == 30042
        assert target_item.status == "active"
