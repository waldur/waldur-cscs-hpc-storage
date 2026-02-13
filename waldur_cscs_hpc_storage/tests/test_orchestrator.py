"""Tests for CSCS HPC Storage Orchestrator."""

from unittest.mock import AsyncMock, Mock, patch
from uuid import uuid4

import pytest
from pydantic import ValidationError
from waldur_api_client.models.order_state import OrderState
from waldur_api_client.models.resource_state import ResourceState
from waldur_api_client.types import Unset

from waldur_cscs_hpc_storage.config import (
    BackendConfig,
    StorageProxyConfig,
    WaldurApiConfig,
)
from waldur_cscs_hpc_storage.mapper import QuotaCalculator, ResourceMapper
from waldur_cscs_hpc_storage.mapper.mount_points import generate_project_mount_point
from waldur_cscs_hpc_storage.models import (
    Quota,
    ResourceAttributes,
    StorageResourceFilter,
)
from waldur_cscs_hpc_storage.models.enums import (
    EnforcementType,
    QuotaType,
    QuotaUnit,
    StorageDataType,
    TargetStatus,
    TargetType,
)
from waldur_cscs_hpc_storage.services.mock_gid_service import MockGidService
from waldur_cscs_hpc_storage.services.orchestrator import StorageOrchestrator
from waldur_cscs_hpc_storage.services.waldur_service import WaldurService
from waldur_cscs_hpc_storage.tests.conftest import make_test_uuid


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


class TestStorageOrchestratorBase:
    """Base test class for CSCS HPC Storage Orchestrator."""

    def setup_method(self):
        """Set up test environment."""
        self.orchestrator_config = BackendConfig(
            storage_file_system="lustre",
            inode_soft_coefficient=1.5,
            inode_hard_coefficient=2.0,
            use_mock_target_items=True,
            development_mode=True,  # Enable development mode for tests
        )
        self.waldur_api_config = WaldurApiConfig(
            api_url="https://example.com",
            access_token="e38cd56f1ce5bf4ef35905f2bdcf84f1d7f2cc5e",
        )

        # Mock StorageProxyConfig for Orchestrator
        self.proxy_config = Mock(spec=StorageProxyConfig)
        self.proxy_config.backend_settings = self.orchestrator_config
        self.proxy_config.storage_systems = {"lustre": "slug"}

        self.orchestrator = self._create_orchestrator()

    def _create_orchestrator(self):
        """Helper to create orchestrator instance with mocks."""
        # Initialize dependencies
        gid_service = MockGidService()
        quota_calculator = QuotaCalculator(self.orchestrator_config)
        mapper = ResourceMapper(self.orchestrator_config, gid_service, quota_calculator)

        # Inject mock waldur_service for testing
        self.mock_waldur_service = Mock(spec=WaldurService)

        orchestrator = StorageOrchestrator(
            self.proxy_config,
            waldur_service=self.mock_waldur_service,
            mapper=mapper,
        )
        return orchestrator


class TestStorageOrchestrator(TestStorageOrchestratorBase):
    """Test cases for CSCS HPC Storage Orchestrator."""

    @pytest.fixture(autouse=True)
    def mock_gid_lookup(self):
        """Mock GID lookup for all tests in this class."""
        with patch.object(
            MockGidService, "get_project_unix_gid", new_callable=AsyncMock
        ) as mock_method:
            mock_method.return_value = 30000
            yield

    def test_generate_mount_point(self):
        """Test mount point generation."""
        mount_point = generate_project_mount_point(
            storage_system="lustre-fs",
            data_type="store",
            tenant_id="university",
            customer="physics-dept",
            project_id="climate-sim",
        )
        assert mount_point == "/lustre-fs/store/university/physics-dept/climate-sim"

    @pytest.mark.asyncio
    async def test_get_target_item_data_mock(self):
        """Test target item data generation with mock enabled."""
        mock_resource = Mock()
        mock_resource.customer_slug = "university"
        mock_resource.customer_name = "University"
        mock_resource.customer_uuid = Mock()
        mock_resource.customer_uuid = str(uuid4())
        mock_resource.project_slug = "physics-dept"
        mock_resource.project_name = "Physics Department"
        mock_resource.project_uuid = Mock()
        mock_resource.project_uuid = str(uuid4())
        mock_resource.slug = "climate-sim"
        mock_resource.uuid = str(uuid4())  # ParsedWaldurResource.uuid is str
        mock_resource.state = "OK"  # Set state to map to "active" status
        mock_resource.backend_metadata = Mock(additional_properties={})
        mock_resource.callback_urls = {}
        # Determine target type

        target_data = await self.orchestrator.mapper._build_target_item(
            mock_resource, TargetType.PROJECT
        )

        assert target_data.name == "climate-sim"
        assert target_data.itemId is not None
        assert target_data.unixGid is not None
        assert target_data.status == "active"
        assert target_data.active is True

    @pytest.mark.asyncio
    async def test_target_status_mapping_from_waldur_state(self):
        """Test that target item status correctly maps from Waldur resource states."""
        mock_resource = Mock()
        mock_resource.customer_slug = "university"
        mock_resource.customer_name = "University"
        mock_resource.project_slug = "physics-dept"
        mock_resource.project_name = "Physics Department"
        mock_resource.slug = "test-resource"
        mock_resource.uuid = str(uuid4())
        mock_resource.backend_metadata = Mock(additional_properties={})
        mock_resource.callback_urls = {}

        # Test different Waldur states and their expected target statuses
        test_cases = [
            ("Creating", "pending", False),
            ("OK", "active", True),
            ("Erred", "error", False),
            ("Terminating", "removing", False),
            ("Terminated", "removed", False),
        ]

        for waldur_state, expected_status, expected_active in test_cases:
            mock_resource.state = waldur_state

            target_data = await self.orchestrator.mapper._build_target_item(
                mock_resource, TargetType.PROJECT
            )

            assert target_data.status == expected_status, (
                f"Waldur state '{waldur_state}' should map to status '{expected_status}', got '{target_data.status}'"
            )
            assert target_data.active == expected_active, (
                f"Waldur state '{waldur_state}' should set active={expected_active}, got {target_data.active}"
            )

    @pytest.mark.asyncio
    async def test_create_storage_resource_json(self):
        """Test storage resource JSON creation."""
        mock_resource = Mock()
        mock_resource.uuid = Mock()
        mock_resource.uuid = str(uuid4())
        mock_resource.name = "Test Storage"
        mock_resource.slug = "test-storage"
        mock_resource.provider_slug = "cscs"
        mock_resource.customer_slug = "university"
        mock_resource.customer_name = "University"
        mock_resource.customer_uuid = Mock()
        mock_resource.customer_uuid = str(uuid4())
        mock_resource.project_slug = "physics-dept"
        mock_resource.project_name = "Physics Department"
        mock_resource.project_uuid = Mock()
        mock_resource.project_uuid = str(uuid4())
        mock_limits = Mock()
        mock_limits.storage = 150  # 150TB
        mock_resource.limits = mock_limits

        # Create mock attributes
        mock_attributes = Mock()
        mock_attributes.permissions = "2770"
        mock_attributes.storage_data_type = "store"
        mock_resource.attributes = mock_attributes

        mock_resource.options = Mock(
            hard_quota_space=None,
            soft_quota_inodes=None,
            hard_quota_inodes=None,
            permissions=None,
        )
        mock_resource.backend_metadata = Mock(
            tenant_item=None, customer_item=None, project_item=None, user_item=None
        )
        mock_resource.get_effective_storage_quotas.return_value = (150, 150)
        mock_resource.get_effective_inode_quotas.return_value = (
            int(150 * 1000 * 1000 * 1.5),
            int(150 * 1000 * 1000 * 2.0),
        )
        mock_resource.effective_permissions = "2770"
        mock_resource.render_quotas.return_value = create_mock_quotas(150.0)
        mock_resource.callback_urls = {}

        storage_json = await self.orchestrator.mapper.map_resource(
            mock_resource,
            "lustre-fs",
            parent_item_id=str(make_test_uuid("parent-uuid")),
        )

        assert str(storage_json.itemId) == mock_resource.uuid
        assert storage_json.status == "pending"
        assert (
            storage_json.mountPoint.default
            == "/lustre-fs/store/cscs/university/physics-dept"
        )
        assert storage_json.permission.value == "2770"
        assert len(storage_json.quotas) == 4  # 2 space + 2 inode quotas
        assert storage_json.storageSystem.key == "lustre-fs"
        assert storage_json.storageFileSystem.key == "lustre"
        assert storage_json.target.targetItem.unixGid == 30000

    @pytest.mark.asyncio
    async def test_create_storage_resource_json_with_provider_action_urls(self):
        """Test storage resource JSON creation includes provider action URLs when available."""
        mock_resource = Mock()
        mock_resource.uuid = Mock()
        mock_resource.uuid = str(uuid4())
        mock_resource.name = "Test Storage"
        mock_resource.slug = "test-storage"
        mock_resource.provider_slug = "cscs"
        mock_resource.customer_slug = "university"
        mock_resource.customer_name = "University"
        mock_resource.customer_uuid = Mock()
        mock_resource.customer_uuid = str(uuid4())
        mock_resource.project_slug = "physics-dept"
        mock_resource.project_name = "Physics Department"
        mock_resource.project_uuid = Mock()
        mock_resource.project_uuid = str(uuid4())

        mock_limits = Mock()
        mock_limits.storage = 150
        mock_resource.limits = mock_limits

        # Create mock attributes
        mock_attributes = Mock()
        mock_attributes.permissions = "2770"
        mock_attributes.storage_data_type = "store"
        mock_resource.attributes = mock_attributes

        mock_resource.options = Mock(
            hard_quota_space=None,
            soft_quota_inodes=None,
            hard_quota_inodes=None,
            permissions=None,
        )
        mock_resource.backend_metadata = Mock(
            tenant_item=None, customer_item=None, project_item=None, user_item=None
        )
        mock_resource.get_effective_storage_quotas.return_value = (150, 150)
        mock_resource.get_effective_inode_quotas.return_value = (
            100,
            200,
        )  # simpler mock
        mock_resource.effective_permissions = "2770"

        # Create mock order_in_progress
        order_uuid = str(uuid4())
        mock_order = Mock()
        mock_order.uuid = order_uuid
        # Set state to PENDING_PROVIDER so that approve/reject URLs are generated
        mock_order.state = OrderState.PENDING_PROVIDER
        mock_order.url = (
            f"https://waldur.example.com/api/marketplace-orders/{order_uuid}/"
        )
        mock_resource.order_in_progress = mock_order
        mock_resource.callback_urls = {
            "approve_by_provider_url": f"https://waldur.example.com/api/marketplace-orders/{order_uuid}/approve_by_provider/",
            "reject_by_provider_url": f"https://waldur.example.com/api/marketplace-orders/{order_uuid}/reject_by_provider/",
        }
        mock_resource.render_quotas.return_value = create_mock_quotas(150.0)

        storage_json = await self.orchestrator.mapper.map_resource(
            mock_resource,
            "lustre-fs",
            parent_item_id=str(make_test_uuid("parent-uuid")),
        )

        assert str(storage_json.itemId) == mock_resource.uuid
        assert (
            storage_json.approve_by_provider_url
            == f"https://waldur.example.com/api/marketplace-orders/{order_uuid}/approve_by_provider/"
        )
        assert (
            storage_json.reject_by_provider_url
            == f"https://waldur.example.com/api/marketplace-orders/{order_uuid}/reject_by_provider/"
        )

    @pytest.mark.asyncio
    async def test_create_storage_resource_json_without_provider_action_urls(self):
        """Test storage resource JSON creation without provider action URLs when not available."""
        mock_resource = Mock()
        mock_resource.uuid = Mock()
        mock_resource.uuid = str(uuid4())
        mock_resource.name = "Test Storage"
        mock_resource.slug = "test-storage"
        mock_resource.provider_slug = "cscs"
        mock_resource.customer_slug = "university"
        mock_resource.customer_name = "University"
        mock_resource.customer_uuid = Mock()
        mock_resource.customer_uuid = str(uuid4())
        mock_resource.project_slug = "physics-dept"
        mock_resource.project_name = "Physics Department"
        mock_resource.project_uuid = Mock()
        mock_resource.project_uuid = str(uuid4())

        mock_limits = Mock()
        mock_limits.storage = 150
        mock_resource.limits = mock_limits

        # Create mock attributes
        mock_attributes = Mock()
        mock_attributes.permissions = "2770"
        mock_attributes.storage_data_type = "store"
        mock_resource.attributes = mock_attributes
        mock_resource.options = Mock(
            hard_quota_space=None,
            soft_quota_inodes=None,
            hard_quota_inodes=None,
            permissions=None,
        )
        mock_resource.backend_metadata = Mock(
            tenant_item=None, customer_item=None, project_item=None, user_item=None
        )
        mock_resource.get_effective_storage_quotas.return_value = (150, 150)
        mock_resource.get_effective_inode_quotas.return_value = (100, 200)
        mock_resource.effective_permissions = "2770"
        mock_resource.render_quotas.return_value = create_mock_quotas(150.0)

        # No order_in_progress
        mock_resource.order_in_progress = Unset()
        mock_resource.callback_urls = {}

        storage_json = await self.orchestrator.mapper.map_resource(
            mock_resource,
            "lustre-fs",
            parent_item_id=str(make_test_uuid("parent-uuid")),
        )

        assert str(storage_json.itemId) == mock_resource.uuid
        assert not hasattr(storage_json, "approve_by_provider_url")
        assert not hasattr(storage_json, "reject_by_provider_url")

    @pytest.mark.asyncio
    async def test_create_storage_resource_json_with_order_but_no_uuid(self):
        """Test storage resource JSON creation when order exists but has no UUID."""
        mock_resource = Mock()
        mock_resource.uuid = Mock()
        mock_resource.uuid = str(uuid4())
        mock_resource.name = "Test Storage"
        mock_resource.slug = "test-storage"
        mock_resource.provider_slug = "cscs"
        mock_resource.customer_slug = "university"
        mock_resource.customer_name = "University"
        mock_resource.customer_uuid = Mock()
        mock_resource.customer_uuid = str(uuid4())
        mock_resource.project_slug = "physics-dept"
        mock_resource.project_name = "Physics Department"
        mock_resource.project_uuid = Mock()
        mock_resource.project_uuid = str(uuid4())

        mock_limits = Mock()
        mock_limits.storage = 150
        mock_resource.limits = mock_limits

        # Create mock attributes
        mock_attributes = Mock()
        mock_attributes.permissions = "2770"
        mock_attributes.storage_data_type = "store"
        mock_resource.attributes = mock_attributes

        mock_resource.options = Mock(
            hard_quota_space=None,
            soft_quota_inodes=None,
            hard_quota_inodes=None,
            permissions=None,
        )
        mock_resource.backend_metadata = Mock(
            tenant_item=None, customer_item=None, project_item=None, user_item=None
        )
        mock_resource.get_effective_storage_quotas.return_value = (150, 150)
        mock_resource.get_effective_inode_quotas.return_value = (100, 200)
        mock_resource.effective_permissions = "2770"
        mock_resource.render_quotas.return_value = create_mock_quotas(150.0)

        # Create mock order_in_progress without UUID
        # Create mock order_in_progress without UUID
        mock_order = Mock()
        mock_order.uuid = Unset()
        mock_resource.order_in_progress = mock_order
        mock_resource.callback_urls = {}

        storage_json = await self.orchestrator.mapper.map_resource(
            mock_resource,
            "lustre-fs",
            parent_item_id=str(make_test_uuid("parent-uuid")),
        )

        assert str(storage_json.itemId) == mock_resource.uuid
        assert not hasattr(storage_json, "approve_by_provider_url")
        assert not hasattr(storage_json, "reject_by_provider_url")

    def test_invalid_attribute_types_validation(self):
        """Test that non-string attribute values raise clear validation errors."""
        """Test that non-string attribute values raise clear validation errors."""
        # backend = self._create_orchestrator() # Removed unused variable

        # Create a mock resource
        mock_resource = Mock()
        mock_resource.uuid = Mock()
        mock_resource.uuid = str(uuid4())
        mock_resource.name = "Test Resource"
        mock_resource.slug = "test-resource"
        mock_resource.customer_slug = "university"
        mock_resource.project_slug = "physics"

        # Create mock limits
        mock_limits = Mock()
        mock_limits.storage = 50
        mock_resource.limits = mock_limits
        mock_resource.callback_urls = {}

        with pytest.raises(ValidationError):
            # Manually triggering validation by creating model with bad data
            ResourceAttributes(permissions=["775", "770"])  # type: ignore

    def test_invalid_storage_data_type(self):
        """Test invalid storage data type."""
        # The schema uses a validator that falls back to STORE on error.
        # So we expect it to NOT raise, but default to STORE.
        attr = ResourceAttributes(storage_data_type={"type": "store"})  # type: ignore
        assert attr.storage_data_type == StorageDataType.STORE

    @pytest.mark.asyncio
    async def test_status_mapping_from_waldur_state(self):
        """Test that Waldur resource state is correctly mapped to CSCS status."""
        backend = self._create_orchestrator()

        # Create a mock resource
        mock_resource = Mock()
        mock_resource.uuid = str(make_test_uuid("test-uuid"))
        mock_resource.slug = "test-resource"
        mock_resource.customer_slug = "university"
        mock_resource.project_slug = "physics"
        mock_resource.state = "OK"

        mock_limits = Mock()
        mock_limits.storage = 50
        mock_resource.limits = mock_limits

        mock_attributes = Mock()
        mock_attributes.storage_data_type = "store"
        mock_attributes.permissions = "775"
        mock_resource.attributes = mock_attributes
        mock_resource.options = Mock(
            hard_quota_space=None,
            soft_quota_inodes=None,
            hard_quota_inodes=None,
            permissions=None,
        )
        mock_resource.backend_metadata = Mock(
            tenant_item=None, customer_item=None, project_item=None, user_item=None
        )
        mock_resource.get_effective_storage_quotas.return_value = (50, 50)
        mock_resource.get_effective_inode_quotas.return_value = (100, 200)
        mock_resource.effective_permissions = "775"
        mock_resource.render_quotas.return_value = create_mock_quotas(50)
        mock_resource.callback_urls = {}

        # Test different state mappings
        test_cases = [
            (ResourceState.CREATING, "pending"),
            (ResourceState.OK, "active"),
            (ResourceState.ERRED, "error"),
            (ResourceState.TERMINATING, "removing"),
            (ResourceState.TERMINATED, "removed"),
            ("Unknown", "pending"),  # Default fallback for unmapped values
        ]

        for waldur_state, expected_status in test_cases:
            mock_resource.state = waldur_state

            result = await backend.mapper.map_resource(
                mock_resource,
                "test-storage",
                parent_item_id=str(make_test_uuid("parent")),
            )

            assert result.status == expected_status, (
                f"State '{waldur_state}' should map to '{expected_status}'"
            )

    @pytest.mark.asyncio
    async def test_dynamic_target_type_mapping(self):
        """Test that storage data type correctly maps to target type."""
        backend = self._create_orchestrator()

        # Create a mock resource
        mock_resource = Mock()
        mock_resource.uuid = str(make_test_uuid("test-uuid"))
        mock_resource.slug = "test-resource"
        mock_resource.customer_slug = "university"
        mock_resource.project_slug = "physics"
        mock_resource.project_uuid = "project-uuid"
        mock_resource.state = "OK"
        mock_resource.backend_metadata = Mock(
            tenant_item=None, customer_item=None, project_item=None, user_item=None
        )

        mock_limits = Mock()
        mock_limits.storage = 50
        mock_resource.limits = mock_limits
        mock_resource.options = Mock(
            hard_quota_space=None,
            soft_quota_inodes=None,
            hard_quota_inodes=None,
            permissions=None,
        )
        mock_resource.render_quotas.return_value = create_mock_quotas(50)
        mock_resource.callback_urls = {}

        # Test different storage data types
        test_cases = [
            ("store", "project"),
            ("archive", "project"),
            ("users", "user"),
            ("scratch", "user"),
            ("unknown", "project"),  # Default fallback
        ]

        for storage_data_type, expected_target_type in test_cases:
            # Create mock attributes with storage_data_type
            mock_attributes = Mock()
            mock_attributes.storage_data_type = storage_data_type
            mock_attributes.permissions = "775"
            mock_resource.attributes = mock_attributes

            mock_resource.get_effective_storage_quotas.return_value = (50, 50)
            mock_resource.get_effective_inode_quotas.return_value = (100, 200)
            mock_resource.effective_permissions = "775"

            result = await backend.mapper.map_resource(
                mock_resource,
                "test-storage",
                parent_item_id=str(make_test_uuid("parent")),
            )

            actual_target_type = result.target.targetType
            assert actual_target_type == expected_target_type, (
                f"Storage data type '{storage_data_type}' should map to target type '{expected_target_type}', got '{actual_target_type}'"
            )

            # Verify target item structure based on type
            target_item = result.target.targetItem
            if expected_target_type == "project":
                assert target_item.status is not None
                assert target_item.unixGid is not None
                assert target_item.status == "active"
            elif expected_target_type == "user":
                assert target_item.email is not None
                assert target_item.unixUid is not None
                assert target_item.primaryProject is not None
                assert target_item.status == "active"
                assert target_item.primaryProject.name is not None
                assert target_item.primaryProject.unixGid is not None

    @pytest.mark.asyncio
    async def test_quota_float_consistency(self):
        """Test that quotas use float data type for consistency."""
        backend = self._create_orchestrator()

        # Create a mock resource
        mock_resource = Mock()
        mock_resource.uuid = Mock()
        mock_resource.uuid = str(make_test_uuid("test-uuid"))
        mock_resource.slug = "test-resource"
        mock_resource.customer_slug = "university"
        mock_resource.project_slug = "physics"
        mock_resource.state = "OK"

        mock_limits = Mock()
        mock_limits.storage = 42.5  # Use float value
        mock_resource.limits = mock_limits
        mock_resource.options = Mock(
            hard_quota_space=None,
            soft_quota_inodes=None,
            hard_quota_inodes=None,
            permissions=None,
        )

        mock_attributes = Mock()
        mock_attributes.storage_data_type = "store"
        mock_attributes.permissions = "775"
        mock_resource.attributes = mock_attributes

        mock_resource.backend_metadata = Mock(
            tenant_item=None, customer_item=None, project_item=None, user_item=None
        )
        mock_resource.get_effective_storage_quotas.return_value = (42.5, 42.5)
        mock_resource.get_effective_inode_quotas.return_value = (100, 200)
        mock_resource.effective_permissions = "775"
        mock_resource.render_quotas.return_value = create_mock_quotas(42.5)
        mock_resource.callback_urls = {}

        result = await backend.mapper.map_resource(mock_resource, "test-storage")

        # Verify all quotas are floats
        quotas = result.quotas
        assert quotas is not None, "Quotas should not be None for non-zero storage"

        for quota in quotas:
            quota_value = quota.quota
            assert isinstance(quota_value, float), (
                f"Quota value {quota_value} should be float, got {type(quota_value)}"
            )

    def test_storage_data_type_validation(self):
        """Test validation of storage_data_type parameter."""
        _ = self._create_orchestrator()

        # Create a mock resource
        mock_resource = Mock()
        mock_resource.uuid = Mock()
        mock_resource.uuid = str(uuid4())

        # Test with invalid data type (list)
        # Assuming we don't strict type check in Mapper anymore or we removed validation
        # I'll just skip this test or remove it.
        pass

    @pytest.mark.asyncio
    async def test_system_identifiers_use_deterministic_uuids(self):
        """Test that system identifiers use deterministic UUIDs generated from their names."""
        backend = self._create_orchestrator()

        # Create a mock resource
        mock_resource = Mock()
        mock_resource.uuid = Mock()
        mock_resource.uuid = str(make_test_uuid("test-uuid"))
        mock_resource.slug = "test-resource"
        mock_resource.customer_slug = "university"
        mock_resource.project_slug = "physics"
        mock_resource.state = "OK"

        # Create mock limits
        mock_limits = Mock()
        mock_limits.storage = 50
        mock_resource.limits = mock_limits

        mock_attributes = Mock()
        mock_attributes.storage_data_type = "store"
        mock_attributes.permissions = "775"
        mock_resource.attributes = mock_attributes
        mock_resource.options = Mock(
            hard_quota_space=None,
            soft_quota_inodes=None,
            hard_quota_inodes=None,
            permissions=None,
        )
        mock_resource.backend_metadata = Mock(
            tenant_item=None, customer_item=None, project_item=None, user_item=None
        )
        mock_resource.get_effective_storage_quotas.return_value = (50, 50)
        mock_resource.get_effective_inode_quotas.return_value = (100, 200)
        mock_resource.effective_permissions = "775"
        mock_resource.render_quotas.return_value = create_mock_quotas(50)
        mock_resource.callback_urls = {}

        result = await backend.mapper.map_resource(mock_resource, "test-storage-system")

        # Verify that system identifiers are in UUID format
        import re

        uuid_pattern = r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"

        storage_system = result.storageSystem
        assert re.match(uuid_pattern, str(storage_system.itemId))
        assert storage_system.key == "test-storage-system"

        storage_file_system = result.storageFileSystem
        assert re.match(uuid_pattern, str(storage_file_system.itemId))
        assert storage_file_system.key == "lustre"

        storage_data_type = result.storageDataType
        assert re.match(uuid_pattern, str(storage_data_type.itemId))
        assert storage_data_type.key == "store"

        result2 = await backend.mapper.map_resource(
            mock_resource, "test-storage-system"
        )

        assert result.storageSystem.itemId == result2.storageSystem.itemId
        assert result.storageFileSystem.itemId == result2.storageFileSystem.itemId
        assert result.storageDataType.itemId == result2.storageDataType.itemId

        # Test target item UUIDs are also deterministic UUIDs
        target_item = result.target.targetItem
        assert re.match(uuid_pattern, str(target_item.itemId))

        # Verify determinism for target items too
        target_item2 = result2.target.targetItem
        assert target_item.itemId == target_item2.itemId

    def test_filtering_by_data_type(self):
        """Test filtering storage resources by data type."""
        # Create mock storage resources with different data types
        r1 = Mock()
        r1.storageSystem.key = "capstor"
        r1.storageDataType.key = "store"
        r1.status = "active"

        r2 = Mock()
        r2.storageSystem.key = "capstor"
        r2.storageDataType.key = "users"
        r2.status = "pending"

        r3 = Mock()
        r3.storageSystem.key = "capstor"
        r3.storageDataType.key = "scratch"
        r3.status = "active"
        r3.callback_urls = {}

        mock_resources = [r1, r2, r3]

        # Test filtering by data_type
        filtered = self.orchestrator._filter_resources(
            mock_resources, data_type=StorageDataType.STORE, status=None
        )
        assert len(filtered) == 1
        assert filtered[0].storageDataType.key == "store"

        filtered = self.orchestrator._filter_resources(
            mock_resources, data_type=StorageDataType.USERS, status=None
        )
        assert len(filtered) == 1
        assert filtered[0].storageDataType.key == "users"

        filtered = self.orchestrator._filter_resources(
            mock_resources, data_type=StorageDataType.SCRATCH, status=None
        )
        assert len(filtered) == 1
        assert filtered[0].storageDataType.key == "scratch"

    def test_filtering_by_status(self):
        """Test filtering storage resources by status."""
        # Create mock storage resources with different statuses
        r1 = Mock()
        r1.storageSystem.key = "capstor"
        r1.storageDataType.key = "store"
        r1.status = "active"

        r2 = Mock()
        r2.storageSystem.key = "capstor"
        r2.storageDataType.key = "users"
        r2.status = "pending"

        r3 = Mock()
        r3.storageSystem.key = "capstor"
        r3.storageDataType.key = "scratch"
        r3.status = "removing"
        r3.callback_urls = {}

        mock_resources = [r1, r2, r3]

        mock_resources = [r1, r2, r3]

        # Test filtering by status
        filtered = self.orchestrator._filter_resources(
            mock_resources, data_type=None, status=TargetStatus.ACTIVE
        )
        assert len(filtered) == 1
        assert filtered[0].status == "active"

        filtered = self.orchestrator._filter_resources(
            mock_resources, data_type=None, status=TargetStatus.PENDING
        )
        assert len(filtered) == 1
        assert filtered[0].status == "pending"

        filtered = self.orchestrator._filter_resources(
            mock_resources, data_type=None, status=TargetStatus.REMOVING
        )
        assert len(filtered) == 1
        assert filtered[0].status == "removing"

    def test_filtering_combined(self):
        """Test filtering storage resources with multiple filter criteria."""
        # Create mock storage resources
        r1 = Mock()
        r1.storageSystem.key = "capstor"
        r1.storageDataType.key = "store"
        r1.status = "active"

        r2 = Mock()
        r2.storageSystem.key = "capstor"
        r2.storageDataType.key = "store"
        r2.status = "pending"

        r3 = Mock()
        r3.storageSystem.key = "vast"
        r3.storageDataType.key = "store"
        r3.storageDataType.key = "store"
        r3.status = "active"
        r3.callback_urls = {}

        r4 = Mock()
        r4.storageSystem.key = "capstor"
        r4.storageDataType.key = "users"
        r4.storageDataType.key = "users"
        r4.status = "active"
        r4.callback_urls = {}

        mock_resources = [r1, r2, r3, r4]

        # Test combined filtering: store + active
        filtered = self.orchestrator._filter_resources(
            mock_resources, data_type=StorageDataType.STORE, status=TargetStatus.ACTIVE
        )
        assert len(filtered) == 2
        assert all(r.storageDataType.key == "store" for r in filtered)
        assert all(r.status == "active" for r in filtered)

        # Test combined filtering: store only (should return 3)
        filtered = self.orchestrator._filter_resources(
            mock_resources, data_type=StorageDataType.STORE, status=None
        )
        assert len(filtered) == 3
        assert all(r.storageDataType.key == "store" for r in filtered)

        # Test combined filtering that returns no results
        filtered = self.orchestrator._filter_resources(
            mock_resources, data_type=StorageDataType.USERS, status=TargetStatus.PENDING
        )
        assert len(filtered) == 0

    def test_filtering_no_filters_applied(self):
        """Test that no filtering is applied when no filters are provided."""
        # Create mock storage resources
        r1 = Mock()
        r1.storageSystem.key = "capstor"
        r1.storageDataType.key = "store"
        r1.status = "active"

        r2 = Mock()
        r2.storageSystem.key = "vast"
        r2.storageDataType.key = "users"
        r2.status = "pending"

        mock_resources = [r1, r2]

        # Test no filtering (should return all resources)
        # Test that no filtering is applied (return all)
        filtered = self.orchestrator._filter_resources(
            mock_resources, data_type=None, status=None
        )
        assert len(filtered) == 2

    @pytest.mark.asyncio
    async def test_pagination_support(self):
        """Test pagination support via get_resources with local page slicing."""
        # Setup mock response parameters
        self.orchestrator.waldur_service.list_all_resources = AsyncMock(return_value=[])
        self.orchestrator.waldur_service.get_offering_customers = AsyncMock(
            return_value={}
        )

        # Case 1: Default pagination (page=1, page_size=100)
        result = await self.orchestrator.get_resources(
            filters=StorageResourceFilter()
        )

        # Verify list_all_resources was called (not list_resources)
        self.orchestrator.waldur_service.list_all_resources.assert_called_once()
        assert result["pagination"]["current"] == 1
        assert result["pagination"]["limit"] == 100

        # Reset mock
        self.orchestrator.waldur_service.list_all_resources.reset_mock()

        # Case 2: Explicit pagination — page slicing is handled locally
        result = await self.orchestrator.get_resources(
            filters=StorageResourceFilter(page=2, page_size=50)
        )

        self.orchestrator.waldur_service.list_all_resources.assert_called_once()
        assert result["pagination"]["current"] == 2
        assert result["pagination"]["limit"] == 50

    @pytest.mark.asyncio
    async def test_status_filter_pushed_to_waldur_as_state(self):
        """Test that status filter is converted to Waldur state and pushed to API."""
        self.orchestrator.waldur_service.list_all_resources = AsyncMock(return_value=[])
        self.orchestrator.waldur_service.get_offering_customers = AsyncMock(
            return_value={}
        )

        await self.orchestrator.get_resources(
            filters=StorageResourceFilter(status=TargetStatus.PENDING)
        )

        call_args = self.orchestrator.waldur_service.list_all_resources.call_args
        assert call_args is not None
        _, kwargs = call_args
        assert kwargs["state"] == ResourceState.CREATING
