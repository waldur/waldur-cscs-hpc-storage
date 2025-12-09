"""Tests for CSCS HPC Storage backend."""

from unittest.mock import Mock, patch
from uuid import uuid4

import pytest
from pydantic import ValidationError
from waldur_api_client.types import Unset
from waldur_api_client.models.order_state import OrderState
from waldur_cscs_hpc_storage.backend import (
    CscsHpcStorageBackend,
    make_storage_resource_predicate,
)
from waldur_cscs_hpc_storage.base.schemas import (
    ResourceAttributes,
)
from waldur_cscs_hpc_storage.base.enums import (
    EnforcementType,
    QuotaType,
    QuotaUnit,
    StorageDataType,
    TargetStatus,
)
from waldur_cscs_hpc_storage.base.models import Quota
from waldur_cscs_hpc_storage.config import (
    BackendConfig,
    HpcUserApiConfig,
    WaldurApiConfig,
)
from waldur_cscs_hpc_storage.base.mount_points import generate_project_mount_point
from waldur_api_client.models.resource_state import ResourceState


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


class TestCscsHpcStorageBackendBase:
    """Base test class for CSCS HPC Storage backend."""

    def setup_method(self):
        """Set up test environment."""
        self.backend_config = BackendConfig(
            storage_file_system="lustre",
            inode_soft_coefficient=1.5,
            inode_hard_coefficient=2.0,
            use_mock_target_items=True,
            development_mode=True,  # Enable development mode for tests
        )
        self.waldur_api_config = WaldurApiConfig(
            api_url="https://example.com", access_token="token"
        )
        self.backend = self._create_backend()

    def _create_backend(self, hpc_user_api_config=None):
        """Helper to create backend instance with mocks."""
        backend = CscsHpcStorageBackend(
            self.backend_config,
            waldur_api_config=self.waldur_api_config,
            hpc_user_api_config=hpc_user_api_config,
        )
        # Inject mock waldur_service for testing
        backend.waldur_service = Mock()
        return backend


class TestCscsHpcStorageBackend(TestCscsHpcStorageBackendBase):
    """Test cases for CSCS HPC Storage backend."""

    @pytest.fixture(autouse=True)
    def mock_gid_lookup(self):
        """Mock GID lookup for all tests in this class."""
        with patch.object(
            CscsHpcStorageBackend, "_get_project_unix_gid", return_value=30000
        ):
            yield

    def test_backend_initialization(self):
        """Test backend initialization."""
        assert self.backend.storage_file_system == "lustre"
        assert self.backend.inode_soft_coefficient == 1.5
        assert self.backend.inode_hard_coefficient == 2.0
        assert self.backend.use_mock_target_items is True

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

    def test_get_target_item_data_mock(self):
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

        target_data = self.backend._get_target_item_data(mock_resource, "project")

        assert target_data.name == "climate-sim"
        assert target_data.itemId is not None
        assert target_data.unixGid is not None
        assert target_data.status == "active"
        assert target_data.active is True

    def test_target_status_mapping_from_waldur_state(self):
        """Test that target item status correctly maps from Waldur resource states."""
        mock_resource = Mock()
        mock_resource.customer_slug = "university"
        mock_resource.customer_name = "University"
        mock_resource.project_slug = "physics-dept"
        mock_resource.project_name = "Physics Department"
        mock_resource.slug = "test-resource"
        mock_resource.uuid = str(uuid4())
        mock_resource.backend_metadata = Mock(additional_properties={})

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

            target_data = self.backend._get_target_item_data(mock_resource, "project")

            assert target_data.status == expected_status, (
                f"Waldur state '{waldur_state}' should map to status '{expected_status}', got '{target_data.status}'"
            )
            assert target_data.active == expected_active, (
                f"Waldur state '{waldur_state}' should set active={expected_active}, got {target_data.active}"
            )

    def test_create_storage_resource_json(self):
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
            soft_quota_space=None,
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

        storage_json = self.backend._create_storage_resource_json(
            mock_resource, "lustre-fs"
        )

        assert storage_json.itemId == mock_resource.uuid
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

    def test_create_storage_resource_json_with_provider_action_urls(self):
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
            soft_quota_space=None,
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

        storage_json = self.backend._create_storage_resource_json(
            mock_resource, "lustre-fs"
        )

        assert storage_json.itemId == mock_resource.uuid
        assert (
            storage_json.extra_fields["approve_by_provider_url"]
            == f"https://waldur.example.com/api/marketplace-orders/{order_uuid}/approve_by_provider/"
        )
        assert (
            storage_json.extra_fields["reject_by_provider_url"]
            == f"https://waldur.example.com/api/marketplace-orders/{order_uuid}/reject_by_provider/"
        )

    def test_create_storage_resource_json_without_provider_action_urls(self):
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
            soft_quota_space=None,
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

        # No order_in_progress
        mock_resource.order_in_progress = Unset()
        mock_resource.callback_urls = {}

        storage_json = self.backend._create_storage_resource_json(
            mock_resource, "lustre-fs"
        )

        assert storage_json.itemId == mock_resource.uuid
        assert "approve_by_provider_url" not in storage_json.extra_fields
        assert "reject_by_provider_url" not in storage_json.extra_fields

    def test_create_storage_resource_json_with_order_but_no_uuid(self):
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
            soft_quota_space=None,
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

        # Create mock order_in_progress without UUID
        # Create mock order_in_progress without UUID
        mock_order = Mock()
        mock_order.uuid = Unset()
        mock_resource.order_in_progress = mock_order
        mock_resource.callback_urls = {}

        storage_json = self.backend._create_storage_resource_json(
            mock_resource, "lustre-fs"
        )

        assert storage_json.itemId == mock_resource.uuid
        assert "approve_by_provider_url" not in storage_json.extra_fields
        assert "reject_by_provider_url" not in storage_json.extra_fields

    def test_invalid_storage_system_type_validation(self):
        """Test that non-string storage_system raises clear validation error."""
        backend = self._create_backend()

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

        # Create mock attributes
        mock_attributes = Mock()
        mock_resource.attributes = mock_attributes

        # Test with list storage_system (should raise TypeError)
        with pytest.raises(TypeError) as exc_info:
            backend._create_storage_resource_json(mock_resource, ["system1", "system2"])

        error_message = str(exc_info.value)
        assert "Invalid storage_system type" in error_message
        assert "expected string, got list" in error_message
        assert str(mock_resource.uuid) in error_message

        with pytest.raises(TypeError) as exc_info:
            backend._create_storage_resource_json(mock_resource, None)

        error_message = str(exc_info.value)
        assert "Invalid storage_system type" in error_message
        assert "expected string, got NoneType" in error_message

        with pytest.raises(TypeError) as exc_info:
            backend._create_storage_resource_json(mock_resource, "")

        error_message = str(exc_info.value)
        assert "Empty storage_system provided" in error_message
        assert "valid storage system name is required" in error_message

    def test_invalid_attribute_types_validation(self):
        """Test that non-string attribute values raise clear validation errors."""
        """Test that non-string attribute values raise clear validation errors."""
        # backend = self._create_backend() # Removed unused variable

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

        with pytest.raises(ValidationError):
            # Manually triggering validation by creating model with bad data
            ResourceAttributes(permissions=["775", "770"])  # type: ignore

    def test_invalid_storage_data_type(self):
        """Test invalid storage data type."""
        # The schema uses a validator that falls back to STORE on error.
        # So we expect it to NOT raise, but default to STORE.
        attr = ResourceAttributes(storage_data_type={"type": "store"})  # type: ignore
        assert attr.storage_data_type == StorageDataType.STORE

    def test_status_mapping_from_waldur_state(self):
        """Test that Waldur resource state is correctly mapped to CSCS status."""
        backend = self._create_backend()

        # Create a mock resource
        mock_resource = Mock()
        mock_resource.uuid = "test-uuid"
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
        mock_resource.backend_metadata = Mock(
            tenant_item=None, customer_item=None, project_item=None, user_item=None
        )
        mock_resource.get_effective_storage_quotas.return_value = (50, 50)
        mock_resource.get_effective_inode_quotas.return_value = (100, 200)
        mock_resource.effective_permissions = "775"

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

            result = backend._create_storage_resource_json(
                mock_resource, "test-storage"
            )

            assert result.status == expected_status, (
                f"State '{waldur_state}' should map to '{expected_status}'"
            )

    def test_dynamic_target_type_mapping(self):
        """Test that storage data type correctly maps to target type."""
        backend = self._create_backend()

        # Create a mock resource
        mock_resource = Mock()
        mock_resource.uuid = "test-uuid"
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

            result = backend._create_storage_resource_json(
                mock_resource, "test-storage"
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

    def test_quota_float_consistency(self):
        """Test that quotas use float data type for consistency."""
        backend = self._create_backend()

        # Create a mock resource
        mock_resource = Mock()
        mock_resource.uuid = Mock()
        mock_resource.uuid = "test-uuid"
        mock_resource.slug = "test-resource"
        mock_resource.customer_slug = "university"
        mock_resource.project_slug = "physics"
        mock_resource.state = "OK"

        # Create mock limits with non-zero storage
        mock_limits = Mock()
        mock_limits.storage = 42.5  # Use float value
        mock_resource.limits = mock_limits

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

        result = backend._create_storage_resource_json(mock_resource, "test-storage")

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
        backend = self._create_backend()

        # Create a mock resource
        mock_resource = Mock()
        mock_resource.uuid = Mock()
        mock_resource.uuid = str(uuid4())

        # Test with invalid data type (list)
        with pytest.raises(TypeError) as exc_info:
            backend._get_target_data(mock_resource, ["store", "archive"])  # type: ignore

        error_message = str(exc_info.value)
        assert "Invalid storage_data_type" in error_message
        assert "expected string, got list" in error_message
        assert str(mock_resource.uuid) in error_message

        # Test with None
        with pytest.raises(TypeError) as exc_info:
            backend._get_target_data(mock_resource, None)  # type: ignore

        error_message = str(exc_info.value)
        assert "Invalid storage_data_type" in error_message
        assert "expected string, got NoneType" in error_message

        # Test with valid but unknown storage_data_type (should log warning but not fail)
        # Mock _get_project_unix_gid
        with patch.object(self.backend, "_get_project_unix_gid", return_value=30000):
            result = backend._get_target_data(mock_resource, "unknown_type")
            assert result.targetType == "project"  # Should fallback to default

    def test_system_identifiers_use_deterministic_uuids(self):
        """Test that system identifiers use deterministic UUIDs generated from their names."""
        backend = self._create_backend()

        # Create a mock resource
        mock_resource = Mock()
        mock_resource.uuid = Mock()
        mock_resource.uuid = "test-uuid"
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
            soft_quota_space=None,
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

        result = backend._create_storage_resource_json(
            mock_resource, "test-storage-system"
        )

        # Verify that system identifiers are in UUID format
        import re

        uuid_pattern = r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"

        storage_system = result.storageSystem
        assert re.match(uuid_pattern, storage_system.itemId)
        assert storage_system.key == "test-storage-system"

        storage_file_system = result.storageFileSystem
        assert re.match(uuid_pattern, storage_file_system.itemId)
        assert storage_file_system.key == "lustre"

        storage_data_type = result.storageDataType
        assert re.match(uuid_pattern, storage_data_type.itemId)
        assert storage_data_type.key == "store"

        result2 = self.backend._create_storage_resource_json(
            mock_resource, "test-storage-system"
        )

        assert result.storageSystem.itemId == result2.storageSystem.itemId
        assert result.storageFileSystem.itemId == result2.storageFileSystem.itemId
        assert result.storageDataType.itemId == result2.storageDataType.itemId

        # Test target item UUIDs are also deterministic UUIDs
        target_item = result.target.targetItem
        assert re.match(uuid_pattern, target_item.itemId)

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

        mock_resources = [r1, r2, r3]

        # Test filtering by data_type
        predicate = make_storage_resource_predicate(data_type=StorageDataType.STORE)
        filtered = list(filter(predicate, mock_resources))
        assert len(filtered) == 1
        assert filtered[0].storageDataType.key == "store"

        predicate = make_storage_resource_predicate(data_type=StorageDataType.USERS)
        filtered = list(filter(predicate, mock_resources))
        assert len(filtered) == 1
        assert filtered[0].storageDataType.key == "users"

        predicate = make_storage_resource_predicate(data_type=StorageDataType.SCRATCH)
        filtered = list(filter(predicate, mock_resources))
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

        mock_resources = [r1, r2, r3]

        # Test filtering by status
        predicate = make_storage_resource_predicate(status=TargetStatus.ACTIVE)
        filtered = list(filter(predicate, mock_resources))
        assert len(filtered) == 1
        assert filtered[0].status == "active"

        predicate = make_storage_resource_predicate(status=TargetStatus.PENDING)
        filtered = list(filter(predicate, mock_resources))
        assert len(filtered) == 1
        assert filtered[0].status == "pending"

        predicate = make_storage_resource_predicate(status=TargetStatus.REMOVING)
        filtered = list(filter(predicate, mock_resources))
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
        r3.status = "active"

        r4 = Mock()
        r4.storageSystem.key = "capstor"
        r4.storageDataType.key = "users"
        r4.status = "active"

        mock_resources = [r1, r2, r3, r4]

        # Test combined filtering: store + active
        predicate = make_storage_resource_predicate(
            data_type=StorageDataType.STORE, status=TargetStatus.ACTIVE
        )
        filtered = list(filter(predicate, mock_resources))
        assert len(filtered) == 2
        assert all(r.storageDataType.key == "store" for r in filtered)
        assert all(r.status == "active" for r in filtered)

        # Test combined filtering: store only (should return 3)
        predicate = make_storage_resource_predicate(data_type=StorageDataType.STORE)
        filtered = list(filter(predicate, mock_resources))
        assert len(filtered) == 3
        assert all(r.storageDataType.key == "store" for r in filtered)

        # Test combined filtering that returns no results
        predicate = make_storage_resource_predicate(
            data_type=StorageDataType.USERS, status=TargetStatus.PENDING
        )
        filtered = list(filter(predicate, mock_resources))
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
        predicate = make_storage_resource_predicate()
        filtered = list(filter(predicate, mock_resources))
        assert len(filtered) == 2
        assert filtered == mock_resources


class TestHpcUserGidLookup(TestCscsHpcStorageBackendBase):
    """Test cases for GID lookup logic using HPC User API."""

    def setup_method(self):
        super().setup_method()
        # Initialize backend with HPC User API settings
        hpc_user_settings = HpcUserApiConfig(
            api_url="https://api.example.com",
            client_id="client",
            client_secret="secret",
            oidc_token_url="https://auth.example.com",
            oidc_scope="scope",
        )
        self.backend = self._create_backend(hpc_user_api_config=hpc_user_settings)
        # Mock the gid_service
        self.backend.gid_service = Mock()

    def test_get_project_unix_gid_success(self):
        """Test successful GID lookup."""
        self.backend.gid_service.get_project_unix_gid.return_value = 30042
        gid = self.backend._get_project_unix_gid("test-project")
        assert gid == 30042
        self.backend.gid_service.get_project_unix_gid.assert_called_once_with(
            "test-project"
        )

    def test_get_project_unix_gid_prod_failure(self):
        """Test lookup failure in production mode returns None."""
        self.backend.development_mode = False
        # The client catches exceptions and returns None, so we simulate that
        self.backend.gid_service.get_project_unix_gid.return_value = None

        gid = self.backend._get_project_unix_gid("test-project")
        assert gid is None
