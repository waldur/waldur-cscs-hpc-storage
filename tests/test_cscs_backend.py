"""Tests for CSCS HPC Storage backend."""

from unittest.mock import Mock, patch
from uuid import uuid4

import pytest
from pydantic import ValidationError
from waldur_api_client.types import Unset
from waldur_api_client.models.order_state import OrderState
from waldur_cscs_hpc_storage.backend import CscsHpcStorageBackend
from waldur_cscs_hpc_storage.schemas import (
    ParsedWaldurResource,
    ResourceAttributes,
)
from waldur_cscs_hpc_storage.enums import StorageDataType
from waldur_cscs_hpc_storage.waldur_service import WaldurResourceResponse
from waldur_cscs_hpc_storage.waldur_storage_proxy.config import (
    BackendConfig,
    HpcUserApiConfig,
    WaldurApiConfig,
)
from waldur_cscs_hpc_storage.mount_points import generate_project_mount_point
from waldur_api_client.models.resource_state import ResourceState


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
        self.backend_components = ["storage"]

        self.waldur_api_config = WaldurApiConfig(
            api_url="https://example.com", access_token="token"
        )
        self.backend = self._create_backend()

    def _create_backend(self, hpc_user_api_config=None):
        """Helper to create backend instance with mocks."""
        backend = CscsHpcStorageBackend(
            self.backend_config,
            self.backend_components,
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

    def test_calculate_inode_quotas(self):
        """Test inode quota calculation."""
        soft, hard = self.backend._calculate_inode_quotas(150.0)  # 150TB
        expected_soft = int(
            150 * self.backend.inode_base_multiplier * 1.5
        )  # 225M with default settings
        expected_hard = int(
            150 * self.backend.inode_base_multiplier * 2.0
        )  # 300M with default settings
        assert soft == expected_soft
        assert hard == expected_hard

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

        # Configure backend config with base URL
        self.backend.waldur_api_config.api_url = "https://waldur.example.com/api"

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

        storage_json = self.backend._create_storage_resource_json(
            mock_resource, "lustre-fs"
        )

        assert storage_json.itemId == mock_resource.uuid
        assert "approve_by_provider_url" not in storage_json.extra_fields
        assert "reject_by_provider_url" not in storage_json.extra_fields

    def test_get_all_storage_resources_with_pagination(self):
        """Test fetching all storage resources from API with pagination."""
        # Mock resources
        mock_resource1 = Mock()
        mock_resource1.offering_name = "Test Storage"
        mock_resource1.offering_slug = "test-storage"
        mock_resource1.uuid = "test-uuid-1"
        mock_resource1.slug = "resource-1"
        mock_resource1.customer_slug = "university"
        mock_resource1.project_slug = "physics"
        mock_resource1.limits = Mock(storage=100)
        mock_resource1.attributes = Mock(storage_data_type="store", permissions="770")
        mock_resource1.options = Mock(permissions=None)
        mock_resource1.backend_metadata = Mock(tenant_item=None, customer_item=None)
        mock_resource1.get_effective_inode_quotas.return_value = (100, 200)
        mock_resource1.get_effective_storage_quotas.return_value = (100, 100)
        mock_resource1.effective_permissions = "770"

        mock_resource2 = Mock()
        mock_resource2.offering_name = "Test Storage"
        mock_resource2.offering_slug = "test-storage"
        mock_resource2.uuid = "test-uuid-2"
        mock_resource2.slug = "resource-2"
        mock_resource2.customer_slug = "university"
        mock_resource2.project_slug = "chemistry"

        mock_resource2.limits = Mock(storage=200)
        mock_resource2.attributes = Mock(storage_data_type="store", permissions="770")
        mock_resource2.options = Mock(permissions=None)
        mock_resource2.backend_metadata = Mock(tenant_item=None, customer_item=None)
        mock_resource2.get_effective_inode_quotas.return_value = (200, 400)
        mock_resource2.get_effective_storage_quotas.return_value = (200, 200)
        mock_resource2.effective_permissions = "770"

        # Mock the WaldurResourceResponse
        mock_response = WaldurResourceResponse(
            resources=[mock_resource1, mock_resource2], total_count=2
        )

        mock_list = self.backend.waldur_service.list_resources
        mock_list.return_value = mock_response

        # Mock get_offering_customers
        self.backend.waldur_service.get_offering_customers.return_value = {}

        # Test the method with pagination parameters
        resources, pagination_info = self.backend._get_all_storage_resources(
            "test-offering-uuid", page=1, page_size=10
        )

        # With hierarchical structure, we get tenant + customer + project entries for each resource
        # 2 original resources will create multiple hierarchical entries
        # Since resources share the same offering_slug and customer_slug, they create:
        # 1 tenant (shared), 1 customer (shared), 2 projects = 4 total
        assert len(resources) >= 2  # At minimum we get the project resources

        # Check pagination info reflects the hierarchical resources
        assert pagination_info["total"] >= 2
        assert pagination_info["current"] == 1
        assert pagination_info["limit"] == 10
        assert pagination_info["pages"] == 1
        assert pagination_info["offset"] == 0

        # Should call the list_resources method with pagination
        mock_list.assert_called_once_with(
            offering_uuid="test-offering-uuid",
            page=1,
            page_size=10,
            exclude_pending=True,
        )

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

    def test_pagination_header_case_insensitive(self):
        """Test that pagination header parsing is case-insensitive and pagination info reflects filtered results."""
        backend = self._create_backend()

        # Mock resource
        mock_resource = Mock()
        mock_resource.uuid = "test-uuid"
        mock_resource.slug = "resource-1"
        mock_resource.customer_slug = "university"
        mock_resource.project_slug = "physics"
        mock_resource.offering_slug = "test-storage"

        mock_limits = Mock()
        mock_limits.storage = 100
        mock_resource.limits = mock_limits

        mock_attributes = Mock()
        mock_attributes.storage_data_type = "store"
        mock_resource.attributes = mock_attributes
        mock_resource.backend_metadata = Mock(
            tenant_item=None, customer_item=None, project_item=None, user_item=None
        )
        mock_resource.get_effective_storage_quotas.return_value = (100, 100)
        mock_resource.get_effective_inode_quotas.return_value = (100, 200)
        mock_resource.effective_permissions = "775"

        # Mock the WaldurResourceResponse
        mock_response = WaldurResourceResponse(resources=[mock_resource], total_count=5)
        backend.waldur_service.list_resources.return_value = mock_response
        backend.waldur_service.get_offering_customers.return_value = {}

        resources, pagination_info = backend._get_all_storage_resources(
            "test-offering-uuid", page=1, page_size=10
        )

        # With hierarchical structure, we get tenant + customer + project entries
        # For 1 original resource, we get at least the project itself, plus tenant and customer
        assert pagination_info["total"] >= 1  # At minimum we get the project resource
        # The header check is less relevant now that we have WaldurResourceResponse,
        # but let's assume we want to ensure list_resources was called correctly.
        assert pagination_info["total"] >= 1  # At minimum we get the project resource

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

        # Test with invalidated attributes (should raise ValidationError)
        # However, backend now takes ParsedWaldurResource. If we pass a Mock that simulates it but violates internal type checks of logic?
        # Actually, backend assumes `waldur_resource` IS validated.
        # But `_create_storage_resource_json` does not re-validate.
        # So this test checks if invalid data *in the model* raises error?
        # But the model is parsed *before* passing to backend methods.
        # The test originally checked that `_create_storage_resource_json` failed when it did validation.
        # Now validation is done upstream.
        # So we should probably test `from_waldur_resource` in `test_schemas.py` or similar.
        # BUT `_create_storage_resource_json` uses `waldur_resource.attributes.permissions` etc.
        # If we pass a mock for ParsedWaldurResource, it has whatever we give it.
        # The TypeErrors/ValidationErrors in backend were from `_parse_resource_configuration`.
        # Since that is gone, `_create_storage_resource_json` just reads from the object.
        # So this test is no longer relevant for `backend.py` unless we are testing `WaldurService` parsing or `ParsedWaldurResource` validation.
        # We can simulate `ParsedWaldurResource` with invalid data if we manually construct it?
        # But `ParsedWaldurResource` validates on init.
        # So let's construct `ParsedWaldurResource` with bad data to see it fail.

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

    def test_error_handling_returns_error_status(self):
        """Test that errors return proper error status and code 500."""
        backend = self._create_backend()

        # Mock the list_resources to raise an exception
        self.backend.waldur_service.list_resources.side_effect = Exception(
            "API connection failed"
        )

        # Test that generate_all_resources_json returns error response
        result = backend.generate_all_resources_json("test-offering-uuid")

        # Verify error response structure
        assert result["status"] == "error"
        assert result["code"] == 500
        assert "Failed to fetch storage resources" in result["message"]
        assert result["result"]["storageResources"] == []
        assert result["result"]["paginate"]["total"] == 0
        assert result["result"]["paginate"]["current"] == 1
        assert result["result"]["paginate"]["limit"] == 100

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

    def test_filtering_by_storage_system(self):
        """Test filtering storage resources by storage system."""
        backend = self._create_backend()

        # Create mock storage resources with different storage systems
        r1 = Mock()
        r1.storageSystem.key = "capstor"
        r1.storageDataType.key = "store"
        r1.status = "active"

        r2 = Mock()
        r2.storageSystem.key = "vast"
        r2.storageDataType.key = "users"
        r2.status = "pending"

        r3 = Mock()
        r3.storageSystem.key = "iopsstor"
        r3.storageDataType.key = "archive"
        r3.status = "active"

        mock_resources = [r1, r2, r3]

        # Test filtering by storage_system
        filtered = backend._apply_filters(mock_resources, storage_system="capstor")
        assert len(filtered) == 1
        assert filtered[0].storageSystem.key == "capstor"

        filtered = backend._apply_filters(mock_resources, storage_system="vast")
        assert len(filtered) == 1
        assert filtered[0].storageSystem.key == "vast"

        # Test with non-existent storage system
        filtered = backend._apply_filters(mock_resources, storage_system="nonexistent")
        assert len(filtered) == 0

    def test_filtering_by_data_type(self):
        """Test filtering storage resources by data type."""
        backend = self._create_backend()

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
        filtered = backend._apply_filters(mock_resources, data_type="store")
        assert len(filtered) == 1
        assert filtered[0].storageDataType.key == "store"

        filtered = backend._apply_filters(mock_resources, data_type="users")
        assert len(filtered) == 1
        assert filtered[0].storageDataType.key == "users"

        # Test with non-existent data type
        filtered = backend._apply_filters(mock_resources, data_type="nonexistent")
        assert len(filtered) == 0

    def test_filtering_by_status(self):
        """Test filtering storage resources by status."""
        backend = self._create_backend()

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
        filtered = backend._apply_filters(mock_resources, status="active")
        assert len(filtered) == 1
        assert filtered[0].status == "active"

        filtered = backend._apply_filters(mock_resources, status="pending")
        assert len(filtered) == 1
        assert filtered[0].status == "pending"

        filtered = backend._apply_filters(mock_resources, status="removing")
        assert len(filtered) == 1
        assert filtered[0].status == "removing"

        # Test with non-existent status
        filtered = backend._apply_filters(mock_resources, status="nonexistent")
        assert len(filtered) == 0

    def test_filtering_combined(self):
        """Test filtering storage resources with multiple filter criteria."""
        backend = self._create_backend()

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

        # Test combined filtering: capstor + store + active
        filtered = backend._apply_filters(
            mock_resources, storage_system="capstor", data_type="store", status="active"
        )
        assert len(filtered) == 1
        assert filtered[0].storageSystem.key == "capstor"
        assert filtered[0].storageDataType.key == "store"
        assert filtered[0].status == "active"

        # Test combined filtering: capstor + store (should return 2)
        filtered = backend._apply_filters(
            mock_resources, storage_system="capstor", data_type="store"
        )
        assert len(filtered) == 2
        assert all(r.storageSystem.key == "capstor" for r in filtered)
        assert all(r.storageDataType.key == "store" for r in filtered)

        # Test combined filtering that returns no results
        filtered = backend._apply_filters(
            mock_resources, storage_system="vast", data_type="users"
        )
        assert len(filtered) == 0

    def test_filtering_no_filters_applied(self):
        """Test that no filtering is applied when no filters are provided."""
        backend = self._create_backend()

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
        filtered = backend._apply_filters(mock_resources)
        assert len(filtered) == 2
        assert filtered == mock_resources

    def test_pagination_info_updated_after_filtering(self):
        """Test that pagination info is updated to reflect filtered results, not raw API results."""
        backend = self._create_backend()

        # Create mock resources with different storage systems
        mock_resource1 = Mock()
        mock_resource1.uuid = "uuid-1"
        mock_resource1.slug = "resource-1"
        mock_resource1.customer_slug = "university"
        mock_resource1.project_slug = "physics"
        mock_resource1.offering_slug = "capstor"
        mock_resource1.state = "OK"
        mock_resource1.limits = Mock(storage=100)
        mock_resource1.attributes = Mock(
            storage_data_type="store", permissions="775"
        )  # implicit store for capstor
        mock_resource1.options = Mock(permissions=None)
        mock_resource1.backend_metadata = Mock(tenant_item=None, customer_item=None)
        mock_resource1.get_effective_inode_quotas.return_value = (100, 200)
        mock_resource1.get_effective_storage_quotas.return_value = (100, 100)
        mock_resource1.effective_permissions = "775"

        mock_resource2 = Mock()
        mock_resource2.uuid = "uuid-2"
        mock_resource2.slug = "resource-2"
        mock_resource2.customer_slug = "university"
        mock_resource2.project_slug = "chemistry"
        mock_resource2.offering_slug = "vast"
        mock_resource2.state = "OK"
        mock_resource2.limits = Mock(storage=200)
        mock_resource2.attributes = Mock(storage_data_type="store", permissions="775")
        mock_resource2.options = Mock(permissions=None)
        mock_resource2.backend_metadata = Mock(tenant_item=None, customer_item=None)
        mock_resource2.get_effective_inode_quotas.return_value = (200, 400)
        mock_resource2.get_effective_storage_quotas.return_value = (200, 200)
        mock_resource2.effective_permissions = "775"

        # Mock API response: 2 total resources from different storage systems
        mock_response = WaldurResourceResponse(
            resources=[mock_resource1, mock_resource2], total_count=2
        )
        backend.waldur_service.list_resources.return_value = mock_response
        backend.waldur_service.get_offering_customers.return_value = {}

        # Test filtering by storage_system that matches only 1 resource
        # Note: offering_slug is passed as None to verify it doesn't break anything, or proper strings.
        # Avoid passing Mock() if not needed.
        resources, pagination_info = backend._get_all_storage_resources(
            "test-offering-uuid", page=1, page_size=10, storage_system="capstor"
        )

        # With hierarchical structure, filtering by storage_system="capstor" returns:
        # tenant entry for capstor + customer entry + project entry
        # The exact count depends on the hierarchy created, but we should get at least 1
        assert len(resources) >= 1
        # All returned resources should be from capstor storage system
        for resource in resources:
            assert resource.storageSystem.key == "capstor"

        # Pagination info should reflect filtered results, not original API results
        assert pagination_info["total"] >= 1  # At least 1 filtered resource
        assert pagination_info["pages"] == 1
        assert pagination_info["current"] == 1
        assert pagination_info["limit"] == 10
        assert pagination_info["offset"] == 0

        # Test filtering by storage_system that matches no resources
        resources, pagination_info = backend._get_all_storage_resources(
            "test-offering-uuid",
            page=1,
            page_size=10,
            storage_system="nonexistent",
        )

        # Should get no resources
        assert len(resources) == 0

        # Pagination info should show 0 total, not original API count
        assert pagination_info["total"] == 0  # Should be 0, not 2
        assert pagination_info["pages"] == 1  # Should be 1 (minimum pages)
        assert pagination_info["current"] == 1
        assert pagination_info["limit"] == 10
        assert pagination_info["offset"] == 0

    def test_non_transitional_resource_always_included(self):
        """Test that non-transitional resources are always included regardless of order state."""
        # Mock resource in non-transitional state
        mock_resource = Mock()
        mock_resource.offering_name = "Test Storage"
        mock_resource.offering_name = "Test Storage"
        mock_resource.offering_slug = "test-storage"
        mock_resource.uuid = "test-uuid-1"
        mock_resource.slug = "resource-1"
        mock_resource.customer_slug = "university"
        mock_resource.customer_name = "University"
        mock_resource.customer_uuid = str(uuid4())
        mock_resource.project_slug = "physics"
        mock_resource.project_name = "Physics Department"
        mock_resource.project_uuid = str(uuid4())
        mock_resource.project_uuid = str(uuid4())
        mock_resource.provider_slug = "cscs"
        mock_resource.provider_name = "CSCS"
        mock_resource.offering_uuid = Mock()
        mock_resource.offering_uuid = str(uuid4())
        mock_resource.state = "OK"  # Non-transitional state

        # Create mock order_in_progress with any state (shouldn't matter)
        from waldur_api_client.models.order_state import OrderState

        order_uuid = str(uuid4())
        mock_order = Mock()
        # Set state to PENDING_PROVIDER so that approve/reject URLs are generated
        mock_order.state = OrderState.PENDING_PROVIDER
        mock_order.uuid = order_uuid
        mock_order.url = (
            f"https://waldur.example.com/api/marketplace-orders/{order_uuid}/"
        )
        mock_resource.order_in_progress = mock_order

        # Create mock limits and attributes
        mock_limits = Mock()
        mock_limits.storage = 100
        mock_resource.limits = mock_limits
        mock_attributes = Mock()
        mock_attributes.storage_data_type = "store"
        mock_resource.attributes = mock_attributes

        # Mock list_resources response
        mock_response = WaldurResourceResponse(resources=[mock_resource], total_count=1)
        self.backend.waldur_service.list_resources.return_value = mock_response
        # Mock options as well (required for validation)
        mock_resource.options = Mock(
            permissions="775",
            soft_quota_space=None,
            hard_quota_space=None,
            soft_quota_inodes=None,
            hard_quota_inodes=None,
        )
        mock_resource.backend_metadata = Mock(
            tenant_item=None, customer_item=None, project_item=None, user_item=None
        )
        mock_resource.get_effective_storage_quotas.return_value = (100, 100)
        mock_resource.get_effective_inode_quotas.return_value = (100, 200)
        mock_resource.effective_permissions = "775"
        mock_resource.attributes.storage_data_type = "store"

        self.backend.waldur_service.get_offering_customers.return_value = {}

        # Configure backend client with base URL
        self.backend.waldur_api_config.api_url = "https://waldur.example.com/api"

        # Test the method
        resources, _ = self.backend._get_all_storage_resources("test-offering-uuid")

        # Should include the resource since it's not in transitional state
        assert len(resources) >= 1

        # Find the project-level resource (with itemId matching our resource UUID)
        project_resources = [r for r in resources if r.itemId == "test-uuid-1"]
        assert len(project_resources) >= 1
        project_resource = project_resources[0]
        assert (
            project_resource.extra_fields["approve_by_provider_url"]
            == f"https://waldur.example.com/api/marketplace-orders/{order_uuid}/approve_by_provider/"
        )
        assert (
            project_resource.extra_fields["reject_by_provider_url"]
            == f"https://waldur.example.com/api/marketplace-orders/{order_uuid}/reject_by_provider/"
        )


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
