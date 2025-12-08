"""Tests for CSCS HPC Storage backend."""

from unittest.mock import Mock, patch
from uuid import uuid4

import pytest
from waldur_api_client.types import Unset
from waldur_api_client.models.order_state import OrderState
from waldur_cscs_hpc_storage.backend import CscsHpcStorageBackend
from waldur_cscs_hpc_storage.waldur_storage_proxy.config import (
    BackendConfig,
    HpcUserApiConfig,
    WaldurApiConfig,
)
from waldur_cscs_hpc_storage.exceptions import BackendError


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
        # Inject mock client for testing
        backend._client = Mock()
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
        mount_point = self.backend._generate_mount_point(
            storage_system="lustre-fs",
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
        mock_resource.customer_uuid.hex = str(uuid4())
        mock_resource.project_slug = "physics-dept"
        mock_resource.project_name = "Physics Department"
        mock_resource.project_uuid = Mock()
        mock_resource.project_uuid.hex = str(uuid4())
        mock_resource.slug = "climate-sim"
        mock_resource.uuid = Mock()
        mock_resource.uuid.hex = str(uuid4())
        mock_resource.state = "OK"  # Set state to map to "active" status

        target_data = self.backend._get_target_item_data(mock_resource, "project")

        assert target_data["name"] == "climate-sim"
        assert "itemId" in target_data
        assert "unixGid" in target_data
        assert target_data["status"] == "active"
        assert target_data["active"] is True

    def test_target_status_mapping_from_waldur_state(self):
        """Test that target item status correctly maps from Waldur resource states."""
        mock_resource = Mock()
        mock_resource.customer_slug = "university"
        mock_resource.customer_name = "University"
        mock_resource.project_slug = "physics-dept"
        mock_resource.project_name = "Physics Department"
        mock_resource.slug = "test-resource"
        mock_resource.uuid = Mock()
        mock_resource.uuid.hex = str(uuid4())

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

            assert target_data["status"] == expected_status, (
                f"Waldur state '{waldur_state}' should map to status '{expected_status}', got '{target_data['status']}'"
            )
            assert target_data["active"] == expected_active, (
                f"Waldur state '{waldur_state}' should set active={expected_active}, got {target_data['active']}"
            )

    def test_create_storage_resource_json(self):
        """Test storage resource JSON creation."""
        mock_resource = Mock()
        mock_resource.uuid = Mock()
        mock_resource.uuid.hex = str(uuid4())
        mock_resource.name = "Test Storage"
        mock_resource.slug = "test-storage"
        mock_resource.provider_slug = "cscs"
        mock_resource.customer_slug = "university"
        mock_resource.customer_name = "University"
        mock_resource.customer_uuid = Mock()
        mock_resource.customer_uuid.hex = str(uuid4())
        mock_resource.project_slug = "physics-dept"
        mock_resource.project_name = "Physics Department"
        mock_resource.project_uuid = Mock()
        mock_resource.project_uuid.hex = str(uuid4())
        # Create mock limits with additional_properties
        mock_limits = Mock()
        mock_limits.additional_properties = {"storage": 150}  # 150TB
        mock_resource.limits = mock_limits

        # Create mock attributes with additional_properties
        mock_attributes = Mock()
        mock_attributes.additional_properties = {"permissions": "2770"}
        mock_resource.attributes = mock_attributes

        storage_json = self.backend._create_storage_resource_json(
            mock_resource, "lustre-fs"
        )

        assert storage_json["itemId"] == mock_resource.uuid.hex
        assert storage_json["status"] == "pending"
        assert (
            storage_json["mountPoint"]["default"]
            == "/lustre-fs/store/cscs/university/physics-dept"
        )
        assert storage_json["permission"]["value"] == "2770"
        assert len(storage_json["quotas"]) == 4  # 2 space + 2 inode quotas
        assert storage_json["storageSystem"]["key"] == "lustre-fs"
        assert storage_json["storageFileSystem"]["key"] == "lustre"
        assert storage_json["target"]["targetItem"]["unixGid"] == 30000

    def test_create_storage_resource_json_with_provider_action_urls(self):
        """Test storage resource JSON creation includes provider action URLs when available."""
        mock_resource = Mock()
        mock_resource.uuid = Mock()
        mock_resource.uuid.hex = str(uuid4())
        mock_resource.name = "Test Storage"
        mock_resource.slug = "test-storage"
        mock_resource.provider_slug = "cscs"
        mock_resource.customer_slug = "university"
        mock_resource.customer_name = "University"
        mock_resource.customer_uuid = Mock()
        mock_resource.customer_uuid.hex = str(uuid4())
        mock_resource.project_slug = "physics-dept"
        mock_resource.project_name = "Physics Department"
        mock_resource.project_uuid = Mock()
        mock_resource.project_uuid.hex = str(uuid4())

        # Create mock limits with additional_properties
        mock_limits = Mock()
        mock_limits.additional_properties = {"storage": 150}  # 150TB
        mock_resource.limits = mock_limits

        # Create mock attributes with additional_properties
        mock_attributes = Mock()
        mock_attributes.additional_properties = {"permissions": "2770"}
        mock_resource.attributes = mock_attributes

        # Create mock order_in_progress
        order_uuid = str(uuid4())
        mock_order = Mock()
        mock_order.uuid = order_uuid
        # Set state to PENDING_PROVIDER so that approve/reject URLs are generated
        mock_order.state = OrderState.PENDING_PROVIDER
        mock_resource.order_in_progress = mock_order

        # Configure backend client with base URL
        mock_httpx_client = Mock()
        mock_httpx_client.base_url = "https://waldur.example.com/api"
        self.backend.waldur_api_config.api_url = "https://waldur.example.com/api"
        self.backend._client.get_httpx_client.return_value = mock_httpx_client

        storage_json = self.backend._create_storage_resource_json(
            mock_resource, "lustre-fs"
        )

        assert storage_json["itemId"] == mock_resource.uuid.hex
        assert (
            storage_json["approve_by_provider_url"]
            == f"https://waldur.example.com/api/marketplace-orders/{order_uuid}/approve_by_provider/"
        )
        assert (
            storage_json["reject_by_provider_url"]
            == f"https://waldur.example.com/api/marketplace-orders/{order_uuid}/reject_by_provider/"
        )

    def test_create_storage_resource_json_without_provider_action_urls(self):
        """Test storage resource JSON creation without provider action URLs when not available."""
        mock_resource = Mock()
        mock_resource.uuid = Mock()
        mock_resource.uuid.hex = str(uuid4())
        mock_resource.name = "Test Storage"
        mock_resource.slug = "test-storage"
        mock_resource.provider_slug = "cscs"
        mock_resource.customer_slug = "university"
        mock_resource.customer_name = "University"
        mock_resource.customer_uuid = Mock()
        mock_resource.customer_uuid.hex = str(uuid4())
        mock_resource.project_slug = "physics-dept"
        mock_resource.project_name = "Physics Department"
        mock_resource.project_uuid = Mock()
        mock_resource.project_uuid.hex = str(uuid4())

        # Create mock limits with additional_properties
        mock_limits = Mock()
        mock_limits.additional_properties = {"storage": 150}  # 150TB
        mock_resource.limits = mock_limits

        # Create mock attributes with additional_properties
        mock_attributes = Mock()
        mock_attributes.additional_properties = {"permissions": "2770"}
        mock_resource.attributes = mock_attributes

        # No order_in_progress
        mock_resource.order_in_progress = Unset()

        storage_json = self.backend._create_storage_resource_json(
            mock_resource, "lustre-fs"
        )

        assert storage_json["itemId"] == mock_resource.uuid.hex
        assert "approve_by_provider_url" not in storage_json
        assert "reject_by_provider_url" not in storage_json

    def test_create_storage_resource_json_with_order_but_no_uuid(self):
        """Test storage resource JSON creation when order exists but has no UUID."""
        mock_resource = Mock()
        mock_resource.uuid = Mock()
        mock_resource.uuid.hex = str(uuid4())
        mock_resource.name = "Test Storage"
        mock_resource.slug = "test-storage"
        mock_resource.provider_slug = "cscs"
        mock_resource.customer_slug = "university"
        mock_resource.customer_name = "University"
        mock_resource.customer_uuid = Mock()
        mock_resource.customer_uuid.hex = str(uuid4())
        mock_resource.project_slug = "physics-dept"
        mock_resource.project_name = "Physics Department"
        mock_resource.project_uuid = Mock()
        mock_resource.project_uuid.hex = str(uuid4())

        # Create mock limits with additional_properties
        mock_limits = Mock()
        mock_limits.additional_properties = {"storage": 150}  # 150TB
        mock_resource.limits = mock_limits

        # Create mock attributes with additional_properties
        mock_attributes = Mock()
        mock_attributes.additional_properties = {"permissions": "2770"}
        mock_resource.attributes = mock_attributes

        # Create mock order_in_progress without UUID
        # Create mock order_in_progress without UUID
        mock_order = Mock()
        mock_order.uuid = Unset()
        mock_resource.order_in_progress = mock_order

        storage_json = self.backend._create_storage_resource_json(
            mock_resource, "lustre-fs"
        )

        assert storage_json["itemId"] == mock_resource.uuid.hex
        assert "approve_by_provider_url" not in storage_json
        assert "reject_by_provider_url" not in storage_json

    @patch("waldur_cscs_hpc_storage.backend.marketplace_resources_list")
    def test_get_all_storage_resources_with_pagination(self, mock_list):
        """Test fetching all storage resources from API with pagination."""
        # Mock resources
        mock_resource1 = Mock()
        mock_resource1.offering_name = "Test Storage"
        mock_resource1.offering_slug = "test-storage"
        mock_resource1.uuid.hex = "test-uuid-1"
        mock_resource1.slug = "resource-1"
        mock_resource1.customer_slug = "university"
        mock_resource1.project_slug = "physics"
        # Create mock limits with additional_properties for resource1
        mock_limits1 = Mock()
        mock_limits1.additional_properties = {"storage": 100}
        mock_resource1.limits = mock_limits1

        # Create mock attributes with additional_properties for resource1
        mock_attributes1 = Mock()
        mock_attributes1.additional_properties = {}
        mock_resource1.attributes = mock_attributes1

        mock_resource2 = Mock()
        mock_resource2.offering_name = "Test Storage"
        mock_resource2.offering_slug = "test-storage"
        mock_resource2.uuid.hex = "test-uuid-2"
        mock_resource2.slug = "resource-2"
        mock_resource2.customer_slug = "university"
        mock_resource2.project_slug = "chemistry"

        # Create mock limits with additional_properties for resource2
        mock_limits2 = Mock()
        mock_limits2.additional_properties = {"storage": 200}
        mock_resource2.limits = mock_limits2

        # Create mock attributes with additional_properties for resource2
        mock_attributes2 = Mock()
        mock_attributes2.additional_properties = {}
        mock_resource2.attributes = mock_attributes2

        # Mock the sync_detailed response
        mock_response = Mock()
        mock_response.parsed = [mock_resource1, mock_resource2]
        # Mock headers as a dict-like object (httpx.Headers behavior)
        mock_headers = Mock()
        mock_headers.get = Mock(return_value="2")
        mock_response.headers = mock_headers
        mock_list.sync_detailed.return_value = mock_response

        # Mock API client with base URL
        # Configure backend client with base URL
        mock_httpx_client = Mock()
        mock_httpx_client.base_url = "https://waldur.example.com/api"
        self.backend._client.get_httpx_client.return_value = mock_httpx_client

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

        # Should call the sync_detailed endpoint with pagination
        mock_list.sync_detailed.assert_called_once_with(
            client=self.backend._client,
            offering_uuid=["test-offering-uuid"],
            page=1,
            page_size=10,
            exclude_pending_transitional=True,
        )

    def test_unset_offering_slug_validation(self):
        """Test that Unset offering_slug raises a clear validation error."""
        backend = self._create_backend()

        # Create a mock resource with Unset offering_slug
        mock_resource = Mock()
        mock_resource.uuid = Mock()
        mock_resource.uuid.hex = str(uuid4())
        mock_resource.slug = "test-resource"
        mock_resource.name = "Test Resource"
        mock_resource.offering_slug = Unset()  # This should raise a validation error
        mock_resource.customer_slug = "university"
        mock_resource.project_slug = "physics-dept"

        # Test that validation raises BackendError with clear message
        with pytest.raises(BackendError) as exc_info:
            backend._validate_resource_data(mock_resource)

        error_message = str(exc_info.value)
        assert "offering_slug" in error_message
        assert "missing required fields" in error_message
        assert (
            "test-resource" in error_message
        )  # Should include resource ID for context

    def test_multiple_unset_fields_validation(self):
        """Test validation error when multiple fields are Unset."""
        backend = self._create_backend()

        # Create a mock resource with multiple Unset fields
        mock_resource = Mock()
        mock_resource.uuid = Mock()
        mock_resource.uuid.hex = str(uuid4())
        mock_resource.slug = "test-resource"
        mock_resource.offering_slug = Unset()
        mock_resource.customer_slug = Unset()
        mock_resource.project_slug = Unset()

        # Test that validation raises BackendError listing all missing fields
        with pytest.raises(BackendError) as exc_info:
            backend._validate_resource_data(mock_resource)

        error_message = str(exc_info.value)
        assert "offering_slug" in error_message
        assert "customer_slug" in error_message
        assert "project_slug" in error_message
        assert "test-resource" in error_message

    def test_invalid_storage_system_type_validation(self):
        """Test that non-string storage_system raises clear validation error."""
        backend = self._create_backend()

        # Create a mock resource
        mock_resource = Mock()
        mock_resource.uuid = Mock()
        mock_resource.uuid.hex = str(uuid4())
        mock_resource.name = "Test Resource"
        mock_resource.slug = "test-resource"
        mock_resource.customer_slug = "university"
        mock_resource.project_slug = "physics"

        # Create mock limits
        mock_limits = Mock()
        mock_limits.additional_properties = {"storage": 50}
        mock_resource.limits = mock_limits

        # Create mock attributes
        mock_attributes = Mock()
        mock_attributes.additional_properties = {}
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
        mock_resource.uuid = Mock()
        mock_resource.uuid.hex = "test-uuid"
        mock_resource.slug = "resource-1"
        mock_resource.customer_slug = "university"
        mock_resource.project_slug = "physics"
        mock_resource.offering_slug = "test-storage"

        mock_limits = Mock()
        mock_limits.additional_properties = {"storage": 100}
        mock_resource.limits = mock_limits

        mock_attributes = Mock()
        mock_attributes.additional_properties = {}
        mock_resource.attributes = mock_attributes

        # Test with different header case variations
        with patch(
            "waldur_cscs_hpc_storage.backend.marketplace_resources_list"
        ) as mock_list:
            mock_response = Mock()
            mock_response.parsed = [mock_resource]
            # httpx.Headers is case-insensitive, so we just need to test it returns the right value
            mock_headers = Mock()
            mock_headers.get = Mock(return_value="5")
            mock_response.headers = mock_headers
            mock_list.sync_detailed.return_value = mock_response

            resources, pagination_info = backend._get_all_storage_resources(
                "test-offering-uuid", Mock(), page=1, page_size=10
            )

            # With hierarchical structure, we get tenant + customer + project entries
            # For 1 original resource, we get at least the project itself, plus tenant and customer
            assert (
                pagination_info["total"] >= 1
            )  # At minimum we get the project resource
            # Verify that get was called with lowercase key (httpx normalizes to lowercase)
            mock_headers.get.assert_called_with("x-result-count")

    def test_invalid_attribute_types_validation(self):
        """Test that non-string attribute values raise clear validation errors."""
        backend = self._create_backend()

        # Create a mock resource
        mock_resource = Mock()
        mock_resource.uuid = Mock()
        mock_resource.uuid.hex = str(uuid4())
        mock_resource.name = "Test Resource"
        mock_resource.slug = "test-resource"
        mock_resource.customer_slug = "university"
        mock_resource.project_slug = "physics"

        # Create mock limits
        mock_limits = Mock()
        mock_limits.additional_properties = {"storage": 50}
        mock_resource.limits = mock_limits

        # Test with list permissions (should raise TypeError)
        mock_attributes = Mock()
        mock_attributes.additional_properties = {"permissions": ["775", "770"]}
        mock_resource.attributes = mock_attributes

        with pytest.raises(TypeError) as exc_info:
            backend._create_storage_resource_json(mock_resource, "test-storage")

        error_message = str(exc_info.value)
        assert "Invalid permissions type" in error_message
        assert "expected string or None, got list" in error_message
        assert str(mock_resource.uuid) in error_message

        # Test with dict storage_data_type (should raise TypeError)
        mock_attributes.additional_properties = {"storage_data_type": {"type": "store"}}
        mock_resource.attributes = mock_attributes

        with pytest.raises(TypeError) as exc_info:
            backend._create_storage_resource_json(mock_resource, "test-storage")

        error_message = str(exc_info.value)
        assert "Invalid storage_data_type" in error_message
        assert "expected string or None, got dict" in error_message
        assert str(mock_resource.uuid) in error_message

    def test_status_mapping_from_waldur_state(self):
        """Test that Waldur resource state is correctly mapped to CSCS status."""
        backend = self._create_backend()

        # Create a mock resource
        mock_resource = Mock()
        mock_resource.uuid = Mock()
        mock_resource.uuid.hex = "test-uuid"
        mock_resource.slug = "test-resource"
        mock_resource.customer_slug = "university"
        mock_resource.project_slug = "physics"

        # Create mock limits
        mock_limits = Mock()
        mock_limits.additional_properties = {"storage": 50}
        mock_resource.limits = mock_limits

        # Create mock attributes
        mock_attributes = Mock()
        mock_attributes.additional_properties = {}
        mock_resource.attributes = mock_attributes

        # Import ResourceState
        from waldur_api_client.models.resource_state import ResourceState

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

            assert result["status"] == expected_status, (
                f"State '{waldur_state}' should map to '{expected_status}'"
            )

        # Test with Unset state
        from waldur_api_client.types import Unset

        mock_resource.state = Unset()
        result = backend._create_storage_resource_json(mock_resource, "test-storage")
        assert result["status"] == "pending"

        # Test with no state attribute
        delattr(mock_resource, "state")
        result = backend._create_storage_resource_json(mock_resource, "test-storage")
        assert result["status"] == "pending"

    def test_error_handling_returns_error_status(self):
        """Test that errors return proper error status and code 500."""
        backend = self._create_backend()

        with patch(
            "waldur_cscs_hpc_storage.backend.marketplace_resources_list"
        ) as mock_list:
            # Mock the sync_detailed to raise an exception
            mock_list.sync_detailed.side_effect = Exception("API connection failed")

            # Test that generate_all_resources_json returns error response
            result = backend.generate_all_resources_json(
                "test-offering-uuid", Mock(), page=1, page_size=10
            )

            # Verify error response structure
            assert result["status"] == "error"
            assert result["code"] == 500
            assert "Failed to fetch storage resources" in result["message"]
            assert result["result"]["storageResources"] == []
            assert result["result"]["paginate"]["total"] == 0
            assert result["result"]["paginate"]["current"] == 1
            assert result["result"]["paginate"]["limit"] == 10

    def test_dynamic_target_type_mapping(self):
        """Test that storage data type correctly maps to target type."""
        backend = self._create_backend()

        # Create a mock resource
        mock_resource = Mock()
        mock_resource.uuid = Mock()
        mock_resource.uuid.hex = "test-uuid"
        mock_resource.slug = "test-resource"
        mock_resource.customer_slug = "university"
        mock_resource.project_slug = "physics"
        mock_resource.project_uuid = Mock()
        mock_resource.project_uuid.hex = "project-uuid"
        mock_resource.state = "OK"

        # Create mock limits
        mock_limits = Mock()
        mock_limits.additional_properties = {"storage": 50}
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
            mock_attributes.additional_properties = {
                "storage_data_type": storage_data_type
            }
            mock_resource.attributes = mock_attributes

            result = backend._create_storage_resource_json(
                mock_resource, "test-storage"
            )

            actual_target_type = result["target"]["targetType"]
            assert actual_target_type == expected_target_type, (
                f"Storage data type '{storage_data_type}' should map to target type '{expected_target_type}', got '{actual_target_type}'"
            )

            # Verify target item structure based on type
            target_item = result["target"]["targetItem"]
            if expected_target_type == "project":
                assert "status" in target_item
                assert "unixGid" in target_item
                assert target_item["status"] == "active"
            elif expected_target_type == "user":
                assert "email" in target_item
                assert "unixUid" in target_item
                assert "primaryProject" in target_item
                assert target_item["status"] == "active"
                assert "name" in target_item["primaryProject"]
                assert "unixGid" in target_item["primaryProject"]

    def test_quota_float_consistency(self):
        """Test that quotas use float data type for consistency."""
        backend = self._create_backend()

        # Create a mock resource
        mock_resource = Mock()
        mock_resource.uuid = Mock()
        mock_resource.uuid.hex = "test-uuid"
        mock_resource.slug = "test-resource"
        mock_resource.customer_slug = "university"
        mock_resource.project_slug = "physics"
        mock_resource.state = "OK"

        # Create mock limits with non-zero storage
        mock_limits = Mock()
        mock_limits.additional_properties = {"storage": 42.5}  # Use float value
        mock_resource.limits = mock_limits

        mock_attributes = Mock()
        mock_attributes.additional_properties = {}
        mock_resource.attributes = mock_attributes

        result = backend._create_storage_resource_json(mock_resource, "test-storage")

        # Verify all quotas are floats
        quotas = result["quotas"]
        assert quotas is not None, "Quotas should not be None for non-zero storage"

        for quota in quotas:
            quota_value = quota["quota"]
            assert isinstance(quota_value, float), (
                f"Quota value {quota_value} should be float, got {type(quota_value)}"
            )

    def test_storage_data_type_validation(self):
        """Test validation of storage_data_type parameter."""
        backend = self._create_backend()

        # Create a mock resource
        mock_resource = Mock()
        mock_resource.uuid = Mock()
        mock_resource.uuid.hex = str(uuid4())

        # Test with invalid data type (list)
        with pytest.raises(TypeError) as exc_info:
            backend._get_target_data(mock_resource, ["store", "archive"])

        error_message = str(exc_info.value)
        assert "Invalid storage_data_type" in error_message
        assert "expected string, got list" in error_message
        assert str(mock_resource.uuid) in error_message

        # Test with None
        with pytest.raises(TypeError) as exc_info:
            backend._get_target_data(mock_resource, None)

        error_message = str(exc_info.value)
        assert "Invalid storage_data_type" in error_message
        assert "expected string, got NoneType" in error_message

        # Test with valid but unknown storage_data_type (should log warning but not fail)
        # Mock _get_project_unix_gid
        with patch.object(self.backend, "_get_project_unix_gid", return_value=30000):
            result = backend._get_target_data(mock_resource, "unknown_type")
            assert result["targetType"] == "project"  # Should fallback to default

    def test_system_identifiers_use_deterministic_uuids(self):
        """Test that system identifiers use deterministic UUIDs generated from their names."""
        backend = self._create_backend()

        # Create a mock resource
        mock_resource = Mock()
        mock_resource.uuid = Mock()
        mock_resource.uuid.hex = "test-uuid"
        mock_resource.slug = "test-resource"
        mock_resource.customer_slug = "university"
        mock_resource.project_slug = "physics"
        mock_resource.state = "OK"

        # Create mock limits
        mock_limits = Mock()
        mock_limits.additional_properties = {"storage": 50}
        mock_resource.limits = mock_limits

        mock_attributes = Mock()
        mock_attributes.additional_properties = {"storage_data_type": "store"}
        mock_resource.attributes = mock_attributes

        result = backend._create_storage_resource_json(
            mock_resource, "test-storage-system"
        )

        # Verify that system identifiers are in UUID format
        import re

        uuid_pattern = r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"

        storage_system = result["storageSystem"]
        assert re.match(uuid_pattern, storage_system["itemId"])
        assert storage_system["key"] == "test-storage-system"

        storage_file_system = result["storageFileSystem"]
        assert re.match(uuid_pattern, storage_file_system["itemId"])
        assert storage_file_system["key"] == "lustre"

        storage_data_type = result["storageDataType"]
        assert re.match(uuid_pattern, storage_data_type["itemId"])
        assert storage_data_type["key"] == "store"

        result2 = self.backend._create_storage_resource_json(
            mock_resource, "test-storage-system"
        )

        assert result["storageSystem"]["itemId"] == result2["storageSystem"]["itemId"]
        assert (
            result["storageFileSystem"]["itemId"]
            == result2["storageFileSystem"]["itemId"]
        )
        assert (
            result["storageDataType"]["itemId"] == result2["storageDataType"]["itemId"]
        )

        # Test target item UUIDs are also deterministic UUIDs
        target_item = result["target"]["targetItem"]
        assert re.match(uuid_pattern, target_item["itemId"])

        # Verify determinism for target items too
        target_item2 = result2["target"]["targetItem"]
        assert target_item["itemId"] == target_item2["itemId"]

    def test_filtering_by_storage_system(self):
        """Test filtering storage resources by storage system."""
        backend = self._create_backend()

        # Create mock storage resources with different storage systems
        mock_resources = [
            {
                "storageSystem": {"key": "capstor", "name": "CAPSTOR"},
                "storageDataType": {"key": "store", "name": "STORE"},
                "status": "active",
            },
            {
                "storageSystem": {"key": "vast", "name": "VAST"},
                "storageDataType": {"key": "users", "name": "USERS"},
                "status": "pending",
            },
            {
                "storageSystem": {"key": "iopsstor", "name": "IOPSSTOR"},
                "storageDataType": {"key": "archive", "name": "ARCHIVE"},
                "status": "active",
            },
        ]

        # Test filtering by storage_system
        filtered = backend._apply_filters(mock_resources, storage_system="capstor")
        assert len(filtered) == 1
        assert filtered[0]["storageSystem"]["key"] == "capstor"

        filtered = backend._apply_filters(mock_resources, storage_system="vast")
        assert len(filtered) == 1
        assert filtered[0]["storageSystem"]["key"] == "vast"

        # Test with non-existent storage system
        filtered = backend._apply_filters(mock_resources, storage_system="nonexistent")
        assert len(filtered) == 0

    def test_filtering_by_data_type(self):
        """Test filtering storage resources by data type."""
        backend = self._create_backend()

        # Create mock storage resources with different data types
        mock_resources = [
            {
                "storageSystem": {"key": "capstor", "name": "CAPSTOR"},
                "storageDataType": {"key": "store", "name": "STORE"},
                "status": "active",
            },
            {
                "storageSystem": {"key": "capstor", "name": "CAPSTOR"},
                "storageDataType": {"key": "users", "name": "USERS"},
                "status": "pending",
            },
            {
                "storageSystem": {"key": "capstor", "name": "CAPSTOR"},
                "storageDataType": {"key": "scratch", "name": "SCRATCH"},
                "status": "active",
            },
        ]

        # Test filtering by data_type
        filtered = backend._apply_filters(mock_resources, data_type="store")
        assert len(filtered) == 1
        assert filtered[0]["storageDataType"]["key"] == "store"

        filtered = backend._apply_filters(mock_resources, data_type="users")
        assert len(filtered) == 1
        assert filtered[0]["storageDataType"]["key"] == "users"

        # Test with non-existent data type
        filtered = backend._apply_filters(mock_resources, data_type="nonexistent")
        assert len(filtered) == 0

    def test_filtering_by_status(self):
        """Test filtering storage resources by status."""
        backend = self._create_backend()

        # Create mock storage resources with different statuses
        mock_resources = [
            {
                "storageSystem": {"key": "capstor", "name": "CAPSTOR"},
                "storageDataType": {"key": "store", "name": "STORE"},
                "status": "active",
            },
            {
                "storageSystem": {"key": "capstor", "name": "CAPSTOR"},
                "storageDataType": {"key": "users", "name": "USERS"},
                "status": "pending",
            },
            {
                "storageSystem": {"key": "capstor", "name": "CAPSTOR"},
                "storageDataType": {"key": "scratch", "name": "SCRATCH"},
                "status": "removing",
            },
        ]

        # Test filtering by status
        filtered = backend._apply_filters(mock_resources, status="active")
        assert len(filtered) == 1
        assert filtered[0]["status"] == "active"

        filtered = backend._apply_filters(mock_resources, status="pending")
        assert len(filtered) == 1
        assert filtered[0]["status"] == "pending"

        filtered = backend._apply_filters(mock_resources, status="removing")
        assert len(filtered) == 1
        assert filtered[0]["status"] == "removing"

        # Test with non-existent status
        filtered = backend._apply_filters(mock_resources, status="nonexistent")
        assert len(filtered) == 0

    def test_filtering_combined(self):
        """Test filtering storage resources with multiple filter criteria."""
        backend = self._create_backend()

        # Create mock storage resources
        mock_resources = [
            {
                "storageSystem": {"key": "capstor", "name": "CAPSTOR"},
                "storageDataType": {"key": "store", "name": "STORE"},
                "status": "active",
            },
            {
                "storageSystem": {"key": "capstor", "name": "CAPSTOR"},
                "storageDataType": {"key": "store", "name": "STORE"},
                "status": "pending",
            },
            {
                "storageSystem": {"key": "vast", "name": "VAST"},
                "storageDataType": {"key": "store", "name": "STORE"},
                "status": "active",
            },
            {
                "storageSystem": {"key": "capstor", "name": "CAPSTOR"},
                "storageDataType": {"key": "users", "name": "USERS"},
                "status": "active",
            },
        ]

        # Test combined filtering: capstor + store + active
        filtered = backend._apply_filters(
            mock_resources, storage_system="capstor", data_type="store", status="active"
        )
        assert len(filtered) == 1
        assert filtered[0]["storageSystem"]["key"] == "capstor"
        assert filtered[0]["storageDataType"]["key"] == "store"
        assert filtered[0]["status"] == "active"

        # Test combined filtering: capstor + store (should return 2)
        filtered = backend._apply_filters(
            mock_resources, storage_system="capstor", data_type="store"
        )
        assert len(filtered) == 2
        assert all(r["storageSystem"]["key"] == "capstor" for r in filtered)
        assert all(r["storageDataType"]["key"] == "store" for r in filtered)

        # Test combined filtering that returns no results
        filtered = backend._apply_filters(
            mock_resources, storage_system="vast", data_type="users"
        )
        assert len(filtered) == 0

    def test_filtering_no_filters_applied(self):
        """Test that no filtering is applied when no filters are provided."""
        backend = self._create_backend()

        # Create mock storage resources
        mock_resources = [
            {
                "storageSystem": {"key": "capstor", "name": "CAPSTOR"},
                "storageDataType": {"key": "store", "name": "STORE"},
                "status": "active",
            },
            {
                "storageSystem": {"key": "vast", "name": "VAST"},
                "storageDataType": {"key": "users", "name": "USERS"},
                "status": "pending",
            },
        ]

        # Test no filtering (should return all resources)
        filtered = backend._apply_filters(mock_resources)
        assert len(filtered) == 2
        assert filtered == mock_resources

    @patch("waldur_cscs_hpc_storage.backend.marketplace_resources_list")
    def test_pagination_info_updated_after_filtering(self, mock_list):
        """Test that pagination info is updated to reflect filtered results, not raw API results."""
        backend = self._create_backend()

        # Create mock resources with different storage systems
        mock_resource1 = Mock()
        mock_resource1.uuid.hex = "uuid-1"
        mock_resource1.slug = "resource-1"
        mock_resource1.customer_slug = "university"
        mock_resource1.project_slug = "physics"
        mock_resource1.offering_slug = "capstor"
        mock_resource1.state = "OK"
        mock_limits1 = Mock()
        mock_limits1.additional_properties = {"storage": 100}
        mock_resource1.limits = mock_limits1
        mock_attributes1 = Mock()
        mock_attributes1.additional_properties = {"storage_system": "capstor"}
        mock_resource1.attributes = mock_attributes1

        mock_resource2 = Mock()
        mock_resource2.uuid.hex = "uuid-2"
        mock_resource2.slug = "resource-2"
        mock_resource2.customer_slug = "university"
        mock_resource2.project_slug = "chemistry"
        mock_resource2.offering_slug = "vast"
        mock_resource2.state = "OK"
        mock_limits2 = Mock()
        mock_limits2.additional_properties = {"storage": 200}
        mock_resource2.limits = mock_limits2
        mock_attributes2 = Mock()
        mock_attributes2.additional_properties = {"storage_system": "vast"}
        mock_resource2.attributes = mock_attributes2

        # Mock API response: 2 total resources from different storage systems
        mock_response = Mock()
        mock_response.parsed = [mock_resource1, mock_resource2]
        mock_headers = Mock()
        mock_headers.get = Mock(
            return_value="2"
        )  # API says there are 2 total resources
        mock_response.headers = mock_headers
        mock_list.sync_detailed.return_value = mock_response

        # Test filtering by storage_system that matches only 1 resource
        resources, pagination_info = backend._get_all_storage_resources(
            "test-offering-uuid", Mock(), page=1, page_size=10, storage_system="capstor"
        )

        # With hierarchical structure, filtering by storage_system="capstor" returns:
        # tenant entry for capstor + customer entry + project entry
        # The exact count depends on the hierarchy created, but we should get at least 1
        assert len(resources) >= 1
        # All returned resources should be from capstor storage system
        for resource in resources:
            assert resource["storageSystem"]["key"] == "capstor"

        # Pagination info should reflect filtered results, not original API results
        assert pagination_info["total"] >= 1  # At least 1 filtered resource
        assert pagination_info["pages"] == 1
        assert pagination_info["current"] == 1
        assert pagination_info["limit"] == 10
        assert pagination_info["offset"] == 0

        # Test filtering by storage_system that matches no resources
        resources, pagination_info = backend._get_all_storage_resources(
            "test-offering-uuid",
            Mock(),
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

    @patch("waldur_cscs_hpc_storage.backend.marketplace_resources_list")
    @patch(
        "waldur_cscs_hpc_storage.backend.marketplace_provider_offerings_customers_list"
    )
    def test_non_transitional_resource_always_included(self, mock_customers, mock_list):
        """Test that non-transitional resources are always included regardless of order state."""
        # Mock resource in non-transitional state
        mock_resource = Mock()
        mock_resource.offering_name = "Test Storage"
        mock_resource.offering_slug = "test-storage"
        mock_resource.uuid = Mock()
        mock_resource.uuid.hex = "test-uuid-1"
        mock_resource.slug = "resource-1"
        mock_resource.customer_slug = "university"
        mock_resource.customer_name = "University"
        mock_resource.customer_uuid = Mock()
        mock_resource.customer_uuid.hex = str(uuid4())
        mock_resource.project_slug = "physics"
        mock_resource.project_name = "Physics Department"
        mock_resource.project_uuid = Mock()
        mock_resource.project_uuid.hex = str(uuid4())
        mock_resource.provider_slug = "cscs"
        mock_resource.provider_name = "CSCS"
        mock_resource.offering_uuid = Mock()
        mock_resource.offering_uuid.hex = str(uuid4())
        mock_resource.state = "OK"  # Non-transitional state

        # Create mock order_in_progress with any state (shouldn't matter)
        from waldur_api_client.models.order_state import OrderState

        order_uuid = str(uuid4())
        mock_order = Mock()
        # Set state to PENDING_PROVIDER so that approve/reject URLs are generated
        mock_order.state = OrderState.PENDING_PROVIDER
        mock_order.uuid = order_uuid
        mock_resource.order_in_progress = mock_order

        # Create mock limits and attributes
        mock_limits = Mock()
        mock_limits.additional_properties = {"storage": 100}
        mock_resource.limits = mock_limits
        mock_attributes = Mock()
        mock_attributes.additional_properties = {}
        mock_resource.attributes = mock_attributes
        # Mock options as well (required for validation)
        mock_resource.options = {}

        # Mock the sync_detailed response
        mock_response = Mock()
        mock_response.parsed = [mock_resource]
        mock_headers = Mock()
        mock_headers.get = Mock(return_value="1")
        mock_response.headers = mock_headers
        mock_list.sync_detailed.return_value = mock_response

        # Mock customers response
        mock_customers.sync_detailed.return_value = Mock(parsed=[])

        # Configure backend client with base URL
        mock_httpx_client = Mock()
        mock_httpx_client.base_url = "https://waldur.example.com/api"
        self.backend.waldur_api_config.api_url = "https://waldur.example.com/api"
        self.backend._client.get_httpx_client.return_value = mock_httpx_client

        # Test the method
        resources, _ = self.backend._get_all_storage_resources("test-offering-uuid")

        # Should include the resource since it's not in transitional state
        assert len(resources) >= 1

        # Find the project-level resource (with itemId matching our resource UUID)
        project_resources = [r for r in resources if r.get("itemId") == "test-uuid-1"]
        assert len(project_resources) >= 1
        project_resource = project_resources[0]
        assert (
            project_resource["approve_by_provider_url"]
            == f"https://waldur.example.com/api/marketplace-orders/{order_uuid}/approve_by_provider/"
        )
        assert (
            project_resource["reject_by_provider_url"]
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
        # Mock the hpc_user_client
        self.backend.hpc_user_client = Mock()

    def test_get_project_unix_gid_success(self):
        """Test successful GID lookup."""
        self.backend.hpc_user_client.get_project_unix_gid.return_value = 30042
        gid = self.backend._get_project_unix_gid("test-project")
        assert gid == 30042
        self.backend.hpc_user_client.get_project_unix_gid.assert_called_once_with(
            "test-project"
        )

    def test_get_project_unix_gid_cache(self):
        """Test GID caching."""
        self.backend.hpc_user_client.get_project_unix_gid.return_value = 30042

        # First call
        gid1 = self.backend._get_project_unix_gid("test-project")
        assert gid1 == 30042

        # Second call should use cache
        gid2 = self.backend._get_project_unix_gid("test-project")
        assert gid2 == 30042

        # Client called only once
        self.backend.hpc_user_client.get_project_unix_gid.assert_called_once()

    def test_get_project_unix_gid_prod_failure(self):
        """Test lookup failure in production mode returns None."""
        self.backend.development_mode = False
        self.backend.hpc_user_client.get_project_unix_gid.side_effect = Exception(
            "API Error"
        )

        gid = self.backend._get_project_unix_gid("test-project")
        assert gid is None
