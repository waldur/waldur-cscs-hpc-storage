"""Integration tests for hierarchical storage API endpoints."""

import os
from unittest.mock import Mock, AsyncMock
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from waldur_cscs_hpc_storage.config import (
    BackendConfig,
    HpcUserApiConfig,
    StorageProxyConfig,
    WaldurApiConfig,
)
from pathlib import Path

# Set up environment before importing the app
test_config_path = Path(__file__).parent / "test_config.yaml"
os.environ["WALDUR_CSCS_STORAGE_PROXY_CONFIG_PATH"] = str(test_config_path)
os.environ["DISABLE_AUTH"] = "true"  # Also disable auth for these tests

# Remove skipif since we set the config path
# pytestmark = pytest.mark.skipif(...)


try:
    from waldur_cscs_hpc_storage.api.main import app
    from waldur_cscs_hpc_storage.api.dependencies import get_waldur_service, get_config
    from waldur_cscs_hpc_storage.hierarchy_builder import CustomerInfo
except SystemExit:
    # If import fails due to configuration issues, skip all tests
    pytest.skip("Configuration not available for API tests", allow_module_level=True)


@pytest.fixture
def mock_waldur_service():
    """Create a mock WaldurService."""
    mock = Mock()
    mock.list_resources = AsyncMock()
    mock.get_offering_customers = AsyncMock()
    return mock


@pytest.fixture
def mock_config():
    """Create a mock configuration with development mode enabled."""
    return StorageProxyConfig(
        waldur_api=WaldurApiConfig(
            api_url="http://mock-waldur",
            access_token="11111111111111111111111111111111",
        ),
        backend_settings=BackendConfig(
            development_mode=True,
            inode_base_multiplier=1000000,
            inode_soft_coefficient=0.9,
            inode_hard_coefficient=1.0,
        ),
        storage_systems={"capstor": "capstor", "vast": "vast", "iopsstor": "iopsstor"},
        auth=None,
        hpc_user_api=HpcUserApiConfig(development_mode=True),
    )


@pytest.fixture
def client(mock_waldur_service, mock_config):
    """Create a test client with mocked dependencies."""
    app.dependency_overrides[get_config] = lambda: mock_config
    app.dependency_overrides[get_waldur_service] = lambda: mock_waldur_service
    with TestClient(app) as c:
        yield c
    # Clean up overrides
    app.dependency_overrides.clear()


@pytest.fixture
def mock_waldur_resources():
    """Create mock Waldur resources for testing."""

    def create_mock_resource(
        resource_uuid=None,
        customer_slug="test-customer",
        customer_name="Test Customer",
        project_slug="test-project",
        project_name="Test Project",
        offering_slug="capstor",
        provider_slug="cscs",
        provider_name="CSCS",
        storage_data_type="store",
        storage_limit=150.0,
    ):
        if resource_uuid is None:
            resource_uuid = str(uuid4())

        resource = Mock()
        # Ensure uuid is a string, not a Mock object, to satisfy JSON serialization
        resource.uuid = resource_uuid
        resource.slug = project_slug
        resource.name = project_name
        resource.state = "OK"
        resource.customer_slug = customer_slug
        resource.customer_name = customer_name
        resource.customer_uuid = str(uuid4())
        resource.project_slug = project_slug
        resource.project_name = project_name
        resource.project_uuid = str(uuid4())
        resource.offering_slug = offering_slug
        resource.provider_slug = provider_slug
        resource.provider_name = provider_name
        resource.offering_uuid = str(uuid4())

        # Mock limits
        resource.limits = Mock()
        resource.limits.additional_properties = {"storage": storage_limit}
        resource.limits.storage = storage_limit

        # Mock options
        resource.options = Mock(
            soft_quota_space=None,
            hard_quota_space=None,
            soft_quota_inodes=None,
            hard_quota_inodes=None,
            permissions=None,
        )

        # Mock attributes
        resource.attributes = Mock()
        resource.attributes.storage_data_type = storage_data_type
        # Ensure additional_properties if accessed directly also works
        resource.attributes.additional_properties = {
            "storage_data_type": storage_data_type,
            "permissions": "2770",
        }
        resource.effective_permissions = "2770"
        resource.backend_metadata = Mock()
        resource.backend_metadata.project_item = None
        resource.backend_metadata.user_item = None
        resource.callback_urls = {}

        # Mock render_quotas to return a list of dicts compatible with Quota model
        quota_dict = {
            "type": "space",
            "quota": storage_limit,
            "unit": "tera",
            "enforcementType": "hard",
        }
        resource.render_quotas = Mock(return_value=[quota_dict])

        return resource

    return [
        create_mock_resource(
            customer_slug="mch",
            customer_name="MCH",
            project_slug="msclim",
            project_name="MSCLIM",
            offering_slug="capstor",
            storage_data_type="store",
        ),
        create_mock_resource(
            customer_slug="eth",
            customer_name="ETH",
            project_slug="climate-data",
            project_name="Climate Data",
            offering_slug="vast",
            storage_data_type="scratch",
        ),
        create_mock_resource(
            customer_slug="mch",
            customer_name="MCH",
            project_slug="user-homes",
            project_name="User Homes",
            offering_slug="capstor",
            storage_data_type="users",
        ),
    ]


@pytest.fixture
def mock_offering_customers():
    """Mock customer data from offering."""
    return {
        "mch": CustomerInfo(
            itemId="mch-customer-id",
            key="mch",
            name="MCH",
        ),
        "eth": CustomerInfo(
            itemId="eth-customer-id",
            key="eth",
            name="ETH",
        ),
    }


class TestHierarchicalStorageAPI:
    """Test the hierarchical storage resource API."""

    def test_three_tier_hierarchy_response(
        self,
        client,
        mock_waldur_service,
        mock_waldur_resources,
        mock_offering_customers,
    ):
        """Test that the API returns a proper three-tier hierarchy."""
        mock_waldur_service.get_offering_customers.return_value = (
            mock_offering_customers
        )

        mock_response = Mock()
        mock_response.resources = mock_waldur_resources
        mock_response.total_count = len(mock_waldur_resources)
        mock_waldur_service.list_resources.return_value = mock_response

        response = client.get("/api/storage-resources/")

        assert response.status_code == 200
        data = response.json()

        # Verify response structure
        assert "status" in data
        assert "resources" in data
        assert data["status"] == "success"

        resources = data["resources"]

        # Group by type
        tenants = [r for r in resources if r["target"]["targetType"] == "tenant"]
        customers = [r for r in resources if r["target"]["targetType"] == "customer"]
        projects = [r for r in resources if r["target"]["targetType"] == "project"]

        # Verify we have all three tiers (assuming mock works)
        assert len(tenants) > 0
        assert len(customers) > 0
        assert len(projects) > 0

        # Verify hierarchy relationships
        # All tenants should have no parent
        for tenant in tenants:
            assert tenant["parentItemId"] is None

        # All customers should have a parent tenant
        tenant_ids = {t["itemId"] for t in tenants}
        for customer in customers:
            assert customer["parentItemId"] is not None
            assert customer["parentItemId"] in tenant_ids

        # All projects should have a parent customer
        customer_ids = {c["itemId"] for c in customers}
        for project in projects:
            assert project["parentItemId"] is not None
            assert project["parentItemId"] in customer_ids

    def test_mount_point_hierarchy(
        self,
        client,
        mock_waldur_service,
        mock_waldur_resources,
        mock_offering_customers,
    ):
        """Test that mount points follow the correct hierarchy."""
        mock_waldur_service.get_offering_customers.return_value = (
            mock_offering_customers
        )

        mock_response = Mock()
        mock_response.resources = mock_waldur_resources
        mock_response.total_count = len(mock_waldur_resources)
        mock_waldur_service.list_resources.return_value = mock_response

        response = client.get("/api/storage-resources/")
        data = response.json()
        resources = data["resources"]

        # Find a complete hierarchy chain
        capstor_resources = [
            r for r in resources if r["storageSystem"]["key"] == "capstor"
        ]
        capstor_tenants = [
            r for r in capstor_resources if r["target"]["targetType"] == "tenant"
        ]
        capstor_customers = [
            r for r in capstor_resources if r["target"]["targetType"] == "customer"
        ]
        capstor_projects = [
            r for r in capstor_resources if r["target"]["targetType"] == "project"
        ]

        if capstor_tenants and capstor_customers and capstor_projects:
            tenant = capstor_tenants[0]
            customer = next(
                c for c in capstor_customers if c["parentItemId"] == tenant["itemId"]
            )
            project = next(
                p for p in capstor_projects if p["parentItemId"] == customer["itemId"]
            )

            # Verify mount point hierarchy
            tenant_mount = tenant["mountPoint"]["default"]
            customer_mount = customer["mountPoint"]["default"]
            project_mount = project["mountPoint"]["default"]

            # Customer mount should start with tenant mount
            assert customer_mount.startswith(tenant_mount + "/")

            # Project mount should start with customer mount
            assert project_mount.startswith(customer_mount + "/")

    def test_storage_system_filter_maintains_hierarchy(
        self,
        client,
        mock_waldur_service,
        mock_waldur_resources,
        mock_offering_customers,
    ):
        """Test that filtering by storage system maintains the hierarchy."""
        mock_waldur_service.get_offering_customers.return_value = (
            mock_offering_customers
        )
        # Filter to only capstor resources
        capstor_resources = [
            r for r in mock_waldur_resources if r.offering_slug == "capstor"
        ]

        mock_response = Mock()
        mock_response.resources = capstor_resources
        mock_response.total_count = len(capstor_resources)
        mock_waldur_service.list_resources.return_value = mock_response

        response = client.get("/api/storage-resources/?storage_system=capstor")

        assert response.status_code == 200
        data = response.json()
        resources = data["resources"]

        # Verify all resources are from capstor
        for resource in resources:
            assert resource["storageSystem"]["key"] == "capstor"

        # Verify hierarchy is maintained
        tenants = [r for r in resources if r["target"]["targetType"] == "tenant"]
        customers = [r for r in resources if r["target"]["targetType"] == "customer"]
        projects = [r for r in resources if r["target"]["targetType"] == "project"]

        # Should have at least one of each type for capstor
        assert len(tenants) >= 1
        assert len(customers) >= 1
        assert len(projects) >= 1

        # Verify parent-child relationships
        tenant_ids = {t["itemId"] for t in tenants}
        customer_ids = {c["itemId"] for c in customers}

        for customer in customers:
            assert customer["parentItemId"] in tenant_ids

        for project in projects:
            assert project["parentItemId"] in customer_ids

    def test_pagination_with_hierarchy(self, client, mock_waldur_service):
        """Test that pagination works correctly with hierarchical resources."""
        mock_waldur_service.list_resources.return_value = Mock(
            resources=[], total_count=0
        )

        response = client.get("/api/storage-resources/?page=1&page_size=5")

        assert response.status_code == 200
        data = response.json()

        # Verify pagination info is present
        assert "pagination" in data
        pagination = data["pagination"]
        assert "current" in pagination
        assert "limit" in pagination
        assert "total" in pagination

        # Verify resources are limited by page_size
        resources = data["resources"]
        assert len(resources) <= 5

    def test_invalid_storage_system_filter(self, client):
        """Test filtering with an invalid storage system."""
        response = client.get("/api/storage-resources/?storage_system=nonexistent")

        # In current FastAPI/Pydantic implementation, an invalid enum value triggers 422
        assert response.status_code == 422

    def test_empty_storage_system_parameter(self, client):
        """Test handling of empty storage_system parameter."""
        response = client.get("/api/storage-resources/?storage_system=")
        assert response.status_code == 422

    def test_data_type_filter_affects_hierarchy(
        self,
        client,
        mock_waldur_service,
        mock_waldur_resources,
        mock_offering_customers,
    ):
        """Test that data_type filter affects the hierarchy appropriately."""
        mock_waldur_service.get_offering_customers.return_value = (
            mock_offering_customers
        )

        mock_response = Mock()
        mock_response.resources = mock_waldur_resources
        mock_response.total_count = len(mock_waldur_resources)
        mock_waldur_service.list_resources.return_value = mock_response

        response = client.get("/api/storage-resources/?data_type=store")

        assert response.status_code == 200
        data = response.json()
        resources = data["resources"]

        # All resources should be store type
        for resource in resources:
            assert resource["storageDataType"]["key"] == "store"

        # Should still have hierarchy
        types = {r["target"]["targetType"] for r in resources}
        # Might not have all types if data is limited, but structure should be consistent
        assert len(types) >= 1


class TestHierarchyValidation:
    """Test hierarchy validation and consistency."""

    def test_no_orphaned_resources(
        self,
        client,
        mock_waldur_service,
        mock_waldur_resources,
        mock_offering_customers,
    ):
        """Test that no resources are orphaned in the hierarchy."""
        mock_waldur_service.get_offering_customers.return_value = (
            mock_offering_customers
        )

        mock_response = Mock()
        mock_response.resources = mock_waldur_resources
        mock_response.total_count = len(mock_waldur_resources)
        mock_waldur_service.list_resources.return_value = mock_response

        response = client.get("/api/storage-resources/")
        data = response.json()
        resources = data["resources"]

        # Create maps for validation
        resource_map = {r["itemId"]: r for r in resources}

        tenants = [r for r in resources if r["target"]["targetType"] == "tenant"]
        customers = [r for r in resources if r["target"]["targetType"] == "customer"]
        projects = [r for r in resources if r["target"]["targetType"] == "project"]

        # Verify all tenants have no parent (top-level)
        for tenant in tenants:
            assert tenant["parentItemId"] is None

        # Verify all customers have valid parent tenants
        for customer in customers:
            parent_id = customer["parentItemId"]
            assert parent_id is not None
            assert parent_id in resource_map
            parent = resource_map[parent_id]
            assert parent["target"]["targetType"] == "tenant"

        # Verify all projects have valid parent customers
        for project in projects:
            parent_id = project["parentItemId"]
            assert parent_id is not None
            assert parent_id in resource_map
            parent = resource_map[parent_id]
            assert parent["target"]["targetType"] == "customer"

    def test_consistent_storage_metadata(
        self,
        client,
        mock_waldur_service,
        mock_waldur_resources,
        mock_offering_customers,
    ):
        """Test that storage system metadata is consistent across hierarchy levels."""
        mock_waldur_service.get_offering_customers.return_value = (
            mock_offering_customers
        )

        mock_response = Mock()
        mock_response.resources = mock_waldur_resources
        mock_response.total_count = len(mock_waldur_resources)
        mock_waldur_service.list_resources.return_value = mock_response

        response = client.get("/api/storage-resources/")
        data = response.json()
        resources = data["resources"]

        # Group resources by storage system and data type
        hierarchy_groups = {}

        for resource in resources:
            storage_key = resource["storageSystem"]["key"]
            data_type_key = resource["storageDataType"]["key"]
            group_key = f"{storage_key}-{data_type_key}"

            if group_key not in hierarchy_groups:
                hierarchy_groups[group_key] = {
                    "tenant": None,
                    "customers": [],
                    "projects": [],
                }

            target_type = resource["target"]["targetType"]
            if target_type == "tenant":
                hierarchy_groups[group_key]["tenant"] = resource
            elif target_type == "customer":
                hierarchy_groups[group_key]["customers"].append(resource)
            elif target_type == "project":
                hierarchy_groups[group_key]["projects"].append(resource)

        # Verify each group has consistent metadata
        for group_key, group in hierarchy_groups.items():
            storage_system = None
            storage_file_system = None
            storage_data_type = None

            # Collect metadata from all resources in the group
            all_resources = [group["tenant"]] + group["customers"] + group["projects"]
            all_resources = [r for r in all_resources if r is not None]

            for resource in all_resources:
                if storage_system is None:
                    storage_system = resource["storageSystem"]
                    storage_file_system = resource["storageFileSystem"]
                    storage_data_type = resource["storageDataType"]
                else:
                    # All resources in the group should have identical metadata
                    assert resource["storageSystem"] == storage_system
                    assert resource["storageFileSystem"] == storage_file_system
                    assert resource["storageDataType"] == storage_data_type

    def test_quota_assignment_by_level(
        self,
        client,
        mock_waldur_service,
        mock_waldur_resources,
        mock_offering_customers,
    ):
        """Test that quotas are assigned only to project-level resources."""
        mock_waldur_service.get_offering_customers.return_value = (
            mock_offering_customers
        )
        mock_response = Mock()
        mock_response.resources = mock_waldur_resources
        mock_response.total_count = len(mock_waldur_resources)
        mock_waldur_service.list_resources.return_value = mock_response

        response = client.get("/api/storage-resources/")

        if response.status_code == 200:
            data = response.json()
            resources = data["resources"]

            for resource in resources:
                target_type = resource["target"]["targetType"]

                if target_type in ["tenant", "customer"]:
                    # Tenants and customers should not have quotas
                    assert resource["quotas"] is None
                elif target_type == "project":
                    # Projects should have quotas
                    assert resource["quotas"] is not None
                    assert len(resource["quotas"]) > 0

                    # Verify quota structure
                    for quota in resource["quotas"]:
                        assert "type" in quota
                        assert "quota" in quota
                        assert "unit" in quota
                        assert "enforcementType" in quota
                        assert quota["type"] in ["space", "inodes"]
                        assert quota["enforcementType"] in ["soft", "hard"]


class TestAPIResponseStructure:
    """Test the structure and format of API responses."""

    def test_response_schema_compliance(self, client, mock_waldur_service):
        """Test that the API response follows the expected schema."""
        mock_waldur_service.list_resources.return_value = Mock(
            resources=[], total_count=0
        )

        response = client.get("/api/storage-resources/")

        assert response.status_code == 200
        data = response.json()

        # Verify top-level structure
        required_fields = ["status", "resources", "pagination"]
        for field in required_fields:
            assert field in data

        assert data["status"] == "success"
        assert isinstance(data["resources"], list)
        assert isinstance(data["pagination"], dict)

        # Verify pagination structure
        pagination_fields = ["current", "limit", "offset", "pages", "total"]
        for field in pagination_fields:
            assert field in data["pagination"]

        # Verify resource structure
        if data["resources"]:
            resource = data["resources"][0]
            resource_fields = [
                "itemId",
                "status",
                "mountPoint",
                "permission",
                "quotas",
                "target",
                "storageSystem",
                "storageFileSystem",
                "storageDataType",
            ]
            for field in resource_fields:
                assert field in resource

            # Verify nested structures
            assert "targetType" in resource["target"]
            assert "targetItem" in resource["target"]
            assert "default" in resource["mountPoint"]
            assert "permissionType" in resource["permission"]
            assert "value" in resource["permission"]

    def test_error_response_structure(self, client):
        """Test error response structure."""
        # Test invalid enum value
        response = client.get("/api/storage-resources/?storage_system=invalid")

        assert response.status_code == 422

    def test_filters_applied_info(self, client, mock_waldur_service):
        """Test that filters_applied information is included in responses."""
        mock_waldur_service.list_resources.return_value = Mock(
            resources=[], total_count=0
        )

        response = client.get(
            "/api/storage-resources/?storage_system=capstor&data_type=store&status=active"
        )

        assert response.status_code == 200
        data = response.json()

        # Check if filters_applied is documented
        if "filters_applied" in data:
            filters = data["filters_applied"]
            # New implementation uses offering_slugs, but also data_type/status
            assert "offering_slugs" in filters
            assert "data_type" in filters
            assert "status" in filters

            # Check values
            assert "capstor" in filters["offering_slugs"]
            assert filters["data_type"] == "store"
            assert filters["status"] == "active"
