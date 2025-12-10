"""Tests for hierarchical storage resource generation."""

from typing import Optional
from unittest.mock import Mock, patch
from uuid import uuid4

import pytest

from waldur_cscs_hpc_storage.base.enums import (
    QuotaType,
    QuotaUnit,
    EnforcementType,
)
from waldur_cscs_hpc_storage.base.models import Quota
from waldur_cscs_hpc_storage.base.mount_points import generate_project_mount_point
from waldur_cscs_hpc_storage.base.mount_points import generate_customer_mount_point
from waldur_cscs_hpc_storage.base.mount_points import generate_tenant_mount_point
from waldur_cscs_hpc_storage.config import (
    BackendConfig,
)

from waldur_cscs_hpc_storage.hierarchy_builder import CustomerInfo, HierarchyBuilder
from waldur_cscs_hpc_storage.services.mapper import ResourceMapper
from waldur_cscs_hpc_storage.services.mock_gid_service import MockGidService
from waldur_cscs_hpc_storage.services.orchestrator import StorageOrchestrator
from waldur_cscs_hpc_storage.services.waldur_service import WaldurService


@pytest.fixture
def backend():
    """Create a orchestrator instance for testing (mimicking backend interface)."""
    backend_settings = BackendConfig(
        storage_file_system="lustre",
        inode_soft_coefficient=1.33,
        inode_hard_coefficient=2.0,
        inode_base_multiplier=1_000_000,
        use_mock_target_items=True,
        development_mode=True,
    )

    gid_service = MockGidService()
    mapper = ResourceMapper(backend_settings, gid_service)

    # Inject mock waldur_service for testing
    waldur_service = Mock(spec=WaldurService)

    orchestrator = StorageOrchestrator(
        backend_settings, waldur_service=waldur_service, mapper=mapper
    )
    return orchestrator


@pytest.fixture
def hierarchy_builder():
    """Create a HierarchyBuilder instance for testing."""
    return HierarchyBuilder(storage_file_system="lustre")


@pytest.fixture(autouse=True)
def mock_gid_lookup():
    """Mock GID lookup for all tests in this module."""
    from waldur_cscs_hpc_storage.services.mock_gid_service import MockGidService
    from unittest.mock import AsyncMock

    with patch.object(
        MockGidService, "get_project_unix_gid", new_callable=AsyncMock
    ) as mock_method:
        mock_method.return_value = 30000
        yield


def create_mock_resource(
    resource_uuid: Optional[str] = None,
    customer_slug: str = "test-customer",
    customer_name: str = "Test Customer",
    project_slug: str = "test-project",
    project_name: str = "Test Project",
    offering_slug: str = "capstor",
    provider_slug: str = "cscs",
    provider_name: str = "CSCS",
    storage_data_type: str = "store",
    storage_limit: float = 150.0,
) -> Mock:
    """Create a mock Waldur resource."""
    if resource_uuid is None:
        resource_uuid = str(uuid4())

    resource = Mock()
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

    # Mock limits (use direct attribute access for ParsedWaldurResource)
    resource.limits = Mock()
    resource.limits.storage = storage_limit

    # Mock attributes
    resource.attributes = Mock()
    resource.attributes.storage_data_type = storage_data_type
    resource.attributes.permissions = "2770"

    # Mock options
    resource.options = Mock(
        permissions=None,
        soft_quota_space=None,
        hard_quota_space=None,
        soft_quota_inodes=None,
        hard_quota_inodes=None,
    )
    resource.backend_metadata = Mock(
        tenant_item=None, customer_item=None, project_item=None, user_item=None
    )
    resource.get_effective_storage_quotas.return_value = (storage_limit, storage_limit)
    resource.get_effective_inode_quotas.return_value = (
        int(storage_limit * 1000 * 1000 * 1.5),
        int(storage_limit * 1000 * 1000 * 2.0),
    )
    resource.effective_permissions = "2770"

    # Mock render_quotas to return proper Quota objects
    soft_inode = int(storage_limit * 1000 * 1000 * 1.5)
    hard_inode = int(storage_limit * 1000 * 1000 * 2.0)
    resource.render_quotas.return_value = [
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

    resource.callback_urls = {}

    return resource


class TestTenantLevelGeneration:
    """Tests for tenant-level resource generation using HierarchyBuilder."""

    def test_create_tenant_storage_resource(self, hierarchy_builder):
        """Test creating a tenant-level storage resource."""
        tenant_id = "cscs"
        tenant_name = "CSCS"
        storage_system = "capstor"
        storage_data_type = "store"

        hierarchy_builder.get_or_create_tenant(
            tenant_id=tenant_id,
            tenant_name=tenant_name,
            storage_system=storage_system,
            storage_data_type=storage_data_type,
        )

        resources = hierarchy_builder.get_hierarchy_resources()
        assert len(resources) == 1
        result = resources[0]

        # Verify structure
        assert result.target.targetType == "tenant"
        assert result.target.targetItem.key == tenant_id.lower()
        assert result.target.targetItem.name == tenant_name

        # Verify mount point
        assert (
            result.mountPoint.default
            == f"/{storage_system}/{storage_data_type}/{tenant_id}"
        )

        # Verify no parent (top-level)
        assert result.parentItemId is None

        # Verify permissions
        assert result.permission.permissionType == "octal"
        assert result.permission.value == "775"

        # Verify no quotas
        assert result.quotas is None

        # Verify storage system info
        assert result.storageSystem.key == storage_system.lower()
        assert result.storageSystem.name == storage_system.upper()
        assert result.storageDataType.key == storage_data_type.lower()
        assert result.storageDataType.name == storage_data_type.upper()

    def test_tenant_different_data_types(self, hierarchy_builder):
        """Test tenant entries for different storage data types."""
        tenant_id = "cscs"
        storage_system = "capstor"

        # Test different data types
        data_types = ["store", "archive", "users", "scratch"]

        for data_type in data_types:
            hierarchy_builder.get_or_create_tenant(
                tenant_id=tenant_id,
                tenant_name="CSCS",
                storage_system=storage_system,
                storage_data_type=data_type,
            )

        results = hierarchy_builder.get_hierarchy_resources()
        assert len(results) == 4

        # Verify unique mount points
        mount_points = [r.mountPoint.default for r in results]
        assert len(mount_points) == len(set(mount_points))

        # Verify correct paths
        for data_type, result in zip(data_types, results):
            expected_path = f"/{storage_system}/{data_type}/{tenant_id}"
            assert result.mountPoint.default == expected_path


class TestCustomerLevelGeneration:
    """Tests for customer-level resource generation using HierarchyBuilder."""

    def test_create_customer_storage_resource_with_parent(self, hierarchy_builder):
        """Test creating a customer-level storage resource with parent tenant."""
        tenant_id = "cscs"
        storage_system = "capstor"
        storage_data_type = "store"

        # First create tenant
        parent_tenant_id = hierarchy_builder.get_or_create_tenant(
            tenant_id=tenant_id,
            tenant_name="CSCS",
            storage_system=storage_system,
            storage_data_type=storage_data_type,
            offering_uuid="tenant-uuid",
        )

        customer_info = CustomerInfo(
            itemId=str(uuid4()),
            key="mch",
            name="MCH",
        )

        hierarchy_builder.get_or_create_customer(
            customer_info=customer_info,
            storage_system=storage_system,
            storage_data_type=storage_data_type,
            tenant_id=tenant_id,
        )

        resources = hierarchy_builder.get_hierarchy_resources()
        assert len(resources) == 2
        result = resources[1]  # Customer is second

        # Verify structure
        assert result.target.targetType == "customer"
        assert result.target.targetItem.key == customer_info.key
        assert result.target.targetItem.name == customer_info.name

        # Verify mount point
        expected_path = (
            f"/{storage_system}/{storage_data_type}/{tenant_id}/{customer_info.key}"
        )
        assert result.mountPoint.default == expected_path

        # Verify parent reference
        assert result.parentItemId == parent_tenant_id

        # Verify permissions
        assert result.permission.value == "775"

        # Verify no quotas
        assert result.quotas is None

    def test_customer_without_parent_tenant(self, hierarchy_builder):
        """Test creating a customer-level resource without parent (legacy mode)."""
        # Create customer without creating tenant first
        customer_info = CustomerInfo(
            itemId=str(uuid4()),
            key="eth",
            name="ETH",
        )

        hierarchy_builder.get_or_create_customer(
            customer_info=customer_info,
            storage_system="vast",
            storage_data_type="scratch",
            tenant_id="cscs",
        )

        resources = hierarchy_builder.get_hierarchy_resources()
        result = resources[0]

        # Verify no parent (tenant wasn't created)
        assert result.parentItemId is None

        # Verify rest of structure is intact
        assert result.target.targetType == "customer"
        assert result.mountPoint.default == "/vast/scratch/cscs/eth"


class TestProjectLevelGeneration:
    """Tests for project-level resource generation."""

    @pytest.mark.asyncio
    async def test_create_project_storage_resource(self, backend):
        """Test creating a project-level storage resource."""
        resource = create_mock_resource(
            project_slug="msclim",
            project_name="MSCLIM",
            customer_slug="mch",
            provider_slug="cscs",
            storage_limit=150.0,
        )

        result = await backend.mapper.map_resource(resource, "capstor")

        # Verify structure
        assert result.target.targetType == "project"
        assert result.target.targetItem.name == "msclim"

        # Verify mount point
        assert "/capstor/" in result.mountPoint.default
        assert "/cscs/" in result.mountPoint.default
        assert "/mch/" in result.mountPoint.default
        assert "/msclim" in result.mountPoint.default

        # Verify quotas are present
        assert result.quotas is not None
        assert len(result.quotas) == 4  # 2 space + 2 inode quotas

        # Verify space quotas
        space_quotas = [q for q in result.quotas if q.type == "space"]
        assert len(space_quotas) == 2
        hard_quota = next(q for q in space_quotas if q.enforcementType == "hard")
        assert hard_quota.quota == 150.0
        assert hard_quota.unit == "tera"

    @pytest.mark.asyncio
    async def test_project_with_custom_permissions(self, backend):
        """Test project with custom permissions from attributes."""
        resource = create_mock_resource()
        resource.attributes.permissions = "0755"
        resource.effective_permissions = "0755"

        result = await backend.mapper.map_resource(resource, "capstor")

        assert result.permission.value == "0755"


class TestThreeTierHierarchyGeneration:
    """Tests for complete three-tier hierarchy generation."""

    @pytest.mark.asyncio
    async def test_full_hierarchy_creation(self, backend, hierarchy_builder):
        """Test creating a complete three-tier hierarchy from resources."""
        # Mock customer data
        backend.waldur_service.get_offering_customers.return_value = {
            "mch": CustomerInfo(
                itemId="customer-mch-id",
                key="mch",
                name="MCH",
            ),
            "eth": CustomerInfo(
                itemId="customer-eth-id",
                key="eth",
                name="ETH",
            ),
        }

        # Create mock resources
        resources = [
            create_mock_resource(
                customer_slug="mch",
                customer_name="MCH",
                project_slug="msclim",
                provider_slug="cscs",
                provider_name="CSCS",
                storage_data_type="store",
            ),
            create_mock_resource(
                customer_slug="eth",
                customer_name="ETH",
                project_slug="climate-data",
                provider_slug="cscs",
                provider_name="CSCS",
                storage_data_type="store",
            ),
            create_mock_resource(
                customer_slug="mch",
                customer_name="MCH",
                project_slug="user-homes",
                provider_slug="cscs",
                provider_name="CSCS",
                storage_data_type="users",
            ),
        ]

        # Process resources using HierarchyBuilder
        storage_resources = []

        for resource in resources:
            storage_system_name = resource.offering_slug
            storage_data_type = resource.attributes.storage_data_type or "store"
            tenant_id = resource.provider_slug
            tenant_name = resource.provider_name

            # Create tenant entry
            hierarchy_builder.get_or_create_tenant(
                tenant_id=tenant_id,
                tenant_name=tenant_name,
                storage_system=storage_system_name,
                storage_data_type=storage_data_type,
            )

            # Create customer entry
            customer_info = (
                backend.waldur_service.get_offering_customers.return_value.get(
                    resource.customer_slug
                )
            )
            if customer_info:
                hierarchy_builder.get_or_create_customer(
                    customer_info=customer_info,
                    storage_system=storage_system_name,
                    storage_data_type=storage_data_type,
                    tenant_id=tenant_id,
                )

            # Create project entry
            project_resource = await backend.mapper.map_resource(
                resource, storage_system_name
            )
            if project_resource:
                hierarchy_builder.assign_parent_to_project(
                    project_resource=project_resource,
                    customer_slug=resource.customer_slug,
                    storage_system=storage_system_name,
                    storage_data_type=storage_data_type,
                )
                storage_resources.append(project_resource)

        # Combine hierarchy resources with project resources
        all_resources = hierarchy_builder.get_hierarchy_resources() + storage_resources

        # Verify results
        tenants = [r for r in all_resources if r.target.targetType == "tenant"]
        customers = [r for r in all_resources if r.target.targetType == "customer"]
        projects = [r for r in all_resources if r.target.targetType == "project"]

        # Should have unique tenants for each storage_system-data_type combo
        assert len(tenants) == 2  # cscs-capstor-store, cscs-capstor-users

        # Should have unique customers for each customer-storage_system-data_type combo
        assert (
            len(customers) == 3
        )  # mch-capstor-store, eth-capstor-store, mch-capstor-users

        # Should have successfully created projects
        assert len(projects) >= 2

        # Verify hierarchy relationships
        for tenant in tenants:
            assert tenant.parentItemId is None

        for customer in customers:
            assert customer.parentItemId is not None

        for project in projects:
            assert project.parentItemId is not None

    def test_mount_path_hierarchy(self, backend):
        """Test that mount paths follow the correct hierarchy."""
        tenant_id = "cscs"
        customer_key = "mch"
        project_slug = "msclim"
        storage_system = "capstor"
        data_type = "store"

        # Generate mount points for each level
        tenant_mount = generate_tenant_mount_point(
            storage_system=storage_system,
            tenant_id=tenant_id,
            data_type=data_type,
        )

        customer_mount = generate_customer_mount_point(
            storage_system=storage_system,
            tenant_id=tenant_id,
            customer=customer_key,
            data_type=data_type,
        )

        project_mount = generate_project_mount_point(
            storage_system=storage_system,
            tenant_id=tenant_id,
            customer=customer_key,
            project_id=project_slug,
            data_type=data_type,
        )

        # Verify hierarchy in paths
        assert tenant_mount == f"/{storage_system}/{data_type}/{tenant_id}"
        assert (
            customer_mount
            == f"/{storage_system}/{data_type}/{tenant_id}/{customer_key}"
        )
        assert (
            project_mount
            == f"/{storage_system}/{data_type}/{tenant_id}/{customer_key}/{project_slug}"
        )

        # Verify each level is a parent path of the next
        assert customer_mount.startswith(tenant_mount + "/")
        assert project_mount.startswith(customer_mount + "/")


class TestHierarchyFiltering:
    """Tests for filtering hierarchical resources."""

    def test_filter_maintains_hierarchy(self, backend):
        """Test that filtering by data_type maintains the hierarchy."""
        # Use two separate HierarchyBuilders for different data types
        builder_store = HierarchyBuilder(storage_file_system="lustre")
        builder_scratch = HierarchyBuilder(storage_file_system="lustre")

        # Add store resources
        builder_store.get_or_create_tenant(
            tenant_id="cscs",
            tenant_name="CSCS",
            storage_system="capstor",
            storage_data_type="store",
        )

        builder_store.get_or_create_customer(
            customer_info=CustomerInfo(
                itemId="cust1",
                key="mch",
                name="MCH",
            ),
            storage_system="capstor",
            storage_data_type="store",
            tenant_id="cscs",
        )

        # Add scratch resources
        builder_scratch.get_or_create_tenant(
            tenant_id="cscs",
            tenant_name="CSCS",
            storage_system="capstor",
            storage_data_type="scratch",
        )

        builder_scratch.get_or_create_customer(
            customer_info=CustomerInfo(
                itemId="cust2",
                key="eth",
                name="ETH",
            ),
            storage_system="capstor",
            storage_data_type="scratch",
            tenant_id="cscs",
        )

        # Combine all resources
        all_resources = (
            builder_store.get_hierarchy_resources()
            + builder_scratch.get_hierarchy_resources()
        )

        # Filter by data_type
        # predicate = make_storage_resource_predicate(data_type=StorageDataType.STORE)
        # Using lambda replacement
        store_resources = list(
            filter(lambda r: r.storageDataType.key == "store", all_resources)
        )

        # Verify only store resources returned
        assert len(store_resources) == 2
        for resource in store_resources:
            assert resource.storageDataType.key == "store"

        # Verify hierarchy is maintained
        store_tenants = [r for r in store_resources if r.target.targetType == "tenant"]
        store_customers = [
            r for r in store_resources if r.target.targetType == "customer"
        ]

        assert len(store_tenants) == 1
        assert len(store_customers) == 1
        assert store_customers[0].parentItemId == store_tenants[0].itemId


class TestEdgeCases:
    """Tests for edge cases and error handling."""

    @pytest.mark.asyncio
    async def test_resource_without_customer_info(self, backend):
        """Test handling resource when customer info is not available."""
        backend.waldur_service.get_offering_customers.return_value = {}
        resource = create_mock_resource(customer_slug="unknown-customer")

        # Process with empty customer info using HierarchyBuilder
        hierarchy_builder = HierarchyBuilder(storage_file_system="lustre")

        hierarchy_builder.get_or_create_tenant(
            tenant_id="cscs",
            tenant_name="CSCS",
            storage_system="capstor",
            storage_data_type="store",
        )

        # Project should still be created but without parent
        project_resource = await backend.mapper.map_resource(resource, "capstor")
        if project_resource:
            hierarchy_builder.assign_parent_to_project(
                project_resource=project_resource,
                customer_slug="unknown-customer",
                storage_system="capstor",
                storage_data_type="store",
            )

        storage_resources = hierarchy_builder.get_hierarchy_resources()
        if project_resource:
            storage_resources.append(project_resource)

        # Verify results
        assert len(storage_resources) == 2  # Only tenant and project
        tenants = [r for r in storage_resources if r.target.targetType == "tenant"]
        projects = [r for r in storage_resources if r.target.targetType == "project"]

        assert len(tenants) == 1
        assert len(projects) == 1
        assert projects[0].parentItemId is None

    def test_duplicate_prevention(self):
        """Test that duplicate entries are not created by HierarchyBuilder."""
        hierarchy_builder = HierarchyBuilder(storage_file_system="lustre")

        # Create the same tenant multiple times
        for _ in range(3):
            hierarchy_builder.get_or_create_tenant(
                tenant_id="cscs",
                tenant_name="CSCS",
                storage_system="capstor",
                storage_data_type="store",
            )

        # Should only have one tenant
        tenants = hierarchy_builder.get_hierarchy_resources()
        assert len(tenants) == 1

        # Create the same customer multiple times
        for _ in range(3):
            hierarchy_builder.get_or_create_customer(
                customer_info=CustomerInfo(
                    itemId="cust1",
                    key="mch",
                    name="MCH",
                ),
                storage_system="capstor",
                storage_data_type="store",
                tenant_id="cscs",
            )

        # Should have one tenant and one customer
        resources = hierarchy_builder.get_hierarchy_resources()
        assert len(resources) == 2


class TestIntegrationScenarios:
    """Integration tests for realistic scenarios."""

    @pytest.mark.asyncio
    async def test_multi_storage_system_hierarchy(self, backend):
        """Test hierarchy with multiple storage systems."""
        backend.waldur_service.get_offering_customers.return_value = {
            "customer1": CustomerInfo(
                itemId="c1",
                key="customer1",
                name="Customer 1",
            ),
        }

        # Create resources across different storage systems
        resources = [
            create_mock_resource(
                customer_slug="customer1",
                project_slug="proj1",
                offering_slug="capstor",
                storage_data_type="store",
            ),
            create_mock_resource(
                customer_slug="customer1",
                project_slug="proj2",
                offering_slug="vast",
                storage_data_type="scratch",
            ),
            create_mock_resource(
                customer_slug="customer1",
                project_slug="proj3",
                offering_slug="iopsstor",
                storage_data_type="archive",
            ),
        ]

        hierarchy_builder = HierarchyBuilder(storage_file_system="lustre")
        project_resources = []

        for resource in resources:
            storage_system = resource.offering_slug
            data_type = resource.attributes.storage_data_type
            tenant_id = resource.provider_slug

            # Create tenant
            hierarchy_builder.get_or_create_tenant(
                tenant_id=tenant_id,
                tenant_name="CSCS",
                storage_system=storage_system,
                storage_data_type=data_type,
            )

            # Create customer
            customer_info = backend.waldur_service.get_offering_customers.return_value[
                "customer1"
            ]
            hierarchy_builder.get_or_create_customer(
                customer_info=customer_info,
                storage_system=storage_system,
                storage_data_type=data_type,
                tenant_id=tenant_id,
            )

            # Create project
            project = await backend.mapper.map_resource(resource, storage_system)
            if project is not None:
                hierarchy_builder.assign_parent_to_project(
                    project_resource=project,
                    customer_slug="customer1",
                    storage_system=storage_system,
                    storage_data_type=data_type,
                )
                project_resources.append(project)

        all_resources = hierarchy_builder.get_hierarchy_resources() + project_resources

        # Verify we have 3 separate hierarchies
        tenants = [r for r in all_resources if r.target.targetType == "tenant"]
        customers = [r for r in all_resources if r.target.targetType == "customer"]
        projects = [r for r in all_resources if r.target.targetType == "project"]

        assert len(tenants) == 3  # One per storage system
        assert len(customers) == 3  # One per storage system
        assert len(projects) >= 2  # At least some projects created

        # Verify each hierarchy is independent
        storage_systems = set(t.storageSystem.key for t in tenants)
        assert storage_systems == {"capstor", "vast", "iopsstor"}
