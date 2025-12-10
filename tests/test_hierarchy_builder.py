"""Tests for HierarchyBuilder class."""

import pytest

from waldur_cscs_hpc_storage.hierarchy_builder import CustomerInfo, HierarchyBuilder
from waldur_cscs_hpc_storage.models.enums import TargetStatus, TargetType
from waldur_cscs_hpc_storage.models import StorageResource, MountPoint, Permission


class TestHierarchyBuilder:
    """Tests for the HierarchyBuilder class."""

    @pytest.fixture
    def builder(self):
        """Create a HierarchyBuilder instance for testing."""
        return HierarchyBuilder(storage_file_system="GPFS")

    def test_get_or_create_tenant_new(self, builder):
        """Test creating a new tenant entry."""
        tenant_id = builder.get_or_create_tenant(
            tenant_id="cscs",
            tenant_name="CSCS",
            storage_system="capstor",
            storage_data_type="store",
            offering_uuid="offering-uuid-123",
        )

        assert tenant_id == "offering-uuid-123"

        # Verify the tenant resource was created
        resources = builder.get_hierarchy_resources()
        assert len(resources) == 1

        tenant_resource = resources[0]
        assert tenant_resource.itemId == "offering-uuid-123"
        assert tenant_resource.target.targetType == TargetType.TENANT
        assert tenant_resource.target.targetItem.key == "cscs"
        assert tenant_resource.target.targetItem.name == "CSCS"
        assert tenant_resource.storageSystem.key == "capstor"
        assert tenant_resource.storageDataType.key == "store"
        assert tenant_resource.parentItemId is None
        assert tenant_resource.status == TargetStatus.PENDING

    def test_get_or_create_tenant_existing(self, builder):
        """Test that second call to get_or_create_tenant returns the same ID."""
        tenant_id_1 = builder.get_or_create_tenant(
            tenant_id="cscs",
            tenant_name="CSCS",
            storage_system="capstor",
            storage_data_type="store",
            offering_uuid="offering-uuid-123",
        )

        tenant_id_2 = builder.get_or_create_tenant(
            tenant_id="cscs",
            tenant_name="CSCS Different Name",
            storage_system="capstor",
            storage_data_type="store",
            offering_uuid="different-uuid",
        )

        assert tenant_id_1 == tenant_id_2
        # Only one resource should be created
        assert len(builder.get_hierarchy_resources()) == 1

    def test_get_or_create_tenant_different_data_types(self, builder):
        """Test that different data types create different tenant entries."""
        tenant_id_store = builder.get_or_create_tenant(
            tenant_id="cscs",
            tenant_name="CSCS",
            storage_system="capstor",
            storage_data_type="store",
            offering_uuid="uuid-store",
        )

        tenant_id_scratch = builder.get_or_create_tenant(
            tenant_id="cscs",
            tenant_name="CSCS",
            storage_system="capstor",
            storage_data_type="scratch",
            offering_uuid="uuid-scratch",
        )

        assert tenant_id_store == "uuid-store"
        assert tenant_id_scratch == "uuid-scratch"
        assert len(builder.get_hierarchy_resources()) == 2

    def test_get_or_create_customer_new(self, builder):
        """Test creating a new customer entry."""
        # First create a tenant (required for parent reference)
        builder.get_or_create_tenant(
            tenant_id="cscs",
            tenant_name="CSCS",
            storage_system="capstor",
            storage_data_type="store",
            offering_uuid="tenant-uuid",
        )

        customer_info = CustomerInfo(
            itemId="customer-uuid-123",
            key="ethz",
            name="ETH Zurich",
        )

        customer_id = builder.get_or_create_customer(
            customer_info=customer_info,
            storage_system="capstor",
            storage_data_type="store",
            tenant_id="cscs",
        )

        assert customer_id == "customer-uuid-123"

        resources = builder.get_hierarchy_resources()
        assert len(resources) == 2

        customer_resource = resources[1]
        assert customer_resource.itemId == "customer-uuid-123"
        assert customer_resource.target.targetType == TargetType.CUSTOMER
        assert customer_resource.target.targetItem.key == "ethz"
        assert customer_resource.target.targetItem.name == "ETH Zurich"
        assert customer_resource.parentItemId == "tenant-uuid"

    def test_get_or_create_customer_existing(self, builder):
        """Test that second call to get_or_create_customer returns the same ID."""
        builder.get_or_create_tenant(
            tenant_id="cscs",
            tenant_name="CSCS",
            storage_system="capstor",
            storage_data_type="store",
            offering_uuid="tenant-uuid",
        )

        customer_info = CustomerInfo(
            itemId="customer-uuid-123",
            key="ethz",
            name="ETH Zurich",
        )

        customer_id_1 = builder.get_or_create_customer(
            customer_info=customer_info,
            storage_system="capstor",
            storage_data_type="store",
            tenant_id="cscs",
        )

        customer_id_2 = builder.get_or_create_customer(
            customer_info=CustomerInfo(
                itemId="different-uuid",
                key="ethz",
                name="Different",
            ),
            storage_system="capstor",
            storage_data_type="store",
            tenant_id="cscs",
        )

        assert customer_id_1 == customer_id_2
        # Only tenant + one customer should exist
        assert len(builder.get_hierarchy_resources()) == 2

    def test_get_or_create_customer_without_key(self, builder):
        """Test that customer info without 'key' returns None."""
        builder.get_or_create_tenant(
            tenant_id="cscs",
            tenant_name="CSCS",
            storage_system="capstor",
            storage_data_type="store",
            offering_uuid="tenant-uuid",
        )

        customer_info = CustomerInfo(
            itemId="customer-uuid", name="No Key Customer", key=""
        )

        customer_id = builder.get_or_create_customer(
            customer_info=customer_info,
            storage_system="capstor",
            storage_data_type="store",
            tenant_id="cscs",
        )

        assert customer_id is None
        # Only tenant should exist
        assert len(builder.get_hierarchy_resources()) == 1

    def test_get_customer_uuid(self, builder):
        """Test retrieving customer ID."""
        builder.get_or_create_tenant(
            tenant_id="cscs",
            tenant_name="CSCS",
            storage_system="capstor",
            storage_data_type="store",
            offering_uuid="tenant-uuid",
        )

        customer_info = CustomerInfo(
            itemId="customer-uuid-123",
            key="ethz",
            name="ETH Zurich",
        )

        builder.get_or_create_customer(
            customer_info=customer_info,
            storage_system="capstor",
            storage_data_type="store",
            tenant_id="cscs",
        )

        # Test retrieving existing customer
        customer_id = builder.get_customer_uuid(
            customer_slug="ethz",
            storage_system="capstor",
            storage_data_type="store",
        )
        assert customer_id == "customer-uuid-123"

        # Test retrieving non-existent customer
        non_existent = builder.get_customer_uuid(
            customer_slug="unknown",
            storage_system="capstor",
            storage_data_type="store",
        )
        assert non_existent is None

    def test_assign_parent_to_project(self, builder):
        """Test assigning parentItemId to a project resource."""
        builder.get_or_create_tenant(
            tenant_id="cscs",
            tenant_name="CSCS",
            storage_system="capstor",
            storage_data_type="store",
            offering_uuid="tenant-uuid",
        )

        customer_info = CustomerInfo(
            itemId="customer-uuid-123",
            key="ethz",
            name="ETH Zurich",
        )

        builder.get_or_create_customer(
            customer_info=customer_info,
            storage_system="capstor",
            storage_data_type="store",
            tenant_id="cscs",
        )

        # Create a mock project resource
        from waldur_cscs_hpc_storage.models import (
            Target,
            StorageItem,
            ProjectTargetItem,
        )

        project_resource = StorageResource(
            itemId="project-resource-123",
            status=TargetStatus.ACTIVE,
            mountPoint=MountPoint(default="/test/path"),
            permission=Permission(value="775"),
            quotas=None,
            target=Target(
                targetType=TargetType.PROJECT,
                targetItem=ProjectTargetItem(itemId="project-123"),
            ),
            storageSystem=StorageItem(itemId="ss-1", key="capstor", name="CAPSTOR"),
            storageFileSystem=StorageItem(itemId="fs-1", key="gpfs", name="GPFS"),
            storageDataType=StorageItem(itemId="dt-1", key="store", name="STORE"),
            parentItemId=None,
        )

        builder.assign_parent_to_project(
            project_resource=project_resource,
            customer_slug="ethz",
            storage_system="capstor",
            storage_data_type="store",
        )

        assert project_resource.parentItemId == "customer-uuid-123"

    def test_assign_parent_to_project_no_matching_customer(self, builder):
        """Test that parentItemId remains None when customer doesn't exist."""
        from waldur_cscs_hpc_storage.models import (
            Target,
            StorageItem,
            ProjectTargetItem,
        )

        project_resource = StorageResource(
            itemId="project-resource-123",
            status=TargetStatus.ACTIVE,
            mountPoint=MountPoint(default="/test/path"),
            permission=Permission(value="775"),
            quotas=None,
            target=Target(
                targetType=TargetType.PROJECT,
                targetItem=ProjectTargetItem(itemId="project-123"),
            ),
            storageSystem=StorageItem(itemId="ss-1", key="capstor", name="CAPSTOR"),
            storageFileSystem=StorageItem(itemId="fs-1", key="gpfs", name="GPFS"),
            storageDataType=StorageItem(itemId="dt-1", key="store", name="STORE"),
            parentItemId=None,
        )

        builder.assign_parent_to_project(
            project_resource=project_resource,
            customer_slug="non-existent",
            storage_system="capstor",
            storage_data_type="store",
        )

        assert project_resource.parentItemId is None

    def test_get_hierarchy_resources_returns_copy(self, builder):
        """Test that get_hierarchy_resources returns a copy of the list."""
        builder.get_or_create_tenant(
            tenant_id="cscs",
            tenant_name="CSCS",
            storage_system="capstor",
            storage_data_type="store",
            offering_uuid="tenant-uuid",
        )

        resources1 = builder.get_hierarchy_resources()
        resources2 = builder.get_hierarchy_resources()

        # Should be different list objects
        assert resources1 is not resources2
        # But with the same content
        assert resources1 == resources2

    def test_reset(self, builder):
        """Test that reset clears all tracked entries."""
        builder.get_or_create_tenant(
            tenant_id="cscs",
            tenant_name="CSCS",
            storage_system="capstor",
            storage_data_type="store",
            offering_uuid="tenant-uuid",
        )

        customer_info = CustomerInfo(
            itemId="customer-uuid",
            key="ethz",
            name="ETH",
        )
        builder.get_or_create_customer(
            customer_info=customer_info,
            storage_system="capstor",
            storage_data_type="store",
            tenant_id="cscs",
        )

        assert len(builder.get_hierarchy_resources()) == 2

        builder.reset()

        assert len(builder.get_hierarchy_resources()) == 0
        assert builder.get_customer_uuid("ethz", "capstor", "store") is None

    def test_tenant_without_offering_uuid_generates_deterministic_id(self, builder):
        """Test that tenant without offering_uuid generates a deterministic ID."""
        tenant_id_1 = builder.get_or_create_tenant(
            tenant_id="cscs",
            tenant_name="CSCS",
            storage_system="capstor",
            storage_data_type="store",
            offering_uuid=None,
        )

        # Reset and create again
        builder.reset()

        tenant_id_2 = builder.get_or_create_tenant(
            tenant_id="cscs",
            tenant_name="CSCS",
            storage_system="capstor",
            storage_data_type="store",
            offering_uuid=None,
        )

        # Same inputs should generate same ID
        assert tenant_id_1 == tenant_id_2
