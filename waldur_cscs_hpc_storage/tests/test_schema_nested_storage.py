from unittest.mock import Mock
from uuid import uuid4
from waldur_cscs_hpc_storage.models.schemas import (
    ParsedWaldurResource,
    ResourceState,
    StorageDataType,
)
from waldur_api_client.models.resource import Resource
from waldur_api_client.types import Unset


def test_parsed_waldur_resource_nested_storage():
    # Helper to generate UUIDs
    def gen_uuid():
        return Mock(hex=str(uuid4()))

    # Create mock Resource with nested storage in attributes
    mock_resource = Mock(spec=Resource)
    mock_resource.uuid = gen_uuid()
    mock_resource.name = "Test Resource"
    mock_resource.slug = "test-resource"
    mock_resource.state = ResourceState.OK
    mock_resource.offering_uuid = gen_uuid()
    mock_resource.offering_name = "Offering"
    mock_resource.offering_slug = "offering"
    mock_resource.project_uuid = gen_uuid()
    mock_resource.project_name = "Project"
    mock_resource.project_slug = "project"
    mock_resource.customer_uuid = gen_uuid()
    mock_resource.customer_name = "Customer"
    mock_resource.customer_slug = "customer"
    mock_resource.provider_slug = Unset()
    mock_resource.provider_name = Unset()

    mock_resource.limits = None
    mock_resource.backend_metadata = None
    mock_resource.order_in_progress = None
    mock_resource.options = {}  # Empty options

    # The new attributes payload structure
    mock_resource.attributes = Mock(
        additional_properties={
            "storage": {
                "storage_data_type": "store",  # Expecting 'store', not 'default' which maps to store anyway
                "permissions": "2770",
                "hard_quota_space": 100,
                "soft_quota_inodes": 500,
                "hard_quota_inodes": 1000,
            },
            "name": "some-name",
        }
    )

    parsed = ParsedWaldurResource.from_waldur_resource(mock_resource)

    # Verify attributes extracted from storage dict
    assert parsed.attributes.storage.permissions == "2770"
    assert parsed.attributes.storage.storage_data_type == StorageDataType.STORE

    # Verify options extracted from storage dict
    assert parsed.options.hard_quota_space == 100
    assert parsed.options.soft_quota_inodes == 500
    assert parsed.options.hard_quota_inodes == 1000


def test_parsed_waldur_resource_nested_storage_defaults():
    # Helper to generate UUIDs
    def gen_uuid():
        return Mock(hex=str(uuid4()))

    # Create mock Resource with nested storage in attributes
    mock_resource = Mock(spec=Resource)
    mock_resource.uuid = gen_uuid()
    mock_resource.name = "Test Resource"
    mock_resource.slug = "test-resource"
    mock_resource.state = ResourceState.OK
    mock_resource.offering_uuid = gen_uuid()
    mock_resource.offering_name = "Offering"
    mock_resource.offering_slug = "offering"
    mock_resource.project_uuid = gen_uuid()
    mock_resource.project_name = "Project"
    mock_resource.project_slug = "project"
    mock_resource.customer_uuid = gen_uuid()
    mock_resource.customer_name = "Customer"
    mock_resource.customer_slug = "customer"
    mock_resource.provider_slug = Unset()
    mock_resource.provider_name = Unset()

    mock_resource.limits = None
    mock_resource.backend_metadata = None
    mock_resource.order_in_progress = None
    mock_resource.options = {}

    # 'default' data type should map to STORE
    mock_resource.attributes = Mock(
        additional_properties={
            "storage": {
                "storage_data_type": "default",
            }
        }
    )

    parsed = ParsedWaldurResource.from_waldur_resource(mock_resource)

    # Verify attributes extracted from storage dict
    assert parsed.attributes.storage.storage_data_type == StorageDataType.STORE


def test_parsed_waldur_resource_custom_storage_field():
    # Helper to generate UUIDs
    def gen_uuid():
        return Mock(hex=str(uuid4()))

    # Create mock Resource
    mock_resource = Mock(spec=Resource)
    mock_resource.uuid = gen_uuid()
    mock_resource.name = "Test Resource"
    mock_resource.slug = "test-resource"
    mock_resource.state = ResourceState.OK
    mock_resource.offering_uuid = gen_uuid()
    mock_resource.offering_name = "Offering"
    mock_resource.offering_slug = "offering"
    mock_resource.project_uuid = gen_uuid()
    mock_resource.project_name = "Project"
    mock_resource.project_slug = "project"
    mock_resource.customer_uuid = gen_uuid()
    mock_resource.customer_name = "Customer"
    mock_resource.customer_slug = "customer"
    mock_resource.provider_slug = Unset()
    mock_resource.provider_name = Unset()

    mock_resource.limits = None
    mock_resource.backend_metadata = None
    mock_resource.order_in_progress = None
    mock_resource.options = {}

    # Use a custom field name 'custom_storage'
    mock_resource.attributes = Mock(
        additional_properties={
            "custom_storage": {
                "storage_data_type": "scratch",
                "permissions": "2770",
            }
        }
    )

    # Pass the custom field name
    parsed = ParsedWaldurResource.from_waldur_resource(
        mock_resource, storage_attributes_field="custom_storage"
    )

    # Verify fields are in the nested storage object
    assert parsed.attributes.storage.storage_data_type == StorageDataType.SCRATCH
    assert parsed.attributes.storage.permissions == "2770"


def test_parsed_waldur_resource_legacy_attributes_ignored():
    """Test that top-level (legacy) attributes are ignored if not in the nested dict."""

    # Helper to generate UUIDs
    def gen_uuid():
        return Mock(hex=str(uuid4()))

    mock_resource = Mock(spec=Resource)
    mock_resource.uuid = gen_uuid()
    mock_resource.name = "Test Resource"
    mock_resource.slug = "test-resource"
    mock_resource.state = ResourceState.OK
    mock_resource.offering_uuid = gen_uuid()
    mock_resource.offering_name = "Offering"
    mock_resource.offering_slug = "offering"
    mock_resource.project_uuid = gen_uuid()
    mock_resource.project_name = "Project"
    mock_resource.project_slug = "project"
    mock_resource.customer_uuid = gen_uuid()
    mock_resource.customer_name = "Customer"
    mock_resource.customer_slug = "customer"
    mock_resource.provider_slug = Unset()
    mock_resource.provider_name = Unset()

    mock_resource.limits = None
    mock_resource.backend_metadata = None
    mock_resource.order_in_progress = None
    mock_resource.options = {}

    # Attributes containing top-level fields (Legacy) AND a nested storage dict
    mock_resource.attributes = Mock(
        additional_properties={
            # Legacy fields - Should be IGNORED
            "permissions": "0000",
            "storage_data_type": "scratch",
            # Nested storage dict - Should be USED
            "storage": {
                "storage_data_type": "store",
                "permissions": "2770",
            },
        }
    )

    parsed = ParsedWaldurResource.from_waldur_resource(mock_resource)

    # Verify that values come from 'storage' dict, NOT top-level
    assert parsed.attributes.storage.permissions == "2770"
    assert parsed.attributes.storage.storage_data_type == StorageDataType.STORE
