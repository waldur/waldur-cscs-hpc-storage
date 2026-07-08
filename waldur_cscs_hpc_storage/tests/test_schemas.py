from unittest.mock import Mock

from waldur_api_client.models.order_details import OrderDetails
from waldur_api_client.models.order_state import OrderState
from waldur_api_client.models.resource import Resource
from waldur_api_client.models.resource_state import ResourceState

from waldur_cscs_hpc_storage.models import (
    ParsedWaldurResource,
    ResourceAttributes,
    StorageResourceFilter,
)
from waldur_cscs_hpc_storage.tests.conftest import make_test_uuid


class TestParsedWaldurResource:
    def test_callback_urls_empty(self):
        resource = ParsedWaldurResource(
            uuid=make_test_uuid("123"),
            offering_uuid=make_test_uuid("456"),
            project_uuid=make_test_uuid("789"),
            customer_uuid=make_test_uuid("abc"),
            order_in_progress=None,
        )
        assert resource.callback_urls == {}

    def test_callback_urls_pending_provider(self):
        mock_order = Mock(spec=OrderDetails)
        mock_order.state = OrderState.PENDING_PROVIDER
        mock_order.url = "http://example.com/api/marketplace-orders/123/"

        resource_uuid = make_test_uuid("123")
        resource = ParsedWaldurResource(
            uuid=resource_uuid,
            offering_uuid=make_test_uuid("456"),
            project_uuid=make_test_uuid("789"),
            customer_uuid=make_test_uuid("abc"),
            order_in_progress=mock_order,
        )

        urls = resource.callback_urls
        # Order-level actions
        assert "approve_by_provider_url" in urls
        assert (
            urls["approve_by_provider_url"]
            == "http://example.com/api/marketplace-orders/123/approve_by_provider/"
        )
        assert "reject_by_provider_url" in urls
        assert (
            urls["reject_by_provider_url"]
            == "http://example.com/api/marketplace-orders/123/reject_by_provider/"
        )
        # Resource-level actions
        assert "set_backend_id_url" in urls
        assert (
            urls["set_backend_id_url"]
            == f"http://example.com/api/marketplace-provider-resources/{resource_uuid}/set_backend_id/"
        )
        assert "update_resource_options_url" in urls
        assert (
            urls["update_resource_options_url"]
            == f"http://example.com/api/marketplace-provider-resources/{resource_uuid}/update_options_direct/"
        )

    def test_callback_urls_executing(self):
        mock_order = Mock(spec=OrderDetails)
        mock_order.state = OrderState.EXECUTING
        mock_order.url = "http://example.com/api/marketplace-orders/123/"

        resource_uuid = make_test_uuid("123")
        resource = ParsedWaldurResource(
            uuid=resource_uuid,
            offering_uuid=make_test_uuid("456"),
            project_uuid=make_test_uuid("789"),
            customer_uuid=make_test_uuid("abc"),
            order_in_progress=mock_order,
        )

        urls = resource.callback_urls
        # Order-level actions
        assert "set_state_done_url" in urls
        assert (
            urls["set_state_done_url"]
            == "http://example.com/api/marketplace-orders/123/set_state_done/"
        )
        assert "set_state_erred_url" in urls
        assert (
            urls["set_state_erred_url"]
            == "http://example.com/api/marketplace-orders/123/set_state_erred/"
        )
        # Resource-level actions
        assert "set_backend_id_url" in urls
        assert (
            urls["set_backend_id_url"]
            == f"http://example.com/api/marketplace-provider-resources/{resource_uuid}/set_backend_id/"
        )
        assert "update_resource_options_url" not in urls

    def test_callback_urls_done(self):
        mock_order = Mock(spec=OrderDetails)
        mock_order.state = OrderState.DONE
        mock_order.url = "http://example.com/api/marketplace-orders/123/"

        resource_uuid = make_test_uuid("123")
        resource = ParsedWaldurResource(
            uuid=resource_uuid,
            offering_uuid=make_test_uuid("456"),
            project_uuid=make_test_uuid("789"),
            customer_uuid=make_test_uuid("abc"),
            order_in_progress=mock_order,
        )

        urls = resource.callback_urls
        # No order-level actions for DONE state
        assert "approve_by_provider_url" not in urls
        assert "set_state_done_url" not in urls
        # Resource-level actions are always available when order URL is valid
        assert "set_backend_id_url" in urls
        assert (
            urls["set_backend_id_url"]
            == f"http://example.com/api/marketplace-provider-resources/{resource_uuid}/set_backend_id/"
        )
        assert "update_resource_options_url" not in urls

    def test_callback_urls_set_state_ok(self):
        for state in [
            ResourceState.ERRED,
            ResourceState.CREATING,
            ResourceState.UPDATING,
            ResourceState.TERMINATING,
        ]:
            resource_uuid = make_test_uuid("123")
            resource = ParsedWaldurResource(
                uuid=resource_uuid,
                offering_uuid=make_test_uuid("456"),
                project_uuid=make_test_uuid("789"),
                customer_uuid=make_test_uuid("abc"),
                state=state,
                order_in_progress=Mock(
                    spec=OrderDetails,
                    url="http://example.com/api/marketplace-orders/123/",
                ),
            )
            urls = resource.callback_urls
            assert "set_state_ok_url" in urls
            assert (
                urls["set_state_ok_url"]
                == f"http://example.com/api/marketplace-provider-resources/{resource_uuid}/set_state_ok/"
            )

        # Test disallowed state
        resource = ParsedWaldurResource(
            uuid=make_test_uuid("123"),
            offering_uuid=make_test_uuid("456"),
            project_uuid=make_test_uuid("789"),
            customer_uuid=make_test_uuid("abc"),
            state=ResourceState.OK,
            order_in_progress=Mock(
                spec=OrderDetails, url="http://example.com/api/marketplace-orders/123/"
            ),
        )
        assert "set_state_ok_url" not in resource.callback_urls


class TestResourceStateParsing:
    def _get_resource(self, state, order_state=None):
        resource = Mock(spec=Resource)
        resource.uuid = make_test_uuid("123")
        resource.name = "Test Resource"
        resource.slug = "test-resource"
        resource.state = state
        resource.offering_uuid = make_test_uuid("456")
        resource.offering_name = "Test Offering"
        resource.offering_slug = "test-offering"
        resource.project_uuid = make_test_uuid("789")
        resource.project_name = "Test Project"
        resource.project_slug = "test-project"
        resource.customer_uuid = make_test_uuid("abc")
        resource.customer_name = "Test Customer"
        resource.customer_slug = "test-customer"
        resource.provider_slug = "test-provider"
        resource.provider_name = "Test Provider"
        resource.limits = None
        resource.attributes = None
        resource.options = None
        resource.backend_metadata = None

        if order_state:
            order = Mock(spec=OrderDetails)
            order.state = order_state
            resource.order_in_progress = order
        else:
            resource.order_in_progress = None

        return resource

    def test_updating_pending_consumer(self):
        resource = self._get_resource(
            ResourceState.UPDATING, OrderState.PENDING_CONSUMER
        )
        parsed = ParsedWaldurResource.from_waldur_resource(resource)
        assert parsed.state == ResourceState.OK

    def test_terminating_pending_consumer(self):
        resource = self._get_resource(
            ResourceState.TERMINATING, OrderState.PENDING_CONSUMER
        )
        parsed = ParsedWaldurResource.from_waldur_resource(resource)
        assert parsed.state == ResourceState.OK

    def test_updating_executing(self):
        resource = self._get_resource(ResourceState.UPDATING, OrderState.EXECUTING)
        parsed = ParsedWaldurResource.from_waldur_resource(resource)
        assert parsed.state == ResourceState.UPDATING

    def test_ok_pending_consumer(self):
        resource = self._get_resource(ResourceState.OK, OrderState.PENDING_CONSUMER)
        parsed = ParsedWaldurResource.from_waldur_resource(resource)
        assert parsed.state == ResourceState.OK

    def test_no_order(self):
        resource = self._get_resource(ResourceState.UPDATING)
        parsed = ParsedWaldurResource.from_waldur_resource(resource)
        assert parsed.state == ResourceState.UPDATING


class TestResourceAttributesDataType:
    """The storage data type is defined per offering, so any value is accepted
    and only normalized — it must never be silently rewritten to a default."""

    def test_builtin_type_preserved(self):
        assert ResourceAttributes(storage_data_type="scratch").storage_data_type == (
            "scratch"
        )

    def test_offering_defined_type_preserved(self):
        # Regression: "workspace" used to be coerced to "store".
        assert (
            ResourceAttributes(storage_data_type="workspace").storage_data_type
            == "workspace"
        )

    def test_case_is_normalized(self):
        # Offering choices may be capitalized (e.g. "Workspace").
        assert (
            ResourceAttributes(storage_data_type="Workspace").storage_data_type
            == "workspace"
        )

    def test_whitespace_is_stripped(self):
        assert (
            ResourceAttributes(storage_data_type="  Store  ").storage_data_type
            == "store"
        )

    def test_empty_falls_back_to_store(self):
        assert ResourceAttributes(storage_data_type="").storage_data_type == "store"

    def test_none_falls_back_to_store(self):
        assert ResourceAttributes(storage_data_type=None).storage_data_type == "store"

    def test_default_is_store(self):
        assert ResourceAttributes().storage_data_type == "store"


class TestStorageResourceFilterDataType:
    """Filtering by an offering-defined data type must not raise (previously a
    non-enum value produced a 422) and must normalize the same way."""

    def test_offering_defined_type_accepted(self):
        assert StorageResourceFilter(data_type="workspace").data_type == "workspace"

    def test_case_is_normalized(self):
        assert StorageResourceFilter(data_type="Workspace").data_type == "workspace"

    def test_blank_becomes_none(self):
        assert StorageResourceFilter(data_type="  ").data_type is None

    def test_default_is_none(self):
        assert StorageResourceFilter().data_type is None
