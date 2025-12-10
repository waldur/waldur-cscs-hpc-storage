from unittest.mock import Mock
from waldur_api_client.models.order_state import OrderState
from waldur_api_client.models.order_details import OrderDetails
from waldur_cscs_hpc_storage.base.schemas import ParsedWaldurResource


class TestParsedWaldurResource:
    def test_callback_urls_empty(self):
        resource = ParsedWaldurResource(
            uuid="123",
            offering_uuid="456",
            project_uuid="789",
            customer_uuid="abc",
            order_in_progress=None,
        )
        assert resource.callback_urls == {}

    def test_callback_urls_pending_provider(self):
        mock_order = Mock(spec=OrderDetails)
        mock_order.state = OrderState.PENDING_PROVIDER
        mock_order.url = "http://example.com/api/orders/123/"

        resource = ParsedWaldurResource(
            uuid="123",
            offering_uuid="456",
            project_uuid="789",
            customer_uuid="abc",
            order_in_progress=mock_order,
        )

        urls = resource.callback_urls
        assert "approve_by_provider_url" in urls
        assert (
            urls["approve_by_provider_url"]
            == "http://example.com/api/orders/123/approve_by_provider/"
        )
        assert "reject_by_provider_url" in urls
        assert (
            urls["reject_by_provider_url"]
            == "http://example.com/api/orders/123/reject_by_provider/"
        )

    def test_callback_urls_executing(self):
        mock_order = Mock(spec=OrderDetails)
        mock_order.state = OrderState.EXECUTING
        mock_order.url = "http://example.com/api/orders/123/"

        resource = ParsedWaldurResource(
            uuid="123",
            offering_uuid="456",
            project_uuid="789",
            customer_uuid="abc",
            order_in_progress=mock_order,
        )

        urls = resource.callback_urls
        assert "set_state_done_url" in urls
        assert (
            urls["set_state_done_url"]
            == "http://example.com/api/orders/123/set_state_done/"
        )
        assert "set_state_erred_url" in urls
        assert (
            urls["set_state_erred_url"]
            == "http://example.com/api/orders/123/set_state_erred/"
        )

    def test_callback_urls_done(self):
        mock_order = Mock(spec=OrderDetails)
        mock_order.state = OrderState.DONE
        mock_order.url = "http://example.com/api/orders/123/"

        resource = ParsedWaldurResource(
            uuid="123",
            offering_uuid="456",
            project_uuid="789",
            customer_uuid="abc",
            order_in_progress=mock_order,
        )

        urls = resource.callback_urls
        assert "set_backend_id_url" in urls
        assert (
            urls["set_backend_id_url"]
            == "http://example.com/api/orders/123/set_backend_id/"
        )
