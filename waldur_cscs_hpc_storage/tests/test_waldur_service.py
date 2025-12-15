import pytest
from unittest.mock import AsyncMock, Mock, patch
from waldur_cscs_hpc_storage.services.waldur_service import WaldurService
from waldur_cscs_hpc_storage.config import WaldurApiConfig
from waldur_cscs_hpc_storage.exceptions import WaldurClientError


class TestWaldurService:
    @pytest.fixture
    def waldur_api_config(self):
        return WaldurApiConfig(
            api_url="https://example.com",
            access_token="e38cd56f1ce5bf4ef35905f2bdcf84f1d7f2cc5e",
        )

    @pytest.fixture
    def service(self, waldur_api_config):
        with patch(
            "waldur_cscs_hpc_storage.services.waldur_service.AuthenticatedClient"
        ) as mock_client_class:
            mock_client_class.return_value = Mock()
            service = WaldurService(waldur_api_config)
            return service

    @pytest.mark.asyncio
    @patch(
        "waldur_cscs_hpc_storage.services.waldur_service.marketplace_provider_offerings_customers_list"
    )
    async def test_get_offering_customers_success(self, mock_list, service):
        mock_customer = Mock()
        mock_customer.slug = "customer-1"
        mock_customer.name = "Customer 1"
        mock_customer.uuid.hex = "uuid-1"
        mock_list.asyncio_all = AsyncMock(return_value=[mock_customer])

        customers = await service.get_offering_customers("offering-uuid")

        assert len(customers) == 1
        assert customers["customer-1"].key == "customer-1"
        assert customers["customer-1"].itemId == "uuid-1"
        mock_list.asyncio_all.assert_called_once_with(
            uuid="offering-uuid", client=service.client
        )

    @pytest.mark.asyncio
    @patch(
        "waldur_cscs_hpc_storage.services.waldur_service.marketplace_provider_offerings_customers_list"
    )
    async def test_get_offering_customers_empty(self, mock_list, service):
        mock_list.asyncio_all = AsyncMock(return_value=[])

        customers = await service.get_offering_customers("offering-uuid")

        assert customers == {}

    @pytest.mark.asyncio
    @patch(
        "waldur_cscs_hpc_storage.services.waldur_service.marketplace_provider_offerings_customers_list"
    )
    async def test_get_offering_customers_error(self, mock_list, service):
        mock_list.asyncio_all = AsyncMock(side_effect=Exception("API Error"))

        with pytest.raises(WaldurClientError):
            await service.get_offering_customers("offering-uuid")

    @pytest.mark.asyncio
    @patch("waldur_cscs_hpc_storage.services.waldur_service.marketplace_resources_list")
    async def test_list_resources_with_slug_list(self, mock_list, service):
        mock_response = Mock()
        mock_response.parsed = []
        mock_response.headers = {}
        mock_list.asyncio_detailed = AsyncMock(return_value=mock_response)

        await service.list_resources(offering_slug=["slug1", "slug2"])

        mock_list.asyncio_detailed.assert_called_once_with(
            client=service.client,
            offering_slug=["slug1,slug2"],
            visible_to_providers=True,
            page=1,
            page_size=100,
        )
