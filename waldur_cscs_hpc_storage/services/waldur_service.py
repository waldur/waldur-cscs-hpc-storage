import logging
from dataclasses import dataclass
from typing import Optional, Union

from waldur_api_client import AuthenticatedClient
from waldur_api_client.api.marketplace_provider_offerings import (
    marketplace_provider_offerings_customers_list,
)
from waldur_api_client.api.marketplace_resources import marketplace_resources_list
from waldur_api_client.models.resource_state import ResourceState

from waldur_cscs_hpc_storage.base.schemas import ParsedWaldurResource
from waldur_cscs_hpc_storage.config import WaldurApiConfig
from waldur_cscs_hpc_storage.exceptions import WaldurClientError
from waldur_cscs_hpc_storage.hierarchy_builder import CustomerInfo

logger = logging.getLogger(__name__)


@dataclass
class WaldurResourceResponse:
    resources: list[ParsedWaldurResource]
    total_count: int


class WaldurService:
    def __init__(self, waldur_api_config: WaldurApiConfig):
        headers = (
            {"User-Agent": waldur_api_config.agent_header}
            if waldur_api_config.agent_header
            else {}
        )
        url = waldur_api_config.api_url.rstrip("/api")

        # Configure httpx args with proxy if specified
        httpx_args = {}
        if waldur_api_config.socks_proxy:
            httpx_args["proxy"] = waldur_api_config.socks_proxy

        self.client = AuthenticatedClient(
            base_url=url,
            token=waldur_api_config.access_token,
            timeout=600,
            headers=headers,
            verify_ssl=waldur_api_config.verify_ssl,
            httpx_args=httpx_args,
        )
        logger.debug(
            "Waldur API client initialized for URL: %s", waldur_api_config.api_url
        )

    async def get_offering_customers(
        self, offering_uuid: str
    ) -> dict[str, CustomerInfo]:
        """Get customers for a specific offering.

        Args:
            offering_uuid: UUID of the offering

        Returns:
            Dictionary mapping customer slugs to CustomerInfo objects
        """
        try:
            response = await marketplace_provider_offerings_customers_list.asyncio_all(
                uuid=offering_uuid, client=self.client
            )
        except Exception as e:
            msg = f"Failed to fetch customers for offering {offering_uuid}"
            logger.exception(msg)
            raise WaldurClientError(msg, original_error=e) from e

        if not response:
            logger.warning("No customers found for offering %s", offering_uuid)
            return {}

        customers = {}
        for customer in response:
            customers[customer.slug] = CustomerInfo(
                itemId=customer.uuid.hex,
                key=customer.slug,
                name=customer.name,
            )

        logger.debug(
            "Found %d customers for offering %s", len(customers), offering_uuid
        )
        return customers

    async def list_resources(
        self,
        offering_uuid: Optional[str] = None,
        offering_slug: Optional[Union[str, list[str]]] = None,
        state: Optional[ResourceState] = None,
        page: int = 1,
        page_size: int = 100,
        exclude_pending: bool = False,
        **kwargs,
    ):
        """Fetch resources from Waldur API.

        Args:
            offering_uuid: Optional UUID of the offering
            offering_slug: Optional slug or list of slugs of the offering
            state: Optional resource state filter
            page: Page number
            page_size: Page size
            exclude_pending: Whether to exclude pending transitional resources
            **kwargs: Additional filters

        Returns:
            WaldurResourceResponse object containing parsed resources and pagination info
        """
        filters = {}
        if state:
            filters["state"] = [state]

        if offering_uuid:
            # The API client expects a list for offering_uuid
            filters["offering_uuid"] = [offering_uuid]

        if offering_slug:
            if isinstance(offering_slug, list):
                filters["offering_slug"] = ",".join(offering_slug)
            else:
                filters["offering_slug"] = offering_slug

        if exclude_pending:
            filters["exclude_pending_transitional"] = True

        filters.update(kwargs)

        response = await marketplace_resources_list.asyncio_detailed(
            client=self.client,
            page=page,
            page_size=page_size,
            **filters,
        )

        parsed_resources = [
            ParsedWaldurResource.from_waldur_resource(r) for r in response.parsed
        ]
        total = int(response.headers.get("x-result-count", 0))

        return WaldurResourceResponse(resources=parsed_resources, total_count=total)
