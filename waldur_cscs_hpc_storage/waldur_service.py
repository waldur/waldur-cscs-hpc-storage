import logging
from typing import Any, Optional, Union

from waldur_api_client.api.marketplace_provider_offerings import (
    marketplace_provider_offerings_customers_list,
)
from waldur_api_client.api.marketplace_resources import marketplace_resources_list
from waldur_api_client.models.resource_state import ResourceState
from waldur_cscs_hpc_storage.utils import get_client
from waldur_cscs_hpc_storage.waldur_storage_proxy.config import WaldurApiConfig

logger = logging.getLogger(__name__)


class WaldurService:
    def __init__(self, waldur_api_config: WaldurApiConfig):
        self.client = get_client(waldur_api_config)
        logger.debug(
            "Waldur API client initialized for URL: %s", waldur_api_config.api_url
        )

    def get_offering_customers(self, offering_uuid: str) -> dict[str, dict[str, Any]]:
        """Get customers for a specific offering.

        Args:
            offering_uuid: UUID of the offering

        Returns:
            Dictionary mapping customer slugs to customer information
        """
        try:
            response = marketplace_provider_offerings_customers_list.sync_all(
                uuid=offering_uuid, client=self.client
            )

            if not response.parsed:
                logger.warning("No customers found for offering %s", offering_uuid)
                return {}

            customers = {}
            for customer in response.parsed:
                customers[customer.slug] = {
                    "itemId": customer.uuid.hex,
                    "key": customer.slug,
                    "name": customer.name,
                    "uuid": customer.uuid.hex,
                }

            logger.debug(
                "Found %d customers for offering %s", len(customers), offering_uuid
            )
            return customers

        except Exception as e:
            logger.error(
                "Failed to fetch customers for offering %s: %s", offering_uuid, e
            )
            return {}

    def list_resources(
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
            Response object from the API client
        """
        filters = {}
        if state:
            filters["state"] = state

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

        return marketplace_resources_list.sync_detailed(
            client=self.client,
            page=page,
            page_size=page_size,
            **filters,
        )
