"""Custom exceptions for the application."""

from typing import Optional


class StorageProxyError(Exception):
    """Base class for all application exceptions."""

    def __init__(self, message: str, original_error: Optional[Exception] = None):
        super().__init__(message)
        self.original_error = original_error


class UpstreamServiceError(StorageProxyError):
    """Base for external API failures."""


class WaldurClientError(UpstreamServiceError):
    """Waldur API returned an error or timed out."""


class HpcUserApiClientError(UpstreamServiceError):
    """HPC User/GID API returned an error or timed out."""


class ResourceProcessingError(StorageProxyError):
    """Base for failures while mapping/transforming a specific resource."""


class MissingIdentityError(ResourceProcessingError):
    """Required UID/GID could not be found for a resource."""

    def __init__(self, resource_uuid: str, identity_key: str):
        super().__init__(
            f"Could not resolve identity '{identity_key}' for resource {resource_uuid}"
        )
        self.resource_uuid = resource_uuid


class ConfigurationError(StorageProxyError):
    """Critical startup configuration missing or invalid."""
