"""CSCS HPC User API client implementation."""

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import httpx

from waldur_cscs_hpc_storage.config import HpcUserApiConfig
from waldur_cscs_hpc_storage.exceptions import ConfigurationError, HpcUserApiClientError

logger = logging.getLogger(__name__)

HTTP_OK = 200


class GidService:
    """Client for interacting with CSCS HPC User API for project information."""

    def __init__(self, api_config: HpcUserApiConfig) -> None:
        """Initialize CSCS HPC User client.

        Args:
            api_config: Configuration object for HPC User API
        """
        if not api_config.api_url:
            raise ConfigurationError("api_url is required via HpcUserApiConfig")
        if not api_config.client_id:
            raise ConfigurationError("client_id is required via HpcUserApiConfig")
        if not api_config.client_secret:
            raise ConfigurationError("client_secret is required via HpcUserApiConfig")

        self.api_url = api_config.api_url.rstrip("/")
        self.client_id = api_config.client_id
        self.client_secret = api_config.client_secret
        self.oidc_token_url = api_config.oidc_token_url
        self.oidc_scope = api_config.oidc_scope or "openid"
        self.socks_proxy = api_config.socks_proxy
        self.development_mode = api_config.development_mode
        self._token: Optional[str] = None
        self._token_expires_at: Optional[datetime] = None
        self._gid_cache: dict[str, int] = {}

        if self.socks_proxy:
            logger.info(
                "SOCKS proxy configured from hpc_user_api settings: %s",
                self.socks_proxy,
            )
        logger.info(
            "HPC User client initialized with URL: %s (dev_mode: %s)",
            self.api_url,
            self.development_mode,
        )

    async def _get_auth_token(self) -> str:
        """Get or refresh OIDC authentication token.

        Returns:
            Valid authentication token

        Raises:
            httpx.HTTPError: If token acquisition fails
        """
        # Check if we have a valid cached token
        if (
            self._token
            and self._token_expires_at
            and datetime.now(tz=timezone.utc) < self._token_expires_at
        ):
            return self._token

        # Fail if OIDC endpoint not configured
        if not self.oidc_token_url:
            error_msg = (
                "OIDC authentication failed: hpc_user_oidc_token_url not configured. "
                "Set 'hpc_user_oidc_token_url' in backend_config for production use."
            )
            logger.error(error_msg)
            raise ConfigurationError(error_msg)

        # Request new token from OIDC provider
        return await self._acquire_oidc_token()

    async def _acquire_oidc_token(self) -> str:
        """Acquire a new OIDC token from the configured provider.

        Returns:
            Valid authentication token

        Raises:
            httpx.HTTPError: If token acquisition fails
        """
        logger.debug("Acquiring new OIDC token from %s", self.oidc_token_url)

        token_data = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }

        # Add scope if specified
        if self.oidc_scope:
            token_data["scope"] = self.oidc_scope

        headers = {"Content-Type": "application/x-www-form-urlencoded"}

        # Configure httpx client with SOCKS proxy if specified
        client_args: dict[str, Any] = {"timeout": 30.0}
        if self.socks_proxy:
            client_args["proxy"] = self.socks_proxy
            logger.debug(
                "Using SOCKS proxy for token acquisition: %s", self.socks_proxy
            )

        async with httpx.AsyncClient(**client_args) as client:
            try:
                response = await client.post(
                    self.oidc_token_url, data=token_data, headers=headers
                )
                response.raise_for_status()
                token_response = response.json()
            except httpx.HTTPError as e:
                msg = f"Failed to acquire OIDC token from {self.oidc_token_url}"
                logger.error(msg)
                raise HpcUserApiClientError(msg, original_error=e) from e

            # Extract token and expiry information
            access_token = token_response.get("access_token")
            if not access_token:
                msg = f"No access_token in OIDC response: {token_response}"
                raise HpcUserApiClientError(msg)

            # Calculate token expiry time
            expires_in = token_response.get("expires_in", 3600)  # Default to 1 hour
            # Subtract 5 minutes from expiry for safety margin
            safe_expires_in = max(300, expires_in - 300)

            self._token = access_token
            self._token_expires_at = datetime.now(tz=timezone.utc) + timedelta(
                seconds=safe_expires_in
            )

            logger.info(
                "Successfully acquired OIDC token, expires in %d seconds", expires_in
            )

            return access_token

    async def get_projects(self, project_slugs: list[str]) -> list[dict[str, Any]]:
        """Get project information for multiple project slugs.

        Args:
            project_slugs: List of project slugs to query

        Returns:
            List of project data dictionaries

        Raises:
            httpx.HTTPError: If API request fails
        """
        token = await self._get_auth_token()

        params: dict[str, Any] = {}

        # Add project filters if provided
        if project_slugs:
            params["projects"] = project_slugs

        headers = {"Authorization": f"Bearer {token}"}

        url = f"{self.api_url}/api/v1/export/waldur/projects"

        logger.debug("Fetching project information for slugs: %s", project_slugs)

        # Configure httpx client with SOCKS proxy if specified
        client_args: dict[str, Any] = {"timeout": 30.0}
        if self.socks_proxy:
            client_args["proxy"] = self.socks_proxy
            logger.debug("Using SOCKS proxy for API request: %s", self.socks_proxy)

        async with httpx.AsyncClient(**client_args) as client:
            try:
                response = await client.get(url, params=params, headers=headers)
                response.raise_for_status()
                return response.json()["projects"]
            except httpx.HTTPError as e:
                msg = f"Failed to fetch projects from {url}"
                logger.error(msg)
                raise HpcUserApiClientError(msg, original_error=e) from e

    def _generate_mock_gid(self, project_slug: str) -> int:
        """Generate a deterministic mock GID for development/testing.

        Args:
            project_slug: Project slug to generate mock GID for

        Returns:
            Mock GID value (30000 + hash-based offset)
        """
        return 30000 + hash(project_slug) % 10000

    async def get_project_unix_gid(self, project_slug: str) -> Optional[int]:
        """Get unixGid for a specific project slug.

        In development mode, returns a mock GID if the API lookup fails.
        In production mode, returns None if the lookup fails.

        Args:
            project_slug: Project slug to look up

        Returns:
            unixGid if found, mock value (dev mode), or None (prod mode on failure)
        """
        if project_slug in self._gid_cache:
            logger.debug(
                "Found cached unixGid %d for project %s",
                self._gid_cache[project_slug],
                project_slug,
            )
            return self._gid_cache[project_slug]

        try:
            projects_data = await self.get_projects([project_slug])
            if len(projects_data) > 1:
                logger.error("Multiple projects found for slug: %s", project_slug)
                return self._handle_lookup_failure(
                    project_slug, "multiple projects found"
                )
            if len(projects_data) == 0:
                logger.warning(
                    "Project %s not found in HPC User API response", project_slug
                )
                return self._handle_lookup_failure(project_slug, "project not found")
            project = projects_data[0]
            if project.get("posixName") == project_slug:
                unix_gid = project.get("unixGid")
                if unix_gid is not None:
                    self._gid_cache[project_slug] = unix_gid
                    return unix_gid

            logger.warning(
                "Project %s not found in HPC User API response", project_slug
            )
            return self._handle_lookup_failure(project_slug, "project not found")
        except HpcUserApiClientError:
            raise
        except Exception as e:
            logger.exception(
                "Unexpected error fetching unixGid for project %s", project_slug
            )
            raise HpcUserApiClientError(
                f"Unexpected error: {e}", original_error=e
            ) from e

    def _handle_lookup_failure(self, project_slug: str, reason: str) -> Optional[int]:
        """Handle GID lookup failure based on development mode.

        Args:
            project_slug: Project slug that failed lookup
            reason: Reason for the failure

        Returns:
            Mock GID in development mode, None in production mode
        """
        if self.development_mode:
            mock_gid = self._generate_mock_gid(project_slug)
            logger.warning(
                "Using mock unixGid %d for project %s (%s, dev mode)",
                mock_gid,
                project_slug,
                reason,
            )
            return mock_gid
        else:
            logger.error(
                "Project %s lookup failed (%s), skipping resource (production mode)",
                project_slug,
                reason,
            )
            return None

    async def ping(self) -> bool:
        """Check if CSCS HPC User API is accessible.

        Returns:
            True if API is accessible, False otherwise
        """
        try:
            token = await self._get_auth_token()
            headers = {"Authorization": f"Bearer {token}"}

            url = f"{self.api_url}/api/v1/export/waldur/projects"

            # Configure httpx client with SOCKS proxy if specified
            client_args: dict[str, Any] = {"timeout": 10.0}
            if self.socks_proxy:
                client_args["proxy"] = self.socks_proxy
                logger.debug("Using SOCKS proxy for ping: %s", self.socks_proxy)

            # Test with a simple request (no project filters)
            async with httpx.AsyncClient(**client_args) as client:
                response = await client.get(url, headers=headers)
                return response.status_code == HTTP_OK
        except Exception:
            logger.exception("HPC User API ping failed")
            return False
