"""Tests for CSCS HPC User API client."""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, Mock, patch

import httpx
import pytest

from waldur_cscs_hpc_storage.services.gid_service import GidService
from waldur_cscs_hpc_storage.config import HpcUserApiConfig
from waldur_cscs_hpc_storage.exceptions import ConfigurationError, HpcUserApiClientError


class TestGidService:
    """Test cases for GidService."""

    @pytest.fixture
    def client_config(self):
        """Basic client configuration."""
        return HpcUserApiConfig(
            api_url="https://api-user.hpc-user.example.com",
            client_id="test_client",
            client_secret="test_secret",
            oidc_token_url="https://auth.example.com/token",
            oidc_scope="openid profile",
        )

    @pytest.fixture
    def gid_service(self, client_config):
        """Create GidService instance."""
        return GidService(client_config)

    def test_init(self, client_config):
        """Test client initialization."""
        client = GidService(client_config)

        assert client.api_url == "https://api-user.hpc-user.example.com"
        assert client.client_id == "test_client"
        assert client.client_secret == "test_secret"
        assert client.oidc_token_url == "https://auth.example.com/token"
        assert client.oidc_scope == "openid profile"
        assert client._token is None
        assert client._token_expires_at is None

    def test_init_strips_trailing_slash(self):
        """Test that API URL trailing slashes are stripped."""
        config = HpcUserApiConfig(
            api_url="https://api-user.hpc-user.example.com/",
            client_id="test_client",
            client_secret="test_secret",
            oidc_token_url="https://auth.example.com/token",
            oidc_scope="openid profile",
        )
        client = GidService(config)
        assert client.api_url == "https://api-user.hpc-user.example.com"

    def test_init_default_scope(self):
        """Test default OIDC scope is set."""
        config = HpcUserApiConfig(
            api_url="https://api-user.hpc-user.example.com",
            client_id="test_client",
            client_secret="test_secret",
            oidc_token_url="https://auth.example.com/token",
            # oidc_scope missing (None)
        )
        client = GidService(config)
        assert client.oidc_scope == "openid"

    @pytest.mark.asyncio
    @patch("waldur_cscs_hpc_storage.services.gid_service.httpx.AsyncClient")
    async def test_acquire_oidc_token_success(self, mock_client_class, gid_service):
        """Test successful OIDC token acquisition."""
        # Mock HTTP response
        mock_response = Mock()
        mock_response.json.return_value = {
            "access_token": "test_access_token",
            "token_type": "Bearer",
            "expires_in": 3600,
        }
        mock_response.raise_for_status.return_value = None

        mock_client_instance = AsyncMock()
        mock_client_instance.post.return_value = mock_response
        mock_client_class.return_value.__aenter__.return_value = mock_client_instance

        # Test token acquisition
        token = await gid_service._acquire_oidc_token()

        assert token == "test_access_token"
        assert gid_service._token == "test_access_token"
        assert gid_service._token_expires_at is not None

        # Verify HTTP request
        mock_client_instance.post.assert_called_once_with(
            "https://auth.example.com/token",
            data={
                "grant_type": "client_credentials",
                "client_id": "test_client",
                "client_secret": "test_secret",
                "scope": "openid profile",
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )

    @pytest.mark.asyncio
    @patch("waldur_cscs_hpc_storage.services.gid_service.httpx.AsyncClient")
    async def test_acquire_oidc_token_no_access_token(
        self, mock_client_class, gid_service
    ):
        """Test OIDC token acquisition when no access_token in response."""
        mock_response = Mock()
        mock_response.json.return_value = {"token_type": "Bearer"}
        mock_response.raise_for_status.return_value = None

        mock_client_instance = AsyncMock()
        mock_client_instance.post.return_value = mock_response
        mock_client_class.return_value.__aenter__.return_value = mock_client_instance

        with pytest.raises(
            HpcUserApiClientError, match="No access_token in OIDC response"
        ):
            await gid_service._acquire_oidc_token()

    @pytest.mark.asyncio
    @patch("waldur_cscs_hpc_storage.services.gid_service.httpx.AsyncClient")
    async def test_acquire_oidc_token_http_error(self, mock_client_class, gid_service):
        """Test OIDC token acquisition HTTP error handling."""
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "401 Unauthorized", request=Mock(), response=Mock()
        )

        mock_client_instance = AsyncMock()
        mock_client_instance.post.return_value = mock_response
        mock_client_class.return_value.__aenter__.return_value = mock_client_instance

        with pytest.raises(HpcUserApiClientError):
            await gid_service._acquire_oidc_token()

    @pytest.mark.asyncio
    async def test_get_auth_token_no_oidc_url(self):
        """Test auth token acquisition when OIDC URL not configured."""
        config = HpcUserApiConfig(
            api_url="https://api-user.hpc-user.example.com",
            client_id="test_client",
            client_secret="test_secret",
            # oidc_token_url missing (None)
        )
        client = GidService(config)

        with pytest.raises(
            ConfigurationError, match="hpc_user_oidc_token_url not configured"
        ):
            await client._get_auth_token()

    @pytest.mark.asyncio
    async def test_get_auth_token_cached_valid(self, gid_service):
        """Test auth token returns cached token when still valid."""
        from datetime import timedelta

        # Set up cached token that expires in the future
        future_time = datetime.now(tz=timezone.utc) + timedelta(hours=1)
        gid_service._token = "cached_token"
        gid_service._token_expires_at = future_time

        token = await gid_service._get_auth_token()
        assert token == "cached_token"

    @pytest.mark.asyncio
    @patch("waldur_cscs_hpc_storage.services.gid_service.httpx.AsyncClient")
    async def test_get_projects_success(self, mock_client_class, gid_service):
        """Test successful project data retrieval."""
        from datetime import timedelta

        # Mock token acquisition
        future_time = datetime.now(tz=timezone.utc) + timedelta(hours=1)
        gid_service._token = "test_token"
        gid_service._token_expires_at = future_time

        # Mock API response
        mock_response = Mock()
        mock_response.json.return_value = {
            "projects": [
                {
                    "posixName": "project1",
                    "unixGid": 30001,
                    "displayName": "Test Project 1",
                },
                {
                    "posixName": "project2",
                    "unixGid": 30002,
                    "displayName": "Test Project 2",
                },
            ]
        }
        mock_response.raise_for_status.return_value = None

        mock_client_instance = AsyncMock()
        mock_client_instance.get.return_value = mock_response
        mock_client_class.return_value.__aenter__.return_value = mock_client_instance

        # Test projects retrieval
        result = await gid_service.get_projects(["project1", "project2"])

        assert len(result) == 2
        assert result[0]["posixName"] == "project1"
        assert result[0]["unixGid"] == 30001

        # Verify HTTP request
        mock_client_instance.get.assert_called_once_with(
            "https://api-user.hpc-user.example.com/api/v1/export/waldur/projects",
            params={"projects": ["project1", "project2"]},
            headers={"Authorization": "Bearer test_token"},
        )

    @pytest.mark.asyncio
    @patch("waldur_cscs_hpc_storage.services.gid_service.httpx.AsyncClient")
    async def test_get_projects_empty_list(self, mock_client_class, gid_service):
        """Test project retrieval with empty project list."""
        from datetime import timedelta

        # Mock token acquisition
        future_time = datetime.now(tz=timezone.utc) + timedelta(hours=1)
        gid_service._token = "test_token"
        gid_service._token_expires_at = future_time

        # Mock API response
        mock_response = Mock()
        mock_response.json.return_value = {"projects": []}
        mock_response.raise_for_status.return_value = None

        mock_client_instance = AsyncMock()
        mock_client_instance.get.return_value = mock_response
        mock_client_class.return_value.__aenter__.return_value = mock_client_instance

        result = await gid_service.get_projects([])

        assert result == []

        # Verify no projects parameter when empty list
        mock_client_instance.get.assert_called_once_with(
            "https://api-user.hpc-user.example.com/api/v1/export/waldur/projects",
            params={},
            headers={"Authorization": "Bearer test_token"},
        )

    @pytest.mark.asyncio
    @patch("waldur_cscs_hpc_storage.services.gid_service.httpx.AsyncClient")
    async def test_get_project_unix_gid_found(self, mock_client_class, gid_service):
        """Test successful unixGid lookup for existing project."""
        from datetime import timedelta

        # Mock token acquisition
        future_time = datetime.now(tz=timezone.utc) + timedelta(hours=1)
        gid_service._token = "test_token"
        gid_service._token_expires_at = future_time

        # Mock API response
        mock_response = Mock()
        mock_response.json.return_value = {
            "projects": [
                {
                    "posixName": "project1",
                    "unixGid": 30001,
                    "displayName": "Test Project 1",
                }
            ]
        }
        mock_response.raise_for_status.return_value = None

        mock_client_instance = AsyncMock()
        mock_client_instance.get.return_value = mock_response
        mock_client_class.return_value.__aenter__.return_value = mock_client_instance

        result = await gid_service.get_project_unix_gid("project1")

        assert result == 30001

        # Verify it cached the result
        assert gid_service._gid_cache["project1"] == 30001

    @pytest.mark.asyncio
    @patch("waldur_cscs_hpc_storage.services.gid_service.httpx.AsyncClient")
    async def test_get_project_unix_gid_uses_cache(
        self, mock_client_class, gid_service
    ):
        """Test that get_project_unix_gid uses cached value if available."""
        # Pre-populate cache
        gid_service._gid_cache["cached_project"] = 12345

        # Call method
        result = await gid_service.get_project_unix_gid("cached_project")

        # Should return cached value
        assert result == 12345

        # Should NOT make any API calls
        mock_client_class.assert_not_called()

    @pytest.mark.asyncio
    @patch("waldur_cscs_hpc_storage.services.gid_service.httpx.AsyncClient")
    async def test_get_project_unix_gid_not_found(self, mock_client_class, gid_service):
        """Test unixGid lookup for non-existent project."""
        from datetime import timedelta

        # Mock token acquisition
        future_time = datetime.now(tz=timezone.utc) + timedelta(hours=1)
        gid_service._token = "test_token"
        gid_service._token_expires_at = future_time

        # Mock API response with different project
        mock_response = Mock()
        mock_response.json.return_value = {
            "projects": [
                {
                    "posixName": "other_project",
                    "unixGid": 30099,
                    "displayName": "Other Project",
                }
            ]
        }
        mock_response.raise_for_status.return_value = None

        mock_client_instance = AsyncMock()
        mock_client_instance.get.return_value = mock_response
        mock_client_class.return_value.__aenter__.return_value = mock_client_instance

        result = await gid_service.get_project_unix_gid("project1")

        assert result is None

    @pytest.mark.asyncio
    @patch("waldur_cscs_hpc_storage.services.gid_service.httpx.AsyncClient")
    async def test_get_project_unix_gid_api_error(self, mock_client_class, gid_service):
        """Test unixGid lookup when API request fails."""
        from datetime import timedelta

        # Mock token acquisition
        future_time = datetime.now(tz=timezone.utc) + timedelta(hours=1)
        gid_service._token = "test_token"
        gid_service._token_expires_at = future_time

        mock_response = Mock()
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "500 Server Error", request=Mock(), response=Mock()
        )

        mock_client_instance = AsyncMock()
        mock_client_instance.get.return_value = mock_response
        mock_client_class.return_value.__aenter__.return_value = mock_client_instance

        with pytest.raises(HpcUserApiClientError):
            await gid_service.get_project_unix_gid("project1")

    @pytest.mark.asyncio
    @patch("waldur_cscs_hpc_storage.services.gid_service.httpx.AsyncClient")
    async def test_ping_success(self, mock_client_class, gid_service):
        """Test successful ping to HPC User API."""
        from datetime import timedelta

        # Mock token acquisition
        future_time = datetime.now(tz=timezone.utc) + timedelta(hours=1)
        gid_service._token = "test_token"
        gid_service._token_expires_at = future_time

        # Mock API response
        mock_response = Mock()
        mock_response.status_code = 200

        mock_client_instance = AsyncMock()
        mock_client_instance.get.return_value = mock_response
        mock_client_class.return_value.__aenter__.return_value = mock_client_instance

        result = await gid_service.ping()

        assert result is True

        # Verify ping request
        mock_client_instance.get.assert_called_once_with(
            "https://api-user.hpc-user.example.com/api/v1/export/waldur/projects",
            headers={"Authorization": "Bearer test_token"},
        )

    @pytest.mark.asyncio
    @patch("waldur_cscs_hpc_storage.services.gid_service.httpx.AsyncClient")
    async def test_ping_failure(self, mock_client_class, gid_service):
        """Test ping failure when API is not accessible."""
        from datetime import timedelta

        # Mock token acquisition
        future_time = datetime.now(tz=timezone.utc) + timedelta(hours=1)
        gid_service._token = "test_token"
        gid_service._token_expires_at = future_time

        # Mock API response with non-200 status
        mock_response = Mock()
        mock_response.status_code = 503

        mock_client_instance = AsyncMock()
        mock_client_instance.get.return_value = mock_response
        mock_client_class.return_value.__aenter__.return_value = mock_client_instance

        result = await gid_service.ping()

        assert result is False

    @pytest.mark.asyncio
    async def test_ping_exception(self, gid_service):
        """Test ping handles exceptions gracefully."""
        # Don't mock anything to trigger exception in _get_auth_token
        result = await gid_service.ping()

        assert result is False

    @pytest.mark.asyncio
    @patch("waldur_cscs_hpc_storage.services.gid_service.httpx.AsyncClient")
    async def test_batch_resolve_gids_success(self, mock_client_class, gid_service):
        """Test batch GID resolution fetches all slugs in one call."""
        from datetime import timedelta

        future_time = datetime.now(tz=timezone.utc) + timedelta(hours=1)
        gid_service._token = "test_token"
        gid_service._token_expires_at = future_time

        mock_response = Mock()
        mock_response.json.return_value = {
            "projects": [
                {"posixName": "proj1", "unixGid": 1001},
                {"posixName": "proj2", "unixGid": 1002},
                {"posixName": "proj3", "unixGid": 1003},
            ]
        }
        mock_response.raise_for_status.return_value = None

        mock_client_instance = AsyncMock()
        mock_client_instance.get.return_value = mock_response
        mock_client_class.return_value.__aenter__.return_value = mock_client_instance

        result = await gid_service.batch_resolve_gids(["proj1", "proj2", "proj3"])

        assert result == {"proj1": 1001, "proj2": 1002, "proj3": 1003}
        # Single API call for all 3 projects
        assert mock_client_instance.get.call_count == 1

    @pytest.mark.asyncio
    @patch("waldur_cscs_hpc_storage.services.gid_service.httpx.AsyncClient")
    async def test_batch_resolve_gids_skips_cached(
        self, mock_client_class, gid_service
    ):
        """Test batch GID resolution skips already-cached slugs."""
        from datetime import timedelta

        future_time = datetime.now(tz=timezone.utc) + timedelta(hours=1)
        gid_service._token = "test_token"
        gid_service._token_expires_at = future_time

        # Pre-populate cache
        gid_service._gid_cache["proj1"] = 1001

        mock_response = Mock()
        mock_response.json.return_value = {
            "projects": [{"posixName": "proj2", "unixGid": 1002}]
        }
        mock_response.raise_for_status.return_value = None

        mock_client_instance = AsyncMock()
        mock_client_instance.get.return_value = mock_response
        mock_client_class.return_value.__aenter__.return_value = mock_client_instance

        result = await gid_service.batch_resolve_gids(["proj1", "proj2"])

        assert result == {"proj1": 1001, "proj2": 1002}
        # Only fetched proj2 (proj1 was cached)
        call_args = mock_client_instance.get.call_args
        assert call_args[1]["params"]["projects"] == ["proj2"]

    @pytest.mark.asyncio
    async def test_batch_resolve_gids_all_cached(self, gid_service):
        """Test batch GID resolution returns immediately when all slugs are cached."""
        gid_service._gid_cache["proj1"] = 1001
        gid_service._gid_cache["proj2"] = 1002

        result = await gid_service.batch_resolve_gids(["proj1", "proj2"])

        assert result == {"proj1": 1001, "proj2": 1002}

    @pytest.mark.asyncio
    @patch("waldur_cscs_hpc_storage.services.gid_service.httpx.AsyncClient")
    async def test_batch_resolve_gids_populates_cache_for_individual_lookups(
        self, mock_client_class, gid_service
    ):
        """Test that batch resolution populates cache used by get_project_unix_gid."""
        from datetime import timedelta

        future_time = datetime.now(tz=timezone.utc) + timedelta(hours=1)
        gid_service._token = "test_token"
        gid_service._token_expires_at = future_time

        mock_response = Mock()
        mock_response.json.return_value = {
            "projects": [
                {"posixName": "proj1", "unixGid": 1001},
                {"posixName": "proj2", "unixGid": 1002},
            ]
        }
        mock_response.raise_for_status.return_value = None

        mock_client_instance = AsyncMock()
        mock_client_instance.get.return_value = mock_response
        mock_client_class.return_value.__aenter__.return_value = mock_client_instance

        await gid_service.batch_resolve_gids(["proj1", "proj2"])

        # Now individual lookups should use cache (no API calls)
        mock_client_instance.get.reset_mock()
        result = await gid_service.get_project_unix_gid("proj1")
        assert result == 1001
        # No additional API call was made
        mock_client_instance.get.assert_not_called()

    @pytest.mark.asyncio
    @patch("waldur_cscs_hpc_storage.services.gid_service.httpx.AsyncClient")
    async def test_batch_resolve_gids_handles_api_error_gracefully(
        self, mock_client_class, gid_service
    ):
        """Test batch resolution falls back gracefully on API errors."""
        from datetime import timedelta

        future_time = datetime.now(tz=timezone.utc) + timedelta(hours=1)
        gid_service._token = "test_token"
        gid_service._token_expires_at = future_time

        mock_response = Mock()
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "500 Server Error", request=Mock(), response=Mock()
        )

        mock_client_instance = AsyncMock()
        mock_client_instance.get.return_value = mock_response
        mock_client_class.return_value.__aenter__.return_value = mock_client_instance

        # Should not raise, returns empty dict
        result = await gid_service.batch_resolve_gids(["proj1", "proj2"])
        assert result == {}
