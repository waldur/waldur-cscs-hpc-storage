"""Mock GID service for development/testing without HPC User API."""

import logging
from typing import Optional

logger = logging.getLogger(__name__)


class MockGidService:
    """Mock GID service for development/testing without HPC User API.

    This service is used when no HPC User API is configured. In development
    mode, it returns deterministic mock GIDs. In production mode, it returns
    None to indicate that resources should be skipped.
    """

    def __init__(self, development_mode: bool = True) -> None:
        """Initialize MockGidService.

        Args:
            development_mode: If True, return mock GIDs. If False, return None.
        """
        self.development_mode = development_mode
        self._gid_cache: dict[str, int] = {}
        logger.info(
            "MockGidService initialized (dev_mode: %s)",
            self.development_mode,
        )

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

        In development mode, returns a deterministic mock GID.
        In production mode, returns None (resource should be skipped).

        Args:
            project_slug: Project slug to look up

        Returns:
            Mock GID in development mode, None in production mode
        """
        if not self.development_mode:
            logger.error(
                "MockGidService: No HPC User API configured for project %s, "
                "skipping resource (production mode)",
                project_slug,
            )
            return None

        # Check cache first
        if project_slug in self._gid_cache:
            logger.debug(
                "Found cached mock unixGid %d for project %s",
                self._gid_cache[project_slug],
                project_slug,
            )
            return self._gid_cache[project_slug]

        # Generate and cache mock GID
        mock_gid = self._generate_mock_gid(project_slug)
        self._gid_cache[project_slug] = mock_gid
        logger.debug(
            "Using mock unixGid %d for project %s (dev mode)",
            mock_gid,
            project_slug,
        )
        return mock_gid

    async def ping(self) -> bool:
        """Check if service is available.

        MockGidService is always "available" since it doesn't depend on external API.

        Returns:
            Always True
        """
        return True
