"""Utility functions for CSCS HPC Storage backend."""

from pathlib import Path
from typing import Optional

import yaml
from waldur_api_client import AuthenticatedClient


def get_client(
    api_url: str,
    access_token: str,
    agent_header: Optional[str] = None,
    verify_ssl: bool = True,
    proxy: Optional[str] = None,
) -> AuthenticatedClient:
    """Create an authenticated Waldur API client.

    Args:
        api_url: Base URL for the Waldur API (e.g., 'https://waldur.example.com/api/')
        access_token: Authentication token for API access
        agent_header: Optional User-Agent string for HTTP requests
        verify_ssl: Whether or not to verify SSL certificates
        proxy: Optional proxy URL (e.g., 'socks5://localhost:12345')

    Returns:
        Configured AuthenticatedClient instance ready for API calls
    """
    headers = {"User-Agent": agent_header} if agent_header else {}
    url = api_url.rstrip("/api")

    # Configure httpx args with proxy if specified
    httpx_args = {}
    if proxy:
        httpx_args["proxy"] = proxy

    return AuthenticatedClient(
        base_url=url,
        token=access_token,
        timeout=600,
        headers=headers,
        verify_ssl=verify_ssl,
        httpx_args=httpx_args,
    )


def load_configuration(
    config_file_path: str, user_agent_suffix: str = "cscs-storage"
) -> dict:
    """Load configuration from YAML file.

    Args:
        config_file_path: Path to the YAML configuration file
        user_agent_suffix: Suffix to add to the user agent string

    Returns:
        Configuration dictionary with offerings

    Raises:
        FileNotFoundError: If the configuration file cannot be found
        yaml.YAMLError: If the configuration file is malformed
    """
    with Path(config_file_path).open(encoding="UTF-8") as stream:
        config = yaml.safe_load(stream)

    # Add user agent
    config["waldur_user_agent"] = f"waldur-cscs-hpc-storage-{user_agent_suffix}/0.7.0"

    return config
