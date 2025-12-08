"""Utility functions for CSCS HPC Storage backend."""

from pathlib import Path

import yaml
from waldur_api_client import AuthenticatedClient


from waldur_cscs_hpc_storage.waldur_storage_proxy.config import WaldurApiConfig


def get_client(waldur_api_config: WaldurApiConfig) -> AuthenticatedClient:
    """Create an authenticated Waldur API client.

    Args:
        waldur_api_config: Waldur API configuration object

    Returns:
        Configured AuthenticatedClient instance ready for API calls
    """
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

    return AuthenticatedClient(
        base_url=url,
        token=waldur_api_config.access_token,
        timeout=600,
        headers=headers,
        verify_ssl=waldur_api_config.verify_ssl,
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
