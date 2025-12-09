"""Utility functions for CSCS HPC Storage backend."""

from pathlib import Path

import yaml


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
