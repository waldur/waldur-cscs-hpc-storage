"""Tests for configuration logging and environment variable loading."""

import os
from unittest import mock
import pytest
from waldur_cscs_hpc_storage.api.config_parser import load_config, mask_sensitive_data
from waldur_cscs_hpc_storage.config import (
    StorageProxyConfig,
    WaldurApiConfig,
)
from waldur_cscs_hpc_storage.base.enums import StorageSystem


def test_mask_sensitive_data():
    """Test that sensitive fields are masked correctly."""
    data = {
        "debug": True,
        "waldur_api": {
            "api_url": "http://example.com",
            "access_token": "secret-token",  # target
        },
        "auth": {
            "keycloak_client_secret": "secret-client-secret",  # target
            "other": "value",
        },
        "list_data": [
            {"client_secret": "nested-secret"},  # target
            {"safe": "value"},
        ],
        "safe_field": "safe",
    }

    masked = mask_sensitive_data(data)

    assert masked["debug"] is True
    assert masked["waldur_api"]["api_url"] == "http://example.com"
    assert masked["waldur_api"]["access_token"] == "********"
    assert masked["auth"]["keycloak_client_secret"] == "********"
    assert masked["auth"]["other"] == "value"
    assert masked["list_data"][0]["client_secret"] == "********"
    assert masked["list_data"][1]["safe"] == "value"
    assert masked["safe_field"] == "safe"


@pytest.fixture(autouse=True)
def clean_env():
    """Ensure no config file is loaded from env during tests."""
    old_path = os.environ.pop("WALDUR_CSCS_STORAGE_PROXY_CONFIG_PATH", None)
    yield
    if old_path:
        os.environ["WALDUR_CSCS_STORAGE_PROXY_CONFIG_PATH"] = old_path


@mock.patch("waldur_cscs_hpc_storage.api.config_parser.logging.basicConfig")
@mock.patch("waldur_cscs_hpc_storage.api.config_parser.logger")
def test_load_config_logs_masked_data(mock_logger, mock_basic_config):
    """Test that load_config logs the configuration with masked data."""
    # Setup minimal valid environment
    env = {
        "WALDUR_API_URL": "http://test.com",
        "WALDUR_API_TOKEN": "44444444444444444444444444444444",
        "STORAGE_SYSTEMS": '{"capstor": "lustre"}',
        "DEBUG": "true",
        "CSCS_KEYCLOAK_CLIENT_SECRET": "s3cr3t",
        "HPC_USER_DEVELOPMENT_MODE": "true",
    }

    with mock.patch.dict(os.environ, env):
        config = load_config()

    # Check if config was loaded correctly
    assert config.waldur_api.access_token == "44444444444444444444444444444444"
    assert config.auth.keycloak_client_secret == "s3cr3t"

    # Verify storage systems loaded correctly
    # Note: keys in dictionary are now Enum members, but input JSON keys are strings.
    # Pydantic should convert them if possible.
    assert config.storage_systems[StorageSystem.CAPSTOR] == "lustre"

    # Verify logger usage
    # We expect an INFO log with "Merged configuration"
    # and the formatted dict.
    assert mock_logger.info.call_count >= 2

    # Iterate through calls to find the one logging the merged config
    log_call = None
    for call in mock_logger.info.call_args_list:
        if "Merged configuration" in str(call):
            log_call = call
            break

    assert log_call is not None
    args, _ = log_call
    # args[0] is format string, args[1] is the value
    logged_config_str = args[1]

    # Check that secrets are NOT present in the log string
    assert "real-token" not in logged_config_str
    assert "s3cr3t" not in logged_config_str
    assert "********" in logged_config_str


def test_env_loading_flat_fields():
    """Test loading flat fields in nested config from environment variables."""
    env = {
        "WALDUR_API_URL": "http://env-url.com",
        "WALDUR_API_TOKEN": "55555555555555555555555555555555",
    }

    with mock.patch.dict(os.environ, env):
        # We can instantiate the nested config directly to test
        config = WaldurApiConfig()
        assert str(config.api_url) == "http://env-url.com/"
        assert config.access_token == "55555555555555555555555555555555"


def test_env_loading_nested_in_main_config():
    """Test that top-level StorageProxyConfig picks up env vars for nested fields."""
    env = {
        # WaldurApiConfig fields
        "WALDUR_API_URL": "http://main-env.com",
        "WALDUR_API_TOKEN": "66666666666666666666666666666666",
        # AuthConfig fields
        "CSCS_KEYCLOAK_CLIENT_ID": "env-client-id",
        "DISABLE_AUTH": "true",
        # StorageProxyConfig field
        "STORAGE_SYSTEMS": '{"capstor": "lustre"}',
        "DEBUG": "true",
        "HPC_USER_DEVELOPMENT_MODE": "true",
    }

    with mock.patch.dict(os.environ, env):
        config = StorageProxyConfig()

        # Verify WaldurApiConfig via main config
        assert str(config.waldur_api.api_url) == "http://main-env.com/"
        assert config.waldur_api.access_token == "66666666666666666666666666666666"

        # Verify AuthConfig via main config
        assert config.auth.keycloak_client_id == "env-client-id"
        assert config.auth.disable_auth is True

        # Verify StorageProxyConfig direct fields
        assert config.debug is True
        assert config.storage_systems == {StorageSystem.CAPSTOR: "lustre"}
