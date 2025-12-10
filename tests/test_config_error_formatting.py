from unittest import mock
import pytest
from pydantic import ValidationError
from waldur_cscs_hpc_storage.api.config_parser import (
    _format_validation_error,
    load_config,
)
from waldur_cscs_hpc_storage.config import (
    StorageProxyConfig,
    WaldurApiConfig,
    StorageSystem,
)
import sys


def test_format_validation_error_basic():
    """Test formatting of a simple validation error."""
    # Create a ValidationError manually (easiest way is to instantiate a model with invalid data)
    with mock.patch.dict("os.environ", {}, clear=True):
        try:
            StorageProxyConfig(
                waldur_api={"api_url": "not-a-url"},
                storage_systems={},
                auth={"disable_auth": True},  # Disable auth to avoid unrelated errors
                hpc_user_api={"development_mode": True},  # Avoid unrelated errors
            )
        except ValidationError as e:
            formatted = _format_validation_error(e)
            assert "Configuration Error:" in formatted
            assert "waldur_api.api_url: Input should be a valid URL" in formatted
            assert (
                "storage_systems: Value error, At least one storage_system mapping is required"
                in formatted
            )


def test_format_validation_error_enum():
    """Test formatting of enum validation error."""
    with mock.patch.dict("os.environ", {}, clear=True):
        try:
            StorageProxyConfig(
                waldur_api={"api_url": "http://example.com", "access_token": "0" * 40},
                storage_systems={
                    "invalid_key": "lustre"
                },  # Should be StorageSystem enum
                auth={"disable_auth": True},
                hpc_user_api={"development_mode": True},
            )
        except ValidationError as e:
            formatted = _format_validation_error(e)
            assert "storage_systems.invalid_key" in formatted
            # Exact message depends on pydantic version but should indicate invalid enum


def test_load_config_prints_error_and_exits(capsys):
    """Test that load_config prints to stderr and exits on validation error."""
    # Mock environment to be empty/invalid to force validation error
    with mock.patch.dict("os.environ", {}, clear=True):
        with pytest.raises(SystemExit) as exc:
            load_config()

        assert exc.value.code == 1

    captured = capsys.readouterr()
    assert "Configuration Error:" in captured.err
    # Pydantic Settings uses aliases or field names depending on configuration
    assert "WALDUR_API_URL" in captured.err or "waldur_api" in captured.err
