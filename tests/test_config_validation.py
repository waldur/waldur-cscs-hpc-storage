"""Tests for configuration validation."""

import pytest
from pydantic import ValidationError

from waldur_cscs_hpc_storage.config import (
    BackendConfig,
    HpcUserApiConfig,
    SentryConfig,
    StorageProxyConfig,
    WaldurApiConfig,
)
from waldur_cscs_hpc_storage.base.enums import StorageSystem


@pytest.fixture(autouse=True)
def clean_env():
    """Ensure no config file is loaded from env during validation tests."""
    import os

    # Remove config path to prevent YamlConfigSettingsSource from loading the test config
    old_path = os.environ.pop("WALDUR_CSCS_STORAGE_PROXY_CONFIG_PATH", None)
    yield
    if old_path:
        os.environ["WALDUR_CSCS_STORAGE_PROXY_CONFIG_PATH"] = old_path


class TestBackendConfigValidation:
    """Test cases for BackendConfig validation."""

    def test_inode_coefficients_relationship(self):
        """Test that hard coefficient must be greater than or equal to soft coefficient."""
        # Valid case: hard > soft
        BackendConfig(inode_soft_coefficient=1.0, inode_hard_coefficient=2.0)

        # Valid case: hard == soft
        # The validator checks: if hard < soft -> raise
        BackendConfig(inode_soft_coefficient=1.5, inode_hard_coefficient=1.5)

        # Invalid case: hard < soft
        with pytest.raises(ValidationError) as excinfo:
            BackendConfig(inode_soft_coefficient=2.0, inode_hard_coefficient=1.5)
        assert "must be greater than inode_soft_coefficient" in str(excinfo.value)

    def test_numeric_constraints(self):
        """Test gt=0 constraints for numeric fields."""
        # inode_soft_coefficient
        with pytest.raises(ValidationError) as exc:
            BackendConfig(inode_soft_coefficient=0)
        assert "greater than 0" in str(exc.value)

        # inode_hard_coefficient
        with pytest.raises(ValidationError) as exc:
            BackendConfig(inode_hard_coefficient=-1.0)
        assert "greater than 0" in str(exc.value)

        # inode_base_multiplier
        with pytest.raises(ValidationError) as exc:
            BackendConfig(inode_base_multiplier=0)
        assert "greater than 0" in str(exc.value)

    def test_storage_file_system_non_empty(self):
        """Test valid non-empty string for storage file system."""
        with pytest.raises(ValidationError) as exc:
            BackendConfig(storage_file_system="")
        assert "String should have at least 1 character" in str(exc.value)


class TestWaldurApiConfigValidation:
    """Test cases for WaldurApiConfig validation."""

    def test_required_fields(self):
        """Test that api_url and access_token are strictly required."""
        with pytest.raises(ValidationError) as exc:
            WaldurApiConfig(api_url="http://example.com")  # missing access_token
        assert "Field required" in str(exc.value)
        assert "WALDUR_API_TOKEN" in str(exc.value)

        with pytest.raises(ValidationError) as exc:
            WaldurApiConfig(access_token="token")  # missing api_url
        assert "Field required" in str(exc.value)
        assert "WALDUR_API_URL" in str(exc.value)

    def test_token_format_validation(self):
        """Test that access_token must be a 40-char hex string."""
        # Correct length but invalid chars (Z is not hex)
        invalid_token = "Z" * 40
        with pytest.raises(ValidationError) as exc:
            WaldurApiConfig(
                api_url="http://example.com",
                access_token=invalid_token,
            )
        assert "String should match pattern" in str(exc.value)

        # Valid hex token
        valid_token = "a" * 40
        config = WaldurApiConfig(
            api_url="http://example.com",
            access_token=valid_token,
        )
        assert config.access_token == valid_token


class TestStorageProxyConfigValidation:
    """Test cases for StorageProxyConfig validation."""

    def test_storage_systems_non_empty(self):
        """Test that storage_systems map cannot be empty."""
        # We must provide valid waldur_api to reach the storage_systems validation
        valid_waldur_api = {
            "api_url": "http://u.com",
            # 40 chars hex
            "access_token": "a" * 40,
        }

        with pytest.raises(ValidationError) as exc:
            StorageProxyConfig(
                waldur_api=valid_waldur_api,
                storage_systems={},
                hpc_user_api=HpcUserApiConfig(development_mode=True),
            )
        assert "At least one storage_system mapping is required" in str(exc.value)

    def test_storage_systems_valid(self):
        """Test successful validation with at least one storage system."""
        valid_waldur_api = {
            "api_url": "http://u.com",
            "access_token": "a" * 40,
        }
        config = StorageProxyConfig(
            waldur_api=valid_waldur_api,
            storage_systems={StorageSystem.CAPSTOR: "slug"},
            hpc_user_api=HpcUserApiConfig(development_mode=True),
        )
        assert config.storage_systems == {StorageSystem.CAPSTOR: "slug"}


class TestSentryConfigValidation:
    """Test cases for SentryConfig validation."""

    def test_traces_sample_rate_numeric(self):
        """Test traces_sample_rate must be a float."""
        with pytest.raises(ValidationError) as exc:
            SentryConfig(traces_sample_rate="invalid-float")
        assert "Input should be a valid number" in str(exc.value)


class TestHpcUserApiConfigValidation:
    """Test cases for HpcUserApiConfig validation."""

    def test_prod_requirements_missing(self):
        """Test failure when prod requirements are missing in strict mode."""
        # By default development_mode is False
        with pytest.raises(ValidationError) as exc:
            HpcUserApiConfig()

        err_msg = str(exc.value)
        assert "Connection credentials" in err_msg
        assert "api_url" in err_msg
        assert "client_id" in err_msg
        assert "client_secret" in err_msg

    def test_prod_requirements_satisfied(self):
        """Test success when prod requirements are met."""
        config = HpcUserApiConfig(
            api_url="http://hpc.api",
            client_id="cid",
            client_secret="secret",
            development_mode=False,
        )
        assert str(config.api_url) == "http://hpc.api/"
        assert config.client_id == "cid"

    def test_dev_mode_relaxed(self):
        """Test that dev mode relaxes requirements."""
        # development_mode=True should allow missing credentials
        config = HpcUserApiConfig(development_mode=True)
        assert config.development_mode is True
        assert config.api_url is None


class TestSocksProxyValidation:
    """Test cases for socks_proxy validation."""

    def test_valid_proxy_urls(self):
        """Test valid proxy URLs."""
        valid_urls = [
            "socks5://localhost:1080",
            "socks5h://user:pass@proxy.example.com:1234",
            "http://proxy.com",
            "https://proxy.com",
            None,
        ]

        # Test HpcUserApiConfig
        for url in valid_urls:
            config = HpcUserApiConfig(development_mode=True, socks_proxy=url)
            assert config.socks_proxy == url

        # Test WaldurApiConfig
        valid_token = "a" * 40
        for url in valid_urls:
            config = WaldurApiConfig(
                api_url="http://example.com", access_token=valid_token, socks_proxy=url
            )
            assert config.socks_proxy == url

    def test_invalid_proxy_urls(self):
        """Test invalid proxy URLs."""
        invalid_urls = [
            "localhost:1080",
            "tcp://localhost:1080",
            "ftp://proxy.com",
            "random-string",
        ]

        # Test HpcUserApiConfig
        for url in invalid_urls:
            with pytest.raises(ValidationError) as exc:
                HpcUserApiConfig(development_mode=True, socks_proxy=url)
            assert "Proxy URL must start with one of" in str(exc.value)

        # Test WaldurApiConfig
        valid_token = "a" * 32
        for url in invalid_urls:
            with pytest.raises(ValidationError) as exc:
                WaldurApiConfig(
                    api_url="http://example.com",
                    access_token=valid_token,
                    socks_proxy=url,
                )
            assert "Proxy URL must start with one of" in str(exc.value)
