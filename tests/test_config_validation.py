"""Tests for configuration validation."""

import pytest
from waldur_cscs_hpc_storage.config import BackendConfig


class TestBackendConfigValidation:
    """Test cases for BackendConfig validation."""

    def test_valid_configuration(self):
        """Test that valid configuration passes validation."""
        config = BackendConfig(
            storage_file_system="lustre",
            inode_soft_coefficient=1.5,
            inode_hard_coefficient=2.0,
            inode_base_multiplier=1000,
            use_mock_target_items=False,
            development_mode=False,
        )
        config.validate()  # Should not raise

    def test_invalid_inode_soft_coefficient(self):
        """Test validation of inode_soft_coefficient."""
        # Non-numeric
        config = BackendConfig(inode_soft_coefficient="invalid")
        with pytest.raises(
            ValueError, match="inode_soft_coefficient must be a positive number"
        ):
            config.validate()

        # Negative
        config = BackendConfig(inode_soft_coefficient=-1.0)
        with pytest.raises(
            ValueError, match="inode_soft_coefficient must be a positive number"
        ):
            config.validate()

        # Zero
        config = BackendConfig(inode_soft_coefficient=0)
        with pytest.raises(
            ValueError, match="inode_soft_coefficient must be a positive number"
        ):
            config.validate()

    def test_invalid_inode_hard_coefficient(self):
        """Test validation of inode_hard_coefficient."""
        # Non-numeric
        config = BackendConfig(inode_hard_coefficient="invalid")
        with pytest.raises(
            ValueError, match="inode_hard_coefficient must be a positive number"
        ):
            config.validate()

        # Negative
        config = BackendConfig(inode_hard_coefficient=-1.0)
        with pytest.raises(
            ValueError, match="inode_hard_coefficient must be a positive number"
        ):
            config.validate()

        # Zero
        config = BackendConfig(inode_hard_coefficient=0)
        with pytest.raises(
            ValueError, match="inode_hard_coefficient must be a positive number"
        ):
            config.validate()

    def test_soft_limit_greater_than_hard_limit(self):
        """Test validation when soft limit > hard limit."""
        config = BackendConfig(
            inode_soft_coefficient=2.0,
            inode_hard_coefficient=1.5,
        )
        with pytest.raises(
            ValueError, match="inode_hard_coefficient .* must be greater than"
        ):
            config.validate()

    def test_invalid_storage_file_system(self):
        """Test validation of storage_file_system."""
        # Non-string
        config = BackendConfig(storage_file_system=123)
        with pytest.raises(
            ValueError, match="storage_file_system must be a non-empty string"
        ):
            config.validate()

        # Empty string
        config = BackendConfig(storage_file_system="")
        with pytest.raises(
            ValueError, match="storage_file_system must be a non-empty string"
        ):
            config.validate()

        # Whitespace string
        config = BackendConfig(storage_file_system="   ")
        with pytest.raises(
            ValueError, match="storage_file_system must be a non-empty string"
        ):
            config.validate()

    def test_invalid_inode_base_multiplier(self):
        """Test validation of inode_base_multiplier."""
        # Non-numeric
        config = BackendConfig(inode_base_multiplier="invalid")
        with pytest.raises(
            ValueError, match="inode_base_multiplier must be a positive number"
        ):
            config.validate()

        # Negative
        config = BackendConfig(inode_base_multiplier=-1)
        with pytest.raises(
            ValueError, match="inode_base_multiplier must be a positive number"
        ):
            config.validate()
