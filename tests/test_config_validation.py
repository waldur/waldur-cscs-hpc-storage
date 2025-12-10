"""Tests for configuration validation."""

import pytest
from pydantic import ValidationError

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
        # Pydantic validation happens at init, so if we are here, it's valid.
        assert config.storage_file_system == "lustre"

    def test_invalid_inode_soft_coefficient(self):
        """Test validation of inode_soft_coefficient."""
        # Non-numeric string that can't be cast to float raises ValidationError
        with pytest.raises(ValidationError) as excinfo:
            BackendConfig(inode_soft_coefficient="invalid")
        assert "should be a valid number" in str(excinfo.value)

        # Negative
        with pytest.raises(ValidationError) as excinfo:
            BackendConfig(inode_soft_coefficient=-1.0)
        assert "greater than 0" in str(excinfo.value)

        # Zero
        with pytest.raises(ValidationError) as excinfo:
            BackendConfig(inode_soft_coefficient=0)
        assert "greater than 0" in str(excinfo.value)

    def test_invalid_inode_hard_coefficient(self):
        """Test validation of inode_hard_coefficient."""
        # Non-numeric
        with pytest.raises(ValidationError) as excinfo:
            BackendConfig(inode_hard_coefficient="invalid")
        assert "should be a valid number" in str(excinfo.value)

        # Negative
        with pytest.raises(ValidationError) as excinfo:
            BackendConfig(inode_hard_coefficient=-1.0)
        assert "greater than 0" in str(excinfo.value)

        # Zero
        with pytest.raises(ValidationError) as excinfo:
            BackendConfig(inode_hard_coefficient=0)
        assert "greater than 0" in str(excinfo.value)

    def test_soft_limit_greater_than_hard_limit(self):
        """Test validation when soft limit > hard limit."""
        # This is caught by the @model_validator
        with pytest.raises(ValidationError) as excinfo:
            BackendConfig(
                inode_soft_coefficient=2.0,
                inode_hard_coefficient=1.5,
            )
        assert "must be greater than inode_soft_coefficient" in str(excinfo.value)

    def test_invalid_storage_file_system(self):
        """Test validation of storage_file_system."""
        # Non-string (int) - Pydantic will coerce int to string "123" which is valid min_length=1
        # So BackendConfig(storage_file_system=123) might actually succeed if strict=False (default).
        # Let's verify strict behavior or check if "123" is acceptable.
        # "must be a non-empty string" was the old requirement. "123" is non-empty.

        # Empty string
        with pytest.raises(ValidationError) as excinfo:
            BackendConfig(storage_file_system="")
        assert "String should have at least 1 character" in str(excinfo.value)

    def test_invalid_inode_base_multiplier(self):
        """Test validation of inode_base_multiplier."""
        # Non-numeric
        with pytest.raises(ValidationError) as excinfo:
            BackendConfig(inode_base_multiplier="invalid")
        assert "should be a valid number" in str(excinfo.value)

        # Negative
        with pytest.raises(ValidationError) as excinfo:
            BackendConfig(inode_base_multiplier=-1)
        assert "greater than 0" in str(excinfo.value)
