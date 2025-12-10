from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class BackendConfig(BaseSettings):
    """Backend configuration settings."""

    storage_file_system: str = Field(default="lustre", min_length=1)
    inode_soft_coefficient: float = Field(default=1.33, gt=0)
    inode_hard_coefficient: float = Field(default=2.0, gt=0)
    inode_base_multiplier: float = Field(default=1_000_000, gt=0)
    use_mock_target_items: bool = False

    @model_validator(mode="after")
    def check_coefficients(self) -> "BackendConfig":
        """Validate logical relationship between coefficients."""
        if self.inode_hard_coefficient < self.inode_soft_coefficient:
            msg = (
                f"inode_hard_coefficient {self.inode_hard_coefficient} must be greater than "
                f"inode_soft_coefficient {self.inode_soft_coefficient}"
            )
            # Pydantic will wrap this ValueError into a ValidationError
            raise ValueError(msg)
        return self

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        populate_by_name=True,
        extra="ignore",
    )
