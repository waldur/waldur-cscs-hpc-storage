import pytest
import os
from uuid import UUID, uuid5, NAMESPACE_DNS


def make_test_uuid(name: str) -> UUID:
    """Generate a deterministic UUID from a string for testing.

    Args:
        name: A readable string identifier

    Returns:
        A deterministic UUID generated from the string

    Example:
        >>> make_test_uuid("customer-123")
        UUID('...')
    """
    return uuid5(NAMESPACE_DNS, f"test:{name}")


@pytest.fixture(autouse=True)
def clean_env():
    """Ensure no config is loaded from env during tests."""
    vars_to_clear = [
        "WALDUR_API_URL",
        "WALDUR_API_TOKEN",
        "WALDUR_VERIFY_SSL",
        "WALDUR_SOCKS_PROXY",
        "WALDUR_AGENT_HEADER",
        "STORAGE_SYSTEMS",
        "DISABLE_AUTH",
        "CSCS_KEYCLOAK_URL",
        "CSCS_KEYCLOAK_REALM",
        "CSCS_KEYCLOAK_CLIENT_ID",
        "CSCS_KEYCLOAK_CLIENT_SECRET",
        "HPC_USER_DEVELOPMENT_MODE",
        "HPC_USER_API_URL",
        "HPC_USER_CLIENT_ID",
        "HPC_USER_CLIENT_SECRET",
        "HPC_USER_OIDC_TOKEN_URL",
        "HPC_USER_OIDC_SCOPE",
        "HPC_USER_SOCKS_PROXY",
        "STORAGE_FILE_SYSTEM",
        "INODE_SOFT_COEFFICIENT",
        "INODE_HARD_COEFFICIENT",
        "INODE_BASE_MULTIPLIER",
        "USE_MOCK_TARGET_ITEMS",
        "SENTRY_DSN",
        "SENTRY_ENVIRONMENT",
        "SENTRY_TRACES_SAMPLE_RATE",
        "DEBUG",
        "WALDUR_CSCS_STORAGE_PROXY_CONFIG_PATH",
    ]
    stashed = {}
    for var in vars_to_clear:
        if var in os.environ:
            stashed[var] = os.environ.pop(var)

    yield

    for var, val in stashed.items():
        os.environ[var] = val
