"""Services package for external integrations."""

from waldur_cscs_hpc_storage.services.auth import (
    User,
    mock_user,
    setup_auth,
    user_mapper,
)
from waldur_cscs_hpc_storage.services.gid_service import GidService
from waldur_cscs_hpc_storage.services.mock_gid_service import MockGidService
from waldur_cscs_hpc_storage.services.waldur_service import (
    WaldurResourceResponse,
    WaldurService,
)

__all__ = [
    "GidService",
    "MockGidService",
    "WaldurService",
    "WaldurResourceResponse",
    "User",
    "mock_user",
    "setup_auth",
    "user_mapper",
]
