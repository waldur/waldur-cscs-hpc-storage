import sys

if sys.version_info >= (3, 11):
    from enum import StrEnum
else:
    from enum import Enum

    class StrEnum(str, Enum):
        """Compatibility StrEnum for Python < 3.11."""

        def __str__(self) -> str:
            return str(self.value)


class TargetStatus(StrEnum):
    """Status of the target item."""

    PENDING = "pending"
    ACTIVE = "active"
    REMOVING = "removing"
    REMOVED = "removed"
    ERROR = "error"
    UNKNOWN = "unknown"


class StorageDataType(StrEnum):
    """Type of the storage data."""

    STORE = "store"
    ARCHIVE = "archive"
    USERS = "users"
    SCRATCH = "scratch"


class TargetType(StrEnum):
    """Type of the target item."""

    PROJECT = "project"
    USER = "user"
    TENANT = "tenant"
    CUSTOMER = "customer"


class StorageSystem(StrEnum):
    """Allowed storage system values."""

    CAPSTOR = "capstor"
    VAST = "vast"
    IOPSSTOR = "iopsstor"


class QuotaType(StrEnum):
    """Type of quota."""

    SPACE = "space"
    INODES = "inodes"


class QuotaUnit(StrEnum):
    """Unit for quota values."""

    TERA = "tera"
    NONE = "none"


class EnforcementType(StrEnum):
    """Enforcement type for quotas."""

    SOFT = "soft"
    HARD = "hard"
