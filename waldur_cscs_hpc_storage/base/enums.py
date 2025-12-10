from enum import StrEnum


class TargetStatus(StrEnum):
    """Status of the target item."""

    PENDING = "pending"
    ACTIVE = "active"
    REMOVING = "removing"
    REMOVED = "removed"
    ERROR = "error"
    UPDATING = "updating"
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


class TargetIdScope(StrEnum):
    """Scope for target ID generation."""

    TENANT = "tenant"
    CUSTOMER = "customer"
    PROJECT = "project"
    USER = "user"
    STORAGE_SYSTEM = "storage_system"
    STORAGE_FILE_SYSTEM = "storage_file_system"
    STORAGE_DATA_TYPE = "storage_data_type"
