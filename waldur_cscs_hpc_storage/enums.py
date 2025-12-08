from enum import StrEnum


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
