"""Mappers for Waldur state and storage data type to target types and statuses."""

import logging
from waldur_api_client.models.resource_state import ResourceState

from waldur_cscs_hpc_storage.models.enums import (
    StorageDataType,
    TargetStatus,
    TargetType,
)

logger = logging.getLogger(__name__)

# Mapping from Waldur resource state to target status
TARGET_STATUS_MAPPING: dict[ResourceState, TargetStatus] = {
    ResourceState.CREATING: TargetStatus.PENDING,
    ResourceState.OK: TargetStatus.ACTIVE,
    ResourceState.ERRED: TargetStatus.ERROR,
    ResourceState.TERMINATING: TargetStatus.REMOVING,
    ResourceState.TERMINATED: TargetStatus.REMOVED,
    ResourceState.UPDATING: TargetStatus.UPDATING,
}

# Mapping from storage data type to target type
DATA_TYPE_TO_TARGET_MAPPING: dict[StorageDataType, TargetType] = {
    StorageDataType.STORE: TargetType.PROJECT,
    StorageDataType.ARCHIVE: TargetType.PROJECT,
    StorageDataType.USERS: TargetType.USER,
    StorageDataType.SCRATCH: TargetType.USER,
}


def get_target_status_from_waldur_state(state: ResourceState) -> TargetStatus:
    """Map Waldur resource state string to target item status.

    Args:
        state: Waldur resource state as a string (e.g., "Creating", "OK", "Erred")

    Returns:
        Corresponding TargetStatus enum value, defaults to PENDING for unknown states
    """
    for rs, ts in TARGET_STATUS_MAPPING.items():
        if str(rs) == state:
            return ts
    return TargetStatus.PENDING


def get_target_type_from_data_type(
    storage_data_type: str,
    resource_uuid: str = "unknown",
) -> TargetType:
    """Map storage data type string to target type.

    Args:
        storage_data_type: Storage data type string (e.g., "store", "archive", "users")
        resource_uuid: Resource UUID for logging purposes

    Returns:
        Corresponding TargetType enum value, defaults to PROJECT for unknown types
    """
    # Validate storage_data_type is a string
    if not isinstance(storage_data_type, str):
        error_msg = (
            f"Invalid storage_data_type for resource {resource_uuid}: "
            f"expected string, got {type(storage_data_type).__name__}. "
            f"Value: {storage_data_type!r}"
        )
        logger.error(error_msg)
        raise TypeError(error_msg)

    # Validate that storage_data_type is a supported type
    try:
        data_type_enum = StorageDataType(storage_data_type)
    except ValueError:
        data_type_enum = None

    if data_type_enum is None or data_type_enum not in DATA_TYPE_TO_TARGET_MAPPING:
        logger.warning(
            "Unknown storage_data_type '%s' for resource %s, using default 'project' "
            "target type. Supported types: %s",
            storage_data_type,
            resource_uuid,
            list(DATA_TYPE_TO_TARGET_MAPPING.keys()),
        )
        return TargetType.PROJECT

    target_type = DATA_TYPE_TO_TARGET_MAPPING[data_type_enum]
    logger.debug(
        "Mapped storage_data_type '%s' to target_type '%s'",
        storage_data_type,
        target_type,
    )
    return target_type
