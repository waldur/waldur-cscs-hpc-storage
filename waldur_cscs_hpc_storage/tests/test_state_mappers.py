"""Tests for state mapper functions."""

from waldur_api_client.models.resource_state import ResourceState

from waldur_cscs_hpc_storage.mapper.state_mappers import (
    get_waldur_state_from_target_status,
    REVERSE_STATUS_MAPPING,
)
from waldur_cscs_hpc_storage.models.enums import TargetStatus


class TestReverseStatusMapping:
    """Test reverse mapping from TargetStatus to ResourceState."""

    def test_all_known_target_statuses_have_mapping(self):
        """Every TargetStatus except UNKNOWN should have a corresponding ResourceState."""
        for status in TargetStatus:
            if status == TargetStatus.UNKNOWN:
                assert get_waldur_state_from_target_status(status) is None
            else:
                assert status in REVERSE_STATUS_MAPPING, (
                    f"TargetStatus.{status.name} has no reverse mapping"
                )

    def test_pending_maps_to_creating(self):
        assert (
            get_waldur_state_from_target_status(TargetStatus.PENDING)
            == ResourceState.CREATING
        )

    def test_active_maps_to_ok(self):
        assert (
            get_waldur_state_from_target_status(TargetStatus.ACTIVE) == ResourceState.OK
        )

    def test_error_maps_to_erred(self):
        assert (
            get_waldur_state_from_target_status(TargetStatus.ERROR)
            == ResourceState.ERRED
        )

    def test_removing_maps_to_terminating(self):
        assert (
            get_waldur_state_from_target_status(TargetStatus.REMOVING)
            == ResourceState.TERMINATING
        )

    def test_removed_maps_to_terminated(self):
        assert (
            get_waldur_state_from_target_status(TargetStatus.REMOVED)
            == ResourceState.TERMINATED
        )

    def test_updating_maps_to_updating(self):
        assert (
            get_waldur_state_from_target_status(TargetStatus.UPDATING)
            == ResourceState.UPDATING
        )
