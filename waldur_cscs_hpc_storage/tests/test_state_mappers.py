"""Tests for state mapper functions."""

import pytest
from waldur_api_client.models.resource_state import ResourceState

from waldur_cscs_hpc_storage.mapper.state_mappers import (
    get_target_type_from_data_type,
    get_waldur_state_from_target_status,
    REVERSE_STATUS_MAPPING,
)
from waldur_cscs_hpc_storage.models.enums import TargetStatus, TargetType
from waldur_cscs_hpc_storage.tests.conftest import make_test_uuid


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


class TestTargetTypeFromDataType:
    """Map storage data type to target type, tolerating case and unknown types."""

    @pytest.mark.parametrize(
        ("data_type", "expected"),
        [
            ("store", TargetType.PROJECT),
            ("archive", TargetType.PROJECT),
            ("users", TargetType.USER),
            ("scratch", TargetType.USER),
        ],
    )
    def test_known_types(self, data_type, expected):
        assert (
            get_target_type_from_data_type(data_type, make_test_uuid("1")) == expected
        )

    @pytest.mark.parametrize("data_type", ["Users", "SCRATCH", " Users "])
    def test_known_types_are_case_insensitive(self, data_type):
        # "users"/"scratch" map to USER even when the offering supplies a
        # capitalized value.
        assert (
            get_target_type_from_data_type(data_type, make_test_uuid("1"))
            == TargetType.USER
        )

    def test_unknown_type_defaults_to_project(self):
        # An offering-defined type like "workspace" is unknown to the code and
        # falls back to a project-scoped target rather than being rejected.
        assert (
            get_target_type_from_data_type("workspace", make_test_uuid("1"))
            == TargetType.PROJECT
        )
