import pytest
import asyncio
from waldur_cscs_hpc_storage.services.auth import user_mapper, User


@pytest.mark.asyncio
async def test_user_mapper_is_async_and_works():
    """Test that user_mapper is an async function and returns a User object."""

    # Define sample userinfo
    userinfo = {
        "preferred_username": "test_user",
        "sub": "12345",
        "clientId": "test_client",
        "roles": ["user"],
        "groups": ["group1"],
    }

    # Check if user_mapper is a coroutine function
    assert asyncio.iscoroutinefunction(user_mapper), (
        "user_mapper should be an async function"
    )

    # Call user_mapper and await the result
    user = await user_mapper(userinfo)

    # Verify the result
    assert isinstance(user, User)
    assert user.preferred_username == "test_user"


@pytest.mark.asyncio
async def test_user_mapper_fallback():
    """Test user_mapper fallback logic."""
    userinfo = {"sub": "fallback_user"}

    user = await user_mapper(userinfo)
    assert user.preferred_username == "fallback_user"
