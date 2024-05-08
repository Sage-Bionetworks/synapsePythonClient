"""Integration tests for UserProfile."""

import pytest
from synapseclient.models import UserProfile
from typing import Callable
import pytest
from synapseclient import (
    Synapse,
)


class TestUser:
    """Integration tests for UserProfile."""

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    async def test_from_id(self) -> None:
        # GIVEN our test profile
        integration_test_profile = await UserProfile().get_async()

        # WHEN we get the profile by ID
        profile = await UserProfile.from_id_async(integration_test_profile.id)

        # THEN we expect the profile to be the same as the one we got from the fixture
        assert profile == integration_test_profile

    async def test_from_username(self) -> None:
        # GIVEN our test profile
        integration_test_profile = await UserProfile().get_async()

        # WHEN we get the profile by username
        profile = await UserProfile.from_username_async(
            integration_test_profile.username
        )

        # THEN we expect the profile to be the same as the one we got from the fixture
        assert profile == integration_test_profile

    async def test_is_certified_id(self) -> None:
        # GIVEN out test profile
        integration_test_profile = await UserProfile().get_async()

        # AND a copy of the profile
        profile_copy = UserProfile(id=integration_test_profile.id)

        # WHEN we check if the profile is certified
        is_certified = await profile_copy.is_certified_async()

        # THEN we expect the profile to not be certified
        assert is_certified is False

    async def test_is_certified_username(self) -> None:
        # GIVEN out test profile
        integration_test_profile = await UserProfile().get_async()

        # AND a copy of the profile
        profile_copy = UserProfile(username=integration_test_profile.username)

        # WHEN we check if the profile is certified
        is_certified = await profile_copy.is_certified_async()

        # THEN we expect the profile to not be certified
        assert is_certified is False
