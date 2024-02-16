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

    def test_from_id(self) -> None:
        # GIVEN our test profile
        integration_test_profile = UserProfile().get()

        # WHEN we get the profile by ID
        profile = UserProfile.from_id(integration_test_profile.id)

        # THEN we expect the profile to be the same as the one we got from the fixture
        assert profile == integration_test_profile

    def test_from_username(self) -> None:
        # GIVEN our test profile
        integration_test_profile = UserProfile().get()

        # WHEN we get the profile by username
        profile = UserProfile.from_username(integration_test_profile.username)

        # THEN we expect the profile to be the same as the one we got from the fixture
        assert profile == integration_test_profile

    def test_is_certified_id(self) -> None:
        # GIVEN out test profile
        integration_test_profile = UserProfile().get()

        # AND a copy of the profile
        profile_copy = UserProfile(id=integration_test_profile.id)

        # WHEN we check if the profile is certified
        is_certified = profile_copy.is_certified()

        # THEN we expect the profile to not be certified
        assert is_certified is False

    def test_is_certified_username(self) -> None:
        # GIVEN out test profile
        integration_test_profile = UserProfile().get()

        # AND a copy of the profile
        profile_copy = UserProfile(username=integration_test_profile.username)

        # WHEN we check if the profile is certified
        is_certified = profile_copy.is_certified()

        # THEN we expect the profile to not be certified
        assert is_certified is False
