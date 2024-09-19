"""Tests for the synapseclient.models.user module."""

from unittest.mock import patch

import pytest

from synapseclient import Synapse
from synapseclient.models import UserPreference, UserProfile
from synapseclient.team import UserProfile as Synapse_UserProfile

FIRST_NAME = "John"
LAST_NAME = "Doe"
USER_NAME = "johndoe"
EMAIL = "john.doe@sagebase.org"
ETAG = "some_value"
OPEN_IDS = ["aa222", "bb333"]
BOGUS_URL = "https://sagebase.org"
SUMMARY = "some summary"
POSITION = "some position"
LOCATION = "some location"
INDUSTRY = "some industry"
COMPANY = "some company"
PROFILE_PICTURE_FILE_HANDLE_ID = "some_file_handle_id"
TEAM_NAME = "some team name"
PREFERENCE_1 = "false_value"
PREFFERENCE_2 = "true_value"
CREATED_ON = "2020-01-01T00:00:00.000Z"


class TestUser:
    """Tests for the UserGroupHeader class."""

    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    def get_example_synapse_user_profile(self) -> Synapse_UserProfile:
        return Synapse_UserProfile(
            ownerId=123,
            etag=ETAG,
            firstName=FIRST_NAME,
            lastName=LAST_NAME,
            emails=[EMAIL],
            openIds=OPEN_IDS,
            userName=USER_NAME,
            rStudioUrl=BOGUS_URL,
            summary=SUMMARY,
            position=POSITION,
            location=LOCATION,
            industry=INDUSTRY,
            company=COMPANY,
            profilePicureFileHandleId=PROFILE_PICTURE_FILE_HANDLE_ID,
            url=BOGUS_URL,
            teamName=TEAM_NAME,
            notificationSettings={
                "sendEmailNotifications": True,
                "markEmailedMessagesAsRead": False,
            },
            preferences=[
                {"name": PREFERENCE_1, "value": False},
                {"name": PREFFERENCE_2, "value": True},
            ],
            createdOn=CREATED_ON,
        )

    def test_fill_from_dict(self) -> None:
        # GIVEN a blank user profile
        user_profile = UserProfile()

        # WHEN we fill it from a dictionary
        user_profile.fill_from_dict(self.get_example_synapse_user_profile())

        # THEN the user profile should be filled
        assert user_profile.id == 123
        assert user_profile.etag == ETAG
        assert user_profile.first_name == FIRST_NAME
        assert user_profile.last_name == LAST_NAME
        assert user_profile.emails == [EMAIL]
        assert user_profile.open_ids == OPEN_IDS
        assert user_profile.username == USER_NAME
        assert user_profile.r_studio_url == BOGUS_URL
        assert user_profile.summary == SUMMARY
        assert user_profile.position == POSITION
        assert user_profile.location == LOCATION
        assert user_profile.industry == INDUSTRY
        assert user_profile.company == COMPANY
        assert (
            user_profile.profile_picure_file_handle_id == PROFILE_PICTURE_FILE_HANDLE_ID
        )
        assert user_profile.url == BOGUS_URL
        assert user_profile.team_name == TEAM_NAME
        assert user_profile.send_email_notifications == True
        assert user_profile.mark_emailed_messages_as_read == False
        assert user_profile.preferences == [
            UserPreference(name=PREFERENCE_1, value=False),
            UserPreference(name=PREFFERENCE_2, value=True),
        ]
        assert user_profile.created_on == CREATED_ON

    def test_get_id(self) -> None:
        # GIVEN a user profile
        user_profile = UserProfile(id=123)

        # WHEN we get the ID
        with patch.object(
            self.syn,
            "get_user_profile_by_id",
            return_value=(self.get_example_synapse_user_profile()),
        ) as mocked_client_call:
            profile = user_profile.get(synapse_client=self.syn)

            # THEN we should call the method with this data
            mocked_client_call.assert_called_once_with(id=123)

            # AND we should get the profile back
            assert profile.id == 123
            assert profile.etag == ETAG
            assert profile.first_name == FIRST_NAME
            assert profile.last_name == LAST_NAME
            assert profile.emails == [EMAIL]
            assert profile.open_ids == OPEN_IDS
            assert profile.username == USER_NAME
            assert profile.r_studio_url == BOGUS_URL
            assert profile.summary == SUMMARY
            assert profile.position == POSITION
            assert profile.location == LOCATION
            assert profile.industry == INDUSTRY
            assert profile.company == COMPANY
            assert (
                profile.profile_picure_file_handle_id == PROFILE_PICTURE_FILE_HANDLE_ID
            )
            assert profile.url == BOGUS_URL
            assert profile.team_name == TEAM_NAME
            assert profile.send_email_notifications == True
            assert profile.mark_emailed_messages_as_read == False
            assert profile.preferences == [
                UserPreference(name=PREFERENCE_1, value=False),
                UserPreference(name=PREFFERENCE_2, value=True),
            ]
            assert profile.created_on == CREATED_ON

    def test_get_username(self) -> None:
        # GIVEN a user profile
        user_profile = UserProfile(username=USER_NAME)

        # WHEN we get the ID
        with patch.object(
            self.syn,
            "get_user_profile_by_username",
            return_value=(self.get_example_synapse_user_profile()),
        ) as mocked_client_call:
            profile = user_profile.get(synapse_client=self.syn)

            # THEN we should call the method with this data
            mocked_client_call.assert_called_once_with(username=USER_NAME)

            # AND we should get the profile back
            assert profile.id == 123
            assert profile.etag == ETAG
            assert profile.first_name == FIRST_NAME
            assert profile.last_name == LAST_NAME
            assert profile.emails == [EMAIL]
            assert profile.open_ids == OPEN_IDS
            assert profile.username == USER_NAME
            assert profile.r_studio_url == BOGUS_URL
            assert profile.summary == SUMMARY
            assert profile.position == POSITION
            assert profile.location == LOCATION
            assert profile.industry == INDUSTRY
            assert profile.company == COMPANY
            assert (
                profile.profile_picure_file_handle_id == PROFILE_PICTURE_FILE_HANDLE_ID
            )
            assert profile.url == BOGUS_URL
            assert profile.team_name == TEAM_NAME
            assert profile.send_email_notifications == True
            assert profile.mark_emailed_messages_as_read == False
            assert profile.preferences == [
                UserPreference(name=PREFERENCE_1, value=False),
                UserPreference(name=PREFFERENCE_2, value=True),
            ]
            assert profile.created_on == CREATED_ON

    def test_get_neither(self) -> None:
        # GIVEN a blank user profile
        user_profile = UserProfile()

        # WHEN we get the ID
        with patch.object(
            self.syn,
            "get_user_profile_by_username",
            return_value=(self.get_example_synapse_user_profile()),
        ) as mocked_client_call:
            profile = user_profile.get(synapse_client=self.syn)

            # THEN we should call the method with this data
            mocked_client_call.assert_called_once_with()

            # AND we should get the profile back
            assert profile.id == 123
            assert profile.etag == ETAG
            assert profile.first_name == FIRST_NAME
            assert profile.last_name == LAST_NAME
            assert profile.emails == [EMAIL]
            assert profile.open_ids == OPEN_IDS
            assert profile.username == USER_NAME
            assert profile.r_studio_url == BOGUS_URL
            assert profile.summary == SUMMARY
            assert profile.position == POSITION
            assert profile.location == LOCATION
            assert profile.industry == INDUSTRY
            assert profile.company == COMPANY
            assert (
                profile.profile_picure_file_handle_id == PROFILE_PICTURE_FILE_HANDLE_ID
            )
            assert profile.url == BOGUS_URL
            assert profile.team_name == TEAM_NAME
            assert profile.send_email_notifications == True
            assert profile.mark_emailed_messages_as_read == False
            assert profile.preferences == [
                UserPreference(name=PREFERENCE_1, value=False),
                UserPreference(name=PREFFERENCE_2, value=True),
            ]
            assert profile.created_on == CREATED_ON

    def test_get_from_id(self) -> None:
        # GIVEN no user profile

        # WHEN we get from ID
        with patch.object(
            self.syn,
            "get_user_profile_by_id",
            return_value=(self.get_example_synapse_user_profile()),
        ) as mocked_client_call:
            profile = UserProfile.from_id(user_id=123, synapse_client=self.syn)

            # THEN we should call the method with this data
            mocked_client_call.assert_called_once_with(id=123)

            # AND we should get the profile back
            assert profile.id == 123
            assert profile.etag == ETAG
            assert profile.first_name == FIRST_NAME
            assert profile.last_name == LAST_NAME
            assert profile.emails == [EMAIL]
            assert profile.open_ids == OPEN_IDS
            assert profile.username == USER_NAME
            assert profile.r_studio_url == BOGUS_URL
            assert profile.summary == SUMMARY
            assert profile.position == POSITION
            assert profile.location == LOCATION
            assert profile.industry == INDUSTRY
            assert profile.company == COMPANY
            assert (
                profile.profile_picure_file_handle_id == PROFILE_PICTURE_FILE_HANDLE_ID
            )
            assert profile.url == BOGUS_URL
            assert profile.team_name == TEAM_NAME
            assert profile.send_email_notifications == True
            assert profile.mark_emailed_messages_as_read == False
            assert profile.preferences == [
                UserPreference(name=PREFERENCE_1, value=False),
                UserPreference(name=PREFFERENCE_2, value=True),
            ]
            assert profile.created_on == CREATED_ON

    def test_get_from_username(self) -> None:
        # GIVEN no user profile

        # WHEN we get from ID
        with patch.object(
            self.syn,
            "get_user_profile_by_username",
            return_value=(self.get_example_synapse_user_profile()),
        ) as mocked_client_call:
            profile = UserProfile.from_username(
                username=USER_NAME, synapse_client=self.syn
            )

            # THEN we should call the method with this data
            mocked_client_call.assert_called_once_with(username=USER_NAME)

            # AND we should get the profile back
            assert profile.id == 123
            assert profile.etag == ETAG
            assert profile.first_name == FIRST_NAME
            assert profile.last_name == LAST_NAME
            assert profile.emails == [EMAIL]
            assert profile.open_ids == OPEN_IDS
            assert profile.username == USER_NAME
            assert profile.r_studio_url == BOGUS_URL
            assert profile.summary == SUMMARY
            assert profile.position == POSITION
            assert profile.location == LOCATION
            assert profile.industry == INDUSTRY
            assert profile.company == COMPANY
            assert (
                profile.profile_picure_file_handle_id == PROFILE_PICTURE_FILE_HANDLE_ID
            )
            assert profile.url == BOGUS_URL
            assert profile.team_name == TEAM_NAME
            assert profile.send_email_notifications == True
            assert profile.mark_emailed_messages_as_read == False
            assert profile.preferences == [
                UserPreference(name=PREFERENCE_1, value=False),
                UserPreference(name=PREFFERENCE_2, value=True),
            ]
            assert profile.created_on == CREATED_ON

    def test_is_certified_id(self) -> None:
        # GIVEN a user profile
        user_profile = UserProfile(id=123)

        # WHEN we check if the user is certified
        with patch.object(
            self.syn,
            "is_certified",
            return_value=True,
        ) as mocked_client_call:
            is_certified = user_profile.is_certified(synapse_client=self.syn)

            # THEN we should call the method with this data
            mocked_client_call.assert_called_once_with(user=123)

            # AND we should get the profile back
            assert is_certified == True

    def test_is_certified_username(self) -> None:
        # GIVEN a user profile
        user_profile = UserProfile(username=USER_NAME)

        # WHEN we check if the user is certified
        with patch.object(
            self.syn,
            "is_certified",
            return_value=True,
        ) as mocked_client_call:
            is_certified = user_profile.is_certified(synapse_client=self.syn)

            # THEN we should call the method with this data
            mocked_client_call.assert_called_once_with(user=USER_NAME)

            # AND we should get the profile back
            assert is_certified == True

    def test_is_certified_neither(self) -> None:
        # GIVEN a user profile
        user_profile = UserProfile()

        # WHEN we check if the user is certified
        with pytest.raises(ValueError) as e:
            user_profile.is_certified(synapse_client=self.syn)

        # THEN we should get an error
        assert str(e.value) == "Must specify either id or username"
