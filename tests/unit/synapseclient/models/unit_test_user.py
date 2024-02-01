from unittest.mock import patch
import pytest
from synapseclient.models import UserPreference, UserProfile
from synapseclient.team import (
    UserProfile as Synapse_UserProfile,
)


class TestUser:
    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn):
        self.syn = syn

    def get_example_synapse_user_profile(self) -> Synapse_UserProfile:
        return Synapse_UserProfile(
            ownerId=123,
            etag="some_value",
            firstName="John",
            lastName="Doe",
            emails=["john.doe@sagebase.org"],
            openIds=["aa222", "bb333"],
            userName="johndoe",
            rStudioUrl="https://sagebase.org",
            summary="some summary",
            position="some position",
            location="some location",
            industry="some industry",
            company="some company",
            profilePicureFileHandleId="some_file_handle_id",
            url="https://sagebase.org",
            teamName="some team name",
            notificationSettings={
                "sendEmailNotifications": True,
                "markEmailedMessagesAsRead": False,
            },
            preferences=[
                {"name": "false_value", "value": False},
                {"name": "true_value", "value": True},
            ],
            createdOn="2020-01-01T00:00:00.000Z",
        )

    def test_fill_from_dict(self) -> None:
        # GIVEN a blank user profile
        user_profile = UserProfile()

        # WHEN we fill it from a dictionary
        user_profile.fill_from_dict(self.get_example_synapse_user_profile())

        # THEN the user profile should be filled
        assert user_profile.id == 123
        assert user_profile.etag == "some_value"
        assert user_profile.first_name == "John"
        assert user_profile.last_name == "Doe"
        assert user_profile.emails == ["john.doe@sagebase.org"]
        assert user_profile.open_ids == ["aa222", "bb333"]
        assert user_profile.username == "johndoe"
        assert user_profile.r_studio_url == "https://sagebase.org"
        assert user_profile.summary == "some summary"
        assert user_profile.position == "some position"
        assert user_profile.location == "some location"
        assert user_profile.industry == "some industry"
        assert user_profile.company == "some company"
        assert user_profile.profile_picure_file_handle_id == "some_file_handle_id"
        assert user_profile.url == "https://sagebase.org"
        assert user_profile.team_name == "some team name"
        assert user_profile.send_email_notifications == True
        assert user_profile.mark_emailed_messages_as_read == False
        assert user_profile.preferences == [
            UserPreference(name="false_value", value=False),
            UserPreference(name="true_value", value=True),
        ]
        assert user_profile.created_on == "2020-01-01T00:00:00.000Z"

    @pytest.mark.asyncio
    async def test_get_id(self) -> None:
        # GIVEN a user profile
        user_profile = UserProfile(id=123)

        # WHEN we get the ID
        with patch.object(
            self.syn,
            "get_user_profile_by_id",
            return_value=(self.get_example_synapse_user_profile()),
        ) as mocked_client_call:
            profile = await user_profile.get()

            # THEN we should call the method with this data
            mocked_client_call.assert_called_once_with(id=123)

            # AND we should get the profile back
            assert profile.id == 123
            assert profile.etag == "some_value"
            assert profile.first_name == "John"
            assert profile.last_name == "Doe"
            assert profile.emails == ["john.doe@sagebase.org"]
            assert profile.open_ids == ["aa222", "bb333"]
            assert profile.username == "johndoe"
            assert profile.r_studio_url == "https://sagebase.org"
            assert profile.summary == "some summary"
            assert profile.position == "some position"
            assert profile.location == "some location"
            assert profile.industry == "some industry"
            assert profile.company == "some company"
            assert profile.profile_picure_file_handle_id == "some_file_handle_id"
            assert profile.url == "https://sagebase.org"
            assert profile.team_name == "some team name"
            assert profile.send_email_notifications == True
            assert profile.mark_emailed_messages_as_read == False
            assert profile.preferences == [
                UserPreference(name="false_value", value=False),
                UserPreference(name="true_value", value=True),
            ]
            assert profile.created_on == "2020-01-01T00:00:00.000Z"

    @pytest.mark.asyncio
    async def test_get_username(self) -> None:
        # GIVEN a user profile
        user_profile = UserProfile(username="johndoe")

        # WHEN we get the ID
        with patch.object(
            self.syn,
            "get_user_profile_by_username",
            return_value=(self.get_example_synapse_user_profile()),
        ) as mocked_client_call:
            profile = await user_profile.get()

            # THEN we should call the method with this data
            mocked_client_call.assert_called_once_with(username="johndoe")

            # AND we should get the profile back
            assert profile.id == 123
            assert profile.etag == "some_value"
            assert profile.first_name == "John"
            assert profile.last_name == "Doe"
            assert profile.emails == ["john.doe@sagebase.org"]
            assert profile.open_ids == ["aa222", "bb333"]
            assert profile.username == "johndoe"
            assert profile.r_studio_url == "https://sagebase.org"
            assert profile.summary == "some summary"
            assert profile.position == "some position"
            assert profile.location == "some location"
            assert profile.industry == "some industry"
            assert profile.company == "some company"
            assert profile.profile_picure_file_handle_id == "some_file_handle_id"
            assert profile.url == "https://sagebase.org"
            assert profile.team_name == "some team name"
            assert profile.send_email_notifications == True
            assert profile.mark_emailed_messages_as_read == False
            assert profile.preferences == [
                UserPreference(name="false_value", value=False),
                UserPreference(name="true_value", value=True),
            ]
            assert profile.created_on == "2020-01-01T00:00:00.000Z"

    @pytest.mark.asyncio
    async def test_get_neither(self) -> None:
        # GIVEN a blank user profile
        user_profile = UserProfile()

        # WHEN we get the ID
        with patch.object(
            self.syn,
            "get_user_profile_by_username",
            return_value=(self.get_example_synapse_user_profile()),
        ) as mocked_client_call:
            profile = await user_profile.get()

            # THEN we should call the method with this data
            mocked_client_call.assert_called_once_with()

            # AND we should get the profile back
            assert profile.id == 123
            assert profile.etag == "some_value"
            assert profile.first_name == "John"
            assert profile.last_name == "Doe"
            assert profile.emails == ["john.doe@sagebase.org"]
            assert profile.open_ids == ["aa222", "bb333"]
            assert profile.username == "johndoe"
            assert profile.r_studio_url == "https://sagebase.org"
            assert profile.summary == "some summary"
            assert profile.position == "some position"
            assert profile.location == "some location"
            assert profile.industry == "some industry"
            assert profile.company == "some company"
            assert profile.profile_picure_file_handle_id == "some_file_handle_id"
            assert profile.url == "https://sagebase.org"
            assert profile.team_name == "some team name"
            assert profile.send_email_notifications == True
            assert profile.mark_emailed_messages_as_read == False
            assert profile.preferences == [
                UserPreference(name="false_value", value=False),
                UserPreference(name="true_value", value=True),
            ]
            assert profile.created_on == "2020-01-01T00:00:00.000Z"

    @pytest.mark.asyncio
    async def test_get_from_id(self) -> None:
        # GIVEN no user profile

        # WHEN we get from ID
        with patch.object(
            self.syn,
            "get_user_profile_by_id",
            return_value=(self.get_example_synapse_user_profile()),
        ) as mocked_client_call:
            profile = await UserProfile.from_id(user_id=123)

            # THEN we should call the method with this data
            mocked_client_call.assert_called_once_with(id=123)

            # AND we should get the profile back
            assert profile.id == 123
            assert profile.etag == "some_value"
            assert profile.first_name == "John"
            assert profile.last_name == "Doe"
            assert profile.emails == ["john.doe@sagebase.org"]
            assert profile.open_ids == ["aa222", "bb333"]
            assert profile.username == "johndoe"
            assert profile.r_studio_url == "https://sagebase.org"
            assert profile.summary == "some summary"
            assert profile.position == "some position"
            assert profile.location == "some location"
            assert profile.industry == "some industry"
            assert profile.company == "some company"
            assert profile.profile_picure_file_handle_id == "some_file_handle_id"
            assert profile.url == "https://sagebase.org"
            assert profile.team_name == "some team name"
            assert profile.send_email_notifications == True
            assert profile.mark_emailed_messages_as_read == False
            assert profile.preferences == [
                UserPreference(name="false_value", value=False),
                UserPreference(name="true_value", value=True),
            ]
            assert profile.created_on == "2020-01-01T00:00:00.000Z"

    @pytest.mark.asyncio
    async def test_get_from_username(self) -> None:
        # GIVEN no user profile

        # WHEN we get from ID
        with patch.object(
            self.syn,
            "get_user_profile_by_username",
            return_value=(self.get_example_synapse_user_profile()),
        ) as mocked_client_call:
            profile = await UserProfile.from_username(username="johndoe")

            # THEN we should call the method with this data
            mocked_client_call.assert_called_once_with(username="johndoe")

            # AND we should get the profile back
            assert profile.id == 123
            assert profile.etag == "some_value"
            assert profile.first_name == "John"
            assert profile.last_name == "Doe"
            assert profile.emails == ["john.doe@sagebase.org"]
            assert profile.open_ids == ["aa222", "bb333"]
            assert profile.username == "johndoe"
            assert profile.r_studio_url == "https://sagebase.org"
            assert profile.summary == "some summary"
            assert profile.position == "some position"
            assert profile.location == "some location"
            assert profile.industry == "some industry"
            assert profile.company == "some company"
            assert profile.profile_picure_file_handle_id == "some_file_handle_id"
            assert profile.url == "https://sagebase.org"
            assert profile.team_name == "some team name"
            assert profile.send_email_notifications == True
            assert profile.mark_emailed_messages_as_read == False
            assert profile.preferences == [
                UserPreference(name="false_value", value=False),
                UserPreference(name="true_value", value=True),
            ]
            assert profile.created_on == "2020-01-01T00:00:00.000Z"

    @pytest.mark.asyncio
    async def test_is_certified_id(self) -> None:
        # GIVEN a user profile
        user_profile = UserProfile(id=123)

        # WHEN we check if the user is certified
        with patch.object(
            self.syn,
            "is_certified",
            return_value=True,
        ) as mocked_client_call:
            is_certified = await user_profile.is_certified()

            # THEN we should call the method with this data
            mocked_client_call.assert_called_once_with(user=123)

            # AND we should get the profile back
            assert is_certified == True

    @pytest.mark.asyncio
    async def test_is_certified_username(self) -> None:
        # GIVEN a user profile
        user_profile = UserProfile(username="johndoe")

        # WHEN we check if the user is certified
        with patch.object(
            self.syn,
            "is_certified",
            return_value=True,
        ) as mocked_client_call:
            is_certified = await user_profile.is_certified()

            # THEN we should call the method with this data
            mocked_client_call.assert_called_once_with(user="johndoe")

            # AND we should get the profile back
            assert is_certified == True

    @pytest.mark.asyncio
    async def test_is_certified_neither(self) -> None:
        # GIVEN a user profile
        user_profile = UserProfile()

        # WHEN we check if the user is certified
        with pytest.raises(ValueError) as e:
            await user_profile.is_certified()

        # THEN we should get an error
        assert str(e.value) == "Must specify either id or username"
