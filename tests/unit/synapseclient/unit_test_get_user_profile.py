"""Unit tests for user profile"""

from unittest.mock import patch

import pytest

from synapseclient import Synapse

test_user_profile = {
    "ownerId": "1234567",
    "etag": "test-etag",
    "firstName": "Test",
    "lastName": "User",
    "emails": ["test.user@example.com"],
    "openIds": [],
    "userName": "test_user",
    "displayName": "Test User",
    "summary": "This is a test user",
    "position": "Test Position",
    "location": "Test Location",
    "industry": "Test Industry",
    "company": "Test Company",
    "profilePicureFileHandleId": "test-file-handle",
    "url": "https://example.com",
    "notificationSettings": {"sendEmailNotifications": False},
    "createdOn": "2022-01-01T00:00:00.000Z",
}


class TestGetUserProfileByUserName:
    principals = [
        {
            "ownerId": "7654321",
            "firstName": "test",
            "lastName": "person",
            "userName": "abc",
            "isIndividual": True,
        },
        {
            "ownerId": "1234567",
            "firstName": "test_2",
            "lastName": "person_2",
            "userName": "test_user",
            "isIndividual": True,
        },
    ]

    @pytest.fixture(autouse=True, scope="function")
    def setup_method(self, syn: Synapse) -> None:
        self.syn = syn
        self.syn.restGET = patch.object(
            self.syn, "restGET", return_value=test_user_profile
        ).start()
        self.syn._findPrincipals = patch.object(
            self.syn, "_findPrincipals", return_value=self.principals
        ).start()

    def teardown_method(self) -> None:
        self.syn.restGET.stop()
        self.syn._findPrincipals.stop()

    def test_that_get_user_profile_returns_expected_with_no_id(self) -> None:
        result = self.syn.get_user_profile_by_username()
        self.syn.restGET.assert_called_once_with("/userProfile/", headers=None)
        assert result == test_user_profile

    def test_that_get_user_profile_returns_expected_with_username(self) -> None:
        result = self.syn.get_user_profile_by_username("test_user")
        self.syn.restGET.assert_called_once_with("/userProfile/1234567", headers=None)
        assert result == test_user_profile

    def test_that_get_user_profile_raises_value_error_when_user_does_not_exist(
        self,
    ) -> None:
        with pytest.raises(ValueError, match="Can't find user *"):
            self.syn.get_user_profile_by_username("not_a_user")

    def test_that_get_user_profile_raises_type_error_when_id_is_not_allowed_type(
        self,
    ) -> None:
        with pytest.raises(TypeError, match="username must be string or None"):
            self.syn.get_user_profile_by_username(1234567)


class TestGetUserProfileByID:
    @pytest.fixture(autouse=True, scope="function")
    def setup_method(self, syn: Synapse) -> None:
        self.syn = syn
        self.syn.restGET = patch.object(
            self.syn, "restGET", return_value=test_user_profile
        ).start()

    def teardown_method(self) -> None:
        self.syn.restGET.stop()

    def test_that_get_user_profile_by_id_returns_expected_when_id_is_defined(
        self,
    ) -> None:
        result = self.syn.get_user_profile_by_id(1234567)
        self.syn.restGET.assert_called_once_with("/userProfile/1234567", headers=None)
        assert result == test_user_profile

    def test_that_get_user_profile_by_id_returns_expected_when_id_is_not_defined(
        self,
    ) -> None:
        result = self.syn.get_user_profile_by_id()
        self.syn.restGET.assert_called_once_with("/userProfile/", headers=None)
        assert result == test_user_profile

    def test_that_get_user_profile_by_id_raises_type_error_when_id_is_not_int(
        self,
    ) -> None:
        with pytest.raises(TypeError, match="id must be an 'ownerId' integer"):
            self.syn.get_user_profile_by_id("1234567")
