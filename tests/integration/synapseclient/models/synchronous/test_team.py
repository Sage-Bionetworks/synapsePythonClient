import uuid
import time

import pytest

from synapseclient import Synapse
from synapseclient.core.exceptions import SynapseHTTPError
from synapseclient.models import Team
from synapseclient.models.user import UserGroupHeader


class TestTeam:
    """Integration tests for Team."""

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse) -> None:
        self.syn = syn
        self.expected_name = "test_team_" + str(uuid.uuid4())
        self.expected_description = "test description"
        self.expected_icon = None
        self.team = Team(
            name=self.expected_name,
            description=self.expected_description,
            icon=self.expected_icon,
        )
        self.TEST_USER = "DPETestUser2"
        self.TEST_MESSAGE = "test message"

    def test_create(self) -> None:
        # GIVEN a team object self.team
        # WHEN I create the team on Synapse
        test_team = self.team.create()
        # THEN I expect the created team to be returned
        assert test_team.id is not None
        assert test_team.name == self.expected_name
        assert test_team.description == self.expected_description
        assert test_team.icon is None
        assert test_team.can_public_join is False
        assert test_team.can_request_membership is True
        assert test_team.etag is not None
        assert test_team.created_on is not None
        assert test_team.modified_on is not None
        assert test_team.created_by is not None
        assert test_team.modified_by is not None
        # Clean up
        test_team.delete()

    def test_delete(self) -> None:
        # GIVEN a team object self.team
        # WHEN I create the team on Synapse
        test_team = self.team.create()
        # AND I delete the team
        test_team.delete()
        # THEN I expect the team to no longer exist
        with pytest.raises(
            SynapseHTTPError,
            match=f"404 Client Error: \nTeam id: '{test_team.id}' does not exist",
        ):
            Team.from_id(id=test_team.id)

    def test_get_with_id(self) -> None:
        # GIVEN a team created in Synapse
        synapse_team = self.team.create()
        # AND a locally created Team object with the same id and name
        id_team = Team(id=self.team.id, name=self.team.name)
        # WHEN I get the team
        id_team = id_team.get()
        # THEN I expect the team to be returned
        assert id_team.id == synapse_team.id
        assert id_team.name == synapse_team.name
        assert id_team.description == synapse_team.description
        assert id_team.icon == synapse_team.icon
        assert id_team.etag == synapse_team.etag
        assert id_team.created_on == synapse_team.created_on
        assert id_team.modified_on == synapse_team.modified_on
        assert id_team.created_by == synapse_team.created_by
        assert id_team.modified_by == synapse_team.modified_by
        # Clean up
        synapse_team.delete()

    def test_get_with_name(self) -> None:
        # GIVEN a team created in Synapse
        synapse_team = self.team.create()
        # This sleep is necessary because the API is eventually consistent
        time.sleep(5)
        # AND a locally created Team object with the same name, but no id
        name_team = Team(name=self.team.name)
        # WHEN I get the team
        name_team = name_team.get()
        # THEN I expect the team to be returned
        assert name_team.id == synapse_team.id
        assert name_team.name == synapse_team.name
        assert name_team.description == synapse_team.description
        assert name_team.icon == synapse_team.icon
        assert name_team.etag == synapse_team.etag
        assert name_team.created_on == synapse_team.created_on
        assert name_team.modified_on == synapse_team.modified_on
        assert name_team.created_by == synapse_team.created_by
        assert name_team.modified_by == synapse_team.modified_by
        # Clean up
        synapse_team.delete()

    def test_from_id(self) -> None:
        # GIVEN a team object self.team
        # WHEN I create the team on Synapse
        test_team = self.team.create()
        # THEN I expect the team to be returned by from_id
        test_team_from_id = Team.from_id(id=test_team.id)
        assert test_team_from_id.id == test_team.id
        assert test_team_from_id.name == test_team.name
        assert test_team_from_id.description == test_team.description
        assert test_team_from_id.icon == test_team.icon
        assert test_team_from_id.etag == test_team.etag
        assert test_team_from_id.created_on == test_team.created_on
        assert test_team_from_id.modified_on == test_team.modified_on
        assert test_team_from_id.created_by == test_team.created_by
        assert test_team_from_id.modified_by == test_team.modified_by
        # Clean up
        test_team.delete()

    def test_from_name(self) -> None:
        # GIVEN a team object self.team
        # WHEN I create the team on Synapse
        test_team = self.team.create()

        # Searching by name is eventually consistent
        time.sleep(5)

        # THEN I expect the team to be returned by from_name
        test_team_from_name = Team.from_name(name=test_team.name)
        assert test_team_from_name.id == test_team.id
        assert test_team_from_name.name == test_team.name
        assert test_team_from_name.description == test_team.description
        assert test_team_from_name.icon == test_team.icon
        assert test_team_from_name.etag == test_team.etag
        assert test_team_from_name.created_on == test_team.created_on
        assert test_team_from_name.modified_on == test_team.modified_on
        assert test_team_from_name.created_by == test_team.created_by
        assert test_team_from_name.modified_by == test_team.modified_by
        # Clean up
        test_team.delete()

    def test_members(self) -> None:
        # GIVEN a team object self.team
        # WHEN I create the team on Synapse
        test_team = self.team.create()
        # THEN I expect the team members to be returned by members
        test_team_members = test_team.members()
        assert len(test_team_members) == 1
        assert test_team_members[0].team_id == test_team.id
        assert isinstance(test_team_members[0].member, UserGroupHeader)
        assert test_team_members[0].is_admin == True
        # Clean up
        test_team.delete()

    def test_invite(self) -> None:
        # GIVEN a team object self.team
        # WHEN I create the team on Synapse
        test_team = self.team.create()
        # AND I invite a user to the team
        test_invite = test_team.invite(
            user=self.TEST_USER,
            message=self.TEST_MESSAGE,
        )
        # THEN I expect the invite to be returned
        assert test_invite["id"] is not None
        assert test_invite["teamId"] == str(test_team.id)
        assert test_invite["inviteeId"] is not None
        assert test_invite["message"] == self.TEST_MESSAGE
        assert test_invite["createdOn"] is not None
        assert test_invite["createdBy"] is not None

        # Clean up
        test_team.delete()

    def test_open_invitations(self) -> None:
        # GIVEN a team object self.team
        # WHEN I create the team on Synapse
        test_team = self.team.create()
        # AND I invite a user to the team
        test_team.invite(
            user=self.TEST_USER,
            message=self.TEST_MESSAGE,
        )
        # THEN I expect the invite to be returned by open_invitations
        test_open_invitations = test_team.open_invitations()
        assert len(test_open_invitations) == 1
        assert test_open_invitations[0]["id"] is not None
        assert test_open_invitations[0]["teamId"] == str(test_team.id)
        assert test_open_invitations[0]["inviteeId"] is not None
        assert test_open_invitations[0]["message"] == self.TEST_MESSAGE
        assert test_open_invitations[0]["createdOn"] is not None
        assert test_open_invitations[0]["createdBy"] is not None

        # Clean up
        test_team.delete()
