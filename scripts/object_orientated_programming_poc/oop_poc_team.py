"""The purpose of this script is to demonstrate how to use the new OOP interface for teams.
The following actions are shown in this script:
1. Creating a Team
2. Instantiating a Team object from Synapse
3. Getting information about the members of a Team
4. Inviting a user to a Team
5. Checking on invitations to join a Team
6. Deleting a Team
"""

import time

import synapseclient
from synapseclient.models.team import Team

syn = synapseclient.Synapse(debug=True)
syn.login()


def new_team():
    # Create a team
    new_team = Team(
        name="python-client-test-team",
        description="testing OOP interface",
        can_public_join=False,
    )
    my_synapse_team = new_team.create()
    print(my_synapse_team)

    # Instantiate a Team object from a Synapse team
    my_team = Team.from_id(id=my_synapse_team.id)
    print(my_team)

    # Sleep because the API for retrieving a team by name is eventually consistent. from_id works right away, however.
    time.sleep(5)
    my_team = Team.from_name(name=my_synapse_team.name)
    print(my_team)

    # Refresh the team to get the latest information
    my_team.get()
    print(my_team)

    # Get information about the members of a Team
    members = my_team.members()
    print(members)

    # Invite a user to a Team
    invite = my_team.invite(
        user="test_account_synapse_client",
        message="testing OOP interface (do not accept)",
    )
    print(invite)

    # Get open invitations for the Team
    invitations = my_team.open_invitations()
    print(invitations)

    # Delete the Team
    my_team.delete()


new_team()
