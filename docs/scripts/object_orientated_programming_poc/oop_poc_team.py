"""The purpose of this script is to demonstrate how to use the new OOP interface for teams.
The following actions are shown in this script:
1. Creating a Team
2. Instantiating a Team object from Synapse
3. Getting information about the members of a Team
4. Inviting a user to a Team
5. Checking on invitations to join a Team
6. Deleting a Team
"""

import asyncio

from synapseclient.models.team import Team
import synapseclient


syn = synapseclient.Synapse(debug=True)
syn.login()


async def new_team():
    # Create a team
    new_team = Team(
        name="python-client-test-team",
        description="testing OOP interface",
        can_public_join=False,
    )
    my_synapse_team = await new_team.create()
    print(my_synapse_team)
    # sleep for a bit to allow the team to be created (Synapse API slow?)
    await asyncio.sleep(5)
    # Instantiate a Team object from a Synapse team
    my_team = await Team().from_id(id=my_synapse_team.id)
    print(my_team)

    my_team = await Team().from_name(name=my_synapse_team.name)
    print(my_team)
    # Get information about the members of a Team
    members = await my_team.members()
    print(members)

    # Invite a user to a Team
    invite = await my_team.invite(
        user="test_account_synapse_client",
        message="testing OOP interface (do not accept)",
    )
    print(invite)

    # Get open invitations for the Team
    invitations = await my_team.open_invitations()
    print(invitations)

    # Delete the Team
    await my_team.delete()


asyncio.run(new_team())
