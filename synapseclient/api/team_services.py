"""This module is responsible for exposing the services defined at:
<https://rest-docs.synapse.org/rest/#org.sagebionetworks.repo.web.controller.TeamController>
"""

import json
from typing import TYPE_CHECKING, AsyncGenerator, Dict, List, Optional, Union

from synapseclient.api import rest_get_paginated_async
from synapseclient.core.exceptions import SynapseNotFoundError
from synapseclient.core.utils import id_of

if TYPE_CHECKING:
    from synapseclient import Synapse


async def post_team_list(
    team_ids: List[int],
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Optional[List[Dict[str, Union[str, bool]]]]:
    """
    Retrieve a list of Teams given their IDs. Invalid IDs in the list are ignored:
    The results list is simply smaller than the list of IDs passed in.

    Arguments:
        team_ids: List of team IDs to retrieve
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Returns:
        List of dictionaries representing <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/Team.html>
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    request_body = {"list": team_ids}

    response = await client.rest_post_async(
        uri="/teamList", body=json.dumps(request_body)
    )

    if "list" in response:
        return response["list"] or None

    return None


async def create_team(
    name: str,
    description: Optional[str] = None,
    icon: Optional[str] = None,
    can_public_join: bool = False,
    can_request_membership: bool = True,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict:
    """
    Creates a new team.

    Arguments:
        name: The name of the team to create.
        description: A description of the team.
        icon: The FileHandleID of the icon to be used for the team.
        can_public_join: Whether the team can be joined by anyone. Defaults to False.
        can_request_membership: Whether the team can request membership. Defaults to True.
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Returns:
        Dictionary representing the created team
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    request_body = {
        "name": name,
        "description": description,
        "icon": icon,
        "canPublicJoin": can_public_join,
        "canRequestMembership": can_request_membership,
    }

    response = await client.rest_post_async(uri="/team", body=json.dumps(request_body))
    return response


async def delete_team(
    id: int,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> None:
    """
    Deletes a team.

    Arguments:
        id: The ID of the team to delete.
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    await client.rest_delete_async(uri=f"/team/{id}")


async def get_teams_for_user(
    user_id: str,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> AsyncGenerator[Dict, None]:
    """
    Retrieve teams for the matching user ID as an async generator.

    This function yields team dictionaries one by one as they are retrieved from the
    paginated API response, allowing for memory-efficient processing of large result sets.

    Arguments:
        user_id: Identifier of a user.
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Yields:
        Team dictionaries that the user is a member of. Each dictionary matches the
        <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/Team.html>
        structure.
    """
    async for result in rest_get_paginated_async(
        uri=f"/user/{user_id}/team", synapse_client=synapse_client
    ):
        yield result


async def get_team(
    id: Union[int, str],
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict:
    """
    Finds a team with a given ID or name.

    Arguments:
        id: The ID or name of the team to retrieve.
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Returns:
        Dictionary representing the team
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    # Retrieves team id
    teamid = id_of(id)
    try:
        int(teamid)
    except (TypeError, ValueError):
        if isinstance(id, str):
            teams = await find_team(id, synapse_client=client)
            for team in teams:
                if team.get("name") == id:
                    teamid = team.get("id")
                    break
            else:
                raise ValueError(f'Can\'t find team "{teamid}"')
        else:
            raise ValueError(f'Can\'t find team "{teamid}"')

    response = await client.rest_get_async(uri=f"/team/{teamid}")
    return response


async def find_team(
    name: str,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> List[Dict]:
    """
    Retrieve a Teams matching the supplied name fragment

    Arguments:
        name: A team name
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Returns:
        List of team dictionaries
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    results = []
    async for result in rest_get_paginated_async(
        uri=f"/teams?fragment={name}", synapse_client=client
    ):
        results.append(result)
    return results


async def get_team_members(
    team: Union[int, str],
    *,
    synapse_client: Optional["Synapse"] = None,
) -> List[Dict]:
    """
    Lists the members of the given team.

    Arguments:
        team: A team ID or name.
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Returns:
        List of team member dictionaries
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    team_id = id_of(team)
    results = []
    async for result in rest_get_paginated_async(
        uri=f"/teamMembers/{team_id}", synapse_client=client
    ):
        results.append(result)
    return results


async def send_membership_invitation(
    team_id: int,
    invitee_id: Optional[str] = None,
    invitee_email: Optional[str] = None,
    message: Optional[str] = None,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict:
    """
    Create a membership invitation and send an email notification to the invitee.

    Arguments:
        team_id: Synapse team ID
        invitee_id: Synapse username or profile id of user
        invitee_email: Email of user
        message: Additional message for the user getting invited to the team.
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Returns:
        MembershipInvitation dictionary
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    invite_request = {"teamId": str(team_id), "message": message}
    if invitee_email is not None:
        invite_request["inviteeEmail"] = str(invitee_email)
    if invitee_id is not None:
        invite_request["inviteeId"] = str(invitee_id)

    response = await client.rest_post_async(
        uri="/membershipInvitation", body=json.dumps(invite_request)
    )
    return response


async def get_team_open_invitations(
    team: Union[int, str],
    *,
    synapse_client: Optional["Synapse"] = None,
) -> List[Dict]:
    """
    Retrieve the open requests submitted to a Team

    Arguments:
        team: A team ID or name.
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Returns:
        List of MembershipRequest dictionaries
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    team_id = id_of(team)
    results = []
    async for result in rest_get_paginated_async(
        uri=f"/team/{team_id}/openInvitation", synapse_client=client
    ):
        results.append(result)
    return results


async def get_membership_status(
    user_id: str,
    team: Union[int, str],
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict:
    """
    Retrieve a user's Team Membership Status bundle.

    Arguments:
        user_id: Synapse user ID
        team: A team ID or name.
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Returns:
        Dictionary of TeamMembershipStatus:
        <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/TeamMembershipStatus.html>
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    team_id = id_of(team)
    uri = f"/team/{team_id}/member/{user_id}/membershipStatus"
    response = await client.rest_get_async(uri=uri)
    return response


async def delete_membership_invitation(
    invitation_id: str,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> None:
    """
    Delete an invitation. Note: The client must be an administrator of the
    Team referenced by the invitation or the invitee to make this request.

    Arguments:
        invitation_id: Open invitation id
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    await client.rest_delete_async(uri=f"/membershipInvitation/{invitation_id}")


async def invite_to_team(
    team: Union[int, str],
    user: Optional[str] = None,
    invitee_email: Optional[str] = None,
    message: Optional[str] = None,
    force: bool = False,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Optional[Dict]:
    """
    Invite user to a Synapse team via Synapse username or email (choose one or the other)

    Arguments:
        team: A team ID or name.
        user: Synapse username or profile id of user
        invitee_email: Email of user
        message: Additional message for the user getting invited to the team.
        force: If an open invitation exists for the invitee, the old invite will be cancelled.
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Returns:
        MembershipInvitation or None if user is already a member
    """
    from synapseclient.models import UserProfile

    # Input validation
    id_email_specified = invitee_email is not None and user is not None
    id_email_notspecified = invitee_email is None and user is None
    if id_email_specified or id_email_notspecified:
        raise ValueError("Must specify either 'user' or 'inviteeEmail'")

    team_id = id_of(team)
    is_member = False
    open_invitations = await get_team_open_invitations(
        team_id, synapse_client=synapse_client
    )

    if user is not None:
        try:
            user_profile = await UserProfile(username=str(user)).get_async(
                synapse_client=synapse_client
            )
        except SynapseNotFoundError:
            try:
                user_profile = await UserProfile(id=int(user)).get_async(
                    synapse_client=synapse_client
                )
            except (ValueError, TypeError) as ex:
                raise SynapseNotFoundError(f'Can\'t find user "{user}"') from ex
        invitee_id = user_profile.id

        membership_status = await get_membership_status(
            user_id=invitee_id, team=team_id, synapse_client=synapse_client
        )
        is_member = membership_status["isMember"]
        open_invites_to_user = [
            invitation
            for invitation in open_invitations
            if int(invitation.get("inviteeId")) == invitee_id
        ]
    else:
        invitee_id = None
        open_invites_to_user = [
            invitation
            for invitation in open_invitations
            if invitation.get("inviteeEmail") == invitee_email
        ]

    # Only invite if the invitee is not a member and
    # if invitee doesn't have an open invitation unless force=True
    if not is_member and (not open_invites_to_user or force):
        # Delete all old invitations
        for invite in open_invites_to_user:
            await delete_membership_invitation(
                invitation_id=invite["id"], synapse_client=synapse_client
            )
        return await send_membership_invitation(
            team_id,
            invitee_id=invitee_id,
            invitee_email=invitee_email,
            message=message,
            synapse_client=synapse_client,
        )
    else:
        from synapseclient import Synapse

        client = Synapse.get_client(synapse_client=synapse_client)

        if is_member:
            not_sent_reason = f"`{user_profile.username}` is already a member"
        else:
            not_sent_reason = (
                f"`{user_profile.username}` already has an open invitation "
                "Set `force=True` to send new invite."
            )

        client.logger.warning("No invitation sent: {}".format(not_sent_reason))
        return None
