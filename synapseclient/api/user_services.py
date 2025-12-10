"""This module is responsible for exposing the services defined at:
<https://rest-docs.synapse.org/rest/#org.sagebionetworks.repo.web.controller.UserGroupController>
"""

import urllib.parse as urllib_urlparse
from typing import TYPE_CHECKING, Dict, List, Optional, Union

from synapseclient.api import rest_get_paginated_async
from synapseclient.core.exceptions import (
    SynapseError,
    SynapseHTTPError,
    SynapseNotFoundError,
)

if TYPE_CHECKING:
    from synapseclient import Synapse


async def get_user_group_headers_batch(
    ids: List[str],
    *,
    synapse_client: Optional["Synapse"] = None,
) -> List[Dict[str, Union[str, bool]]]:
    """
    Batch get UserGroupHeaders. This fetches information about a collection of
    users or groups, specified by Synapse IDs.

    Arguments:
        ids: List of user/group IDs to retrieve
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Returns:
        List representing "children" in
        <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/UserGroupHeaderResponsePage.html>
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    ids_param = ",".join(ids)
    params = {"ids": ids_param}

    response = await client.rest_get_async(uri="/userGroupHeaders/batch", params=params)

    if "children" in response:
        return response["children"] or []

    return []


async def get_user_profile_by_id(
    id: Optional[int] = None,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict:
    """
    Get the details about a Synapse user by ID.
    Retrieves information on the current user if 'id' is omitted.

    Arguments:
        id: The ownerId of a user
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Returns:
        The user profile for the user of interest.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    if id:
        if not isinstance(id, int):
            raise TypeError("id must be an 'ownerId' integer")
    else:
        id = ""

    uri = f"/userProfile/{id}"
    response = await client.rest_get_async(uri=uri)
    return response


async def get_user_profile_by_username(
    username: Optional[str] = None,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict:
    """
    Get the details about a Synapse user by username.
    Retrieves information on the current user if 'username' is omitted or empty string.

    Arguments:
        username: The userName of a user
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Returns:
        The user profile for the user of interest.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    is_none = username is None
    is_str = isinstance(username, str)
    if not is_str and not is_none:
        raise TypeError("username must be string or None")

    if is_str:
        principals = await _find_principals(username, synapse_client=synapse_client)
        for principal in principals:
            if principal.get("userName", None).lower() == username.lower():
                id = principal["ownerId"]
                break
        else:
            raise SynapseNotFoundError(f"Can't find user '{username}'")
    else:
        id = ""

    uri = f"/userProfile/{id}"
    response = await client.rest_get_async(uri=uri)
    return response


async def is_user_certified(
    user: Union[str, int],
    *,
    synapse_client: Optional["Synapse"] = None,
) -> bool:
    """
    Determines whether a Synapse user is a certified user.

    Arguments:
        user: Synapse username or Id
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Returns:
        True if the Synapse user is certified
    """

    # Check if userid or username exists - get user profile first
    try:
        # if id is unset or a userID, this will succeed
        user_id = "" if user is None else int(user)
    except (TypeError, ValueError):
        # It's a username, need to look it up
        if isinstance(user, str):
            principals = await _find_principals(user, synapse_client=synapse_client)
            for principal in principals:
                if principal.get("userName", None).lower() == user.lower():
                    user_id = principal["ownerId"]
                    break
            else:  # no break
                raise ValueError(f'Can\'t find user "{user}": ')
        else:
            raise ValueError(f"Invalid user identifier: {user}")

    # Get passing record
    try:
        certification_status = await _get_certified_passing_record(
            user_id, synapse_client=synapse_client
        )
        return certification_status["passed"]
    except SynapseHTTPError as ex:
        if ex.response.status_code == 404:
            # user hasn't taken the quiz
            return False
        raise


async def _find_principals(
    query_string: str,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> List[Dict]:
    """
    Find users or groups by name or email.

    Arguments:
        query_string: The string to search for
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Returns:
        A list of userGroupHeader objects with fields displayName, email, firstName, lastName, isIndividual, ownerId
    """

    uri = "/userGroupHeaders?prefix=%s" % urllib_urlparse.quote(query_string)

    # Collect all results from the paginated endpoint
    results = []
    async for result in rest_get_paginated_async(
        uri=uri, synapse_client=synapse_client
    ):
        results.append(result)

    return results


async def _get_certified_passing_record(
    userid: int,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Union[str, int, bool, list]]:
    """
    Retrieve the Passing Record on the User Certification test for the given user.

    Arguments:
        userid: Synapse user Id
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Returns:
        Synapse Passing Record. A record of whether a given user passed a given test.
        <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/quiz/PassingRecord.html>
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    response = await client.rest_get_async(
        uri=f"/user/{userid}/certifiedUserPassingRecord"
    )
    return response


async def get_user_by_principal_id_or_name(
    principal_id: Optional[Union[str, int]] = None,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> int:
    """
    Given either a string, int or None finds the corresponding user where None implies PUBLIC.

    Arguments:
        principal_id: Identifier of a user or group. '273948' is for all registered Synapse users
                     and '273949' is for public access. None implies public access.
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Returns:
        The integer ID of the user.

    Raises:
        SynapseError: If the user cannot be found or is ambiguous.

    Example: Get user ID by principal ID
        Get the user ID for a given principal ID.

        ```python
        import asyncio
        from synapseclient import Synapse
        from synapseclient.api import get_user_by_principal_id_or_name

        syn = Synapse()
        syn.login()

        async def main():
            # Get public user ID
            public_id = await get_user_by_principal_id_or_name(principal_id=None)
            print(f"Public user ID: {public_id}")

            # Get user ID by username
            user_id = await get_user_by_principal_id_or_name(principal_id="username")
            print(f"User ID for 'username': {user_id}")

        asyncio.run(main())
        ```
    """
    from synapseclient import PUBLIC, Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    if principal_id is None or principal_id == "PUBLIC":
        return PUBLIC
    try:
        return int(principal_id)
    # If principal_id is not a number assume it is a name or email
    except ValueError as ex:
        user_profiles = await client.rest_get_async(
            uri=f"/userGroupHeaders?prefix={principal_id}"
        )
        total_results = len(user_profiles["children"])
        if total_results == 1:
            return int(user_profiles["children"][0]["ownerId"])
        elif total_results > 1:
            for profile in user_profiles["children"]:
                if profile["userName"] == principal_id:
                    return int(profile["ownerId"])

        supplemental_message = (
            "Please be more specific" if total_results > 1 else "No matches"
        )
        raise SynapseError(
            f"Unknown Synapse user ({principal_id}). {supplemental_message}."
        ) from ex


async def get_user_bundle(
    user_id: int,
    mask: int,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Optional[Dict[str, Union[str, dict]]]:
    """
    Retrieve the user bundle for the given user.

    Arguments:
        user_id: Synapse user Id
        mask: Bit field indicating which components to include in the bundle.
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Returns:
        Synapse User Bundle or None if user not found
        <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/UserBundle.html>
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    try:
        return await client.rest_get_async(uri=f"/user/{user_id}/bundle?mask={mask}")
    except SynapseHTTPError as ex:
        if ex.response.status_code == 404:
            return None
        raise
