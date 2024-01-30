"""The purpose of this script is to demonstrate how to use the OOP interface for
UserProfile and permissions on some entities.
The following actions are shown in this script:
1. Get a UserProfile by name and id
2. Check if a UserProfile is certified
3. Set permissions on a File
4. Get permissions on a File
"""
import asyncio
import os

from synapseclient.models import (
    File,
    UserProfile,
)
import synapseclient

PROJECT_ID = "syn52948289"
DPE_TEAM_SERVICE_ACCOUNT_NAME = "synapse-service-dpe-team"
DPE_TEAM_SERVICE_ACCOUNT_ID = "3485485"

syn = synapseclient.Synapse(debug=True)
syn.login()


def create_random_file(
    path: str,
) -> None:
    """Create a random file with random data.

    :param path: The path to create the file at.
    """
    with open(path, "wb") as f:
        f.write(os.urandom(1))


async def user_profile():
    dpe_team_profile = await UserProfile(username=DPE_TEAM_SERVICE_ACCOUNT_NAME).get()
    print(f"Getting account by name: {dpe_team_profile}")

    dpe_team_profile = await UserProfile(id=DPE_TEAM_SERVICE_ACCOUNT_ID).get()
    print(f"Getting account by id: {dpe_team_profile}")

    dpe_team_profile = await UserProfile.from_username(
        username=DPE_TEAM_SERVICE_ACCOUNT_NAME
    )
    print(f"Getting account by name: {dpe_team_profile}")

    dpe_team_profile = await UserProfile.from_id(user_id=DPE_TEAM_SERVICE_ACCOUNT_ID)
    print(f"Getting account by id: {dpe_team_profile}")

    print(
        f"is certified - By ID?: {await UserProfile(id=DPE_TEAM_SERVICE_ACCOUNT_ID).is_certified()}"
    )
    print(
        f"is certified - By name?: {await UserProfile(username=DPE_TEAM_SERVICE_ACCOUNT_NAME).is_certified()}"
    )


async def set_permissions():
    name_of_file = "my_file_with_read_permissions_for_service_account.txt"
    path_to_file = os.path.join(os.path.expanduser("~/temp"), name_of_file)
    create_random_file(path_to_file)

    file_with_read = await File(
        path=path_to_file,
        name=name_of_file,
        parent_id=PROJECT_ID,
    ).store()

    name_of_file = "my_file_with_default_permissions_for_service_account.txt"
    path_to_file = os.path.join(os.path.expanduser("~/temp"), name_of_file)
    create_random_file(path_to_file)

    file_with_default = await File(
        path=path_to_file,
        name=name_of_file,
        parent_id=PROJECT_ID,
    ).store()

    await file_with_read.set_permissions(
        principal_id=DPE_TEAM_SERVICE_ACCOUNT_ID, access_type=["READ"]
    )
    await file_with_default.set_permissions(principal_id=DPE_TEAM_SERVICE_ACCOUNT_ID)

    print(
        f"Read ACL for service account: {await file_with_read.get_acl(principal_id=DPE_TEAM_SERVICE_ACCOUNT_ID)}"
    )
    print(
        f"Read/Download ACL for service acccount: {await file_with_default.get_acl(principal_id=DPE_TEAM_SERVICE_ACCOUNT_ID)}"
    )
    print(f"Admin permissions for self: {await file_with_read.get_permissions()}")
    print(f"Admin permissions for self: {await file_with_default.get_permissions()}")


asyncio.run(user_profile())
asyncio.run(set_permissions())
