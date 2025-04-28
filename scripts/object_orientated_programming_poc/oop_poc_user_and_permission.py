"""The purpose of this script is to demonstrate how to use the OOP interface for
UserProfile and permissions on some entities.
The following actions are shown in this script:
1. Get a UserProfile by name and id
2. Check if a UserProfile is certified
3. Set permissions on a File
4. Get permissions on a File
"""

import os

import synapseclient
from synapseclient.models import File, UserProfile

PROJECT_ID = "syn52948289"
TEST_ACCOUNT_NAME = "bfauble_synapse_test_account"
TEST_ACCOUNT_ID = 3489192

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


def user_profile():
    test_profile = UserProfile(username=TEST_ACCOUNT_NAME).get()
    print(f"Getting account by name: {test_profile}")

    test_profile = UserProfile(id=TEST_ACCOUNT_ID).get()
    print(f"Getting account by id: {test_profile}")

    test_profile = UserProfile.from_username(username=TEST_ACCOUNT_NAME)
    print(f"Getting account by name: {test_profile}")

    test_profile = UserProfile.from_id(user_id=TEST_ACCOUNT_ID)
    print(f"Getting account by id: {test_profile}")

    print(f"is certified - By ID?: {UserProfile(id=TEST_ACCOUNT_ID).is_certified()}")
    print(
        f"is certified - By name?: {UserProfile(username=TEST_ACCOUNT_NAME).is_certified()}"
    )


def set_permissions():
    name_of_file = "my_file_with_read_permissions_for_service_account.txt"
    path_to_file = os.path.join(os.path.expanduser("~/temp"), name_of_file)
    create_random_file(path_to_file)

    file_with_read = File(
        path=path_to_file,
        name=name_of_file,
        parent_id=PROJECT_ID,
    ).store()

    name_of_file = "my_file_with_default_permissions_for_service_account.txt"
    path_to_file = os.path.join(os.path.expanduser("~/temp"), name_of_file)
    create_random_file(path_to_file)

    file_with_default = File(
        path=path_to_file,
        name=name_of_file,
        parent_id=PROJECT_ID,
    ).store()

    file_with_read.set_permissions(principal_id=TEST_ACCOUNT_ID, access_type=["READ"])
    file_with_default.set_permissions(principal_id=TEST_ACCOUNT_ID)

    print(
        f"Read ACL for service account: {file_with_read.get_acl(principal_id=TEST_ACCOUNT_ID)}"
    )
    print(
        f"Read/Download ACL for service acccount: {file_with_default.get_acl(principal_id=TEST_ACCOUNT_ID)}"
    )
    print(f"Admin permissions for self: {file_with_read.get_permissions()}")
    print(f"Admin permissions for self: {file_with_default.get_permissions()}")


user_profile()
set_permissions()
